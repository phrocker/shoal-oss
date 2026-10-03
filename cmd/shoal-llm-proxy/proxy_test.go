// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// fakePlane stands in for the explorer's admission surface.
type fakePlane struct {
	outcome  string
	withhold []string
	token    *admissionToken
	status   int
	requests []admissionRequestWire
	reports  []admissionReportWire
	server   *httptest.Server
}

func newFakePlane(t *testing.T, outcome string, withhold []string) *fakePlane {
	t.Helper()
	plane := &fakePlane{outcome: outcome, withhold: withhold, status: http.StatusOK}
	plane.token = &admissionToken{
		ActionID: "YWN0aW9u", TokenID: "dG9rZW4", Version: 1,
		ExpiresAt: time.Now().Add(time.Minute),
	}
	plane.server = httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			if plane.status != http.StatusOK {
				writer.WriteHeader(plane.status)
				return
			}
			raw, _ := io.ReadAll(request.Body)
			switch {
			case strings.HasSuffix(request.URL.Path, "/request"):
				var decoded admissionRequestWire
				_ = json.Unmarshal(raw, &decoded)
				plane.requests = append(plane.requests, decoded)
				body := admissionGrantWire{
					Outcome: plane.outcome, Withhold: plane.withhold,
				}
				if plane.outcome != outcomeDenied {
					body.Token = plane.token
				}
				_ = json.NewEncoder(writer).Encode(body)
			case strings.HasSuffix(request.URL.Path, "/report"):
				var decoded admissionReportWire
				_ = json.Unmarshal(raw, &decoded)
				plane.reports = append(plane.reports, decoded)
				writer.WriteHeader(http.StatusOK)
			default:
				writer.WriteHeader(http.StatusNotFound)
			}
		}))
	t.Cleanup(plane.server.Close)
	return plane
}

// fakeUpstream stands in for the model provider.
type fakeUpstream struct {
	received []byte
	status   int
	body     string
	calls    int
	server   *httptest.Server
}

func newFakeUpstream(t *testing.T) *fakeUpstream {
	t.Helper()
	upstream := &fakeUpstream{status: http.StatusOK, body: `{"id":"done"}`}
	upstream.server = httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			upstream.calls++
			upstream.received, _ = io.ReadAll(request.Body)
			writer.WriteHeader(upstream.status)
			_, _ = io.WriteString(writer, upstream.body)
		}))
	t.Cleanup(upstream.server.Close)
	return upstream
}

func newTestProxy(t *testing.T, plane *fakePlane, upstream *fakeUpstream) (*proxy, *[]string) {
	t.Helper()
	var logged []string
	base, err := url.Parse(plane.server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := &admissionClient{
		base: base, http: plane.server.Client(),
		credential:      func() (string, error) { return "plane-token", nil },
		agentID:         "agent",
		agentGeneration: 1,
		capability:      "llm.proxy", action: "complete",
		sourceID: []byte("source"), policyID: []byte("policy"),
		lease: time.Minute,
	}
	governed, err := newProxy(
		client, upstream.server.URL,
		func() (string, error) { return "upstream-key", nil },
		[]string{"example.test"},
		5*time.Second, time.Now,
		// The rendered line, not the format string. Capturing only the format
		// made TestNoPromptOrCompletionIsLogged unable to fail for the one
		// thing it is named for: content passed as an argument, which is how
		// content would actually reach a log.
		func(format string, values ...any) {
			logged = append(logged, fmt.Sprintf(format, values...))
		})
	if err != nil {
		t.Fatal(err)
	}
	governed.client = upstream.server.Client()
	return governed, &logged
}

func post(t *testing.T, governed *proxy, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(
		http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	request.Host = "example.test"
	recorder := httptest.NewRecorder()
	governed.routes().ServeHTTP(recorder, request)
	return recorder
}

const plainCall = `{"model":"gpt","messages":[{"role":"user","content":"hello"}]}`

// TestAnUnmodifiedClientIsGoverned is the acceptance criterion the proxy exists
// for: a caller that knows nothing about Shoal works through it unchanged.
func TestAnUnmodifiedClientIsGoverned(t *testing.T) {
	plane := newFakePlane(t, outcomeAllowed, nil)
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)

	recorder := post(t, governed, plainCall)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if upstream.calls != 1 {
		t.Fatalf("upstream calls = %d, want 1", upstream.calls)
	}
	if recorder.Body.String() != `{"id":"done"}` {
		t.Fatalf("body = %q", recorder.Body.String())
	}
	if len(plane.requests) != 1 || len(plane.reports) != 1 {
		t.Fatalf("admissions = %d, reports = %d; want 1 and 1",
			len(plane.requests), len(plane.reports))
	}
}

// TestShoalNeverReceivesThePrompt is the property the whole design rests on.
//
// The admission surface takes a declaration precisely so the plane deciding
// whether content may be transmitted does not hold a copy of it. A proxy that
// forwarded the prompt for adjudication would defeat the thing it is built on.
func TestShoalNeverReceivesThePrompt(t *testing.T) {
	plane := newFakePlane(t, outcomeAllowed, nil)
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)

	const secret = "the-sky-is-the-colour-of-television"
	post(t, governed, `{"model":"gpt","messages":[{"role":"user","content":"`+secret+`"}]}`)

	if len(plane.requests) != 1 {
		t.Fatalf("admissions = %d", len(plane.requests))
	}
	everything, err := json.Marshal(plane.requests[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(everything), secret) {
		t.Fatalf("the prompt reached the decision plane: %s", everything)
	}
	// The declaration still has to be useful, or the plane is adjudicating
	// nothing.
	var declaration map[string]any
	if err := json.Unmarshal(plane.requests[0].Input, &declaration); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"model", "message_count", "content_bytes"} {
		if _, ok := declaration[key]; !ok {
			t.Fatalf("declaration omits %q: %s", key, plane.requests[0].Input)
		}
	}
}

// TestADenialNeverReachesTheUpstream is the stop this proxy is for. Everywhere
// else in Shoal enforcement withholds from a response that is still produced.
func TestADenialNeverReachesTheUpstream(t *testing.T) {
	plane := newFakePlane(t, outcomeDenied, nil)
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)

	recorder := post(t, governed, plainCall)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", recorder.Code)
	}
	if upstream.calls != 0 {
		t.Fatal("a denied call reached the upstream")
	}
	// A denial carries no token, so there is nothing to report against — and
	// reporting would be a statement that an effect occurred.
	if len(plane.reports) != 0 {
		t.Fatalf("a denial was reported: %#v", plane.reports)
	}
	assertRefusalNamesNothing(t, recorder.Body.String())
}

// TestAnUnreachablePlaneDeniesAndSaysSo covers the fail-closed requirement and
// the distinction the issue insists on: a caller told "denied" will not retry,
// one told "unavailable" should, and an operator needs to tell an outage from a
// policy change.
func TestAnUnreachablePlaneDeniesAndSaysSo(t *testing.T) {
	plane := newFakePlane(t, outcomeAllowed, nil)
	plane.status = http.StatusInternalServerError
	upstream := newFakeUpstream(t)
	governed, logged := newTestProxy(t, plane, upstream)

	recorder := post(t, governed, plainCall)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 so the caller can tell this from a denial",
			recorder.Code)
	}
	if upstream.calls != 0 {
		t.Fatal("an unreachable plane still let the call through")
	}
	var operatorSaw bool
	for _, line := range *logged {
		if strings.Contains(line, "unavailable") {
			operatorSaw = true
		}
	}
	if !operatorSaw {
		t.Fatalf("the operator cannot tell this from a policy denial: %v", *logged)
	}
	assertRefusalNamesNothing(t, recorder.Body.String())
}

// TestObligationsAreAppliedToTheOutboundRequest is the criterion that fails if
// the obligation is dropped. Obligations are better than refusal: the plane
// said which part to remove, so removing it beats refusing the whole call.
func TestObligationsAreAppliedToTheOutboundRequest(t *testing.T) {
	plane := newFakePlane(t, outcomeObligated, []string{"doc-b"})
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)

	recorder := post(t, governed,
		`{"model":"gpt","messages":[{"role":"user","content":"hi"}],`+
			`"shoal_references":["doc-a","doc-b"]}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if upstream.calls != 1 {
		t.Fatalf("upstream calls = %d, want 1", upstream.calls)
	}
	forwarded := string(upstream.received)
	if strings.Contains(forwarded, "doc-b") {
		t.Fatalf("the withheld reference was forwarded: %s", forwarded)
	}
	if !strings.Contains(forwarded, "doc-a") {
		t.Fatalf("a reference that was not withheld was dropped: %s", forwarded)
	}
}

// TestAnUnsatisfiableObligationRefusesAndReports is the one case where refusing
// the whole call is right. The plane believes it constrained this call, so
// ignoring an obligation it cannot satisfy would be worse than refusing.
func TestAnUnsatisfiableObligationRefusesAndReports(t *testing.T) {
	// The obligation names a reference the caller never declared, so there is
	// nothing to drop that would satisfy it.
	plane := newFakePlane(t, outcomeObligated, []string{"doc-never-declared"})
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)

	recorder := post(t, governed,
		`{"model":"gpt","messages":[{"role":"user","content":"hi"}],`+
			`"shoal_references":["doc-a"]}`)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", recorder.Code)
	}
	if upstream.calls != 0 {
		t.Fatal("an unsatisfiable obligation still reached the upstream")
	}
	// Reported, so the plane learns the obligation was unsatisfiable rather
	// than that the caller went dark.
	if len(plane.reports) != 1 || !plane.reports[0].Failed {
		t.Fatalf("reports = %#v, want one failure", plane.reports)
	}
	if plane.reports[0].ErrorCode != "obligation_unsatisfiable" {
		t.Fatalf("error code = %q", plane.reports[0].ErrorCode)
	}
}

// TestAnUpstreamFailureIsReportedAsOne keeps the loop honest: the plane must be
// able to tell a call that happened and failed from one that never happened.
func TestAnUpstreamFailureIsReportedAsOne(t *testing.T) {
	plane := newFakePlane(t, outcomeAllowed, nil)
	upstream := newFakeUpstream(t)
	upstream.status = http.StatusTooManyRequests
	governed, _ := newTestProxy(t, plane, upstream)

	post(t, governed, plainCall)
	if len(plane.reports) != 1 {
		t.Fatalf("reports = %d, want 1", len(plane.reports))
	}
	if !plane.reports[0].Failed || plane.reports[0].ErrorCode != "upstream_error" {
		t.Fatalf("report = %#v, want a reported upstream failure", plane.reports[0])
	}
}

// TestTheReportCarriesNoCompletion is the other half of the content property:
// the prompt never goes in, and the completion never comes back out.
//
// This one is belt-and-braces rather than the real guarantee. reportOutcome is
// never given the response body, and relay streams it through without
// buffering, so no variable at the report site holds a completion — leaking
// one would require changing a signature. Injecting an unrelated literal into
// the report does not fail this test, and should not: it asserts that the
// actual completion does not travel, which is the property that matters.
func TestTheReportCarriesNoCompletion(t *testing.T) {
	plane := newFakePlane(t, outcomeAllowed, nil)
	upstream := newFakeUpstream(t)
	const secret = "a-completion-nobody-should-store"
	upstream.body = `{"choices":[{"text":"` + secret + `"}]}`
	governed, _ := newTestProxy(t, plane, upstream)

	post(t, governed, plainCall)
	if len(plane.reports) != 1 {
		t.Fatalf("reports = %d", len(plane.reports))
	}
	everything, err := json.Marshal(plane.reports[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(everything), secret) {
		t.Fatalf("the completion reached the decision plane: %s", everything)
	}
}

// TestNoPromptOrCompletionIsLogged pins the default configuration. An audit
// record references the admission, not the payload.
func TestNoPromptOrCompletionIsLogged(t *testing.T) {
	plane := newFakePlane(t, outcomeAllowed, nil)
	upstream := newFakeUpstream(t)
	const prompt = "prompt-text-that-must-not-be-logged"
	const completion = "completion-text-that-must-not-be-logged"
	upstream.body = `{"text":"` + completion + `"}`
	governed, logged := newTestProxy(t, plane, upstream)

	post(t, governed, `{"model":"gpt","messages":[{"role":"user","content":"`+prompt+`"}]}`)
	for _, line := range *logged {
		if strings.Contains(line, prompt) || strings.Contains(line, completion) {
			t.Fatalf("content reached the log: %q", line)
		}
	}
}

// TestAnUnrecognisedOutcomeIsNotAnAllowance covers a plane newer than this
// proxy. A build that met an outcome it predates and treated it as permission
// would make every future outcome default to the permissive reading — the
// opposite of how every other unknown value in Shoal is handled.
func TestAnUnrecognisedOutcomeIsNotAnAllowance(t *testing.T) {
	plane := newFakePlane(t, "allowed_pending_review", nil)
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)

	recorder := post(t, governed, plainCall)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: an outcome this build does not know "+
			"is not permission", recorder.Code)
	}
	if upstream.calls != 0 {
		t.Fatal("an unrecognised outcome let the call through")
	}
}

// TestAnAllowanceWithoutATokenIsRefused covers the other malformed grant. A
// call admitted with no token can never be reported, and an unreportable call
// is one the plane can never learn the outcome of.
func TestAnAllowanceWithoutATokenIsRefused(t *testing.T) {
	plane := newFakePlane(t, outcomeAllowed, nil)
	plane.token = nil
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)

	recorder := post(t, governed, plainCall)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", recorder.Code)
	}
	if upstream.calls != 0 {
		t.Fatal("an allowance with no token let the call through")
	}
}

// TestAForeignHostIsRefusedBeforeAnythingHappens is the DNS-rebinding gate.
//
// The default listener is loopback, so without this a page in a browser can
// resolve a name it controls to 127.0.0.1, POST here, and spend the operator's
// upstream credential on a call nobody made. A JSON body is not protection: a
// form post with a text/plain content type reaches the same handler and needs
// no preflight.
func TestAForeignHostIsRefusedBeforeAnythingHappens(t *testing.T) {
	plane := newFakePlane(t, outcomeAllowed, nil)
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)

	request := httptest.NewRequest(
		http.MethodPost, "/v1/chat/completions", strings.NewReader(plainCall))
	request.Host = "attacker.example"
	recorder := httptest.NewRecorder()
	governed.routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusMisdirectedRequest {
		t.Fatalf("status = %d, want 421", recorder.Code)
	}
	if upstream.calls != 0 {
		t.Fatal("a misdirected request reached the upstream")
	}
	// Refused before the plane is even asked: the gate runs before routing, so
	// a rebound request costs no admission and no upstream credential.
	if len(plane.requests) != 0 {
		t.Fatalf("a misdirected request was adjudicated: %#v", plane.requests)
	}
	if strings.Contains(recorder.Body.String(), "attacker.example") {
		t.Fatalf("the refusal echoes the submitted authority: %s", recorder.Body.String())
	}
}

// assertRefusalNamesNothing checks a caller-facing refusal discloses nothing. A
// refusal that explains itself is an oracle over whatever it explains.
func assertRefusalNamesNothing(t *testing.T, body string) {
	t.Helper()
	for _, leak := range []string{"policy", "compartment", "doc-", "source", "agent"} {
		if strings.Contains(strings.ToLower(body), leak) {
			t.Fatalf("refusal names %q: %s", leak, body)
		}
	}
}
