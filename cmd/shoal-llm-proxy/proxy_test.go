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
	"errors"
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
	// paths records what was actually requested. The fake answered every path
	// identically, which is why the acceptance test could not see the endpoint
	// being built with a duplicated version segment.
	paths    []string
	location string
	server   *httptest.Server
}

func newFakeUpstream(t *testing.T) *fakeUpstream {
	t.Helper()
	upstream := &fakeUpstream{status: http.StatusOK, body: `{"id":"done"}`}
	upstream.server = httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			upstream.calls++
			upstream.paths = append(upstream.paths, request.URL.Path)
			upstream.received, _ = io.ReadAll(request.Body)
			if upstream.location != "" {
				writer.Header().Set("Location", upstream.location)
			}
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
	planeClient := newHTTPClient(5 * time.Second)
	planeClient.Transport = plane.server.Client().Transport
	client := &admissionClient{
		base: base, http: planeClient,
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
	// Only the transport is replaced. Assigning a whole http.Client here
	// discarded the production redirect policy, which is how a test asserting
	// that a redirect cannot carry the prompt passed against a client that
	// followed one.
	governed.client.Transport = upstream.server.Client().Transport
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
// TestAWithholdObligationCannotBeSatisfiedOnThisRequestShape replaces a test
// that pinned the wrong behaviour.
//
// It used to assert that a withheld reference was removed from the forwarded
// body and the others kept — the metadata edit — and it passed. What it never
// asserted was that anything was withheld from the provider, which is what a
// withhold obligation means. Nothing was: shoal_references is a flat list of
// IDs, the material lives in messages[].content as free text, and nothing
// connects the two, so the proxy removed the label and forwarded the content.
// Providers ignore unknown fields, so even the label's removal changed nothing
// about what the model received.
//
// The case below is the one the old test could not express, and is why greping
// the forwarded body for the reference ID was never enough: the ID and the
// material are different strings. A caller declares a restricted document and
// pastes its text in. Under the old behaviour the text was forwarded with the
// label stripped, and every assertion passed.
func TestAWithholdObligationCannotBeSatisfiedOnThisRequestShape(t *testing.T) {
	const material = "the restricted paragraph that doc-b actually contains"
	plane := newFakePlane(t, outcomeObligated, []string{"doc-b"})
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)

	recorder := post(t, governed,
		`{"model":"gpt","messages":[{"role":"user","content":"`+material+`"}],`+
			`"shoal_references":["doc-a","doc-b"]}`)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403: %s", recorder.Code, recorder.Body.String())
	}
	if upstream.calls != 0 {
		t.Fatalf("the call was forwarded under an obligation nothing enforced: %s",
			upstream.received)
	}
	if strings.Contains(string(upstream.received), material) {
		t.Fatal("withheld material reached the provider")
	}
	// Reported as unsatisfiable, so the plane learns its obligation could not
	// be met rather than that the caller went dark.
	if len(plane.reports) != 1 || !plane.reports[0].Failed ||
		plane.reports[0].ErrorCode != "obligation_unsatisfiable" {
		t.Fatalf("reports = %+v, want one naming the unsatisfiable obligation",
			plane.reports)
	}

	// An allow with no obligations still forwards, or this has turned the
	// proxy into something that refuses everything the plane constrains and
	// also everything it does not.
	plane.withhold, plane.outcome = nil, outcomeAllowed
	plane.reports = nil
	if response := post(t, governed, plainCall); response.Code != http.StatusOK {
		t.Fatalf("an unobligated call was refused: %d", response.Code)
	}
	if upstream.calls != 1 {
		t.Fatalf("upstream calls = %d, want 1", upstream.calls)
	}
}

// TestAnUnsatisfiableObligationRefusesAndReports is the one case where refusing
// the whole call is right. The plane believes it constrained this call, so
// ignoring an obligation it cannot satisfy would be worse than refusing.
func TestAnUnsatisfiableObligationRefusesAndReports(t *testing.T) {
	// The obligation names a reference the caller never declared. This is a
	// different diagnosis from the case above — the plane and the caller
	// disagree about what this call is, rather than the proxy being unable to
	// locate content — and both refuse.
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

// deadlineRecorder records how long each outbound admission call was given.
//
// The bound cannot be observed from the plane's side: HTTP carries no client
// deadline, so a handler sees only its own connection's context. It has to be
// read from the outbound request before it leaves.
type deadlineRecorder struct {
	inner    http.RoundTripper
	headroom map[string]time.Duration
}

func (d *deadlineRecorder) RoundTrip(request *http.Request) (*http.Response, error) {
	key := "request"
	if strings.HasSuffix(request.URL.Path, "/report") {
		key = "report"
	}
	if deadline, ok := request.Context().Deadline(); ok {
		d.headroom[key] = time.Until(deadline)
	} else {
		d.headroom[key] = -1
	}
	return d.inner.RoundTrip(request)
}

// TestTheReportFitsInTheWindowReservedForIt closes the gap between the two
// numbers that have to agree.
//
// validateDurations withholds minimumReportWindow from the lease so the report
// can still be made after the upstream call returns. The report then has to fit
// in that margin — and it did not: it was given its own ten-second timeout
// against a window reserved as five, so a report that used the time it was
// granted outlived the lease it was closing. The failure needs an upstream call
// that runs to the edge of the timeout to appear in production, which is why no
// existing test saw it.
//
// Asserting an upper bound rather than an exact value is deliberate: a report
// bounded more tightly than the reservation is still correct, one bounded
// looser is the bug. The lower bound is here too, because an unbounded report
// hangs the goroutine that should be closing the grant.
func TestTheReportFitsInTheWindowReservedForIt(t *testing.T) {
	plane := newFakePlane(t, outcomeAllowed, nil)
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)
	recorder := &deadlineRecorder{
		inner:    plane.server.Client().Transport,
		headroom: map[string]time.Duration{},
	}
	governed.admission.http = &http.Client{Transport: recorder}

	if response := post(t, governed, plainCall); response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if len(plane.reports) != 1 {
		t.Fatalf("reports = %d, want 1", len(plane.reports))
	}

	headroom, seen := recorder.headroom["report"]
	if !seen {
		t.Fatal("the report never went out, so its bound was never exercised")
	}
	if headroom <= 0 {
		t.Fatal("the report carried no deadline; an unbounded report can " +
			"outlive the lease it closes and never return")
	}
	if headroom > minimumReportWindow {
		t.Fatalf("the report was given %s inside a window reserved as %s, so a "+
			"report that takes the time it is granted expires the lease",
			headroom, minimumReportWindow)
	}
}

// TestTheUpstreamPathIsTheDocumentedOne pins the endpoint the proxy builds.
//
// An OpenAI-compatible base URL conventionally carries the version segment —
// OPENAI_BASE_URL is https://api.openai.com/v1, and the chart's own case uses
// http://localhost:11434/v1. The proxy appended "v1/chat/completions" to that,
// producing /v1/v1/chat/completions: a 404 from every real provider, against
// the configuration the chart and the deployment guide both document.
//
// No test could see it because the fake upstream answered every path the same
// way. That is the recurring shape of the defects found on this PR — a fixture
// unable to express the condition, rather than a missing assertion — so the
// fake records the path now and this asserts it exactly.
func TestTheUpstreamPathIsTheDocumentedOne(t *testing.T) {
	plane := newFakePlane(t, outcomeAllowed, nil)
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)
	// A base with the version segment, as every OpenAI-compatible client and
	// the chart's own rendering case supply it.
	base, err := url.Parse(upstream.server.URL + "/v1")
	if err != nil {
		t.Fatal(err)
	}
	governed.upstream = base

	if response := post(t, governed, plainCall); response.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", response.Code, response.Body.String())
	}
	if len(upstream.paths) != 1 || upstream.paths[0] != "/v1/chat/completions" {
		t.Fatalf("upstream path = %v, want [/v1/chat/completions]", upstream.paths)
	}
}

// TestARedirectCannotCarryThePromptOffTheCheckedTransport covers a hole in a
// rule that is otherwise enforced carefully.
//
// absoluteURL settles the scheme of the URL that is configured. A followed
// redirect is a different URL it never saw, and a 307 or 308 preserves the
// method and the body — so one hop to http:// replays the prompt, and on the
// upstream path the operator's credential with it, past a check that passed.
// Go keeps the Authorization header across a same-host redirect, which is the
// case that matters most.
//
// Both clients are covered, because the admission client had the same default
// and the declaration is still worth not leaking.
func TestARedirectCannotCarryThePromptOffTheCheckedTransport(t *testing.T) {
	// Where a followed redirect would land. Nothing may reach it.
	var landed int
	elsewhere := httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, _ *http.Request) {
			landed++
			writer.WriteHeader(http.StatusOK)
		}))
	t.Cleanup(elsewhere.Close)

	t.Run("upstream", func(t *testing.T) {
		plane := newFakePlane(t, outcomeAllowed, nil)
		upstream := newFakeUpstream(t)
		upstream.status, upstream.location, upstream.body =
			http.StatusTemporaryRedirect, elsewhere.URL+"/v1/chat/completions", ""
		governed, _ := newTestProxy(t, plane, upstream)

		response := post(t, governed, plainCall)
		if landed != 0 {
			t.Fatal("the prompt was replayed to the redirect target")
		}
		// The 3xx is handed back as the provider's own answer, so the caller
		// sees what happened rather than a synthesised error.
		if response.Code != http.StatusTemporaryRedirect {
			t.Fatalf("status = %d, want the upstream's 307", response.Code)
		}
		// And it is still reported: an unreported grant is the outcome this
		// proxy exists to prevent, redirect or not.
		if len(plane.reports) != 1 {
			t.Fatalf("reports = %d, want 1", len(plane.reports))
		}
	})

	t.Run("admission", func(t *testing.T) {
		landed = 0
		plane := newFakePlane(t, outcomeAllowed, nil)
		upstream := newFakeUpstream(t)
		governed, _ := newTestProxy(t, plane, upstream)
		redirecting := httptest.NewServer(http.HandlerFunc(
			func(writer http.ResponseWriter, request *http.Request) {
				writer.Header().Set("Location", elsewhere.URL+request.URL.Path)
				writer.WriteHeader(http.StatusTemporaryRedirect)
			}))
		t.Cleanup(redirecting.Close)
		base, err := url.Parse(redirecting.URL)
		if err != nil {
			t.Fatal(err)
		}
		governed.admission.base = base

		response := post(t, governed, plainCall)
		if landed != 0 {
			t.Fatal("the declaration and bearer token were replayed to the redirect target")
		}
		// No decision means no call. The redirect is not an allow.
		if upstream.calls != 0 {
			t.Fatal("the call was forwarded without a decision")
		}
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", response.Code)
		}
	})
}

// TestALoopbackProviderNeedsNoCredential covers a configuration the chart
// asserts renders and the binary refused on every call.
//
// validate-chart.sh has a case named "loopback upstream needs no credential"
// and the deployment guide says a loopback provider needs none — a local model
// server generally has no notion of one. The forward path demanded a credential
// unconditionally, so that documented configuration spent an admission and then
// refused the call it had just been granted.
func TestALoopbackProviderNeedsNoCredential(t *testing.T) {
	absent := func() (string, error) { return "", errors.New("not configured") }

	t.Run("loopback forwards unauthenticated", func(t *testing.T) {
		plane := newFakePlane(t, outcomeAllowed, nil)
		upstream := newFakeUpstream(t)
		governed, _ := newTestProxy(t, plane, upstream)
		governed.credential = absent

		if response := post(t, governed, plainCall); response.Code != http.StatusOK {
			t.Fatalf("status = %d: %s", response.Code, response.Body.String())
		}
		if upstream.calls != 1 {
			t.Fatalf("upstream calls = %d, want 1", upstream.calls)
		}
	})

	// Optional is not ignored. A remote provider with no credential fails here
	// rather than being sent an unauthenticated prompt, which would reach a
	// third party and be rejected — after the egress.
	t.Run("a remote provider still requires one", func(t *testing.T) {
		plane := newFakePlane(t, outcomeAllowed, nil)
		upstream := newFakeUpstream(t)
		governed, _ := newTestProxy(t, plane, upstream)
		governed.credential = absent
		remote, err := url.Parse("https://api.example.test/v1")
		if err != nil {
			t.Fatal(err)
		}
		governed.upstream = remote

		if response := post(t, governed, plainCall); response.Code != http.StatusBadGateway {
			t.Fatalf("status = %d, want 502", response.Code)
		}
		if upstream.calls != 0 {
			t.Fatal("a prompt was sent to a remote provider with no credential")
		}
		if len(plane.reports) != 1 || plane.reports[0].ErrorCode != "upstream_credential_unavailable" {
			t.Fatalf("reports = %+v, want one naming the missing credential", plane.reports)
		}
	})
}
