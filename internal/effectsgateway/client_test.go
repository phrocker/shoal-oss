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

package effectsgateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// The dispatch client's wire behaviour is tested against the real explorer
// handler in cmd/shoal-explore-web (effects_gateway_client_test.go). These are
// the pure parts: the status mapping, which the real handler cannot be made to
// produce on demand for every row, and the local refusals.

func TestStatusErrorMapping(t *testing.T) {
	for _, row := range []struct {
		status int
		header http.Header
		body   string
		kind   DispatchErrorKind
		code   string
	}{
		{404, nil, `{"code":"not_found","message":"fleet action not found"}`, DispatchNotFound, "not_found"},
		{409, nil, `{"code":"conflict"}`, DispatchConflict, "conflict"},
		{503, http.Header{"Shoal-Commit-Outcome": {"indeterminate"}}, `{"code":"unavailable","indeterminate":true}`, DispatchIndeterminate, "unavailable"},
		{503, http.Header{"Shoal-Commit-Outcome": {" Indeterminate "}}, ``, DispatchIndeterminate, ""},
		{503, nil, `{"code":"unavailable"}`, DispatchUnavailable, "unavailable"},
		{503, http.Header{"Shoal-Commit-Outcome": {"committed"}}, ``, DispatchUnavailable, ""},
		{400, nil, `{"code":"invalid_argument"}`, DispatchInvalid, "invalid_argument"},
		{401, nil, ``, DispatchUnauthorized, ""},
		{403, nil, ``, DispatchUnauthorized, ""},
		{504, nil, ``, DispatchDeadline, ""},
		{500, nil, `{"code":"internal"}`, DispatchStatus, "internal"},
		{502, nil, `<html>bad gateway</html>`, DispatchStatus, ""},
		// A code that is not a short snake-case token is dropped, so a proxy
		// cannot put text into the error through it.
		{409, nil, `{"code":"Conflict: see https://evil.example"}`, DispatchConflict, ""},
	} {
		header := row.header
		if header == nil {
			header = http.Header{}
		}
		err := statusError("claim", &http.Response{StatusCode: row.status, Header: header}, []byte(row.body))
		var dispatchErr *DispatchError
		if !errors.As(err, &dispatchErr) || dispatchErr.Kind != row.kind ||
			dispatchErr.Code != row.code || dispatchErr.Status != row.status {
			t.Errorf("%d %v %s: %#v", row.status, row.header, row.body, err)
		}
		if DispatchKind(err) != row.kind {
			t.Errorf("%d: DispatchKind = %q", row.status, DispatchKind(err))
		}
	}
	if DispatchKind(errors.New("other")) != "" {
		t.Error("a foreign error has a dispatch kind")
	}
}

func TestDispatchErrorNeverFormatsItsCause(t *testing.T) {
	const secret = "/api/v1/fleet/actions/c2VjcmV0/claim?token=s3cret"
	cause := &url.Error{Op: "Post", URL: "https://explorer" + secret, Err: context.DeadlineExceeded}
	err := &DispatchError{Op: "claim", Kind: DispatchTransport, Failure: ClassifyFailure(cause), cause: cause}
	if strings.Contains(err.Error(), "s3cret") || strings.Contains(err.Error(), "c2VjcmV0") {
		t.Fatalf("error text carries the cause: %s", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("the cause is not reachable through errors.Is")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("error text lacks the classified kind: %s", err)
	}
}

func TestNewClaimID(t *testing.T) {
	random := bytes.NewReader(bytes.Repeat([]byte{0xaa}, 64))
	id, nonce, err := NewClaimID("gateway-7f9c-0", random)
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte("gw1|gateway-7f9c-0|"), bytes.Repeat([]byte{0xaa}, ClaimNonceBytes)...)
	if !bytes.Equal(id, want) || nonce != (ClaimNonce{0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa}) {
		t.Fatalf("claim ID = %q, nonce = %x", id, nonce)
	}
	// Fresh per attempt from the real source.
	a, _, err := NewClaimID("pod", nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _, _ := NewClaimID("pod", nil)
	if bytes.Equal(a, b) {
		t.Fatal("two claim attempts produced one claim ID")
	}
	longest := strings.Repeat("p", fleet.MaxActionIDBytes-len(claimIDPrefix)-1-ClaimNonceBytes)
	if id, _, err := NewClaimID(longest, nil); err != nil || len(id) != fleet.MaxActionIDBytes {
		t.Fatalf("longest pod name: %d bytes, %v", len(id), err)
	}
	for _, pod := range []string{"", longest + "p", "a|b", "a b", "pod\n", "pöd"} {
		if _, _, err := NewClaimID(pod, nil); err == nil {
			t.Errorf("pod %q accepted", pod)
		}
	}
	if _, _, err := NewClaimID("pod", bytes.NewReader(nil)); err == nil {
		t.Fatal("an exhausted random source produced a claim ID")
	}
}

func TestRequestContextWire(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.FixedZone("EST", -5*3600))
	good := RequestContext{RequestID: []byte{0, 1}, ReasonCode: "gateway_claim", Deadline: now.Add(time.Minute)}
	wire, err := good.wire("claim", now)
	if err != nil {
		t.Fatal(err)
	}
	if wire.Deadline.Location() != time.UTC || wire.RequestID != "AAE" || wire.CorrelationID != "" {
		t.Fatalf("wire = %#v", wire)
	}
	for name, mutate := range map[string]func(*RequestContext){
		"no request ID":        func(r *RequestContext) { r.RequestID = nil },
		"long request ID":      func(r *RequestContext) { r.RequestID = make([]byte, fleet.MaxActionIDBytes+1) },
		"long correlation ID":  func(r *RequestContext) { r.CorrelationID = make([]byte, fleet.MaxActionIDBytes+1) },
		"no reason":            func(r *RequestContext) { r.ReasonCode = "" },
		"untrimmed reason":     func(r *RequestContext) { r.ReasonCode = "claim " },
		"long reason":          func(r *RequestContext) { r.ReasonCode = strings.Repeat("r", fleet.MaxReasonCodeBytes+1) },
		"deadline now":         func(r *RequestContext) { r.Deadline = now },
		"deadline in the past": func(r *RequestContext) { r.Deadline = now.Add(-time.Second) },
		"no deadline":          func(r *RequestContext) { r.Deadline = time.Time{} },
	} {
		request := good
		mutate(&request)
		if _, err := request.wire("claim", now); DispatchKind(err) != DispatchRefusedLocal {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestDispatchClientRefusesLocallyBeforeSending(t *testing.T) {
	base, _ := url.Parse("https://explorer.invalid")
	transport := &countingTransport{}
	client, err := NewDispatchClient(base, &http.Client{Transport: transport},
		func() (string, error) { return "token", nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	request := RequestContext{RequestID: []byte("r"), ReasonCode: "x", Deadline: time.Now().Add(time.Minute)}
	claimID := []byte("claim")
	checks := map[string]error{}
	_, checks["pull limit 0"] = client.Pull(ctx, request, "", 0)
	_, checks["pull limit 257"] = client.Pull(ctx, request, "", fleet.MaxDispatchListResults+1)
	_, checks["pull bad cursor"] = client.Pull(ctx, request, "!!", 1)
	_, checks["claim no ID"] = client.Claim(ctx, nil, ClaimRequest{Context: request, ExpectedVersion: 1, ClaimID: claimID, Lease: time.Minute})
	_, checks["claim no version"] = client.Claim(ctx, []byte("a"), ClaimRequest{Context: request, ClaimID: claimID, Lease: time.Minute})
	_, checks["claim no claim ID"] = client.Claim(ctx, []byte("a"), ClaimRequest{Context: request, ExpectedVersion: 1, Lease: time.Minute})
	_, checks["claim zero lease"] = client.Claim(ctx, []byte("a"), ClaimRequest{Context: request, ExpectedVersion: 1, ClaimID: claimID})
	_, checks["claim lease over ceiling"] = client.Claim(ctx, []byte("a"), ClaimRequest{Context: request, ExpectedVersion: 1, ClaimID: claimID, Lease: fleet.MaxActionClaimTTL + 1})
	_, checks["complete both output and failure"] = client.Complete(ctx, []byte("a"), Completion{Context: request, ExpectedVersion: 1, ClaimID: claimID, Output: []byte(`{}`), Failed: true, ErrorCode: ErrorOutcomeUnknown})
	_, checks["complete success with code"] = client.Complete(ctx, []byte("a"), Completion{Context: request, ExpectedVersion: 1, ClaimID: claimID, Output: []byte(`{}`), ErrorCode: ErrorOutcomeUnknown})
	_, checks["complete success without output"] = client.Complete(ctx, []byte("a"), Completion{Context: request, ExpectedVersion: 1, ClaimID: claimID})
	_, checks["complete failure off vocabulary"] = client.Complete(ctx, []byte("a"), Completion{Context: request, ExpectedVersion: 1, ClaimID: claimID, Failed: true, ErrorCode: "executor_error"})
	_, checks["complete failure without code"] = client.Complete(ctx, []byte("a"), Completion{Context: request, ExpectedVersion: 1, ClaimID: claimID, Failed: true})
	_, checks["resolve no agent"] = client.Resolve(ctx, nil, request)
	for name, err := range checks {
		if DispatchKind(err) != DispatchRefusedLocal {
			t.Errorf("%s: %v", name, err)
		}
	}
	if transport.calls != 0 {
		t.Fatalf("%d requests left for locally refused calls", transport.calls)
	}

	noToken, _ := NewDispatchClient(base, &http.Client{Transport: transport},
		func() (string, error) { return "", ErrNoCredential }, nil)
	if _, err := noToken.Pull(ctx, request, "", 1); DispatchKind(err) != DispatchNoCredential {
		t.Fatalf("no credential = %v", err)
	}
	injected, _ := NewDispatchClient(base, &http.Client{Transport: transport},
		func() (string, error) { return "t\r\nX-Evil: 1", nil }, nil)
	if _, err := injected.Pull(ctx, request, "", 1); DispatchKind(err) != DispatchNoCredential {
		t.Fatalf("credential with a line break = %v", err)
	}
	if transport.calls != 0 {
		t.Fatal("a request left without a usable credential")
	}
}

type countingTransport struct{ calls int }

func (c *countingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	c.calls++
	return nil, errors.New("no network in this test")
}

// scriptedTransport answers each request with the next scripted reply. The
// completion recovery is about what the client does with a sequence of
// answers, which the real handler cannot be made to produce on demand; the
// wire shapes themselves are pinned against the real handler.
type scriptedTransport struct {
	replies []func() (*http.Response, error)
	bodies  [][]byte
}

func (s *scriptedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(request.Body)
	s.bodies = append(s.bodies, body)
	if len(s.replies) == 0 {
		return nil, errors.New("script exhausted")
	}
	next := s.replies[0]
	s.replies = s.replies[1:]
	return next()
}

func reply(status int, body string, header ...string) func() (*http.Response, error) {
	return func() (*http.Response, error) {
		h := http.Header{"Content-Type": {"application/json"}}
		for i := 0; i+1 < len(header); i += 2 {
			h.Set(header[i], header[i+1])
		}
		return &http.Response{StatusCode: status, Header: h,
			Body: io.NopCloser(strings.NewReader(body))}, nil
	}
}

func lost() (*http.Response, error) { return nil, io.ErrUnexpectedEOF }

var indeterminate = []string{"Shoal-Commit-Outcome", "indeterminate"}

func committed(t *testing.T, version uint64, state fleet.DispatchState, code, output string) string {
	t.Helper()
	record := map[string]any{
		"id": base64.RawURLEncoding.EncodeToString([]byte("action")), "version": version,
		"state": state, "agent_id": "", "claim_id": base64.RawURLEncoding.EncodeToString([]byte("claim")),
		"effect_possible": true,
	}
	if code != "" {
		record["error_code"] = code
	}
	if output != "" {
		record["output"] = json.RawMessage(output)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func TestCompleteRecoversOnlyWhatTheResendConfirms(t *testing.T) {
	failure := Completion{ExpectedVersion: 2, ClaimID: []byte("claim"), Failed: true,
		ErrorCode: TargetRejected(422)}
	success := Completion{ExpectedVersion: 2, ClaimID: []byte("claim"),
		Output: json.RawMessage(`{"status":200,"idempotency":"key","reference":"ch_1"}`)}
	failedRecord := committed(t, 3, fleet.DispatchFailed, "target_rejected_422", "")
	successRecord := committed(t, 3, fleet.DispatchSucceeded, "", `{"idempotency":"key","reference":"ch_1","status":200}`)
	for _, row := range []struct {
		name       string
		completion Completion
		replies    []func() (*http.Response, error)
		kind       DispatchErrorKind
		calls      int
	}{
		{"clean success", success, []func() (*http.Response, error){reply(200, successRecord)}, "", 1},
		{"stored output re-encoded is the same output", success,
			[]func() (*http.Response, error){reply(200, successRecord)}, "", 1},
		// The defect: the resend's lost answer was replaced by the first
		// attempt's 500, a definite-looking status for a report that may
		// have committed.
		{"500 then indeterminate 503", failure,
			[]func() (*http.Response, error){reply(500, `{"code":"internal"}`), reply(503, `{}`, indeterminate...)},
			DispatchIndeterminate, 2},
		{"500 then transport loss", failure,
			[]func() (*http.Response, error){reply(500, `{}`), lost}, DispatchIndeterminate, 2},
		{"400 then indeterminate 503", success,
			[]func() (*http.Response, error){reply(400, `{}`), reply(503, `{}`, indeterminate...)},
			DispatchIndeterminate, 2},
		{"#492 workaround: 500 then the recorded failure", failure,
			[]func() (*http.Response, error){reply(500, `{}`), reply(200, failedRecord)}, "", 2},
		{"lost response then the committed record", success,
			[]func() (*http.Response, error){lost, reply(200, successRecord)}, "", 2},
		{"indeterminate then the committed record", failure,
			[]func() (*http.Response, error){reply(503, `{}`, indeterminate...), reply(200, failedRecord)}, "", 2},
		{"lost twice", success, []func() (*http.Response, error){lost, lost}, DispatchIndeterminate, 2},
		{"indeterminate twice", success,
			[]func() (*http.Response, error){reply(503, `{}`, indeterminate...), reply(503, `{}`, indeterminate...)},
			DispatchIndeterminate, 2},
		// A #492 400/500 after a lost first attempt is not definite: the
		// record is read once more through the replay branch.
		{"lost then 500 then the recorded failure", failure,
			[]func() (*http.Response, error){lost, reply(500, `{}`), reply(200, failedRecord)}, "", 3},
		{"lost then 400 then lost", success,
			[]func() (*http.Response, error){lost, reply(400, `{}`), lost}, DispatchIndeterminate, 3},
		{"lost then 500 then 500", failure,
			[]func() (*http.Response, error){lost, reply(500, `{}`), reply(500, `{}`)}, DispatchIndeterminate, 3},
		{"indeterminate then 400 then the record", success,
			[]func() (*http.Response, error){reply(503, `{}`, indeterminate...), reply(400, `{}`),
				reply(200, committed(t, 3, fleet.DispatchFailed, "invalid_executor_output", ""))},
			DispatchRecordedOtherwise, 3},
		// The resend's own definite answer describes the record now.
		{"lost then conflict", success,
			[]func() (*http.Response, error){lost, reply(409, `{"code":"conflict"}`)}, DispatchConflict, 2},
		{"500 then 500", failure,
			[]func() (*http.Response, error){reply(500, `{}`), reply(500, `{}`)}, DispatchStatus, 2},
		// Not a lost response: nothing is resent.
		{"plain 503", success, []func() (*http.Response, error){reply(503, `{}`)}, DispatchUnavailable, 1},
		{"404", success, []func() (*http.Response, error){reply(404, `{}`)}, DispatchNotFound, 1},
		// Recorded otherwise: every reported field is compared.
		{"failure recorded with another code", failure,
			[]func() (*http.Response, error){reply(200, committed(t, 3, fleet.DispatchFailed, "outcome_unknown", ""))},
			DispatchRecordedOtherwise, 1},
		{"success recorded with other output", success,
			[]func() (*http.Response, error){reply(200, committed(t, 3, fleet.DispatchSucceeded, "", `{"status":200,"idempotency":"key"}`))},
			DispatchRecordedOtherwise, 1},
		{"success recorded as failed", success,
			[]func() (*http.Response, error){reply(400, `{}`), reply(200, committed(t, 3, fleet.DispatchFailed, "invalid_executor_output", ""))},
			DispatchRecordedOtherwise, 2},
		// 200 comes only at exactly ExpectedVersion+1; anything else is not
		// this report's record.
		{"version past the report", failure,
			[]func() (*http.Response, error){reply(200, committed(t, 5, fleet.DispatchFailed, "target_rejected_422", ""))},
			DispatchProtocol, 1},
		{"version not past the report", failure,
			[]func() (*http.Response, error){reply(200, committed(t, 2, fleet.DispatchFailed, "target_rejected_422", ""))},
			DispatchProtocol, 1},
	} {
		transport := &scriptedTransport{replies: row.replies}
		base, _ := url.Parse("https://explorer.invalid")
		client, err := NewDispatchClient(base, &http.Client{Transport: transport},
			func() (string, error) { return "token", nil }, nil)
		if err != nil {
			t.Fatal(err)
		}
		completion := row.completion
		completion.Context = RequestContext{RequestID: []byte("r"), ReasonCode: "gateway_complete",
			Deadline: time.Now().Add(time.Minute)}
		action, err := client.Complete(context.Background(), []byte("action"), completion)
		if DispatchKind(err) != row.kind {
			t.Errorf("%s: %v (kind %q), want kind %q", row.name, err, DispatchKind(err), row.kind)
		}
		if len(transport.bodies) != row.calls {
			t.Errorf("%s: %d requests, want %d", row.name, len(transport.bodies), row.calls)
		}
		for i := 1; i < len(transport.bodies); i++ {
			if !bytes.Equal(transport.bodies[i], transport.bodies[0]) {
				t.Errorf("%s: the resend was not the identical body", row.name)
			}
		}
		if row.kind == DispatchRecordedOtherwise && action.State == "" {
			t.Errorf("%s: recorded_otherwise without the committed record", row.name)
		}
	}
}
