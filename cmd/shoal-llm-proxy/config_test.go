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
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// TestTheRoleFieldCannotCarryThePrompt closes the channel the declaration
// guarantee was open to.
//
// A role is a free-form string on the wire, and the declaration copied it
// verbatim. TestShoalNeverReceivesThePrompt could not see it, because that test
// puts its secret in content — the field anyone would think to check. A
// guarantee about shape has to hold for every string that travels, not the
// obvious one.
func TestTheRoleFieldCannotCarryThePrompt(t *testing.T) {
	plane := newFakePlane(t, outcomeAllowed, nil)
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)

	const secret = "a-prompt-smuggled-through-the-role-field"
	post(t, governed,
		`{"model":"gpt","messages":[{"role":"`+secret+`","content":"hi"}]}`)

	if len(plane.requests) != 1 {
		t.Fatalf("admissions = %d", len(plane.requests))
	}
	everything, err := json.Marshal(plane.requests[0])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(everything), secret) {
		t.Fatalf("the role reached the decision plane: %s", everything)
	}
	// The declaration still has to say something useful about the roles, or
	// sanitising them has thrown away the signal along with the content.
	if !strings.Contains(string(plane.requests[0].Input), roleOther) {
		t.Fatalf("an unknown role left no trace at all: %s", plane.requests[0].Input)
	}
	// A known role is reported as itself. Collapsing everything to a marker
	// would be safe and useless.
	plane.requests = nil
	post(t, governed, plainCall)
	if !strings.Contains(string(plane.requests[0].Input), "user") {
		t.Fatalf("a known role was not reported: %s", plane.requests[0].Input)
	}
}

// TestTheRequestKeepsTheCallersOwnRole is the other half: only the declaration
// is sanitised. Rewriting the caller's role would make the proxy the reason an
// unmodified client gets a different answer.
func TestTheRequestKeepsTheCallersOwnRole(t *testing.T) {
	plane := newFakePlane(t, outcomeAllowed, nil)
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)

	post(t, governed,
		`{"model":"gpt","messages":[{"role":"custom-agent","content":"hi"}]}`)
	if !strings.Contains(string(upstream.received), "custom-agent") {
		t.Fatalf("the caller's role was rewritten: %s", upstream.received)
	}
}

// TestRemotePlaintextEndpointsAreRefused covers both URLs with one rule. The
// upstream request carries the operator's credential and the caller's prompt,
// so plain HTTP there is worse than on the admission path where only the
// declaration travels.
func TestRemotePlaintextEndpointsAreRefused(t *testing.T) {
	for _, probe := range []struct {
		name    string
		url     string
		refused bool
	}{
		{"remote https", "https://api.example.test", false},
		{"loopback http", "http://127.0.0.1:11434", false},
		{"localhost http", "http://localhost:11434", false},
		{"remote plaintext", "http://api.example.test", true},
		// The loopback exemption is for http specifically. Admitting any
		// scheme on loopback only moves the failure from startup to every
		// request, where it is far more expensive to diagnose.
		{"non-http loopback", "ftp://localhost/models", true},
		{"relative", "/v1", true},
		{"hostless", "https://", true},
	} {
		_, err := absoluteURL(probe.url)
		if probe.refused && err == nil {
			t.Fatalf("%s (%s) was accepted", probe.name, probe.url)
		}
		if !probe.refused && err != nil {
			t.Fatalf("%s (%s) was refused: %v", probe.name, probe.url, err)
		}
	}
}

// TestTheProxyRefusesAPlaintextUpstreamAtConstruction is the call-site half.
//
// Testing absoluteURL alone proves the rule exists, not that the upstream URL
// goes through it — swapping newProxy back to a bare url.Parse passed every
// absoluteURL assertion. The rule and its application need separate tests.
func TestTheProxyRefusesAPlaintextUpstreamAtConstruction(t *testing.T) {
	client := &admissionClient{}
	for _, probe := range []struct {
		name    string
		url     string
		refused bool
	}{
		{"remote plaintext", "http://api.example.test", true},
		{"non-http loopback", "ftp://localhost/models", true},
		{"remote https", "https://api.example.test", false},
		{"loopback http", "http://127.0.0.1:11434", false},
	} {
		_, err := newProxy(
			client, probe.url, func() (string, error) { return "k", nil },
			[]string{"example.test"}, time.Minute, time.Now,
			func(string, ...any) {})
		if probe.refused && err == nil {
			t.Fatalf("%s upstream (%s) was accepted", probe.name, probe.url)
		}
		if !probe.refused && err != nil {
			t.Fatalf("%s upstream (%s) was refused: %v", probe.name, probe.url, err)
		}
	}
}

// TestAnUnreportableTokenIsRefusedBeforeTheEgress is the fail-closed rule the
// nil check only half enforced.
//
// Checking for nil let every other unreportable shape through, and in each case
// the call would have been forwarded and the report then rejected — the exact
// unreportable grant the refusal exists to prevent, discovered after the
// irreversible part.
func TestAnUnreportableTokenIsRefusedBeforeTheEgress(t *testing.T) {
	now := time.Now()
	for _, probe := range []struct {
		name  string
		token *admissionToken
	}{
		{"absent", nil},
		{"empty", &admissionToken{}},
		{"unparseable action ID", &admissionToken{
			ActionID: "not base64!", TokenID: "dG9rZW4", Version: 1,
			ExpiresAt: now.Add(time.Minute)}},
		{"unparseable token ID", &admissionToken{
			ActionID: "YWN0aW9u", TokenID: "not base64!", Version: 1,
			ExpiresAt: now.Add(time.Minute)}},
		{"zero version", &admissionToken{
			ActionID: "YWN0aW9u", TokenID: "dG9rZW4", Version: 0,
			ExpiresAt: now.Add(time.Minute)}},
		{"already expired", &admissionToken{
			ActionID: "YWN0aW9u", TokenID: "dG9rZW4", Version: 1,
			ExpiresAt: now.Add(-time.Second)}},
		{"expiring inside the report window", &admissionToken{
			ActionID: "YWN0aW9u", TokenID: "dG9rZW4", Version: 1,
			ExpiresAt: now.Add(minimumReportWindow / 2)}},
	} {
		plane := newFakePlane(t, outcomeAllowed, nil)
		plane.token = probe.token
		upstream := newFakeUpstream(t)
		governed, _ := newTestProxy(t, plane, upstream)

		recorder := post(t, governed, plainCall)
		if recorder.Code != 503 {
			t.Fatalf("%s token: status = %d, want 503", probe.name, recorder.Code)
		}
		if upstream.calls != 0 {
			t.Fatalf("%s token: the egress happened anyway", probe.name)
		}
	}

	// A usable token is still accepted, or the check above is just a refusal.
	plane := newFakePlane(t, outcomeAllowed, nil)
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)
	if recorder := post(t, governed, plainCall); recorder.Code != 200 {
		t.Fatalf("a usable token was refused: %d", recorder.Code)
	}
}

// TestTheLeaseMustOutlastTheCallItAdmits pins a configuration invariant rather
// than a code path.
//
// The shipped defaults violated it: a one-minute lease against a two-minute
// timeout meant every call over a minute was forwarded and then had its report
// rejected as expired. That is an unreportable grant guaranteed by
// configuration, which no amount of runtime checking recovers.
//
// The upper bound is separate and just as sharp: the fleet refuses a lease
// above its claim ceiling rather than shortening it, so too large a value does
// not degrade — it denies every call.
func TestTheLeaseMustOutlastTheCallItAdmits(t *testing.T) {
	for _, probe := range []struct {
		name    string
		lease   time.Duration
		timeout time.Duration
		refused string
	}{
		{"lease shorter than the timeout", time.Minute, 2 * time.Minute, "-lease must exceed"},
		{"lease equal to the timeout", 90 * time.Second, 90 * time.Second, "-lease must exceed"},
		{"lease inside the report window", 92 * time.Second, 90 * time.Second, "-lease must exceed"},
		{"lease above the fleet ceiling", 10 * time.Minute, time.Minute, "-lease must not exceed"},
		{"zero lease", 0, time.Minute, "must be positive"},
		{"zero timeout", time.Minute, 0, "must be positive"},
	} {
		err := validateDurations(probe.lease, probe.timeout)
		if err == nil {
			t.Fatalf("%s was accepted", probe.name)
		}
		if !strings.Contains(err.Error(), probe.refused) {
			t.Fatalf("%s refused for the wrong reason: %v", probe.name, err)
		}
	}

	// A lease that leaves room is accepted, and the fleet ceiling itself is
	// allowed — a rule nothing valid passes is a rule against the feature.
	for _, probe := range []struct {
		name    string
		lease   time.Duration
		timeout time.Duration
	}{
		{"room to report", 4 * time.Minute, 90 * time.Second},
		{"exactly the ceiling", fleet.MaxActionClaimTTL, time.Minute},
	} {
		if err := validateDurations(probe.lease, probe.timeout); err != nil {
			t.Fatalf("%s was refused: %v", probe.name, err)
		}
	}
}

// TestTheShippedDefaultsSatisfyTheirOwnInvariant is the case that matters most,
// because the previous defaults did not. Reading the flag block is not enough:
// the two values are declared forty lines apart.
func TestTheShippedDefaultsSatisfyTheirOwnInvariant(t *testing.T) {
	lease, timeout := defaultLease, defaultRequestTimeout
	if err := validateDurations(lease, timeout); err != nil {
		t.Fatalf("the shipped defaults violate the invariant: %v", err)
	}
}

// proxyArgs is a configuration that run() accepts, so a probe can change one
// field and attribute the refusal to that field.
func proxyArgs(overrides ...string) []string {
	args := []string{
		"-admission-url", "https://workspace.test",
		"-upstream-base-url", "https://api.example.test",
		"-agent-id", "YWdlbnQ",
		"-agent-generation", "1",
		"-capability", "llm.proxy",
		"-action", "complete",
		"-source-id", "c291cmNl",
		"-policy-id", "cG9saWN5",
	}
	return append(args, overrides...)
}

// refusal drives run() and returns why it refused. Every probe here refuses
// during configuration, which is before run() binds a listener — the point of
// the exercise is that these are startup failures and not per-call ones.
func refusal(t *testing.T, args []string) string {
	t.Helper()
	err := run(context.Background(), args, io.Discard)
	if err == nil {
		t.Fatalf("configuration was accepted: %v", args)
	}
	return err.Error()
}

// TestAMisencodedAgentIDIsRefusedAtStartup covers a contract that was stated
// nowhere and could not be guessed.
//
// The workspace decodes agent_id as unpadded base64url. The flag documented no
// encoding and sent any string verbatim, so a descriptor's readable name — the
// obvious thing to pass — was denied once per call as a descriptor that does
// not exist. The operator reads that as a registration problem and goes looking
// in the wrong place.
//
// What this check does and does not reach is worth being exact about, because
// the gap is not fixable here. A shoal.ID is opaque and variable-length, so
// there is no canonical width to compare against, and a readable name that
// happens to decode is indistinguishable from a real ID at startup: of fifteen
// plausible names, eleven decode cleanly to garbage. Those still fail at the
// first call. What moves to startup is the encoding mistakes below — and the
// value of that is not the count, it is that the error names the encoding
// instead of blaming the registry.
func TestAMisencodedAgentIDIsRefusedAtStartup(t *testing.T) {
	for _, probe := range []struct{ name, id, refused string }{
		// Undecodable length: anything with len%4 == 1, which catches a good
		// share of short names ("proxy", "agent", "llm-proxy").
		{"a name of undecodable length", "llm-proxy", "base64url"},
		{"a short name of undecodable length", "proxy", "base64url"},
		// Padding and the standard alphabet are the two ways someone who does
		// know it is base64 still gets it wrong.
		{"padded base64", "YWdlbnQ=", "base64url"},
		{"standard base64 alphabet", "YWdlbnQ/", "base64url"},
		{"empty", "", "-agent-id"},
	} {
		detail := refusal(t, proxyArgs("-agent-id", probe.id))
		if !strings.Contains(detail, probe.refused) {
			t.Fatalf("%s refused for the wrong reason: %s", probe.name, detail)
		}
	}

	// The honest half of the contract, pinned so it is not mistaken for a
	// guarantee later: a name that decodes is accepted here. If this ever
	// starts refusing, something has learned to tell IDs apart locally and
	// this test should be replaced rather than deleted.
	if _, err := decodeID("-agent-id", "gateway"); err != nil {
		t.Fatalf("a decodable name was refused, so the stated gap has closed: %v", err)
	}
}

// TestTheAdmissionTokenCanComeFromAFile covers the form a rotating credential
// actually takes.
//
// A projected ServiceAccount token is a file the kubelet rewrites in place when
// it rotates. It never updates an environment variable, so a proxy that can
// only read env could not use one at all — it would have to be given a static
// secret, which is the thing projected tokens exist to avoid.
func TestTheAdmissionTokenCanComeFromAFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("  first-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	read, err := credentialSource("-admission-token", "", path, "SHOAL_ADMISSION_TOKEN")
	if err != nil {
		t.Fatal(err)
	}
	value, err := read()
	if err != nil {
		t.Fatal(err)
	}
	if value != "first-token" {
		t.Fatalf("token = %q, want the trimmed file contents", value)
	}

	// Read per call, not captured. If the file were read once at startup the
	// proxy would keep presenting the token that has since expired, which is
	// the entire failure mode rotation introduces.
	if err := os.WriteFile(path, []byte("rotated-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	if value, err = read(); err != nil || value != "rotated-token" {
		t.Fatalf("token = %q, %v; want the rotated value", value, err)
	}

	// An unreadable or empty file is an error at use time rather than a silent
	// empty bearer token, which the workspace would reject as unauthenticated
	// and the operator would read as a policy problem.
	if err := os.WriteFile(path, []byte("   \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = read(); err == nil {
		t.Fatal("an empty token file was accepted")
	}
	absent, err := credentialSource(
		"-admission-token", "", filepath.Join(t.TempDir(), "absent"),
		"SHOAL_ADMISSION_TOKEN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = absent(); err == nil {
		t.Fatal("a missing token file was accepted")
	}
}

// TestTwoTokenSourcesAreRefusedRatherThanRanked pins the refusal instead of a
// precedence rule.
//
// Picking one silently makes the effective credential invisible: an operator
// who adds a file while a stale env var is still in the manifest cannot tell
// from the configuration which is being presented, and the symptom of the wrong
// choice is an authentication failure naming neither source.
//
// The env flag ships with a non-empty default, so the default being present
// cannot count as a second choice — otherwise the file form would be
// unreachable without also blanking the env flag.
func TestTwoTokenSourcesAreRefusedRatherThanRanked(t *testing.T) {
	const envDefault = "SHOAL_ADMISSION_TOKEN"
	if _, err := credentialSource(
		"-admission-token", "OTHER_VAR", "/run/token", envDefault); err == nil {
		t.Fatal("two explicitly chosen token sources were accepted")
	}
	if _, err := credentialSource(
		"-admission-token", envDefault, "/run/token", envDefault); err != nil {
		t.Fatalf("the file form is unreachable with the env flag at its default: %v", err)
	}
	if _, err := credentialSource("-admission-token", "", "", envDefault); err == nil {
		t.Fatal("no token source at all was accepted")
	}
	// And the refusal is reached through run(), not only in isolation.
	detail := refusal(t, proxyArgs(
		"-admission-token-env", "OTHER_VAR", "-admission-token-file", "/run/token"))
	if !strings.Contains(detail, "mutually exclusive") {
		t.Fatalf("run() did not apply the rule: %s", detail)
	}
}
