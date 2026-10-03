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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// The admission client is the proxy's whole relationship with Shoal.
//
// It speaks HTTP to the explorer rather than importing the fleet package,
// because the proxy is a separate process on purpose: it handles untrusted
// prompt content from arbitrary callers and talks to third-party endpoints, and
// the explorer holds the policy store and the corpus. Linking them would put
// prompt injection in the same address space as the decision plane (#390).

// ErrPlaneUnreachable means the decision could not be obtained at all.
//
// It is deliberately distinct from a denial. A denial is an answer; this is the
// absence of one, and the two must not look alike — to the operator, because
// one is a policy outcome and the other is an outage, and to the caller,
// because a caller told "denied" will not retry while one told "unavailable"
// should.
var ErrPlaneUnreachable = errors.New("shoal-llm-proxy: decision plane unreachable")

// ErrDenied means Shoal refused the call.
var ErrDenied = errors.New("shoal-llm-proxy: admission denied")

// admissionOutcome mirrors the three answers the seam can give. The proxy
// compares against these strings rather than importing the fleet package, so a
// value it does not recognise is handled below rather than silently accepted.
const (
	outcomeDenied    = "denied"
	outcomeAllowed   = "allowed"
	outcomeObligated = "allowed_with_obligations"
)

// admissionClient asks before the call and reports after it.
type admissionClient struct {
	base       *url.URL
	http       *http.Client
	credential func() (string, error)

	clock           func() time.Time
	agentID         string
	agentGeneration int64
	capability      string
	action          string
	// effects is the set this proxy declares, derived from the configured
	// provider rather than hardcoded. Empty is never correct, so newProxy sets
	// it where the provider is resolved.
	effects  []string
	sourceID []byte
	policyID []byte
	lease    time.Duration
}

// grant is what the proxy acts on.
type grant struct {
	// Withhold is the subset of references the caller declared that must not
	// appear in what it sends. An obligated grant is still a grant: refusing
	// the whole call when the plane has said which part to drop would discard
	// the useful answer for the blunt one (#390).
	Withhold []string
	token    admissionToken
}

type admissionToken struct {
	ActionID  string    `json:"action_id"`
	TokenID   string    `json:"token_id"`
	Version   uint64    `json:"version"`
	ExpiresAt time.Time `json:"expires_at"`
}

// reportable reports whether a grant's token could actually close the loop.
//
// minimumReportWindow is the margin the report itself needs. A token that
// expires while the upstream call is still running cannot be reported at all,
// so a grant arriving with less than this left is refused rather than spent.
const minimumReportWindow = 5 * time.Second

func (t *admissionToken) reportable(now time.Time) error {
	if t == nil {
		return errors.New("allowed without a token")
	}
	for name, value := range map[string]string{
		"action ID": t.ActionID, "token ID": t.TokenID,
	} {
		decoded, err := base64.RawURLEncoding.DecodeString(value)
		if err != nil || len(decoded) == 0 {
			return fmt.Errorf("token %s is not a usable identity", name)
		}
	}
	if t.Version == 0 {
		return errors.New("token version is invalid")
	}
	// A missing expiry is refused rather than treated as "no deadline". The
	// check read "if an expiry is set and it is too close", which let the one
	// shape it most needed to catch straight through: a plane that answers
	// without an expiry produced a token this function called reportable and
	// nothing could establish a window for. Absent is not generous here, it is
	// unknown, and an unknown deadline cannot be shown to leave room.
	if t.ExpiresAt.IsZero() {
		return errors.New("token carries no expiry, so no report window can be established")
	}
	if t.ExpiresAt.Sub(now) < minimumReportWindow {
		return errors.New("token expires before the call could be reported")
	}
	return nil
}

type requestContextWire struct {
	RequestID     string    `json:"request_id"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	ReasonCode    string    `json:"reason_code"`
	ReasonDetail  string    `json:"reason_detail,omitempty"`
	Deadline      time.Time `json:"deadline"`
}

type admissionRequestWire struct {
	Context         requestContextWire `json:"context"`
	ID              string             `json:"id"`
	IdempotencyKey  string             `json:"idempotency_key"`
	TokenID         string             `json:"token_id"`
	AgentID         string             `json:"agent_id"`
	AgentGeneration int64              `json:"agent_generation"`
	Capability      string             `json:"capability"`
	Action          string             `json:"action"`
	SourceID        []byte             `json:"source_id"`
	PolicyID        []byte             `json:"policy_id"`
	ObjectID        string             `json:"object_id"`
	Effects         []string           `json:"effects"`
	Input           json.RawMessage    `json:"input"`
	Disclosures     []string           `json:"disclosures,omitempty"`
	Lease           time.Duration      `json:"lease"`
}

type admissionGrantWire struct {
	Outcome  string          `json:"outcome"`
	Token    *admissionToken `json:"token,omitempty"`
	Withhold []string        `json:"withhold"`
}

type admissionReportWire struct {
	Context   requestContextWire `json:"context"`
	Token     admissionToken     `json:"token"`
	Outcome   json.RawMessage    `json:"outcome,omitempty"`
	Failed    bool               `json:"failed,omitempty"`
	ErrorCode string             `json:"error_code,omitempty"`
}

// request asks whether a call may happen.
//
// declaration is what the call would do, never the prompt. Shoal's admission
// surface takes a declaration precisely so the plane deciding whether content
// may be transmitted does not itself receive a copy of that content, and the
// proxy must not be the thing that undoes it.
func (c *admissionClient) request(
	ctx context.Context,
	identity callerIdentity,
	declaration json.RawMessage,
	disclosures []string,
	now time.Time,
) (grant, error) {
	body := admissionRequestWire{
		Context: requestContextWire{
			RequestID:     identity.RequestID,
			CorrelationID: identity.CorrelationID,
			ReasonCode:    "llm_proxy_call",
			Deadline:      now.Add(c.lease),
		},
		ID:              identity.AdmissionID,
		IdempotencyKey:  identity.AdmissionID,
		TokenID:         identity.TokenID,
		AgentID:         c.agentID,
		AgentGeneration: c.agentGeneration,
		Capability:      c.capability,
		Action:          c.action,
		SourceID:        c.sourceID,
		PolicyID:        c.policyID,
		ObjectID:        identity.ObjectID,
		// Derived from the configured provider, not fixed. See
		// declaredEffects: the fleet contract requires it, and declaring
		// egress against a loopback provider is denied rather than refused.
		Effects:     c.effects,
		Input:       declaration,
		Disclosures: disclosures,
		Lease:       c.lease,
	}
	var decoded admissionGrantWire
	if err := c.post(ctx, "request", body, &decoded); err != nil {
		return grant{}, err
	}
	switch decoded.Outcome {
	case outcomeDenied:
		return grant{}, ErrDenied
	case outcomeAllowed, outcomeObligated:
	default:
		// An outcome this build does not recognise is not an allowance. A
		// newer plane could answer in a vocabulary this proxy predates, and
		// treating the unknown as permission would make every future outcome
		// default to the permissive reading.
		return grant{}, fmt.Errorf(
			"%w: unrecognised admission outcome", ErrPlaneUnreachable)
	}
	// An allowance the proxy could not report is refused before the egress,
	// not discovered after it. Checking only for a nil token left every other
	// unreportable shape through — an empty object, unparseable identities, a
	// zero version, a lease already spent — and in each case the call would
	// have been forwarded and the report then rejected, which is precisely the
	// unreportable grant this refusal exists to prevent.
	if err := decoded.Token.reportable(c.clockNow()); err != nil {
		return grant{}, fmt.Errorf("%w: %v", ErrPlaneUnreachable, err)
	}
	// The token ID is an echo, so it is checked rather than trusted.
	//
	// The claim is created under the token ID this proxy chose and sent
	// (dispatch_service.go:378 sets ClaimID from request.TokenID), and the
	// report selects the claim by the ID that came back. So a response carrying
	// a different one does not merely misdescribe this call: it makes the proxy
	// forward the call and then close somebody else's outstanding admission.
	//
	// Structural validity could not catch that, because a swapped ID is
	// perfectly well formed. This is the same rule as not trusting
	// X-Forwarded-Host — a value we are told, which we can compare against a
	// value we know, must be compared.
	if decoded.Token.TokenID != identity.TokenID {
		return grant{}, fmt.Errorf(
			"%w: the grant names a different claim than the one requested",
			ErrPlaneUnreachable)
	}
	return grant{Withhold: decoded.Withhold, token: *decoded.Token}, nil
}

// report closes the loop.
//
// Without it admission is a stateless gate: the plane grants permission and
// never learns whether the call happened. The report is the edge a cross-call
// accumulator charges (#389), which is why a failure to report is logged and
// surfaced rather than discarded — a silently unreported grant is
// indistinguishable from a caller that went dark.
func (c *admissionClient) report(
	ctx context.Context,
	token admissionToken,
	identity callerIdentity,
	outcome json.RawMessage,
	failure string,
	now time.Time,
) error {
	body := admissionReportWire{
		Context: requestContextWire{
			RequestID:     identity.RequestID,
			CorrelationID: identity.CorrelationID,
			ReasonCode:    "llm_proxy_report",
			Deadline:      now.Add(c.lease),
		},
		Token: token,
	}
	// An outcome and a failure are exclusive: the admission surface refuses a
	// report carrying both, because the pair would make an exact-replay
	// comparison undefined.
	if failure != "" {
		body.Failed = true
		body.ErrorCode = failure
	} else {
		body.Outcome = outcome
	}
	return c.post(ctx, "report", body, nil)
}

func (c *admissionClient) post(
	ctx context.Context, route string, body any, out any,
) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPlaneUnreachable, err)
	}
	endpoint := c.base.JoinPath("api", "v1", "admission", route)
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, endpoint.String(), bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPlaneUnreachable, err)
	}
	credential, err := c.credential()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPlaneUnreachable, err)
	}
	request.Header.Set("Authorization", "Bearer "+credential)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPlaneUnreachable, err)
	}
	defer response.Body.Close()
	switch {
	case response.StatusCode == http.StatusOK,
		response.StatusCode == http.StatusCreated:
	case response.StatusCode == http.StatusForbidden,
		response.StatusCode == http.StatusUnauthorized:
		// The plane answered and refused this proxy's own credential. That is
		// a denial of the proxy, not of the caller, and it is reported as
		// unreachable rather than as a policy denial on the call: the caller's
		// request was never adjudicated.
		return fmt.Errorf("%w: proxy credential rejected", ErrPlaneUnreachable)
	default:
		// The body is deliberately not included. A plane error can carry
		// detail the caller is not entitled to, and this error reaches a
		// caller-facing response.
		return fmt.Errorf(
			"%w: admission %s returned %d",
			ErrPlaneUnreachable, route, response.StatusCode)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil
	}
	if err := json.NewDecoder(response.Body).Decode(out); err != nil {
		return fmt.Errorf("%w: %v", ErrPlaneUnreachable, err)
	}
	return nil
}

// encodeID renders an opaque identity the way every fleet wire field expects.
func encodeID(value []byte) string {
	return base64.RawURLEncoding.EncodeToString(value)
}

// decodeID parses a configured base64url identity.
func decodeID(name, value string) ([]byte, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, fmt.Errorf("%s is required", name)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(trimmed)
	if err != nil {
		return nil, fmt.Errorf("%s must be unpadded base64url", name)
	}
	if len(decoded) == 0 {
		return nil, fmt.Errorf("%s is required", name)
	}
	return decoded, nil
}

// newHTTPClient builds every outbound client this proxy uses.
//
// The redirect policy lives here rather than at each call site because it is a
// security property, and a call site that only meant to change the transport
// should not be able to drop it by assigning a fresh http.Client. The test
// harness swaps the transport for the same reason.
func newHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, CheckRedirect: refuseRedirect}
}

// absoluteURL parses a configured base URL and refuses anything that could not
// address a service.
func absoluteURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !parsed.IsAbs() || parsed.Hostname() == "" {
		return nil, errors.New("must be an absolute URL with a host")
	}
	if parsed.Scheme != "https" &&
		!(parsed.Scheme == "http" && isLoopback(parsed.Hostname())) {
		// Plain HTTP to a remote endpoint would put the bearer token and every
		// declaration on the wire in clear. Loopback is exempted because it
		// does not leave the host, which is the same line pkg/model draws for
		// model providers.
		//
		// The exemption is for http specifically, not for every scheme on
		// loopback. "ftp://localhost" is not a transport this can speak, and
		// admitting it at startup only moves the failure to every request.
		return nil, errors.New("must use https, or http addressing loopback")
	}
	return parsed, nil
}

// planeURL applies absoluteURL to the admission plane, with the one
// acknowledged exception.
//
// The chart already has llmProxy.admission.allowPlaintext for a mesh that
// supplies the transport authentication the scheme would, and asserts that
// configuration renders — but nothing carried the acknowledgement into the
// process, so absoluteURL refused it at startup and the documented mesh
// deployment produced a pod in CrashLoopBackOff. A chart that renders a
// configuration the binary refuses is the third mismatch of this kind found on
// this work; the acknowledgement is a flag now so the two agree.
//
// It is deliberately narrower than "allow plaintext". The upstream URL carries
// the prompt and the operator's credential and has no opt-out, because a mesh
// that authenticates the hop to the decision plane says nothing about the hop
// to a third-party provider.
func planeURL(raw string, allowPlaintext bool) (*url.URL, error) {
	parsed, err := absoluteURL(raw)
	if err == nil || !allowPlaintext {
		return parsed, err
	}
	// Re-parse under the relaxed rule, still refusing anything that is not an
	// absolute http(s) URL with a host. The acknowledgement covers the
	// transport, not the shape.
	relaxed, parseErr := url.Parse(strings.TrimSpace(raw))
	if parseErr != nil || !relaxed.IsAbs() || relaxed.Hostname() == "" ||
		relaxed.Scheme != "http" {
		return nil, err
	}
	return relaxed, nil
}

// declaredEffects derives the effect set from the provider this proxy was
// configured with, which the fleet contract requires rather than permits.
//
// pkg/explorer/fleet/model.go says it outright: EffectEgressesContent "is not a
// property of the code. The same executor is egress-free against a loopback
// model provider and egress-bearing against a hosted one, so an executor
// declaring this must derive it from the provider it was actually configured
// with."
//
// This declared both classes unconditionally, with a comment arguing that
// declaring less than you do is the understatement #385's effect floor exists
// to refuse. That reasoning is right about understatement and wrong about this:
// the floor refuses declaring too little, so over-declaring is never refused —
// it is silently denied instead. A policy that forbids egress then denies every
// call on a deployment where nothing leaves the host, and the chart supports
// exactly that deployment and asserts it renders. Over-declaration is not the
// safe direction here, it is the invisible one.
//
// reads-corpus stays unconditional: the proxy reads the references the caller
// declared whatever the provider is.
func declaredEffects(upstream *url.URL) []string {
	if upstream != nil && isLoopback(upstream.Hostname()) {
		return []string{string(fleet.EffectReadsCorpus)}
	}
	return []string{
		string(fleet.EffectEgressesContent),
		string(fleet.EffectReadsCorpus),
	}
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}

// clockNow defaults so a zero-value client in a test still has a clock.
func (c *admissionClient) clockNow() time.Time {
	if c == nil || c.clock == nil {
		return time.Now()
	}
	return c.clock()
}

// normalizeAuthority folds a Host or :authority into a comparable form, using
// the standard library for the host:port split so bracketed IPv6 literals are
// handled rather than guessed at.
//
// The host is lowercased because hostnames are case-insensitive, and a single
// trailing dot is dropped so the FQDN-root spelling matches its bare form. Both
// foldings apply to configured and request authorities alike, which is what
// keeps the comparison symmetric.
func normalizeAuthority(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", false
	}
	host, port, err := net.SplitHostPort(trimmed)
	if err != nil {
		host, port = trimmed, ""
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if host == "" {
		return "", false
	}
	if port == "" {
		return host, true
	}
	return net.JoinHostPort(host, port), true
}
