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

	agentID         string
	agentGeneration int64
	capability      string
	action          string
	sourceID        []byte
	policyID        []byte
	lease           time.Duration
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
		// Both classes, always. The proxy reads the corpus references the
		// caller declared and transmits them off-host; declaring less than it
		// does is the understatement the effect floor exists to refuse (#385).
		Effects:     []string{"egresses-content", "reads-corpus"},
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
	if decoded.Token == nil {
		// An allowance with no token cannot be reported, and an unreportable
		// call is one the plane can never learn the outcome of.
		return grant{}, fmt.Errorf(
			"%w: allowed without a token", ErrPlaneUnreachable)
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

// absoluteURL parses a configured base URL and refuses anything that could not
// address a service.
func absoluteURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !parsed.IsAbs() || parsed.Hostname() == "" {
		return nil, errors.New("must be an absolute URL with a host")
	}
	if parsed.Scheme != "https" && !isLoopback(parsed.Hostname()) {
		// Plain HTTP to a remote decision plane would put the proxy's bearer
		// token and every declaration on the wire in clear. Loopback is
		// exempted because it does not leave the host, which is the same line
		// the model providers draw.
		return nil, errors.New("must use https unless it addresses loopback")
	}
	return parsed, nil
}

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	address := net.ParseIP(host)
	return address != nil && address.IsLoopback()
}
