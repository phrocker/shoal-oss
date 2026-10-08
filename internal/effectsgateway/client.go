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
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// The dispatch client speaks the routes that exist on main and no others.
// Claim renewal (#430) and the lost-fence ambiguity route (#484) are absent on
// purpose: a client method for a route that does not exist yet is a method a
// worker could be written against and that fails only in production.
//
// Wire rules, taken from pkg/explorer/webapi/fleet_dispatch.go and checked
// against the real handler in this package's tests:
//
//   - every byte field is unpadded base64url;
//   - a lease is an integer number of nanoseconds, positive and at most
//     fleet.MaxActionClaimTTL — the service refuses a longer one rather than
//     clamping it;
//   - every request carries a context with a request ID, a non-empty reason
//     code and a deadline in the future, in UTC;
//   - 404 and 409 are refusals, and 503 with Shoal-Commit-Outcome:
//     indeterminate means the mutation may have committed.

const (
	// commitOutcomeHeader and commitOutcomeIndeterminate mirror
	// webapi.CommitOutcomeHeader; restated rather than imported so this
	// package does not depend on the server's transport package.
	commitOutcomeHeader        = "Shoal-Commit-Outcome"
	commitOutcomeIndeterminate = "indeterminate"
	// maxDispatchResponseBytes bounds what the client will read from the
	// explorer. A claim response carries the input (at most
	// fleet.MaxActionPayloadBytes) and a pull page carries no inputs.
	maxDispatchResponseBytes = 8 << 20
	// ClaimNonceBytes is the CSPRNG part of a claim ID.
	ClaimNonceBytes = 16
	// claimIDPrefix versions the claim ID layout.
	claimIDPrefix = "gw1|"
)

// DispatchErrorKind is a dispatch failure reduced to what the worker does
// about it. Closed, and loggable.
type DispatchErrorKind string

const (
	// DispatchNotFound (404): on claim, re-pull — the loser of a claim race
	// is told not-found deliberately, to avoid an existence oracle, so it
	// never means "this action is gone".
	DispatchNotFound DispatchErrorKind = "not_found"
	// DispatchConflict (409): the version or claim moved.
	DispatchConflict DispatchErrorKind = "conflict"
	// DispatchIndeterminate (503 + Shoal-Commit-Outcome: indeterminate): the
	// mutation may have committed; resend the identical body.
	DispatchIndeterminate DispatchErrorKind = "indeterminate"
	// DispatchUnavailable (503 without the header).
	DispatchUnavailable  DispatchErrorKind = "unavailable"
	DispatchInvalid      DispatchErrorKind = "invalid_argument"
	DispatchUnauthorized DispatchErrorKind = "unauthorized"
	DispatchDeadline     DispatchErrorKind = "deadline"
	DispatchStatus       DispatchErrorKind = "unexpected_status"
	DispatchTransport    DispatchErrorKind = "transport"
	DispatchProtocol     DispatchErrorKind = "protocol"
	DispatchRefusedLocal DispatchErrorKind = "refused_locally"
	DispatchNoCredential DispatchErrorKind = "no_credential"
	// DispatchRecordedOtherwise: the record is terminal under this claim, in
	// a different state from the one reported — for example a success whose
	// output the explorer refused and recorded as failed. Final; do not
	// report again.
	DispatchRecordedOtherwise DispatchErrorKind = "recorded_otherwise"
)

var validDispatchErrors = map[DispatchErrorKind]bool{
	DispatchNotFound: true, DispatchConflict: true, DispatchIndeterminate: true,
	DispatchUnavailable: true, DispatchInvalid: true, DispatchUnauthorized: true,
	DispatchDeadline: true, DispatchStatus: true, DispatchTransport: true,
	DispatchProtocol: true, DispatchRefusedLocal: true, DispatchNoCredential: true,
	DispatchRecordedOtherwise: true,
}

// DispatchError is every error the client returns after validation. Its text
// is composed here from closed values; it never includes a transport error's
// text or a response body.
type DispatchError struct {
	Op     string
	Kind   DispatchErrorKind
	Status int
	// Code is the explorer's error code, when it sent a well-formed one.
	Code string
	// Failure categorizes a transport failure.
	Failure FailureKind
	// reason is a fixed local explanation for refused_locally.
	reason string
	cause  error
}

func (e *DispatchError) Error() string {
	var b strings.Builder
	b.WriteString("dispatch ")
	b.WriteString(e.Op)
	b.WriteString(": ")
	b.WriteString(string(e.Kind))
	if e.Status != 0 {
		fmt.Fprintf(&b, " (HTTP %d", e.Status)
		if e.Code != "" {
			b.WriteString(", ")
			b.WriteString(e.Code)
		}
		b.WriteString(")")
	}
	if e.Failure != FailureNone {
		b.WriteString(" (")
		b.WriteString(string(e.Failure))
		b.WriteString(")")
	}
	if e.reason != "" {
		b.WriteString(": ")
		b.WriteString(e.reason)
	}
	return b.String()
}

// Unwrap exposes the cause to errors.Is and errors.As only. It is never
// formatted: Error does not include it.
func (e *DispatchError) Unwrap() error { return e.cause }

// DispatchKind returns the kind of a client error, or "" if err is not one.
func DispatchKind(err error) DispatchErrorKind {
	var dispatchErr *DispatchError
	if errors.As(err, &dispatchErr) {
		return dispatchErr.Kind
	}
	return ""
}

func refusedLocally(op, reason string) error {
	return &DispatchError{Op: op, Kind: DispatchRefusedLocal, reason: reason}
}

// RequestContext is the context every dispatch call carries.
type RequestContext struct {
	RequestID     []byte
	CorrelationID []byte
	ReasonCode    string
	ReasonDetail  string
	Deadline      time.Time
}

type contextWire struct {
	RequestID     string    `json:"request_id"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	ReasonCode    string    `json:"reason_code"`
	ReasonDetail  string    `json:"reason_detail,omitempty"`
	Deadline      time.Time `json:"deadline"`
}

func (r RequestContext) wire(op string, now time.Time) (contextWire, error) {
	if len(r.RequestID) == 0 || len(r.RequestID) > fleet.MaxActionIDBytes {
		return contextWire{}, refusedLocally(op, "request ID is outside its bound")
	}
	if len(r.CorrelationID) > fleet.MaxActionIDBytes {
		return contextWire{}, refusedLocally(op, "correlation ID is outside its bound")
	}
	if r.ReasonCode == "" || len(r.ReasonCode) > fleet.MaxReasonCodeBytes ||
		strings.TrimSpace(r.ReasonCode) != r.ReasonCode {
		return contextWire{}, refusedLocally(op, "reason code is required, at most "+
			fmt.Sprint(fleet.MaxReasonCodeBytes)+" bytes, with no surrounding whitespace")
	}
	if r.Deadline.IsZero() || !now.Before(r.Deadline) {
		return contextWire{}, refusedLocally(op, "request deadline must be in the future")
	}
	wire := contextWire{
		RequestID:    base64.RawURLEncoding.EncodeToString(r.RequestID),
		ReasonCode:   r.ReasonCode,
		ReasonDetail: r.ReasonDetail,
		// The explorer refuses a deadline whose location is not UTC.
		Deadline: r.Deadline.UTC(),
	}
	if len(r.CorrelationID) > 0 {
		wire.CorrelationID = base64.RawURLEncoding.EncodeToString(r.CorrelationID)
	}
	return wire, nil
}

// NewRequestID returns 16 fresh CSPRNG bytes.
func NewRequestID(random io.Reader) ([]byte, error) {
	if random == nil {
		random = rand.Reader
	}
	id := make([]byte, 16)
	if _, err := io.ReadFull(random, id); err != nil {
		return nil, errors.New("request ID: random source failed")
	}
	return id, nil
}

// NewClaimID builds "gw1|" ‖ pod ‖ "|" ‖ 16 CSPRNG bytes.
//
// Fresh per claim attempt of a pulled record, and reused verbatim for
// transport retries of that attempt so the claim's replay branch answers
// them. The pod name makes it pod-identifying in the record; the random
// suffix is what makes it unique, because two replicas sharing a claim ID are
// both told they hold the claim and both told they succeeded (#391). It is
// not an identity: Status and lapsed-claim Pull expose it to co-principals,
// and the worker reports under the principal that claimed, not under this.
func NewClaimID(pod string, random io.Reader) ([]byte, ClaimNonce, error) {
	maxPod := fleet.MaxActionIDBytes - len(claimIDPrefix) - 1 - ClaimNonceBytes
	if pod == "" || len(pod) > maxPod {
		return nil, ClaimNonce{}, fmt.Errorf("pod name must be 1 to %d bytes", maxPod)
	}
	for i := 0; i < len(pod); i++ {
		if pod[i] < 0x21 || pod[i] > 0x7e || pod[i] == '|' {
			return nil, ClaimNonce{}, errors.New("pod name must be printable " +
				"ASCII without spaces or |")
		}
	}
	if random == nil {
		random = rand.Reader
	}
	var nonce ClaimNonce
	if _, err := io.ReadFull(random, nonce[:]); err != nil {
		return nil, ClaimNonce{}, errors.New("claim ID: random source failed")
	}
	id := make([]byte, 0, len(claimIDPrefix)+len(pod)+1+ClaimNonceBytes)
	id = append(id, claimIDPrefix...)
	id = append(id, pod...)
	id = append(id, '|')
	id = append(id, nonce[:]...)
	return id, nonce, nil
}

// Action is a dispatch record as the worker sees it. Input and ExecutorKey
// are populated on the claim response only; the pull page never carries them,
// and re-reading the action does not return them, so the worker keeps them
// from the claim.
type Action struct {
	ID              []byte
	Version         uint64
	State           fleet.DispatchState
	AgentID         []byte
	AgentGeneration int64
	Capability      string
	Action          string
	Input           json.RawMessage
	Output          json.RawMessage
	ErrorCode       string
	Deadline        time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
	ExecutorKey     ExecutorKey
	ClaimID         []byte
	ClaimFence      uint64
	ClaimLeaseUntil time.Time
	EffectPossible  bool
}

// ClaimTimes returns the server timestamps Anchor needs.
func (a Action) ClaimTimes() ClaimTimes {
	return ClaimTimes{
		UpdatedAt: a.UpdatedAt, ClaimLeaseUntil: a.ClaimLeaseUntil, Deadline: a.Deadline,
	}
}

type actionWire struct {
	ID              string              `json:"id"`
	Version         uint64              `json:"version"`
	State           fleet.DispatchState `json:"state"`
	AgentID         string              `json:"agent_id"`
	AgentGeneration int64               `json:"agent_generation"`
	Capability      string              `json:"capability"`
	Action          string              `json:"action"`
	Input           json.RawMessage     `json:"input,omitempty"`
	Output          json.RawMessage     `json:"output,omitempty"`
	ErrorCode       string              `json:"error_code,omitempty"`
	Deadline        time.Time           `json:"deadline"`
	CreatedAt       time.Time           `json:"created_at"`
	UpdatedAt       time.Time           `json:"updated_at"`
	ExecutorKey     string              `json:"executor_key,omitempty"`
	ClaimID         string              `json:"claim_id,omitempty"`
	ClaimFence      uint64              `json:"claim_fence,omitempty"`
	ClaimLeaseUntil time.Time           `json:"claim_lease_until,omitempty"`
	EffectPossible  bool                `json:"effect_possible"`
}

func (w actionWire) decode() (Action, error) {
	id, err := base64.RawURLEncoding.DecodeString(w.ID)
	if err != nil || len(id) == 0 {
		return Action{}, errors.New("action ID is not unpadded base64url")
	}
	agent, err := base64.RawURLEncoding.DecodeString(w.AgentID)
	if err != nil {
		return Action{}, errors.New("agent ID is not unpadded base64url")
	}
	claimID, err := base64.RawURLEncoding.DecodeString(w.ClaimID)
	if err != nil {
		return Action{}, errors.New("claim ID is not unpadded base64url")
	}
	action := Action{
		ID: id, Version: w.Version, State: w.State, AgentID: agent,
		AgentGeneration: w.AgentGeneration, Capability: w.Capability,
		Action: w.Action, Input: w.Input, Output: w.Output,
		ErrorCode: w.ErrorCode, Deadline: w.Deadline, CreatedAt: w.CreatedAt,
		UpdatedAt: w.UpdatedAt, ClaimID: claimID, ClaimFence: w.ClaimFence,
		ClaimLeaseUntil: w.ClaimLeaseUntil, EffectPossible: w.EffectPossible,
	}
	if w.ExecutorKey != "" {
		// The HTTP wire spells the key raw-URL; ParseExecutorKey also accepts
		// the MCP spelling, and either way the key is held as bytes.
		key, err := ParseExecutorKey(w.ExecutorKey)
		if err != nil {
			return Action{}, fmt.Errorf("executor key: %w", err)
		}
		action.ExecutorKey = key
	}
	return action, nil
}

// Descriptor is the part of a resolved descriptor the startup check reads.
type Descriptor struct {
	ID           []byte
	Generation   int64
	ExecutorRef  string
	Capabilities []fleet.Capability
}

// Actions returns the named capability's actions for VerifyDescriptor.
func (d Descriptor) Actions(capability string) ([]DescriptorAction, bool) {
	for _, declared := range d.Capabilities {
		if declared.Name != capability {
			continue
		}
		actions := make([]DescriptorAction, len(declared.Actions))
		for i, action := range declared.Actions {
			actions[i] = DescriptorAction{
				Name: action.Name, Effects: action.Effects,
				InputSchema:  action.InputSchema,
				OutputSchema: action.OutputSchema,
			}
		}
		return actions, true
	}
	return nil, false
}

// PullPage is one page of pullable actions plus what PRECHECK needs to
// estimate the explorer's clock.
type PullPage struct {
	Actions []Action
	Next    string
	// Header holds the response's Date header only.
	Header     http.Header
	ReceivedAt time.Time
}

// DispatchClient calls the explorer's fleet dispatch routes.
type DispatchClient struct {
	base       *url.URL
	http       *http.Client
	credential func() (string, error)
	clock      Clock
}

// NewDispatchClient binds a client to an explorer base URL (which must already
// have passed the dispatch URL rule) and a bearer-token source read per call.
func NewDispatchClient(
	base *url.URL, client *http.Client, credential func() (string, error), clock Clock,
) (*DispatchClient, error) {
	if base == nil || !base.IsAbs() || base.Host == "" {
		return nil, errors.New("dispatch client requires an absolute explorer URL")
	}
	if client == nil || credential == nil {
		return nil, errors.New("dispatch client requires an HTTP client and a credential source")
	}
	if clock == nil {
		clock = time.Now
	}
	copied := *base
	copied.Path = strings.TrimSuffix(copied.Path, "/")
	copied.RawPath = ""
	return &DispatchClient{base: &copied, http: client, credential: credential, clock: clock}, nil
}

// Pull lists actions the worker's principal may claim. The page has no
// capability or action filter on the server, so the caller filters before
// claiming: a claim is what sets EffectPossible.
func (c *DispatchClient) Pull(
	ctx context.Context, request RequestContext, after string, limit int,
) (PullPage, error) {
	const op = "pull"
	if limit <= 0 || limit > fleet.MaxDispatchListResults {
		return PullPage{}, refusedLocally(op, fmt.Sprintf(
			"limit must be 1 to %d", fleet.MaxDispatchListResults))
	}
	if after != "" {
		if _, err := base64.RawURLEncoding.DecodeString(after); err != nil {
			return PullPage{}, refusedLocally(op, "cursor is not unpadded base64url")
		}
	}
	contextValue, err := request.wire(op, c.clock())
	if err != nil {
		return PullPage{}, err
	}
	body := struct {
		Context contextWire `json:"context"`
		After   string      `json:"after,omitempty"`
		Limit   int         `json:"limit"`
	}{Context: contextValue, After: after, Limit: limit}
	var response struct {
		Actions []actionWire `json:"actions"`
		Next    string       `json:"next,omitempty"`
	}
	header, receivedAt, err := c.post(ctx, op, "/api/v1/fleet/actions/pull", body, &response)
	if err != nil {
		return PullPage{}, err
	}
	page := PullPage{
		Actions: make([]Action, 0, len(response.Actions)), Next: response.Next,
		Header: http.Header{}, ReceivedAt: receivedAt,
	}
	if date := header.Get("Date"); date != "" {
		page.Header.Set("Date", date)
	}
	for _, wire := range response.Actions {
		action, err := wire.decode()
		if err != nil {
			return PullPage{}, &DispatchError{Op: op, Kind: DispatchProtocol, reason: err.Error()}
		}
		page.Actions = append(page.Actions, action)
	}
	return page, nil
}

// ClaimRequest names the claim being taken.
type ClaimRequest struct {
	Context         RequestContext
	ExpectedVersion uint64
	ClaimID         []byte
	Lease           time.Duration
}

// Claim takes an action under a fence. The response is the only one that
// carries the input and the executor key.
func (c *DispatchClient) Claim(ctx context.Context, actionID []byte, request ClaimRequest) (Action, error) {
	const op = "claim"
	if err := checkActionID(op, actionID); err != nil {
		return Action{}, err
	}
	if request.ExpectedVersion == 0 {
		return Action{}, refusedLocally(op, "expected version is required")
	}
	if len(request.ClaimID) == 0 || len(request.ClaimID) > fleet.MaxActionIDBytes {
		return Action{}, refusedLocally(op, "claim ID is outside its bound")
	}
	if request.Lease <= 0 || request.Lease > fleet.MaxActionClaimTTL {
		return Action{}, refusedLocally(op, "lease must be positive and at most "+
			fleet.MaxActionClaimTTL.String()+"; the explorer refuses a longer one "+
			"rather than shortening it")
	}
	contextValue, err := request.Context.wire(op, c.clock())
	if err != nil {
		return Action{}, err
	}
	body := struct {
		Context         contextWire   `json:"context"`
		ExpectedVersion uint64        `json:"expected_version"`
		ClaimID         string        `json:"claim_id"`
		Lease           time.Duration `json:"lease"`
	}{
		Context: contextValue, ExpectedVersion: request.ExpectedVersion,
		ClaimID: base64.RawURLEncoding.EncodeToString(request.ClaimID),
		Lease:   request.Lease,
	}
	var response actionWire
	if _, _, err := c.post(ctx, op, actionPath(actionID, "claim"), body, &response); err != nil {
		return Action{}, err
	}
	action, err := response.decode()
	if err != nil {
		return Action{}, &DispatchError{Op: op, Kind: DispatchProtocol, reason: err.Error()}
	}
	if !bytes.Equal(action.ID, actionID) || !bytes.Equal(action.ClaimID, request.ClaimID) ||
		action.State != fleet.DispatchClaimed {
		return Action{}, &DispatchError{Op: op, Kind: DispatchProtocol,
			reason: "claim response does not describe the requested claim"}
	}
	return action, nil
}

// Completion is a terminal report. Exactly one of Output and Failed+ErrorCode.
type Completion struct {
	Context         RequestContext
	ExpectedVersion uint64
	ClaimID         []byte
	Output          json.RawMessage
	Failed          bool
	ErrorCode       string
}

// Complete reports the outcome under the claim. Evidence is never sent: the
// gateway is the first executor that is not trusted, and an evidence anchor is
// a claim about the corpus it never read.
//
// A lost response is recovered by resending the identical body once: on a
// transport error, or on 503 with Shoal-Commit-Outcome: indeterminate, the
// report may have committed, and the completion route's replay branch answers
// a resend with the committed record. That recovery is permanent.
//
// If the resend's answer is itself lost — a transport error or another
// indeterminate 503 — Complete returns DispatchIndeterminate. It never
// returns the first attempt's status after a resend: that status describes a
// request whose outcome the resend was sent to learn, and reporting it would
// turn "possibly committed" into a definite refusal.
//
// A resend answered 400 or 500 is not definite either (#492): the first
// attempt may have committed, or — if the first answer was a genuine error
// that committed nothing — the resend may have committed and had its record
// discarded. So the record is read a third time through the replay branch,
// and anything but a record is DispatchIndeterminate.
//
// The 400 and 500 triggers are a workaround for #492, to be removed when it
// lands. On main, CompleteClaim returns the committed record together with an
// error for a reported failure ("remote executor reported failure") and for a
// success whose output the explorer refuses (recorded as failed,
// invalid_executor_output), and the /complete handler discards the record and
// answers 500 and 400 respectively — a terminal record that was durably
// written, indistinguishable from a refusal that wrote nothing.
//
// When the committed record is terminal under this claim but differs from
// what was reported — state, error code, or output — Complete returns it
// together with DispatchRecordedOtherwise: the record is final, and the
// caller must not report again. That check is permanent.
func (c *DispatchClient) Complete(ctx context.Context, actionID []byte, completion Completion) (Action, error) {
	const op = "complete"
	if err := checkActionID(op, actionID); err != nil {
		return Action{}, err
	}
	if completion.ExpectedVersion == 0 {
		return Action{}, refusedLocally(op, "expected version is required")
	}
	if len(completion.ClaimID) == 0 || len(completion.ClaimID) > fleet.MaxActionIDBytes {
		return Action{}, refusedLocally(op, "claim ID is outside its bound")
	}
	switch {
	case completion.Failed && len(completion.Output) != 0:
		return Action{}, refusedLocally(op, "a failed completion carries no output")
	case completion.Failed && !ValidErrorCode(completion.ErrorCode):
		return Action{}, refusedLocally(op, "error code is not in the closed vocabulary")
	case !completion.Failed && completion.ErrorCode != "":
		return Action{}, refusedLocally(op, "a successful completion carries no error code")
	case !completion.Failed && len(completion.Output) == 0:
		return Action{}, refusedLocally(op, "a successful completion requires output")
	}
	contextValue, err := completion.Context.wire(op, c.clock())
	if err != nil {
		return Action{}, err
	}
	body := struct {
		Context         contextWire     `json:"context"`
		ExpectedVersion uint64          `json:"expected_version"`
		ClaimID         string          `json:"claim_id"`
		Output          json.RawMessage `json:"output,omitempty"`
		ErrorCode       string          `json:"error_code,omitempty"`
		Failed          bool            `json:"failed,omitempty"`
	}{
		Context: contextValue, ExpectedVersion: completion.ExpectedVersion,
		ClaimID: base64.RawURLEncoding.EncodeToString(completion.ClaimID),
		Output:  completion.Output, ErrorCode: completion.ErrorCode,
		Failed: completion.Failed,
	}
	path := actionPath(actionID, "complete")
	var response actionWire
	unconfirmed := func(cause error) error {
		return &DispatchError{Op: op, Kind: DispatchIndeterminate,
			reason: "the report may have committed and the resend could not confirm it",
			cause:  cause}
	}
	_, _, err = c.post(ctx, op, path, body, &response)
	if responseLost(err) || hidesCommittedRecord(err) {
		response = actionWire{}
		_, _, err = c.post(ctx, op, path, body, &response)
		switch {
		case responseLost(err):
			return Action{}, unconfirmed(err)
		case hidesCommittedRecord(err):
			// #492 workaround. A resend answered 400 or 500 is never
			// definite: whatever preceded it — a lost response, or a 400/500
			// that may have been a genuine error committing nothing — this
			// resend may itself have committed the report and then had its
			// record discarded. Read once more through the replay branch;
			// only a record settles it.
			response = actionWire{}
			if _, _, err = c.post(ctx, op, path, body, &response); err != nil {
				return Action{}, unconfirmed(err)
			}
		}
	}
	if err != nil {
		return Action{}, err
	}
	action, err := response.decode()
	if err != nil {
		return Action{}, &DispatchError{Op: op, Kind: DispatchProtocol, reason: err.Error()}
	}
	// The completion route answers 200 only for a fresh terminal write or a
	// replay of one, both at exactly ExpectedVersion+1, and a terminal record
	// does not move past that. Any other version is not this report's record.
	if !bytes.Equal(action.ID, actionID) || !bytes.Equal(action.ClaimID, completion.ClaimID) ||
		action.Version != completion.ExpectedVersion+1 ||
		(action.State != fleet.DispatchSucceeded && action.State != fleet.DispatchFailed) {
		return Action{}, &DispatchError{Op: op, Kind: DispatchProtocol,
			reason: "completion response does not describe this claim's terminal record"}
	}
	if !recordedAsReported(action, completion) {
		return action, &DispatchError{Op: op, Kind: DispatchRecordedOtherwise}
	}
	return action, nil
}

// hidesCommittedRecord is the #492 shape: a 400 or 500 from /complete may be
// a terminal record that was written and then discarded by the handler.
// Workaround for #492; remove when the handler returns the committed record
// it is given alongside an error.
func hidesCommittedRecord(err error) bool {
	var dispatchErr *DispatchError
	return errors.As(err, &dispatchErr) &&
		(dispatchErr.Status == http.StatusInternalServerError ||
			dispatchErr.Status == http.StatusBadRequest)
}

// responseLost is a transport failure or an indeterminate commit: the request
// may have been applied and its answer did not arrive.
func responseLost(err error) bool {
	kind := DispatchKind(err)
	return kind == DispatchTransport || kind == DispatchIndeterminate
}

// recordedAsReported compares the committed record with the report: state,
// error code, and for a success the output, compared as JSON values because
// the explorer canonicalizes what it stores.
func recordedAsReported(action Action, completion Completion) bool {
	if completion.Failed {
		return action.State == fleet.DispatchFailed && action.ErrorCode == completion.ErrorCode
	}
	if action.State != fleet.DispatchSucceeded || action.ErrorCode != "" {
		return false
	}
	var recorded, reported any
	if json.Unmarshal(action.Output, &recorded) != nil ||
		json.Unmarshal(completion.Output, &reported) != nil {
		return false
	}
	return reflect.DeepEqual(recorded, reported)
}

// Resolve reads the gateway's own descriptor, once at startup.
func (c *DispatchClient) Resolve(ctx context.Context, agentID []byte, request RequestContext) (Descriptor, error) {
	const op = "resolve"
	if len(agentID) == 0 {
		return Descriptor{}, refusedLocally(op, "agent ID is required")
	}
	contextValue, err := request.wire(op, c.clock())
	if err != nil {
		return Descriptor{}, err
	}
	var response struct {
		ID           string             `json:"id"`
		Generation   int64              `json:"generation"`
		ExecutorRef  string             `json:"executor_ref"`
		Capabilities []fleet.Capability `json:"capabilities"`
	}
	path := "/api/v1/fleet/agents/" + base64.RawURLEncoding.EncodeToString(agentID) + "/resolve"
	if _, _, err := c.post(ctx, op, path, contextValue, &response); err != nil {
		return Descriptor{}, err
	}
	id, err := base64.RawURLEncoding.DecodeString(response.ID)
	if err != nil || !bytes.Equal(id, agentID) {
		return Descriptor{}, &DispatchError{Op: op, Kind: DispatchProtocol,
			reason: "resolve response names a different descriptor"}
	}
	return Descriptor{
		ID: id, Generation: response.Generation,
		ExecutorRef: response.ExecutorRef, Capabilities: response.Capabilities,
	}, nil
}

func checkActionID(op string, id []byte) error {
	if len(id) == 0 || len(id) > fleet.MaxActionIDBytes {
		return refusedLocally(op, "action ID is outside its bound")
	}
	return nil
}

func actionPath(id []byte, verb string) string {
	return "/api/v1/fleet/actions/" + base64.RawURLEncoding.EncodeToString(id) + "/" + verb
}

// post sends one JSON request and decodes a 2xx answer into out.
func (c *DispatchClient) post(
	ctx context.Context, op, path string, body, out any,
) (http.Header, time.Time, error) {
	token, err := c.credential()
	if err != nil {
		// The credential source's error names a file path or a variable, never
		// the value; it is still not carried, for the same reason transport
		// errors are not.
		return nil, time.Time{}, &DispatchError{Op: op, Kind: DispatchNoCredential}
	}
	if strings.ContainsAny(token, "\r\n") || token == "" {
		return nil, time.Time{}, &DispatchError{Op: op, Kind: DispatchNoCredential}
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, time.Time{}, refusedLocally(op, "request could not be encoded")
	}
	endpoint := *c.base
	endpoint.Path = c.base.Path + path
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(),
		bytes.NewReader(encoded))
	if err != nil {
		return nil, time.Time{}, refusedLocally(op, "request could not be built")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", UserAgent)
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := c.http.Do(request)
	if err != nil {
		return nil, time.Time{}, &DispatchError{
			Op: op, Kind: DispatchTransport, Failure: ClassifyFailure(err), cause: err,
		}
	}
	defer response.Body.Close()
	receivedAt := c.clock()
	data, oversize, readErr := ReadBounded(response.Body, maxDispatchResponseBytes)
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return nil, receivedAt, statusError(op, response, data)
	}
	if readErr != nil {
		return nil, receivedAt, &DispatchError{Op: op, Kind: DispatchTransport,
			Status: response.StatusCode, Failure: ClassifyFailure(readErr), cause: readErr}
	}
	if oversize {
		return nil, receivedAt, &DispatchError{Op: op, Kind: DispatchProtocol,
			Status: response.StatusCode, reason: "response exceeds the client's bound"}
	}
	if err := json.Unmarshal(data, out); err != nil {
		return nil, receivedAt, &DispatchError{Op: op, Kind: DispatchProtocol,
			Status: response.StatusCode, reason: "response is not the expected JSON"}
	}
	return response.Header, receivedAt, nil
}

func statusError(op string, response *http.Response, body []byte) error {
	result := &DispatchError{Op: op, Status: response.StatusCode}
	var envelope struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(body, &envelope) == nil && wellFormedCode(envelope.Code) {
		result.Code = envelope.Code
	}
	switch response.StatusCode {
	case http.StatusNotFound:
		result.Kind = DispatchNotFound
	case http.StatusConflict:
		result.Kind = DispatchConflict
	case http.StatusServiceUnavailable:
		result.Kind = DispatchUnavailable
		if strings.EqualFold(strings.TrimSpace(response.Header.Get(commitOutcomeHeader)),
			commitOutcomeIndeterminate) {
			result.Kind = DispatchIndeterminate
		}
	case http.StatusBadRequest:
		result.Kind = DispatchInvalid
	case http.StatusUnauthorized, http.StatusForbidden:
		result.Kind = DispatchUnauthorized
	case http.StatusGatewayTimeout:
		result.Kind = DispatchDeadline
	default:
		result.Kind = DispatchStatus
	}
	return result
}

// wellFormedCode admits the explorer's short snake-case error codes and
// nothing else, so a proxy in front of the explorer cannot put text into an
// error message through the code field.
func wellFormedCode(code string) bool {
	if code == "" || len(code) > 64 {
		return false
	}
	for i := 0; i < len(code); i++ {
		c := code[i]
		if !(c >= 'a' && c <= 'z' || c == '_') {
			return false
		}
	}
	return true
}
