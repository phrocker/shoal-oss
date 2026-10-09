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
//     indeterminate means the mutation may have committed;
//   - until #505, a 503 *without* that header may also hide a committed
//     write, so the client does not read a bare 503 from /complete or /claim
//     as a clean refusal either (see Complete and Claim).

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
	// mutation may have committed. Claim reports it as DispatchRepull.
	DispatchIndeterminate DispatchErrorKind = "indeterminate"
	// DispatchUnavailable (503 without the header). Claim and Complete never
	// return it for a 503 of their own: until #505 a bare 503 from either may
	// hide a committed write (see DispatchRepull and Complete).
	DispatchUnavailable DispatchErrorKind = "unavailable"
	// DispatchRepull (Claim only: a transport error, any 503, a 502 or a
	// 504): the claim may or may not have been taken. The caller holds no
	// claim — it must not execute — and re-pulls. If the claim did commit,
	// the record reappears on the pull page when its lease lapses, and a
	// completion against it is refused by the claim check, so acting as
	// though nothing is held is safe. The original error is its cause. The
	// bare-503 case is interim until #505; see Claim.
	DispatchRepull       DispatchErrorKind = "repull"
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
	DispatchRecordedOtherwise: true, DispatchRepull: true,
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
	// Effected is the volume a failed record says escaped before the failure
	// (#427), zero when none was reported. Read back on a resend, never
	// re-derived: the field is write-once and compared on replay.
	Effected fleet.EffectedVolume
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
	Effected        *effectedWire       `json:"effected,omitempty"`
}

// effectedWire is the /complete route's volume object, accepted on a failed
// completion and returned on the record.
type effectedWire struct {
	Bytes  int64 `json:"bytes"`
	Chunks int64 `json:"chunks,omitempty"`
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
	if w.Effected != nil {
		action.Effected = fleet.EffectedVolume{
			Bytes: w.Effected.Bytes, Chunks: w.Effected.Chunks,
		}
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
		// Every answer after which the claim may have committed — a
		// transport error, any 503, a 502 or 504 (answerLost) — is a
		// re-pull signal, never a definite failure, and the caller contract
		// is unchanged: Claim returns no Action, so the caller holds no claim
		// and executes nothing. If the claim did commit, it lapses at its
		// lease and reappears on the pull page; a completion against a claim
		// the worker does not hold is refused. The original error, with its
		// kind, is kept as the cause.
		//
		// The bare-503 case is interim until #505: today ErrActionCommitted
		// (the claim was durably written and only its publication failed)
		// reaches the wire as a bare 503, indistinguishable from a clean
		// refusal. After #505 a header-less 503 is a clean pre-write refusal
		// and narrows back to DispatchUnavailable; the other cases stay.
		if answerLost(err) {
			var dispatchErr *DispatchError
			errors.As(err, &dispatchErr)
			return Action{}, &DispatchError{Op: op, Kind: DispatchRepull,
				Status: dispatchErr.Status, Code: dispatchErr.Code,
				Failure: dispatchErr.Failure, cause: err}
		}
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
	// Effected is how much of an irreversible egress happened before a
	// failure (#427): an upper bound on bytes handed to the transport, "at
	// most N bytes may have reached the target", never a receipt. Zero when
	// nothing left, and then omitted from the wire. Valid only on a failure,
	// and only for an action declaring egresses-content; on any other action
	// the explorer adjudicates it as invalid_executor_effected, which
	// Complete returns as DispatchRecordedOtherwise.
	//
	// The caller fixes it once, when its send has completed or failed, and
	// the resend below carries the identical value: the record is write-once
	// and the replay branch compares it, so a different number is a different
	// report.
	Effected fleet.EffectedVolume
}

// Complete reports the outcome under the claim. Evidence is never sent: the
// gateway is the first executor that is not trusted, and an evidence anchor is
// a claim about the corpus it never read.
//
// A lost response is recovered by resending the identical body once: on a
// transport error, any 503, a 502 or 504 (a proxy can answer either after
// the explorer processed the request), or a 2xx whose body does not decode or
// does not describe this report's terminal record (the route answered
// success, so something committed), the report may have committed, and the
// completion route's replay branch answers a resend with the committed record.
// The resend on a genuinely lost or unreadable answer is permanent.
//
// Every 503 counts, header or not — interim until #505. ErrExecutionAmbiguous
// and ErrActionCommitted (a durable write whose publication failed) arrive
// today as bare 503s without Shoal-Commit-Outcome: indeterminate, so a bare
// 503 may hide a committed write. ErrRecordingUnavailable is also a bare 503;
// on the dispatch surface every audit precedes its store write, so it is in
// fact a clean refusal, but nothing on the wire distinguishes it and the
// client never reads error-message text to try. (The "committed" in
// explorer.MarkCommittedInteraction refers to an interaction record, not to
// the action, and is not part of this.) Once #505 marks the genuinely
// indeterminate sentinels, a header-less 503 means a clean refusal before any
// write, and the trigger narrows back to the header; the resend itself stays.
//
// After a possibly-committed first answer, Complete returns a record or
// DispatchIndeterminate and nothing else. It never returns the first
// attempt's status — that status describes a request whose outcome the resend
// was sent to learn — and it never returns a refusal from the resend as
// definite either:
//
//   - a resend answered by a transport error, any 503, or a 502 or 504 is
//     DispatchIndeterminate;
//   - a resend answered by anything else that is not a record — 409, 404, a
//     400/500, a 2xx that is not this report's record, or any other
//     status — is followed by one third read
//     through the replay branch, and anything but a record is
//     DispatchIndeterminate.
//
// Why a 409 (or 404) is not definite here: ErrExecutionAmbiguous says the
// service cannot tell whether its store write landed, so that write may still
// be in flight when the resend reads the record. The resend can then see the
// claim at the old version with its lease lapsed and answer ErrClaimLost
// (409) for a report that commits a moment later. The third read gives such a
// write the chance to surface through the replay branch; if it does not, the
// honest answer is still "may have committed", never "refused". With no
// possibly-committed answer before it, a first-attempt 409 or 404 is definite
// and returned as it is.
//
// A resend answered 400 or 500 is not definite either (#492): the first
// attempt may have committed, or — if the first answer was a genuine error
// that committed nothing — the resend may have committed and had its record
// discarded by a server before #547. So the record is read a third time
// through the replay branch, and anything but a record is
// DispatchIndeterminate.
//
// The 400 and 500 triggers exist for servers before #547 (#492). There, the
// /complete handler discarded the committed record of a reported failure and
// of a success whose output the explorer refuses (recorded as failed,
// invalid_executor_output) and answered 500 and 400 — a durably written
// terminal record, indistinguishable from a refusal that wrote nothing. A
// current server answers both with 200 and the committed record, so they
// complete in one request. A gateway cannot tell which server it talks to, so
// the triggers stay; the cost is that a genuine 400 or 500 refusal from a
// current server still takes the three-request path and ends
// DispatchIndeterminate.
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
	case !completion.Failed && !completion.Effected.Zero():
		return Action{}, refusedLocally(op, "a successful completion carries no effected volume")
	case completion.Effected.Bytes < 0 || completion.Effected.Chunks < 0 ||
		completion.Effected.Bytes > fleet.MaxEffectedBytes ||
		completion.Effected.Chunks > fleet.MaxEffectedChunks:
		return Action{}, refusedLocally(op, "effected volume is outside its bound")
	case completion.Effected.Chunks > 0 && completion.Effected.Bytes == 0:
		// The explorer would adjudicate this rather than refuse it, and the
		// record would then say the volume is unknown when the worker meant
		// "nothing": a chunk that left carried something.
		return Action{}, refusedLocally(op, "effected volume counts chunks but no bytes")
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
		Effected        *effectedWire   `json:"effected,omitempty"`
	}{
		Context: contextValue, ExpectedVersion: completion.ExpectedVersion,
		ClaimID: base64.RawURLEncoding.EncodeToString(completion.ClaimID),
		Output:  completion.Output, ErrorCode: completion.ErrorCode,
		Failed: completion.Failed,
	}
	if !completion.Effected.Zero() {
		// Built once with the body, so every attempt below sends this value.
		body.Effected = &effectedWire{
			Bytes: completion.Effected.Bytes, Chunks: completion.Effected.Chunks,
		}
	}
	path := actionPath(actionID, "complete")
	unconfirmed := func(cause error) error {
		return &DispatchError{Op: op, Kind: DispatchIndeterminate,
			reason: "the report may have committed and the resend could not confirm it",
			cause:  cause}
	}
	// attempt sends the body and returns this report's terminal record, or an
	// error. A 2xx whose body does not decode or does not describe this
	// claim's terminal record is DispatchProtocol, and only a 2xx produces
	// that kind here.
	attempt := func() (Action, error) {
		var response actionWire
		if _, _, err := c.post(ctx, op, path, body, &response); err != nil {
			return Action{}, err
		}
		action, err := response.decode()
		if err != nil {
			return Action{}, &DispatchError{Op: op, Kind: DispatchProtocol, reason: err.Error()}
		}
		// The completion route answers 200 only for a fresh terminal write or
		// a replay of one, both at exactly ExpectedVersion+1, and a terminal
		// record does not move past that. Any other version is not this
		// report's record.
		if !bytes.Equal(action.ID, actionID) || !bytes.Equal(action.ClaimID, completion.ClaimID) ||
			action.Version != completion.ExpectedVersion+1 ||
			(action.State != fleet.DispatchSucceeded && action.State != fleet.DispatchFailed) {
			return Action{}, &DispatchError{Op: op, Kind: DispatchProtocol,
				reason: "completion response does not describe this claim's terminal record"}
		}
		return action, nil
	}
	action, err := attempt()
	if mayHaveCommitted(err) || hidesCommittedRecord(err) {
		action, err = attempt()
		switch {
		case err == nil:
		case answerLost(err):
			return Action{}, unconfirmed(err)
		default:
			// Not a record, and not definite after a possibly-committed
			// first answer. For a 400/500 (pre-#547 servers): whatever
			// preceded it — a lost response, or a 400/500 that may have been
			// a genuine error committing nothing — this resend may itself
			// have committed the report and then had its record discarded.
			// For a 409, 404 or any other refusal: the first attempt's write
			// may still be in flight (ErrExecutionAmbiguous). For a 2xx that
			// is not this report's record: the route answered success, so
			// something committed. Read once more through the replay branch;
			// only a record settles it.
			if action, err = attempt(); err != nil {
				return Action{}, unconfirmed(err)
			}
		}
	}
	if err != nil {
		return Action{}, err
	}
	if !recordedAsReported(action, completion) {
		return action, &DispatchError{Op: op, Kind: DispatchRecordedOtherwise}
	}
	return action, nil
}

// hidesCommittedRecord is the #492 shape: from a server before #547, a 400 or
// 500 from /complete may be a terminal record that was written and then
// discarded by the handler. A current server answers those outcomes 200 with
// the record, but the gateway cannot tell the generations apart, so this
// stays.
func hidesCommittedRecord(err error) bool {
	var dispatchErr *DispatchError
	return errors.As(err, &dispatchErr) &&
		(dispatchErr.Status == http.StatusInternalServerError ||
			dispatchErr.Status == http.StatusBadRequest)
}

// mayHaveCommitted is an answer after which the report may have committed:
// a lost answer (answerLost), or a 2xx whose body does not decode or does not
// describe this report's terminal record. The route answered success, so a
// write or a replay happened; the identical resend reads the record back
// through the replay branch rather than returning DispatchProtocol alone.
func mayHaveCommitted(err error) bool {
	return answerLost(err) || DispatchKind(err) == DispatchProtocol
}

// answerLost is a request that may have been applied whose answer did not
// arrive, or arrived without saying what happened:
//
//   - a transport failure, or an indeterminate 503 (permanent);
//   - a 502 or 504, which a proxy in front of the explorer can answer after
//     the explorer processed the request (permanent);
//   - a bare 503 (DispatchUnavailable) — interim until #505: today
//     ErrExecutionAmbiguous and ErrActionCommitted reach the wire as bare
//     503s. After #505 a header-less 503 is a clean pre-write refusal and
//     this case is dropped.
func answerLost(err error) bool {
	var dispatchErr *DispatchError
	if !errors.As(err, &dispatchErr) {
		return false
	}
	switch dispatchErr.Kind {
	case DispatchTransport, DispatchIndeterminate, DispatchUnavailable:
		return true
	}
	return dispatchErr.Status == http.StatusBadGateway ||
		dispatchErr.Status == http.StatusGatewayTimeout
}

// recordedAsReported compares the committed record with the report: state,
// error code, for a failure the effected volume, and for a success the output,
// compared as JSON values because the explorer canonicalizes what it stores.
//
// The volume is read back from the record and compared, never assumed: a
// record that kept a different number (or dropped it, as an adjudicated
// invalid_executor_effected does) is recorded otherwise.
func recordedAsReported(action Action, completion Completion) bool {
	if completion.Failed {
		return action.State == fleet.DispatchFailed &&
			action.ErrorCode == completion.ErrorCode &&
			action.Effected == completion.Effected
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
