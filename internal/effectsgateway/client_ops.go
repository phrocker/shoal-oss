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
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
	"unicode"
	"unicode/utf8"

	attestationapi "github.com/phrocker/shoal-oss/pkg/attestation/api"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/sdk"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// CorrelationIDHeader mirrors the explorer's Shoal-Correlation-ID (#527),
// restated rather than imported from the command that defines it.
const CorrelationIDHeader = "Shoal-Correlation-ID"

// pollCorrelationPrefix marks a correlation the gateway minted for one poll.
const pollCorrelationPrefix = "gw-poll-"

// correlationHeader checks a correlation ID the way the explorer will, before
// anything is sent, and returns the header value. Required: every request
// that calls it is about an action whose record carries one, or is a poll
// that has just minted one.
func correlationHeader(op string, id []byte) (string, error) {
	if len(id) == 0 {
		return "", refusedLocally(op,
			"correlation ID is required: take it from the action record")
	}
	if len(id) > fleet.MaxActionIDBytes ||
		interaction.ValidateCorrelationID(shoal.ID(id)) != nil {
		// The validator's text is not carried: it is fixed, but the rule
		// for this error type is that only closed text is formatted.
		return "", refusedLocally(op,
			"correlation ID must be bounded, valid UTF-8, printable and without spaces")
	}
	return string(id), nil
}

// newPollCorrelation mints a fresh correlation for one poll.
func newPollCorrelation(random io.Reader) ([]byte, error) {
	if random == nil {
		random = rand.Reader
	}
	nonce := make([]byte, 16)
	if _, err := io.ReadFull(random, nonce); err != nil {
		return nil, errors.New("poll correlation: random source failed")
	}
	return []byte(pollCorrelationPrefix + hex.EncodeToString(nonce)), nil
}

// ExtendRequest renews the lease of a claim this worker holds.
type ExtendRequest struct {
	// Context's correlation is the action record's (Action.Correlate).
	Context         RequestContext
	ExpectedVersion uint64
	ClaimID         []byte
	// ClaimFence is the fence from the claim response. The route does not
	// take it; an extension never moves the fence, and the client refuses a
	// response under any other fence as not describing this claim.
	ClaimFence uint64
	// Lease is the silence budget requested from the explorer's now, at most
	// fleet.MaxActionClaimTTL. The explorer clamps the new end to the
	// action's deadline, so the end granted is read from the response.
	Lease time.Duration
}

// Extend renews a live claim (#430) and returns the record as renewed.
//
// The lease end to act on is the returned Action.ClaimLeaseUntil — the
// explorer's clamped end, at most the action's deadline — never now plus the
// requested lease. The fence is unchanged, so Complete and ReportAmbiguity
// keep using the claim's.
//
// A definite refusal is DispatchFenceLost. Nothing was written, and the
// claim, if still held, runs to the lease end it already had and no further:
//
//   - 404: the caller no longer holds this claim on this ref — it lapsed and
//     was re-claimed, the descriptor was rebound to another executor ref
//     (#391: an extension needs the current and the claimed ref), or the
//     action is gone. Completion stays possible after a rebind.
//   - 409: the lease or the deadline has passed (ErrClaimLost), the record's
//     version moved, or the attestation gate refused the renewal
//     (ErrAttestationRequired). The explorer answers all three with the one
//     code and the client never reads message text, so the status is kept
//     on the error: a worker whose action requires attestation re-attests
//     and extends once more before treating it as lost.
//
// A lost answer is handled as Complete handles one: a transport error, any
// 503 (until #505 a bare 503 may hide ErrActionCommitted), a 502 or 504, or a
// 2xx that does not describe this renewal, may have committed, and the
// identical body is resent once. A record from the resend is returned. Any
// other answer is DispatchIndeterminate — the lease may or may not have
// moved, and the version may have advanced. There is no replay branch on this
// route, so a resend after a committed first attempt sees the version moved
// and answers 409; that 409 is never read as a refusal. On
// DispatchIndeterminate the worker acts on the lease end it had before, and
// completes with the fence, which does not compare the version.
func (c *DispatchClient) Extend(ctx context.Context, actionID []byte, request ExtendRequest) (Action, error) {
	const op = "extend"
	if err := checkActionID(op, actionID); err != nil {
		return Action{}, err
	}
	if request.ExpectedVersion == 0 {
		return Action{}, refusedLocally(op, "expected version is required")
	}
	if len(request.ClaimID) == 0 || len(request.ClaimID) > fleet.MaxActionIDBytes {
		return Action{}, refusedLocally(op, "claim ID is outside its bound")
	}
	if request.ClaimFence == 0 {
		return Action{}, refusedLocally(op, "claim fence is required: take it from the claim response")
	}
	if request.Lease <= 0 || request.Lease > fleet.MaxActionClaimTTL {
		return Action{}, refusedLocally(op, "lease must be positive and at most "+
			fleet.MaxActionClaimTTL.String())
	}
	correlation, err := correlationHeader(op, request.Context.CorrelationID)
	if err != nil {
		return Action{}, err
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
	path := actionPath(actionID, "extend")
	attempt := func() (Action, error) {
		var response actionWire
		if _, _, err := c.post(ctx, op, path, correlation, body, &response); err != nil {
			return Action{}, err
		}
		action, err := response.decode()
		if err != nil {
			return Action{}, &DispatchError{Op: op, Kind: DispatchProtocol, reason: err.Error()}
		}
		// An extension writes exactly ExpectedVersion+1 and keeps the claim,
		// its fence and its state. The end it grants is after nothing in
		// particular the client can check against its own clock, but it is
		// never past the action's deadline.
		if !bytes.Equal(action.ID, actionID) || !bytes.Equal(action.ClaimID, request.ClaimID) ||
			action.ClaimFence != request.ClaimFence ||
			action.Version != request.ExpectedVersion+1 ||
			action.State != fleet.DispatchClaimed || action.ClaimLeaseUntil.IsZero() ||
			(!action.Deadline.IsZero() && action.ClaimLeaseUntil.After(action.Deadline)) {
			return Action{}, &DispatchError{Op: op, Kind: DispatchProtocol,
				reason: "extend response does not describe this claim's renewal"}
		}
		return action, nil
	}
	action, err := attempt()
	if mayHaveCommitted(err) {
		resent, resendErr := attempt()
		if resendErr == nil {
			return resent, nil
		}
		return Action{}, &DispatchError{Op: op, Kind: DispatchIndeterminate,
			reason: "the renewal may have committed and the resend could not confirm it",
			cause:  resendErr}
	}
	if err != nil {
		var dispatchErr *DispatchError
		if errors.As(err, &dispatchErr) &&
			(dispatchErr.Kind == DispatchNotFound || dispatchErr.Kind == DispatchConflict) {
			return Action{}, &DispatchError{Op: op, Kind: DispatchFenceLost,
				Status: dispatchErr.Status, Code: dispatchErr.Code, cause: err}
		}
		return Action{}, err
	}
	return action, nil
}

// AmbiguityReport is what a worker attempted when it can no longer report
// the outcome through Complete (#438).
type AmbiguityReport struct {
	// Context's correlation is the action record's (Action.Correlate).
	Context RequestContext
	// ClaimFence is the fence of the attempt being reported: the one this
	// worker held, whether or not it still does.
	ClaimFence uint64
	// Outcome is fleet.AmbiguityRequestNotSent, AmbiguityEffectObserved or
	// AmbiguityOutcomeUnknown.
	Outcome fleet.AmbiguityOutcome
	// Target and Reference are optional, bounded at
	// fleet.MaxAmbiguityTargetBytes and fleet.MaxAmbiguityReferenceBytes,
	// and must be valid UTF-8 made only of printable characters — the
	// explorer's rule, applied here so a report is refused before it is sent
	// rather than lost to a 400. Reference is the handle the target returned,
	// the thing an operator takes to the other system.
	Target    string
	Reference string
}

func (r AmbiguityReport) check(op string) error {
	if r.ClaimFence == 0 {
		return refusedLocally(op, "claim fence is required")
	}
	switch r.Outcome {
	case fleet.AmbiguityRequestNotSent, fleet.AmbiguityEffectObserved,
		fleet.AmbiguityOutcomeUnknown:
	default:
		return refusedLocally(op, "outcome is not in the closed vocabulary")
	}
	if len(r.Target) > fleet.MaxAmbiguityTargetBytes || !printableAmbiguityText(r.Target) {
		return refusedLocally(op, "target must be at most "+
			strconv.Itoa(fleet.MaxAmbiguityTargetBytes)+" bytes of printable UTF-8")
	}
	if len(r.Reference) > fleet.MaxAmbiguityReferenceBytes || !printableAmbiguityText(r.Reference) {
		return refusedLocally(op, "reference must be at most "+
			strconv.Itoa(fleet.MaxAmbiguityReferenceBytes)+" bytes of printable UTF-8")
	}
	return nil
}

// printableAmbiguityText is the explorer's validateAmbiguityText: valid
// UTF-8 and unicode.IsPrint for every codepoint, which admits the ASCII space
// and refuses every control, format (bidi overrides, zero-width) and
// unassigned codepoint. A parity test holds the two to the same verdicts.
func printableAmbiguityText(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, codepoint := range value {
		if !unicode.IsPrint(codepoint) {
			return false
		}
	}
	return true
}

// ReportAmbiguity records a lost-fence report and returns the record that
// holds it. It always appends (expected_version 0): the caller this route
// exists for cannot learn the current version, and the fence is the
// invariant the explorer asserts.
//
// Four answers, and only the first two are success:
//
//   - accepted: the record now carries this report.
//   - an identical replay accepted (#542): the explorer answers with the
//     record that already carries it, which reads the same as accepted.
//   - refused or not found: DispatchAmbiguityUnrecorded, matching
//     ErrAmbiguityUnrecorded. The explorer answered definitely — a 400 (the
//     per-record report budget is spent, which a co-tenant can do, #514), a
//     404 (this principal's holder entry is gone or never existed, or the
//     action is), or a 401/403 — and nothing about this attempt is on the
//     record. Per #514 this is never "nothing to report": the caller writes
//     the report to its local unrecorded log.
//   - indeterminate: DispatchIndeterminate. The answer was lost (a transport
//     error, any 5xx, or a 2xx that is not a record carrying this report)
//     and an identical resend — safe, since an identical report replays —
//     did not confirm it either. The caller logs it as unrecorded too.
//
// A 409 means a concurrent write moved the record between the explorer's read
// and its compare-and-set, so nothing was written; the identical report is
// resent once, and a definite refusal of the resend is unrecorded.
func (c *DispatchClient) ReportAmbiguity(ctx context.Context, actionID []byte, report AmbiguityReport) (Action, error) {
	const op = "ambiguity"
	if err := checkActionID(op, actionID); err != nil {
		return Action{}, err
	}
	if err := report.check(op); err != nil {
		return Action{}, err
	}
	correlation, err := correlationHeader(op, report.Context.CorrelationID)
	if err != nil {
		return Action{}, err
	}
	contextValue, err := report.Context.wire(op, c.clock())
	if err != nil {
		return Action{}, err
	}
	body := struct {
		Context         contextWire `json:"context"`
		ExpectedVersion uint64      `json:"expected_version"`
		ClaimFence      uint64      `json:"claim_fence"`
		Outcome         string      `json:"outcome"`
		Target          string      `json:"target,omitempty"`
		Reference       string      `json:"reference,omitempty"`
	}{
		Context: contextValue, ExpectedVersion: 0, ClaimFence: report.ClaimFence,
		Outcome: string(report.Outcome), Target: report.Target, Reference: report.Reference,
	}
	path := actionPath(actionID, "ambiguity")
	attempt := func() (Action, error) {
		var response actionWire
		if _, _, err := c.post(ctx, op, path, correlation, body, &response); err != nil {
			return Action{}, err
		}
		action, err := response.decode()
		if err != nil {
			return Action{}, &DispatchError{Op: op, Kind: DispatchProtocol, reason: err.Error()}
		}
		if !bytes.Equal(action.ID, actionID) || !carriesReport(action, report) {
			return Action{}, &DispatchError{Op: op, Kind: DispatchProtocol,
				reason: "ambiguity response is not a record carrying this report"}
		}
		return action, nil
	}
	unrecorded := func(cause error) error {
		var dispatchErr *DispatchError
		errors.As(cause, &dispatchErr)
		return &DispatchError{Op: op, Kind: DispatchAmbiguityUnrecorded,
			Status: dispatchErr.Status, Code: dispatchErr.Code, cause: cause}
	}
	action, err := attempt()
	switch {
	case err == nil:
		return action, nil
	case ambiguityRefused(err):
		return Action{}, unrecorded(err)
	case DispatchKind(err) == DispatchConflict:
		// Nothing was written; a definite answer to the resend stands.
		action, err = attempt()
		switch {
		case err == nil:
			return action, nil
		case ambiguityRefused(err) || DispatchKind(err) == DispatchConflict:
			return Action{}, unrecorded(err)
		}
	default:
		// Possibly committed. Only a record from the resend settles it.
		if action, err = attempt(); err == nil {
			return action, nil
		}
	}
	return Action{}, &DispatchError{Op: op, Kind: DispatchIndeterminate,
		reason: "the report may have been recorded and the resend could not confirm it",
		cause:  err}
}

// ambiguityRefused is a definite answer from the explorer that wrote nothing.
func ambiguityRefused(err error) bool {
	var dispatchErr *DispatchError
	if !errors.As(err, &dispatchErr) || dispatchErr.Status == 0 {
		return false
	}
	switch dispatchErr.Status {
	case http.StatusBadRequest, http.StatusNotFound,
		http.StatusUnauthorized, http.StatusForbidden:
		return dispatchErr.Kind != DispatchProtocol
	}
	return false
}

// carriesReport reports whether the record holds this report. The reporter's
// identity is the explorer's to record, and is not compared.
func carriesReport(action Action, report AmbiguityReport) bool {
	for _, recorded := range action.AmbiguityReports {
		if recorded.ClaimFence == report.ClaimFence && recorded.Outcome == report.Outcome &&
			recorded.Target == report.Target && recorded.Reference == report.Reference {
			return true
		}
	}
	return false
}

// PresentAttestation presents the operator-signed statement in statementFile
// for this client's executor ref, under the idempotency key in keyFile, and
// returns the attestation's expiry (the receipt's expires_at) (#528).
//
// Both files are read on every call, so a statement the operator's signer
// rotates is picked up by the next presentation. The key file's bytes are the
// key exactly as the statement's nonce binds it: nothing is trimmed. A claim
// or extension needs an attestation whose expiry is at or after the lease end
// it would grant, so the worker re-presents before it extends past this.
//
// It is built on sdk.Attestation(), with this client's HTTP client and
// credential: the explorer requires the presenting principal to be the one
// that claims, and the ref to be the credential's binding. The opaque
// verification refusal is DispatchAttestationRefused. Presenting the same
// statement again is idempotent, so a lost or indeterminate answer is
// presented once more; if that is lost too, the result is
// DispatchIndeterminate.
func (c *DispatchClient) PresentAttestation(ctx context.Context, statementFile, keyFile string) (time.Time, error) {
	const op = "attest"
	if c.executorRef == "" {
		return time.Time{}, refusedLocally(op, "presentation needs the executor ref this gateway is bound to")
	}
	statement, err := readBoundedFile(statementFile, attestationapi.MaxReportBytes)
	if err != nil {
		return time.Time{}, refusedLocally(op, "statement file: "+err.Error())
	}
	key, err := readBoundedFile(keyFile, attestationapi.MaxIdempotencyKeyBytes)
	if err != nil {
		return time.Time{}, refusedLocally(op, "key file: "+err.Error())
	}
	if _, err := attestationapi.NewRequest(c.executorRef, key, statement); err != nil {
		return time.Time{}, refusedLocally(op, "the presentation is outside the route's bounds")
	}
	client, err := sdk.New(sdk.Config{
		BaseURL: c.base.String(), HTTPClient: c.http,
		Token: func(context.Context) (string, error) { return c.credential() },
	})
	if err != nil {
		return time.Time{}, refusedLocally(op, "the explorer URL cannot address the attestation route")
	}
	present := func() (attestationapi.Receipt, error) {
		return client.Attestation().Present(ctx, c.executorRef, key, statement)
	}
	receipt, err := present()
	if err != nil && attestationLost(err) {
		if receipt, err = present(); err != nil {
			if attestationLost(err) {
				return time.Time{}, &DispatchError{Op: op, Kind: DispatchIndeterminate,
					reason: "the presentation may have been recorded and the retry could not confirm it",
					cause:  err}
			}
		}
	}
	if err != nil {
		return time.Time{}, attestationError(op, err)
	}
	return receipt.ExpiresAt, nil
}

// attestationLost is an answer after which the presentation may have been
// recorded: a transport error, an indeterminate answer, any 5xx, or a 2xx the
// client could not read.
func attestationLost(err error) bool {
	if errors.Is(err, attestationapi.ErrRefused) {
		return false
	}
	var httpErr *attestationapi.HTTPError
	if !errors.As(err, &httpErr) {
		// The client's own validation errors are refused before sending and
		// were checked above; anything else here is the transport.
		return true
	}
	return httpErr.Indeterminate || httpErr.Status >= 500 ||
		(httpErr.Status >= 200 && httpErr.Status <= 299)
}

func attestationError(op string, err error) error {
	if errors.Is(err, attestationapi.ErrRefused) {
		return &DispatchError{Op: op, Kind: DispatchAttestationRefused,
			Status: http.StatusUnauthorized, cause: err}
	}
	var httpErr *attestationapi.HTTPError
	if !errors.As(err, &httpErr) {
		return &DispatchError{Op: op, Kind: DispatchTransport, Failure: ClassifyFailure(err), cause: err}
	}
	result := &DispatchError{Op: op, Status: httpErr.Status, cause: err}
	if wellFormedCode(httpErr.Code) {
		result.Code = httpErr.Code
	}
	switch httpErr.Status {
	case http.StatusNotFound:
		result.Kind = DispatchNotFound
	case http.StatusConflict:
		result.Kind = DispatchConflict
	case http.StatusBadRequest:
		result.Kind = DispatchInvalid
	case http.StatusUnauthorized, http.StatusForbidden:
		result.Kind = DispatchUnauthorized
	default:
		result.Kind = DispatchStatus
	}
	return result
}

// readBoundedFile reads at most limit bytes, refusing an empty or larger
// file. Its errors name neither the path nor the contents.
func readBoundedFile(path string, limit int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot be read")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(limit)+1))
	if err != nil {
		return nil, errors.New("cannot be read")
	}
	if len(data) == 0 || len(data) > limit {
		return nil, errors.New("is empty or exceeds its bound")
	}
	return data, nil
}
