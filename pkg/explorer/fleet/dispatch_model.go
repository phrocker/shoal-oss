// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	MaxActionIDBytes       = 256
	MaxActionPayloadBytes  = 1 << 20
	MaxActionOutputBytes   = 1 << 20
	MaxActionEvidence      = 256
	MaxActionErrorBytes    = 1024
	MaxActionClaimTTL      = 5 * time.Minute
	MaxActionDeadline      = 24 * time.Hour
	MaxDispatchListResults = 256
)

var (
	ErrActionNotFound       = errors.New("fleet dispatch: action not found")
	ErrActionConflict       = errors.New("fleet dispatch: action conflict")
	ErrClaimLost            = errors.New("fleet dispatch: claim lost")
	ErrActionTerminal       = errors.New("fleet dispatch: action is terminal")
	ErrExecutionAmbiguous   = errors.New("fleet dispatch: execution may have occurred")
	ErrActionCommitted      = errors.New("fleet dispatch: action state committed")
	ErrRecordingUnavailable = errors.New("fleet dispatch: recording is unavailable")
)

type DispatchState string

const (
	DispatchQueued    DispatchState = "queued"
	DispatchClaimed   DispatchState = "claimed"
	DispatchSucceeded DispatchState = "succeeded"
	DispatchFailed    DispatchState = "failed"
	DispatchCanceled  DispatchState = "canceled"
)

func (s DispatchState) terminal() bool {
	return s == DispatchSucceeded || s == DispatchFailed || s == DispatchCanceled
}

// EvidenceRef is a complete, bounded evidence anchor returned by a trusted
// executor. It preserves the same immutable identity as interaction evidence.
type EvidenceRef struct {
	AnchorID   shoal.ID
	Kind       interaction.EvidenceKind
	Citation   document.Citation
	NodeIDs    []shoal.ID
	EdgeIDs    []shoal.ID
	Assertions []interaction.AssertionReference
	Visibility []string
}

// ActionRecord is the durable source of truth for one dispatch.
type ActionRecord struct {
	ID                             []byte
	IdempotencyKey                 []byte
	Version                        uint64
	State                          DispatchState
	AgentID                        shoal.ID
	AgentGeneration                int64
	Capability                     string
	Action                         string
	SourceID                       []byte
	PolicyID                       []byte
	ObjectID                       shoal.ID
	Input                          json.RawMessage
	Output                         json.RawMessage
	ErrorCode                      string
	Subject                        shoal.ID
	Actor                          shoal.ID
	ClientID                       shoal.ID
	OnBehalfOf                     []shoal.ID
	AuthorizationFingerprint       auth.Fingerprint
	PolicyGeneration               int64
	AuthorizationExpiresAt         time.Time
	ExecutionFingerprint           auth.Fingerprint
	ExecutionPolicyGeneration      int64
	ExecutionExpiresAt             time.Time
	AuthorizedOperations           []auth.Operation
	RequestID                      shoal.ID
	CorrelationID                  shoal.ID
	TransitionRequestID            shoal.ID
	TransitionCorrelationID        shoal.ID
	CancelAuthorizationFingerprint auth.Fingerprint
	CancelAuthorizationExpiresAt   time.Time
	Reason                         interaction.Reason
	Deadline                       time.Time
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
	ClaimID                        []byte
	// The claimant's own principal chain, recorded when the claim is taken
	// and compared when it is reported on.
	//
	// ClaimID alone cannot do this job. It is caller-supplied with no entropy
	// requirement — validateOpaque accepts one to MaxActionIDBytes bytes — and
	// it is published: Status returns it to every co-principal, and Pull
	// returns a claimed record whose lease has lapsed, ClaimID included, to
	// every principal authorized to execute the descriptor. So it identifies
	// *a* claim, not *who* holds it.
	//
	// That distinction did not matter while claiming was gated on
	// sameActionPrincipal, because the claimant was by construction the
	// enqueuer and the enqueuer's chain was already on the record. #437 made
	// the claimant a different principal, at which point the record stopped
	// carrying any statement of who holds the claim.
	//
	// Compared rather than the claim-time ExecutionFingerprint on purpose.
	// That fingerprint covers policyGeneration, the operation set, the service
	// role and the selected ontology as well as identity, so a routine policy
	// reload would change it for every worker at once and strand every
	// in-flight claim — each one mid-effect, with no way to report. Identity
	// is the question being asked, so identity is what is stored.
	// TransitionOperation is the operation that authorized the transition this
	// record is currently in, as distinct from the operations its enqueuer
	// held.
	//
	// AuthorizedOperations cannot answer this. It accumulates across
	// transitions and, worse, it accumulates a *claim* rather than a fact:
	// applyClaim merges decisionOperations(decision, OperationInvoke), and
	// decisionOperations does not consult the decision at all — it returns the
	// operation it was handed. So before #437, when every claim was authorized
	// under invoke, the field happened to be true; after it, a record claimed
	// under execute still asserts invoke.
	//
	// That mattered beyond the audit trail. The event publisher selects an
	// operation per event kind and then authorizes the publishing decision
	// against it, so a claim authorized under execute was published under
	// invoke — which the claimant does not hold — and the publication failed.
	// A failed publication is reported as ErrActionCommitted, so the write
	// landed and the worker was told to reconcile. Recording the real
	// operation is what lets the publisher ask the right question.
	TransitionOperation  auth.Operation
	ClaimantSubject      shoal.ID
	ClaimantActor        shoal.ID
	ClaimantClientID     shoal.ID
	ClaimantOnBehalfOf   []shoal.ID
	ClaimFence           uint64
	ClaimLease           time.Duration
	ClaimLeaseUntil      time.Time
	CancelKey            []byte
	ExecutorKey          []byte
	EvidenceSnapshotID   shoal.ID
	EvidenceSnapshotAsOf time.Time
	Evidence             []EvidenceRef
	EffectPossible       bool
	// AdmittedEffects is the effect set a pre-call admission declared it was
	// about to perform. Empty on an action that was dispatched rather than
	// admitted.
	//
	// It is part of the record's enqueue identity, not decoration. Without it
	// the durable record says only that some admission was granted under this
	// token, and a retry carrying a different declaration is indistinguishable
	// from the original — so a caller could be admitted for one thing and hold
	// a token that reads as permission for another.
	AdmittedEffects Effects
	// AdmittedDisclosures is a digest over the canonical corpus references an
	// admission declared its payload would carry, or nil when it declared none.
	//
	// A digest rather than the references themselves. The references are corpus
	// identities the caller supplied, and copying them into a dispatch record
	// would put a caller's claimed reading list somewhere the team overview
	// reads. What the record has to do is refuse a retry that changes them,
	// which needs only equality.
	AdmittedDisclosures []byte
	// AdmittedObligation is the obligation returned with the grant, as a bitmap
	// over the canonical order of the declared references. Bit i set means the
	// reference at index i must be withheld.
	//
	// The obligation is the decision, and a decision that is recomputed is not
	// the decision that was made. The co-occurrence budget it comes from is
	// windowed and moves as an identity reads, so recomputing on a retry can
	// return a weaker obligation for a token that is already live — a caller
	// could replay its way out of a restriction — and a restrictor that is
	// briefly unreachable would make an already-granted admission impossible to
	// recover at all.
	//
	// Indices rather than identities, for the reason AdmittedDisclosures is a
	// digest: positions disclose nothing without the list they index, and the
	// caller supplies that list again on the retry, where the digest proves it
	// is the same one.
	AdmittedObligation []byte
	// AdmittedIdentityScheme records which identity scheme produced this
	// admission's durable key. Zero means the superseded one, where the key was
	// the caller's own name; AdmittedIdentitySchemeDerived means the key is
	// derived from the principal.
	//
	// It exists because the identity cannot answer this question. Under the
	// superseded scheme the key was caller-supplied opaque bytes, so a legacy
	// record's key may lie anywhere — including inside the reserved span, and
	// including the exact prefix-plus-digest shape a derived key has. Reading
	// the scheme off the key therefore certifies an adversarially-named legacy
	// record as new, a retry derives a different key, and a second live grant is
	// issued for work already permitted.
	//
	// Zero is the right value for a legacy record and gob gives it for free: a
	// record written before this field existed decodes with it absent, which is
	// exactly the claim "produced by the superseded scheme".
	AdmittedIdentityScheme uint32
}

// AdmittedIdentitySchemeDerived marks an admission whose durable key is derived
// from the principal rather than supplied by the caller.
const AdmittedIdentitySchemeDerived uint32 = 1

// MaxAdmittedObligationBytes bounds the obligation bitmap: one bit per
// reference the declaration may carry.
const MaxAdmittedObligationBytes = (MaxActionEvidence + 7) / 8

// MaxAdmittedEffects bounds the declared set a durable record may carry. The
// taxonomy has three classes; the bound is larger so a record written by a
// build that knows more of them still decodes here and is refused at
// resolution, which is where an unrecognised class is supposed to be caught.
const MaxAdmittedEffects = 16

// validateAdmittedDeclaration checks the shape of what an admission declared.
//
// Membership is deliberately not checked. A durable decoder reads whatever
// strings are stored, and Effects.exceeds already fails closed on a class it
// does not recognise — refusing to decode the record instead would turn a
// forward-compatible record into an undecodable one and lose the audit trail
// for exactly the admissions most worth reading.
//
// Canonical order is checked, because it is what makes two declarations
// comparable: the same classes in a different order are the same declaration
// and must not produce a record a retry cannot match.
func validateAdmittedDeclaration(record ActionRecord) error {
	if len(record.AdmittedEffects) > MaxAdmittedEffects {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "admitted effects exceed their bound")
	}
	for i := 1; i < len(record.AdmittedEffects); i++ {
		if record.AdmittedEffects[i-1] >= record.AdmittedEffects[i] {
			return shoal.NewError(
				shoal.ErrorInvalidArgument, "admitted effects are not canonical")
		}
	}
	// A scheme marker without an admission marker is incoherent: it claims how
	// an admission's key was produced for a record that is not an admission.
	if record.AdmittedIdentityScheme != 0 && len(record.AdmittedEffects) == 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"admitted identity scheme requires an admitted effect")
	}
	if len(record.AdmittedObligation) > MaxAdmittedObligationBytes {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "admitted obligation exceeds its bound")
	}
	// An obligation names positions in a declared reference list. Without the
	// list there is nothing for it to index, so a record carrying one and no
	// declaration has been assembled by something that skipped the grant path.
	if len(record.AdmittedObligation) > 0 &&
		len(record.AdmittedDisclosures) == 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"admitted obligation requires declared references")
	}
	if len(record.AdmittedDisclosures) == 0 {
		return nil
	}
	if len(record.AdmittedDisclosures) != sha256.Size {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"admitted disclosure digest is not a digest")
	}
	// Declared references without a declared effect is not a shape any
	// admission can produce: Request refuses an empty effect set before it
	// reaches a record. A record carrying one has been assembled by something
	// that skipped that check.
	if len(record.AdmittedEffects) == 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"admitted disclosures require an admitted effect")
	}
	return nil
}

// ActionEventProvenance identifies the authorization and request that
// committed the record's current transition.
type ActionEventProvenance struct {
	RequestID                shoal.ID
	CorrelationID            shoal.ID
	AuthorizationFingerprint auth.Fingerprint
	AuthorizationExpiresAt   time.Time
}

// EventProvenance returns durable transition provenance, falling back to the
// enqueue provenance for records written before transition provenance existed.
func (r ActionRecord) EventProvenance() ActionEventProvenance {
	result := ActionEventProvenance{
		RequestID: r.RequestID, CorrelationID: r.CorrelationID,
		AuthorizationFingerprint: r.AuthorizationFingerprint,
		AuthorizationExpiresAt:   r.AuthorizationExpiresAt,
	}
	if r.TransitionRequestID != "" {
		result.RequestID = r.TransitionRequestID
		result.CorrelationID = r.TransitionCorrelationID
	}
	switch r.State {
	case DispatchCanceled:
		if !r.CancelAuthorizationExpiresAt.IsZero() {
			result.AuthorizationFingerprint = r.CancelAuthorizationFingerprint
			result.AuthorizationExpiresAt = r.CancelAuthorizationExpiresAt
		}
	case DispatchClaimed, DispatchSucceeded, DispatchFailed:
		if !r.ExecutionExpiresAt.IsZero() {
			result.AuthorizationFingerprint = r.ExecutionFingerprint
			result.AuthorizationExpiresAt = r.ExecutionExpiresAt
		}
	}
	return result
}

type EnqueueRequest struct {
	ID              []byte
	IdempotencyKey  []byte
	AgentID         shoal.ID
	AgentGeneration int64
	Capability      string
	Action          string
	SourceID        []byte
	PolicyID        []byte
	ObjectID        shoal.ID
	Input           json.RawMessage
	Context         RequestContext
}

type ClaimRequest struct {
	ID              []byte
	ExpectedVersion uint64
	// ClaimID is both the durable claimant identity and this mutation's
	// caller-supplied idempotency key.
	ClaimID []byte
	Lease   time.Duration
	Context RequestContext
}

type CancelRequest struct {
	ID              []byte
	ExpectedVersion uint64
	// MutationKey must be stable across retries of the same cancellation.
	MutationKey []byte
	Context     RequestContext
}

// CompletionRequest is a remote worker reporting the outcome of work it
// performed out of process.
//
// It is bound to a claim, not to an action. ClaimID and ExpectedVersion must
// name the claim the reporter actually holds: a worker whose lease expired
// while it was working must be refused rather than allowed to overwrite
// whatever happened next, because by then the action may have been reclaimed
// and run again by someone else.
//
// Result carries exactly what an in-process ActionExecutor returns, and is
// validated identically. A remote worker cannot record output or evidence an
// in-process one could not.
type CompletionRequest struct {
	ID              []byte
	ExpectedVersion uint64
	// ClaimID must equal the claim currently held on the action.
	ClaimID []byte
	Result  ExecutionResult
	// Failed reports that the work did not succeed. Result.ErrorCode carries
	// the reason. The two are separate because a worker that fails with no
	// error code is a protocol error, not a success.
	Failed  bool
	Context RequestContext
}

type StatusRequest struct {
	ID      []byte
	Context RequestContext
}

type PullActionsRequest struct {
	After   []byte
	Limit   int
	Context RequestContext
}

type ActionPage struct {
	Actions []ActionRecord
	Next    []byte
}

// TeamActionListRequest asks for a bounded page of action state visible to a
// team-overview reader. The object and agent filters are narrowing only.
type TeamActionListRequest struct {
	After     []byte
	Limit     int
	SourceIDs [][]byte
	PolicyIDs [][]byte
	ObjectIDs []shoal.ID
	AgentIDs  []shoal.ID
	Context   RequestContext
}

type InvokeRequest struct {
	Enqueue EnqueueRequest
	ClaimID []byte
	Lease   time.Duration
}

type Invocation struct {
	ActionID        []byte
	IdempotencyKey  []byte
	ClaimFence      uint64
	AgentID         shoal.ID
	AgentGeneration int64
	Capability      string
	Action          string
	SourceID        []byte
	PolicyID        []byte
	ObjectID        shoal.ID
	Input           json.RawMessage
	Subject         shoal.ID
	Actor           shoal.ID
	ClientID        shoal.ID
	OnBehalfOf      []shoal.ID
	RequestID       shoal.ID
	CorrelationID   shoal.ID
	Deadline        time.Time
}

type ExecutionResult struct {
	Output               json.RawMessage
	ErrorCode            string
	EvidenceSnapshotID   shoal.ID
	EvidenceSnapshotAsOf time.Time
	Evidence             []EvidenceRef
}

// ActionTransition is the immutable, durable event outbox entry created in the
// same transaction as an action-state mutation.
type ActionTransition struct {
	ID     []byte
	Kind   string
	Record ActionRecord
}

// NewActionTransition snapshots the exact event-producing state of one
// committed dispatch transition.
func NewActionTransition(
	id []byte, kind string, record ActionRecord,
) (ActionTransition, error) {
	switch kind {
	case "action.enqueued":
	case "action.claimed":
	case "action.completed":
	case "action.failed":
	case "action.canceled":
	default:
		return ActionTransition{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet action transition kind is invalid")
	}
	if actionEventKind(record) != kind {
		return ActionTransition{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet action transition kind does not match action state")
	}
	result := ActionTransition{
		ID: append([]byte(nil), id...), Kind: kind, Record: cloneActionRecord(record),
	}
	if err := result.Validate(); err != nil {
		return ActionTransition{}, err
	}
	return result, nil
}

func (t ActionTransition) Validate() error {
	if err := validateOpaque("transition ID", t.ID, false); err != nil {
		return err
	}
	if err := t.Record.Validate(); err != nil {
		return err
	}
	if actionEventKind(t.Record) != t.Kind {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet action transition kind does not match action state")
	}
	return nil
}

type ActionExecutor interface {
	Execute(context.Context, Invocation) (ExecutionResult, error)
}

type DispatchMutation struct {
	Token           []byte
	ExpectedVersion uint64
	ExpectedFence   uint64
	TransitionKind  string
	Record          ActionRecord
}

type DispatchStore interface {
	GetAction(context.Context, []byte) (ActionRecord, error)
	ApplyAction(context.Context, DispatchMutation) (ActionRecord, error)
	ScanActions(context.Context, []byte, int) (ActionPage, error)
}

type ActionTransitionStore interface {
	DispatchStore
	PendingActionTransitions(
		context.Context, []byte, []byte, int,
	) (ActionTransitionPage, error)
	CompleteActionTransition(context.Context, ActionTransition) error
}

type ActionAudit struct {
	Phase       string
	Operation   auth.Operation
	Record      ActionRecord
	EffectError error
}

type ActionRecorder interface {
	// RecordAction must be idempotent for phase, action ID, and record version.
	RecordAction(context.Context, ActionAudit) error
}

type ActionEventPublisher interface {
	PublishActionEvent(context.Context, string, ActionRecord) error
}

type ActionTransitionPage struct {
	Transitions []ActionTransition
	Next        []byte
}

type DispatchConfig struct {
	Store    DispatchStore
	Registry *Service
	Resolver auth.Resolver
	Recorder ActionRecorder
	Events   ActionEventPublisher
	Clock    func() time.Time
}

func (r ActionRecord) Validate() error {
	if err := validateOpaque("action ID", r.ID, false); err != nil {
		return err
	}
	if err := validateOpaque("action idempotency key", r.IdempotencyKey, false); err != nil {
		return err
	}
	if r.Version == 0 {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action version is required")
	}
	switch r.State {
	case DispatchQueued, DispatchClaimed, DispatchSucceeded, DispatchFailed, DispatchCanceled:
	default:
		return shoal.NewError(shoal.ErrorInvalidArgument, "action state is invalid")
	}
	if err := shoal.ValidateRequiredID("action agent ID", r.AgentID); err != nil {
		return err
	}
	if r.AgentGeneration <= 0 {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action agent generation is invalid")
	}
	if err := validateName("capability", r.Capability); err != nil {
		return err
	}
	if err := validateName("action", r.Action); err != nil {
		return err
	}
	for name, value := range map[string][]byte{"action source": r.SourceID, "action policy": r.PolicyID} {
		if len(value) == 0 || len(value) > auth.MaxPolicyComponentBytes {
			return shoal.NewError(shoal.ErrorInvalidArgument, name+" is outside its byte bound")
		}
	}
	if err := shoal.ValidateRequiredID("action object ID", r.ObjectID); err != nil {
		return err
	}
	if _, _, err := validateJSONDocument("action input", r.Input, MaxActionPayloadBytes); err != nil {
		return err
	}
	if len(r.Output) > 0 {
		if _, _, err := validateJSONDocument("action output", r.Output, MaxActionOutputBytes); err != nil {
			return err
		}
	}
	if err := validateActionRecordError(r.ErrorCode); err != nil {
		return err
	}
	if err := shoal.ValidateRequiredID("action subject", r.Subject); err != nil {
		return err
	}
	if err := shoal.ValidateRequiredID("action actor", r.Actor); err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID("action client", r.ClientID); err != nil {
		return err
	}
	if len(r.OnBehalfOf) > auth.MaxOnBehalfOfEntries {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action delegation chain exceeds its bound")
	}
	for _, identity := range r.OnBehalfOf {
		if err := shoal.ValidateRequiredID("action delegation identity", identity); err != nil {
			return err
		}
	}
	// The claimant's chain is bounded and validated exactly like the
	// enqueuer's. It reaches the record from a decision rather than from a
	// request body, so this is defence in depth and nothing more — an earlier
	// version of this comment claimed it guarded a record decoded from an
	// older encoding, which cannot happen: such a record has no claimant chain
	// at all, not an over-long one. What it does guard is a corrupt or
	// tampered stored value.
	if err := shoal.ValidateOptionalID(
		"action claimant", r.ClaimantSubject); err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID(
		"action claimant actor", r.ClaimantActor); err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID(
		"action claimant client", r.ClaimantClientID); err != nil {
		return err
	}
	if len(r.ClaimantOnBehalfOf) > auth.MaxOnBehalfOfEntries {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"action claimant delegation chain exceeds its bound")
	}
	for _, identity := range r.ClaimantOnBehalfOf {
		if err := shoal.ValidateRequiredID(
			"action claimant delegation identity", identity); err != nil {
			return err
		}
	}
	if r.PolicyGeneration <= 0 || r.AuthorizationExpiresAt.IsZero() {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action authorization provenance is incomplete")
	}
	if err := shoal.ValidateRequiredID("action request ID", r.RequestID); err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID("action correlation ID", r.CorrelationID); err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID(
		"action transition request ID", r.TransitionRequestID,
	); err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID(
		"action transition correlation ID", r.TransitionCorrelationID,
	); err != nil {
		return err
	}
	if r.TransitionRequestID == "" && r.TransitionCorrelationID != "" {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"action transition correlation requires a request ID",
		)
	}
	if r.CancelAuthorizationExpiresAt.IsZero() !=
		(r.CancelAuthorizationFingerprint == auth.Fingerprint{}) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"action cancellation authorization provenance is incomplete",
		)
	}
	if err := r.Reason.Validate(); err != nil {
		return err
	}
	for name, value := range map[string]time.Time{
		"deadline": r.Deadline, "created time": r.CreatedAt, "updated time": r.UpdatedAt,
	} {
		if value.IsZero() || value.Location() != time.UTC {
			return shoal.NewError(shoal.ErrorInvalidArgument, "action "+name+" must be UTC")
		}
	}
	if r.UpdatedAt.Before(r.CreatedAt) || !r.CreatedAt.Before(r.Deadline) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action timestamps are inconsistent")
	}
	if len(r.AuthorizedOperations) == 0 {
		return shoal.NewError(shoal.ErrorInvalidArgument, "authorized action operations are required")
	}
	for i, operation := range r.AuthorizedOperations {
		if err := operation.Validate(); err != nil {
			return err
		}
		if i > 0 && r.AuthorizedOperations[i-1] >= operation {
			return shoal.NewError(shoal.ErrorInvalidArgument, "authorized action operations are not canonical")
		}
	}
	if err := validateOpaque("executor idempotency key", r.ExecutorKey, false); err != nil {
		return err
	}
	if r.State == DispatchCanceled {
		if err := validateOpaque("cancel mutation key", r.CancelKey, false); err != nil {
			return err
		}
	} else if len(r.CancelKey) != 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"non-canceled action carries a cancel mutation key",
		)
	}
	if r.State == DispatchQueued {
		if len(r.ClaimID) != 0 || r.ClaimFence != 0 ||
			r.ClaimLease != 0 || !r.ClaimLeaseUntil.IsZero() {
			return shoal.NewError(shoal.ErrorInvalidArgument, "queued action carries claim state")
		}
	} else if r.State != DispatchCanceled || r.ClaimFence != 0 {
		if err := validateOpaque("action claim ID", r.ClaimID, false); err != nil {
			return err
		}
		if r.ClaimFence == 0 || r.ClaimLease <= 0 ||
			r.ClaimLease > MaxActionClaimTTL || r.ClaimLeaseUntil.IsZero() ||
			r.ClaimLeaseUntil.Location() != time.UTC {
			return shoal.NewError(shoal.ErrorInvalidArgument, "action claim state is incomplete")
		}
	}
	if len(r.ClaimID) == 0 && r.ClaimLease != 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "action claim lease lacks a claim ID")
	}
	if (r.State == DispatchClaimed || r.State == DispatchSucceeded || r.State == DispatchFailed) &&
		(r.ExecutionPolicyGeneration <= 0 || r.ExecutionExpiresAt.IsZero()) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action execution authorization is incomplete")
	}
	if r.State == DispatchSucceeded && (len(r.Output) == 0 || !r.EffectPossible) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "successful action outcome is incomplete")
	}
	if r.State == DispatchFailed && (r.ErrorCode == "" || !r.EffectPossible) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "failed action outcome is incomplete")
	}
	if len(r.Evidence) > 0 {
		if err := shoal.ValidateRequiredID(
			"action evidence snapshot ID", r.EvidenceSnapshotID); err != nil {
			return err
		}
		if r.EvidenceSnapshotAsOf.IsZero() {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"action evidence snapshot time is required")
		}
		if r.UpdatedAt.Before(r.EvidenceSnapshotAsOf) ||
			r.ExecutionExpiresAt.Before(r.EvidenceSnapshotAsOf) {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"action evidence snapshot time is outside execution bounds")
		}
	} else if r.EvidenceSnapshotID != "" ||
		!r.EvidenceSnapshotAsOf.IsZero() {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"action evidence snapshot requires evidence")
	}
	if err := validateAdmittedDeclaration(r); err != nil {
		return err
	}
	return validateEvidence(r.Evidence)
}

func validateActionRecordError(value string) error {
	if len(value) > MaxActionErrorBytes || strings.TrimSpace(value) != value {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action error code is invalid")
	}
	return nil
}

func validateOpaque(name string, value []byte, optional bool) error {
	if optional && len(value) == 0 {
		return nil
	}
	if len(value) == 0 || len(value) > MaxActionIDBytes {
		return shoal.NewError(shoal.ErrorInvalidArgument, name+" is outside its byte bound")
	}
	return nil
}

func validateEvidence(values []EvidenceRef) error {
	if len(values) > MaxActionEvidence {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action evidence exceeds its bound")
	}
	totalMembers := 0
	for index, value := range values {
		reference, err := value.interactionReference().Canonical()
		if err != nil {
			return err
		}
		// An empty label set is public: a source that declares no visibility
		// is unrestricted everywhere else in the platform, and evidence drawn
		// from one therefore carries no labels. Requiring a label here made
		// every anchor from an unlabeled corpus unrecordable. A reader
		// matching labels must treat the empty set as matching every reader.
		normalized, err := interaction.Conjoin(value.Visibility)
		if err != nil {
			return err
		}
		if len(normalized) != len(value.Visibility) {
			return shoal.NewError(shoal.ErrorInvalidArgument, "evidence visibility must be canonical")
		}
		for i := range normalized {
			if normalized[i] != value.Visibility[i] {
				return shoal.NewError(shoal.ErrorInvalidArgument, "evidence visibility must be canonical")
			}
		}
		totalMembers += len(reference.NodeIDs) + len(reference.EdgeIDs) +
			len(reference.Assertions)
		if totalMembers > MaxActionEvidence {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"action evidence members exceed their bound")
		}
		for prior := 0; prior < index; prior++ {
			if equalEvidenceRef(values[prior], value) {
				return shoal.NewError(shoal.ErrorInvalidArgument, "action evidence contains duplicates")
			}
		}
	}
	return nil
}

func equalEvidenceRef(left, right EvidenceRef) bool {
	leftCanonical, leftErr := left.interactionReference().Canonical()
	rightCanonical, rightErr := right.interactionReference().Canonical()
	return leftErr == nil && rightErr == nil &&
		reflect.DeepEqual(leftCanonical, rightCanonical) &&
		len(left.Visibility) == len(right.Visibility) &&
		equalEvidenceVisibility(left.Visibility, right.Visibility)
}

func (r EvidenceRef) interactionReference() interaction.EvidenceReference {
	return interaction.EvidenceReference{
		AnchorID: r.AnchorID, Kind: r.Kind, Citation: r.Citation,
		NodeIDs:    append([]shoal.ID(nil), r.NodeIDs...),
		EdgeIDs:    append([]shoal.ID(nil), r.EdgeIDs...),
		Assertions: append([]interaction.AssertionReference(nil), r.Assertions...),
	}
}

func equalEvidenceVisibility(left, right []string) bool {
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validateJSONDocument(name string, raw json.RawMessage, limit int) (json.RawMessage, any, error) {
	if len(raw) == 0 || len(raw) > limit {
		return nil, nil, shoal.NewError(shoal.ErrorInvalidArgument, name+" is outside its byte bound")
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, nil, shoal.NewError(shoal.ErrorInvalidArgument, name+" is invalid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, nil, shoal.NewError(shoal.ErrorInvalidArgument, name+" has trailing JSON")
	}
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > limit {
		return nil, nil, shoal.NewError(shoal.ErrorInvalidArgument, name+" exceeds its byte bound")
	}
	return encoded, value, nil
}

// validateAgainstSchema implements the bounded declarative subset supported by
// fleet descriptors: type, properties, required, items, enum, and
// additionalProperties. Unsupported schema keywords fail closed.
func validateAgainstSchema(schema json.RawMessage, document json.RawMessage, name string, limit int) (json.RawMessage, error) {
	canonical, value, err := validateJSONDocument(name, document, limit)
	if err != nil {
		return nil, err
	}
	var root map[string]any
	decoder := json.NewDecoder(bytes.NewReader(schema))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "registered action schema is invalid")
	}
	if err := validateSchemaValue(root, value, "$", 0); err != nil {
		return nil, shoal.WrapError(shoal.ErrorInvalidArgument, name+" does not match its schema", err)
	}
	return canonical, nil
}

func validateSchemaValue(schema map[string]any, value any, path string, depth int) error {
	if depth > 32 {
		return fmt.Errorf("%s exceeds schema depth", path)
	}
	allowed := map[string]bool{"type": true, "properties": true, "required": true, "items": true, "enum": true, "additionalProperties": true}
	for key := range schema {
		if !allowed[key] {
			return fmt.Errorf("%s uses unsupported schema keyword %q", path, key)
		}
	}
	if rawType, exists := schema["type"]; exists {
		if _, ok := rawType.(string); !ok {
			return fmt.Errorf("%s type must be a string", path)
		}
	}
	if rawProperties, exists := schema["properties"]; exists {
		if _, ok := rawProperties.(map[string]any); !ok {
			return fmt.Errorf("%s properties must be an object", path)
		}
	}
	if rawRequired, exists := schema["required"]; exists {
		if _, ok := rawRequired.([]any); !ok {
			return fmt.Errorf("%s required must be an array", path)
		}
	}
	if rawItems, exists := schema["items"]; exists {
		if _, ok := rawItems.(map[string]any); !ok {
			return fmt.Errorf("%s items must be an object", path)
		}
	}
	if rawAdditional, exists := schema["additionalProperties"]; exists {
		if _, ok := rawAdditional.(bool); !ok {
			return fmt.Errorf("%s additionalProperties must be a boolean", path)
		}
	}
	if enums, ok := schema["enum"].([]any); ok {
		matched := false
		for _, candidate := range enums {
			left, _ := json.Marshal(candidate)
			right, _ := json.Marshal(value)
			if bytes.Equal(left, right) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s is not an allowed value", path)
		}
	} else if _, exists := schema["enum"]; exists {
		return fmt.Errorf("%s enum must be an array", path)
	}
	kind, _ := schema["type"].(string)
	if kind == "" {
		if _, ok := schema["items"]; ok {
			kind = "array"
		} else if _, properties := schema["properties"]; properties {
			kind = "object"
		} else if _, required := schema["required"]; required {
			kind = "object"
		} else if _, additional := schema["additionalProperties"]; additional {
			kind = "object"
		}
	}
	switch kind {
	case "", "object", "array", "string", "number", "integer", "boolean", "null":
	default:
		return fmt.Errorf("%s has unsupported type %q", path, kind)
	}
	switch kind {
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s must be an object", path)
		}
		properties, _ := schema["properties"].(map[string]any)
		required, requiredOK := schema["required"].([]any)
		if _, exists := schema["required"]; exists && !requiredOK {
			return fmt.Errorf("%s required must be an array", path)
		}
		for _, entry := range required {
			key, ok := entry.(string)
			if !ok {
				return fmt.Errorf("%s required must contain strings", path)
			}
			if _, ok := object[key]; !ok {
				return fmt.Errorf("%s.%s is required", path, key)
			}
		}
		additional, hasAdditional := schema["additionalProperties"].(bool)
		for key, child := range object {
			rawChild, found := properties[key]
			if !found {
				if hasAdditional && !additional {
					return fmt.Errorf("%s.%s is not allowed", path, key)
				}
				continue
			}
			childSchema, ok := rawChild.(map[string]any)
			if !ok {
				return fmt.Errorf("%s.%s schema is invalid", path, key)
			}
			if err := validateSchemaValue(childSchema, child, path+"."+key, depth+1); err != nil {
				return err
			}
		}
	case "array":
		array, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%s must be an array", path)
		}
		if rawItems, ok := schema["items"]; ok {
			itemSchema, ok := rawItems.(map[string]any)
			if !ok {
				return fmt.Errorf("%s items schema is invalid", path)
			}
			for index, item := range array {
				if err := validateSchemaValue(itemSchema, item, fmt.Sprintf("%s[%d]", path, index), depth+1); err != nil {
					return err
				}
			}
		}
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s must be a string", path)
		}
	case "number":
		if _, ok := value.(json.Number); !ok {
			return fmt.Errorf("%s must be a number", path)
		}
	case "integer":
		number, ok := value.(json.Number)
		if !ok || strings.ContainsAny(number.String(), ".eE") {
			return fmt.Errorf("%s must be an integer", path)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s must be a boolean", path)
		}
	case "null":
		if value != nil {
			return fmt.Errorf("%s must be null", path)
		}
	}
	return nil
}

func cloneActionRecord(input ActionRecord) ActionRecord {
	result := input
	result.ID = append([]byte(nil), input.ID...)
	result.IdempotencyKey = append([]byte(nil), input.IdempotencyKey...)
	result.SourceID = append([]byte(nil), input.SourceID...)
	result.PolicyID = append([]byte(nil), input.PolicyID...)
	result.Input = append(json.RawMessage(nil), input.Input...)
	result.Output = append(json.RawMessage(nil), input.Output...)
	result.OnBehalfOf = append([]shoal.ID(nil), input.OnBehalfOf...)
	result.AuthorizedOperations = append([]auth.Operation(nil), input.AuthorizedOperations...)
	result.ClaimID = append([]byte(nil), input.ClaimID...)
	result.ClaimantOnBehalfOf = append(
		[]shoal.ID(nil), input.ClaimantOnBehalfOf...)
	result.CancelKey = append([]byte(nil), input.CancelKey...)
	result.ExecutorKey = append([]byte(nil), input.ExecutorKey...)
	result.Evidence = cloneActionEvidence(input.Evidence)
	result.AdmittedEffects = input.AdmittedEffects.clone()
	result.AdmittedDisclosures = append(
		[]byte(nil), input.AdmittedDisclosures...)
	result.AdmittedObligation = append(
		[]byte(nil), input.AdmittedObligation...)
	// AdmittedIdentityScheme needs no line: the struct assignment above copies
	// a scalar, and unlike the slices there is no backing array to share. An
	// explicit copy for it would be a line no mutation could observe.
	return result
}

// isAdmission reports whether this record was produced by the pre-call
// admission seam rather than by dispatch.
//
// The declared effect set is the marker, and it is a reliable one in both
// directions: an admission is refused before it reaches a record unless it
// declares an effect, and no dispatch enqueue ever sets one.
func (r ActionRecord) isAdmission() bool {
	return len(r.AdmittedEffects) > 0
}

func cloneActionEvidence(input []EvidenceRef) []EvidenceRef {
	result := make([]EvidenceRef, len(input))
	for i, evidence := range input {
		result[i] = evidence
		result[i].NodeIDs = append(
			[]shoal.ID(nil), evidence.NodeIDs...)
		result[i].EdgeIDs = append(
			[]shoal.ID(nil), evidence.EdgeIDs...)
		result[i].Assertions = append(
			[]interaction.AssertionReference(nil), evidence.Assertions...)
		result[i].Visibility = append([]string(nil), evidence.Visibility...)
	}
	return result
}

func canonicalOperations(values []auth.Operation) []auth.Operation {
	result := append([]auth.Operation(nil), values...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	out := result[:0]
	for _, operation := range result {
		if len(out) == 0 || out[len(out)-1] != operation {
			out = append(out, operation)
		}
	}
	return out
}
