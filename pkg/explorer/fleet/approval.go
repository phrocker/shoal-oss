// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Approval holds an operation for a human decision (#451).
//
// A held request is not an action. It is its own record, in its own key space,
// and no ActionRecord exists for it until an approver distinct from the
// requester has approved that exact request and the requester has come back for
// it. That is the whole of the safety argument, and it is the same one the
// admission seam makes for never passing through the queued state: Pull, Claim,
// Invoke and ExecuteClaim all start from an ActionRecord, so work that has no
// ActionRecord cannot be pulled, claimed or performed through any of them,
// across restart and retry, without a single edit to any of them.
//
// A new DispatchState was the alternative and it was refused. A "held" state
// would fall through Claim's checks into applyClaim unless every one of them
// learned about it, and the approver's transition would land in the requester's
// outbox — the mixed-identity outbox #480 describes, which no single principal
// can drain.
//
// The flow is three steps, each a compare-and-set on the approval record:
//
//   - Request, by the requester under dispatch. Builds exactly the version-1
//     record an enqueue would commit, stores it as the request, and holds it
//     pending until ExpiresAt.
//   - Decide, by an approver under OperationActionApprove. Approves or refuses
//     the pending request, naming the digest and the policy generation it
//     reviewed.
//   - Materialize, by the requester re-requesting. The rebuilt request must
//     equal the stored one; the approval moves approved → enqueued, and only
//     then is the ActionRecord created, from the stored request plus the
//     approval provenance, through the same audit and publication path an
//     enqueue uses.
//
// An approval grants the approver nothing, and it grants the requester nothing
// beyond the one queued record. The work still runs only through that record's
// principal and the claim fence: a ZTAT is a record, not a grant.
var (
	// ErrApprovalRequired reports an attempt to enqueue or invoke an action
	// whose registration requires approval through any route but the
	// approval one. See approvalRequired for the transport code it carries.
	ErrApprovalRequired = errors.New("fleet dispatch: action requires approval")
	// ErrApprovalNotFound is the store's sentinel for an absent approval. It
	// never reaches a caller: every service path normalises it to the
	// not-found shape every other absent record gets.
	ErrApprovalNotFound = errors.New("fleet approval: approval not found")
	// ErrApprovalConflict reports a request or a decision that is not the one
	// already on the record: a changed request under the same identity, a
	// digest the approver did not review, or a decision that differs from
	// one already made.
	ErrApprovalConflict = errors.New("fleet approval: approval conflict")
	// ErrApprovalExpired reports a decision attempted after the approval
	// window closed. The expiry is written before this is returned.
	ErrApprovalExpired = errors.New("fleet approval: approval window has closed")
	// ErrApprovalSuperseded reports a decision made under a policy
	// generation other than the one the request was made under.
	ErrApprovalSuperseded = errors.New(
		"fleet approval: policy generation has moved since the request")
)

// approvalRequired is what enqueue and invoke return for an action that
// requires approval.
//
// Conflict, deliberately, and every client will branch on it, so the choice
// is worth stating. The request is well formed (not invalid_argument) and the
// caller is authorized to make it (not unauthorized — the action may well
// happen, pending somebody else's decision). Nothing is unavailable, so a
// retry of the same request will never succeed (not unavailable). What remains
// is the closest thing this code set has to a failed precondition: the request
// conflicts with the registered state of the action, and the remedy is a
// different route, not a different request. The message names that route.
func approvalRequired() error {
	return shoal.WrapError(
		shoal.ErrorConflict,
		"action requires approval; request it through the approval route",
		ErrApprovalRequired)
}

const (
	// DefaultApprovalWindow is how long a request stays decidable when the
	// host configures nothing. It is further clamped to the request's own
	// deadline.
	DefaultApprovalWindow = time.Hour
	// MaxApprovalWindow bounds the configured window. A request cannot
	// outlive its action deadline, which MaxActionDeadline already bounds.
	MaxApprovalWindow = MaxActionDeadline
	// MaxApprovalListResults bounds one page of pending approvals.
	MaxApprovalListResults = 64
)

// ApprovalState is where a held request is.
type ApprovalState string

const (
	// ApprovalPending is held and decidable until ExpiresAt.
	ApprovalPending ApprovalState = "pending"
	// ApprovalApproved has been approved and not yet materialized.
	ApprovalApproved ApprovalState = "approved"
	// ApprovalRefused has been refused. It is final.
	ApprovalRefused ApprovalState = "refused"
	// ApprovalExpired closed without becoming work: undecided at ExpiresAt,
	// or approved and not materialized before it. Verdict says which. Final.
	ApprovalExpired ApprovalState = "expired"
	// ApprovalEnqueued has become an ActionRecord at the same identity. Final.
	ApprovalEnqueued ApprovalState = "enqueued"
)

func (s ApprovalState) valid() bool {
	switch s {
	case ApprovalPending, ApprovalApproved, ApprovalRefused,
		ApprovalExpired, ApprovalEnqueued:
		return true
	default:
		return false
	}
}

// ApprovalVerdict is what an approver decided.
type ApprovalVerdict string

const (
	ApprovalVerdictApprove ApprovalVerdict = "approve"
	ApprovalVerdictRefuse  ApprovalVerdict = "refuse"
)

func (v ApprovalVerdict) validate() error {
	switch v {
	case ApprovalVerdictApprove, ApprovalVerdictRefuse:
		return nil
	default:
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "approval verdict is invalid")
	}
}

// ApprovalRecord is the durable statement of one held request and what became
// of it.
//
// Request is the exact version-1 queued record the requester's decision built,
// stored rather than rebuilt so that what was approved is what materializes:
// a materialization copies it, it never recomputes it. RequestDigest is a
// digest over the fields equivalentEnqueue compares, and it is what an
// approver names to say which request it reviewed.
//
// Everything a dataset of adjudications (#401, #419) needs is here: the exact
// request, the requester, the approver, the verdict, the time, and the policy
// generation both were made under. Exporting it is deferred; storing it is not.
type ApprovalRecord struct {
	ID               []byte
	Version          uint64
	State            ApprovalState
	Request          ActionRecord
	RequestDigest    []byte
	PolicyGeneration int64
	RequestedAt      time.Time
	ExpiresAt        time.Time
	UpdatedAt        time.Time
	// Verdict and the approver fields are set together, by Decide, and are
	// kept when an approved request expires unmaterialized: an approval that
	// was given and never used is still an adjudication.
	Verdict                  ApprovalVerdict
	ApproverSubject          shoal.ID
	ApproverActor            shoal.ID
	ApproverClientID         shoal.ID
	ApproverFingerprint      auth.Fingerprint
	ApproverPolicyGeneration int64
	DecidedAt                time.Time
	DecisionRequestID        shoal.ID
	DecisionCorrelationID    shoal.ID
	// MaterializedAt is when the approved request was committed to become
	// work. It is written by the approved → enqueued compare-and-set, before
	// the ActionRecord exists, and is the UpdatedAt the ActionRecord is
	// created with, so a retry after a crash between the two writes builds
	// the same record.
	MaterializedAt time.Time
}

// Validate checks a record's shape, including that the stored digest is the
// digest of the stored request. A record whose digest does not describe its
// request would let an approver review one request and approve another.
func (r ApprovalRecord) Validate() error {
	if err := validateOpaque("approval ID", r.ID, false); err != nil {
		return err
	}
	if reservedAdmissionID(r.ID) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "approval ID is reserved")
	}
	if r.Version == 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "approval version is required")
	}
	if !r.State.valid() {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "approval state is invalid")
	}
	if err := r.Request.Validate(); err != nil {
		return err
	}
	if r.Request.State != DispatchQueued || r.Request.Version != 1 ||
		!bytes.Equal(r.Request.ID, r.ID) || r.Request.isAdmission() ||
		len(r.Request.ApprovalRequestDigest) != 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"approval request is not a fresh queued record")
	}
	if !bytes.Equal(r.RequestDigest, ApprovalRequestDigest(r.Request)) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"approval digest does not describe its request")
	}
	if r.PolicyGeneration != r.Request.PolicyGeneration {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"approval policy generation does not match its request")
	}
	for name, value := range map[string]time.Time{
		"requested": r.RequestedAt, "expiry": r.ExpiresAt, "updated": r.UpdatedAt,
	} {
		if value.IsZero() || value.Location() != time.UTC {
			return shoal.NewError(
				shoal.ErrorInvalidArgument, "approval "+name+" time must be UTC")
		}
	}
	if !r.RequestedAt.Equal(r.Request.CreatedAt) ||
		!r.RequestedAt.Before(r.ExpiresAt) ||
		r.ExpiresAt.After(r.Request.Deadline) ||
		r.UpdatedAt.Before(r.RequestedAt) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "approval timestamps are inconsistent")
	}
	decided := r.Verdict != ""
	switch r.State {
	case ApprovalPending:
		if decided {
			return shoal.NewError(
				shoal.ErrorInvalidArgument, "a pending approval carries a verdict")
		}
	case ApprovalApproved, ApprovalEnqueued:
		if r.Verdict != ApprovalVerdictApprove {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"an approved request requires an approving verdict")
		}
	case ApprovalRefused:
		if r.Verdict != ApprovalVerdictRefuse {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"a refused request requires a refusing verdict")
		}
	case ApprovalExpired:
		// Either never decided, or approved and never materialized. A
		// refusal is already final and does not expire.
		if decided && r.Verdict != ApprovalVerdictApprove {
			return shoal.NewError(
				shoal.ErrorInvalidArgument, "an expired approval verdict is invalid")
		}
	}
	if decided {
		if err := r.validateDecision(); err != nil {
			return err
		}
	} else if r.ApproverSubject != "" || r.ApproverActor != "" ||
		r.ApproverClientID != "" ||
		r.ApproverFingerprint != (auth.Fingerprint{}) ||
		r.ApproverPolicyGeneration != 0 || !r.DecidedAt.IsZero() ||
		r.DecisionRequestID != "" || r.DecisionCorrelationID != "" {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"an undecided approval carries approver provenance")
	}
	if (r.State == ApprovalEnqueued) == r.MaterializedAt.IsZero() {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"approval materialization time is inconsistent with its state")
	}
	if !r.MaterializedAt.IsZero() &&
		(r.MaterializedAt.Location() != time.UTC ||
			r.MaterializedAt.Before(r.DecidedAt) ||
			!r.MaterializedAt.Before(r.ExpiresAt)) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"approval materialization time is outside its window")
	}
	return nil
}

func (r ApprovalRecord) validateDecision() error {
	if err := shoal.ValidateRequiredID("approver", r.ApproverSubject); err != nil {
		return err
	}
	if err := shoal.ValidateRequiredID(
		"approver actor", r.ApproverActor); err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID(
		"approver client", r.ApproverClientID); err != nil {
		return err
	}
	if err := shoal.ValidateRequiredID(
		"approval decision request ID", r.DecisionRequestID); err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID(
		"approval decision correlation ID", r.DecisionCorrelationID); err != nil {
		return err
	}
	if r.ApproverFingerprint == (auth.Fingerprint{}) ||
		r.ApproverPolicyGeneration != r.PolicyGeneration ||
		r.DecidedAt.IsZero() || r.DecidedAt.Location() != time.UTC ||
		r.DecidedAt.Before(r.RequestedAt) || !r.DecidedAt.Before(r.ExpiresAt) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "approval decision provenance is incomplete")
	}
	return nil
}

// CloneApprovalRecord returns an independent copy.
func CloneApprovalRecord(input ApprovalRecord) ApprovalRecord {
	result := input
	result.ID = append([]byte(nil), input.ID...)
	result.Request = cloneActionRecord(input.Request)
	result.RequestDigest = append([]byte(nil), input.RequestDigest...)
	return result
}

// ApprovalRequestDigest identifies the exact request an approver reviews.
// Exported for durable stores outside this package, which must be able to
// build and check the records they hold; a client cannot recompute it, since
// it covers the server-side authorization fingerprint.
//
// It covers every field equivalentEnqueue compares and nothing else, so the
// two cannot disagree about whether a re-request is the request that was
// approved: a change that equivalentEnqueue would call a different request
// changes this digest, and one it would call the same request does not. The
// admission fields are omitted because an approval request is never an
// admission and Validate refuses one that is.
//
// Every field is length-framed, so no two distinct requests can collide by
// moving bytes between adjacent fields.
func ApprovalRequestDigest(record ActionRecord) []byte {
	digest := sha256.New()
	writeDispatchTupleField(digest, []byte("shoal.fleet.approval-request.v1"))
	writeDispatchTupleField(digest, record.ID)
	writeDispatchTupleField(digest, record.IdempotencyKey)
	writeDispatchTupleField(digest, []byte(record.AgentID))
	writeApprovalInt64(digest, record.AgentGeneration)
	writeDispatchTupleField(digest, []byte(record.Capability))
	writeDispatchTupleField(digest, []byte(record.Action))
	writeDispatchTupleField(digest, record.SourceID)
	writeDispatchTupleField(digest, record.PolicyID)
	writeDispatchTupleField(digest, []byte(record.ObjectID))
	writeDispatchTupleField(digest, record.Input)
	writeDispatchTupleField(digest, []byte(record.Subject))
	writeDispatchTupleField(digest, []byte(record.Actor))
	writeDispatchTupleField(digest, []byte(record.ClientID))
	writeApprovalInt64(digest, int64(len(record.OnBehalfOf)))
	for _, identity := range record.OnBehalfOf {
		writeDispatchTupleField(digest, []byte(identity))
	}
	writeDispatchTupleField(digest, record.AuthorizationFingerprint[:])
	writeApprovalInt64(digest, record.PolicyGeneration)
	writeDispatchTupleField(digest, []byte(record.Reason.Code))
	writeDispatchTupleField(digest, []byte(record.Reason.Digest))
	writeApprovalInt64(digest, record.Deadline.UnixNano())
	return digest.Sum(nil)
}

func writeApprovalInt64(digest interface{ Write([]byte) (int, error) }, value int64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], uint64(value))
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(raw)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(raw[:])
}

// ApprovalMutation is one compare-and-set on an approval record.
type ApprovalMutation struct {
	Token           []byte
	ExpectedVersion uint64
	Record          ApprovalRecord
}

type ApprovalPage struct {
	Approvals []ApprovalRecord
	Next      []byte
}

// ApprovalStore is the durable approval key space. ApplyApproval must refuse,
// with ErrApprovalConflict, any write whose ExpectedVersion is not the stored
// version, and must accept an identical retry of a write already applied.
type ApprovalStore interface {
	GetApproval(context.Context, []byte) (ApprovalRecord, error)
	ApplyApproval(context.Context, ApprovalMutation) (ApprovalRecord, error)
	ScanApprovals(context.Context, []byte, int) (ApprovalPage, error)
}

// ApprovalAudit is one privileged approval transition, attributed to the
// principal that made it.
//
// The acting identity is carried explicitly rather than read off the record,
// because the record holds two principals — the requester and the approver —
// and which one acted depends on the phase. The action recorder reads the
// actor off the record and is wrong for any caller that is not the enqueuer
// (#480 item 2); this does not repeat that.
type ApprovalAudit struct {
	Phase                    string
	Operation                auth.Operation
	Record                   ApprovalRecord
	Subject                  shoal.ID
	Actor                    shoal.ID
	ClientID                 shoal.ID
	OnBehalfOf               []shoal.ID
	RequestID                shoal.ID
	AuthorizationFingerprint auth.Fingerprint
	AuthorizationExpiresAt   time.Time
}

type ApprovalRecorder interface {
	// RecordApproval must be idempotent for an identical audit.
	RecordApproval(context.Context, ApprovalAudit) error
}

type ApprovalConfig struct {
	Dispatch *DispatchService
	Store    ApprovalStore
	Recorder ApprovalRecorder
	// Narrowed reports whether the request's decision was derived through a
	// narrowing the host applies, such as workspace settings. Required.
	//
	// The approver-separation rule requires an approver to *fail* dispatch,
	// invoke and execute, and a narrowing can only remove authority — so a
	// principal holding approve and dispatch could narrow its own decision to
	// approve alone and pass. Separation has to be judged on the authority a
	// principal actually holds, so every approver path refuses a narrowed
	// decision. The host supplies the predicate because only it knows how it
	// narrows; a host that never narrows supplies one returning false.
	Narrowed func(context.Context) bool
	// Window is how long a request stays decidable. Zero means
	// DefaultApprovalWindow. It is clamped to each request's deadline.
	Window time.Duration
}

type ApprovalService struct {
	dispatch *DispatchService
	store    ApprovalStore
	recorder ApprovalRecorder
	narrowed func(context.Context) bool
	window   time.Duration
}

func NewApprovalService(config ApprovalConfig) (*ApprovalService, error) {
	if config.Dispatch == nil || config.Store == nil || config.Recorder == nil ||
		config.Narrowed == nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet approval dependencies are required")
	}
	window := config.Window
	if window == 0 {
		window = DefaultApprovalWindow
	}
	if window < 0 || window > MaxApprovalWindow {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "approval window is outside its bound")
	}
	return &ApprovalService{
		dispatch: config.Dispatch, store: config.Store,
		recorder: config.Recorder, narrowed: config.Narrowed, window: window,
	}, nil
}

// ApprovalReceipt answers a request. State is where the request is; Action is
// the materialized record and is set only when State is ApprovalEnqueued.
type ApprovalReceipt struct {
	State            ApprovalState
	ID               []byte
	RequestDigest    []byte
	PolicyGeneration int64
	ExpiresAt        time.Time
	Action           ActionRecord
}

type ApprovalDecisionRequest struct {
	ID []byte
	// RequestDigest and PolicyGeneration are what the approver reviewed. Both
	// must match the record: an approval covers one exact request under one
	// policy generation, and an approver must not be able to approve a
	// request it has not seen.
	RequestDigest    []byte
	PolicyGeneration int64
	Verdict          ApprovalVerdict
	Context          RequestContext
}

// ApprovalCondition qualifies an approval's effective state with what a
// reader needs to know and the stored state alone does not say.
type ApprovalCondition string

const (
	// ApprovalConditionNone: the effective state is the whole answer.
	ApprovalConditionNone ApprovalCondition = ""
	// ApprovalConditionWindowClosed: stored as pending or approved, but the
	// window has closed. Expiry is written lazily, by the next request or
	// decision; Status reports it without writing it.
	ApprovalConditionWindowClosed ApprovalCondition = "window_closed"
	// ApprovalConditionTargetMoved: the agent's generation has moved, or the
	// agent is gone, since the request was made. The request names the old
	// generation and can never be decided or materialized again.
	ApprovalConditionTargetMoved ApprovalCondition = "target_moved"
	// ApprovalConditionPolicyMoved: the policy generation has moved since the
	// request was made. Neither the approver nor the requester can act on it.
	ApprovalConditionPolicyMoved ApprovalCondition = "policy_generation_moved"
	// ApprovalConditionDeadlinePassed: the request's action deadline has
	// passed, so it can no longer be re-requested and so never materialized.
	ApprovalConditionDeadlinePassed ApprovalCondition = "deadline_passed"
	// ApprovalConditionAwaitingAction: committed to become work (enqueued),
	// but the action has not been written yet — the second write was lost.
	// The requester's next re-request writes it while the target and the
	// deadline still hold.
	ApprovalConditionAwaitingAction ApprovalCondition = "enqueued_without_action"
)

// ApprovalUnresolvable is an effective state only, never stored: a pending,
// approved or enqueued-without-action request that can no longer progress,
// for the reason its Condition gives.
const ApprovalUnresolvable ApprovalState = "unresolvable"

// ApprovalStatus is what Status answers: the stored record, the state it is
// effectively in now, and why the two differ when they do.
type ApprovalStatus struct {
	Approval  ApprovalRecord
	State     ApprovalState
	Condition ApprovalCondition
}

type ApprovalStatusRequest struct {
	ID      []byte
	Context RequestContext
}

type PendingApprovalsRequest struct {
	After   []byte
	Limit   int
	Context RequestContext
}

type PendingApprovalsPage struct {
	Approvals []ApprovalRecord
	Next      []byte
}

// maxApprovalAttempts bounds the re-read loop a lost compare-and-set takes.
// A loss means somebody else moved the record; re-reading once is normally
// enough to see what they did, and the bound makes a pathological race an
// error rather than a spin.
const maxApprovalAttempts = 8

// errMaterializeRaced is internal: a materialization lost to a concurrent one
// that is not yet readable.
var errMaterializeRaced = errors.New("fleet approval: materialization raced")

// materializeRead classifies a failed read or write at the action identity
// during materialization. The durable store answers a read that lands while a
// concurrent write to the same identity is in flight with a not-found that is
// not ErrActionNotFound — the guard head exists and the cell is not yet
// committed — and a write that collides with one as a conflict. Both mean
// "look again shortly", not "this failed", and are retried by Request.
func materializeRead(err error) error {
	if (shoal.IsErrorCode(err, shoal.ErrorNotFound) &&
		!errors.Is(err, ErrActionNotFound)) ||
		shoal.IsErrorCode(err, shoal.ErrorConflict) {
		return errors.Join(errMaterializeRaced, err)
	}
	return err
}

// Request holds an approval-required action for a decision, or reports where
// a request already made has got to, or materializes an approved one.
//
// It is idempotent in the way enqueue is: the same request returns the same
// answer, and a changed one conflicts. A refused or expired request is a
// successful answer carrying that state, not an error — the caller asked
// where its request is, and that is where it is.
func (s *ApprovalService) Request(
	ctx context.Context, request EnqueueRequest,
) (ApprovalReceipt, error) {
	dispatch := s.dispatch
	ctx, cancel := dispatch.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := dispatch.begin(ctx, auth.OperationDispatch, request.Context)
	if err != nil {
		return ApprovalReceipt{}, err
	}
	if reservedAdmissionID(request.ID) {
		return ApprovalReceipt{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "action ID is reserved")
	}
	// Exactly the record an enqueue would build, under exactly the checks it
	// makes, so the approval route cannot admit an input the dispatch route
	// would refuse.
	base, action, err := dispatch.queuedRecord(
		ctx, decision, request, auth.OperationDispatch, now)
	if err != nil {
		return ApprovalReceipt{}, err
	}
	if !action.RequiresApproval {
		return ApprovalReceipt{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"action does not require approval; enqueue it directly")
	}
	digest := ApprovalRequestDigest(base)
	for attempt := 0; attempt < maxApprovalAttempts; attempt++ {
		current, readErr := s.store.GetApproval(ctx, base.ID)
		if errors.Is(readErr, ErrApprovalNotFound) {
			receipt, err := s.hold(ctx, decision, base, digest, now)
			if errors.Is(err, ErrApprovalConflict) {
				continue
			}
			return receipt, err
		}
		if readErr != nil {
			return ApprovalReceipt{}, readErr
		}
		// The same request, field for field, or nothing. Everything an
		// approver reviewed is in the digest, and the principal, the
		// authorization fingerprint and the policy generation are in it too:
		// a requester whose policy generation has moved is making a different
		// request, and needs a new approval for it.
		if !equivalentEnqueue(current.Request, base) ||
			!bytes.Equal(current.RequestDigest, digest) {
			return ApprovalReceipt{}, approvalConflict()
		}
		receipt, err := s.advance(ctx, decision, current, now)
		if errors.Is(err, ErrApprovalConflict) {
			continue
		}
		if errors.Is(err, errMaterializeRaced) {
			// Jittered so callers that collided do not collide again in
			// lockstep. Bounded by the attempt count and the request context.
			wait := time.Duration(attempt+1)*2*time.Millisecond +
				time.Duration(rand.Int64N(int64(2*time.Millisecond)))
			select {
			case <-ctx.Done():
				return ApprovalReceipt{}, err
			case <-time.After(wait):
			}
			continue
		}
		return receipt, err
	}
	return ApprovalReceipt{}, approvalConflict()
}

// hold writes a new pending approval.
func (s *ApprovalService) hold(
	ctx context.Context,
	decision auth.Decision,
	base ActionRecord,
	digest []byte,
	now time.Time,
) (ApprovalReceipt, error) {
	// An action already at this identity is a conflict, as it is for an
	// enqueue: the identity is the caller's to choose and the materialized
	// record will be written there.
	if _, err := s.dispatch.store.GetAction(ctx, base.ID); err == nil {
		return ApprovalReceipt{}, ErrActionConflict
	} else if !errors.Is(err, ErrActionNotFound) {
		return ApprovalReceipt{}, err
	}
	expires := now.Add(s.window)
	if base.Deadline.Before(expires) {
		expires = base.Deadline
	}
	record := ApprovalRecord{
		ID: append([]byte(nil), base.ID...), Version: 1, State: ApprovalPending,
		Request: cloneActionRecord(base), RequestDigest: digest,
		PolicyGeneration: base.PolicyGeneration,
		RequestedAt:      now, ExpiresAt: expires, UpdatedAt: now,
	}
	stored, err := s.commit(
		ctx, "approval_request", auth.OperationDispatch, decision, record, 0)
	if err != nil {
		return ApprovalReceipt{}, err
	}
	return receiptFor(stored, ActionRecord{}), nil
}

// advance moves a request the requester already made as far as it can go.
func (s *ApprovalService) advance(
	ctx context.Context,
	decision auth.Decision,
	current ApprovalRecord,
	now time.Time,
) (ApprovalReceipt, error) {
	switch current.State {
	case ApprovalPending:
		if !now.Before(current.ExpiresAt) {
			expired, err := s.expire(
				ctx, auth.OperationDispatch, decision, current, now)
			if err != nil {
				return ApprovalReceipt{}, err
			}
			return receiptFor(expired, ActionRecord{}), nil
		}
		return receiptFor(current, ActionRecord{}), nil
	case ApprovalRefused, ApprovalExpired:
		return receiptFor(current, ActionRecord{}), nil
	case ApprovalApproved:
		// Approved and not used in time is expired, not approved. The window
		// bounds when work may begin to exist, not merely when an approver
		// may answer; otherwise an approval would be a standing permission
		// the requester could exercise at any later moment.
		if !now.Before(current.ExpiresAt) {
			expired, err := s.expire(
				ctx, auth.OperationDispatch, decision, current, now)
			if err != nil {
				return ApprovalReceipt{}, err
			}
			return receiptFor(expired, ActionRecord{}), nil
		}
		// Something already at the identity that is not this approval's work
		// — an action enqueued there by another route while the request was
		// held — means the approval can never become work. Found here, before
		// the commit, the request stays approved (and expires) instead of
		// becoming an enqueued approval that names work that does not exist.
		// A race between this read and the commit is still possible and is
		// reported by Status as enqueued without an action.
		if existing, err := s.dispatch.store.GetAction(ctx, current.ID); err == nil {
			if _, replayErr := s.replayMaterialized(
				ctx, existing, current); replayErr != nil {
				return ApprovalReceipt{}, replayErr
			}
		} else if !errors.Is(err, ErrActionNotFound) {
			return ApprovalReceipt{}, materializeRead(err)
		}
		next := CloneApprovalRecord(current)
		next.Version++
		next.State = ApprovalEnqueued
		next.MaterializedAt = notBefore(now, current)
		next.UpdatedAt = next.MaterializedAt
		stored, err := s.commit(
			ctx, "approval_materialize", auth.OperationDispatch,
			decision, next, current.Version)
		if err != nil {
			return ApprovalReceipt{}, err
		}
		current = stored
		fallthrough
	case ApprovalEnqueued:
		// Past ExpiresAt is fine here and deliberate. The decision to make
		// this work was committed inside the window by the write above; a
		// crash between that write and the one below is recovered by the
		// requester re-requesting, and refusing then would turn a crash into
		// a lost approval. Recovery is bounded all the same, not by this
		// window but by whether a re-request can reach here at all: before
		// the action deadline (RequestContext refuses one after it) and while
		// the agent and policy generations hold (queuedRecord resolves the
		// request's generation). Past those the row stays enqueued with no
		// action, and Status reports it as unresolvable.
		action, err := s.materialize(ctx, decision, current)
		if err != nil {
			return ApprovalReceipt{}, err
		}
		return receiptFor(current, action), nil
	default:
		return ApprovalReceipt{}, shoal.NewError(
			shoal.ErrorInternal, "approval state is invalid")
	}
}

// materialize creates the ActionRecord an enqueued approval describes, or
// returns the one already created.
//
// It is the second of two writes and is safe to repeat. Everything that
// identifies the work comes from the stored approval rather than from this
// call, so a retry after a crash between the two writes builds the same
// request; only the provenance of the materializing call itself — which
// credential it ran under — is this call's.
func (s *ApprovalService) materialize(
	ctx context.Context,
	decision auth.Decision,
	approval ApprovalRecord,
) (ActionRecord, error) {
	dispatch := s.dispatch
	record := cloneActionRecord(approval.Request)
	record.ApprovalRequestDigest = append([]byte(nil), approval.RequestDigest...)
	record.ApprovalPolicyGeneration = approval.ApproverPolicyGeneration
	record.ApproverSubject = approval.ApproverSubject
	record.ApproverActor = approval.ApproverActor
	record.ApproverClientID = approval.ApproverClientID
	record.ApprovedAt = approval.DecidedAt
	record.UpdatedAt = approval.MaterializedAt
	// The credential that commits the record. The request's own credential
	// will normally have expired by the time an approval arrives, and the
	// audit and publication paths pin the record to a live one; the
	// fingerprint is unchanged, because equivalentEnqueue has just proven
	// this caller's fingerprint is the request's.
	record.AuthorizationExpiresAt = decision.AuthenticationExpires()
	record.TransitionRequestID = decision.RequestID()
	record.TransitionCorrelationID = decision.CorrelationID()
	if err := record.Validate(); err != nil {
		return ActionRecord{}, err
	}
	current, readErr := dispatch.store.GetAction(ctx, record.ID)
	if readErr == nil {
		return s.replayMaterialized(ctx, current, approval)
	}
	if !errors.Is(readErr, ErrActionNotFound) {
		return ActionRecord{}, materializeRead(readErr)
	}
	if err := dispatch.recorder.RecordAction(ctx, ActionAudit{
		Phase: "approval_enqueue", Operation: auth.OperationDispatch, Record: record,
	}); err != nil {
		return ActionRecord{}, errors.Join(ErrRecordingUnavailable, err)
	}
	stored, err := dispatch.store.ApplyAction(ctx, DispatchMutation{
		Token: transitionToken(
			"approval-enqueue", record.ID, record.IdempotencyKey, record.Version),
		ExpectedVersion: 0, TransitionKind: "action.enqueued", Record: record,
	})
	if err != nil {
		// A concurrent retry of this same materialization may have won; it
		// wrote the same work under a different credential, and that is this
		// request's record. Anything else at the identity is not, and
		// replayMaterialized says so.
		//
		// Re-read on any failure, not only on the fleet sentinel: the durable
		// store reports a lost first write through the coordination guard as
		// a shoal conflict ("fleet registry conflict"), which is not
		// ErrActionConflict, so matching the sentinel alone sent concurrent
		// re-requests of one approval home with a conflict for work that had
		// in fact been created. The read decides, not the error.
		if current, readErr := dispatch.store.GetAction(
			context.WithoutCancel(ctx), record.ID); readErr == nil {
			return s.replayMaterialized(ctx, current, approval)
		}
		// A conflict with nothing yet readable is a concurrent
		// materialization still landing, or several that collided and all
		// lost. Either way the next attempt reads what is there, so the
		// caller retries rather than reporting a conflict for work that is
		// about to exist.
		if shoal.IsErrorCode(err, shoal.ErrorConflict) {
			return ActionRecord{}, errors.Join(errMaterializeRaced, err)
		}
		return ActionRecord{}, materializeRead(err)
	}
	if err := dispatch.publishTransition(
		context.WithoutCancel(ctx), "action.enqueued", stored,
	); err != nil {
		return ActionRecord{}, errors.Join(ErrActionCommitted, err)
	}
	return cloneActionRecord(stored), nil
}

// replayMaterialized returns an existing ActionRecord if and only if it is the
// one this approval materialized, wherever its lifecycle has since got to.
func (s *ApprovalService) replayMaterialized(
	ctx context.Context, current ActionRecord, approval ApprovalRecord,
) (ActionRecord, error) {
	if !equivalentEnqueue(current, approval.Request) ||
		!bytes.Equal(current.ApprovalRequestDigest, approval.RequestDigest) ||
		current.ApproverSubject != approval.ApproverSubject ||
		current.ApproverActor != approval.ApproverActor ||
		current.ApproverClientID != approval.ApproverClientID ||
		!current.ApprovedAt.Equal(approval.DecidedAt) {
		// Something else is at the identity: an action enqueued there by
		// another route while this request was held. The approval cannot
		// become work at an identity it does not own.
		return ActionRecord{}, ErrActionConflict
	}
	if err := s.dispatch.publishTransition(
		context.WithoutCancel(ctx), actionEventKind(current), current,
	); err != nil {
		return ActionRecord{}, errors.Join(ErrActionCommitted, err)
	}
	return cloneActionRecord(current), nil
}

// expire records that a request closed without becoming work.
func (s *ApprovalService) expire(
	ctx context.Context,
	operation auth.Operation,
	decision auth.Decision,
	current ApprovalRecord,
	now time.Time,
) (ApprovalRecord, error) {
	next := CloneApprovalRecord(current)
	next.Version++
	next.State = ApprovalExpired
	next.UpdatedAt = notBefore(now, current)
	return s.commit(ctx, "approval_expiry", operation, decision, next, current.Version)
}

// Decide approves or refuses a pending request.
//
// The order of the checks is the order of the questions. May this caller
// decide this request at all — approve on its scope, the target still
// resolving, and independent of everyone the request involves. Then has the
// window closed, which is recorded as expiry rather than answered as a
// refusal. Then is the record still undecided, where a decision identical to
// the one already made replays and any other conflicts. Only then is what the
// approver reviewed compared with what is on the record.
func (s *ApprovalService) Decide(
	ctx context.Context, request ApprovalDecisionRequest,
) (ApprovalRecord, error) {
	dispatch := s.dispatch
	ctx, cancel := dispatch.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := dispatch.begin(
		ctx, auth.OperationActionApprove, request.Context)
	if err != nil {
		return ApprovalRecord{}, err
	}
	if err := validateOpaque("approval ID", request.ID, false); err != nil {
		return ApprovalRecord{}, err
	}
	if len(request.RequestDigest) != sha256.Size {
		return ApprovalRecord{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "approval request digest is not a digest")
	}
	if request.PolicyGeneration <= 0 {
		return ApprovalRecord{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "approval policy generation is invalid")
	}
	if err := request.Verdict.validate(); err != nil {
		return ApprovalRecord{}, err
	}
	for attempt := 0; attempt < maxApprovalAttempts; attempt++ {
		current, err := s.store.GetApproval(ctx, request.ID)
		if err != nil {
			if errors.Is(err, ErrApprovalNotFound) {
				return ApprovalRecord{}, auth.ObjectNotFound()
			}
			return ApprovalRecord{}, err
		}
		if _, err := s.eligibleApprover(ctx, decision, current, now); err != nil {
			return ApprovalRecord{}, err
		}
		result, err := s.decide(ctx, decision, current, request, now)
		if errors.Is(err, errApprovalRaced) {
			continue
		}
		return result, err
	}
	return ApprovalRecord{}, approvalConflict()
}

// notBefore clamps a transition time to the record's latest, so a replica
// whose clock is behind the one that wrote the record still writes a record
// whose timestamps are ordered.
//
// The clamp moves time forward, never back, and only by the skew between two
// replicas. It never reaches past ExpiresAt: every caller has already checked
// now < ExpiresAt, and every timestamp on the record is also before it.
// Refusing instead — which Validate did — turned a few seconds of skew into a
// 400 for an approver that had done nothing wrong.
func notBefore(now time.Time, record ApprovalRecord) time.Time {
	for _, earlier := range []time.Time{
		record.RequestedAt, record.UpdatedAt, record.DecidedAt,
		record.MaterializedAt,
	} {
		if now.Before(earlier) {
			now = earlier
		}
	}
	return now
}

// errApprovalRaced is internal: the record moved under a compare-and-set and
// the decision has to be re-evaluated against what is there now.
var errApprovalRaced = errors.New("fleet approval: record moved")

func (s *ApprovalService) decide(
	ctx context.Context,
	decision auth.Decision,
	current ApprovalRecord,
	request ApprovalDecisionRequest,
	now time.Time,
) (ApprovalRecord, error) {
	if current.State == ApprovalPending && !now.Before(current.ExpiresAt) {
		if _, err := s.expire(
			ctx, auth.OperationActionApprove, decision, current, now,
		); err != nil {
			if errors.Is(err, ErrApprovalConflict) {
				return ApprovalRecord{}, errApprovalRaced
			}
			return ApprovalRecord{}, err
		}
		return ApprovalRecord{}, approvalExpired()
	}
	if current.State != ApprovalPending {
		// A duplicate of the decision already on the record replays it, so an
		// approver whose response was lost can safely send it again. It does
		// not perform anything a second time because deciding never performs
		// anything; only the requester's re-request materializes, once.
		if sameApprovalDecision(decision, current, request) {
			return CloneApprovalRecord(current), nil
		}
		if current.State == ApprovalExpired {
			return ApprovalRecord{}, approvalExpired()
		}
		return ApprovalRecord{}, approvalConflict()
	}
	// The generation the approver reviewed under, and the one it holds now,
	// must both be the request's. An approval under a superseded generation
	// would be a statement about a policy that is no longer in force.
	if request.PolicyGeneration != current.PolicyGeneration ||
		decision.PolicyGeneration() != current.PolicyGeneration {
		return ApprovalRecord{}, shoal.WrapError(
			shoal.ErrorConflict,
			"approval policy generation does not match the request",
			ErrApprovalSuperseded)
	}
	if !bytes.Equal(request.RequestDigest, current.RequestDigest) {
		return ApprovalRecord{}, approvalConflict()
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return ApprovalRecord{}, err
	}
	next := CloneApprovalRecord(current)
	next.Version++
	next.State = ApprovalApproved
	if request.Verdict == ApprovalVerdictRefuse {
		next.State = ApprovalRefused
	}
	next.Verdict = request.Verdict
	next.ApproverSubject = decision.Subject()
	next.ApproverActor = decision.Actor()
	next.ApproverClientID = decision.ClientID()
	next.ApproverFingerprint = fingerprint
	next.ApproverPolicyGeneration = decision.PolicyGeneration()
	next.DecidedAt = notBefore(now, current)
	next.DecisionRequestID = decision.RequestID()
	next.DecisionCorrelationID = decision.CorrelationID()
	next.UpdatedAt = next.DecidedAt
	stored, err := s.commit(
		ctx, "approval_decision", auth.OperationActionApprove,
		decision, next, current.Version)
	if errors.Is(err, ErrApprovalConflict) {
		return ApprovalRecord{}, errApprovalRaced
	}
	return stored, err
}

// sameApprovalDecision reports whether a decided record is this decision by
// this approver, already made.
func sameApprovalDecision(
	decision auth.Decision,
	current ApprovalRecord,
	request ApprovalDecisionRequest,
) bool {
	return current.Verdict == request.Verdict &&
		current.ApproverSubject == decision.Subject() &&
		current.ApproverActor == decision.Actor() &&
		current.ApproverClientID == decision.ClientID() &&
		bytes.Equal(current.RequestDigest, request.RequestDigest)
}

// eligibleApprover decides whether this caller may decide this request.
//
// Three conditions, and each closes a different way to approve your own work.
//
// Approve must be authorized on the request's scope, and the request's target
// must still resolve under it at the generation it names. Both failures answer
// as an absent record does: a caller that may not decide a request is not told
// it exists.
//
// The approver must share no identity with the request: not with the
// requester's subject, actor or delegation chain, and not with the agent that
// would perform it or the principal that registered it. Equality is across
// fields, as in internal/decisionadjudication: acting as somebody's delegate,
// or changing only the client, does not establish independence from them. A
// decision carrying a delegation chain is refused outright — an approval is a
// human's own judgement, and delegation cannot manufacture a second role.
//
// And the approver must fail the operations that create or perform this work
// on the same scope. Holding approve alone separates nothing where scopes are
// shared, which they are for every OIDC-minted principal: two people with
// dispatch and approve on one workspace could each approve the other's request
// and then enqueue anything they liked by trading approvals. Execute is
// included as well as dispatch and invoke, because an approver that may claim
// and perform the work it approved has not been separated from the effect.
func (s *ApprovalService) eligibleApprover(
	ctx context.Context,
	decision auth.Decision,
	record ApprovalRecord,
	now time.Time,
) (Descriptor, error) {
	request := record.Request
	// First, and as a refusal rather than a concealment: the caller has done
	// something no approver may do, whatever the record.
	if s.narrowed(ctx) {
		return Descriptor{}, approvalUnderNarrowing()
	}
	if err := decision.AuthorizeObject(
		auth.OperationActionApprove, auth.ResourceRequest{
			AuthorizationDomain: decision.AuthorizationDomain(),
			SourceID:            request.SourceID,
			PolicyID:            request.PolicyID,
			ObjectID:            shoal.ID(record.ID),
		}, now); err != nil {
		return Descriptor{}, auth.ObjectNotFound()
	}
	descriptor, _, _, err := s.dispatch.registry.resolveActionBinding(
		ctx, decision, request.AgentID, request.AgentGeneration,
		request.Capability, request.Action, request.SourceID, request.PolicyID,
		request.ObjectID, auth.OperationActionApprove, now,
	)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) ||
			shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			return Descriptor{}, auth.ObjectNotFound()
		}
		return Descriptor{}, err
	}
	if len(decision.OnBehalfOf()) > 0 {
		return Descriptor{}, shoal.NewError(
			shoal.ErrorUnauthorized,
			"an approval cannot be made on behalf of another identity")
	}
	involved := map[shoal.ID]struct{}{
		request.Subject: {}, request.Actor: {},
		request.AgentID: {}, descriptor.Subject: {}, descriptor.Actor: {},
	}
	for _, identity := range request.OnBehalfOf {
		involved[identity] = struct{}{}
	}
	// And every ancestor in the agent's delegation chain: its ID and the
	// principal that registered it. A delegated agent acts with authority its
	// parent granted, so the parent agent and the parent's registrant are as
	// much a party to the work as the child's own registrant is. The chain is
	// read from the registry rather than from the request, and a chain that
	// cannot be read refuses rather than passes.
	chain, err := s.dispatch.registry.activeChain(ctx, descriptor.ID, now)
	if err != nil || len(chain) == 0 || chain[0].Generation != descriptor.Generation {
		return Descriptor{}, auth.ObjectNotFound()
	}
	for _, ancestor := range chain {
		involved[ancestor.ID] = struct{}{}
		involved[ancestor.Subject] = struct{}{}
		involved[ancestor.Actor] = struct{}{}
	}
	for _, identity := range []shoal.ID{decision.Subject(), decision.Actor()} {
		if _, overlaps := involved[identity]; overlaps {
			return Descriptor{}, shoal.NewError(
				shoal.ErrorUnauthorized,
				"approver is not independent of the request")
		}
	}
	resource := auth.ResourceRequest{
		AuthorizationDomain: decision.AuthorizationDomain(),
		SourceID:            request.SourceID,
		PolicyID:            request.PolicyID,
		ObjectID:            request.ObjectID,
	}
	for _, operation := range []auth.Operation{
		auth.OperationDispatch, auth.OperationInvoke, auth.OperationExecute,
	} {
		if decision.AuthorizeObject(operation, resource, now) == nil {
			return Descriptor{}, shoal.NewError(
				shoal.ErrorUnauthorized,
				"approver may also create or perform the work it would approve")
		}
	}
	return descriptor, nil
}

// Status returns one approval to the requester or to a principal eligible to
// decide it, and answers everyone else as though it did not exist.
//
// It reports the state the request is effectively in, not merely the one
// stored. Expiry is written lazily, and a request whose target or policy
// generation has moved, or whose deadline has passed, can never progress
// although its row still says pending or approved; reporting the row would
// tell a reader something live that is not. Nothing is written here.
func (s *ApprovalService) Status(
	ctx context.Context, request ApprovalStatusRequest,
) (ApprovalStatus, error) {
	dispatch := s.dispatch
	ctx, cancel := dispatch.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := dispatch.begin(ctx, auth.OperationDispatch, request.Context)
	if err != nil {
		if !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
			return ApprovalStatus{}, err
		}
		decision, now, err = dispatch.begin(
			ctx, auth.OperationActionApprove, request.Context)
		if err != nil {
			return ApprovalStatus{}, err
		}
	}
	if err := validateOpaque("approval ID", request.ID, false); err != nil {
		return ApprovalStatus{}, err
	}
	current, err := s.store.GetApproval(ctx, request.ID)
	if err != nil {
		if errors.Is(err, ErrApprovalNotFound) {
			return ApprovalStatus{}, auth.ObjectNotFound()
		}
		return ApprovalStatus{}, err
	}
	requester := sameActionPrincipal(decision, current.Request) &&
		decision.AuthorizeObject(auth.OperationDispatch, auth.ResourceRequest{
			AuthorizationDomain: decision.AuthorizationDomain(),
			SourceID:            current.Request.SourceID,
			PolicyID:            current.Request.PolicyID,
			ObjectID:            shoal.ID(current.ID),
		}, now) == nil
	if !requester {
		// The approver path, which is also where a target that has moved
		// stops being visible: eligibility resolves the binding at the
		// request's generation. An approver therefore sees a request only
		// while it is decidable in principle, and the requester always.
		if _, err := s.eligibleApprover(ctx, decision, current, now); err != nil {
			if shoal.IsErrorCode(err, shoal.ErrorNotFound) {
				return ApprovalStatus{}, auth.ObjectNotFound()
			}
			if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
				if s.narrowed(ctx) {
					return ApprovalStatus{}, err
				}
				return ApprovalStatus{}, auth.ObjectNotFound()
			}
			return ApprovalStatus{}, err
		}
	}
	state, condition, err := s.effectiveState(ctx, decision, current, now)
	if err != nil {
		return ApprovalStatus{}, err
	}
	return ApprovalStatus{
		Approval: CloneApprovalRecord(current), State: state, Condition: condition,
	}, nil
}

// effectiveState computes what a stored approval amounts to now.
func (s *ApprovalService) effectiveState(
	ctx context.Context,
	decision auth.Decision,
	current ApprovalRecord,
	now time.Time,
) (ApprovalState, ApprovalCondition, error) {
	switch current.State {
	case ApprovalRefused, ApprovalExpired:
		return current.State, ApprovalConditionNone, nil
	case ApprovalEnqueued:
		_, err := s.dispatch.store.GetAction(ctx, current.ID)
		if err == nil {
			// The work exists; where it has got to is the action's status.
			return ApprovalEnqueued, ApprovalConditionNone, nil
		}
		if !errors.Is(err, ErrActionNotFound) {
			return "", "", err
		}
		if condition := s.unreachable(ctx, decision, current, now); condition !=
			ApprovalConditionNone {
			return ApprovalUnresolvable, condition, nil
		}
		return ApprovalEnqueued, ApprovalConditionAwaitingAction, nil
	}
	// Pending or approved.
	if !now.Before(current.ExpiresAt) {
		return ApprovalExpired, ApprovalConditionWindowClosed, nil
	}
	if condition := s.unreachable(ctx, decision, current, now); condition !=
		ApprovalConditionNone {
		return ApprovalUnresolvable, condition, nil
	}
	return current.State, ApprovalConditionNone, nil
}

// unreachable reports why a request can no longer progress, or none.
//
// The agent is read without the caller's authority: whether the target still
// exists at the request's generation is a fact about the registry, not about
// who is asking, and the caller has already been admitted to this record.
func (s *ApprovalService) unreachable(
	ctx context.Context,
	decision auth.Decision,
	current ApprovalRecord,
	now time.Time,
) ApprovalCondition {
	if !now.Before(current.Request.Deadline) {
		return ApprovalConditionDeadlinePassed
	}
	descriptor, err := s.dispatch.registry.active(
		ctx, current.Request.AgentID, now)
	if err != nil || descriptor.Generation != current.Request.AgentGeneration {
		return ApprovalConditionTargetMoved
	}
	if decision.PolicyGeneration() != current.PolicyGeneration {
		return ApprovalConditionPolicyMoved
	}
	return ApprovalConditionNone
}

// Pending lists requests this caller may decide and that are still decidable.
//
// A request the caller is not eligible to decide is skipped rather than
// refused, including its own: the page is the caller's queue, not a view of
// everyone's. A page may filter down to nothing; only an empty continuation
// ends the scan.
func (s *ApprovalService) Pending(
	ctx context.Context, request PendingApprovalsRequest,
) (PendingApprovalsPage, error) {
	dispatch := s.dispatch
	ctx, cancel := dispatch.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := dispatch.begin(
		ctx, auth.OperationActionApprove, request.Context)
	if err != nil {
		return PendingApprovalsPage{}, err
	}
	// Refused outright rather than left to the per-record check, which would
	// skip every record and answer with an empty queue — the wrong answer to
	// a caller whose queue is not empty but whose credential is unfit.
	if s.narrowed(ctx) {
		return PendingApprovalsPage{}, approvalUnderNarrowing()
	}
	if request.Limit <= 0 || request.Limit > MaxApprovalListResults {
		return PendingApprovalsPage{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "pending approval limit is outside its bound")
	}
	page, err := s.store.ScanApprovals(ctx, request.After, request.Limit)
	if err != nil {
		return PendingApprovalsPage{}, err
	}
	result := PendingApprovalsPage{Next: append([]byte(nil), page.Next...)}
	for _, record := range page.Approvals {
		if record.State != ApprovalPending || !now.Before(record.ExpiresAt) {
			continue
		}
		if _, err := s.eligibleApprover(ctx, decision, record, now); err != nil {
			if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) ||
				shoal.IsErrorCode(err, shoal.ErrorNotFound) {
				continue
			}
			return PendingApprovalsPage{}, err
		}
		result.Approvals = append(result.Approvals, CloneApprovalRecord(record))
	}
	return result, nil
}

// commit writes one approval transition, audit first, as every dispatch
// mutation is.
func (s *ApprovalService) commit(
	ctx context.Context,
	phase string,
	operation auth.Operation,
	decision auth.Decision,
	record ApprovalRecord,
	expectedVersion uint64,
) (ApprovalRecord, error) {
	if err := record.Validate(); err != nil {
		return ApprovalRecord{}, err
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return ApprovalRecord{}, err
	}
	if err := s.recorder.RecordApproval(ctx, ApprovalAudit{
		Phase: phase, Operation: operation, Record: CloneApprovalRecord(record),
		Subject: decision.Subject(), Actor: decision.Actor(),
		ClientID: decision.ClientID(), OnBehalfOf: decision.OnBehalfOf(),
		RequestID:                decision.RequestID(),
		AuthorizationFingerprint: fingerprint,
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
	}); err != nil {
		return ApprovalRecord{}, errors.Join(ErrRecordingUnavailable, err)
	}
	return s.store.ApplyApproval(ctx, ApprovalMutation{
		Token: transitionToken(
			"approval-"+phase, record.ID, nil, record.Version),
		ExpectedVersion: expectedVersion, Record: record,
	})
}

func receiptFor(record ApprovalRecord, action ActionRecord) ApprovalReceipt {
	receipt := ApprovalReceipt{
		State: record.State, ID: append([]byte(nil), record.ID...),
		RequestDigest:    append([]byte(nil), record.RequestDigest...),
		PolicyGeneration: record.PolicyGeneration,
		ExpiresAt:        record.ExpiresAt,
	}
	if record.State == ApprovalEnqueued {
		receipt.Action = cloneActionRecord(action)
	}
	return receipt
}

func approvalConflict() error {
	return shoal.WrapError(
		shoal.ErrorConflict, "approval does not match the held request",
		ErrApprovalConflict)
}

func approvalUnderNarrowing() error {
	return shoal.NewError(
		shoal.ErrorUnauthorized,
		"an approval cannot be decided or listed under workspace settings")
}

func approvalExpired() error {
	return shoal.WrapError(
		shoal.ErrorConflict, "approval window has closed", ErrApprovalExpired)
}
