// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash"
	"slices"
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type DispatchService struct {
	store    DispatchStore
	outbox   ActionTransitionStore
	registry *Service
	resolver auth.Resolver
	recorder ActionRecorder
	events   ActionEventPublisher
	clock    func() time.Time
}

func NewDispatchService(config DispatchConfig) (*DispatchService, error) {
	if config.Store == nil || config.Registry == nil || config.Resolver == nil ||
		config.Recorder == nil || config.Events == nil || config.Clock == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet dispatch dependencies are required")
	}
	outbox, ok := config.Store.(ActionTransitionStore)
	if !ok {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"durable fleet action transition store is required")
	}
	service := &DispatchService{
		store: config.Store, registry: config.Registry, resolver: config.Resolver,
		recorder: config.Recorder, events: config.Events, clock: config.Clock,
		outbox: outbox,
	}
	return service, nil
}

func (s *DispatchService) Enqueue(ctx context.Context, request EnqueueRequest) (ActionRecord, error) {
	return s.enqueue(ctx, request, auth.OperationDispatch)
}

func (s *DispatchService) enqueue(
	ctx context.Context,
	request EnqueueRequest,
	operation auth.Operation,
) (ActionRecord, error) {
	ctx, cancel := s.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := s.begin(ctx, operation, request.Context)
	if err != nil {
		return ActionRecord{}, err
	}
	// The admission region is refused here, and this is the only place it has
	// to be: enqueue is the sole entry point at which a caller names a durable
	// action it does not already own. Everything else takes an ID it must
	// already hold, and answers a foreign or absent one identically.
	//
	// Refused before the store is read, and without regard to what is there,
	// because the answer is the whole point. This read has no principal check —
	// it cannot have one, since a caller legitimately enqueues at an ID nobody
	// holds — so a conditional refusal would still separate occupied from
	// absent. That was the live oracle: an admission ID is derivable from a
	// principal tuple that is knowable, so a caller could compute a victim's
	// admission ID, submit it here, and read occupancy off conflict versus
	// success. It could also squat an unheld one and deny the victim its own
	// admission by name.
	if reservedAdmissionID(request.ID) {
		return ActionRecord{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "action ID is reserved")
	}
	record, _, err := s.queuedRecord(ctx, decision, request, operation, now)
	if err != nil {
		return ActionRecord{}, err
	}
	if current, readErr := s.store.GetAction(ctx, request.ID); readErr == nil {
		if equivalentEnqueue(current, record) {
			if err := s.publishTransition(
				context.WithoutCancel(ctx), actionEventKind(current), current,
			); err != nil {
				return ActionRecord{}, errors.Join(ErrActionCommitted, err)
			}
			return cloneActionRecord(current), nil
		}
		return ActionRecord{}, ErrActionConflict
	} else if !errors.Is(readErr, ErrActionNotFound) {
		return ActionRecord{}, readErr
	}
	if err := s.recorder.RecordAction(ctx, ActionAudit{Phase: "enqueue_admission", Operation: operation, Record: record}); err != nil {
		return ActionRecord{}, errors.Join(ErrRecordingUnavailable, err)
	}
	stored, err := s.store.ApplyAction(ctx, DispatchMutation{
		Token: transitionToken(
			"enqueue", request.ID, request.IdempotencyKey, record.Version),
		ExpectedVersion: 0, TransitionKind: "action.enqueued", Record: record,
	})
	if err != nil {
		return ActionRecord{}, err
	}
	if err := s.publishTransition(
		context.WithoutCancel(ctx), "action.enqueued", stored,
	); err != nil {
		return ActionRecord{}, errors.Join(ErrActionCommitted, err)
	}
	return cloneActionRecord(stored), nil
}

// queuedRecord validates a dispatch request and builds the version-1 record an
// enqueue would commit, without committing anything.
//
// It is split out so admission can reach the same record without going through
// the queued state. Every check an enqueue makes has to be made before an
// admission writes anything, and the alternative — admission repeating the
// validation itself — is how a second entry point ends up admitting an input
// the first would have refused. The resolved Action comes back with it because
// admission needs the declared effect ceiling, and resolving twice would ask
// the registry a question it has already answered.
func (s *DispatchService) queuedRecord(
	ctx context.Context,
	decision auth.Decision,
	request EnqueueRequest,
	operation auth.Operation,
	now time.Time,
) (ActionRecord, Action, error) {
	if err := validateOpaque("action ID", request.ID, false); err != nil {
		return ActionRecord{}, Action{}, err
	}
	if err := validateOpaque("action idempotency key", request.IdempotencyKey, false); err != nil {
		return ActionRecord{}, Action{}, err
	}
	if request.AgentGeneration <= 0 {
		return ActionRecord{}, Action{}, shoal.NewError(shoal.ErrorInvalidArgument, "agent generation must be positive")
	}
	if err := validateName("capability", request.Capability); err != nil {
		return ActionRecord{}, Action{}, err
	}
	if err := validateName("action", request.Action); err != nil {
		return ActionRecord{}, Action{}, err
	}
	if request.Context.Deadline.Sub(now) > MaxActionDeadline {
		return ActionRecord{}, Action{}, shoal.NewError(shoal.ErrorInvalidArgument, "action deadline exceeds its bound")
	}
	// Binding, not execution: queueing work for an executor that runs out of
	// process must not require it to be runnable here.
	descriptor, action, _, err := s.registry.resolveActionBinding(
		ctx, decision, request.AgentID, request.AgentGeneration,
		request.Capability, request.Action, request.SourceID, request.PolicyID,
		request.ObjectID, operation, now,
	)
	if err != nil {
		return ActionRecord{}, Action{}, err
	}
	input, err := validateAgainstSchema(action.InputSchema, request.Input, "action input", MaxActionPayloadBytes)
	if err != nil {
		return ActionRecord{}, Action{}, err
	}
	reason, err := interaction.NewReason(request.Context.ReasonCode, request.Context.ReasonDetail)
	if err != nil {
		return ActionRecord{}, Action{}, err
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return ActionRecord{}, Action{}, err
	}
	return ActionRecord{
		ID: append([]byte(nil), request.ID...), IdempotencyKey: append([]byte(nil), request.IdempotencyKey...),
		Version: 1, State: DispatchQueued, AgentID: descriptor.ID,
		AgentGeneration: descriptor.Generation, Capability: request.Capability, Action: request.Action,
		SourceID: append([]byte(nil), request.SourceID...), PolicyID: append([]byte(nil), request.PolicyID...),
		ObjectID: request.ObjectID, Input: input, Subject: decision.Subject(), Actor: decision.Actor(),
		ClientID: decision.ClientID(), OnBehalfOf: decision.OnBehalfOf(),
		AuthorizationFingerprint: fingerprint, PolicyGeneration: decision.PolicyGeneration(),
		AuthorizationExpiresAt: decision.AuthenticationExpires(),
		AuthorizedOperations:   decisionOperations(decision, operation),
		RequestID:              decision.RequestID(), CorrelationID: decision.CorrelationID(),
		Reason: reason, Deadline: request.Context.Deadline.UTC(), CreatedAt: now, UpdatedAt: now,
		ExecutorKey: executorKey(request.ID, request.IdempotencyKey),
	}, action, nil
}

func (s *DispatchService) Invoke(ctx context.Context, request InvokeRequest) (ActionRecord, error) {
	queued, err := s.enqueue(ctx, request.Enqueue, auth.OperationInvoke)
	if err != nil {
		return ActionRecord{}, err
	}
	if queued.State.terminal() {
		decision, now, authorizeErr := s.begin(ctx, auth.OperationInvoke, request.Enqueue.Context)
		if authorizeErr != nil {
			return ActionRecord{}, authorizeErr
		}
		// The enqueue replay path: the caller is by definition the principal
		// that enqueued, so the principal requirement is kept rather than
		// relaxed with the claimant routes.
		current, _, currentErr := s.authorizedCurrent(
			ctx, decision, queued.ID, auth.OperationInvoke, true, now,
		)
		if currentErr != nil {
			return ActionRecord{}, currentErr
		}
		if err := s.publishTransition(
			context.WithoutCancel(ctx), actionEventKind(current), current,
		); err != nil {
			return ActionRecord{}, errors.Join(ErrActionCommitted, err)
		}
		return current, nil
	}
	if queued.State == DispatchClaimed &&
		bytes.Equal(queued.ClaimID, request.ClaimID) &&
		s.clock().UTC().Before(queued.ClaimLeaseUntil) {
		return s.ExecuteClaim(ctx, queued)
	}
	claimed, err := s.Claim(ctx, ClaimRequest{
		ID: queued.ID, ExpectedVersion: queued.Version, ClaimID: request.ClaimID,
		Lease: request.Lease, Context: request.Enqueue.Context,
	})
	if err != nil {
		return ActionRecord{}, err
	}
	return s.ExecuteClaim(ctx, claimed)
}

func (s *DispatchService) Claim(ctx context.Context, request ClaimRequest) (ActionRecord, error) {
	ctx, cancel := s.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := s.beginClaimant(ctx, request.Context)
	if err != nil {
		return ActionRecord{}, err
	}
	if err := validateOpaque("action ID", request.ID, false); err != nil {
		return ActionRecord{}, err
	}
	if err := validateOpaque("claim ID", request.ClaimID, false); err != nil {
		return ActionRecord{}, err
	}
	if request.ExpectedVersion == 0 || request.Lease <= 0 || request.Lease > MaxActionClaimTTL {
		return ActionRecord{}, shoal.NewError(shoal.ErrorInvalidArgument, "claim version or lease is invalid")
	}
	current, claimedAction, err := s.authorizedClaimant(ctx, decision, request.ID, now)
	if err != nil {
		return ActionRecord{}, err
	}
	// An admission is not dispatch work. Merging the two claim paths gave
	// dispatch's reclaim semantics reach over admission records, and a reclaim
	// means nothing for an admission: the grant was made to one caller which was
	// told it may perform an effect, and nobody else can finish that. Only the
	// original caller knows whether the effect happened, so a second party
	// taking the record and reporting an outcome would be recording a fiction.
	//
	// An expired admission is abandoned, not reclaimable, and a claimed record
	// with a lapsed lease is the honest statement of that — it is exactly what
	// Outstanding reports as Expired. Refused as not-found, because the caller
	// does hold the record and saying so would separate an admission it owns
	// from an action ID that does not exist.
	if current.isAdmission() {
		return ActionRecord{}, auth.ObjectNotFound()
	}
	if current.Version != request.ExpectedVersion {
		// A replayed claim: this caller's own claim landed and the response
		// was lost, so return the record it already holds rather than a
		// conflict against its own write.
		//
		// The claimant check belongs here for the same reason it belongs in
		// completeClaim. Without it the branch keys on ClaimID, version and
		// lease alone — all three of which a second execute-holder knows,
		// because Pull returns a claimed record whose lease has lapsed with
		// its ClaimID and version intact. That caller would receive the full
		// record of a live claim it does not hold, including the action's
		// input, which Pull deliberately withholds by excluding live-claimed
		// records from the page.
		if current.State == DispatchClaimed &&
			current.Version == request.ExpectedVersion+1 &&
			bytes.Equal(current.ClaimID, request.ClaimID) &&
			current.ClaimLease == request.Lease &&
			standingOn(decision, current) {
			if err := s.publishTransition(
				context.WithoutCancel(ctx), "action.claimed", current,
			); err != nil {
				return ActionRecord{}, errors.Join(ErrActionCommitted, err)
			}
			return cloneActionRecord(current), nil
		}
		// A version mismatch is the first answer this route produces, so it is
		// also the first place the record's existence can leak. A caller with
		// standing gets the conflict it needs in order to re-read and retry; a
		// caller without any is told what an absent action is told.
		//
		// This costs an execute-holder nothing it should have. A record it may
		// legitimately take is queued or has a lapsed claim, and Pull hands it
		// that record with its current version — so it arrives here with the
		// right version and never sees this branch. Reaching it at all means
		// guessing, which is the probe being refused.
		if !standingOn(decision, current) {
			return ActionRecord{}, auth.ObjectNotFound()
		}
		return ActionRecord{}, ErrActionConflict
	}
	if current.State == DispatchClaimed && now.Before(current.ClaimLeaseUntil) {
		return ActionRecord{}, ErrActionConflict
	}
	// Terminal and past-deadline records are the two states Pull does not
	// return, so for a caller with no standing these are the only answers that
	// disclose an action it could not otherwise observe. A live-claimed or
	// queued record is already visible to every execute-holder in the scope
	// through Pull, so ErrActionConflict above tells such a caller nothing new.
	if current.State.terminal() {
		if !standingOn(decision, current) {
			return ActionRecord{}, auth.ObjectNotFound()
		}
		return ActionRecord{}, ErrActionTerminal
	}
	if !now.Before(current.Deadline) {
		if !standingOn(decision, current) {
			return ActionRecord{}, auth.ObjectNotFound()
		}
		return ActionRecord{}, ErrClaimLost
	}
	// Resolved for the declared Effect, which decides whether claiming this
	// action already makes an effect possible.
	//
	// The declaration comes from authorizedClaimant, which resolved it under
	// whichever operation authorized this caller. This used to re-resolve with
	// a hardcoded OperationInvoke under a comment saying it added no
	// authorization — true while invoke was the only way in, and false the
	// moment execute existed, because it then refused every worker authorized
	// under execute.
	next := cloneActionRecord(current)
	next.Version++
	next, err = applyClaim(
		next, claimedAction, request.ClaimID, request.Lease, decision, now)
	if err != nil {
		return ActionRecord{}, err
	}
	if err := s.recorder.RecordAction(ctx, ActionAudit{Phase: "claim_admission", Operation: auth.OperationInvoke, Record: next}); err != nil {
		return ActionRecord{}, errors.Join(ErrRecordingUnavailable, err)
	}
	stored, err := s.store.ApplyAction(ctx, DispatchMutation{
		Token:           transitionToken("claim", request.ID, request.ClaimID, next.Version),
		ExpectedVersion: current.Version, ExpectedFence: current.ClaimFence,
		TransitionKind: "action.claimed", Record: next,
	})
	if err != nil {
		return ActionRecord{}, err
	}
	if err := s.publishTransition(
		context.WithoutCancel(ctx), "action.claimed", stored,
	); err != nil {
		return ActionRecord{}, errors.Join(ErrActionCommitted, err)
	}
	return cloneActionRecord(stored), nil
}

// applyClaim turns a record into a claimed one. It is the only place that
// transition is written.
//
// Both callers reach it: the dispatch Claim mutation and the admission grant,
// which is born claimed in a single write rather than queued and then claimed.
// They were separate once, and they drifted within one release — the egress
// correction below landed on Claim and not on admission, so an egress-only
// admission sat outstanding asserting that no effect was possible, which is the
// exact assertion the flag exists to avoid making. A matched pair of conditions
// is a promise someone has to keep; one function is a fact.
//
// The fence is incremented rather than assigned, so a re-claim advances it and
// a record that has never been claimed lands on one.
func applyClaim(
	record ActionRecord,
	action Action,
	claimID []byte,
	lease time.Duration,
	decision auth.Decision,
	now time.Time,
) (ActionRecord, error) {
	record.State = DispatchClaimed
	// An external-effect action is possibly-effected from the moment it is
	// claimed, not from the moment it is executed.
	//
	// An in-process executor sets this at execution because this process owns
	// the window between admission and effect. A remote worker owns that window
	// itself: once it holds the claim it may act at any time, and if it then
	// goes silent the record must not say the effect certainly did not happen.
	// Without this a lease that expires after the work was done is
	// indistinguishable from one that expired before it started.
	//
	// It is narrowed by the declaration rather than set for every claim. An
	// action that neither mutates externally nor transmits leaves its whole
	// outcome in Shoal's own record, so nothing has to be assumed about it.
	//
	// Egress counts. An earlier version excluded it on the grounds that
	// transmitting is a disclosure rather than an effect to reconcile against,
	// and that was wrong: a worker can transmit and then go silent, and a
	// record saying no effect was possible asserts the one thing nobody knows.
	// Content that left the host cannot be recalled, which makes an
	// unacknowledged possible egress exactly the kind of uncertainty this flag
	// exists to preserve.
	if action.Effects.contains(EffectMutatesExternal) ||
		action.Effects.contains(EffectEgressesContent) {
		record.EffectPossible = true
	}
	record.ClaimID = append([]byte(nil), claimID...)
	// Who holds the claim, as distinct from which claim is held. A re-claim
	// after a lapsed lease overwrites these, which is correct: the new
	// claimant is the one that may report, and the previous one has already
	// lost the fence.
	record.ClaimantSubject = decision.Subject()
	record.ClaimantActor = decision.Actor()
	record.ClaimantClientID = decision.ClientID()
	record.ClaimantOnBehalfOf = append(
		[]shoal.ID(nil), decision.OnBehalfOf()...)
	record.ClaimFence++
	record.ClaimLease = lease
	record.ClaimLeaseUntil = now.Add(lease)
	if record.ClaimLeaseUntil.After(record.Deadline) {
		record.ClaimLeaseUntil = record.Deadline
	}
	record.UpdatedAt = now
	// Actor is deliberately not written here. It names the principal the record
	// belongs to, which sameActionPrincipal compares, and it is not a
	// per-transition field however much the two lines below look like company.
	//
	// This used to assign decision.Actor() and was a no-op: the claimant was by
	// construction the enqueuer, because claiming required the principal to
	// match. #437 made a third party able to claim, and the same line then
	// overwrote the record's principal — permanently, since the completion path
	// clones the record forward and never restores it. The enqueuer was then
	// refused Status and Cancel on its own in-flight action by the predicate
	// that had just been handed the claimant's identity, with no fallback:
	// TeamActions needs an operation fleet principals do not hold.
	//
	// So a claim records who holds the claim in ClaimID and the transition
	// provenance below, and leaves the record's own principal alone.
	record.TransitionRequestID = decision.RequestID()
	record.TransitionCorrelationID = decision.CorrelationID()
	record.AuthorizedOperations = canonicalOperations(append(
		record.AuthorizedOperations,
		decisionOperations(decision, auth.OperationInvoke)...))
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return ActionRecord{}, err
	}
	record.ExecutionFingerprint = fingerprint
	record.ExecutionPolicyGeneration = decision.PolicyGeneration()
	record.ExecutionExpiresAt = decision.AuthenticationExpires()
	return record, nil
}

func (s *DispatchService) ExecuteClaim(ctx context.Context, claimed ActionRecord) (ActionRecord, error) {
	now := s.clock().UTC()
	if claimed.State != DispatchClaimed || !now.Before(claimed.ClaimLeaseUntil) ||
		!now.Before(claimed.Deadline) {
		return ActionRecord{}, ErrClaimLost
	}
	decision, err := s.resolver.Resolve(ctx)
	if err != nil {
		return ActionRecord{}, err
	}
	if !sameActionPrincipal(decision, claimed) {
		return ActionRecord{}, shoal.NewError(shoal.ErrorUnauthorized, "execution identity does not match queued action")
	}
	_, action, executor, err := s.registry.resolveAction(
		ctx, decision, claimed.AgentID, claimed.AgentGeneration,
		claimed.Capability, claimed.Action, claimed.SourceID, claimed.PolicyID,
		claimed.ObjectID, auth.OperationInvoke, now,
	)
	if err != nil {
		return ActionRecord{}, err
	}
	// Absence answers as a record the caller may not act on does, which is what
	// closes the enumeration on this path: the store's own sentinel for an
	// absent identity and object-not-found for a stored admission were two
	// answers to the question this surface exists to stop answering.
	//
	// There was a third refusal here, ahead of the read, for any identity in the
	// reserved span. It is gone. It inferred "an admission, or nothing" from the
	// identity, and the span held ordinary actions before it was reserved — so
	// it made a legitimately created action permanently unexecutable. It was
	// also redundant: a reserved identity that is absent is covered by the
	// normalisation below and one that is stored by the marker check, which is
	// why no mutation of it alone was ever observable.
	current, err := s.store.GetAction(ctx, claimed.ID)
	if err != nil {
		if errors.Is(err, ErrActionNotFound) {
			return ActionRecord{}, auth.ObjectNotFound()
		}
		return ActionRecord{}, err
	}
	// The durable marker identifies admissions regardless of their identity
	// format, including admissions written before the reserved prefix existed.
	// This check rejects those records without treating an ordinary action in
	// the reserved span as an admission.
	//
	// Read from the stored record rather than the argument, so a caller that
	// fabricates a record with the marker stripped does not dodge it. That
	// matters here more than on the other paths: ExecuteClaim takes the record
	// instead of loading it from an ID, and a grant holder knows the ID, the
	// token, the version, the action fields and the deadline, and that a fresh
	// grant sits at fence one.
	if current.isAdmission() {
		return ActionRecord{}, auth.ObjectNotFound()
	}
	if current.State.terminal() && current.Version == claimed.Version+1 &&
		current.ClaimFence == claimed.ClaimFence &&
		bytes.Equal(current.ClaimID, claimed.ClaimID) {
		if err := s.publishTransition(
			context.WithoutCancel(ctx), actionEventKind(current), current,
		); err != nil {
			return ActionRecord{}, errors.Join(ErrActionCommitted, err)
		}
		return cloneActionRecord(current), nil
	}
	if current.Version != claimed.Version ||
		current.ClaimFence != claimed.ClaimFence ||
		!bytes.Equal(current.ClaimID, claimed.ClaimID) ||
		current.State != DispatchClaimed || !now.Before(current.ClaimLeaseUntil) {
		return ActionRecord{}, ErrClaimLost
	}
	if err := s.recorder.RecordAction(ctx, ActionAudit{Phase: "effect_admission", Operation: auth.OperationInvoke, Record: current}); err != nil {
		return ActionRecord{}, errors.Join(ErrRecordingUnavailable, err)
	}
	invocationDecision, err := s.resolver.Resolve(ctx)
	if err != nil {
		return ActionRecord{}, err
	}
	if !sameActionPrincipal(invocationDecision, current) {
		return ActionRecord{}, shoal.NewError(shoal.ErrorUnauthorized, "invocation identity changed")
	}
	_, action, executor, err = s.registry.resolveAction(
		ctx, invocationDecision, current.AgentID, current.AgentGeneration,
		current.Capability, current.Action, current.SourceID, current.PolicyID,
		current.ObjectID, auth.OperationInvoke, s.clock().UTC(),
	)
	if err != nil {
		return ActionRecord{}, err
	}
	executionNow := s.clock().UTC()
	current, err = s.store.GetAction(ctx, claimed.ID)
	if err != nil {
		return ActionRecord{}, err
	}
	if current.Version != claimed.Version ||
		current.ClaimFence != claimed.ClaimFence ||
		!bytes.Equal(current.ClaimID, claimed.ClaimID) ||
		current.State != DispatchClaimed || !executionNow.Before(current.ClaimLeaseUntil) ||
		!executionNow.Before(current.Deadline) {
		return ActionRecord{}, ErrClaimLost
	}
	executionDeadline := current.ClaimLeaseUntil
	if current.Deadline.Before(executionDeadline) {
		executionDeadline = current.Deadline
	}
	executionContext, cancel := context.WithDeadline(ctx, executionDeadline)
	var result ExecutionResult
	var executionErr error
	func() {
		defer cancel()
		result, executionErr = executor.Execute(executionContext, Invocation{
			ActionID: append([]byte(nil), current.ID...), IdempotencyKey: append([]byte(nil), current.ExecutorKey...),
			ClaimFence: current.ClaimFence, AgentID: current.AgentID, AgentGeneration: current.AgentGeneration,
			Capability: current.Capability, Action: current.Action, Input: append(json.RawMessage(nil), current.Input...),
			SourceID: append([]byte(nil), current.SourceID...), PolicyID: append([]byte(nil), current.PolicyID...),
			ObjectID: current.ObjectID,
			Subject:  current.Subject, Actor: current.Actor, ClientID: current.ClientID,
			OnBehalfOf: append([]shoal.ID(nil), current.OnBehalfOf...), RequestID: current.RequestID,
			CorrelationID: current.CorrelationID, Deadline: current.Deadline,
		})
	}()
	return s.applyExecutionResult(ctx, current, action, result, executionErr)
}

// CompleteClaim records the outcome of work a remote executor performed out of
// process.
//
// It exists because dispatch was one-directional. A worker outside this process
// could pull an action, claim it under a fence, perform the work, and then had
// nowhere to report: its lease expired and the action returned to the queue as
// though nothing had happened, while the effect had already occurred. The
// execution boundary (#381) refuses to run external work in process and points
// at dispatch as the alternative; this is the half of dispatch that makes the
// alternative reachable.
//
// Every check ExecuteClaim performs, this performs. The decision is resolved
// and matched to the queued principal, the action is re-resolved through the
// registry against the current generation and lease, and the claim fence and
// version are confirmed before anything is written. The result then goes
// through applyExecutionResult, the same terminal transition the in-process
// path uses, which revalidates the fence after the fact and reports a loss as
// ambiguity rather than overwriting whatever committed in the meantime.
//
// Reporting twice is safe. A worker that commits and then loses its response
// replays the request and gets the committed record back, exactly as a repeated
// ExecuteClaim does, because the terminal state at the expected version under
// the same claim is recognised as the reporter's own work rather than a
// conflict.
// CompleteClaim is the dispatch surface's completion. It refuses an admission,
// which the admission surface closes through its own path.
//
// This is not only about a reclaimed record. An admission token carries the
// action ID, the claim ID and the version, which is everything this request
// needs — so the caller that holds a live grant could always have completed it
// here instead of reporting, skipping the one-shot rule, the exact-replay
// comparison and the malformed-report rejection that the admission path exists
// to apply. The expiry the review traced is one way in; holding your own token
// is the other, and it needs no expiry at all.
func (s *DispatchService) CompleteClaim(ctx context.Context, request CompletionRequest) (ActionRecord, error) {
	return s.completeClaim(ctx, request, true)
}

// completeClaim is the shared body. refuseAdmissions is false only for the
// admission surface, which reaches it having already applied its own
// validation; splitting it that way keeps one completion implementation rather
// than a second one that could validate less.
func (s *DispatchService) completeClaim(
	ctx context.Context,
	request CompletionRequest,
	refuseAdmissions bool,
) (ActionRecord, error) {
	ctx, cancel := s.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := s.beginClaimant(ctx, request.Context)
	if err != nil {
		return ActionRecord{}, err
	}
	if err := validateOpaque("action ID", request.ID, false); err != nil {
		return ActionRecord{}, err
	}
	if err := validateOpaque("claim ID", request.ClaimID, false); err != nil {
		return ActionRecord{}, err
	}
	if request.ExpectedVersion == 0 {
		return ActionRecord{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "completion version is invalid")
	}
	// A failure with no reason is a protocol error. Recording it as a plain
	// failure would lose the only thing the report carried.
	if request.Failed && request.Result.ErrorCode == "" {
		return ActionRecord{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "a failed completion requires an error code")
	}
	current, action, err := s.authorizedClaimant(ctx, decision, request.ID, now)
	if err != nil {
		return ActionRecord{}, err
	}
	if refuseAdmissions && current.isAdmission() {
		return ActionRecord{}, auth.ObjectNotFound()
	}
	// Only the principal holding the claim may report on it, and this runs
	// before the replay branch rather than after.
	//
	// Ordering is the whole point. The replay branch exists so a worker whose
	// response was lost gets its own committed outcome back instead of a
	// conflict, and it keys on version and ClaimID. Placed after it, this
	// check would still refuse the write — but a hijacker's write would
	// already have landed, and the branch would then hand the real worker a
	// 200 carrying the hijacker's output, telling it the work it actually
	// performed was recorded when the record says something else.
	//
	// ErrClaimLost rather than a bespoke error, and rather than
	// ObjectNotFound. The caller demonstrably knows the action exists — it is
	// presenting a version and a ClaimID for it — so concealing existence
	// buys nothing here, while "the claim you are reporting under is not
	// yours" is exactly what ErrClaimLost already means to a worker, and it is
	// already the answer for a fence this caller has lost. A worker that
	// receives it must treat the effect as ambiguous, which is the correct
	// posture.
	//
	// The record's own principal is accepted alongside the claimant, and that
	// is not a loophole being left open — it is the pre-#437 contract, which
	// has to keep working.
	//
	// Claiming and completing were both gated on sameActionPrincipal, so the
	// claimant was always the enqueuer. Every record written before
	// ClaimantSubject existed therefore has an empty claimant chain and a
	// legitimate reporter that is the record's own principal; requiring the
	// claimant alone would refuse all of them. The admission surface relies on
	// the same thing: AdmissionService.Report resolves with
	// authorizedCurrent(..., OperationInvoke, true, ...), which already
	// demands the record's principal, and its grants are claimed and reported
	// by one identity.
	//
	// What this costs is that an enqueuer can still commit an outcome for a
	// claim a worker holds, if it presents the live ClaimID that Status shows
	// it. That is worth stating plainly, and it is not the hole being closed:
	// the enqueuer owns the work, can already cancel it at any moment, and
	// fabricating its own action's outcome harms only itself. The hole was a
	// *third* principal — neither the enqueuer nor the claimant — doing it to
	// a worker that then received a success receipt for the fabrication.
	//
	// Concealed rather than refused. ErrClaimLost would tell a caller with no
	// standing that the action exists, which is the same oracle #398 closed —
	// and this route is reachable by every principal authorized to execute the
	// descriptor, not only by the record's own.
	if !standingOn(decision, current) {
		return ActionRecord{}, auth.ObjectNotFound()
	}
	// A replayed report. The action is already terminal at the version this
	// reporter expected to produce, under this reporter's own claim, so the
	// work is committed and the response was lost. Republish and return it
	// rather than reporting a conflict against the reporter's own write.
	//
	// Only succeeded and failed count, because only applyExecutionResult
	// produces those and only it could have been this reporter's write. Cancel
	// also lands on a terminal state at exactly version+1 while preserving the
	// ClaimID it cancelled, so accepting any terminal state here would hand a
	// late reporter the cancelled record and a 200 — telling it the work it
	// performed was recorded, when the record says the opposite.
	if (current.State == DispatchSucceeded || current.State == DispatchFailed) &&
		current.Version == request.ExpectedVersion+1 &&
		bytes.Equal(current.ClaimID, request.ClaimID) {
		if err := s.publishTransition(
			context.WithoutCancel(ctx), actionEventKind(current), current,
		); err != nil {
			return ActionRecord{}, errors.Join(ErrActionCommitted, err)
		}
		return cloneActionRecord(current), nil
	}
	if current.Version != request.ExpectedVersion ||
		!bytes.Equal(current.ClaimID, request.ClaimID) ||
		current.State != DispatchClaimed {
		return ActionRecord{}, ErrClaimLost
	}
	// The lease and the action deadline are both checked here, before anything
	// else is done. A worker reporting after either has passed has lost the
	// right to write this record: the action may already have been reclaimed
	// and run again.
	//
	// applyExecutionResult would catch this too, at the post-effect fence
	// check, but it would report it as ErrExecutionAmbiguous — which says the
	// service got far enough that it cannot tell whether the write landed.
	// Refusing here keeps a plainly-late report a plain ErrClaimLost, and
	// avoids resolving the registry and assembling a record for a report that
	// was never going to be written.
	//
	// The deadline half is currently implied by the lease half: Claim clamps
	// ClaimLeaseUntil to the action deadline, so a live lease always means a
	// live deadline and no test can distinguish the two clauses. It is kept
	// because it costs nothing and the clamp is maintained in a different
	// function, but it is not load-bearing today and should not be read as a
	// second, independent bound.
	if !now.Before(current.ClaimLeaseUntil) || !now.Before(current.Deadline) {
		return ActionRecord{}, ErrClaimLost
	}
	// The queued principal is confirmed by authorizedCurrent above, which
	// refuses a mismatch as not-found rather than unauthorized so a caller
	// cannot probe for actions belonging to someone else. ExecuteClaim repeats
	// the check because it is handed a record instead of loading one; here it
	// would be dead code, and a check no test can distinguish implies a
	// guarantee that is not actually held at this point.
	//
	// Resolved for the Action, which carries the OutputSchema the report is
	// validated against. authorizedCurrent above already resolved it once and
	// refuses a descriptor revoked, re-registered or narrowed since the claim.
	// The declared schema comes back from it rather than from a second resolve
	// with a hardcoded operation, which would refuse a worker authorized under
	// execute.
	var executionErr error
	if request.Failed {
		executionErr = shoal.NewError(shoal.ErrorInternal, "remote executor reported failure")
	}
	return s.applyExecutionResult(ctx, current, action, request.Result, executionErr)
}

// applyExecutionResult is the terminal transition: it turns an ExecutionResult
// into a committed, audited, published ActionRecord.
//
// Both completion paths go through it — the in-process executor above and the
// remote worker in CompleteClaim — so neither can validate less than the other.
// Splitting this between them is how a remote worker ends up able to record
// output or evidence that an in-process one could not.
//
// The caller has already confirmed the claim is live. This re-confirms it
// anyway: everything after the effect races a lease that may have expired while
// the effect was happening, and the point of the fence is that losing it after
// the fact is reported rather than overwritten.
func (s *DispatchService) applyExecutionResult(
	ctx context.Context,
	current ActionRecord,
	action Action,
	result ExecutionResult,
	executionErr error,
) (ActionRecord, error) {
	finishNow := s.clock().UTC()
	next := cloneActionRecord(current)
	next.Version++
	next.UpdatedAt = finishNow
	next.EffectPossible = true
	if executionErr == nil {
		output, validateErr := validateAgainstSchema(action.OutputSchema, result.Output, "action output", MaxActionOutputBytes)
		if validateErr != nil {
			executionErr = validateErr
			result.ErrorCode = "invalid_executor_output"
		} else {
			next.Output = output
		}
	}
	if err := validateExecutionEvidence(
		result, finishNow, current.ExecutionExpiresAt,
	); err != nil {
		next.Evidence = nil
		next.EvidenceSnapshotID = ""
		next.EvidenceSnapshotAsOf = time.Time{}
		if executionErr == nil {
			executionErr = err
			result.ErrorCode = "invalid_executor_evidence"
		}
	} else {
		next.Evidence = result.Evidence
		next.EvidenceSnapshotID = result.EvidenceSnapshotID
		next.EvidenceSnapshotAsOf = result.EvidenceSnapshotAsOf.UTC()
	}
	if err := validateActionErrorCode(result.ErrorCode); err != nil {
		executionErr = err
		result.ErrorCode = "invalid_executor_error"
	}

	if executionErr == nil && result.ErrorCode == "" {
		next.State = DispatchSucceeded
	} else {
		next.State = DispatchFailed
		next.ErrorCode = result.ErrorCode
		if next.ErrorCode == "" {
			next.ErrorCode = "executor_error"
		}
	}
	latest, readErr := s.store.GetAction(ctx, current.ID)
	if readErr != nil || latest.Version != current.Version ||
		latest.ClaimFence != current.ClaimFence ||
		!bytes.Equal(latest.ClaimID, current.ClaimID) ||
		latest.State != DispatchClaimed ||
		!finishNow.Before(latest.ClaimLeaseUntil) ||
		!finishNow.Before(latest.Deadline) {
		if readErr != nil {
			return ActionRecord{}, errors.Join(ErrExecutionAmbiguous, readErr)
		}
		return ActionRecord{}, errors.Join(ErrExecutionAmbiguous, ErrClaimLost)
	}
	// A current decision and registry state are required again after the
	// effect. Failure is explicitly ambiguous; it is never described as a
	// rollback and the executor idempotency key remains stable for recovery.
	finalDecision, err := s.resolver.Resolve(ctx)
	if err != nil {
		return ActionRecord{}, errors.Join(ErrExecutionAmbiguous, err)
	}
	// Still authorized by either of the routes that let the caller take this
	// work, re-evaluated against a decision resolved after the effect.
	//
	// This was sameActionPrincipal plus a resolveActionBinding with a hardcoded
	// OperationInvoke, and it is the most expensive place that pairing could
	// have been wrong: it runs *after* the external effect, so an executor
	// refused here has performed the work and is then told its completion is
	// ambiguous. A worker authorized under execute would have failed every
	// completion at the last step, which looks exactly like the lease-loss case
	// the ambiguity is reserved for.
	//
	// claimableBy resolves the binding itself, so this is one check rather than
	// two and cannot disagree with the one Pull and Claim use.
	stillClaimable, authorizeErr := s.claimableBy(ctx, finalDecision, current, finishNow)
	if authorizeErr != nil {
		return ActionRecord{}, errors.Join(ErrExecutionAmbiguous, authorizeErr)
	}
	if !stillClaimable {
		return ActionRecord{}, errors.Join(ErrExecutionAmbiguous,
			shoal.NewError(shoal.ErrorUnauthorized, "terminal execution identity changed"))
	}
	next.ExecutionFingerprint, err = auth.AuthorizationFingerprint(finalDecision)
	if err != nil {
		return ActionRecord{}, errors.Join(ErrExecutionAmbiguous, err)
	}
	next.ExecutionPolicyGeneration = finalDecision.PolicyGeneration()
	next.ExecutionExpiresAt = finalDecision.AuthenticationExpires()
	next.TransitionRequestID = finalDecision.RequestID()
	next.TransitionCorrelationID = finalDecision.CorrelationID()
	if err := s.recorder.RecordAction(ctx, ActionAudit{
		Phase: "effect_outcome", Operation: auth.OperationInvoke, Record: next, EffectError: executionErr,
	}); err != nil {
		return ActionRecord{}, errors.Join(ErrExecutionAmbiguous, ErrRecordingUnavailable, err)
	}
	stored, err := s.store.ApplyAction(ctx, DispatchMutation{
		Token:           transitionToken("complete", current.ID, current.ExecutorKey, next.Version),
		ExpectedVersion: current.Version, ExpectedFence: current.ClaimFence,
		TransitionKind: actionEventKind(next), Record: next,
	})
	if err != nil {
		return ActionRecord{}, errors.Join(ErrExecutionAmbiguous, err)
	}
	if err := s.publishTransition(
		context.WithoutCancel(ctx), actionEventKind(stored), stored,
	); err != nil {
		return ActionRecord{}, errors.Join(ErrActionCommitted, err)
	}
	if executionErr != nil {
		return cloneActionRecord(stored), executionErr
	}
	return cloneActionRecord(stored), nil
}

func validateExecutionEvidence(
	result ExecutionResult, now, authorizationExpiry time.Time,
) error {
	if err := validateEvidence(result.Evidence); err != nil {
		return err
	}
	if len(result.Evidence) == 0 {
		if result.EvidenceSnapshotID != "" ||
			!result.EvidenceSnapshotAsOf.IsZero() {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"executor evidence snapshot requires evidence")
		}
		return nil
	}
	if err := shoal.ValidateRequiredID(
		"executor evidence snapshot ID", result.EvidenceSnapshotID); err != nil {
		return err
	}
	if result.EvidenceSnapshotAsOf.IsZero() {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"executor evidence snapshot time is required")
	}
	if now.Before(result.EvidenceSnapshotAsOf) ||
		authorizationExpiry.Before(result.EvidenceSnapshotAsOf) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"executor evidence snapshot time is outside execution bounds")
	}
	return nil
}

func (s *DispatchService) Cancel(ctx context.Context, request CancelRequest) (ActionRecord, error) {
	ctx, cancel := s.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := s.begin(ctx, auth.OperationDispatch, request.Context)
	if err != nil {
		return ActionRecord{}, err
	}
	if err := validateOpaque("cancel mutation key", request.MutationKey, false); err != nil {
		return ActionRecord{}, err
	}
	current, _, err := s.authorizedCurrent(ctx, decision, request.ID, auth.OperationDispatch, true, now)
	if err != nil {
		return ActionRecord{}, err
	}
	// Cancelling an admission here would rewrite an abandoned grant as a
	// refusal, and those are opposite statements: a cancelled admission says
	// Shoal refused, while an expired claimed one says Shoal permitted an effect
	// and never learned what happened. Refused as not-found for the same reason
	// Claim refuses it.
	if current.isAdmission() {
		return ActionRecord{}, auth.ObjectNotFound()
	}
	if current.Version != request.ExpectedVersion {
		if current.State == DispatchCanceled &&
			current.Version == request.ExpectedVersion+1 &&
			bytes.Equal(current.CancelKey, request.MutationKey) {
			if err := s.publishTransition(
				context.WithoutCancel(ctx), "action.canceled", current,
			); err != nil {
				return ActionRecord{}, errors.Join(ErrActionCommitted, err)
			}
			return cloneActionRecord(current), nil
		}
		return ActionRecord{}, ErrActionConflict
	}
	if current.State == DispatchClaimed && now.Before(current.ClaimLeaseUntil) {
		return ActionRecord{}, ErrActionConflict
	}
	if current.State.terminal() {
		return ActionRecord{}, ErrActionTerminal
	}
	next := cloneActionRecord(current)
	next.Version++
	next.State = DispatchCanceled
	next.CancelKey = append([]byte(nil), request.MutationKey...)
	next.UpdatedAt = now
	next.TransitionRequestID = decision.RequestID()
	next.TransitionCorrelationID = decision.CorrelationID()
	next.AuthorizedOperations = canonicalOperations(append(
		next.AuthorizedOperations,
		decisionOperations(decision, auth.OperationDispatch)...,
	))
	next.CancelAuthorizationFingerprint, err =
		auth.AuthorizationFingerprint(decision)
	if err != nil {
		return ActionRecord{}, err
	}
	next.CancelAuthorizationExpiresAt = decision.AuthenticationExpires()
	if err := s.recorder.RecordAction(ctx, ActionAudit{Phase: "cancel_admission", Operation: auth.OperationDispatch, Record: next}); err != nil {
		return ActionRecord{}, errors.Join(ErrRecordingUnavailable, err)
	}
	stored, err := s.store.ApplyAction(ctx, DispatchMutation{
		Token: transitionToken(
			"cancel", request.ID, request.MutationKey, next.Version),
		ExpectedVersion: current.Version, ExpectedFence: current.ClaimFence,
		TransitionKind: "action.canceled", Record: next,
	})
	if err != nil {
		return ActionRecord{}, err
	}
	if err := s.publishTransition(
		context.WithoutCancel(ctx), "action.canceled", stored,
	); err != nil {
		return ActionRecord{}, errors.Join(ErrActionCommitted, err)
	}
	return cloneActionRecord(stored), nil
}

func (s *DispatchService) Status(ctx context.Context, request StatusRequest) (ActionRecord, error) {
	ctx, cancel := s.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := s.begin(ctx, auth.OperationDispatch, request.Context)
	if err != nil {
		return ActionRecord{}, err
	}
	// Status keeps the principal requirement, unchanged. #437 separated
	// execute from invoke for *claiming*; whether an executor should be able
	// to read the status of work it holds is a different question with a
	// different operation behind it, and bundling it here would widen dispatch
	// rather than answer it.
	//
	// The consequence is worth naming: a worker cannot read the status of its
	// own claim. It does not need to — Claim and CompleteClaim both return the
	// record — but an operator reconciling goes through TeamActions, which has
	// its own authorization and does not require the reader to be the
	// originating principal.
	current, _, err := s.authorizedCurrent(
		ctx, decision, request.ID, auth.OperationDispatch, true, now)
	if err != nil {
		return ActionRecord{}, err
	}
	// The fourth path, found by looking for a third. Status discloses nothing
	// across principals — authorizedCurrent has already refused a foreign
	// record — so this is not the same class of hole as the terminal paths.
	//
	// It is still the last place a dispatch call answered differently for an
	// admission, and that uniformity is worth having on its own: with this,
	// every dispatch entry point gives one answer for an admission-region ID
	// whether it is absent, another principal's, or the caller's own. An
	// admission's state is Outstanding's to report.
	if current.isAdmission() {
		return ActionRecord{}, auth.ObjectNotFound()
	}
	return current, nil
}

// TeamActions returns a bounded page of action records authorized for the
// read-only team overview. Unlike worker Pull, it includes terminal states and
// does not require the reader to be the action's originating principal.
func (s *DispatchService) TeamActions(
	ctx context.Context,
	request TeamActionListRequest,
) (ActionPage, error) {
	ctx, cancel := s.deadline(ctx, request.Context)
	defer cancel()
	now := s.clock().UTC()
	if err := request.Context.validate(now); err != nil {
		return ActionPage{}, err
	}
	decision, err := s.resolver.Resolve(ctx)
	if err != nil {
		return ActionPage{}, err
	}
	if decision.RequestID() != request.Context.RequestID ||
		decision.CorrelationID() != request.Context.CorrelationID {
		return ActionPage{}, shoal.NewError(
			shoal.ErrorUnauthorized,
			"request identity does not match authentication",
		)
	}
	if err := decision.Authorize(
		auth.OperationTeamOverviewRead,
		auth.ResourceRequest{
			AuthorizationDomain: decision.AuthorizationDomain(),
		},
		now,
	); err != nil {
		return ActionPage{}, err
	}
	if request.Limit <= 0 || request.Limit > MaxDispatchListResults {
		return ActionPage{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"team action list limit is outside its bound",
		)
	}
	if len(request.SourceIDs) == 0 || len(request.PolicyIDs) == 0 {
		return ActionPage{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"team action source and policy filters are required",
		)
	}
	sources, err := decision.IntersectSourceIDs(
		auth.OperationTeamOverviewRead, decision.AuthorizationDomain(),
		request.SourceIDs, now)
	if err != nil {
		return ActionPage{}, err
	}
	policies, err := decision.IntersectPolicyIDs(
		auth.OperationTeamOverviewRead, decision.AuthorizationDomain(),
		request.PolicyIDs, now)
	if err != nil {
		return ActionPage{}, err
	}
	if len(sources) == 0 || len(policies) == 0 {
		return ActionPage{}, nil
	}
	objects, err := canonicalOptionalIDs(
		"team action object", request.ObjectIDs)
	if err != nil {
		return ActionPage{}, err
	}
	agents, err := canonicalOptionalIDs(
		"team action agent", request.AgentIDs)
	if err != nil {
		return ActionPage{}, err
	}
	result := ActionPage{Actions: make([]ActionRecord, 0, request.Limit)}
	cursor := append([]byte(nil), request.After...)
	var continuation []byte
	const maxDiscoveryScans = 4096
	for scanned := 0; scanned < maxDiscoveryScans; scanned++ {
		// Admissions are excluded here, and by the scan rather than by a filter
		// in this loop. TeamActions deliberately does not require the reader to
		// be the action's principal — that is what makes it a team view — so an
		// admission reaching it is handed to another caller along with what its
		// owner declared, the digest of the references it named, and the bitmap
		// of the ones it was obliged to withhold.
		//
		// A filter here was the first attempt and it disabled this surface. It
		// skipped the cursor advance at the bottom of the loop, so the first
		// admission was rescanned four thousand times and nothing past it was
		// ever reached — and admission identities sorted first back then, so
		// that was every deployment with one grant. scanDispatchActions drops
		// them instead, where it cannot interact with this loop's control flow
		// at all.
		//
		// The budget below is affordable because the span is maximal, so this
		// loop reaches every ordinary record before the first admission and
		// spends nothing on them until there is nothing else left to find. The
		// exception is an ordinary action stored inside the span before it was
		// reserved, which sorts among the admissions; a deployment holding one
		// is exactly the deployment ErrAdmissionSpanOccupied is refusing
		// admission to until an operator clears it.
		page, scanErr := s.scanDispatchActions(ctx, cursor, 1)
		if scanErr != nil {
			return ActionPage{}, scanErr
		}
		// Advanced before anything can return or skip. It was at the bottom,
		// which is what let a `continue` strand it; a cursor that only moves on
		// the fall-through path is a cursor that stops moving the first time
		// someone adds an early exit.
		next := append([]byte(nil), page.Next...)
		cursor = next
		// An empty page is not an exhausted scan. scanDispatchActions can
		// filter a page down to nothing and still have more to read — two
		// consecutive admissions outside the reserved region spend both of its
		// attempts — and treating that as the end reported end-of-scan with
		// records still ahead. That was the same outage as the stranded cursor,
		// with a narrower trigger: two pre-prefix admissions rather than one
		// admission of any kind. Only an empty continuation means exhausted.
		if len(page.Actions) == 0 {
			if len(next) == 0 {
				return result, nil
			}
			continue
		}
		record := page.Actions[0]
		visible := containsByteValue(sources, record.SourceID) &&
			containsByteValue(policies, record.PolicyID) &&
			containsIDValue(objects, record.ObjectID) &&
			containsIDValue(agents, record.AgentID) &&
			decision.AuthorizeObject(
				auth.OperationTeamOverviewRead,
				auth.ResourceRequest{
					AuthorizationDomain: decision.AuthorizationDomain(),
					SourceID:            record.SourceID,
					PolicyID:            record.PolicyID,
					ObjectID:            record.ObjectID,
				},
				now,
			) == nil
		if visible {
			if len(result.Actions) == request.Limit {
				result.Next = continuation
				return result, nil
			}
			result.Actions = append(result.Actions, cloneActionRecord(record))
			continuation = append([]byte(nil), next...)
		}
		if len(next) == 0 {
			return result, nil
		}
	}
	result.Next = append([]byte(nil), cursor...)
	return result, nil
}

func canonicalOptionalIDs(name string, values []shoal.ID) ([]shoal.ID, error) {
	result := append([]shoal.ID(nil), values...)
	for _, value := range result {
		if err := shoal.ValidateRequiredID(name, value); err != nil {
			return nil, err
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return shoal.CompareID(result[i], result[j]) < 0
	})
	return slices.Compact(result), nil
}

func containsIDValue(values []shoal.ID, value shoal.ID) bool {
	if len(values) == 0 {
		return true
	}
	index := sort.Search(len(values), func(i int) bool {
		return shoal.CompareID(values[i], value) >= 0
	})
	return index < len(values) && values[index] == value
}

func containsByteValue(values [][]byte, value []byte) bool {
	if len(values) == 0 {
		return false
	}
	index := sort.Search(len(values), func(i int) bool {
		return bytes.Compare(values[i], value) >= 0
	})
	return index < len(values) && bytes.Equal(values[index], value)
}

// scanDispatchActions scans committed actions for a surface that must not see
// admissions, excluding them by their durable marker.
//
// It used to skip them by key range instead, jumping over the reserved span in
// one move. That was unsound for a reason no amount of care inside the jump
// could fix: skipping a range asserts that everything in it is something the
// caller must not see, and the span was a legal dispatch identity space before
// it was reserved — so an ordinary action already stored there was skipped,
// invisible to Pull and TeamActions with nothing to indicate it. Reachability
// is a property of what a record *is*, which only the marker records, not of
// where its identity happens to sort.
//
// The jump existed to keep a caller's scan budget off admissions, which sorted
// first. The budget concern goes with the jump because admissions now sort
// after every ordinary identity, and that holds exactly rather than nearly:
// admissionIDPrefix is maximal — every byte 0xff — so any identity outside the
// span differs from it at a byte that is necessarily smaller and sorts below
// the whole of it. A merely high prefix is not enough. When the span was
// "\xffshoal.admission\x00" an ordinary \xff\xff sorted above every
// admission, so a caller paging toward it spent the budget on grants first and
// a large enough tail of them starved it outright — the same outage the jump
// was covering for, reached from the other end.
//
// What remains is a caller paging to the very end, which filters through the
// span having already been given everything it asked for, and an ordinary
// action stored inside the span before it was reserved, which sorts among the
// admissions rather than below them. The second is the deployment
// ErrAdmissionSpanOccupied refuses admission to.
//
// A page can still filter down to nothing at that tail, and the cursor is
// returned so the caller pages on. An empty page is not an exhausted scan; only
// an empty continuation is.
func (s *DispatchService) scanDispatchActions(
	ctx context.Context, after []byte, limit int,
) (ActionPage, error) {
	page, err := s.store.ScanActions(ctx, after, limit)
	if err != nil {
		return ActionPage{}, err
	}
	result := ActionPage{
		Next:    append([]byte(nil), page.Next...),
		Actions: make([]ActionRecord, 0, len(page.Actions)),
	}
	for _, record := range page.Actions {
		if record.isAdmission() {
			continue
		}
		result.Actions = append(result.Actions, record)
	}
	return result, nil
}

func (s *DispatchService) Pull(ctx context.Context, request PullActionsRequest) (ActionPage, error) {
	ctx, cancel := s.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := s.beginClaimant(ctx, request.Context)
	if err != nil {
		return ActionPage{}, err
	}
	if request.Limit <= 0 || request.Limit > MaxDispatchListResults {
		return ActionPage{}, shoal.NewError(shoal.ErrorInvalidArgument, "dispatch pull limit is outside its bound")
	}
	// An admission is never work a dispatch worker may take. It is only ever
	// claimed, and this loop returns a claimed record whose lease has lapsed —
	// which for an admission is not work waiting to be redone but a grant
	// nobody came back to report on. Claim refuses it anyway; dropping it in
	// the scan keeps it out of the listing that would otherwise offer it.
	//
	// This surface does not refill a page the filter emptied, so an admission
	// ahead of ordinary work would cost a worker an empty page and a retry per
	// grant. It cannot be ahead of any: the span is maximal, so every ordinary
	// identity sorts below every admission.
	page, err := s.scanDispatchActions(ctx, request.After, request.Limit)
	if err != nil {
		return ActionPage{}, err
	}
	result := ActionPage{Next: append([]byte(nil), page.Next...)}
	for _, record := range page.Actions {
		if record.State != DispatchQueued &&
			!(record.State == DispatchClaimed && !now.Before(record.ClaimLeaseUntil)) {
			continue
		}
		if !now.Before(record.Deadline) {
			continue
		}
		// The same two routes authorizedClaimant takes, in the same order and
		// for the same reason: an executor granted execute on this record's
		// descriptor may take work it did not enqueue, and the enqueuing
		// principal under invoke keeps working unchanged.
		//
		// Before #437 this filter required the caller to be the enqueuer, and
		// that is what made a remote worker see an empty page forever — the
		// records were skipped silently, so there was no error, no log line,
		// and nothing to diagnose.
		//
		// Binding, not execution: a worker has no in-process Execute, so
		// resolveActionBinding is the right resolver either way.
		claimable, authorizeErr := s.claimableBy(ctx, decision, record, now)
		if authorizeErr != nil {
			return ActionPage{}, authorizeErr
		}
		if !claimable {
			continue
		}
		result.Actions = append(result.Actions, cloneActionRecord(record))
	}
	return result, nil
}

// ReconcileActionTransitions publishes and acknowledges every durable pending
// transition for one action. Callers may use a fresh authorized context after
// the original dispatch request has expired or after process restart.
func (s *DispatchService) ReconcileActionTransitions(
	ctx context.Context, actionID []byte,
) error {
	if err := validateOpaque("action ID", actionID, false); err != nil {
		return err
	}
	var after []byte
	for {
		page, err := s.outbox.PendingActionTransitions(
			ctx, actionID, after, MaxDispatchListResults)
		if err != nil {
			return err
		}
		for _, transition := range page.Transitions {
			if err := s.events.PublishActionEvent(
				ctx, transition.Kind, transition.Record); err != nil {
				return err
			}
			if err := s.outbox.CompleteActionTransition(
				ctx, transition); err != nil {
				return err
			}
		}
		if len(page.Next) == 0 {
			return nil
		}
		after = append(after[:0], page.Next...)
	}
}

func (s *DispatchService) publishTransition(
	ctx context.Context, kind string, record ActionRecord,
) error {
	if actionEventKind(record) != kind {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet action transition kind does not match action state")
	}
	return s.ReconcileActionTransitions(ctx, record.ID)
}

// beginClaimant admits a caller holding either of the operations that
// authorize taking work: execute, or invoke for the principal that enqueued.
//
// begin is a domain-level gate — it checks the operation against the
// authorization domain with no object in the request — so a worker granted only
// execute was refused here before any record was ever read. That made the
// record-level routes in authorizedClaimant unreachable, which is the shape of
// bug this whole change exists to remove: an authorization that fails at a door
// nobody was looking at.
//
// Execute is tried first and invoke second, the same order and for the same
// reason as authorizedClaimant: nothing that worked before stops working.
// Failing both returns the invoke error, which is exactly what a caller holding
// neither was told before this existed.
func (s *DispatchService) beginClaimant(
	ctx context.Context, request RequestContext,
) (auth.Decision, time.Time, error) {
	decision, now, err := s.begin(ctx, auth.OperationExecute, request)
	if err == nil {
		return decision, now, nil
	}
	if !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		return auth.Decision{}, time.Time{}, err
	}
	return s.begin(ctx, auth.OperationInvoke, request)
}

func (s *DispatchService) begin(ctx context.Context, operation auth.Operation, request RequestContext) (auth.Decision, time.Time, error) {
	now := s.clock().UTC()
	if err := request.validate(now); err != nil {
		return auth.Decision{}, time.Time{}, err
	}
	if err := shoal.ValidateRequiredID("dispatch correlation ID", request.CorrelationID); err != nil {
		return auth.Decision{}, time.Time{}, err
	}
	decision, err := s.resolver.Resolve(ctx)
	if err != nil {
		return auth.Decision{}, time.Time{}, err
	}
	if decision.RequestID() != request.RequestID || decision.CorrelationID() != request.CorrelationID {
		return auth.Decision{}, time.Time{}, shoal.NewError(shoal.ErrorUnauthorized, "request identity does not match authentication")
	}
	if err := decision.Authorize(operation, auth.ResourceRequest{AuthorizationDomain: decision.AuthorizationDomain()}, now); err != nil {
		return auth.Decision{}, time.Time{}, err
	}
	return decision, now, nil
}

func (s *DispatchService) deadline(ctx context.Context, request RequestContext) (context.Context, context.CancelFunc) {
	now := s.clock().UTC()
	if request.Deadline.IsZero() || !now.Before(request.Deadline) {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		return canceled, func() {}
	}
	return context.WithTimeout(ctx, request.Deadline.Sub(now))
}

// authorizedClaimant resolves an action for a caller that means to take or
// finish the work, by either of the two routes that authorize it.
//
// An executor granted OperationExecute on the action's descriptor may claim
// work it did not enqueue. That is the whole point of #437: a gateway performs
// effects other agents asked for, and under one operation it could not — the
// claimant had to be the principal that enqueued, so every record failed
// sameActionPrincipal and was silently skipped, leaving a ready worker and a
// growing queue with no error anywhere.
//
// The enqueuing principal with OperationInvoke remains a second route, so a
// deployment that claims its own work keeps working without being regranted.
// That ordering is deliberate: the broader route is tried first, and the
// narrower principal-bound one only if it fails, so nothing that worked before
// stops working.
//
// Both failures normalise to the same auth.ObjectNotFound() that every other
// path through authorizedCurrent returns. Two routes must not become two
// distinguishable answers — the absent-versus-foreign normalisation is what
// stops this surface being an existence oracle (#398), and adding a second way
// in is exactly how that gets reopened.
func (s *DispatchService) authorizedClaimant(
	ctx context.Context, decision auth.Decision, id []byte, now time.Time,
) (ActionRecord, Action, error) {
	record, action, err := s.authorizedCurrent(ctx, decision, id, auth.OperationExecute, false, now)
	if err == nil {
		return record, action, nil
	}
	// Only an authorization answer is worth a second attempt. A malformed ID or
	// a store failure is the same answer either way, and retrying it would turn
	// one fault into two store reads.
	if !shoal.IsErrorCode(err, shoal.ErrorNotFound) &&
		!shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		return ActionRecord{}, Action{}, err
	}
	return s.authorizedCurrent(ctx, decision, id, auth.OperationInvoke, true, now)
}

// authorizedCurrent resolves an existing action for one operation.
//
// requirePrincipal asks whether the caller must also be the principal that
// enqueued the action. It is true for every operation that existed before
// #437 — invoke and dispatch — and false only for OperationExecute, where the
// grant on the descriptor is the authorization and requiring the enqueuer's
// identity is what made an out-of-process executor impossible.
func (s *DispatchService) authorizedCurrent(ctx context.Context, decision auth.Decision, id []byte, operation auth.Operation, requirePrincipal bool, now time.Time) (ActionRecord, Action, error) {
	if err := validateOpaque("action ID", id, false); err != nil {
		return ActionRecord{}, Action{}, err
	}
	current, err := s.store.GetAction(ctx, id)
	if err != nil {
		// Absence answers exactly as a record the caller may not see does.
		// These were different messages under one status: the store's own
		// sentinel for absent, and the object-not-found shape below for
		// foreign. That difference is #398, and it stops being a cosmetic
		// inconsistency once admission identities are derivable — a caller can
		// compute a victim's admission ID and read existence off the message,
		// which is the enumeration the reserved namespace closed at enqueue,
		// reopened through every ID-taking entry point.
		//
		// Normalised here rather than at each caller because every one of them
		// — Claim, Cancel, Status, CompleteClaim — reaches the store through
		// this function, and a per-caller fix is a fix that the next caller
		// forgets.
		if errors.Is(err, ErrActionNotFound) {
			return ActionRecord{}, Action{}, auth.ObjectNotFound()
		}
		return ActionRecord{}, Action{}, err
	}
	if requirePrincipal && !sameActionPrincipal(decision, current) {
		return ActionRecord{}, Action{}, auth.ObjectNotFound()
	}
	// Operation mapping is explicit: claim/complete/pull use execute and fall
	// back to invoke for the enqueuing principal, while cancel/status use
	// dispatch. Existing-action operations additionally bind
	// authorization to the durable action ID before checking its execution
	// target through registry.resolveAction.
	if err := decision.AuthorizeObject(operation, auth.ResourceRequest{
		AuthorizationDomain: decision.AuthorizationDomain(),
		SourceID:            current.SourceID,
		PolicyID:            current.PolicyID,
		ObjectID:            shoal.ID(current.ID),
	}, now); err != nil {
		return ActionRecord{}, Action{}, auth.ObjectNotFound()
	}
	// resolveActionBinding, not resolveAction: claiming, cancelling, inspecting
	// and completing an action must work for an executor that runs out of
	// process and has no in-process Execute. Only ExecuteClaim needs a runnable
	// one. Every other check — generation, scope, authorization, delegation and
	// the declared effect ceiling — is identical either way.
	// Resolved once and returned, rather than resolved here and re-resolved by
	// the caller. Claim and CompleteClaim each used to resolve again with a
	// hardcoded OperationInvoke under a comment saying it "adds no
	// authorization" — which was true while invoke was the only way in and
	// became false the moment execute existed: a worker authorized under
	// execute was refused by the second call. Handing back the declaration
	// removes the duplicate gate instead of teaching it the second operation.
	_, resolved, _, err := s.registry.resolveActionBinding(ctx, decision, current.AgentID, current.AgentGeneration,
		current.Capability, current.Action, current.SourceID, current.PolicyID, current.ObjectID,
		operation, now)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) ||
			shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			return ActionRecord{}, Action{}, auth.ObjectNotFound()
		}
		// Without the principal check, every failure here is one answer.
		//
		// resolveActionBinding returns a bare ErrorUnavailable when the
		// executor is unregistered or the effect ceiling has narrowed, and that
		// surfaces as a 503 where an absent record is a 404. For the enqueuing
		// principal that distinction is information it already has. For a
		// caller reaching a record on the strength of an execute grant it is an
		// existence oracle, and it reaches the admission namespace — those IDs
		// are computable from a principal tuple, and Claim's isAdmission
		// refusal runs after this, so the 503 would arrive first.
		//
		// That is #398 reopened through a route that did not exist when the
		// normalisation was written, which is why it is collapsed here rather
		// than at the one caller that noticed.
		if !requirePrincipal {
			return ActionRecord{}, Action{}, auth.ObjectNotFound()
		}
		return ActionRecord{}, Action{}, err
	}
	return cloneActionRecord(current), resolved, nil
}

// claimableBy reports whether this caller may take this record, by either
// route, and separates "not for you" from a fault.
//
// That separation is why this is a function rather than two inline conditions.
// An authorization answer is a skip: the page continues and the caller simply
// does not see the record. A fault on the caller's own record aborts the page,
// because telling a caller its queue is empty is a worse answer than telling
// it the lookup failed.
//
// The bound on how much that buys is worth stating, because an earlier version
// of this comment claimed more. resolveActionBinding collapses every error
// from Service.active into auth.ObjectNotFound(), and active propagates real
// store faults up the delegation chain — so a descriptor-store outage already
// reads here as "no such record" and already produces a silent empty page.
// That is pre-existing behaviour and this function does not change it. What it
// does is stop a *foreign* record's fault from aborting a page it has no
// business aborting, which is a narrower and real guarantee.
func (s *DispatchService) claimableBy(
	ctx context.Context, decision auth.Decision, record ActionRecord, now time.Time,
) (bool, error) {
	for _, route := range []struct {
		operation        auth.Operation
		requirePrincipal bool
	}{
		{auth.OperationExecute, false},
		{auth.OperationInvoke, true},
	} {
		if route.requirePrincipal && !sameActionPrincipal(decision, record) {
			continue
		}
		_, _, _, err := s.registry.resolveActionBinding(
			ctx, decision, record.AgentID, record.AgentGeneration,
			record.Capability, record.Action, record.SourceID, record.PolicyID,
			record.ObjectID, route.operation, now,
		)
		if err == nil {
			return true, nil
		}
		if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) ||
			shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			continue
		}
		// A real fault, and whose record it is decides whether the caller is
		// entitled to hear about it.
		//
		// On the enqueuer's own record it aborts, as it always has: a registry
		// that cannot answer is not the same as a record that is not yours, and
		// swallowing it would turn an outage into an empty queue.
		//
		// On somebody else's it is a skip. Before #437 the principal check ran
		// first and such a record never reached this call, so one foreign
		// action with an unregistered executor could not affect your page. With
		// two routes it would abort the page of every execute-holder in the
		// scope, on every pull, until that record expired — a shared-queue
		// outage any principal able to enqueue could plant.
		if sameActionPrincipal(decision, record) {
			return false, err
		}
	}
	return false, nil
}

// standingOn reports whether this caller has any standing to be told that this
// record exists: it is either the principal that enqueued it or the principal
// that holds its claim.
//
// Before #437 the question could not arise. Every route resolved through
// authorizedCurrent, which required sameActionPrincipal unconditionally, so a
// caller with no standing was already refused with auth.ObjectNotFound() and
// could not reach a handler at all. The execute routes removed that gate by
// design — a worker must be able to claim work it did not enqueue — and the
// handlers behind them answer with ErrActionTerminal, ErrActionConflict and
// ErrClaimLost, none of which an absent action produces.
//
// That is the #398 existence oracle, reopened through routes that did not
// exist when the normalisation was written. Action IDs are caller-chosen
// opaque bytes, so they are guessable, and the records that leak are exactly
// the ones Pull does not show: terminal, and past their deadline.
func standingOn(decision auth.Decision, record ActionRecord) bool {
	return sameActionPrincipal(decision, record) ||
		sameClaimantPrincipal(decision, record)
}

// sameClaimantPrincipal reports whether the caller is the principal that holds
// the record's claim.
//
// This is the gate #437 removed without replacing. Claiming and completing
// were both authorized under OperationInvoke and both additionally required
// sameActionPrincipal, so the claimant was the enqueuer and the enqueuer's
// chain was on the record. Relaxing claiming to OperationExecute left the
// completion predicate as version, state and ClaimID — and ClaimID is
// caller-supplied, unconstrained in entropy, and published to co-principals on
// Status and to every execute-holder on Pull once a lease lapses. So a second
// execute-holder that had never claimed anything could present another
// worker's ClaimID and commit a fabricated outcome; worse, the replay branch
// then handed the real worker a success receipt carrying the fabrication,
// which is the one failure a worker cannot detect.
//
// A record claimed before this field existed has an empty chain, and an empty
// chain matches nothing — shoal.ID("") is not a valid principal and no
// decision carries it. That is the right default for a gob-decoded record from
// an older build: it refuses the completion rather than accepting it, and the
// worker's recourse is to re-claim, which writes the field.
func sameClaimantPrincipal(decision auth.Decision, record ActionRecord) bool {
	if record.ClaimantSubject == "" {
		return false
	}
	if decision.Subject() != record.ClaimantSubject ||
		decision.Actor() != record.ClaimantActor ||
		decision.ClientID() != record.ClaimantClientID {
		return false
	}
	left, right := decision.OnBehalfOf(), record.ClaimantOnBehalfOf
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func sameActionPrincipal(decision auth.Decision, record ActionRecord) bool {
	if decision.Subject() != record.Subject || decision.Actor() != record.Actor ||
		decision.ClientID() != record.ClientID {
		return false
	}
	left, right := decision.OnBehalfOf(), record.OnBehalfOf
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// resolveAction resolves an action for in-process execution. It is
// resolveActionBinding plus the assertion that the bound reference can actually
// run the work here.
func (s *Service) resolveAction(
	ctx context.Context,
	decision auth.Decision,
	agentID shoal.ID,
	generation int64,
	capabilityName, actionName string,
	sourceID, policyID []byte,
	objectID shoal.ID,
	operation auth.Operation,
	now time.Time,
) (Descriptor, Action, ActionExecutor, error) {
	descriptor, action, raw, err := s.resolveActionBinding(
		ctx, decision, agentID, generation, capabilityName, actionName,
		sourceID, policyID, objectID, operation, now,
	)
	if err != nil {
		return Descriptor{}, Action{}, nil, err
	}
	executor, ok := raw.(ActionExecutor)
	if !ok {
		return Descriptor{}, Action{}, nil, shoal.NewError(shoal.ErrorUnavailable, "registered executor does not implement action execution")
	}
	return descriptor, action, executor, nil
}

// resolveActionBinding resolves an action and its bound executor reference
// without requiring the reference to be runnable in this process.
//
// Authorization, scope, generation and the declared effect ceiling are all
// checked exactly as they are for in-process execution — the only thing it does
// not demand is an ActionExecutor implementation.
//
// That distinction is what makes an out-of-process executor possible. A gateway
// proxy performs its work itself and reports through CompleteClaim; nothing in
// this process ever runs it, so requiring it to supply an Execute method would
// force every remote deployment to bind a stub whose only job is to be refused.
// Claiming, cancelling, inspecting and completing an action therefore resolve
// through here, and only ExecuteClaim demands a runnable executor.
func (s *Service) resolveActionBinding(
	ctx context.Context,
	decision auth.Decision,
	agentID shoal.ID,
	generation int64,
	capabilityName, actionName string,
	sourceID, policyID []byte,
	objectID shoal.ID,
	operation auth.Operation,
	now time.Time,
) (Descriptor, Action, any, error) {
	descriptor, err := s.active(ctx, agentID, now)
	if err != nil || descriptor.Generation != generation {
		return Descriptor{}, Action{}, nil, auth.ObjectNotFound()
	}
	if !bytes.Equal(descriptor.AuthorizationDomain, decision.AuthorizationDomain()) {
		return Descriptor{}, Action{}, nil, auth.ObjectNotFound()
	}
	scopeFound := false
	for _, scope := range descriptor.Scopes {
		if bytes.Equal(scope.SourceID, sourceID) && bytes.Equal(scope.PolicyID, policyID) {
			scopeFound = true
			break
		}
	}
	if !scopeFound {
		return Descriptor{}, Action{}, nil, auth.ObjectNotFound()
	}
	resource := auth.ResourceRequest{
		AuthorizationDomain: descriptor.AuthorizationDomain, SourceID: sourceID,
		PolicyID: policyID, ObjectID: objectID,
	}
	if err := decision.AuthorizeObject(operation, resource, now); err != nil {
		return Descriptor{}, Action{}, nil, err
	}
	if len(decision.OnBehalfOf()) > 0 {
		if err := decision.AuthorizeObject(auth.OperationDelegate, resource, now); err != nil {
			return Descriptor{}, Action{}, nil, err
		}
	}
	var selected *Action
	for _, capability := range descriptor.Capabilities {
		if capability.Name != capabilityName {
			continue
		}
		for _, action := range capability.Actions {
			if action.Name == actionName {
				copy := action
				selected = &copy
				break
			}
		}
	}
	if selected == nil {
		return Descriptor{}, Action{}, nil, auth.ObjectNotFound()
	}
	raw, ok := s.executors.ResolveExecutor(descriptor.ExecutorRef)
	if !ok {
		return Descriptor{}, Action{}, nil, shoal.NewError(shoal.ErrorUnavailable, "agent executor is unavailable")
	}
	// Re-checked at resolution, not only at registration. A host can rebind an
	// executor reference to a narrower ceiling while descriptors registered
	// under the old one are still live, and those must stop resolving rather
	// than keep running against a binding that no longer permits them.
	if selected.Effects.exceeds(executorCeiling(raw)) {
		return Descriptor{}, Action{}, nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"action declares effects its executor is not bound to perform")
	}
	// Re-checked here too, and for the same reason as the ceiling: a host can
	// rebind a reference to an executor that now always transmits, and a
	// descriptor registered against the old binding must stop resolving rather
	// than keep running while understating what it does.
	if selected.Effects.omits(executorFloor(raw)) {
		return Descriptor{}, Action{}, nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"action omits effects its executor causes on every invocation")
	}
	return cloneDescriptor(descriptor), *selected, raw, nil
}

func executorKey(actionID, idempotency []byte) []byte {
	// v2 replaces the unshipped NUL-delimited encoding. It applies only when a
	// new action is admitted; replay and restart paths keep the stored key.
	digest := sha256.New()
	writeDispatchTupleField(digest, []byte("shoal.fleet.executor-key.v2"))
	writeDispatchTupleField(digest, actionID)
	writeDispatchTupleField(digest, idempotency)
	return digest.Sum(nil)
}

func transitionToken(kind string, actionID, discriminator []byte, version uint64) []byte {
	// Transition tokens are not persisted in ActionRecord, so the unshipped
	// v1 encoding has no compatibility path. Existing durable transitions keep
	// their stored state; retries reconstruct this canonical v2 tuple.
	digest := sha256.New()
	writeDispatchTupleField(
		digest, []byte("shoal.fleet.dispatch-transition.v2"))
	writeDispatchTupleField(digest, []byte(kind))
	writeDispatchTupleField(digest, actionID)
	writeDispatchTupleField(digest, discriminator)
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], version)
	writeDispatchTupleField(digest, raw[:])
	return digest.Sum(nil)
}

func writeDispatchTupleField(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

// equivalentEnqueue reports whether an existing record is the same request as
// the one being written, so a retry replays instead of conflicting.
//
// What an admission declared is part of that identity. Without the two
// Admitted fields below, a caller could ask for an admission carrying corpus
// references, receive obligations restricting them, and then replay the same
// action ID, idempotency key and token with the reference list removed: the
// record would still be recognised as the same request, obligations would be
// recomputed over nothing, and the reply would be an unrestricted allow for a
// token that is already live. The declaration has to be pinned by the record,
// not merely adjudicated on the way past it.
func equivalentEnqueue(current, wanted ActionRecord) bool {
	return equalEffects(current.AdmittedEffects, wanted.AdmittedEffects) &&
		bytes.Equal(current.AdmittedDisclosures, wanted.AdmittedDisclosures) &&
		current.AdmittedIdentityScheme == wanted.AdmittedIdentityScheme &&
		bytes.Equal(current.ID, wanted.ID) &&
		bytes.Equal(current.IdempotencyKey, wanted.IdempotencyKey) &&
		current.AgentID == wanted.AgentID &&
		current.AgentGeneration == wanted.AgentGeneration &&
		current.Capability == wanted.Capability &&
		current.Action == wanted.Action &&
		bytes.Equal(current.SourceID, wanted.SourceID) &&
		bytes.Equal(current.PolicyID, wanted.PolicyID) &&
		current.ObjectID == wanted.ObjectID &&
		bytes.Equal(current.Input, wanted.Input) &&
		current.Subject == wanted.Subject &&
		current.Actor == wanted.Actor &&
		current.ClientID == wanted.ClientID &&
		equalIDs(current.OnBehalfOf, wanted.OnBehalfOf) &&
		current.AuthorizationFingerprint == wanted.AuthorizationFingerprint &&
		current.PolicyGeneration == wanted.PolicyGeneration &&
		current.Reason == wanted.Reason &&
		current.Deadline.Equal(wanted.Deadline)
}

func equalIDs(left, right []shoal.ID) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func decisionOperations(decision auth.Decision, operation auth.Operation) []auth.Operation {
	result := []auth.Operation{operation}
	if len(decision.OnBehalfOf()) > 0 {
		result = append(result, auth.OperationDelegate)
	}
	return canonicalOperations(result)
}

func validateActionErrorCode(value string) error {
	return validateActionRecordError(value)
}

func actionEventKind(record ActionRecord) string {
	switch record.State {
	case DispatchQueued:
		return "action.enqueued"
	case DispatchClaimed:
		return "action.claimed"
	case DispatchSucceeded:
		return "action.completed"
	case DispatchFailed:
		return "action.failed"
	case DispatchCanceled:
		return "action.canceled"
	default:
		return ""
	}
}
