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
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type DispatchService struct {
	store    DispatchStore
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
	return &DispatchService{
		store: config.Store, registry: config.Registry, resolver: config.Resolver,
		recorder: config.Recorder, events: config.Events, clock: config.Clock,
	}, nil
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

	if err := validateOpaque("action ID", request.ID, false); err != nil {
		return ActionRecord{}, err
	}
	if err := validateOpaque("action idempotency key", request.IdempotencyKey, false); err != nil {
		return ActionRecord{}, err
	}
	if request.AgentGeneration <= 0 {
		return ActionRecord{}, shoal.NewError(shoal.ErrorInvalidArgument, "agent generation must be positive")
	}
	if err := validateName("capability", request.Capability); err != nil {
		return ActionRecord{}, err
	}
	if err := validateName("action", request.Action); err != nil {
		return ActionRecord{}, err
	}
	if request.Context.Deadline.Sub(now) > MaxActionDeadline {
		return ActionRecord{}, shoal.NewError(shoal.ErrorInvalidArgument, "action deadline exceeds its bound")
	}
	descriptor, action, _, err := s.registry.resolveAction(
		ctx, decision, request.AgentID, request.AgentGeneration,
		request.Capability, request.Action, request.SourceID, request.PolicyID,
		request.ObjectID, operation, now,
	)
	if err != nil {
		return ActionRecord{}, err
	}
	input, err := validateAgainstSchema(action.InputSchema, request.Input, "action input", MaxActionPayloadBytes)
	if err != nil {
		return ActionRecord{}, err
	}
	reason, err := interaction.NewReason(request.Context.ReasonCode, request.Context.ReasonDetail)
	if err != nil {
		return ActionRecord{}, err
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return ActionRecord{}, err
	}
	record := ActionRecord{
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
	}
	if current, readErr := s.store.GetAction(ctx, request.ID); readErr == nil {
		if equivalentEnqueue(current, record) {
			if err := s.events.PublishActionEvent(ctx, actionEventKind(current), current); err != nil {
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
		ExpectedVersion: 0, Record: record,
	})
	if err != nil {
		return ActionRecord{}, err
	}
	if err := s.events.PublishActionEvent(ctx, "action.enqueued", stored); err != nil {
		return ActionRecord{}, errors.Join(ErrActionCommitted, err)
	}
	return cloneActionRecord(stored), nil
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
		current, currentErr := s.authorizedCurrent(
			ctx, decision, queued.ID, auth.OperationInvoke, now,
		)
		if currentErr != nil {
			return ActionRecord{}, currentErr
		}
		if err := s.events.PublishActionEvent(ctx, actionEventKind(current), current); err != nil {
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
	decision, now, err := s.begin(ctx, auth.OperationInvoke, request.Context)
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
	current, err := s.authorizedCurrent(ctx, decision, request.ID, auth.OperationInvoke, now)
	if err != nil {
		return ActionRecord{}, err
	}
	if current.Version != request.ExpectedVersion {
		if current.State == DispatchClaimed &&
			current.Version == request.ExpectedVersion+1 &&
			bytes.Equal(current.ClaimID, request.ClaimID) &&
			current.ClaimLease == request.Lease {
			if err := s.events.PublishActionEvent(ctx, "action.claimed", current); err != nil {
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
	if !now.Before(current.Deadline) {
		return ActionRecord{}, ErrClaimLost
	}
	next := cloneActionRecord(current)
	next.Version++
	next.State = DispatchClaimed
	next.ClaimID = append([]byte(nil), request.ClaimID...)
	next.ClaimFence++
	next.ClaimLease = request.Lease
	next.ClaimLeaseUntil = now.Add(request.Lease)
	if next.ClaimLeaseUntil.After(next.Deadline) {
		next.ClaimLeaseUntil = next.Deadline
	}
	next.UpdatedAt = now
	next.Actor = decision.Actor()
	next.AuthorizedOperations = canonicalOperations(append(
		next.AuthorizedOperations, decisionOperations(decision, auth.OperationInvoke)...))
	next.ExecutionFingerprint, err = auth.AuthorizationFingerprint(decision)
	if err != nil {
		return ActionRecord{}, err
	}
	next.ExecutionPolicyGeneration = decision.PolicyGeneration()
	next.ExecutionExpiresAt = decision.AuthenticationExpires()
	if err := s.recorder.RecordAction(ctx, ActionAudit{Phase: "claim_admission", Operation: auth.OperationInvoke, Record: next}); err != nil {
		return ActionRecord{}, errors.Join(ErrRecordingUnavailable, err)
	}
	stored, err := s.store.ApplyAction(ctx, DispatchMutation{
		Token:           transitionToken("claim", request.ID, request.ClaimID, next.Version),
		ExpectedVersion: current.Version, ExpectedFence: current.ClaimFence, Record: next,
	})
	if err != nil {
		return ActionRecord{}, err
	}
	if err := s.events.PublishActionEvent(ctx, "action.claimed", stored); err != nil {
		return ActionRecord{}, errors.Join(ErrActionCommitted, err)
	}
	return cloneActionRecord(stored), nil
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
	current, err := s.store.GetAction(ctx, claimed.ID)
	if err != nil {
		return ActionRecord{}, err
	}
	if current.State.terminal() && current.Version == claimed.Version+1 &&
		current.ClaimFence == claimed.ClaimFence &&
		bytes.Equal(current.ClaimID, claimed.ClaimID) {
		if err := s.events.PublishActionEvent(ctx, actionEventKind(current), current); err != nil {
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
	if !sameActionPrincipal(finalDecision, current) {
		return ActionRecord{}, errors.Join(ErrExecutionAmbiguous,
			shoal.NewError(shoal.ErrorUnauthorized, "terminal execution identity changed"))
	}
	if _, _, _, err := s.registry.resolveAction(
		ctx, finalDecision, current.AgentID, current.AgentGeneration,
		current.Capability, current.Action, current.SourceID, current.PolicyID,
		current.ObjectID, auth.OperationInvoke, finishNow,
	); err != nil {
		return ActionRecord{}, errors.Join(ErrExecutionAmbiguous, err)
	}
	next.ExecutionFingerprint, err = auth.AuthorizationFingerprint(finalDecision)
	if err != nil {
		return ActionRecord{}, errors.Join(ErrExecutionAmbiguous, err)
	}
	next.ExecutionPolicyGeneration = finalDecision.PolicyGeneration()
	next.ExecutionExpiresAt = finalDecision.AuthenticationExpires()
	if err := s.recorder.RecordAction(ctx, ActionAudit{
		Phase: "effect_outcome", Operation: auth.OperationInvoke, Record: next, EffectError: executionErr,
	}); err != nil {
		return ActionRecord{}, errors.Join(ErrExecutionAmbiguous, ErrRecordingUnavailable, err)
	}
	stored, err := s.store.ApplyAction(ctx, DispatchMutation{
		Token:           transitionToken("complete", current.ID, current.ExecutorKey, next.Version),
		ExpectedVersion: current.Version, ExpectedFence: current.ClaimFence, Record: next,
	})
	if err != nil {
		return ActionRecord{}, errors.Join(ErrExecutionAmbiguous, err)
	}
	if err := s.events.PublishActionEvent(ctx, actionEventKind(stored), stored); err != nil {
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
	current, err := s.authorizedCurrent(ctx, decision, request.ID, auth.OperationDispatch, now)
	if err != nil {
		return ActionRecord{}, err
	}
	if current.Version != request.ExpectedVersion {
		if current.State == DispatchCanceled &&
			current.Version == request.ExpectedVersion+1 &&
			bytes.Equal(current.CancelKey, request.MutationKey) {
			if err := s.events.PublishActionEvent(ctx, "action.canceled", current); err != nil {
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
	if err := s.recorder.RecordAction(ctx, ActionAudit{Phase: "cancel_admission", Operation: auth.OperationDispatch, Record: next}); err != nil {
		return ActionRecord{}, errors.Join(ErrRecordingUnavailable, err)
	}
	stored, err := s.store.ApplyAction(ctx, DispatchMutation{
		Token: transitionToken(
			"cancel", request.ID, request.MutationKey, next.Version),
		ExpectedVersion: current.Version, ExpectedFence: current.ClaimFence, Record: next,
	})
	if err != nil {
		return ActionRecord{}, err
	}
	if err := s.events.PublishActionEvent(ctx, "action.canceled", stored); err != nil {
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
	return s.authorizedCurrent(ctx, decision, request.ID, auth.OperationDispatch, now)
}

func (s *DispatchService) Pull(ctx context.Context, request PullActionsRequest) (ActionPage, error) {
	ctx, cancel := s.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := s.begin(ctx, auth.OperationInvoke, request.Context)
	if err != nil {
		return ActionPage{}, err
	}
	if request.Limit <= 0 || request.Limit > MaxDispatchListResults {
		return ActionPage{}, shoal.NewError(shoal.ErrorInvalidArgument, "dispatch pull limit is outside its bound")
	}
	page, err := s.store.ScanActions(ctx, request.After, request.Limit)
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
		if !sameActionPrincipal(decision, record) {
			continue
		}
		if _, _, _, authorizeErr := s.registry.resolveAction(
			ctx, decision, record.AgentID, record.AgentGeneration,
			record.Capability, record.Action, record.SourceID, record.PolicyID,
			record.ObjectID, auth.OperationInvoke, now,
		); authorizeErr != nil {
			if shoal.IsErrorCode(authorizeErr, shoal.ErrorUnauthorized) ||
				shoal.IsErrorCode(authorizeErr, shoal.ErrorNotFound) {
				continue
			}
			return ActionPage{}, authorizeErr
		}
		result.Actions = append(result.Actions, cloneActionRecord(record))
	}
	return result, nil
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

func (s *DispatchService) authorizedCurrent(ctx context.Context, decision auth.Decision, id []byte, operation auth.Operation, now time.Time) (ActionRecord, error) {
	if err := validateOpaque("action ID", id, false); err != nil {
		return ActionRecord{}, err
	}
	current, err := s.store.GetAction(ctx, id)
	if err != nil {
		return ActionRecord{}, err
	}
	if !sameActionPrincipal(decision, current) {
		return ActionRecord{}, auth.ObjectNotFound()
	}
	// Operation mapping is explicit: claim/execute/pull use invoke, while
	// cancel/status use dispatch. Existing-action operations additionally bind
	// authorization to the durable action ID before checking its execution
	// target through registry.resolveAction.
	if err := decision.AuthorizeObject(operation, auth.ResourceRequest{
		AuthorizationDomain: decision.AuthorizationDomain(),
		SourceID:            current.SourceID,
		PolicyID:            current.PolicyID,
		ObjectID:            shoal.ID(current.ID),
	}, now); err != nil {
		return ActionRecord{}, auth.ObjectNotFound()
	}
	if _, _, _, err := s.registry.resolveAction(ctx, decision, current.AgentID, current.AgentGeneration,
		current.Capability, current.Action, current.SourceID, current.PolicyID, current.ObjectID,
		operation, now); err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) ||
			shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			return ActionRecord{}, auth.ObjectNotFound()
		}
		return ActionRecord{}, err
	}
	return cloneActionRecord(current), nil
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
	executor, ok := raw.(ActionExecutor)
	if !ok {
		return Descriptor{}, Action{}, nil, shoal.NewError(shoal.ErrorUnavailable, "registered executor does not implement action execution")
	}
	return cloneDescriptor(descriptor), *selected, executor, nil
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

func equivalentEnqueue(current, wanted ActionRecord) bool {
	return bytes.Equal(current.ID, wanted.ID) &&
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
