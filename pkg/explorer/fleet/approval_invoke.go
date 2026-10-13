// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"context"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Synchronous invoke of an approval-required action (#655).
//
// Before approval, Invoke is refused as it always was: 409, ErrApprovalRequired,
// naming the approval route. Invoke never holds a request, so a refused call
// writes nothing.
//
// After approval, Invoke at the same identity — the approval ID is the action
// ID — materializes the approved request and then claims and runs it in
// process, under the requester's own credential and ClaimID. It is the same
// approved → enqueued compare-and-set Request makes, with Request's
// equal-request check, and the request is rebuilt under this caller's
// decision. Exactly-once is the single ActionRecord and the claim fence, as it
// is for every queued record.
//
// The materialization takes dispatch and the run takes invoke, both checked
// against the live decision at the moment each happens. The hold was a
// pre-authorization; the enqueue is still an enqueue, so a requester whose
// dispatch was revoked after making the hold cannot turn it into work.
// AuthorizedOperations is not consulted for this: it accumulates across
// transitions and is not what authorized any one of them (see
// ActionRecord.TransitionOperation). The record starts [dispatch] from the
// request and the claim widens it to [dispatch, invoke] by itself.
//
// An approver never reaches here. Invoke requires invoke and the requester's
// principal, and the separation rule requires an approver to hold neither.

// approvalMaterializer is the hook through which Invoke materializes an
// approved request. It is optional, like graphEvidenceBinder, and consulted
// at one named point: in Invoke, after enqueue's replay branch, when the
// enqueue was refused for want of approval. A record that already exists has
// replayed by then and is never materialized over.
//
// It is installed by the approval service, which is the only implementation.
// Without one, Invoke answers ErrApprovalRequired as before.
type approvalMaterializer interface {
	// materializeForInvoke returns the queued record of an approved request
	// at the invocation's identity, materializing it if it has not been.
	// Every refusal leaves the approval and the dispatch store as they were,
	// apart from the lazy expiry Request also writes.
	materializeForInvoke(context.Context, InvokeRequest) (ActionRecord, error)
}

// approvalMaterializerHook boxes the hook for an atomic pointer.
type approvalMaterializerHook struct {
	approvalMaterializer
}

// installApprovalMaterializer attaches the approval service that Invoke
// materializes through. Once: a dispatch service has one approval key space,
// and a second service would make which window, scheme and terminal-state
// switch an Invoke is judged under depend on composition order.
func (s *DispatchService) installApprovalMaterializer(
	materializer approvalMaterializer,
) error {
	if materializer == nil {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "approval materializer is required")
	}
	if !s.approvals.CompareAndSwap(
		nil, &approvalMaterializerHook{materializer},
	) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet dispatch already has an approval service")
	}
	return nil
}

// materializeApproved consults the hook, or answers required unchanged when
// none is installed.
func (s *DispatchService) materializeApproved(
	ctx context.Context, request InvokeRequest, required error,
) (ActionRecord, error) {
	hook := s.approvals.Load()
	if hook == nil {
		return ActionRecord{}, required
	}
	return hook.materializeForInvoke(ctx, request)
}

// materializeForInvoke is the approval service's side of the hook.
func (s *ApprovalService) materializeForInvoke(
	ctx context.Context, request InvokeRequest,
) (ActionRecord, error) {
	dispatch := s.dispatch
	ctx, cancel := dispatch.deadline(ctx, request.Enqueue.Context)
	defer cancel()
	// The claim this call is about to make, refused before anything is
	// written: a malformed claim must not leave behind work it then fails to
	// claim. Claim checks the same again.
	if err := validateOpaque("claim ID", request.ClaimID, false); err != nil {
		return ActionRecord{}, err
	}
	if request.Lease <= 0 || request.Lease > MaxActionClaimTTL {
		return ActionRecord{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "claim version or lease is invalid")
	}
	// Dispatch, against the live decision, before any approval is read: the
	// answer to a caller without it cannot depend on what is stored.
	decision, now, err := dispatch.begin(
		ctx, auth.OperationDispatch, request.Enqueue.Context)
	if err != nil {
		return ActionRecord{}, materializeUnderDispatch(err)
	}
	// Exactly the record Request builds, so the equal-request check is the
	// one Request makes. A reserved identity never gets here: enqueue
	// refuses it before the store is read, so it is never answered approval
	// required.
	base, action, err := dispatch.queuedRecord(
		ctx, decision, request.Enqueue, auth.OperationDispatch, now, false)
	if err != nil {
		return ActionRecord{}, materializeUnderDispatch(err)
	}
	if !action.RequiresApproval {
		// The registration changed between Invoke's resolve and this one. A
		// retry enqueues it directly.
		return ActionRecord{}, shoal.NewError(
			shoal.ErrorConflict,
			"the action's approval requirement changed during the call; retry")
	}
	// The work must be runnable here before it is made. A dispatch-only
	// reference would otherwise be materialized and then refused at the
	// claim, leaving work behind (#455's ordering). Asked under invoke, of
	// the same credential, because that is the operation the run takes.
	// At the attempt's time, which reach advances on each retry.
	ready := func(at time.Time) error {
		_, _, _, err := dispatch.registry.resolveAction(
			ctx, decision, base.AgentID, base.AgentGeneration,
			base.Capability, base.Action, base.SourceID, base.PolicyID,
			base.ObjectID, auth.OperationInvoke, at)
		return err
	}
	receipt, err := s.reach(
		ctx, decision, base, now, approvalAttempt{invoke: true, ready: ready})
	if err != nil {
		// Including a committed materialization whose approval event did not
		// publish: answered, not run. A retry finds the action through
		// enqueue's replay branch and claims it.
		return ActionRecord{}, err
	}
	return invokeAnswer(receipt)
}

// invokeAnswer is what Invoke makes of where its approval has got to: the
// queued record of an enqueued one, and otherwise the final state as an
// answer. Request reports those states as receipts; Invoke has only an
// action to answer with, and there is none.
func invokeAnswer(receipt ApprovalReceipt) (ActionRecord, error) {
	switch receipt.State {
	case ApprovalEnqueued:
		return receipt.Action, nil
	case ApprovalRefused:
		return ActionRecord{}, approvalRefusedAnswer()
	case ApprovalExpired:
		return ActionRecord{}, approvalExpired()
	case ApprovalWithdrawn:
		return ActionRecord{}, approvalWithdrawn()
	case ApprovalStranded:
		return ActionRecord{}, shoal.WrapError(
			shoal.ErrorConflict, "the request can no longer progress",
			ErrApprovalStranded)
	default:
		// Pending and approved: reach answers an open pending request as an
		// error, and advance turns an approved one into enqueued, expired or
		// an error. Neither is ever a receipt here.
		return ActionRecord{}, shoal.NewError(
			shoal.ErrorInternal, "approval state is invalid")
	}
}

// materializeUnderDispatch answers a caller that holds invoke but not dispatch
// on the request's scope. Still approval required, as Invoke always answered
// such a caller, and decided before any approval is read, so it says nothing
// about whether one exists. Anything that is not an authorization refusal is
// returned unchanged.
func materializeUnderDispatch(err error) error {
	if !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		return err
	}
	return shoal.WrapError(
		shoal.ErrorConflict,
		"action requires approval; an approved request becomes work only "+
			"under dispatch on its scope, which this credential does not hold",
		ErrApprovalRequired)
}
