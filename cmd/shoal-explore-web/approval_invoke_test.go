// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Synchronous invoke of an approval-required action (#655), against the
// composition openService builds. The only additions are a runnable executor
// bound to the harness's "local" reference, which counts what it runs, and a
// wrapper around the real action event publisher that records each record it
// is handed and can lose a publication.

// countingExecutor is an in-process executor that records every run.
type countingExecutor struct {
	runs atomic.Int64
}

func (e *countingExecutor) Execute(
	context.Context, fleet.Invocation,
) (fleet.ExecutionResult, error) {
	e.runs.Add(1)
	return fleet.ExecutionResult{Output: json.RawMessage(`{"ok":true}`)}, nil
}

type publishedAction struct {
	kind   string
	record fleet.ActionRecord
}

// watchedActionEvents wraps the real action event publisher.
type watchedActionEvents struct {
	fleet.ActionEventPublisher
	mu        sync.Mutex
	lose      map[string]int
	published []publishedAction
}

func (w *watchedActionEvents) PublishActionEvent(
	ctx context.Context, kind string, record fleet.ActionRecord,
) error {
	w.mu.Lock()
	if w.lose[kind] > 0 {
		w.lose[kind]--
		w.mu.Unlock()
		return shoal.NewError(shoal.ErrorUnavailable, "injected: lost before publish")
	}
	w.mu.Unlock()
	if err := w.ActionEventPublisher.PublishActionEvent(ctx, kind, record); err != nil {
		return err
	}
	w.mu.Lock()
	w.published = append(w.published, publishedAction{kind: kind, record: record})
	w.mu.Unlock()
	return nil
}

func (w *watchedActionEvents) loseNext(kind string, times int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lose[kind] = times
}

// of is each transition at one identity the real publisher accepted, once:
// a pending row seen by several callers is published by each of them, which
// the publisher answers as the one event.
func (w *watchedActionEvents) of(id []byte) []publishedAction {
	w.mu.Lock()
	defer w.mu.Unlock()
	var result []publishedAction
	seen := map[string]bool{}
	for _, published := range w.published {
		key := fmt.Sprintf("%s/%d", published.kind, published.record.Version)
		if string(published.record.ID) == string(id) && !seen[key] {
			seen[key] = true
			result = append(result, published)
		}
	}
	return result
}

// actionEventsDelivered is the action event kinds the fleet event stream
// delivers for one identity, in order: what a subscriber actually sees.
func (h *invokeHarness) actionEventsDelivered(id []byte) []string {
	h.t.Helper()
	kinds, _ := h.approvalEventsOf(requesterSubscriber, id)
	var result []string
	for _, kind := range kinds {
		if strings.HasPrefix(kind, "action.") {
			result = append(result, kind)
		}
	}
	return result
}

type invokeHarness struct {
	*approvalHarness
	executor *countingExecutor
	events   *watchedActionEvents
}

// newInvokeHarness is newApprovalHarness with a runnable "local" executor
// and the action events watched. wrap, when set, wraps the approval store.
func newInvokeHarness(
	t *testing.T, wrap func(fleet.ApprovalStore) fleet.ApprovalStore,
) *invokeHarness {
	t.Helper()
	executor := &countingExecutor{}
	events := &watchedActionEvents{lose: map[string]int{}}
	h := &approvalHarness{t: t, root: t.TempDir(), wrap: wrap}
	h.clock.Store(time.Now().UTC().Add(time.Minute).Truncate(time.Second).UnixNano())
	authority, err := auth.NewAuthorityWithClock(h.now)
	if err != nil {
		t.Fatal(err)
	}
	h.authority = authority
	h.reader = &mutableFleetGeneration{}
	h.reader.value.Store(workspacePolicyGeneration)
	h.executors = configuredFleetExecutors{"local": executor}
	h.wrapActionEvents = func(inner fleet.ActionEventPublisher) fleet.ActionEventPublisher {
		events.ActionEventPublisher = inner
		return events
	}
	h.open()
	t.Cleanup(h.close)
	h.register("gateway", "approval-registration", 0, true, "")
	return &invokeHarness{approvalHarness: h, executor: executor, events: events}
}

func (h *invokeHarness) invokeAs(
	who principal, request heldRequest, claim string,
) (fleet.ActionRecord, error) {
	return h.opened.fleetDispatch.Invoke(h.as(who), fleet.InvokeRequest{
		Enqueue: h.enqueue(request), ClaimID: []byte(claim), Lease: time.Minute,
	})
}

// approve holds a request and has the approver approve it.
func (h *invokeHarness) approve(request heldRequest) fleet.ApprovalReceipt {
	h.t.Helper()
	receipt := h.mustRequest(requester, request, fleet.ApprovalPending)
	if _, err := h.decide(approver, receipt, fleet.ApprovalVerdictApprove); err != nil {
		h.t.Fatalf("approve %s: %v", request.id, err)
	}
	return receipt
}

// actionAbsent asserts no action exists at a request's identity.
func (h *invokeHarness) actionAbsent(request heldRequest) {
	h.t.Helper()
	if _, err := h.opened.fleetDispatch.Status(h.as(requester), fleet.StatusRequest{
		ID: request.id, Context: h.context(h.now().Add(time.Minute)),
	}); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		h.t.Fatalf("an action exists at %s: %v", request.id, err)
	}
}

func (h *invokeHarness) actionAt(request heldRequest) fleet.ActionRecord {
	h.t.Helper()
	record, err := h.opened.fleetDispatch.Status(h.as(requester), fleet.StatusRequest{
		ID: request.id, Context: h.context(h.now().Add(time.Minute)),
	})
	if err != nil {
		h.t.Fatalf("status of %s: %v", request.id, err)
	}
	return record
}

func (h *invokeHarness) assertRuns(want int64) {
	h.t.Helper()
	if got := h.executor.runs.Load(); got != want {
		h.t.Fatalf("the executor ran %d times, want %d", got, want)
	}
}

// TestInvokeBeforeApprovalWritesNothing: with no request held, and with one
// held and undecided, Invoke is 409 approval required, naming the approval
// route, and writes nothing — no hold, no transition, no action.
func TestInvokeBeforeApprovalWritesNothing(t *testing.T) {
	t.Parallel()
	h := newInvokeHarness(t, nil)

	unheld := h.held("invoke-unheld")
	_, err := h.invokeAs(requester, unheld, "claim")
	if !errors.Is(err, fleet.ErrApprovalRequired) ||
		!shoal.IsErrorCode(err, shoal.ErrorConflict) ||
		!strings.Contains(err.Error(), "/api/v1/fleet/approvals/request") {
		t.Fatalf("invoke with no request held = %v", err)
	}
	// Invoke does not hold: there is still no approval to report.
	if _, err := h.opened.approvals.Status(h.as(requester), fleet.ApprovalStatusRequest{
		ID: unheld.id, Context: h.context(h.now().Add(time.Minute)),
	}); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("invoke created an approval: %v", err)
	}
	h.actionAbsent(unheld)

	pending := h.held("invoke-pending")
	h.mustRequest(requester, pending, fleet.ApprovalPending)
	_, err = h.invokeAs(requester, pending, "claim")
	if !errors.Is(err, fleet.ErrApprovalRequired) ||
		!shoal.IsErrorCode(err, shoal.ErrorConflict) ||
		!strings.Contains(err.Error(), "awaiting a decision") {
		t.Fatalf("invoke of a pending request = %v", err)
	}
	if stored := h.status(pending.id).Approval; stored.State != fleet.ApprovalPending ||
		stored.Version != 1 {
		t.Fatalf("invoke moved a pending request to %q v%d", stored.State, stored.Version)
	}
	h.actionAbsent(pending)
	h.assertRuns(0)
	if published := h.events.of(pending.id); len(published) != 0 {
		t.Fatalf("invoke before approval published %d action events", len(published))
	}
}

// TestInvokeAfterApprovalRunsExactlyOnce: an approved request is
// materialized and run by Invoke, once, under the requester's credential
// and ClaimID. A retry replays the outcome, and a re-request through the
// approval route returns the same record.
//
// The enqueue and the claim happen inside one call under two operations, and
// each event is published under its own: the enqueue under dispatch (the
// materialization), the claim and the completion under invoke (the run). The
// real publisher is asked directly which operation it authorizes each
// published record under.
func TestInvokeAfterApprovalRunsExactlyOnce(t *testing.T) {
	t.Parallel()
	h := newInvokeHarness(t, nil)
	request := h.held("invoke-approved")
	receipt := h.approve(request)

	done, err := h.invokeAs(requester, request, "invoke-claim")
	if err != nil {
		t.Fatalf("invoke after approval: %v", err)
	}
	h.assertRuns(1)
	if done.State != fleet.DispatchSucceeded ||
		string(done.ClaimID) != "invoke-claim" || done.ClaimFence != 1 ||
		done.ClaimantSubject != requester.subject ||
		done.Subject != requester.subject ||
		done.ApproverSubject != approver.subject ||
		string(done.ApprovalRequestDigest) != string(receipt.RequestDigest) ||
		done.TransitionOperation != auth.OperationInvoke {
		t.Fatalf("invoked record = %+v", done)
	}
	if fmt.Sprint(done.AuthorizedOperations) !=
		fmt.Sprint([]auth.Operation{auth.OperationDispatch, auth.OperationInvoke}) {
		t.Fatalf("authorized operations = %v", done.AuthorizedOperations)
	}
	if stored := h.status(request.id).Approval; stored.State != fleet.ApprovalEnqueued {
		t.Fatalf("approval after invoke = %q", stored.State)
	}

	// Retried, and re-requested: the one record, run once.
	again, err := h.invokeAs(requester, request, "invoke-claim")
	if err != nil || again.State != fleet.DispatchSucceeded ||
		again.Version != done.Version {
		t.Fatalf("invoke retry = %+v, %v", again, err)
	}
	if rerequested := h.mustRequest(
		requester, request, fleet.ApprovalEnqueued,
	); rerequested.Action.Version != done.Version {
		t.Fatalf("re-request after invoke = %+v", rerequested.Action)
	}
	h.assertRuns(1)

	// One event per transition, each under its own operation.
	published := h.events.of(request.id)
	var kinds []string
	for _, event := range published {
		kinds = append(kinds, event.kind)
	}
	assertKinds(t, "action events", kinds,
		"action.enqueued", "action.claimed", "action.completed")
	assertKinds(t, "delivered action events", h.actionEventsDelivered(request.id),
		"action.enqueued", "action.claimed", "action.completed")
	only := func(operation auth.Operation) context.Context {
		return h.as(principal{
			subject: requester.subject, actor: requester.actor,
			operations: []auth.Operation{operation},
		})
	}
	for _, event := range published {
		want := auth.OperationInvoke
		if event.kind == "action.enqueued" {
			want = auth.OperationDispatch
		}
		if event.record.TransitionOperation != want {
			t.Fatalf("%s carries transition operation %q, want %q",
				event.kind, event.record.TransitionOperation, want)
		}
		for _, operation := range []auth.Operation{
			auth.OperationDispatch, auth.OperationInvoke,
		} {
			may, err := h.events.ActionEventPublisher.MayPublishActionEvent(
				only(operation), event.kind, event.record)
			if err != nil {
				t.Fatalf("may %s publish %s: %v", operation, event.kind, err)
			}
			if may != (operation == want) {
				t.Fatalf("%s publishes under %s: %v, want only under %s",
					event.kind, operation, may, want)
			}
		}
	}
}

// TestConcurrentInvokesOfOneApprovalRunOnce: several Invokes of one approved
// request, each with its own ClaimID, race through the materialization and
// the claim. The work runs once; every other call is answered with the
// outcome or a conflict, never a second run.
func TestConcurrentInvokesOfOneApprovalRunOnce(t *testing.T) {
	t.Parallel()
	h := newInvokeHarness(t, nil)
	request := h.held("invoke-race")
	h.approve(request)

	const callers = 6
	var wait sync.WaitGroup
	var succeeded atomic.Int64
	errs := make([]error, callers)
	for index := 0; index < callers; index++ {
		wait.Add(1)
		ctx := h.as(requester)
		go func(index int) {
			defer wait.Done()
			record, err := h.opened.fleetDispatch.Invoke(ctx, fleet.InvokeRequest{
				Enqueue: h.enqueue(request),
				ClaimID: []byte(fmt.Sprintf("race-claim-%d", index)),
				Lease:   time.Minute,
			})
			if err == nil && record.State == fleet.DispatchSucceeded {
				succeeded.Add(1)
			}
			errs[index] = err
		}(index)
	}
	wait.Wait()
	h.assertRuns(1)
	if succeeded.Load() == 0 {
		t.Fatalf("no invoke succeeded: %v", errs)
	}
	for index, err := range errs {
		// The claim race's answers, which the HTTP layer serves as 409.
		if err != nil && !errors.Is(err, fleet.ErrActionConflict) &&
			!errors.Is(err, fleet.ErrClaimLost) &&
			!shoal.IsErrorCode(err, shoal.ErrorConflict) {
			t.Fatalf("caller %d = %v, want success or a conflict", index, err)
		}
	}
	if record := h.actionAt(request); record.State != fleet.DispatchSucceeded ||
		record.ClaimFence != 1 {
		t.Fatalf("raced record = %q fence %d", record.State, record.ClaimFence)
	}
	kinds := []string{}
	for _, event := range h.events.of(request.id) {
		kinds = append(kinds, event.kind)
	}
	assertKinds(t, "raced action events", kinds,
		"action.enqueued", "action.claimed", "action.completed")
	assertKinds(t, "raced delivered events", h.actionEventsDelivered(request.id),
		"action.enqueued", "action.claimed", "action.completed")
}

// TestInvokeRequiresDispatchToMaterialize: a requester whose dispatch was
// revoked after making the hold cannot turn it into work through Invoke. The
// refusal is decided before any approval is read, so it is the same for an
// identity with nothing held, and it writes nothing: the requester's
// unrevoked credential still materializes and runs it afterwards.
func TestInvokeRequiresDispatchToMaterialize(t *testing.T) {
	t.Parallel()
	h := newInvokeHarness(t, nil)
	request := h.held("invoke-revoked")
	h.approve(request)
	revoked := principal{
		subject: requester.subject, actor: requester.actor,
		operations: []auth.Operation{auth.OperationInvoke},
	}

	_, held := h.invokeAs(revoked, request, "claim")
	_, absent := h.invokeAs(revoked, h.held("invoke-revoked-absent"), "claim")
	for name, err := range map[string]error{"held": held, "absent": absent} {
		if !errors.Is(err, fleet.ErrApprovalRequired) ||
			!shoal.IsErrorCode(err, shoal.ErrorConflict) ||
			!strings.Contains(err.Error(), "under dispatch") {
			t.Fatalf("%s: invoke without dispatch = %v", name, err)
		}
	}
	if held.Error() != absent.Error() {
		t.Fatalf("the answer depends on what is held: %q vs %q", held, absent)
	}
	if stored := h.status(request.id).Approval; stored.State != fleet.ApprovalApproved ||
		stored.Version != 2 {
		t.Fatalf("a refused invoke moved the approval to %q v%d",
			stored.State, stored.Version)
	}
	h.actionAbsent(request)
	h.assertRuns(0)

	if _, err := h.invokeAs(requester, request, "claim"); err != nil {
		t.Fatalf("invoke with dispatch after a refused one: %v", err)
	}
	h.assertRuns(1)
}

// TestAnApproverCannotInvokeTheRequest: the approver holds no invoke, so it
// is refused before anything is read, exactly as for an identity nobody
// holds.
func TestAnApproverCannotInvokeTheRequest(t *testing.T) {
	t.Parallel()
	h := newInvokeHarness(t, nil)
	request := h.held("invoke-approver")
	h.approve(request)

	_, held := h.invokeAs(approver, request, "claim")
	_, absent := h.invokeAs(approver, h.held("invoke-approver-absent"), "claim")
	if held == nil || absent == nil || held.Error() != absent.Error() ||
		!shoal.IsErrorCode(held, shoal.ErrorUnauthorized) {
		t.Fatalf("approver invoke = %v; of an absent identity = %v", held, absent)
	}
	if stored := h.status(request.id).Approval; stored.State != fleet.ApprovalApproved {
		t.Fatalf("approver invoke moved the approval to %q", stored.State)
	}
	h.actionAbsent(request)
	h.assertRuns(0)
}

// TestInvokeRecoversACrashInsideTheMaterialization: the approved → enqueued
// write lands and the call fails before the action is written. Nothing has
// run. After a restart, the same Invoke completes the materialization and
// runs the work, once.
func TestInvokeRecoversACrashInsideTheMaterialization(t *testing.T) {
	t.Parallel()
	armed := &atomic.Bool{}
	h := newInvokeHarness(t, func(store fleet.ApprovalStore) fleet.ApprovalStore {
		return crashAfterMaterializeWrite{ApprovalStore: store, armed: armed}
	})
	request := h.held("invoke-crash")
	h.approve(request)

	armed.Store(true)
	if _, err := h.invokeAs(requester, request, "claim"); err == nil {
		t.Fatal("the injected crash did not surface")
	}
	armed.Store(false)
	if status := h.status(request.id); status.State != fleet.ApprovalEnqueued ||
		status.Condition != fleet.ApprovalConditionAwaitingAction {
		t.Fatalf("approval after crash = %q/%q", status.State, status.Condition)
	}
	h.actionAbsent(request)
	h.assertRuns(0)

	h.reopen()
	if done, err := h.invokeAs(
		requester, request, "claim",
	); err != nil || done.State != fleet.DispatchSucceeded {
		t.Fatalf("invoke after the crash = %+v, %v", done, err)
	}
	if _, err := h.invokeAs(requester, request, "claim"); err != nil {
		t.Fatalf("second invoke after the crash: %v", err)
	}
	h.assertRuns(1)
}

// TestInvokeRecoversAFailureBetweenMaterializeAndClaim: the action is
// written and its enqueued event is lost, so the call fails after the
// materialization and before the claim. The record is enqueued, under
// dispatch, and nothing has run. A retry of the same Invoke finds it through
// enqueue's replay branch, publishes the lost event once, and runs the work
// once.
func TestInvokeRecoversAFailureBetweenMaterializeAndClaim(t *testing.T) {
	t.Parallel()
	h := newInvokeHarness(t, nil)
	request := h.held("invoke-lost-event")
	h.approve(request)

	h.events.loseNext("action.enqueued", 1)
	if _, err := h.invokeAs(
		requester, request, "claim",
	); !errors.Is(err, fleet.ErrActionCommitted) {
		t.Fatalf("invoke whose enqueued event was lost = %v", err)
	}
	queued := h.actionAt(request)
	if queued.State != fleet.DispatchQueued || queued.Version != 1 ||
		queued.TransitionOperation != auth.OperationDispatch ||
		queued.ApproverSubject != approver.subject {
		t.Fatalf("record after the failure = %+v", queued)
	}
	h.assertRuns(0)

	if done, err := h.invokeAs(
		requester, request, "claim",
	); err != nil || done.State != fleet.DispatchSucceeded {
		t.Fatalf("retried invoke = %+v, %v", done, err)
	}
	h.assertRuns(1)
	kinds := []string{}
	for _, event := range h.events.of(request.id) {
		kinds = append(kinds, event.kind)
	}
	assertKinds(t, "recovered action events", kinds,
		"action.enqueued", "action.claimed", "action.completed")
	assertKinds(t, "recovered delivered events", h.actionEventsDelivered(request.id),
		"action.enqueued", "action.claimed", "action.completed")
}

// TestInvokeAnswersAFinalRequestWithoutRunning: a refused, withdrawn or
// expired request is answered with its state, and nothing runs. A request
// whose window closed — pending or approved — has its expiry written, as
// Request writes it.
func TestInvokeAnswersAFinalRequestWithoutRunning(t *testing.T) {
	t.Parallel()
	h := newInvokeHarness(t, nil)

	refused := h.held("invoke-refused")
	refusedReceipt := h.mustRequest(requester, refused, fleet.ApprovalPending)
	if _, err := h.decide(
		approver, refusedReceipt, fleet.ApprovalVerdictRefuse,
	); err != nil {
		t.Fatal(err)
	}
	withdrawn := h.held("invoke-withdrawn")
	h.mustRequest(requester, withdrawn, fleet.ApprovalPending)
	if _, err := h.withdraw(requester, withdrawn.id); err != nil {
		t.Fatal(err)
	}
	lapsedPending := h.held("invoke-lapsed-pending")
	h.mustRequest(requester, lapsedPending, fleet.ApprovalPending)
	lapsedApproved := h.held("invoke-lapsed-approved")
	h.approve(lapsedApproved)

	for _, check := range []struct {
		request  heldRequest
		sentinel error
	}{
		{refused, fleet.ErrApprovalRefused},
		{withdrawn, fleet.ErrApprovalWithdrawn},
	} {
		before := h.status(check.request.id).Approval
		if _, err := h.invokeAs(
			requester, check.request, "claim",
		); !errors.Is(err, check.sentinel) ||
			!shoal.IsErrorCode(err, shoal.ErrorConflict) {
			t.Fatalf("invoke of %s = %v, want %v", check.request.id, err, check.sentinel)
		}
		if after := h.status(check.request.id).Approval; after.Version != before.Version ||
			after.State != before.State {
			t.Fatalf("invoke of %s moved it from %q v%d to %q v%d", check.request.id,
				before.State, before.Version, after.State, after.Version)
		}
		h.actionAbsent(check.request)
	}

	h.advance(fleet.DefaultApprovalWindow + time.Second)
	for _, request := range []heldRequest{lapsedPending, lapsedApproved} {
		if _, err := h.invokeAs(
			requester, request, "claim",
		); !errors.Is(err, fleet.ErrApprovalExpired) ||
			!shoal.IsErrorCode(err, shoal.ErrorConflict) {
			t.Fatalf("invoke of %s past its window = %v", request.id, err)
		}
		if stored := h.status(request.id).Approval; stored.State != fleet.ApprovalExpired {
			t.Fatalf("%s after invoke past its window = %q", request.id, stored.State)
		}
		h.actionAbsent(request)
		// And again, now that the expiry is stored.
		if _, err := h.invokeAs(
			requester, request, "claim",
		); !errors.Is(err, fleet.ErrApprovalExpired) {
			t.Fatalf("second invoke of %s past its window = %v", request.id, err)
		}
	}
	h.assertRuns(0)
}

// TestInvokeOfAStrandedRequestDoesNotRun: a request whose target moved is
// stranded by the observer that sees it, and an Invoke naming it is refused
// — at the pinned resolve, before any approval is read — and runs nothing.
func TestInvokeOfAStrandedRequestDoesNotRun(t *testing.T) {
	t.Parallel()
	h := newInvokeHarness(t, nil)
	request := h.held("invoke-stranded")
	h.approve(request)
	// The agent re-registers at the next generation: the request names the
	// old one, which can never resolve again.
	if _, err := h.register(
		"gateway", "approval-registration-2", 1, true, "",
	); err != nil {
		t.Fatal(err)
	}
	if status := h.status(request.id); status.Approval.State != fleet.ApprovalStranded {
		t.Fatalf("approval after its target moved = %q/%q",
			status.Approval.State, status.Condition)
	}
	if _, err := h.invokeAs(requester, request, "claim"); err == nil {
		t.Fatal("invoke of a stranded request succeeded")
	}
	h.actionAbsent(request)
	h.assertRuns(0)
}

// TestInvokeRefusesAMalformedClaimBeforeMaterializing: a claim Invoke could
// never take is refused before the approval is touched, so it does not leave
// behind work it then fails to claim.
func TestInvokeRefusesAMalformedClaimBeforeMaterializing(t *testing.T) {
	t.Parallel()
	h := newInvokeHarness(t, nil)
	request := h.held("invoke-bad-claim")
	h.approve(request)
	for name, invoke := range map[string]fleet.InvokeRequest{
		"no claim ID": {Enqueue: h.enqueue(request), Lease: time.Minute},
		"no lease":    {Enqueue: h.enqueue(request), ClaimID: []byte("claim")},
		"too long a lease": {
			Enqueue: h.enqueue(request), ClaimID: []byte("claim"),
			Lease: fleet.MaxActionClaimTTL + time.Second,
		},
	} {
		if _, err := h.opened.fleetDispatch.Invoke(
			h.as(requester), invoke,
		); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
			t.Fatalf("%s: invoke = %v, want invalid argument", name, err)
		}
	}
	if stored := h.status(request.id).Approval; stored.State != fleet.ApprovalApproved {
		t.Fatalf("a refused invoke moved the approval to %q", stored.State)
	}
	h.actionAbsent(request)
	h.assertRuns(0)
}

// TestInvokeOfWorkThatCannotRunHereMaterializesNothing: with no in-process
// executor bound to the reference, an approved request is refused before the
// approved → enqueued commit (#455's ordering), and stays approved for the
// approval route to materialize for a worker.
func TestInvokeOfWorkThatCannotRunHereMaterializesNothing(t *testing.T) {
	t.Parallel()
	h := newApprovalHarness(t)
	request := h.held("invoke-not-runnable")
	receipt := h.mustRequest(requester, request, fleet.ApprovalPending)
	if _, err := h.decide(approver, receipt, fleet.ApprovalVerdictApprove); err != nil {
		t.Fatal(err)
	}
	if _, err := h.opened.fleetDispatch.Invoke(h.as(requester), fleet.InvokeRequest{
		Enqueue: h.enqueue(request), ClaimID: []byte("claim"), Lease: time.Minute,
	}); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("invoke of work that cannot run here = %v", err)
	}
	if stored := h.status(request.id).Approval; stored.State != fleet.ApprovalApproved ||
		stored.Version != 2 {
		t.Fatalf("a refused invoke moved the approval to %q v%d",
			stored.State, stored.Version)
	}
	if _, err := h.opened.fleetDispatch.Status(h.as(requester), fleet.StatusRequest{
		ID: request.id, Context: h.context(h.now().Add(time.Minute)),
	}); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("a refused invoke made work: %v", err)
	}
	h.mustRequest(requester, request, fleet.ApprovalEnqueued)
}
