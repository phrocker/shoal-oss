// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"context"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// executorClaimFixture enqueues as one principal so a second can try to claim.
//
// The two decisions differ in Subject, Actor and RequestID, which is what
// sameActionPrincipal compares — so the worker here is a genuinely different
// principal rather than the same one with a different grant.
type executorClaimFixture struct {
	service *DispatchService
	queued  ActionRecord
	// enqueuer holds invoke and dispatch, as an agent asking for work does.
	enqueuer  context.Context
	authority *auth.Authority
	now       time.Time
}

func newExecutorClaimFixture(t *testing.T) *executorClaimFixture {
	t.Helper()
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	registryStore := newMemoryStore()
	registry, err := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(), Recorder: &memoryRecorder{},
		Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": &remoteBoundExecutor{}},
		Clock:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := dispatchDescriptor(now)
	descriptor.LeaseExpiresAt = now.Add(24 * time.Hour)
	registryStore.records["agent"] = Stored{Descriptor: descriptor}
	service, err := NewDispatchService(DispatchConfig{
		Store: newMemoryDispatchStore(), Registry: registry,
		Resolver: authority.Resolver(), Recorder: &dispatchRecorder{},
		Events: dispatchEvents{}, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	enqueuer := bindDecision(t, authority, dispatchDecision(t,
		"owner", "actor", "request",
		auth.OperationDispatch, auth.OperationInvoke))
	queued, err := service.Enqueue(enqueuer, dispatchEnqueue(now, "request"))
	if err != nil {
		t.Fatal(err)
	}
	return &executorClaimFixture{
		service: service, queued: queued, enqueuer: enqueuer,
		authority: authority, now: now,
	}
}

// worker binds a decision for a principal that is not the enqueuer.
func (f *executorClaimFixture) worker(t *testing.T, operations ...auth.Operation) context.Context {
	t.Helper()
	return bindDecision(t, f.authority, dispatchDecision(t,
		"gateway-subject", "gateway-actor", "gateway-request", operations...))
}

// TestAnExecutorClaimsWorkItDidNotEnqueue is the capability #437 adds, and the
// reason the gateway in docs/gateway-proxy-design.md could not be built.
//
// Claiming was authorized under OperationInvoke and additionally required the
// claimant to be the principal that enqueued. So an agent enqueued, a worker
// pulled with its own token, every record failed sameActionPrincipal, and the
// worker received an empty page — forever, with no error and nothing in any
// log. A ready worker, a growing queue, and nothing happening.
//
// OperationExecute is permission to take an agent's queued work and finish it,
// as distinct from permission to ask that agent to do something. Granted on
// the action's descriptor, it lets the enqueuer and the executor be different
// principals.
func TestAnExecutorClaimsWorkItDidNotEnqueue(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	worker := fixture.worker(t, auth.OperationExecute)

	// It is visible to the worker at all, which is the half that used to fail
	// silently. A test that only exercised Claim would pass against a Pull
	// that still filtered the record out, and the worker would never find the
	// action to claim.
	page, err := fixture.service.Pull(worker, PullActionsRequest{
		Limit: 10, Context: dispatchContext(fixture.now, "gateway-request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Actions) != 1 {
		t.Fatalf("pulled %d actions, want 1: a worker that cannot see the work "+
			"never claims it, and an empty page is not an error", len(page.Actions))
	}

	claimed, err := fixture.service.Claim(worker, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("worker-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "gateway-request"),
	})
	if err != nil {
		t.Fatalf("an executor granted execute could not claim: %v", err)
	}
	if claimed.State != DispatchClaimed {
		t.Fatalf("state = %v, want claimed", claimed.State)
	}

	// And it can finish what it took. A claim it cannot complete is the #384
	// gap again, reached from a different direction.
	completed, err := fixture.service.CompleteClaim(worker, CompletionRequest{
		ID: claimed.ID, ExpectedVersion: claimed.Version,
		ClaimID: []byte("worker-claim"),
		Result:  ExecutionResult{Output: []byte(`{"ok":true}`)},
		Context: dispatchContext(fixture.now, "gateway-request"),
	})
	if err != nil {
		t.Fatalf("an executor could claim and not complete: %v", err)
	}
	if completed.State != DispatchSucceeded {
		t.Fatalf("state = %v, want succeeded", completed.State)
	}
}

// TestInvokeAloneStillCannotClaimAnotherPrincipalsWork bounds the widening.
//
// The separation is the whole point: an agent that may ask a gateway to post a
// message must not thereby be able to claim another agent's queued work and
// read its input. If invoke were enough, #437 would have replaced one
// authorization hole with a larger one.
func TestInvokeAloneStillCannotClaimAnotherPrincipalsWork(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	// Every operation a non-executor could plausibly hold, and no execute.
	other := fixture.worker(t, auth.OperationInvoke, auth.OperationDispatch)

	page, err := fixture.service.Pull(other, PullActionsRequest{
		Limit: 10, Context: dispatchContext(fixture.now, "gateway-request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Actions) != 0 {
		t.Fatalf("a principal without execute pulled %d actions, want 0",
			len(page.Actions))
	}

	_, err = fixture.service.Claim(other, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("other-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "gateway-request"),
	})
	if err == nil {
		t.Fatal("a principal holding only invoke claimed another principal's work")
	}
	// And it is told exactly what an absent action is told. Two routes into
	// authorizedCurrent must not become two distinguishable answers, or the
	// absent-versus-foreign normalisation that keeps this surface from being an
	// existence oracle is gone (#398).
	if !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("foreign claim error = %v, want the not-found shape", err)
	}
	_, absentErr := fixture.service.Claim(other, ClaimRequest{
		ID: []byte("no-such-action"), ExpectedVersion: 1,
		ClaimID: []byte("other-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "gateway-request"),
	})
	if absentErr == nil {
		t.Fatal("an absent action was claimable")
	}
	if absentErr.Error() != err.Error() {
		t.Fatalf("absent and foreign differ:\n  absent: %v\n  foreign: %v",
			absentErr, err)
	}
}

// TestTheEnqueuingPrincipalStillClaimsUnderInvoke is the backward-compatibility
// half, and it is why the two routes are tried in the order they are.
//
// A deployment that enqueues and claims under one identity holds invoke and
// not execute. Requiring execute would have broken it on upgrade, so the
// principal-bound route remains and is tried second — the broader route first,
// the narrower one only if it fails, so nothing that worked stops working.
func TestTheEnqueuingPrincipalStillClaimsUnderInvoke(t *testing.T) {
	fixture := newExecutorClaimFixture(t)

	page, err := fixture.service.Pull(fixture.enqueuer, PullActionsRequest{
		Limit: 10, Context: dispatchContext(fixture.now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Actions) != 1 {
		t.Fatalf("the enqueuer pulled %d actions, want 1", len(page.Actions))
	}
	if _, err = fixture.service.Claim(fixture.enqueuer, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("own-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "request"),
	}); err != nil {
		t.Fatalf("the enqueuing principal lost the ability to claim: %v", err)
	}
}

// TestCancelAndStatusStayEnqueuerOnly pins the scope of this change.
//
// #437 separated execute from invoke for *claiming*. Whether an executor
// should be able to cancel or inspect is a different question behind a
// different operation, and widening dispatch here would answer it by accident.
// So an executor holding execute and dispatch still cannot reach either, and
// that is deliberate rather than overlooked.
func TestCancelAndStatusStayEnqueuerOnly(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	worker := fixture.worker(t, auth.OperationExecute, auth.OperationDispatch)

	if _, err := fixture.service.Status(worker, StatusRequest{
		ID: fixture.queued.ID, Context: dispatchContext(fixture.now, "gateway-request"),
	}); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("status for a non-enqueuer = %v, want the not-found shape", err)
	}
	if _, err := fixture.service.Cancel(worker, CancelRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		MutationKey: []byte("gateway-cancel"),
		Context:     dispatchContext(fixture.now, "gateway-request"),
	}); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("cancel for a non-enqueuer = %v, want the not-found shape", err)
	}
}
