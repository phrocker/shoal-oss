// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
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
	enqueuer      context.Context
	authority     *auth.Authority
	registryStore *memoryStore
	executors     executorMap
	now           time.Time
	// clock is what the service and the authority both read, so advancing it
	// moves them together. The fixture's own now is kept in step because every
	// test builds its RequestContext from it.
	clock *time.Time
}

// advance moves the fixture's clock forward. Tests that need a lease to lapse
// need this rather than a short sleep: the service clock is frozen, so a
// one-nanosecond lease is still live however long the test waits.
func (f *executorClaimFixture) advance(t *testing.T, by time.Duration) {
	t.Helper()
	*f.clock = f.clock.Add(by)
	f.now = *f.clock
}

func newExecutorClaimFixture(t *testing.T) *executorClaimFixture {
	t.Helper()
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	clock := &now
	authority, err := auth.NewAuthorityWithClock(
		func() time.Time { return *clock })
	if err != nil {
		t.Fatal(err)
	}
	registryStore := newMemoryStore()
	executors := executorMap{"exec": &remoteBoundExecutor{}}
	registry, err := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(), Recorder: &memoryRecorder{},
		Snapshots: fixedSnapshot{now},
		Executors: executors,
		Clock:     func() time.Time { return *clock },
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
		Events: dispatchEvents{}, Clock: func() time.Time { return *clock },
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
		authority: authority, registryStore: registryStore, now: now,
		executors: executors, clock: clock,
	}
}

// bindExecutor replaces the descriptor's bound executor with one that runs in
// process, so ExecuteClaim reaches it instead of refusing for want of an
// ActionExecutor implementation.
func (f *executorClaimFixture) bindExecutor(t *testing.T, executor ActionExecutor) {
	t.Helper()
	f.executors["exec"] = executor
}

// breakExecutor makes the record's descriptor unresolvable for a reason that is
// neither unauthorized nor absent, which is what produces the bare
// ErrorUnavailable that used to abort a page.
func (f *executorClaimFixture) breakExecutor(t *testing.T) {
	t.Helper()
	stored := f.registryStore.records["agent"]
	stored.Descriptor.ExecutorRef = "no-such-executor"
	f.registryStore.records["agent"] = stored
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

// TestAForeignClaimDoesNotDetachTheEnqueuer is the destructive case my first
// version of this change shipped, and the case TestCancelAndStatusStayEnqueuerOnly
// cannot see.
//
// applyClaim assigned record.Actor = decision.Actor(). That was a no-op while
// claiming required the principal to match, because the claimant was by
// construction the enqueuer. Once a third party could claim, the same line
// overwrote the record's own principal — permanently, since the completion path
// clones the record forward and never restores it. sameActionPrincipal compares
// Actor, so the enqueuer was then refused Status and Cancel on its own
// in-flight action, with no fallback: TeamActions needs an operation fleet
// principals do not hold.
//
// Claiming with a one-nanosecond lease is the sharp form. The claim commits,
// the lease lapses immediately so the action is reclaimable, and the enqueuer
// is detached from a record that is still live.
//
// The other test checks Status and Cancel on a record that was never claimed,
// which is why it passed throughout. The bodies were unchanged; their
// reachability was not, because the field they key on had become
// third-party-writable.
func TestAForeignClaimDoesNotDetachTheEnqueuer(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	worker := fixture.worker(t, auth.OperationExecute)

	if _, err := fixture.service.Claim(worker, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("worker-claim"), Lease: time.Nanosecond,
		Context: dispatchContext(fixture.now, "gateway-request"),
	}); err != nil {
		t.Fatal(err)
	}

	// The enqueuer can still read its own action.
	if _, err := fixture.service.Status(fixture.enqueuer, StatusRequest{
		ID: fixture.queued.ID, Context: dispatchContext(fixture.now, "request"),
	}); err != nil {
		t.Fatalf("the enqueuer lost Status on its own action after a foreign "+
			"claim: %v", err)
	}
	// And is still *recognised* by Cancel, which is the precise property. The
	// clock is frozen, so a one-nanosecond lease is still live and Cancel
	// correctly refuses with a conflict — that is the pre-existing semantics
	// and not the bug.
	//
	// The bug looked like ObjectNotFound: the predicate no longer recognising
	// the enqueuer at all. So the assertion is on which refusal arrives, not on
	// whether the call succeeds. An earlier version of this test required
	// success and failed for the right reason in the wrong way, which would
	// have been read as the fix not working.
	_, err := fixture.service.Cancel(fixture.enqueuer, CancelRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version + 1,
		MutationKey: []byte("owner-cancel"),
		Context:     dispatchContext(fixture.now, "request"),
	})
	if shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("Cancel no longer recognises the enqueuer after a foreign "+
			"claim: %v", err)
	}
	if !errors.Is(err, ErrActionConflict) {
		t.Fatalf("Cancel = %v, want the live-claim conflict", err)
	}
}

// TestAForeignResolveFailureDoesNotAbortThePage covers the shared-queue outage.
//
// resolveActionBinding returns a bare ErrorUnavailable when an executor is
// unregistered or an effect ceiling has narrowed. Before #437 the principal
// check ran first, so such a record never reached that call for anyone but its
// owner. With two routes it would abort the page of every execute-holder in the
// scope, on every pull, until the record expired — and any principal able to
// enqueue could plant one.
//
// The probe revokes the descriptor the record names, which is the cheapest way
// to make resolution fail for a reason that is neither unauthorized nor absent.
func TestAForeignResolveFailureDoesNotAbortThePage(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	worker := fixture.worker(t, auth.OperationExecute)

	// Sanity: the record is visible before anything is broken, or this test
	// would pass against a page that was empty for an unrelated reason.
	page, err := fixture.service.Pull(worker, PullActionsRequest{
		Limit: 10, Context: dispatchContext(fixture.now, "gateway-request"),
	})
	if err != nil || len(page.Actions) != 1 {
		t.Fatalf("precondition: pulled %d actions, err %v", len(page.Actions), err)
	}

	fixture.breakExecutor(t)

	// The page is now empty rather than an error. A skip is the pre-#437
	// behaviour for a record the caller does not own.
	page, err = fixture.service.Pull(worker, PullActionsRequest{
		Limit: 10, Context: dispatchContext(fixture.now, "gateway-request"),
	})
	if err != nil {
		t.Fatalf("one unresolvable foreign record aborted the whole page: %v", err)
	}
	if len(page.Actions) != 0 {
		t.Fatalf("pulled %d actions, want 0", len(page.Actions))
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

// TestAnExecuteHolderCannotProbeForExistence closes the #398 oracle on the
// route that reopened it.
//
// authorizedCurrent normalised only Unauthorized and NotFound from
// resolveActionBinding. A record whose descriptor fails executor availability
// or the effect ceiling returns a bare ErrorUnavailable, which surfaces as a
// different answer than absent. For the enqueuing principal that distinction is
// information it already has; for a caller reaching a record on the strength of
// an execute grant it is an existence oracle.
//
// It matters most on the admission namespace, whose IDs are computable from a
// principal tuple and whose isAdmission refusal runs *after* this — so the
// distinguishable error would arrive first.
//
// The existing foreign-claim test cannot catch this: a principal holding only
// invoke never produces the bare error, because the route that would resolve
// the binding without a principal check is the execute one, and it is not
// authorized to take it. It is refused at decision.AuthorizeObject on the
// execute route and then at the principal check on the invoke route — the
// order matters only to the comment, not the conclusion, and an earlier
// version of this one named the second mechanism alone. This test holds
// execute, which is the whole difference.
func TestAnExecuteHolderCannotProbeForExistence(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	worker := fixture.worker(t, auth.OperationExecute)
	// Unresolvable for a reason that is neither unauthorized nor absent, which
	// is what produces the bare ErrorUnavailable.
	fixture.breakExecutor(t)

	_, existing := fixture.service.Claim(worker, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("probe"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "gateway-request"),
	})
	_, absent := fixture.service.Claim(worker, ClaimRequest{
		ID: []byte("no-such-action"), ExpectedVersion: 1,
		ClaimID: []byte("probe"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "gateway-request"),
	})
	if existing == nil || absent == nil {
		t.Fatalf("expected both to be refused: existing=%v absent=%v", existing, absent)
	}
	if existing.Error() != absent.Error() {
		t.Fatalf("an execute-holder can tell an existing action from an absent "+
			"one:\n  existing: %v\n  absent:   %v", existing, absent)
	}
	if !shoal.IsErrorCode(existing, shoal.ErrorNotFound) {
		t.Fatalf("probe error = %v, want the not-found shape", existing)
	}
}

// principal is the whole of what sameClaimantPrincipal compares, so a test can
// vary one component at a time. dispatchDecision takes only subject, actor and
// request, which is why three of the four components had no coverage.
type principal struct {
	subject    string
	actor      string
	request    string
	clientID   shoal.ID
	onBehalfOf []shoal.ID
}

func dispatchDecisionFor(
	t *testing.T, who principal, operations ...auth.Operation,
) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: shoal.ID(who.subject), Actor: shoal.ID(who.actor),
		ClientID: who.clientID, OnBehalfOf: who.onBehalfOf,
		AuthorizationDomain: []byte("domain"),
		AllowedOperations:   operations,
		PermittedSourceIDs:  [][]byte{[]byte("source")},
		PermittedPolicyIDs:  [][]byte{[]byte("policy")}, PolicyGeneration: 1,
		AuthenticationExpires: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
		RequestID:             shoal.ID(who.request), CorrelationID: "correlation",
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

// namedWorker binds a decision for a specific principal, so two workers can be
// told apart. The shared worker helper mints one identity, which is enough for
// "not the enqueuer" and not enough for "not the other worker" — and the
// difference between those two is where this PR's worst defect lived.
func (f *executorClaimFixture) namedWorker(
	t *testing.T, name string, operations ...auth.Operation,
) context.Context {
	t.Helper()
	return bindDecision(t, f.authority, dispatchDecision(t,
		name+"-subject", name+"-actor", name+"-request", operations...))
}

// TestOneWorkerCannotCompleteAnotherWorkersClaim is the gate #437 removed
// without replacing, and the reason this PR was held in draft a second time.
//
// Claiming and completing were both authorized under OperationInvoke and both
// additionally required sameActionPrincipal, so the claimant was by
// construction the enqueuer and the enqueuer's chain was already on the record.
// Relaxing claiming to OperationExecute left the completion predicate as
// version, state and ClaimID — and none of those is an identity.
//
// ClaimID cannot stand in for one. validateOpaque accepts any one to
// MaxActionIDBytes bytes with no entropy requirement, dispatch_model.go calls
// it "the durable claimant identity" — which invites a worker to reuse a
// stable value — and it is published twice over: Status returns it to every
// co-principal, and Pull returns a claimed record whose lease has lapsed, its
// ClaimID intact, to every principal authorized to execute the descriptor.
//
// The result was worse than a fabricated outcome. The hijacker's write lands,
// and then the replay branch recognises the real worker's report as its own
// committed write and returns 200 carrying the hijacker's output. A worker that
// performed an irreversible external effect is told the work was recorded and
// handed someone else's result, which is the one failure mode it cannot detect.
func TestOneWorkerCannotCompleteAnotherWorkersClaim(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	holder := fixture.namedWorker(t, "holder", auth.OperationExecute)
	stranger := fixture.namedWorker(t, "stranger", auth.OperationExecute)

	claimed, err := fixture.service.Claim(holder, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("holder-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "holder-request"),
	})
	if err != nil {
		t.Fatalf("the holder could not claim: %v", err)
	}

	// The stranger holds execute on the same descriptor in the same scope and
	// has claimed nothing. It presents the holder's ClaimID and version.
	_, err = fixture.service.CompleteClaim(stranger, CompletionRequest{
		ID: fixture.queued.ID, ExpectedVersion: claimed.Version,
		ClaimID: []byte("holder-claim"),
		Result:  ExecutionResult{Output: json.RawMessage(`{"ok":false}`)},
		Context: dispatchContext(fixture.now, "stranger-request"),
	})
	if err == nil {
		t.Fatal("a principal that never claimed this action completed it, " +
			"which lets any execute-holder commit a fabricated outcome for " +
			"work another worker is performing")
	}
	// Concealed, not refused: ErrClaimLost would confirm the action exists to
	// a caller with no standing, which is the #398 oracle.
	if !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("the refusal discloses the action to a caller with no "+
			"standing: %v", err)
	}

	// The record is untouched, which is the part that matters. A refusal that
	// still wrote the output would be the same defect with an error attached.
	after, err := fixture.service.Status(fixture.enqueuer, StatusRequest{
		ID: fixture.queued.ID, Context: dispatchContext(fixture.now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if after.State != DispatchClaimed || len(after.Output) != 0 {
		t.Fatalf("the refused completion still changed the record: state=%s "+
			"output=%s", after.State, after.Output)
	}

	// And the real claimant still completes, with its own outcome rather than
	// a receipt for someone else's. This is the half that makes the test about
	// identity instead of about refusing everything.
	done, err := fixture.service.CompleteClaim(holder, CompletionRequest{
		ID: fixture.queued.ID, ExpectedVersion: claimed.Version,
		ClaimID: []byte("holder-claim"),
		Result:  ExecutionResult{Output: json.RawMessage(`{"ok":true}`)},
		Context: dispatchContext(fixture.now, "holder-request"),
	})
	if err != nil {
		t.Fatalf("the claim's actual holder was refused: %v", err)
	}
	if string(done.Output) != `{"ok":true}` {
		t.Fatalf("the claimant's own outcome was not recorded: %s", done.Output)
	}
}

// TestAReclaimMovesWhoMayReport covers the case the stored claimant has to get
// right to be usable at all: a lease lapses, a second worker takes the record,
// and the first worker must no longer be able to report on it.
//
// A claimant field that were only ever written once would pass the test above
// and still leave the first worker able to complete after losing the fence.
func TestAReclaimMovesWhoMayReport(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	first := fixture.namedWorker(t, "first", auth.OperationExecute)
	second := fixture.namedWorker(t, "second", auth.OperationExecute)

	claimed, err := fixture.service.Claim(first, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("first-claim"), Lease: time.Nanosecond,
		Context: dispatchContext(fixture.now, "first-request"),
	})
	if err != nil {
		t.Fatalf("the first worker could not claim: %v", err)
	}

	// The clock is frozen, so advance it past the lease rather than sleeping.
	fixture.advance(t, time.Second)

	reclaimed, err := fixture.service.Claim(second, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: claimed.Version,
		ClaimID: []byte("second-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "second-request"),
	})
	if err != nil {
		t.Fatalf("the second worker could not take the lapsed claim: %v", err)
	}
	if reclaimed.ClaimFence <= claimed.ClaimFence {
		t.Fatalf("the fence did not advance across claimants: %d then %d",
			claimed.ClaimFence, reclaimed.ClaimFence)
	}

	// The first worker reports under its own, now-stale claim.
	if _, err := fixture.service.CompleteClaim(first, CompletionRequest{
		ID: fixture.queued.ID, ExpectedVersion: claimed.Version,
		ClaimID: []byte("first-claim"),
		Result:  ExecutionResult{Output: json.RawMessage(`{"ok":false}`)},
		Context: dispatchContext(fixture.now, "first-request"),
	}); err == nil {
		t.Fatal("a worker whose claim was taken over still completed the " +
			"action, so the stored claimant is not updated on re-claim")
	}

	// And the new holder can.
	if _, err := fixture.service.CompleteClaim(second, CompletionRequest{
		ID: fixture.queued.ID, ExpectedVersion: reclaimed.Version,
		ClaimID: []byte("second-claim"),
		Result:  ExecutionResult{Output: json.RawMessage(`{"ok":true}`)},
		Context: dispatchContext(fixture.now, "second-request"),
	}); err != nil {
		t.Fatalf("the current holder was refused: %v", err)
	}
}

// TestAStrangerCannotReplayAnotherWorkersClaim covers Claim's replay branch,
// which is a second way into a live record.
//
// The branch exists so a worker whose response was lost receives the claim it
// already holds instead of a conflict against its own write, and it keys on
// ClaimID, version and lease — all three of which a second execute-holder
// knows, because Pull hands it a lapsed-claim record carrying them. Without an
// identity condition it returned the full record of a live claim, including the
// action's input, which Pull withholds precisely by excluding live-claimed
// records from the page.
func TestAStrangerCannotReplayAnotherWorkersClaim(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	holder := fixture.namedWorker(t, "holder", auth.OperationExecute)
	stranger := fixture.namedWorker(t, "stranger", auth.OperationExecute)

	if _, err := fixture.service.Claim(holder, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("holder-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "holder-request"),
	}); err != nil {
		t.Fatalf("the holder could not claim: %v", err)
	}

	// Exactly the request the holder would replay, from the wrong principal.
	replayed, err := fixture.service.Claim(stranger, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("holder-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "stranger-request"),
	})
	if err == nil {
		t.Fatalf("the replay branch handed a live claim to a principal that "+
			"does not hold it, input included: state=%s input=%s",
			replayed.State, replayed.Input)
	}

	// The holder's own replay still works, so this is an identity condition
	// rather than the branch being disabled.
	again, err := fixture.service.Claim(holder, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("holder-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "holder-request"),
	})
	if err != nil {
		t.Fatalf("the holder's own replay was refused, so a lost response is "+
			"now an unrecoverable conflict: %v", err)
	}
	if !bytes.Equal(again.ClaimID, []byte("holder-claim")) {
		t.Fatalf("the replay returned a different claim: %q", again.ClaimID)
	}
}

// breakingExecutor performs the work and, as a side effect of performing it,
// makes the action's descriptor stop resolving.
//
// That side effect is the only way to reach the post-effect re-authorization
// from a test. Both authorization checks in a completion live inside one call —
// the pre-effect resolve in authorizedClaimant and the post-effect one in
// applyExecutionResult — so nothing outside can act between them. Breaking the
// registry from inside Execute puts the change exactly where a revocation would
// land in production: after the effect, before the record.
type breakingExecutor struct {
	store *memoryStore
}

func (e *breakingExecutor) Execute(
	_ context.Context, invocation Invocation,
) (ExecutionResult, error) {
	stored := e.store.records["agent"]
	stored.Descriptor.ExecutorRef = "no-such-executor"
	e.store.records["agent"] = stored
	return ExecutionResult{Output: json.RawMessage(`{"ok":true}`)}, nil
}

// TestARevokedExecutorCannotCommitItsOutcome covers the post-effect
// re-authorization, which was the one line in this change that no test could
// see. Deleting it left the whole repository green.
//
// It is also the most expensive check here to get wrong, because it runs
// *after* the effect. An executor refused at this point has already performed
// irreversible work and is then told the outcome is ambiguous — so it has to
// refuse exactly the caller whose authorization genuinely went away and nobody
// else. Before this PR it was sameActionPrincipal paired with a
// resolveActionBinding hardcoded to OperationInvoke, which would have failed
// every execute-authorized completion at the last step, indistinguishably from
// the lease loss the ambiguity is reserved for.
func TestARevokedExecutorCannotCommitItsOutcome(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	fixture.bindExecutor(t, &breakingExecutor{store: fixture.registryStore})

	// Driven by the enqueuer, because ExecuteClaim is the in-process path and
	// requires sameActionPrincipal at its entry — in-process execution runs on
	// the enqueuer's behalf. That is also why this reaches the post-effect
	// check at all: the remote completion path resolves before the effect and
	// refuses a broken descriptor there, so a revocation can only be observed
	// after the work when the work happens in process.
	claimed, err := fixture.service.Claim(fixture.enqueuer, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("in-process-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "request"),
	})
	if err != nil {
		t.Fatalf("the enqueuer could not claim: %v", err)
	}

	_, err = fixture.service.ExecuteClaim(fixture.enqueuer, claimed)
	if err == nil {
		t.Fatal("a worker whose authorization to execute this action was " +
			"withdrawn mid-effect still committed its outcome")
	}
	// Ambiguous rather than a plain refusal, and that distinction is the
	// point. The work happened, so "this did not happen" would be a lie;
	// ErrExecutionAmbiguous is what tells a caller to treat the outcome as
	// unknown and reconcile it.
	if !errors.Is(err, ErrExecutionAmbiguous) {
		t.Fatalf("the refusal does not tell the caller its effect is "+
			"unreconciled, which is the only thing it can act on: %v", err)
	}

	// Put the descriptor back before reading the record. The executor broke it
	// permanently, and Status resolves the binding too — so without this the
	// read fails for the reason the test created rather than telling us
	// anything about the record.
	stored := fixture.registryStore.records["agent"]
	stored.Descriptor.ExecutorRef = "exec"
	fixture.registryStore.records["agent"] = stored

	// The record did not quietly become terminal on the way out, and recorded
	// no outcome. A refusal that still wrote the output would be the same
	// defect with an error attached.
	//
	// EffectPossible is deliberately not asserted here: applyClaim sets it
	// only for an action declaring external mutation or egress, and this
	// fixture's action declares neither, so it is false for a reason that has
	// nothing to do with this check. Asserting it would be asserting the
	// fixture.
	after, err := fixture.service.Status(fixture.enqueuer, StatusRequest{
		ID: fixture.queued.ID, Context: dispatchContext(fixture.now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if after.State != DispatchClaimed {
		t.Fatalf("the refused completion still moved the record to %s", after.State)
	}
	if len(after.Output) != 0 {
		t.Fatalf("the refused completion recorded an outcome anyway: %s",
			after.Output)
	}
}

// TestATerminalActionIsNotVisibleToAStranger covers the #398 normalisation on
// the routes this PR added, which is where it was reopened.
//
// Every route used to resolve through authorizedCurrent with
// sameActionPrincipal required unconditionally, so a caller with no standing
// was refused with ObjectNotFound before reaching any handler. The execute
// routes removed that gate deliberately. What they then exposed is that the
// handlers answer with ErrActionConflict, ErrActionTerminal and ErrClaimLost,
// and an absent action answers with ObjectNotFound — so an execute-holder could
// probe arbitrary action IDs, which are caller-chosen opaque bytes, and tell
// the two apart.
//
// Three branches reach an answer before any of them compares the caller to the
// record, and each is covered separately below. A first version of this test
// covered only the version mismatch, because it probed with a stale version —
// and removing the normalisation from either of the other two left the suite
// green.
//
// Terminal and past-deadline are two of the three states Pull does not return;
// the third is live-claimed, which an earlier version of this comment wrongly
// listed as visible through Pull and which has its own test now
// (TestALiveClaimIsNotVisibleToAStranger). A queued record is in the caller's
// page, so a refusal about one conceals nothing and is left informative.
func TestATerminalActionIsNotVisibleToAStranger(t *testing.T) {
	t.Run("a stale version", func(t *testing.T) {
		fixture := newExecutorClaimFixture(t)
		stranger := fixture.namedWorker(t, "stranger", auth.OperationExecute)
		fixture.cancel(t)
		fixture.assertIndistinguishable(t, stranger, 1)
	})

	t.Run("a terminal record at its current version", func(t *testing.T) {
		fixture := newExecutorClaimFixture(t)
		stranger := fixture.namedWorker(t, "stranger", auth.OperationExecute)
		cancelled := fixture.cancel(t)
		// Pull does not offer it, so this is a record the stranger has no
		// other way to observe.
		page, err := fixture.service.Pull(stranger, PullActionsRequest{
			Limit: 10, Context: dispatchContext(fixture.now, "stranger-request"),
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Actions) != 0 {
			t.Fatalf("a terminal action is still pullable, so this case is "+
				"not about a hidden record: %d actions", len(page.Actions))
		}
		fixture.assertIndistinguishable(t, stranger, cancelled.Version)
	})

	t.Run("a record past its deadline", func(t *testing.T) {
		fixture := newExecutorClaimFixture(t)
		// Still queued rather than terminal, so this reaches the deadline
		// check rather than the terminal one above it. The clock moves before
		// the caller is minted, because a decision bound beforehand would
		// expire with it and be refused at begin — both probes identically,
		// which would make this case pass without reaching the branch at all.
		// Two hours, not more: the action's deadline is an hour out and the
		// agent descriptor's lease is twenty-four, so a larger jump expires
		// the descriptor and the probe is refused for that instead — which
		// refuses both probes identically and so passes without reaching the
		// branch.
		fixture.advance(t, 2*time.Hour)
		stranger := fixture.namedWorker(t, "stranger", auth.OperationExecute)
		fixture.assertIndistinguishable(t, stranger, fixture.queued.Version)
	})
}

func (f *executorClaimFixture) cancel(t *testing.T) ActionRecord {
	t.Helper()
	cancelled, err := f.service.Cancel(f.enqueuer, CancelRequest{
		ID: f.queued.ID, ExpectedVersion: f.queued.Version,
		MutationKey: []byte("cancel-once"),
		Context:     dispatchContext(f.now, "request"),
	})
	if err != nil {
		t.Fatalf("the enqueuer could not cancel: %v", err)
	}
	return cancelled
}

// assertIndistinguishable requires that claiming and completing the fixture's
// real action produce byte-identical refusals to claiming and completing an
// action ID that was never used.
func (f *executorClaimFixture) assertIndistinguishable(
	t *testing.T, caller context.Context, version uint64,
) {
	t.Helper()
	absentID := []byte("never-existed")
	for _, probe := range []struct {
		name string
		call func(id []byte) error
	}{
		{"claim", func(id []byte) error {
			_, err := f.service.Claim(caller, ClaimRequest{
				ID: id, ExpectedVersion: version, ClaimID: []byte("probe"),
				Lease:   time.Minute,
				Context: dispatchContext(f.now, "stranger-request"),
			})
			return err
		}},
		{"complete", func(id []byte) error {
			_, err := f.service.CompleteClaim(caller, CompletionRequest{
				ID: id, ExpectedVersion: version, ClaimID: []byte("probe"),
				Result:  ExecutionResult{Output: json.RawMessage(`{"ok":true}`)},
				Context: dispatchContext(f.now, "stranger-request"),
			})
			return err
		}},
	} {
		present, absent := probe.call(f.queued.ID), probe.call(absentID)
		if present == nil {
			t.Fatalf("%s succeeded against a record this caller has no "+
				"standing on", probe.name)
		}
		if absent == nil {
			t.Fatalf("%s succeeded against an action ID that was never used",
				probe.name)
		}
		if present.Error() != absent.Error() {
			t.Errorf("%s distinguishes an existing action from an absent one "+
				"at version %d:\n existing: %v\n absent:   %v",
				probe.name, version, present, absent)
		}
		if !shoal.IsErrorCode(present, shoal.ErrorNotFound) {
			t.Errorf("%s answers an existing action with %v, which is not the "+
				"answer an absent one gets", probe.name, present)
		}
	}
}

// revokingExecutor performs the work and then deletes the agent's descriptor
// outright, so resolution afterwards is a clean not-found rather than a fault.
//
// That distinction is the whole reason this type exists alongside
// breakingExecutor. Pointing the descriptor at a missing executor reference
// produces a bare Unavailable, which claimableBy returns as an *error* — so the
// completion is refused by the `authorizeErr != nil` arm and the authorization
// verdict below it is never consulted. A review found that deleting either arm
// alone left the suite green. Deleting the descriptor exercises the verdict.
type revokingExecutor struct {
	store *memoryStore
}

func (e *revokingExecutor) Execute(
	_ context.Context, _ Invocation,
) (ExecutionResult, error) {
	delete(e.store.records, "agent")
	return ExecutionResult{Output: json.RawMessage(`{"ok":true}`)}, nil
}

// TestAnUnauthorizedExecutorCannotCommitItsOutcome covers the authorization
// verdict of the post-effect re-authorization, as distinct from a fault in
// reaching it.
//
// Both arms refuse, and both must: the work has happened either way, so the
// caller has to be told the outcome is unreconciled rather than that it did not
// occur. They are separate tests because they are separate lines, and a single
// fixture that trips the fault arm leaves the verdict arm uncovered.
func TestAnUnauthorizedExecutorCannotCommitItsOutcome(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	fixture.bindExecutor(t, &revokingExecutor{store: fixture.registryStore})

	claimed, err := fixture.service.Claim(fixture.enqueuer, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("in-process-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "request"),
	})
	if err != nil {
		t.Fatalf("the enqueuer could not claim: %v", err)
	}

	_, err = fixture.service.ExecuteClaim(fixture.enqueuer, claimed)
	if err == nil {
		t.Fatal("an executor no longer authorized for this action committed " +
			"its outcome anyway")
	}
	if !errors.Is(err, ErrExecutionAmbiguous) {
		t.Fatalf("the refusal does not tell the caller its effect is "+
			"unreconciled: %v", err)
	}
	// Specifically the verdict, not the fault arm above it. If the descriptor
	// had merely become unresolvable this would read "unavailable".
	if !strings.Contains(err.Error(), "terminal execution identity changed") {
		t.Fatalf("refused by the fault arm rather than by the authorization "+
			"verdict, so the verdict is still uncovered: %v", err)
	}
}

// TestTheClaimantIsComparedOnItsWholeChain covers the three components of
// sameClaimantPrincipal that no test could see.
//
// The shared worker helper varies subject, actor and request together, so a
// comparison checking only the subject passed every existing test.
//
// Each variant below differs from the claim's holder in **exactly one**
// component. A first version of this test varied three at once — it left the
// client ID and the delegation chain empty while changing the actor — so
// dropping any single comparison still refused, and dropping the Actor, the
// ClientID or the whole chain comparison from the production code left this
// test green. One component at a time is the whole point: a fixture that
// differs in three ways cannot tell you which of the three is checked.
func TestTheClaimantIsComparedOnItsWholeChain(t *testing.T) {
	holder := principal{
		subject: "holder-subject", actor: "holder-actor",
		request: "holder-request", clientID: "holder-client",
		onBehalfOf: []shoal.ID{"delegator"},
	}
	// Derived from the holder so every field matches unless a case changes it.
	differing := func(mutate func(*principal)) principal {
		other := holder
		other.onBehalfOf = append([]shoal.ID(nil), holder.onBehalfOf...)
		other.request = "other-request"
		mutate(&other)
		return other
	}

	for _, variant := range []struct {
		component string
		who       principal
	}{
		{"Actor", differing(func(p *principal) { p.actor = "other-actor" })},
		{"ClientID", differing(func(p *principal) { p.clientID = "other-client" })},
		{"OnBehalfOf (chain emptied)", differing(func(p *principal) { p.onBehalfOf = nil })},
		{"OnBehalfOf (different delegator)", differing(func(p *principal) {
			p.onBehalfOf = []shoal.ID{"other-delegator"}
		})},
	} {
		t.Run(variant.component, func(t *testing.T) {
			fixture := newExecutorClaimFixture(t)
			claimant := bindDecision(t, fixture.authority, dispatchDecisionFor(
				t, holder, auth.OperationExecute, auth.OperationDelegate))

			claimed, err := fixture.service.Claim(claimant, ClaimRequest{
				ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
				ClaimID: []byte("holder-claim"), Lease: time.Minute,
				Context: dispatchContext(fixture.now, holder.request),
			})
			if err != nil {
				t.Fatalf("the holder could not claim: %v", err)
			}

			other := bindDecision(t, fixture.authority, dispatchDecisionFor(
				t, variant.who, auth.OperationExecute, auth.OperationDelegate))
			_, err = fixture.service.CompleteClaim(other, CompletionRequest{
				ID: fixture.queued.ID, ExpectedVersion: claimed.Version,
				ClaimID: []byte("holder-claim"),
				Result:  ExecutionResult{Output: json.RawMessage(`{"ok":false}`)},
				Context: dispatchContext(fixture.now, variant.who.request),
			})
			if err == nil {
				t.Fatalf("a principal differing from the claimant only in %s "+
					"completed its claim, so that component is not compared",
					variant.component)
			}

			// And the holder itself still completes, so the refusal above is
			// about the differing component and not about the fixture having
			// made the claim unreportable by anyone.
			if _, err := fixture.service.CompleteClaim(
				claimant, CompletionRequest{
					ID: fixture.queued.ID, ExpectedVersion: claimed.Version,
					ClaimID: []byte("holder-claim"),
					Result: ExecutionResult{
						Output: json.RawMessage(`{"ok":true}`),
					},
					Context: dispatchContext(fixture.now, holder.request),
				}); err != nil {
				t.Fatalf("the holder was refused too, so this case proves "+
					"nothing about %s: %v", variant.component, err)
			}
		})
	}
}

// TestTheEnqueuerCannotReportOnAWorkersClaim pins the narrowing of who may
// report.
//
// The claimant check first shipped accepting the record's own principal
// alongside the claimant, on two stated grounds that are both false: that the
// enqueuer "can already cancel it at any moment", when Cancel refuses while a
// claim is live; and that fabricating its own action's outcome "harms only
// itself", when the replay branch then hands the worker that performed the
// effect a success receipt carrying the fabrication. That is the same failure
// the gate exists to prevent, with a narrower attacker.
//
// The enqueuer is admitted only where the record carries no claimant at all,
// which is the pre-#437 encoding and nothing else.
func TestTheEnqueuerCannotReportOnAWorkersClaim(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	worker := fixture.namedWorker(t, "worker", auth.OperationExecute)

	claimed, err := fixture.service.Claim(worker, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("worker-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "worker-request"),
	})
	if err != nil {
		t.Fatalf("the worker could not claim: %v", err)
	}

	// The enqueuer can read the ClaimID off Status, so presenting it is not the
	// hard part.
	status, err := fixture.service.Status(fixture.enqueuer, StatusRequest{
		ID: fixture.queued.ID, Context: dispatchContext(fixture.now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(status.ClaimID, []byte("worker-claim")) {
		t.Fatalf("Status does not disclose the claim ID (%q), so this test is "+
			"not exercising the path it describes", status.ClaimID)
	}

	if _, err := fixture.service.CompleteClaim(
		fixture.enqueuer, CompletionRequest{
			ID: fixture.queued.ID, ExpectedVersion: claimed.Version,
			ClaimID: status.ClaimID,
			Result:  ExecutionResult{Output: json.RawMessage(`{"ok":false}`)},
			Context: dispatchContext(fixture.now, "request"),
		}); err == nil {
		t.Fatal("the enqueuer committed an outcome for a claim a worker holds, " +
			"and the worker would then receive a success receipt for it")
	}

	// And the worker still gets its own outcome recorded, which is what makes
	// this a narrowing rather than a refusal of everything.
	done, err := fixture.service.CompleteClaim(worker, CompletionRequest{
		ID: fixture.queued.ID, ExpectedVersion: claimed.Version,
		ClaimID: []byte("worker-claim"),
		Result:  ExecutionResult{Output: json.RawMessage(`{"ok":true}`)},
		Context: dispatchContext(fixture.now, "worker-request"),
	})
	if err != nil {
		t.Fatalf("the claim's holder was refused: %v", err)
	}
	if string(done.Output) != `{"ok":true}` {
		t.Fatalf("the holder's own outcome was not recorded: %s", done.Output)
	}
}

// TestALiveClaimIsNotVisibleToAStranger is the fourth existence oracle, found
// by a review after the first three were closed.
//
// Claiming a live-claimed record answered ErrActionConflict with no standing
// check, on the stated grounds that such a record is already visible through
// Pull. It is not: Pull returns a claimed record only once its lease has
// lapsed. So an execute-holder could guess action IDs and tell a live-claimed
// record from an absent one — cheaply, because a freshly claimed record is at
// version 2 and every wrong guess answers not-found.
func TestALiveClaimIsNotVisibleToAStranger(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	holder := fixture.namedWorker(t, "holder", auth.OperationExecute)
	stranger := fixture.namedWorker(t, "stranger", auth.OperationExecute)

	claimed, err := fixture.service.Claim(holder, ClaimRequest{
		ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
		ClaimID: []byte("holder-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "holder-request"),
	})
	if err != nil {
		t.Fatalf("the holder could not claim: %v", err)
	}

	// Pull withholds it, which is what makes the error the only channel.
	page, err := fixture.service.Pull(stranger, PullActionsRequest{
		Limit: 10, Context: dispatchContext(fixture.now, "stranger-request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Actions) != 0 {
		t.Fatalf("Pull returned a live-claimed record, so this test is not "+
			"about a record the stranger cannot otherwise see: %d actions",
			len(page.Actions))
	}

	fixture.assertIndistinguishable(t, stranger, claimed.Version)
}

// TestTheLoserOfAClaimRaceIsToldNotFound pins a deliberate degradation, which
// is worth a test precisely because it is the losing side of a trade.
//
// Two workers pull the same page and both claim. The winner's claim makes the
// record live-claimed, and Pull withholds live-claimed records — so by the time
// the loser asks, the error is the only channel through which that record's
// existence could be learned, and it has to be the answer an absent action
// gets. The loser therefore receives not-found for work it saw in its own page
// moments earlier.
//
// The alternative was considered and rejected. Answering the loser with
// ErrActionConflict would mean an execute-holder could guess action IDs and
// distinguish a live-claimed record from a nonexistent one, which is cheap:
// every wrong guess answers not-found and a freshly claimed record sits at
// version 2. A misleading error for the loser of a race it can simply retry is
// a smaller cost than a working existence oracle.
//
// **A worker must therefore treat not-found from Claim as "re-pull", not as
// "this action is gone."** Operationally that is what it would do with a
// conflict anyway, which is what makes the trade affordable.
//
// An earlier comment in dispatch_service.go claimed this case could not arise,
// on the grounds that a caller holding the right version never reaches the
// version-mismatch branch. That ignored the race.
func TestTheLoserOfAClaimRaceIsToldNotFound(t *testing.T) {
	fixture := newExecutorClaimFixture(t)
	first := fixture.namedWorker(t, "first", auth.OperationExecute)
	second := fixture.namedWorker(t, "second", auth.OperationExecute)

	page, err := fixture.service.Pull(second, PullActionsRequest{
		Limit: 10, Context: dispatchContext(fixture.now, "second-request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Actions) != 1 {
		t.Fatalf("the loser must have seen the record in its own page for this "+
			"test to describe a race at all: %d actions", len(page.Actions))
	}
	offered := page.Actions[0]

	if _, err := fixture.service.Claim(first, ClaimRequest{
		ID: offered.ID, ExpectedVersion: offered.Version,
		ClaimID: []byte("first-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "first-request"),
	}); err != nil {
		t.Fatalf("the winner could not claim: %v", err)
	}

	_, err = fixture.service.Claim(second, ClaimRequest{
		ID: offered.ID, ExpectedVersion: offered.Version,
		ClaimID: []byte("second-claim"), Lease: time.Minute,
		Context: dispatchContext(fixture.now, "second-request"),
	})
	if err == nil {
		t.Fatal("both workers claimed the same action")
	}
	if !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("the loser of a claim race no longer receives not-found. If "+
			"that is deliberate, confirm an execute-holder still cannot "+
			"distinguish a live-claimed record from an absent one — see "+
			"TestALiveClaimIsNotVisibleToAStranger: %v", err)
	}

	// The record itself is untouched by the losing attempt: the winner still
	// holds it under its own claim.
	status, err := fixture.service.Status(fixture.enqueuer, StatusRequest{
		ID: fixture.queued.ID, Context: dispatchContext(fixture.now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(status.ClaimID, []byte("first-claim")) {
		t.Fatalf("the losing claim changed the holder to %q", status.ClaimID)
	}

	// And a still-queued record does produce a conflict for a stale version,
	// which is why the concealment keys on what Pull shows rather than on
	// standing alone. Nothing is concealed that the caller can already pull.
	fresh := newExecutorClaimFixture(t)
	stale := fresh.namedWorker(t, "stale", auth.OperationExecute)
	_, err = fresh.service.Claim(stale, ClaimRequest{
		ID: fresh.queued.ID, ExpectedVersion: fresh.queued.Version + 7,
		ClaimID: []byte("stale-claim"), Lease: time.Minute,
		Context: dispatchContext(fresh.now, "stale-request"),
	})
	if err == nil {
		t.Fatal("a claim at a version the record never had succeeded")
	}
	if shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("a queued record that Pull would hand this caller is "+
			"concealed anyway, which costs an informative error for nothing: %v",
			err)
	}
}

// TestCloningARecordDoesNotShareTheClaimantChain covers the one deep copy the
// claimant fields needed and nothing exercised.
//
// Three of the four are scalars that the struct assignment in
// cloneActionRecord copies correctly on its own. ClaimantOnBehalfOf is a slice,
// so without an explicit copy the clone shares its backing array — and clones
// are what leave this package: Status, Pull and every completion return one. A
// caller mutating the chain it was handed would be editing the record the
// service still holds, and that chain is half of who may report on the claim.
//
// Asserted by mutating the clone, because a test that only compared the two
// would pass against an aliased copy.
func TestCloningARecordDoesNotShareTheClaimantChain(t *testing.T) {
	original := ActionRecord{
		ID:                 []byte("action"),
		ClaimantSubject:    "holder",
		ClaimantActor:      "holder-actor",
		ClaimantOnBehalfOf: []shoal.ID{"delegator", "second"},
	}

	clone := cloneActionRecord(original)
	if len(clone.ClaimantOnBehalfOf) != 2 {
		t.Fatalf("the clone lost the claimant chain: %v", clone.ClaimantOnBehalfOf)
	}
	clone.ClaimantOnBehalfOf[0] = "attacker"
	if original.ClaimantOnBehalfOf[0] != "delegator" {
		t.Fatal("the clone shares the claimant delegation chain with the " +
			"record it was built from, so a caller holding a clone can edit " +
			"half of who may report on the claim")
	}

	// And the scalars survive, so this is not passing because the fields are
	// simply absent from the clone.
	if clone.ClaimantSubject != "holder" || clone.ClaimantActor != "holder-actor" {
		t.Fatalf("the clone did not carry the claimant identity: %+v", clone)
	}
}
