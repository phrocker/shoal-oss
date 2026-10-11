// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

var errPublishUnavailable = errors.New("event unavailable")

// unentitledEvents refuses entitlement for every row while publishing
// successfully, which is the one combination no existing double covers: the
// others exercise publication failure and answer the entitlement question yes.
type unentitledEvents struct {
	published  int
	publishErr error
}

func (e *unentitledEvents) MayPublishActionEvent(
	context.Context, string, ActionRecord,
) (bool, error) {
	return false, nil
}

func (e *unentitledEvents) PublishActionEvent(
	context.Context, string, ActionRecord,
) error {
	if e.publishErr != nil {
		return e.publishErr
	}
	e.published++
	return nil
}

// TestASkippedPublicationIsCounted pins the counter #642 exposes on the health
// port against the branch it is supposed to measure.
//
// Without this the counter is only ever observed through a stub in the
// renderer, which would render a number whether or not the service ever
// incremented one.
func TestASkippedPublicationIsCounted(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	registryStore := newMemoryStore()
	registry, _ := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(),
		Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": &dispatchExecutor{}},
		Clock:     func() time.Time { return now },
	})
	registryStore.records["agent"] = Stored{Descriptor: dispatchDescriptor(now)}
	events := &unentitledEvents{}
	service, _ := NewDispatchService(DispatchConfig{
		Store: newMemoryDispatchStore(), Registry: registry,
		Resolver: authority.Resolver(), Recorder: &dispatchRecorder{},
		Events: events, Clock: func() time.Time { return now },
	})

	// A pending row has to exist before anything can be skipped, and an
	// enqueue that publishes successfully drains its own row immediately —
	// the first version of this test did exactly that and counted nothing,
	// which its own vacuity check caught. So the enqueue's publication fails,
	// stranding the row: that is the state the skip branch exists for.
	events.publishErr = errPublishUnavailable
	enqueueCtx := bindDecision(t, authority, dispatchDecisionWithProvenance(
		t, now, "enqueue-request", "enqueue-correlation", 1, now.Add(time.Hour)))
	request := dispatchEnqueueWithContext(
		now, "enqueue-request", "enqueue-correlation")
	if _, err := service.Enqueue(enqueueCtx, request); !errors.Is(
		err, ErrActionCommitted) {
		t.Fatalf("enqueue with a failing publication = %v, want "+
			"ErrActionCommitted: the write landed and the event did not", err)
	}
	// The error path returns no record, so the action is named by the request.
	actionID := request.ID
	// Own transitions are never gated on entitlement, so nothing is counted
	// yet even though a row is now pending.
	if got := service.SkippedTransitionPublications(); got != 0 {
		t.Fatalf("skips after an enqueue = %d, want 0: a caller's own "+
			"transition is never gated on entitlement", got)
	}
	events.publishErr = nil

	// A reconcile that is publishing nothing of its own — the operator shape,
	// ownKind empty — asks the entitlement question about every row and is
	// refused, so each row stays pending and is counted.
	before := events.published
	reconcile := ReconcileRequest{ID: actionID, Context: RequestContext{
		RequestID: "enqueue-request", CorrelationID: "enqueue-correlation",
		ReasonCode: "operator_request", Deadline: now.Add(time.Hour),
	}}
	if err := service.ReconcileActionTransitions(
		enqueueCtx, reconcile); err != nil {
		t.Fatalf("a reconcile that may publish nothing returned %v, want nil: "+
			"a row this caller is not entitled to publish is left pending, "+
			"not an error (#480 item 3)", err)
	}
	skipped := service.SkippedTransitionPublications()
	if skipped == 0 {
		t.Fatal("a refused reconcile counted no skip, so the counter the " +
			"health port renders cannot move and measures nothing")
	}
	// Refused means not published. Without this the test would pass on a
	// service that counted a skip and published anyway.
	if events.published != before {
		t.Fatalf("published %d rows while refusing entitlement",
			events.published-before)
	}

	// And it accumulates rather than latching at one.
	if err := service.ReconcileActionTransitions(
		enqueueCtx, reconcile); err != nil {
		t.Fatal(err)
	}
	if again := service.SkippedTransitionPublications(); again <= skipped {
		t.Fatalf("skips = %d after a second refused reconcile, want more "+
			"than %d: a counter that latches reports one stranding forever",
			again, skipped)
	}
}

// TestEveryFleetPrincipalMayReconcile pins the gate's width: a row's owner may
// hold any of the three fleet operations — an enqueuer under dispatch or
// invoke, a claimant under execute — so narrowing the gate would refuse one of
// those groups at the door.
//
// What the width does not do is let anyone drain a row they do not own.
// publisherMatchesTransition requires an exact principal match per row, so a
// caller admitted here publishes only its own. An earlier version of this
// comment claimed the width made "every stranded row reachable by someone";
// that was false, and a review caught it by testing the outcome where I had
// only tested the gate. Admission is necessary and nowhere near sufficient.
//
// A caller holding none of the three is still refused: the gate is wide, not
// absent.
func TestEveryFleetPrincipalMayReconcile(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	for _, probe := range []struct {
		name      string
		operation auth.Operation
		admitted  bool
	}{
		{"execute", auth.OperationExecute, true},
		{"invoke", auth.OperationInvoke, true},
		{"dispatch", auth.OperationDispatch, true},
		{"retrieve only", auth.OperationRetrieve, false},
	} {
		t.Run(probe.name, func(t *testing.T) {
			authority, _ := auth.NewAuthorityWithClock(
				func() time.Time { return now })
			registryStore := newMemoryStore()
			registry, _ := NewService(Config{
				Store: registryStore, Resolver: authority.Resolver(),
				Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
				Executors: executorMap{"exec": &dispatchExecutor{}},
				Clock:     func() time.Time { return now },
			})
			registryStore.records["agent"] = Stored{
				Descriptor: dispatchDescriptor(now)}
			service, _ := NewDispatchService(DispatchConfig{
				Store: newMemoryDispatchStore(), Registry: registry,
				Resolver: authority.Resolver(), Recorder: &dispatchRecorder{},
				Events: &unentitledEvents{}, Clock: func() time.Time { return now },
			})

			decision, err := auth.NewDecision(auth.DecisionConfig{
				Subject: "subject", Actor: "actor",
				AuthorizationDomain:   []byte("domain"),
				AllowedOperations:     []auth.Operation{probe.operation},
				PermittedSourceIDs:    [][]byte{[]byte("source")},
				PermittedPolicyIDs:    [][]byte{[]byte("policy")},
				PolicyGeneration:      1,
				AuthenticationExpires: now.Add(time.Hour),
				RequestID:             "request",
				CorrelationID:         "correlation",
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx := bindDecision(t, authority, decision)
			err = service.ReconcileActionTransitions(ctx, ReconcileRequest{
				ID: []byte("action-one"),
				Context: RequestContext{
					RequestID: "request", CorrelationID: "correlation",
					ReasonCode: "operator_request",
					Deadline:   now.Add(time.Hour),
				},
			})
			if probe.admitted {
				// An empty outbox is a no-op, so admission is the whole claim
				// here: a nil answer means the gate let the caller through.
				if err != nil {
					t.Fatalf("a %s principal was refused at the gate: %v — a "+
						"row's owner may hold any of the three, so narrowing "+
						"this refuses one of those groups before the per-row "+
						"publisher check ever runs", probe.name, err)
				}
				return
			}
			if err == nil {
				t.Fatal("a caller holding no fleet operation drained an " +
					"action; the gate is wide, not absent")
			}
			if !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
				t.Fatalf("refusal = %v, want unauthorized", err)
			}
		})
	}
}
