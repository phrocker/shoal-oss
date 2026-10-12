// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
)

// The terminal-states switch (#655) and the sweep's scan, at the unit level.
// The paths end to end, against the real composition, are in
// cmd/shoal-explore-web/approval_terminal_test.go.

// TestNoTransitionWritesATerminalStateWhileDisabled: with TerminalStates
// off, commit — the one place every transition passes — refuses a withdrawn
// or a stranded record, whatever built it, and the lazy settlement and the
// sweep write nothing. An expiry, which a build before #655 already writes,
// is still allowed.
func TestNoTransitionWritesATerminalStateWhileDisabled(t *testing.T) {
	pending := validApprovalRecord(t)
	store := &memoryApprovalStore{records: map[string]ApprovalRecord{
		string(pending.ID): CloneApprovalRecord(pending),
	}}
	now := pending.RequestedAt.Add(2 * time.Minute)
	service := &ApprovalService{
		dispatch: &DispatchService{clock: func() time.Time { return now }},
		store:    store, recorder: unguardedApprovalRecorder{},
		generations: fixedGenerations{generation: 4},
		sweeper: func(context.Context, ApprovalSweepScope) (context.Context, error) {
			t.Fatal("the sweep bound its observer while terminal states are off")
			return nil, nil
		},
	}
	decision := approvalBranchDecision(t, pending.RequestedAt, auth.OperationDispatch)
	for _, record := range []ApprovalRecord{
		withdrawnRecord(t),
		strandedRecord(pending, ApprovalConditionTargetMoved, now),
	} {
		if _, err := service.commit(
			context.Background(), "approval_strand", auth.OperationDispatch,
			decision, record, pending.Version,
		); !errors.Is(err, ErrApprovalTerminalStatesDisabled) {
			t.Fatalf("commit of a %s record = %v", record.State, err)
		}
	}
	// The policy in force (4) is past the request's (3): with the switch on
	// this would strand.
	if _, settled, err := service.settleObserved(
		context.Background(), auth.OperationDispatch, decision, pending, now,
	); settled || err != nil {
		t.Fatalf("settleObserved while disabled = %v, %v", settled, err)
	}
	if _, err := service.Sweep(context.Background(), ApprovalSweepRequest{
		Limit: MaxApprovalListResults,
	}); !errors.Is(err, ErrApprovalTerminalStatesDisabled) {
		t.Fatalf("sweep while disabled = %v", err)
	}
	stored, _ := store.GetApproval(context.Background(), pending.ID)
	if stored.State != ApprovalPending || stored.Version != pending.Version {
		t.Fatalf("a disabled host wrote %q v%d", stored.State, stored.Version)
	}
	// An expiry is a state every build reads, and is still written.
	expired := CloneApprovalRecord(pending)
	expired.Version++
	expired.State = ApprovalExpired
	expired.UpdatedAt = pending.ExpiresAt
	if _, err := service.commit(
		context.Background(), "approval_expiry", auth.OperationDispatch,
		decision, expired, pending.Version,
	); err != nil {
		t.Fatalf("an expiry while disabled = %v", err)
	}
}

// tolerantStore answers the sweep's tolerant scan with a page that skipped
// some undecodable records, and counts action reads.
type tolerantStore struct {
	*scanningApprovalStore
	undecodable int
}

func (s tolerantStore) ScanApprovalsTolerant(
	ctx context.Context, after []byte, limit int,
) (ApprovalPage, error) {
	page, err := s.ScanApprovals(ctx, after, limit)
	page.Undecodable = s.undecodable
	return page, err
}

type countingActionStore struct {
	*memoryDispatchStore
	reads atomic.Int64
}

func (s *countingActionStore) GetAction(ctx context.Context, id []byte) (ActionRecord, error) {
	s.reads.Add(1)
	return s.memoryDispatchStore.GetAction(ctx, id)
}

// TestTheSweepSkipsWhatCannotChange: an undecodable record no longer ends
// the page (it is counted, in the result and the process counter), and an
// enqueued approval whose action is its materialization is read once and
// then skipped, since nothing can change it again.
func TestTheSweepSkipsWhatCannotChange(t *testing.T) {
	pending := validApprovalRecord(t)
	approved := decided(pending, ApprovalApproved, ApprovalVerdictApprove)
	enqueued := approved
	enqueued.Version++
	enqueued.State = ApprovalEnqueued
	enqueued.MaterializedAt = approved.DecidedAt.Add(time.Minute)
	enqueued.UpdatedAt = enqueued.MaterializedAt
	actions := &countingActionStore{memoryDispatchStore: newMemoryDispatchStore()}
	action := cloneActionRecord(enqueued.Request)
	action.ApprovalRequestDigest = enqueued.RequestDigest
	action.ApproverSubject, action.ApproverActor = enqueued.ApproverSubject, enqueued.ApproverActor
	action.ApprovedAt = enqueued.DecidedAt
	actions.records[string(action.ID)] = action
	store := tolerantStore{
		scanningApprovalStore: &scanningApprovalStore{memoryApprovalStore: &memoryApprovalStore{
			records: map[string]ApprovalRecord{string(enqueued.ID): CloneApprovalRecord(enqueued)},
		}},
		undecodable: 2,
	}
	now := enqueued.UpdatedAt.Add(time.Minute)
	service := &ApprovalService{
		dispatch: &DispatchService{store: actions, clock: func() time.Time { return now }},
		store:    store, recorder: unguardedApprovalRecorder{},
		generations: fixedGenerations{generation: 3}, terminal: true,
		sweeper: func(context.Context, ApprovalSweepScope) (context.Context, error) {
			t.Fatal("the sweep bound its observer for a record that needs nothing")
			return nil, nil
		},
	}
	first, err := service.Sweep(context.Background(), ApprovalSweepRequest{Limit: MaxApprovalListResults})
	if err != nil || first.Examined != 1 || first.Undecodable != 2 || first.Failed != 0 {
		t.Fatalf("first sweep = %+v, %v", first, err)
	}
	if actions.reads.Load() != 1 {
		t.Fatalf("first sweep read the action %d times", actions.reads.Load())
	}
	second, err := service.Sweep(context.Background(), ApprovalSweepRequest{Limit: MaxApprovalListResults})
	if err != nil || second.Settled != 1 || actions.reads.Load() != 1 {
		t.Fatalf("second sweep = %+v, %v, with %d action reads", second, err, actions.reads.Load())
	}
	if undecodable, failed := service.ApprovalSweepCounters(); undecodable != 4 || failed != 0 {
		t.Fatalf("counters = %d undecodable, %d failed", undecodable, failed)
	}
}
