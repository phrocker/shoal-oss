// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
)

// This test is scoped to one guard and deliberately runs WITHOUT the hosted
// interaction recorder. In the hosted composition a superseded approval is
// also refused by the recorder's interaction pin, which would mask whether the
// approval service itself refuses it. Here the approval recorder records
// nothing, so the only thing that can refuse is advance's own in-force check.

type memoryApprovalStore struct {
	mu      sync.Mutex
	records map[string]ApprovalRecord
}

func (s *memoryApprovalStore) GetApproval(
	_ context.Context, id []byte,
) (ApprovalRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[string(id)]
	if !ok {
		return ApprovalRecord{}, ErrApprovalNotFound
	}
	return CloneApprovalRecord(record), nil
}

func (s *memoryApprovalStore) ApplyApproval(
	_ context.Context, mutation ApprovalMutation,
) (ApprovalRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.records[string(mutation.Record.ID)]
	if (!ok && mutation.ExpectedVersion != 0) ||
		(ok && current.Version != mutation.ExpectedVersion) {
		return ApprovalRecord{}, ErrApprovalConflict
	}
	s.records[string(mutation.Record.ID)] = CloneApprovalRecord(mutation.Record)
	return CloneApprovalRecord(mutation.Record), nil
}

func (s *memoryApprovalStore) ScanApprovals(
	context.Context, []byte, int,
) (ApprovalPage, error) {
	return ApprovalPage{}, nil
}

type unguardedApprovalRecorder struct{}

func (unguardedApprovalRecorder) RecordApproval(context.Context, ApprovalAudit) error {
	return nil
}

type fixedGenerations struct{ generation int64 }

func (g fixedGenerations) CurrentPolicyGeneration(
	context.Context, []byte,
) (int64, error) {
	return g.generation, nil
}

// TestApprovalRefusesASupersededApprovalBeforeCommitting: an approval given
// under policy generation 3 is re-requested with a token still at 3, after the
// policy in force has moved to 4. advance must refuse before the approved →
// enqueued commit, leave the row approved, and create no work.
func TestApprovalRefusesASupersededApprovalBeforeCommitting(t *testing.T) {
	approval := decided(validApprovalRecord(t), ApprovalApproved, ApprovalVerdictApprove)
	now := approval.DecidedAt.Add(2 * time.Second)
	store := &memoryApprovalStore{records: map[string]ApprovalRecord{
		string(approval.ID): CloneApprovalRecord(approval),
	}}
	dispatchStore := newMemoryDispatchStore()
	service := func(inForce int64) *ApprovalService {
		return &ApprovalService{
			dispatch: &DispatchService{
				store: dispatchStore, outbox: dispatchStore,
				recorder: &dispatchRecorder{}, events: dispatchEvents{},
				clock: func() time.Time { return now },
			},
			store: store, recorder: unguardedApprovalRecorder{},
			narrowed:    func(context.Context) bool { return false },
			generations: fixedGenerations{generation: inForce},
			window:      DefaultApprovalWindow,
		}
	}
	// The token is at the request's generation, as a stale one would be.
	decision := approvalBranchDecision(t, approval.RequestedAt, auth.OperationDispatch)

	_, err := service(approval.PolicyGeneration+1).advance(
		context.Background(), decision, approval, now)
	if !errors.Is(err, ErrApprovalSuperseded) {
		t.Fatalf("advance under a superseded policy = %v", err)
	}
	stored, _ := store.GetApproval(context.Background(), approval.ID)
	if stored.State != ApprovalApproved {
		t.Fatalf("superseded approval moved to %q", stored.State)
	}
	if _, err := dispatchStore.GetAction(
		context.Background(), approval.ID); !errors.Is(err, ErrActionNotFound) {
		t.Fatalf("work was created under a superseded policy: %v", err)
	}

	// Control: the same call with the policy unchanged materializes, so the
	// refusal above is the generation check and nothing else.
	receipt, err := service(approval.PolicyGeneration).advance(
		context.Background(), decision, approval, now)
	if err != nil || receipt.State != ApprovalEnqueued ||
		!bytes.Equal(receipt.Action.ID, approval.ID) {
		t.Fatalf("control advance = %+v, %v", receipt, err)
	}
}
