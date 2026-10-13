// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Invoke's materialization (#655) at the unit level: the branches the hosted
// composition cannot reach on demand. The behaviour end to end is in
// cmd/shoal-explore-web/approval_invoke_test.go.

// TestInvokeAnswersEachApprovalState: Invoke has only an action to answer
// with, so every state but enqueued is an answer naming that state, and
// none of them is a record to run.
func TestInvokeAnswersEachApprovalState(t *testing.T) {
	action := approvalTestRecord(t)
	if got, err := invokeAnswer(ApprovalReceipt{
		State: ApprovalEnqueued, Action: action,
	}); err != nil || string(got.ID) != string(action.ID) {
		t.Fatalf("enqueued = %+v, %v", got, err)
	}
	for state, sentinel := range map[ApprovalState]error{
		ApprovalRefused:   ErrApprovalRefused,
		ApprovalExpired:   ErrApprovalExpired,
		ApprovalWithdrawn: ErrApprovalWithdrawn,
		ApprovalStranded:  ErrApprovalStranded,
	} {
		got, err := invokeAnswer(ApprovalReceipt{State: state, Action: action})
		if !errors.Is(err, sentinel) || !shoal.IsErrorCode(err, shoal.ErrorConflict) ||
			len(got.ID) != 0 {
			t.Fatalf("%s = %+v, %v", state, got, err)
		}
	}
	for _, state := range []ApprovalState{ApprovalPending, ApprovalApproved} {
		if got, err := invokeAnswer(ApprovalReceipt{
			State: state, Action: action,
		}); !shoal.IsErrorCode(err, shoal.ErrorInternal) || len(got.ID) != 0 {
			t.Fatalf("%s = %+v, %v", state, got, err)
		}
	}
}

// TestInvokesMaterializationNeverHolds: with nothing held, and with a
// request held and undecided, the Invoke mode of reach answers approval
// required and writes nothing. A stranded request is answered as stranded.
func TestInvokesMaterializationNeverHolds(t *testing.T) {
	pending := validApprovalRecord(t)
	now := pending.RequestedAt.Add(time.Minute)
	store := &memoryApprovalStore{records: map[string]ApprovalRecord{}}
	service := &ApprovalService{
		dispatch: &DispatchService{
			store: newMemoryDispatchStore(), clock: func() time.Time { return now },
		},
		store: store, recorder: unguardedApprovalRecorder{},
		generations: fixedGenerations{generation: 3}, terminal: true,
	}
	decision := approvalBranchDecision(t, pending.RequestedAt, auth.OperationDispatch)
	ready := func() error {
		t.Fatal("ready was asked of a request that is not approved")
		return nil
	}
	mode := approvalAttempt{invoke: true, ready: ready}
	if _, err := service.reach(
		context.Background(), decision, pending.Request, now, mode,
	); !errors.Is(err, ErrApprovalRequired) ||
		!strings.Contains(err.Error(), "/api/v1/fleet/approvals/request") {
		t.Fatalf("invoke with nothing held = %v", err)
	}
	if len(store.records) != 0 {
		t.Fatalf("invoke held a request: %d records", len(store.records))
	}

	store.records[string(pending.ID)] = CloneApprovalRecord(pending)
	if _, err := service.reach(
		context.Background(), decision, pending.Request, now, mode,
	); !errors.Is(err, ErrApprovalRequired) ||
		!strings.Contains(err.Error(), "awaiting a decision") {
		t.Fatalf("invoke of a pending request = %v", err)
	}
	if stored := store.records[string(pending.ID)]; stored.Version != 1 ||
		stored.State != ApprovalPending {
		t.Fatalf("invoke moved a pending request to %q v%d", stored.State, stored.Version)
	}

	stranded := strandedRecord(pending, ApprovalConditionTargetMoved, now)
	store.records[string(pending.ID)] = stranded
	receipt, err := service.reach(
		context.Background(), decision, pending.Request, now, mode)
	if err != nil || receipt.State != ApprovalStranded {
		t.Fatalf("invoke of a stranded request = %+v, %v", receipt, err)
	}
	if _, err := invokeAnswer(receipt); !errors.Is(err, ErrApprovalStranded) {
		t.Fatalf("stranded answer = %v", err)
	}
}

// TestInvokesMaterializationAsksReadyBeforeWriting: an approved request
// whose work cannot run here is refused before the approved → enqueued
// commit, so the approval is left exactly as it was.
func TestInvokesMaterializationAsksReadyBeforeWriting(t *testing.T) {
	approved := decided(validApprovalRecord(t), ApprovalApproved, ApprovalVerdictApprove)
	now := approved.DecidedAt.Add(time.Minute)
	store := &memoryApprovalStore{records: map[string]ApprovalRecord{
		string(approved.ID): CloneApprovalRecord(approved),
	}}
	service := &ApprovalService{
		dispatch: &DispatchService{
			store: newMemoryDispatchStore(), clock: func() time.Time { return now },
		},
		store: store, recorder: unguardedApprovalRecorder{},
		generations: fixedGenerations{generation: approved.PolicyGeneration},
	}
	refusal := shoal.NewError(shoal.ErrorUnavailable, "not runnable here")
	asked := 0
	_, err := service.reach(
		context.Background(),
		approvalBranchDecision(t, approved.RequestedAt, auth.OperationDispatch),
		approved.Request, now,
		approvalAttempt{invoke: true, ready: func() error {
			asked++
			return refusal
		}})
	if !errors.Is(err, refusal) || asked != 1 {
		t.Fatalf("approved, not runnable = %v (asked %d)", err, asked)
	}
	if stored := store.records[string(approved.ID)]; stored.Version != approved.Version ||
		stored.State != ApprovalApproved {
		t.Fatalf("a refused materialization wrote %q v%d", stored.State, stored.Version)
	}
}

// TestTheApprovalMaterializerIsInstalledOnce: one dispatch service, one
// approval key space. Without one, Invoke's answer is unchanged.
func TestTheApprovalMaterializerIsInstalledOnce(t *testing.T) {
	dispatch := &DispatchService{}
	required := approvalRequired()
	if _, err := dispatch.materializeApproved(
		context.Background(), InvokeRequest{}, required,
	); err != required {
		t.Fatalf("without a materializer = %v", err)
	}
	if err := dispatch.installApprovalMaterializer(nil); err == nil {
		t.Fatal("a nil materializer was installed")
	}
	first := &ApprovalService{dispatch: dispatch}
	if err := dispatch.installApprovalMaterializer(first); err != nil {
		t.Fatal(err)
	}
	if err := dispatch.installApprovalMaterializer(
		&ApprovalService{dispatch: dispatch},
	); err == nil {
		t.Fatal("a second approval service was installed")
	}
	if hook := dispatch.approvals.Load(); hook == nil ||
		hook.approvalMaterializer != approvalMaterializer(first) {
		t.Fatal("the second install replaced the first")
	}
}
