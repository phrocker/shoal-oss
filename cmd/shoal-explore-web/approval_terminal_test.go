// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// The rollout switch (#655), against the composition openService builds.

// newDisabledHarness is newEventHarness without -approval-terminal-states,
// as the shipped default runs.
func newDisabledHarness(t *testing.T) (*approvalHarness, *eventProbe) {
	t.Helper()
	probe := newEventProbe()
	h := &approvalHarness{t: t, root: t.TempDir(), withoutTerminalStates: true}
	h.clock.Store(time.Now().UTC().Add(time.Minute).Truncate(time.Second).UnixNano())
	authority, err := auth.NewAuthorityWithClock(h.now)
	if err != nil {
		t.Fatal(err)
	}
	h.authority = authority
	h.reader = &mutableFleetGeneration{}
	h.reader.value.Store(workspacePolicyGeneration)
	h.wrapEvents = func(inner fleet.ApprovalEventPublisher) fleet.ApprovalEventPublisher {
		probe.inner = inner
		return probedApprovalEvents{
			ApprovalEventPublisher: inner, probe: probe, resolver: authority.Resolver(),
		}
	}
	h.open()
	t.Cleanup(h.close)
	h.register("gateway", "approval-registration", 0, true, "")
	return h, probe
}

// TestNothingWritesATerminalStateWhileDisabled: with the switch off, which a
// replica must be while any replica runs a build before #655, no path writes
// a withdrawn or stranded record, and nothing writes an outbox row such a
// build cannot read:
//   - Withdraw refuses deliberately, before reading anything, the same for an
//     ID that exists and one that does not;
//   - Status and Decide report a moved target, a moved policy and a closed
//     window exactly as before #655, and write nothing new;
//   - the sweep refuses.
//
// Every request is then checked: never withdrawn or stranded, and every
// outbox row a build before #655 reads (requester or approver observer).
func TestNothingWritesATerminalStateWhileDisabled(t *testing.T) {
	h, probe := newDisabledHarness(t)
	if _, err := h.register("relay", "relay-registration", 0, true, ""); err != nil {
		t.Fatal(err)
	}
	moved := h.held("disabled-moved")
	movedApproved := h.held("disabled-moved-approved")
	policy := h.held("disabled-policy")
	lapsed := h.held("disabled-lapsed")
	withdrawn := h.held("disabled-withdraw")
	movedReceipt := h.requestFor("relay", moved)
	approvedReceipt := h.requestFor("relay", movedApproved)
	h.mustRequest(requester, policy, fleet.ApprovalPending)
	h.mustRequest(requester, lapsed, fleet.ApprovalPending)
	h.mustRequest(requester, withdrawn, fleet.ApprovalPending)
	if _, err := h.decide(approver, approvedReceipt, fleet.ApprovalVerdictApprove); err != nil {
		t.Fatal(err)
	}

	// Withdraw: a deliberate 409, the same for an ID that exists and one
	// that does not, and nothing written.
	_, existing := h.withdraw(requester, withdrawn.id)
	_, missing := h.withdraw(requester, []byte("disabled-missing"))
	if !errors.Is(existing, fleet.ErrApprovalTerminalStatesDisabled) ||
		!shoal.IsErrorCode(existing, shoal.ErrorConflict) ||
		existing.Error() != missing.Error() {
		t.Fatalf("withdraw while disabled = %v, missing = %v", existing, missing)
	}

	// The relay's generation moves, and the policy too.
	if _, err := h.register("relay", "relay-regenerate", 1, true, ""); err != nil {
		t.Fatal(err)
	}
	for _, request := range []heldRequest{moved, movedApproved} {
		for _, who := range []principal{requester, approver} {
			status := h.statusAs(who, request.id)
			if status.State != fleet.ApprovalUnresolvable ||
				status.Condition != fleet.ApprovalConditionTargetMoved {
				t.Fatalf("%s status as %s = %q/%q stored %q", request.id, who.subject,
					status.State, status.Condition, status.Approval.State)
			}
		}
	}
	// Decide on the moved target decides, as before #655: approval of a
	// request whose target moved is a statement the requester can never use.
	if _, err := h.decide(approver, movedReceipt, fleet.ApprovalVerdictApprove); err != nil {
		t.Fatalf("decide on a moved target while disabled = %v", err)
	}
	h.reader.value.Store(workspacePolicyGeneration + 1)
	newer := approver
	newer.generation = workspacePolicyGeneration + 1
	policyStatus := h.statusAs(newer, policy.id)
	if policyStatus.State != fleet.ApprovalUnresolvable ||
		policyStatus.Condition != fleet.ApprovalConditionPolicyMoved {
		t.Fatalf("policy-moved status = %q/%q", policyStatus.State, policyStatus.Condition)
	}
	if _, err := h.decide(newer, fleet.ApprovalReceipt{
		ID: policy.id, RequestDigest: policyStatus.Approval.RequestDigest,
		PolicyGeneration: policyStatus.Approval.PolicyGeneration,
	}, fleet.ApprovalVerdictApprove); !errors.Is(err, fleet.ErrApprovalSuperseded) ||
		errors.Is(err, fleet.ErrApprovalStranded) {
		t.Fatalf("decide under a moved policy while disabled = %v", err)
	}
	h.reader.value.Store(workspacePolicyGeneration)

	// The window closes: Status reports it and writes nothing.
	h.advance(fleet.DefaultApprovalWindow + time.Second)
	if status := h.status(lapsed.id); status.State != fleet.ApprovalExpired ||
		status.Condition != fleet.ApprovalConditionWindowClosed ||
		status.Approval.State != fleet.ApprovalPending {
		t.Fatalf("lapsed status = %q/%q stored %q", status.State,
			status.Condition, status.Approval.State)
	}
	if _, err := sweepApprovals(context.Background(), h.opened.approvalService); !errors.Is(
		err, fleet.ErrApprovalTerminalStatesDisabled) {
		t.Fatalf("sweep while disabled = %v", err)
	}

	for _, request := range []heldRequest{moved, movedApproved, policy, lapsed, withdrawn} {
		rows, err := h.opened.approvalOutbox.ApprovalTransitions(context.Background(), request.id)
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			state := row.Transition.Record.State
			observer := row.Transition.Publisher.Observer
			if state == fleet.ApprovalWithdrawn || state == fleet.ApprovalStranded ||
				observer == fleet.ApprovalObservedBySweeper {
				t.Fatalf("%s: a disabled host wrote a %s row observed by %q",
					request.id, state, observer)
			}
		}
	}
	for _, published := range probe.publications() {
		if published.kind == fleet.ApprovalEventWithdrawn ||
			published.kind == fleet.ApprovalEventStranded {
			t.Fatalf("a disabled host published %s", published.kind)
		}
	}
}

// TestTheSweepRequiresTheTerminalStates: the host refuses to start a sweep
// that the switch does not allow, before it opens anything.
func TestTheSweepRequiresTheTerminalStates(t *testing.T) {
	err := run(context.Background(), []string{
		"-data", t.TempDir(), "-listen", "127.0.0.1:0", "-dev-auth",
		"-approval-sweep-interval", "1m",
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "requires -approval-terminal-states") {
		t.Fatalf("a sweep without the terminal states = %v", err)
	}
}

// TestTheSweepCoversAStoreLargerThanOnePass: a pass whose budget covers part
// of the key space resumes where the last stopped, so every record is
// reached over several passes, and the cycle then wraps to the beginning.
func TestTheSweepCoversAStoreLargerThanOnePass(t *testing.T) {
	h := newApprovalHarness(t)
	const requests = 5
	for i := 0; i < requests; i++ {
		h.mustRequest(requester, h.held(fmt.Sprintf("sweep-large-%d", i)),
			fleet.ApprovalPending)
	}
	h.advance(fleet.DefaultApprovalWindow + time.Second)
	sweeper := newApprovalSweeper(h.opened.approvalService)
	sweeper.pageLimit, sweeper.maxPages = 2, 1
	expired, passes := 0, 0
	for ; passes < 10 && expired < requests; passes++ {
		total, err := sweeper.pass(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if total.Examined > 2 {
			t.Fatalf("a pass exceeded its budget: %+v", total)
		}
		expired += total.Expired
	}
	if expired != requests || passes != 3 || len(sweeper.cursor) != 0 {
		t.Fatalf("expired %d in %d passes, cursor %q", expired, passes, sweeper.cursor)
	}
	for i := 0; i < requests; i++ {
		id := []byte(fmt.Sprintf("sweep-large-%d", i))
		if stored := h.status(id).Approval; stored.State != fleet.ApprovalExpired {
			t.Fatalf("%s = %q", id, stored.State)
		}
	}
	// The cycle wrapped: the next pass starts at the beginning again.
	total, err := sweeper.pass(context.Background())
	if err != nil || total.Examined != 2 || len(sweeper.cursor) == 0 {
		t.Fatalf("pass after the wrap = %+v, %v", total, err)
	}
}

// TestTheSweepServesErrorCountersOnly: the health surface gains the sweep's
// two error counters and nothing that tracks tenant workload.
func TestTheSweepServesErrorCountersOnly(t *testing.T) {
	if approvalSweepMetrics(nil) != nil || combineMetrics(nil, nil) != nil {
		t.Fatal("a metrics writer with nothing to serve")
	}
	h := newApprovalHarness(t)
	var b strings.Builder
	combineMetrics(nil, approvalSweepMetrics(h.opened.approvalService))(&b)
	for _, line := range strings.Split(strings.TrimSpace(b.String()), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		name := strings.Fields(line)[0]
		if name != "shoal_fleet_approval_sweep_undecodable_total" &&
			name != "shoal_fleet_approval_sweep_failed_total" {
			t.Fatalf("unexpected series %q", name)
		}
	}
	if !strings.Contains(b.String(), "shoal_fleet_approval_sweep_undecodable_total 0") {
		t.Fatalf("metrics = %s", b.String())
	}
}
