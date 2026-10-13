// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestAMisshapenErrorCodeIsAdjudicated is #740 phase 1.
//
// ErrorCode is executor-written and was bounded only by length and trimming,
// so up to a kilobyte of free text rode it into the record and out through
// every surface that returns one — including to a reader whose evidence
// references were withheld. It is reachable where Output is not: a failed
// action carries a code and no Output, so #696's withholding never fires
// for it.
//
// What this phase buys is MaxActionErrorBytes reduced to
// MaxReportedActionErrorCodeBytes — a kilobyte down to sixty-four — and the
// readable-token probe below is here to keep that honest. A shape rule
// constrains form and not content, so the channel is narrowed, not closed;
// closing it needs the declared vocabulary of phase 2.
//
// Adjudicated, never refused. By the time a code is seen the executor has
// done whatever the action describes, and refusing after its effect would
// strand the work and lose the outcome.
func TestAMisshapenErrorCodeIsAdjudicated(t *testing.T) {
	for _, probe := range []struct {
		name  string
		value string
		want  string
		// wantService is whether the service takes authorship. It is not
		// the same question as whether the text changed: an executor that
		// reports the service's own code verbatim changes nothing and must
		// still lose authorship of it.
		wantService bool
	}{
		{"a token passes", "upstream_unreachable",
			"upstream_unreachable", false},
		{"digits and underscores pass", "target_rejected_502",
			"target_rejected_502", false},
		{"no error is left alone", "", "", false},

		{"prose", "the target said: no", "invalid_executor_error", true},
		{"a sentence with no punctuation",
			"the target refused the request", "invalid_executor_error", true},
		{"an embedded quotation", `it said "no"`, "invalid_executor_error", true},
		{"newlines", "refused\nby upstream", "invalid_executor_error", true},
		{"uppercase", "UpstreamRefused", "invalid_executor_error", true},
		{"a leading digit", "502_rejected", "invalid_executor_error", true},
		{"over length", strings.Repeat("a", MaxReportedActionErrorCodeBytes+1),
			"invalid_executor_error", true},

		// The service's own codes stay the service's. The remote route
		// refuses these at its boundary (#508); this is the in-process
		// route, which cannot tell a worker's code from one the service
		// has just assigned because both sit in the same field.
		{"a service code reported verbatim keeps the text and loses the " +
			"authorship", "invalid_executor_error",
			"invalid_executor_error", true},
		{"another service code", "invalid_executor_output",
			"invalid_executor_error", true},

		// Phase 1 is a narrowing, not a closure, and this probe is what
		// says so. It must keep passing: the day it fails, either the
		// grammar has started rejecting valid tokens or phase 2 has
		// landed and this file is the wrong place to assert it.
		{"a readable token passes, which is why this is not the answer",
			"patient_4471_hiv_positive", "patient_4471_hiv_positive", false},
	} {
		t.Run(probe.name, func(t *testing.T) {
			// The default service: the shape rule, no declared vocabulary.
			// The closed rule has its own test.
			adjudicated := (&DispatchService{}).
				reportedErrorCodeIsAdjudicated(Action{}, probe.value)
			if adjudicated != probe.wantService {
				t.Fatalf("service authorship = %v for %q, want %v; this is "+
					"what sets ErrorCodeOrigin, and #508 turns on the "+
					"record saying which of the two wrote the code",
					adjudicated, probe.value, probe.wantService)
			}
			// What the record ends up carrying, stated here as the probe's
			// want so the table reads as the vocabulary it decides. The
			// call site assigns the literal; this is the same rule.
			got := probe.value
			if adjudicated {
				got = "invalid_executor_error"
			}
			if got != probe.want {
				t.Fatalf("code = %q, want %q", got, probe.want)
			}
		})
	}
}

// TestTheRemoteRouteStillRefusesAReservedCode pins the half of #508 that did
// not move. The shape rule left the boundary for adjudication, because by
// completion the work is done; a reserved code is different. It is refused
// where the caller is still known to be the caller, before the claim
// resolves, so an executor cannot write the service's own adjudication and
// have the record say the service chose it.
func TestTheRemoteRouteStillRefusesAReservedCode(t *testing.T) {
	if err := validateReportedActionErrorCode(
		"invalid_executor_error"); err == nil {
		t.Fatal("the remote route accepted a code the service assigns")
	}
	// And the grammar is no longer refused here, or the adjudication above
	// would be unreachable from this route.
	if err := validateReportedActionErrorCode(
		"the target said: no"); err != nil {
		t.Fatalf("the remote route refused a misshapen code (%v) instead of "+
			"letting applyExecutionResult adjudicate it; refusing it strands "+
			"work the executor has already performed", err)
	}
	if err := validateReportedActionErrorCode(""); err != nil {
		t.Fatalf("the remote route refused an empty code: %v", err)
	}
}

// prosingExecutor is an in-process executor that fails the way a library
// wrapping an upstream client does: by handing back whatever the upstream
// said as the code.
//
// It returns an error alongside, because that is what an actual failure
// looks like on this route. A result carrying a code with a nil error is a
// success with no Output, which the service adjudicates as
// invalid_executor_output before ever reaching the code — a fixture built
// that way reports the wrong adjudication and would read like a passing
// test of something else.
type prosingExecutor struct{ code string }

func (e prosingExecutor) Execute(
	context.Context, Invocation,
) (ExecutionResult, error) {
	return ExecutionResult{ErrorCode: e.code},
		shoal.NewError(shoal.ErrorInternal, "upstream refused")
}

// TestBothCompletionRoutesAdjudicateTheCodeTheyRecord is the half of #740 a
// unit test of the adjudicator cannot reach.
//
// The grammar first landed only on the remote route, at CompleteClaim's
// boundary. Invoke does not go through CompleteClaim: it calls
// applyExecutionResult directly, where the code was checked by the permissive
// validateActionErrorCode, so an in-process executor could still write a
// kilobyte of upstream prose into the record. An adjudicator with no call
// site is worth nothing, and the function-level test above passes with the
// call site deleted — this one does not.
//
// Both routes are asserted together because the thing that has to hold is
// that they agree. A record cannot carry something through one door that the
// other refuses, or the rule is a property of the door rather than the
// record. The admission surface is a third door and is covered by the
// second: AdmissionReport funnels into the same unexported completeClaim.
func TestBothCompletionRoutesAdjudicateTheCodeTheyRecord(t *testing.T) {
	const prose = "upstream said: patient 4471 is HIV positive, refusing"

	t.Run("in process, through Invoke", func(t *testing.T) {
		fixture := newExecutorClaimFixture(t)
		fixture.bindExecutor(t, prosingExecutor{code: prose})
		enqueue := dispatchEnqueue(fixture.now, "request")
		enqueue.ID = []byte("prose-action")
		enqueue.IdempotencyKey = []byte("prose-idempotency")

		// The execution error propagates on this route — this caller invoked
		// the work, so the failure is its own. What matters is that the
		// outcome was still recorded rather than the report refused.
		invoked, err := fixture.service.Invoke(fixture.enqueuer, InvokeRequest{
			Enqueue: enqueue,
			ClaimID: []byte("invoke-claim"), Lease: time.Minute,
		})
		if err == nil {
			t.Fatal("a failed execution was answered as a success")
		}
		stored, ok := fixture.dispatchStore.records[string(enqueue.ID)]
		if !ok {
			t.Fatal("the outcome was not recorded at all, so this test is " +
				"asserting nothing about what a record may carry")
		}
		if stored.State != DispatchFailed {
			t.Fatalf("state = %q, want the failure recorded", stored.State)
		}
		assertAdjudicated(t, stored, prose)
		if invoked.ErrorCode != "" {
			assertAdjudicated(t, invoked, prose)
		}
	})

	t.Run("remote, through CompleteClaim", func(t *testing.T) {
		fixture := newExecutorClaimFixture(t)
		worker := fixture.namedExecutor(t, "worker")
		record, claimErr := fixture.service.Claim(worker, ClaimRequest{
			ID: fixture.queued.ID, ExpectedVersion: fixture.queued.Version,
			ClaimID: []byte("worker-claim"), Lease: time.Minute,
			Context: dispatchContext(fixture.now, "worker-request"),
		})
		if claimErr != nil {
			t.Fatalf("the claim this test needs was refused: %v", claimErr)
		}

		returned, err := fixture.service.CompleteClaim(
			worker, CompletionRequest{
				ID: fixture.queued.ID, ExpectedVersion: record.Version,
				ClaimFence: record.ClaimFence, ClaimID: []byte("worker-claim"),
				Failed:  true,
				Result:  ExecutionResult{ErrorCode: prose},
				Context: dispatchContext(fixture.now, "worker-request"),
			})
		// This route used to refuse at the boundary. It must not: the worker
		// performed the work out of process, and a refusal here loses the
		// outcome of an effect that has already happened, leaving the record
		// to lapse back to the queue as though nothing occurred.
		if err != nil {
			t.Fatalf("the report was refused (%v) rather than adjudicated; "+
				"the work is done by the time the code is seen", err)
		}
		assertAdjudicated(t, returned, prose)
		assertAdjudicated(t,
			fixture.dispatchStore.records[string(fixture.queued.ID)], prose)
	})
}

// assertAdjudicated states the two things that have to be true of a recorded
// code: the executor's text is gone, and the record says the service chose
// what replaced it. The second is not decoration — #508 exists because a
// reader cannot otherwise tell the service's adjudication from an executor
// that reported the same string.
func assertAdjudicated(t *testing.T, record ActionRecord, prose string) {
	t.Helper()
	if record.ErrorCode == prose {
		t.Fatalf("the record carries the executor's prose verbatim: %q",
			record.ErrorCode)
	}
	if record.ErrorCode != "invalid_executor_error" {
		t.Fatalf("code = %q, want the service's adjudication",
			record.ErrorCode)
	}
	if record.ErrorCodeOrigin != ErrorCodeOriginService {
		t.Fatalf("origin = %q, want %q: the service assigned this code, and "+
			"a record that still attributes it to the executor says the "+
			"executor reported invalid_executor_error itself",
			record.ErrorCodeOrigin, ErrorCodeOriginService)
	}
}
