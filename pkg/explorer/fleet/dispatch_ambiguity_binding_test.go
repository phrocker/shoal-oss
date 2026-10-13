// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// ambiguityReported puts one lost-fence report on a record, with the two
// fields #746 is about filled with content a worker could have derived from
// the evidence it read.
func ambiguityReported(record ActionRecord) ActionRecord {
	record.AmbiguityReports = []AmbiguityReport{{
		ClaimFence: 1, Outcome: AmbiguityEffectObserved,
		Target:    "mail.example/patient-4471-hiv-positive",
		Reference: "msg-derived-from-the-labelled-document",
		Subject:   "worker-subject", Actor: "worker-actor",
		ReportedAt: record.UpdatedAt,
	}}
	return record
}

// TestAmbiguityReportsAreNotServedPastTheirEvidence is #746, the last of the
// executor-written fields served past withheld evidence.
//
// Target and Reference are worker-supplied text describing an exchange with
// a system Shoal does not control — the bound's own comment says the bytes
// are target-controlled — and nothing binds them to the evidence the action
// read. readableRecords did not touch AmbiguityReports at all, so both went
// unchanged to a reader whose references were withheld, exactly as Output
// did before #696 and ErrorCode did before #740.
//
// The probe that matters carries no Output. #727's gate runs only on a
// record that has one, so a failed action — which is the only kind that
// carries an ambiguity report — went straight past it. A test built on a
// record with both fields would have passed before this change.
func TestAmbiguityReportsAreNotServedPastTheirEvidence(t *testing.T) {
	for _, probe := range []struct {
		name         string
		enqueuer     bool
		subject      string
		actor        string
		wantAbsent   bool
		wantWithheld bool
	}{
		{
			name: "the action's own principal", enqueuer: true,
			wantWithheld: true,
		},
		{
			// It wrote the report, and its own grants no longer cover the
			// evidence, so it takes the own-principal answer.
			name: "the claimant", subject: "worker-subject",
			actor: "worker-actor", wantWithheld: true,
		},
		{
			// The TeamActions audience. A reference is the handle an
			// operator takes to the other system; a third party gets the
			// answer an action that was never enqueued gets.
			name: "a third party", subject: "stranger",
			actor: "stranger-actor", wantAbsent: true,
		},
	} {
		t.Run(probe.name, func(t *testing.T) {
			fixture := labelledFixture(t)
			labelled, _ := completedByAnotherPrincipal(t, fixture)
			withSnapshot(fixture, labelled)
			stored := ambiguityReported(cloneActionRecord(
				fixture.dispatchStore.records[string(fixture.queued.ID)]))
			// No Output: this is the gap #727 left, and with an Output
			// present the probe would pass before this change.
			stored.Output = nil
			stored.OutputWithheld = false
			stored.State = DispatchFailed
			stored.ErrorCode = "upstream_unreachable"

			reader := fixture.enqueuer
			if !probe.enqueuer {
				reader = bindDecision(t, fixture.authority,
					planeReader(t, fixture, probe.subject, probe.actor))
			}
			readable, err := fixture.service.readableRecord(reader, stored)

			if probe.wantAbsent {
				if err == nil {
					t.Fatalf("a third party was served the reports %+v",
						readable.AmbiguityReports)
				}
				if !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
					t.Fatalf("err = %v, want not-found: any other refusal "+
						"says the action exists (#398)", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v, want the record served to its own "+
					"principal", err)
			}
			if len(readable.AmbiguityReports) != 0 {
				t.Fatalf("the reports were served past withheld evidence: "+
					"%+v", readable.AmbiguityReports)
			}
			if !readable.AmbiguityReportsWithheld {
				t.Fatal("the reports were dropped without saying so; a " +
					"record with none is ordinary, so a principal entitled " +
					"to a reference would conclude there is nothing to " +
					"reconcile")
			}
			// The marker must describe a real redaction: the evidence it
			// was withheld alongside has to have been withheld too.
			if len(readable.Evidence) != 0 {
				t.Fatalf("evidence = %+v, want it withheld; the probe is "+
					"vacuous", readable.Evidence)
			}
			if err := readable.Validate(); err != nil {
				t.Fatalf("the redacted record is not a producible state: %v",
					err)
			}
		})
	}
}

// TestTheWithheldReportsMarkerCannotClaimAnUnredactedRecord keeps the marker
// honest, as #727's does for the Output: a record cannot assert a redaction
// that did not happen.
func TestTheWithheldReportsMarkerCannotClaimAnUnredactedRecord(t *testing.T) {
	fixture := labelledFixture(t)
	completedByAnotherPrincipal(t, fixture)
	base := ambiguityReported(cloneActionRecord(
		fixture.dispatchStore.records[string(fixture.queued.ID)]))
	base.Evidence = nil
	base.EvidenceSnapshotID = ""
	base.EvidenceSnapshotAsOf = time.Time{}
	// Failed, because that is the only kind of record that carries an
	// ambiguity report: a worker has to lose its claim mid-effect for one
	// to exist at all.
	base.State = DispatchFailed
	base.Output = nil
	base.OutputWithheld = false
	base.ErrorCode = "upstream_unreachable"

	if err := base.Validate(); err != nil {
		t.Fatalf("the unredacted record is invalid, so this test compares "+
			"against nothing: %v", err)
	}
	both := cloneActionRecord(base)
	both.AmbiguityReportsWithheld = true
	if err := both.Validate(); err == nil {
		t.Fatal("a record claiming its reports were withheld while " +
			"carrying them was accepted")
	}
	redacted := cloneActionRecord(base)
	redacted.AmbiguityReports = nil
	redacted.AmbiguityReportsWithheld = true
	if err := redacted.Validate(); err != nil {
		t.Fatalf("the redacted shape is not producible: %v", err)
	}
}
