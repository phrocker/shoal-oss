// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// planeReader binds a decision for one named principal, so a test can read
// the same record as the action's own principal, as its claimant, and as a
// third party.
func planeReader(
	t *testing.T, fixture *executorClaimFixture, subject, actor string,
) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: shoal.ID(subject), Actor: shoal.ID(actor),
		AuthorizationDomain: []byte("domain"),
		AllowedOperations: []auth.Operation{
			auth.OperationDispatch, auth.OperationInvoke},
		PermittedSourceIDs:    [][]byte{[]byte("source")},
		PermittedPolicyIDs:    [][]byte{[]byte("policy")},
		PolicyGeneration:      1,
		AuthenticationExpires: fixture.now.Add(time.Hour),
		RequestID:             "request", CorrelationID: "correlation",
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

// TestOutputIsNotServedPastItsEvidence is #696 A1 and A2.
//
// Output is executor-supplied and checked only against the descriptor's
// OutputSchema. It carries no labels and no binding to the evidence it was
// derived from, so a reader whose references are withheld was still handed
// whatever the executor wrote from them. The built-in ask executor also puts
// EvidenceCount and EvidenceConsidered there, which compared against a
// shortened reference list say that labelled evidence existed — the count
// oracle #398 forbids. Both live inside Output, so both go with it.
//
// Three classes of reader, decided deliberately:
//
//   - The action's own principal and its claimant already know the action
//     exists and what it did, so they keep it and are told plainly that an
//     Output was withheld. OutputWithheld makes that a state Validate
//     accepts; clearing Output alone would leave a succeeded record the
//     system cannot produce, which a reader decodes as the same disclosure
//     without anyone having chosen it.
//   - Every other reader is answered as though the action does not exist.
func TestOutputIsNotServedPastItsEvidence(t *testing.T) {
	for _, probe := range []struct {
		name         string
		enqueuer     bool
		noResolver   bool
		subject      string
		actor        string
		wantAbsent   bool
		wantWithheld bool
	}{
		{
			// sameActionPrincipal compares the whole chain — subject, actor,
			// client and delegation — so the fixture's own enqueuer context
			// is used rather than one rebuilt from two of those fields.
			name: "the action's own principal", enqueuer: true,
			wantWithheld: true,
		},
		{
			// It produced the Output, and its own grants no longer cover the
			// evidence, so it takes the own-principal answer.
			name: "the claimant", subject: "worker-subject",
			actor: "worker-actor", wantWithheld: true,
		},
		{
			// The TeamActions audience this issue is about.
			name: "a third party", subject: "stranger",
			actor: "stranger-actor", wantAbsent: true,
		},
		{
			// A service that cannot identify its reader cannot tell whether
			// this is the action's own principal, so it answers as it would
			// an outsider. Failing open here would serve the Output to
			// everyone the moment a resolver broke.
			name:       "a reader the service cannot identify",
			noResolver: true, wantAbsent: true,
		},
	} {
		t.Run(probe.name, func(t *testing.T) {
			fixture := labelledFixture(t)
			labelled, _ := completedByAnotherPrincipal(t, fixture)
			withSnapshot(fixture, labelled)
			stored := cloneActionRecord(
				fixture.dispatchStore.records[string(fixture.queued.ID)])
			stored.Output = json.RawMessage(`{"answer":"derived from secret"}`)

			if probe.noResolver {
				fixture.service.resolver = nil
			}
			reader := fixture.enqueuer
			if !probe.enqueuer && !probe.noResolver {
				reader = bindDecision(t, fixture.authority,
					planeReader(t, fixture, probe.subject, probe.actor))
			}
			readable, err := fixture.service.readableRecord(reader, stored)

			if probe.wantAbsent {
				if err == nil {
					t.Fatalf("the record was served with Output %q to a "+
						"reader entitled to neither it nor the evidence it "+
						"came from", readable.Output)
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
			if !probe.wantWithheld {
				return
			}
			if !readable.OutputWithheld || len(readable.Output) != 0 {
				t.Fatalf("Output = %q, withheld = %v; want it removed and "+
					"the removal stated", readable.Output,
					readable.OutputWithheld)
			}
			// The marker must describe a real redaction, or it is noise:
			// the evidence it was derived from has to have been withheld too.
			if len(readable.Evidence) != 0 {
				t.Fatalf("evidence = %+v, want it withheld; the probe is "+
					"vacuous", readable.Evidence)
			}
			// And the record must still be one the system can produce, or a
			// reader decodes the impossibility rather than reading the
			// marker.
			if err := readable.Validate(); err != nil {
				t.Fatalf("the redacted record is not a producible state: %v",
					err)
			}
		})
	}
}

// TestAnUnreadableOutputLeavesThePage pins the page half: a record a reader
// may not be told had an Output is dropped, not served with fields missing.
// Next is the scan's cursor rather than a count, so a shorter page still
// pages correctly.
func TestAnUnreadableOutputLeavesThePage(t *testing.T) {
	fixture := labelledFixture(t)
	labelled, open := completedByAnotherPrincipal(t, fixture)
	withSnapshot(fixture, labelled)
	hidden := cloneActionRecord(
		fixture.dispatchStore.records[string(fixture.queued.ID)])
	hidden.Output = json.RawMessage(`{"answer":"derived from secret"}`)

	visible := cloneActionRecord(hidden)
	visible.ID = []byte("open-action")
	visible.Evidence = []EvidenceRef{open}

	stranger := bindDecision(t, fixture.authority,
		planeReader(t, fixture, "stranger", "stranger-actor"))
	page, err := fixture.service.readablePage(stranger, ActionPage{
		Actions: []ActionRecord{hidden, visible}, Next: []byte("cursor"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Actions) != 1 {
		t.Fatalf("page carried %d records, want 1: the record whose Output "+
			"cannot be shown must leave the page rather than be served "+
			"with it missing", len(page.Actions))
	}
	if string(page.Actions[0].ID) != "open-action" {
		t.Fatalf("the surviving record is %q, want the readable one",
			page.Actions[0].ID)
	}
	if string(page.Next) != "cursor" {
		t.Fatalf("Next = %q, want the scan's cursor untouched", page.Next)
	}
}

// TestTheWithheldMarkerCannotClaimAnUnredactedOutput pins the two checks that
// keep the marker honest.
//
// Validate refuses a marker alongside a present Output, so a record cannot
// assert a redaction that did not happen, and the store refuses the marker
// outright because it is a read-path value — a stored one would make a
// redaction durable and withhold the Output from the principals the marker
// exists to serve.
func TestTheWithheldMarkerCannotClaimAnUnredactedOutput(t *testing.T) {
	fixture := labelledFixture(t)
	completedByAnotherPrincipal(t, fixture)
	base := cloneActionRecord(
		fixture.dispatchStore.records[string(fixture.queued.ID)])
	// No evidence: this test is about the marker, and a pinned snapshot
	// would drag in citation requirements that say nothing about it.
	base.Evidence = nil
	base.EvidenceSnapshotID = ""
	base.EvidenceSnapshotAsOf = time.Time{}
	base.Output = json.RawMessage(`{"ok":true}`)

	if err := base.Validate(); err != nil {
		t.Fatalf("the unredacted record is invalid, so this test compares "+
			"against nothing: %v", err)
	}

	both := cloneActionRecord(base)
	both.OutputWithheld = true
	if err := both.Validate(); err == nil {
		t.Fatal("a record claiming its Output was withheld while carrying " +
			"one was accepted")
	}

	// And the redacted shape itself stays valid, or the read path could not
	// produce it.
	redacted := cloneActionRecord(base)
	redacted.Output = nil
	redacted.OutputWithheld = true
	if err := redacted.Validate(); err != nil {
		t.Fatalf("the redacted shape is not producible: %v", err)
	}
}
