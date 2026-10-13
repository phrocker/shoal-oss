// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleetevents

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleetevents"
)

// openAdapters returns two adapters over one store: one that writes
// publications under the moved guard letter and one that does not. They
// stand for two replicas mid-rollout, which is the only configuration in
// which the interesting failures happen.
func openAdapters(t *testing.T) (
	*explorercoord.Runtime, *Adapter, *Adapter,
) {
	t.Helper()
	config := runtimeConfig(t.TempDir())
	runtime, err := explorercoord.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runtime.Close() })
	moved, err := New(runtime, config.Domain)
	if err != nil {
		t.Fatal(err)
	}
	moved.MovePublicationGuard()
	shared, err := New(runtime, config.Domain)
	if err != nil {
		t.Fatal(err)
	}
	return runtime, moved, shared
}

// TestAnApprovalCannotOccupyAPublicationEntityOnceTheGuardMoves is #681.
//
// A lifecycle event's ID is derived from fields that are all on the
// committed record, so any party that can read the record can compute the
// event's guard entity. While the event is unpublished, that party can
// request an approval at the same hex string — approvals and publications
// shared guard letter 'P' — and the publication is then refused for good.
// The event is suppressed, and since reconcileActionTransitions returns on
// the first error, every later transition of that action fails to publish
// too: the action's outbox wedges.
//
// Both halves are asserted. The suppression is pinned as the behaviour being
// fixed rather than described in prose, because a test that only shows the
// fix working cannot tell a fix from a fixture that never reproduced the
// defect.
func TestAnApprovalCannotOccupyAPublicationEntityOnceTheGuardMoves(t *testing.T) {
	for _, probe := range []struct {
		name        string
		moved       bool
		wantRefused bool
	}{
		{"sharing the letter, the approval suppresses the event",
			false, true},
		{"with its own letter, the publication is unaffected",
			true, false},
	} {
		t.Run(probe.name, func(t *testing.T) {
			ctx := context.Background()
			config := runtimeConfig(t.TempDir())
			runtime, err := explorercoord.Open(config)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()
			adapter, err := New(runtime, config.Domain)
			if err != nil {
				t.Fatal(err)
			}
			if probe.moved {
				adapter.MovePublicationGuard()
			}

			now := time.Now().UTC()
			request := fleetevents.PublishRequest{
				Token: []byte("lifecycle"), RetryUntil: now.Add(time.Hour),
				Event: testEvent(1),
			}
			// What the attacker computes and occupies: the entity the
			// publication would have taken under the shared letter.
			squat(t, runtime, config.Domain, publicationEntity(
				publicationEventID(request), publicationKind))

			published, err := adapter.Append(ctx, request, now)
			if probe.wantRefused {
				assertRefused(t, "the publication", err)
				return
			}
			if err != nil {
				t.Fatalf("the publication was still refused: %v", err)
			}
			if published.Sequence != 1 {
				t.Fatalf("sequence = %d, want the event published",
					published.Sequence)
			}
			// And the occupied head is untouched: the move separates the
			// two stores rather than letting one take the other's head.
			head, _, err := runtime.ReadEntity(ctx, publicationEntity(
				publicationEventID(request), publicationKind))
			if err != nil || head == nil ||
				string(head.LogicalPolicyID) != "fleet/approval/v1" {
				t.Fatalf("the approval's head after the publication = "+
					"%+v, %v; the publication must not have adopted it",
					head, err)
			}
		})
	}
}

// TestAReplicaThatDoesNotWriteTheMovedLetterStillPrunesIt is the half that
// has to ship before anything writes the new letter, and the reason this is
// two phases rather than one.
//
// buildPruneTargetLocked looks for a live owning guard on the publication's
// own intent at the entity the prune names. A replica that assumed the
// shared letter for a record written under the moved one would find none and
// fail with ErrInternal — and Append tolerates only ErrConflict from the
// sweep, so that replica's every Append would fail once such a publication
// expired. The kind is therefore read off the record, not off the replica.
func TestAReplicaThatDoesNotWriteTheMovedLetterStillPrunesIt(t *testing.T) {
	ctx := context.Background()
	runtime, moved, shared := openAdapters(t)

	now := time.Now().UTC().Truncate(time.Second)
	first := fleetevents.PublishRequest{
		Token: []byte("moved"), RetryUntil: now.Add(time.Hour),
		Event: testEvent(1),
	}
	first.Event.OccurredAt = now
	if _, err := moved.Append(ctx, first, now); err != nil {
		t.Fatalf("the moved-letter publication: %v", err)
	}

	// The other replica sweeps it after it expires. This is the call that
	// fails if the prune assumes a letter instead of reading one.
	later := now.Add(2 * time.Hour)
	second := testEvent(2)
	second.OccurredAt = later
	if _, err := shared.Append(ctx, fleetevents.PublishRequest{
		Token: []byte("sweeper"), RetryUntil: later.Add(time.Hour),
		Event: second,
	}, later); err != nil {
		t.Fatalf("a replica that does not write the moved letter could not "+
			"publish once a moved-letter publication expired, which wedges "+
			"it for every later publication too: %v", err)
	}

	// The sweep actually ran, or the assertion above is vacuous.
	receipts, err := runtime.ScanCommitted(
		ctx, explorercoord.CommittedScanRequest{
			Table: Table, RowPrefix: publicationPrefix,
			Family: recordFamily, Qualifier: recordQualifier, Limit: 100,
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts.Cells) != 1 {
		t.Fatalf("publication receipts = %d, want 1: the expired "+
			"moved-letter receipt was not swept, so this test never "+
			"exercised the prune it is about", len(receipts.Cells))
	}
}

// TestAReplayAcrossTheRolloutIsStillARepeat pins the field's provenance.
//
// A repeated publication is recognized by re-marshalling the record the
// request would produce and comparing bytes with the stored one. The guard
// kind belongs to whoever wrote the publication, exactly as Audit does, so
// it is taken from the stored record — otherwise two replicas configured
// differently would each read the other's publications as a conflict, and
// the rollout itself would be the outage.
func TestAReplayAcrossTheRolloutIsStillARepeat(t *testing.T) {
	ctx := context.Background()
	_, moved, shared := openAdapters(t)
	now := time.Now().UTC().Truncate(time.Second)

	for _, probe := range []struct {
		name             string
		writer, replayer *Adapter
		token            string
	}{
		{"written sharing the letter, replayed by a moved replica",
			shared, moved, "shared-first"},
		{"written under the moved letter, replayed by a sharing replica",
			moved, shared, "moved-first"},
	} {
		t.Run(probe.name, func(t *testing.T) {
			request := fleetevents.PublishRequest{
				Token: []byte(probe.token), RetryUntil: now.Add(time.Hour),
				Event: testEvent(1),
			}
			request.Event.OccurredAt = now
			published, err := probe.writer.Append(ctx, request, now)
			if err != nil {
				t.Fatalf("the first publication: %v", err)
			}
			replayed, err := probe.replayer.Append(
				ctx, request, now.Add(time.Minute))
			if err != nil {
				t.Fatalf("the replay was answered with an error rather than "+
					"the committed outcome: %v", err)
			}
			if !replayed.Repeated {
				t.Fatal("the replay was taken for a new publication")
			}
			if replayed.Sequence != published.Sequence {
				t.Fatalf("replayed sequence = %d, want %d",
					replayed.Sequence, published.Sequence)
			}
		})
	}
}

// TestAPublicationReceiptNamingAnUnknownGuardKindIsRefused: a label this
// build does not know came from a later one. Defaulting to the shared letter
// for it would retire a guard the publication does not own, so the sweep
// stops instead.
func TestAPublicationReceiptNamingAnUnknownGuardKindIsRefused(t *testing.T) {
	for _, probe := range []struct {
		name  string
		label string
		want  byte
	}{
		{"no label is the letter approvals share", "", publicationKind},
		{"the moved label", movedPublicationKindLabel, movedPublicationKind},
	} {
		t.Run(probe.name, func(t *testing.T) {
			entity, err := prunePublicationEntity(eventRecord{
				Event:     fleetevents.Event{EventID: []byte("event")},
				GuardKind: probe.label,
			})
			if err != nil {
				t.Fatal(err)
			}
			if entity.Kind != probe.want {
				t.Fatalf("kind = %q, want %q", entity.Kind, probe.want)
			}
		})
	}
	if _, err := prunePublicationEntity(eventRecord{
		Event: fleetevents.Event{EventID: []byte("event")}, GuardKind: "Z",
	}); err == nil {
		t.Fatal("an unknown guard kind was resolved to an entity")
	}
}

// TestARecordWrittenWithTheMoveOffIsUnchangedOnTheWire is the compatibility
// claim the two-phase rollout rests on, asserted on the bytes rather than
// argued in a comment.
//
// The replay branch recognizes a repeated publication by re-marshalling the
// record the request would produce and comparing bytes. If a build that
// contains this change wrote the field unconditionally, every replay of its
// publications by a replica from before the change would compare unequal and
// be answered as a conflict — so the upgrade itself, before anyone enabled
// anything, would be the outage.
//
// The field is therefore absent from the wire while the move is off, which
// also makes rolling the build back safe.
func TestARecordWrittenWithTheMoveOffIsUnchangedOnTheWire(t *testing.T) {
	record := eventRecord{
		Event:         testEvent(1),
		PublicationID: []byte("publication"),
		RetryUntil:    time.Now().UTC().Truncate(time.Second),
	}
	shared, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(shared, []byte("GuardKind")) {
		t.Fatalf("a record written with the move off names its guard kind, "+
			"so a replica from before this change reads every replay of it "+
			"as a conflict: %s", shared)
	}

	// And the adapter is what decides to leave it empty, so pin that here
	// rather than relying on a replay test elsewhere to notice.
	sharing, moving := &Adapter{}, &Adapter{}
	moving.MovePublicationGuard()
	if sharing.publicationGuardKindLabel() != "" {
		t.Fatalf("an adapter with the move off labels its records %q",
			sharing.publicationGuardKindLabel())
	}
	if moving.publicationGuardKindLabel() != movedPublicationKindLabel {
		t.Fatalf("an adapter with the move on labels its records %q",
			moving.publicationGuardKindLabel())
	}

	record.GuardKind = movedPublicationKindLabel
	moved, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(moved, []byte(`"GuardKind":"B"`)) {
		t.Fatalf("a record written under the moved letter does not say so, "+
			"so nothing can prune it: %s", moved)
	}

	// And it survives the round trip, which is what the prune path reads.
	var decoded eventRecord
	if err := json.Unmarshal(moved, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.GuardKind != movedPublicationKindLabel {
		t.Fatalf("GuardKind = %q after a round trip, want %q",
			decoded.GuardKind, movedPublicationKindLabel)
	}
}
