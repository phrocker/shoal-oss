// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleetevents

import (
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestWithholdingAReferenceWithholdsItsBinding is #705's delivery half.
//
// A binding names the kind, document and revision of every registration its
// reference's members were recorded over. Carrying it past the reference it
// belonged to hands over exactly what withholding the reference refused
// (#398), and leaves the group misaligned so the delivered event fails if it
// is ever read again.
//
// readableEvidenceGroup refiltered references and visibility and left the
// binding group whole, which is what this pins.
func TestWithholdingAReferenceWithholdsItsBinding(t *testing.T) {
	open := interaction.EvidenceReference{
		Kind: interaction.EvidenceGraph, NodeIDs: []shoal.ID{"node-open"},
	}
	secret := interaction.EvidenceReference{
		Kind: interaction.EvidenceGraph, NodeIDs: []shoal.ID{"node-secret"},
	}
	references := []interaction.EvidenceReference{open, secret}
	visibility := [][]string{nil, {"secret"}}
	binding := [][]byte{[]byte("BIND-OPEN"), []byte("BIND-SECRET")}
	labelled := []labelledReference{
		{reference: open, binding: binding[0]},
		{reference: secret, visibility: visibility[1], binding: binding[1]},
	}

	kept, keptVisibility, keptBinding, withheld, err := readableEvidenceGroup(
		references, visibility, binding, labelled, []bool{true, false})
	if err != nil {
		t.Fatal(err)
	}
	if !withheld || len(kept) != 1 {
		t.Fatalf("kept %d references, withheld = %v; want one kept",
			len(kept), withheld)
	}
	if len(keptBinding) != len(kept) {
		t.Fatalf("binding group has %d entries for %d references: a "+
			"misaligned group holds a reference to another's registrations "+
			"and fails every later read", len(keptBinding), len(kept))
	}
	if string(keptBinding[0]) != "BIND-OPEN" {
		t.Fatalf("kept binding = %q, want the surviving reference's",
			keptBinding[0])
	}
	for _, value := range keptBinding {
		if string(value) == "BIND-SECRET" {
			t.Fatal("the withheld reference's binding was delivered; it " +
				"names the registrations its members were recorded over")
		}
	}
	// Visibility is filtered the same way, so a regression in one is not
	// masked by the other still being right.
	if len(keptVisibility) != 0 {
		t.Fatalf("visibility = %v, want nil once the only labelled "+
			"reference is gone", keptVisibility)
	}
}

// TestADeliveredEventCanBeReadAgain pins the shape, not just the contents: a
// group that survives delivery misaligned fails the alignment check the next
// time it is read, which turns a withheld reference into an internal error
// for every later consumer of that page.
func TestADeliveredEventCanBeReadAgain(t *testing.T) {
	reference := interaction.EvidenceReference{
		AnchorID: "anchor", Kind: interaction.EvidenceGraph,
		NodeIDs: []shoal.ID{"node"},
	}
	delivered := Event{
		EventID: []byte("event"), Kind: "action.completed",
		ProducerID: []byte("producer"), ProducerGeneration: 1,
		ActionID: []byte("action"), TransitionID: []byte("transition"),
		Reason: interaction.Reason{Code: "completed"},
		Evidence: []Evidence{{
			SourceID: []byte("source"), PolicyID: []byte("policy"),
			ObjectID: "object",
		}, {
			SourceID: []byte("source"), PolicyID: []byte("policy"),
			ObjectID: "anchor", Reference: &reference,
		}},
		ConsumedEvidence:        []interaction.EvidenceReference{reference},
		ConsumedEvidenceBinding: [][]byte{[]byte("BIND")},
		OccurredAt:              time.Date(2026, 10, 8, 9, 0, 0, 0, time.UTC),
	}
	if _, err := normalizeEvent(delivered, false); err != nil {
		t.Fatalf("an aligned binding group was refused: %v", err)
	}

	// And the misalignment itself is refused rather than stored, so a
	// caller cannot poison a subscriber's page with one.
	delivered.ConsumedEvidenceBinding = [][]byte{[]byte("BIND"), []byte("X")}
	if _, err := normalizeEvent(delivered, false); err == nil {
		t.Fatal("a binding group longer than its references was accepted")
	}
}

// TestThePublicRouteDropsCallerBindings is #705's integrity half on the event
// plane. Only the completion path has a node gate to ask, so a binding
// presented by any other caller is forged — the same opening closed for
// executors in the dispatch plane — and the read gate trusts it.
func TestThePublicRouteDropsCallerBindings(t *testing.T) {
	stripped := withoutCallerBindings(Event{
		Kind:                       "custom.kind",
		ConsumedEvidence:           []interaction.EvidenceReference{{AnchorID: "a"}},
		ConsumedEvidenceBinding:    [][]byte{[]byte("forged")},
		CitedEvidenceBinding:       [][]byte{[]byte("forged")},
		ConsumedEvidenceVisibility: [][]string{{"secret"}},
	})
	if stripped.ConsumedEvidenceBinding != nil ||
		stripped.CitedEvidenceBinding != nil {
		t.Fatalf("a caller's bindings survived: consumed=%q cited=%q",
			stripped.ConsumedEvidenceBinding, stripped.CitedEvidenceBinding)
	}
	// Only the bindings go. A caller's visibility and references are its own
	// to declare, and dropping them here would change what the route does.
	if len(stripped.ConsumedEvidence) != 1 ||
		len(stripped.ConsumedEvidenceVisibility) != 1 {
		t.Fatalf("the strip removed more than the bindings: %#v", stripped)
	}
}
