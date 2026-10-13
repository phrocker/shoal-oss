// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleetevents

import (
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleetevents"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestAnEventKeepsItsEvidenceBindingThroughStorage is the durability half of
// #705 on the event plane.
//
// The event is persisted through eventWire with field-by-field copies in both
// directions, so a field absent there is silently dropped on the way to disk.
// A dropped binding is not a visible failure: delivery holds an unbound
// reference to every registered revision, so the evidence simply stops
// reaching subscribers who should see it, permanently and quietly.
func TestAnEventKeepsItsEvidenceBindingThroughStorage(t *testing.T) {
	event := fleetevents.Event{
		Kind: "action.completed", ActionID: []byte("action"),
		ConsumedEvidence: []interaction.EvidenceReference{
			{Kind: interaction.EvidenceGraph, NodeIDs: []shoal.ID{"node"}},
			{Kind: interaction.EvidenceGraph, NodeIDs: []shoal.ID{"other"}},
		},
		ConsumedEvidenceBinding: [][]byte{[]byte("binding-one"), nil},
	}

	restored := eventToWire(event).domain()

	if len(restored.ConsumedEvidenceBinding) != 2 {
		t.Fatalf("bindings = %q, want two entries index for index with the "+
			"references", restored.ConsumedEvidenceBinding)
	}
	if string(restored.ConsumedEvidenceBinding[0]) != "binding-one" {
		t.Fatalf("binding = %q, want it verbatim: the fleet plane stores it "+
			"opaquely and the gate reads it, so any change is a different "+
			"registration set", restored.ConsumedEvidenceBinding[0])
	}
	if restored.ConsumedEvidenceBinding[1] != nil {
		t.Fatalf("an unbound entry became %q; nil is how one reference in a "+
			"bound group says it has no binding",
			restored.ConsumedEvidenceBinding[1])
	}
	// And the copy must not alias the original, for the reason every other
	// group copy does not.
	restored.ConsumedEvidenceBinding[0][0] = 'X'
	if event.ConsumedEvidenceBinding[0][0] == 'X' {
		t.Fatal("the stored event shares its binding array with the original")
	}
}

// TestAPublishedEventCarriesTheRecordsBindings pins the publisher half: a
// binding stored on the action's evidence reaches the event, or delivery
// decides on current registrations alone and a relabel opens it.
func TestAPublishedEventCarriesTheRecordsBindings(t *testing.T) {
	evidence := []fleet.EvidenceRef{
		{Kind: interaction.EvidenceGraph, NodeIDs: []shoal.ID{"node"},
			Binding: []byte("binding-one")},
		{Kind: interaction.EvidenceGraph, NodeIDs: []shoal.ID{"other"}},
	}
	got := actionEvidenceBinding(evidence)
	if len(got) != 2 || string(got[0]) != "binding-one" || got[1] != nil {
		t.Fatalf("bindings = %q, want the first bound and the second not", got)
	}
	// An all-unbound group carries nothing rather than a slice of nils, so a
	// record written before bindings existed produces the same event as one
	// whose references simply name no graph members.
	if actionEvidenceBinding([]fleet.EvidenceRef{
		{Kind: interaction.EvidenceGraph, NodeIDs: []shoal.ID{"node"}},
	}) != nil {
		t.Fatal("an unbound group produced a binding array")
	}
}

// TestACompletionPublishesItsEvidenceBindings pins the publisher's call
// site, not just the helper it calls.
//
// An earlier version of this file tested actionEvidenceBinding directly and
// let a mutation deleting the publisher's call to it survive — the helper was
// right and nothing used it. This drives a real completion through the plane
// whose dispatch side wires the authorized client's NodeGate (fullLabels),
// which is the only gate that binds, and asserts the durable event carries
// what the record carries.
func TestACompletionPublishesItsEvidenceBindings(t *testing.T) {
	plane := newLabelPlaneWith(t, planeOptions{
		choose: fullLabels, wireEvents: true, label: "secret",
	})
	stored := storedCompletion(t, labelledDelivery{backend: plane.backend})

	if len(stored.ConsumedEvidence) == 0 {
		t.Fatal("the completion carried no evidence, so this test pins " +
			"nothing about bindings")
	}
	if len(stored.ConsumedEvidenceBinding) != len(stored.ConsumedEvidence) {
		t.Fatalf("bindings = %d for %d references: the durable event must "+
			"carry one entry per reference, or delivery holds them all to "+
			"every registered revision",
			len(stored.ConsumedEvidenceBinding), len(stored.ConsumedEvidence))
	}
	bound := 0
	for _, binding := range stored.ConsumedEvidenceBinding {
		if len(binding) > 0 {
			bound++
		}
	}
	if bound == 0 {
		t.Fatal("every reference published unbound; this plane's gate binds, " +
			"so either the record stored no binding or the publisher " +
			"dropped it")
	}
}
