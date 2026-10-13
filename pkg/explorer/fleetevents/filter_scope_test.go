// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleetevents

import "testing"

// TestANamedScopeIsNotSatisfiedByAnEventWithoutEvidence pins what a filter
// promises.
//
// Both scope checks live inside matchesFilter's loop over event.Evidence, so
// an evidence-free event used to skip them and satisfy every SourceIDs and
// PolicyIDs filter vacuously.
//
// No publisher can produce such an event: normalizeEvent has refused
// len(Evidence) == 0 since #343, and every publication path goes through it.
// So this is defence in depth over a stored row with no evidence — a corrupt
// one, or a direct backend write, which the adapter's Scan decodes without
// re-validating. It would not widen past authorization either: Filter only
// narrows, and the subscription's own scope is checked at creation. It is
// still not what the filter says it does.
func TestANamedScopeIsNotSatisfiedByAnEventWithoutEvidence(t *testing.T) {
	inScope := Event{Kind: "action.enqueued", Evidence: []Evidence{{
		SourceID: []byte("source-a"), PolicyID: []byte("policy-a"),
	}}}
	outOfScope := Event{Kind: "action.enqueued", Evidence: []Evidence{{
		SourceID: []byte("source-b"), PolicyID: []byte("policy-a"),
	}}}
	bare := Event{Kind: "action.enqueued"}

	for _, probe := range []struct {
		name   string
		filter Filter
		event  Event
		want   bool
	}{
		// The regression. A named source, an event that carries none.
		{"no evidence against a source filter",
			Filter{SourceIDs: [][]byte{[]byte("source-a")}}, bare, false},
		{"no evidence against a policy filter",
			Filter{PolicyIDs: [][]byte{[]byte("policy-a")}}, bare, false},
		// And the cases that must keep working, so the fix is a narrowing of
		// exactly one condition rather than of delivery generally.
		{"no evidence against a kind-only filter",
			Filter{Kinds: []string{"action.enqueued"}}, bare, true},
		{"no evidence against an empty filter", Filter{}, bare, true},
		{"evidence inside the named source",
			Filter{SourceIDs: [][]byte{[]byte("source-a")}}, inScope, true},
		{"evidence outside the named source",
			Filter{SourceIDs: [][]byte{[]byte("source-a")}}, outOfScope, false},
		{"evidence inside the named policy",
			Filter{PolicyIDs: [][]byte{[]byte("policy-a")}}, inScope, true},
		{"a kind that does not match",
			Filter{Kinds: []string{"action.failed"}}, inScope, false},
	} {
		t.Run(probe.name, func(t *testing.T) {
			if got := matchesFilter(probe.filter, probe.event); got != probe.want {
				t.Fatalf("matchesFilter = %v, want %v", got, probe.want)
			}
		})
	}
}
