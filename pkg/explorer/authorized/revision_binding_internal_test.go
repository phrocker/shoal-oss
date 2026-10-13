/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package authorized

// The durable revision binding (#695): what is written, in what order, how
// strictly it is read back, and what a binary from before it does with it.

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/internal/engine"
	"github.com/phrocker/shoal-oss/internal/iterrt"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// rawPolicyCell returns the newest record cell stored at row.
func rawPolicyCell(t *testing.T, dir string, row []byte) ([]byte, bool) {
	t.Helper()
	eng, err := engine.Open(dir, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	scanner, err := eng.Scan(policyTable, iterrt.InfiniteRange(), engine.ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	var value []byte
	found := false
	for scanner.Next() {
		if bytes.Equal(scanner.Key().Row, row) && !found {
			value = append([]byte(nil), scanner.Value()...)
			found = true
		}
		if err := scanner.Advance(); err != nil {
			t.Fatal(err)
		}
	}
	return value, found
}

func openDurableForTest(t *testing.T, dir string) *DurablePolicyStore {
	t.Helper()
	store, err := OpenDurablePolicyStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func closeDurableForTest(t *testing.T, store *DurablePolicyStore) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

var bindingTestRevisions = []RevisionReference{
	{DocumentID: "document", RevisionID: "revision"},
}

// requireOldBinaryRefuses checks the format floor the way every durable
// binary before #695 reads it: as a row under the source-claim prefix, which
// such a binary decodes strictly as a source claim, so it refuses to open.
func requireOldBinaryRefuses(t *testing.T, dir string) {
	t.Helper()
	value, ok := rawPolicyCell(t, dir, []byte(policyRowFormatFloor))
	if !ok {
		t.Fatal("the catalog holds a revision binding but no format floor")
	}
	if !strings.HasPrefix(policyRowFormatFloor, policyRowSourceClaim) {
		t.Fatal("the format floor is not under the source-claim prefix")
	}
	var claim persistedSourceClaim
	err := decodePolicyRecord(value, policyKindSourceClaim, &claim)
	if err == nil || !strings.Contains(err.Error(), "kind") {
		t.Fatalf("an old binary would decode the floor as a source claim: %v", err)
	}
	var floor persistedFormatFloor
	if err := decodePolicyRecord(value, policyKindFormatFloor, &floor); err != nil ||
		floor.Floor != formatFloorRevisionBindings {
		t.Fatalf("floor = %+v, %v", floor, err)
	}
}

// TestFormatFloorIsWrittenOnlyBeforeTheFirstBinding: a catalog with no
// binding stays openable by older binaries; the first interaction binding
// writes the floor first, and both survive a reopen.
func TestFormatFloorIsWrittenOnlyBeforeTheFirstBinding(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openDurableForTest(t, dir)
	seedDurableStore(t, store) // an unbound application edge
	if err := store.PutInteractionRevisions(ctx, "session", nil); err != nil {
		t.Fatal(err)
	}
	closeDurableForTest(t, store)
	if _, ok := rawPolicyCell(t, dir, []byte(policyRowFormatFloor)); ok {
		t.Fatal("a catalog without bindings was given a format floor")
	}

	store = openDurableForTest(t, dir)
	if err := store.PutInteractionRevisions(
		ctx, "session", bindingTestRevisions); err != nil {
		t.Fatal(err)
	}
	more := []RevisionReference{{DocumentID: "document", RevisionID: "older"}}
	if err := store.PutInteractionRevisions(ctx, "session", more); err != nil {
		t.Fatal(err)
	}
	closeDurableForTest(t, store)
	requireOldBinaryRefuses(t, dir)
	if _, ok := rawPolicyCell(t, dir, policyInteractionRevisionsRow("session")); !ok {
		t.Fatal("the interaction binding was not persisted")
	}

	store = openDurableForTest(t, dir)
	defer closeDurableForTest(t, store)
	got, err := store.InteractionRevisions(ctx, []shoal.ID{"session", "other"})
	if err != nil {
		t.Fatal(err)
	}
	want := map[shoal.ID][]RevisionReference{"session": {
		{DocumentID: "document", RevisionID: "older"},
		{DocumentID: "document", RevisionID: "revision"},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reopened bindings = %+v, want the union %+v", got, want)
	}
}

// TestBoundEdgeWritesFormatFloorAndRoundTrips: a bound edge reservation
// writes the floor before itself, and the binding is restored exactly.
func TestBoundEdgeWritesFormatFloorAndRoundTrips(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := openDurableForTest(t, dir)
	seedDurableStore(t, store)
	registration := EdgeRegistration{
		Edge: graph.Edge{
			ID: "bound-edge", From: "document", To: "node-a",
			Type: "links", Weight: 1,
		},
		Rule: batchTestRule(t, "source", "policy"), Kind: RegistrationApplication,
		SourceRevisions: bindingTestRevisions, SourceRevisionsBound: true,
	}
	if err := store.ReserveEdge(ctx, registration); err != nil {
		t.Fatal(err)
	}
	closeDurableForTest(t, store)
	requireOldBinaryRefuses(t, dir)

	store = openDurableForTest(t, dir)
	if err := store.PutEdge(ctx, registration); err != nil {
		t.Fatal(err)
	}
	closeDurableForTest(t, store)
	store = openDurableForTest(t, dir)
	defer closeDurableForTest(t, store)
	stored, ok, err := store.Edge(ctx, "bound-edge")
	if err != nil || !ok {
		t.Fatalf("Edge = %v, %v", ok, err)
	}
	if !stored.SourceRevisionsBound ||
		!reflect.DeepEqual(stored.SourceRevisions, bindingTestRevisions) {
		t.Fatalf("reopened binding = %+v", stored)
	}
	legacy, ok, err := store.Edge(ctx, "application-edge")
	if err != nil || !ok || legacy.SourceRevisionsBound || legacy.SourceRevisions != nil {
		t.Fatalf("an edge written without a binding reopened bound: %+v", legacy)
	}
}

// TestEdgeBindingIsPartOfTheRegistration: a reservation and its commit must
// carry the same binding, and an extracted edge that replaces a bound
// application edge keeps its binding, so replacing can only narrow.
func TestEdgeBindingIsPartOfTheRegistration(t *testing.T) {
	ctx := context.Background()
	durable := openDurableForTest(t, t.TempDir())
	defer closeDurableForTest(t, durable)
	for name, store := range map[string]PolicyStore{
		"memory": NewMemoryPolicyStore(), "durable": durable,
	} {
		rule := batchTestRule(t, "source", "policy")
		edge := graph.Edge{ID: "edge", From: "a", To: "b", Type: "links", Weight: 1}
		bound := EdgeRegistration{
			Edge: edge, Rule: rule, Kind: RegistrationApplication,
			SourceRevisions: bindingTestRevisions, SourceRevisionsBound: true,
		}
		loose := bound
		loose.SourceRevisionsBound = false
		if err := store.ReserveEdge(ctx, loose); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
			t.Fatalf("%s: revisions without a bound registration = %v", name, err)
		}
		materialized := bound
		materialized.Kind = RegistrationMaterialized
		if err := store.PutEdge(ctx, materialized); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
			t.Fatalf("%s: a bound materialized edge = %v", name, err)
		}
		if err := store.ReserveEdge(ctx, bound); err != nil {
			t.Fatal(err)
		}
		other := bound
		other.SourceRevisions = []RevisionReference{
			{DocumentID: "document", RevisionID: "other"},
		}
		if err := store.PutEdge(ctx, other); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
			t.Fatalf("%s: a commit with another binding = %v, want conflict", name, err)
		}
		if err := store.PutEdge(ctx, unboundEdge(bound)); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
			t.Fatalf("%s: an unbound commit = %v, want conflict", name, err)
		}
		if err := store.PutEdge(ctx, bound); err != nil {
			t.Fatal(err)
		}
		extracted := EdgeRegistration{
			Edge: edge, Rule: rule, Kind: RegistrationExtracted,
			DocumentID: "extracting", RevisionID: "extracting-revision",
		}
		if err := store.PutEdge(ctx, extracted); err != nil {
			t.Fatal(err)
		}
		stored, ok, err := store.Edge(ctx, "edge")
		if err != nil || !ok {
			t.Fatalf("%s: Edge = %v, %v", name, ok, err)
		}
		if stored.Kind != RegistrationExtracted || !stored.SourceRevisionsBound ||
			!reflect.DeepEqual(stored.SourceRevisions, bindingTestRevisions) {
			t.Fatalf("%s: replacing dropped the binding: %+v", name, stored)
		}
		stored.SourceRevisions[0].RevisionID = "mutated"
		again, _, _ := store.Edge(ctx, "edge")
		if !reflect.DeepEqual(again.SourceRevisions, bindingTestRevisions) {
			t.Fatalf("%s: a read aliases the stored binding", name)
		}
	}
}

func unboundEdge(registration EdgeRegistration) EdgeRegistration {
	registration.SourceRevisions = nil
	registration.SourceRevisionsBound = false
	return registration
}

// TestRevisionBindingRowsAreDecodedStrictly: every malformed binding fails
// the open, as every other malformed policy record does.
func TestRevisionBindingRowsAreDecodedStrictly(t *testing.T) {
	floor := func(t *testing.T, value uint64) []byte {
		return mustEncodePolicyRecord(t, policyKindFormatFloor,
			persistedFormatFloor{Seq: 900, Floor: value})
	}
	interactionRow := func(t *testing.T, session string, refs ...persistedRevisionReference) []byte {
		return mustEncodePolicyRecord(t, policyKindInteractionRevisions,
			persistedInteractionRevisions{Seq: 901, SessionID: session, Revisions: refs})
	}
	edgeRow := func(t *testing.T, kind RegistrationKind, bound bool, refs ...persistedRevisionReference) []byte {
		return mustEncodePolicyRecord(t, policyKindEdge, persistedEdge{
			Seq: 902,
			Edge: persistedGraphEdge{
				ID: "raw-edge", From: "document", To: "node-a",
				Type: "links", Weight: 1,
			},
			Rule: mustPersistRuleForTest(t), Kind: uint8(kind),
			SourceRevisions: refs, SourceRevisionsBound: bound,
		})
	}
	a := persistedRevisionReference{DocumentID: "document", RevisionID: "a"}
	b := persistedRevisionReference{DocumentID: "document", RevisionID: "b"}
	type row struct {
		key   []byte
		value func(*testing.T) []byte
	}
	floorRow := row{[]byte(policyRowFormatFloor), func(t *testing.T) []byte { return floor(t, 1) }}
	sessionRow := func(value func(*testing.T) []byte) row {
		return row{policyInteractionRevisionsRow("session"), value}
	}
	rawEdgeRow := func(value func(*testing.T) []byte) row {
		return row{policyEdgeRow("raw-edge"), value}
	}
	for _, test := range []struct {
		name     string
		rows     []row
		accepted bool
	}{
		{"valid interaction binding", []row{floorRow, sessionRow(func(t *testing.T) []byte {
			return interactionRow(t, "session", a, b)
		})}, true},
		{"valid bound edge", []row{floorRow, rawEdgeRow(func(t *testing.T) []byte {
			return edgeRow(t, RegistrationApplication, true, a, b)
		})}, true},
		{"valid bound edge with no revisions", []row{floorRow, rawEdgeRow(func(t *testing.T) []byte {
			return edgeRow(t, RegistrationApplication, true)
		})}, true},
		{"floor alone", []row{floorRow}, true},
		{"unknown floor", []row{{[]byte(policyRowFormatFloor), func(t *testing.T) []byte {
			return floor(t, 2)
		}}}, false},
		{"floor of the wrong kind", []row{{[]byte(policyRowFormatFloor), func(t *testing.T) []byte {
			return mustEncodePolicyRecord(t, policyKindSourceClaim, persistedSourceClaim{
				Seq: 1, SourceURI: "file:///x", Version: 1, Rule: mustPersistRuleForTest(t),
			})
		}}}, false},
		{"interaction binding without floor", []row{sessionRow(func(t *testing.T) []byte {
			return interactionRow(t, "session", a)
		})}, false},
		{"bound edge without floor", []row{rawEdgeRow(func(t *testing.T) []byte {
			return edgeRow(t, RegistrationApplication, true, a)
		})}, false},
		{"empty interaction binding", []row{floorRow, sessionRow(func(t *testing.T) []byte {
			return interactionRow(t, "session")
		})}, false},
		{"unsorted interaction binding", []row{floorRow, sessionRow(func(t *testing.T) []byte {
			return interactionRow(t, "session", b, a)
		})}, false},
		{"duplicated interaction binding", []row{floorRow, sessionRow(func(t *testing.T) []byte {
			return interactionRow(t, "session", a, a)
		})}, false},
		{"interaction binding with an empty revision", []row{floorRow, sessionRow(func(t *testing.T) []byte {
			return interactionRow(t, "session", persistedRevisionReference{DocumentID: "document"})
		})}, false},
		{"interaction binding under another session's row", []row{floorRow, sessionRow(func(t *testing.T) []byte {
			return interactionRow(t, "another", a)
		})}, false},
		{"interaction binding of the wrong kind", []row{floorRow, sessionRow(func(t *testing.T) []byte {
			return mustEncodePolicyRecord(t, policyKindEdge, persistedInteractionRevisions{
				Seq: 1, SessionID: "session", Revisions: []persistedRevisionReference{a},
			})
		})}, false},
		{"edge revisions without binding", []row{floorRow, rawEdgeRow(func(t *testing.T) []byte {
			return edgeRow(t, RegistrationApplication, false, a)
		})}, false},
		{"unsorted edge binding", []row{floorRow, rawEdgeRow(func(t *testing.T) []byte {
			return edgeRow(t, RegistrationApplication, true, b, a)
		})}, false},
		{"bound materialized edge", []row{floorRow, rawEdgeRow(func(t *testing.T) []byte {
			return edgeRow(t, RegistrationMaterialized, true, a)
		})}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			store := openDurableForTest(t, dir)
			seedDurableStore(t, store)
			closeDurableForTest(t, store)
			for _, row := range test.rows {
				corruptPolicyRow(t, dir, row.key, row.value(t))
			}
			reopened, err := OpenDurablePolicyStore(dir)
			if err == nil {
				_ = reopened.Close()
			}
			if (err == nil) != test.accepted {
				t.Fatalf("open = %v, want accepted=%v", err, test.accepted)
			}
			if err != nil && !shoal.IsErrorCode(err, shoal.ErrorInternal) {
				t.Fatalf("open = %v, want internal", err)
			}
		})
	}
}

// TestInteractionRevisionsOnlyGrow: a second recording unions with the first
// in both stores, and never removes a revision.
func TestInteractionRevisionsOnlyGrow(t *testing.T) {
	ctx := context.Background()
	durable := openDurableForTest(t, t.TempDir())
	defer closeDurableForTest(t, durable)
	for name, store := range map[string]PolicyStore{
		"memory": NewMemoryPolicyStore(), "durable": durable,
	} {
		first := []RevisionReference{{DocumentID: "d", RevisionID: "r2"}}
		second := []RevisionReference{
			{DocumentID: "d", RevisionID: "r1"}, {DocumentID: "d", RevisionID: "r1"},
		}
		if err := store.PutInteractionRevisions(ctx, "s", first); err != nil {
			t.Fatal(err)
		}
		if err := store.PutInteractionRevisions(ctx, "s", second); err != nil {
			t.Fatal(err)
		}
		if err := store.PutInteractionRevisions(ctx, "s", nil); err != nil {
			t.Fatal(err)
		}
		got, err := store.InteractionRevisions(ctx, []shoal.ID{"s"})
		if err != nil {
			t.Fatal(err)
		}
		want := []RevisionReference{
			{DocumentID: "d", RevisionID: "r1"}, {DocumentID: "d", RevisionID: "r2"},
		}
		if !reflect.DeepEqual(got["s"], want) {
			t.Fatalf("%s: recorded %+v, want %+v", name, got["s"], want)
		}
		if err := store.PutInteractionRevisions(ctx, "s", []RevisionReference{
			{DocumentID: "d"},
		}); err == nil {
			t.Fatalf("%s: an incomplete revision was recorded", name)
		}
	}
}

// TestDocumentRevisionRulesListsEveryRevision: every registered revision is
// reported, ordered by revision ID, with only the current one marked, and in
// a reopened durable store too.
func TestDocumentRevisionRulesListsEveryRevision(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	durable := openDurableForTest(t, dir)
	memory := NewMemoryPolicyStore()
	rules := []AccessRule{
		batchTestRule(t, "source", "one"), batchTestRule(t, "source", "two"),
	}
	for _, store := range []PolicyStore{memory, durable} {
		for index, revision := range []shoal.ID{"rev-one", "rev-two"} {
			if err := store.PutRevision(ctx, RevisionRegistration{
				DocumentID: "document", RevisionID: revision,
				NodeIDs:       []shoal.ID{"document"},
				ContentDigest: [32]byte{byte(index + 1)},
				Rule:          rules[index], Current: true,
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	closeDurableForTest(t, durable)
	durable = openDurableForTest(t, dir)
	defer closeDurableForTest(t, durable)
	for name, store := range map[string]PolicyStore{"memory": memory, "durable": durable} {
		got, err := store.DocumentRevisionRules(ctx,
			[]shoal.ID{"document", "absent", "document"})
		if err != nil {
			t.Fatal(err)
		}
		listed := got["document"]
		if len(got) != 1 || len(listed) != 2 ||
			listed[0].RevisionID != "rev-one" || listed[0].Current ||
			!listed[0].Rule.equal(rules[0]) ||
			listed[1].RevisionID != "rev-two" || !listed[1].Current ||
			!listed[1].Rule.equal(rules[1]) {
			t.Fatalf("%s: DocumentRevisionRules = %+v", name, got)
		}
		// RevisionRules: the referenced revision and the current one only.
		partial, err := store.RevisionRules(ctx, []RevisionReference{
			{DocumentID: "document", RevisionID: "rev-one"},
			{DocumentID: "document", RevisionID: "unregistered"},
			{DocumentID: "absent", RevisionID: "rev-one"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(revisionIDsOf(partial), map[shoal.ID][]string{
			"document": {"rev-one", "rev-two*"},
		}) {
			t.Fatalf("%s: RevisionRules = %+v", name, revisionIDsOf(partial))
		}
		current, err := store.RevisionRules(ctx, []RevisionReference{
			{DocumentID: "document", RevisionID: "rev-two"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(revisionIDsOf(current), map[shoal.ID][]string{
			"document": {"rev-two*"},
		}) {
			t.Fatalf("%s: RevisionRules(current) = %+v", name, revisionIDsOf(current))
		}
		if _, err := store.RevisionRules(ctx, []RevisionReference{
			{DocumentID: "document"},
		}); err == nil {
			t.Fatalf("%s: an incomplete reference was accepted", name)
		}
	}
}

// revisionIDsOf renders rules as revision IDs, the current one starred.
func revisionIDsOf(rules map[shoal.ID][]RevisionRule) map[shoal.ID][]string {
	rendered := make(map[shoal.ID][]string, len(rules))
	for documentID, listed := range rules {
		for _, rule := range listed {
			id := string(rule.RevisionID)
			if rule.Current {
				id += "*"
			}
			rendered[documentID] = append(rendered[documentID], id)
		}
	}
	return rendered
}
