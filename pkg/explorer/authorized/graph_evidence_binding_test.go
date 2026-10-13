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

package authorized_test

// Fleet graph evidence is held to the registrations it was recorded over
// (#705): a relabel after the record must not open it, a materialization
// must not be sent down the revision path, and evidence recorded before
// bindings existed is held to the legacy bound.

import (
	"context"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/explorer/evidencelabels"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// nodeEvidence is exact graph evidence naming one node of the corpus.
func nodeEvidence(
	t testing.TB, corpus *explorer.Explorer, nodeID shoal.ID,
) interaction.EvidenceReference {
	t.Helper()
	around, err := corpus.Neighborhood(context.Background(), explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{nodeID}, Depth: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range around.Nodes {
		if node.ID != nodeID {
			continue
		}
		anchor, err := inference.NewGraphAnchor(graph.Path{Nodes: []graph.Node{node}})
		if err != nil {
			t.Fatal(err)
		}
		reference, err := anchor.EvidenceReference()
		if err != nil {
			t.Fatal(err)
		}
		return reference
	}
	t.Fatalf("node %s is not in the corpus", nodeID)
	return interaction.EvidenceReference{}
}

// fleetEvidence is one recorded fleet reference: what the dispatch plane
// would store, and the graph it would pass back on a read.
type fleetEvidence struct {
	reference interaction.EvidenceReference
	binding   []byte
	asOf      time.Time
}

func (e fleetEvidence) bound() evidencelabels.Graph {
	return evidencelabels.Graph{
		NodeIDs: e.reference.NodeIDs, EdgeIDs: e.reference.EdgeIDs, Binding: e.binding,
	}
}

// legacy is the same reference as a record written before bindings existed.
func (e fleetEvidence) legacy() evidencelabels.Graph {
	return evidencelabels.Graph{NodeIDs: e.reference.NodeIDs, EdgeIDs: e.reference.EdgeIDs}
}

// bindFleetEvidence records references as ctx's principal at the world's
// current snapshot, exactly as the dispatch plane does at completion.
func (w *bindingWorld) bindFleetEvidence(
	client *authorized.Client, ctx context.Context,
	references ...interaction.EvidenceReference,
) []fleetEvidence {
	w.t.Helper()
	bindings, valid, err := client.NodeGate().BindGraphEvidence(ctx,
		shoal.ID(w.snapshot.ID), w.snapshot.AsOf, references)
	if err != nil || !valid {
		w.t.Fatalf("BindGraphEvidence: valid = %v, err = %v", valid, err)
	}
	if len(bindings) != len(references) {
		w.t.Fatalf("%d bindings for %d references", len(bindings), len(references))
	}
	recorded := make([]fleetEvidence, len(references))
	for index, reference := range references {
		if len(bindings[index]) == 0 {
			w.t.Fatalf("reference %d was recorded with no binding", index)
		}
		recorded[index] = fleetEvidence{
			reference: reference, binding: bindings[index], asOf: w.snapshot.AsOf,
		}
	}
	return recorded
}

func gateVerdicts(
	t testing.TB, client *authorized.Client, ctx context.Context,
	graphs ...evidencelabels.Graph,
) []bool {
	t.Helper()
	verdicts, err := client.NodeGate().GraphsVisibleToReader(ctx, graphs)
	if err != nil {
		t.Fatal(err)
	}
	return verdicts
}

func requireVerdicts(t testing.TB, name string, got []bool, want ...bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d verdicts, want %d", name, len(got), len(want))
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("%s: verdicts %v, want %v", name, got, want)
		}
	}
}

// fleetWorld is the bindingWorld with a materialized team node, an
// application edge from the open document into the labelled one, and fleet
// evidence over each, recorded by the holder before the relabel.
type fleetWorld struct {
	*bindingWorld
	outsider auth.Decision
	team     shoal.ID
	edge     shoal.ID
	closed   fleetEvidence // the labelled document's node
	open     fleetEvidence // the open document's node (control)
	teamNode fleetEvidence // the materialized node
	edgeRef  fleetEvidence // the application edge open -> closed
}

func newFleetWorld(t *testing.T, store authorized.PolicyStore) *fleetWorld {
	t.Helper()
	w := &fleetWorld{bindingWorld: newBindingWorld(t, newFixture(t), store)}
	admin := w.f.labelIngester(t, "admin", "secret")
	snapshot, err := w.f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := w.client.MaterializeGraph(admin, explorer.GraphMaterializationRequest{
		Namespace: []byte("fleet-teams"), MutationID: "fleet-teams",
		SourceID: w.f.sourceA, PolicyID: w.f.policyA, ExpectedSnapshot: snapshot,
		Nodes: []explorer.GraphNodeSpec{{
			Key: []byte("team"), Kind: "team", Properties: shoal.Metadata{"name": "Team"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	w.team = materialized.Nodes[0].ID
	w.connect(w.client, bindingEdge, w.bindingWorld.open.Document.ID,
		w.bindingWorld.closed.Document.ID)
	w.edge = w.edgeID(w.client, bindingEdge)
	w.resnapshot()
	w.outsider = w.f.decision(t, "outsider",
		[][]byte{w.f.sourceB}, [][]byte{w.f.policyB}, labelReaderOperations)
	recorded := w.bindFleetEvidence(w.client, w.as(w.holder),
		nodeEvidence(t, w.f.base, w.bindingWorld.closed.Document.ID),
		nodeEvidence(t, w.f.base, w.bindingWorld.open.Document.ID),
		nodeEvidence(t, w.f.base, w.team),
		exactAuthorizedGraphEvidence(t, w.f.base, graph.Edge{
			ID: w.edge, From: w.bindingWorld.open.Document.ID,
			To: w.bindingWorld.closed.Document.ID,
		}))
	w.closed, w.open, w.teamNode, w.edgeRef = recorded[0], recorded[1], recorded[2], recorded[3]
	return w
}

// TestFleetGraphEvidenceStaysClosedAfterARelabel is the issue's
// reproduction: graph evidence a fleet action recorded over the labelled
// revision R1 must stay closed to non-holders once the document is
// relabelled, for a document's node and for an application edge into it; a
// materialized node stays visible to every reader of its materialization's
// rule; and the holder keeps seeing all of it.
func TestFleetGraphEvidenceStaysClosedAfterARelabel(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newFleetWorld(t, store)
		reader, holder, outsider := w.as(w.reader), w.as(w.holder), w.as(w.outsider)
		check := func(stage string) {
			t.Helper()
			for _, probe := range []struct {
				name     string
				evidence fleetEvidence
				reader   bool
			}{
				{"labelled document", w.closed, false},
				{"open document", w.open, true},
				{"materialized node", w.teamNode, true},
				{"application edge", w.edgeRef, false},
			} {
				graphs := []evidencelabels.Graph{
					probe.evidence.bound(), probe.evidence.legacy(),
				}
				requireVerdicts(t, stage+": reader, "+probe.name,
					gateVerdicts(t, w.client, reader, graphs...),
					probe.reader, probe.reader)
				requireVerdicts(t, stage+": holder, "+probe.name,
					gateVerdicts(t, w.client, holder, graphs...), true, true)
			}
			// The materialization's rule is what decides its node: no
			// reader outside it sees it, bound or not.
			requireVerdicts(t, stage+": outsider, materialized node",
				gateVerdicts(t, w.client, outsider,
					w.teamNode.bound(), w.teamNode.legacy()), false, false)
		}
		check("before the relabel")
		relabelled := w.relabel(w.client)
		check("after the relabel")

		// Not vacuous: evidence over the same node recorded after the
		// relabel names the unlabelled revision, and the reader sees it.
		// That is what the current registrations alone decided for the
		// evidence recorded over R1 before #705.
		w.resnapshot()
		reader, holder = w.as(w.reader), w.as(w.holder)
		later := w.bindFleetEvidence(w.client, holder,
			nodeEvidence(t, w.f.base, relabelled.Document.ID))
		requireVerdicts(t, "evidence recorded after the relabel",
			gateVerdicts(t, w.client, reader, later[0].bound(), w.closed.bound()),
			true, false)
	})
}

// TestFleetGraphEvidenceDenialsLookLikeAbsence: every denial, a binding
// that does not decode included, is the same false verdict, with no error,
// that a reference to something never registered gets (#398).
func TestFleetGraphEvidenceDenialsLookLikeAbsence(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newFleetWorld(t, store)
		w.relabel(w.client)
		never := evidencelabels.Graph{NodeIDs: []shoal.ID{"node-never-registered"}}
		neverBound := never
		neverBound.Binding = w.open.binding
		corrupt := w.closed.bound()
		corrupt.Binding = append(append([]byte(nil), corrupt.Binding...), 0)
		requireVerdicts(t, "reader",
			gateVerdicts(t, w.client, w.as(w.reader),
				w.closed.bound(), never, neverBound, corrupt),
			false, false, false, false)
		// A corrupt binding is denied even to the holder: nothing vouches
		// for what it was recorded over.
		requireVerdicts(t, "holder",
			gateVerdicts(t, w.client, w.as(w.holder), corrupt, never), false, false)
	})
}

// edgeReplaced serves one application edge as the extracted relation that
// replaced it (extractedEdgeMerge), keeping its Connect binding.
type edgeReplaced struct {
	authorized.PolicyStore
	edgeID, documentID, revisionID shoal.ID
}

func (s edgeReplaced) Edges(
	ctx context.Context, edgeIDs []shoal.ID,
) (map[shoal.ID]authorized.EdgeRegistration, error) {
	resolved, err := s.PolicyStore.Edges(ctx, edgeIDs)
	if err != nil {
		return nil, err
	}
	if registration, ok := resolved[s.edgeID]; ok {
		registration.Kind = authorized.RegistrationExtracted
		registration.DocumentID, registration.RevisionID = s.documentID, s.revisionID
		resolved[s.edgeID] = registration
	}
	return resolved, nil
}

// kindless serves one node registration with RegistrationUnspecified, which
// no catalog yields (legacyNodeKind maps pre-kind records at load).
type kindless struct {
	authorized.PolicyStore
	nodeID shoal.ID
}

func (s kindless) Nodes(
	ctx context.Context, nodeIDs []shoal.ID,
) (map[shoal.ID]authorized.NodeRegistration, error) {
	resolved, err := s.PolicyStore.Nodes(ctx, nodeIDs)
	if err != nil {
		return nil, err
	}
	if registration, ok := resolved[s.nodeID]; ok {
		registration.Kind = authorized.RegistrationUnspecified
		resolved[s.nodeID] = registration
	}
	return resolved, nil
}

// rebind re-encodes evidence's binding after edit changes its entries.
func rebind(
	t testing.TB, evidence fleetEvidence,
	edit func([]authorized.GraphBindingEntry) []authorized.GraphBindingEntry,
) evidencelabels.Graph {
	t.Helper()
	binding, err := authorized.DecodeGraphEvidenceBinding(evidence.binding)
	if err != nil {
		t.Fatal(err)
	}
	edited, err := authorized.NewGraphEvidenceBinding(edit(binding.Entries())...)
	if err != nil {
		t.Fatal(err)
	}
	graph := evidence.bound()
	graph.Binding = nil
	if !edited.Empty() {
		graph.Binding, err = edited.Encode()
		if err != nil {
			t.Fatal(err)
		}
	}
	return graph
}

// TestFleetGraphEvidenceBindingCoversEveryMember: what the resolver records,
// and what each entry kind requires at read. Each probe is decided for the
// holder, who sees all of it through a binding as recorded.
func TestFleetGraphEvidenceBindingCoversEveryMember(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newFleetWorld(t, store)
		closedID := w.bindingWorld.closed.Document.ID
		openID := w.bindingWorld.open.Document.ID
		binding, err := authorized.DecodeGraphEvidenceBinding(w.edgeRef.binding)
		if err != nil {
			t.Fatal(err)
		}
		want := map[shoal.ID]authorized.GraphBindingEntry{
			w.edge: {Object: authorized.GraphBindingEdge, ID: w.edge,
				Kind: authorized.RegistrationApplication},
			openID: {Object: authorized.GraphBindingNode, ID: openID,
				Kind: authorized.RegistrationDocument, DocumentID: openID,
				RevisionID: w.bindingWorld.open.Revision.ID},
			closedID: {Object: authorized.GraphBindingNode, ID: closedID,
				Kind: authorized.RegistrationDocument, DocumentID: closedID,
				RevisionID: w.bindingWorld.closed.Revision.ID},
		}
		if entries := binding.Entries(); len(entries) != len(want) {
			t.Fatalf("edge binding %+v, want %+v", entries, want)
		}
		for _, entry := range binding.Entries() {
			if want[entry.ID] != entry {
				t.Fatalf("edge binding entry %+v, want %+v", entry, want[entry.ID])
			}
		}
		team, err := authorized.DecodeGraphEvidenceBinding(w.teamNode.binding)
		if err != nil {
			t.Fatal(err)
		}
		if entries := team.Entries(); len(entries) != 1 ||
			entries[0] != (authorized.GraphBindingEntry{Object: authorized.GraphBindingNode,
				ID: w.team, Kind: authorized.RegistrationMaterialized}) {
			t.Fatalf("a materialized node is recorded as %+v, with no revision", entries)
		}

		without := func(id shoal.ID) func([]authorized.GraphBindingEntry) []authorized.GraphBindingEntry {
			return func(entries []authorized.GraphBindingEntry) []authorized.GraphBindingEntry {
				var kept []authorized.GraphBindingEntry
				for _, entry := range entries {
					if entry.ID != id {
						kept = append(kept, entry)
					}
				}
				return kept
			}
		}
		recast := func(id shoal.ID, kind authorized.RegistrationKind) func(
			[]authorized.GraphBindingEntry) []authorized.GraphBindingEntry {
			return func(entries []authorized.GraphBindingEntry) []authorized.GraphBindingEntry {
				for index := range entries {
					if entries[index].ID == id {
						entries[index].Kind = kind
						entries[index].DocumentID, entries[index].RevisionID = "", ""
					}
				}
				return entries
			}
		}
		holder := w.as(w.holder)
		requireVerdicts(t, "as recorded", gateVerdicts(t, w.client, holder,
			w.edgeRef.bound(), w.open.bound(), w.teamNode.bound()), true, true, true)
		requireVerdicts(t, "an uncovered member", gateVerdicts(t, w.client, holder,
			rebind(t, w.edgeRef, without(closedID)),
			rebind(t, w.edgeRef, without(w.edge)),
			rebind(t, w.edgeRef, without(openID))), false, false, false)
		// An unspecified entry denies, whether it is a member's only entry or
		// sits beside one that covers it.
		requireVerdicts(t, "an unspecified entry", gateVerdicts(t, w.client, holder,
			rebind(t, w.open, recast(openID, authorized.RegistrationUnspecified)),
			rebind(t, w.open, func(entries []authorized.GraphBindingEntry) []authorized.GraphBindingEntry {
				return append(entries, authorized.GraphBindingEntry{
					Object: authorized.GraphBindingNode, ID: openID,
					Kind: authorized.RegistrationUnspecified,
				})
			})), false, false)
		// A document's node cannot be moved off the revision path by an
		// entry of another kind, nor a materialized node vouched for by one.
		requireVerdicts(t, "an entry of another kind", gateVerdicts(t, w.client, holder,
			rebind(t, w.open, recast(openID, authorized.RegistrationMaterialized)),
			rebind(t, w.teamNode, func(entries []authorized.GraphBindingEntry) []authorized.GraphBindingEntry {
				entries[0].Kind = authorized.RegistrationDocument
				entries[0].DocumentID = openID
				entries[0].RevisionID = w.bindingWorld.open.Revision.ID
				return entries
			})),
			false, false)
		// An application edge the catalog has since replaced by an extracted
		// relation (which keeps its Connect binding) is still covered by its
		// application entry.
		selector := w.f.staticSelector(t, w.f.sourceA, w.f.policyA)
		replaced := w.f.labelClient(t, w.f.base, edgeReplaced{
			PolicyStore: store, edgeID: w.edge,
			documentID: openID, revisionID: w.bindingWorld.open.Revision.ID,
		}, selector, selector)
		requireVerdicts(t, "a replaced application edge",
			gateVerdicts(t, replaced, holder, w.edgeRef.bound()), true)
		// A member the catalog serves with no kind denies, bound or legacy.
		unspecified := w.f.labelClient(t, w.f.base, kindless{
			PolicyStore: store, nodeID: openID,
		}, selector, selector)
		requireVerdicts(t, "an unspecified registration",
			gateVerdicts(t, unspecified, holder,
				w.open.bound(), w.open.legacy()), false, false)
		// An entry naming a revision the catalog never registered denies.
		requireVerdicts(t, "an unregistered revision", gateVerdicts(t, w.client, holder,
			rebind(t, w.open, func(entries []authorized.GraphBindingEntry) []authorized.GraphBindingEntry {
				entries[0].RevisionID = "revision-never-registered"
				return entries
			})), false)
	})
}

// ownerMoved serves one extracted node as owned by another revision, under
// rule: the catalog after its owner entry moved to a newer revision of the
// same document (recordOwner), which a relabel that keeps the entity's
// namespace would do.
type ownerMoved struct {
	authorized.PolicyStore
	nodeID, revisionID shoal.ID
	rule               authorized.AccessRule
}

func (s ownerMoved) Nodes(
	ctx context.Context, nodeIDs []shoal.ID,
) (map[shoal.ID]authorized.NodeRegistration, error) {
	resolved, err := s.PolicyStore.Nodes(ctx, nodeIDs)
	if err != nil {
		return nil, err
	}
	if registration, ok := resolved[s.nodeID]; ok {
		registration.RevisionID, registration.Rule = s.revisionID, s.rule
		resolved[s.nodeID] = registration
	}
	return resolved, nil
}

func (s ownerMoved) Node(
	ctx context.Context, nodeID shoal.ID,
) (authorized.NodeRegistration, bool, error) {
	resolved, err := s.Nodes(ctx, []shoal.ID{nodeID})
	if err != nil {
		return authorized.NodeRegistration{}, false, err
	}
	registration, ok := resolved[nodeID]
	return registration, ok, nil
}

// TestFleetGraphEvidenceOverARelabelledExtractedEntity: evidence over an
// entity extracted from the labelled revision R1 stays closed after the
// document is relabelled, both as the catalog leaves it (the entity stays
// R1's) and once its owner entry has moved to the unlabelled R2, which
// opens its current registration.
func TestFleetGraphEvidenceOverARelabelledExtractedEntity(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newExtractedEdgeWorld(t, store)
		const uri = "file:///fleet/omicron/SKILL.md"
		body := skillBody("omicron", "omicron-cli")
		labelled := w.ingest(uri, body, shoal.Metadata{interaction.PropertyVisibility: "secret"})
		extracted, err := w.extractAs(w.owner, labelled)
		if err != nil {
			t.Fatal(err)
		}
		if len(extracted.EntityNodeIDs) == 0 {
			t.Fatal("the extraction produced no entity")
		}
		entity := extracted.EntityNodeIDs[0]
		snapshot, err := w.f.base.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		w.f.clock.Set(snapshot.AsOf.Add(time.Second))
		w.owner = w.f.labelIngester(t, "owner", "secret")
		w.reader = w.f.labelIngester(t, "reader")
		bindings, valid, err := w.client.NodeGate().BindGraphEvidence(w.owner,
			shoal.ID(snapshot.ID), snapshot.AsOf,
			[]interaction.EvidenceReference{nodeEvidence(t, w.f.base, entity)})
		if err != nil || !valid {
			t.Fatalf("BindGraphEvidence: valid = %v, err = %v", valid, err)
		}
		recorded := fleetEvidence{
			reference: interaction.EvidenceReference{NodeIDs: []shoal.ID{entity}},
			binding:   bindings[0], asOf: snapshot.AsOf,
		}
		entry, err := authorized.DecodeGraphEvidenceBinding(recorded.binding)
		if err != nil {
			t.Fatal(err)
		}
		if entries := entry.Entries(); len(entries) != 1 ||
			entries[0].Kind != authorized.RegistrationExtracted ||
			entries[0].DocumentID != labelled.Document.ID ||
			entries[0].RevisionID != labelled.Revision.ID {
			t.Fatalf("entity binding %+v, want extracted from R1", entries)
		}
		graphs := []evidencelabels.Graph{recorded.bound(), recorded.legacy()}
		requireVerdicts(t, "reader before", gateVerdicts(t, w.client, w.reader, graphs...),
			false, false)
		requireVerdicts(t, "holder before", gateVerdicts(t, w.client, w.owner, graphs...),
			true, true)

		declassify, err := authorized.WithDeclassificationReason(w.owner, "ticket:SEC-705")
		if err != nil {
			t.Fatal(err)
		}
		public := w.ingestAs(declassify, uri, body+"\nNow public.\n", nil)
		if public.Revision.ID == labelled.Revision.ID {
			t.Fatal("the relabel did not make a new revision")
		}
		requireVerdicts(t, "reader after", gateVerdicts(t, w.client, w.reader, graphs...),
			false, false)
		requireVerdicts(t, "holder after", gateVerdicts(t, w.client, w.owner, graphs...),
			true, true)

		// The owner entry moved to R2, under R2's rule (the document node's).
		document, ok, err := store.Node(context.Background(), public.Document.ID)
		if err != nil || !ok {
			t.Fatalf("document node: %v %v", ok, err)
		}
		selector := w.f.staticSelector(t, w.f.sourceA, w.f.policyA)
		moved := w.f.labelClient(t, w.f.base, ownerMoved{
			PolicyStore: store, nodeID: entity,
			revisionID: public.Revision.ID, rule: document.Rule,
		}, selector, selector)
		// Not vacuous: the entity's current registration now opens it.
		if !nodeVisible(t, moved, w.reader, entity) {
			t.Fatal("the moved entity is not readable through R2")
		}
		requireVerdicts(t, "reader after the owner moved",
			gateVerdicts(t, moved, w.reader, graphs...), false, false)
		requireVerdicts(t, "holder after the owner moved",
			gateVerdicts(t, moved, w.owner, graphs...), true, true)
	})
}

// TestFleetEvidenceOverASharedEntityBindsTheRecordersOwner: a shared
// extracted entity (#700) is bound to the owner its recorder reads it
// through, so a recorder without the hidden first owner's label still sees
// its own evidence, and the hidden owner is never named in its binding.
func TestFleetEvidenceOverASharedEntityBindsTheRecordersOwner(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newExtractedEdgeWorld(t, store)
		body := skillBody("zeta", "zeta-cli")
		hidden := w.extract(w.ingest(sharedHiddenURI, body, nil))
		w.relabelSecret(sharedHiddenURI, body)
		attacker := w.f.labelIngester(t, "attacker")
		ownIngest := w.ingestAs(attacker, sharedAttackerURI, body, nil)
		own, err := w.extractAs(attacker, ownIngest)
		if err != nil {
			t.Fatal(err)
		}
		if len(own.EntityNodeIDs) == 0 || len(hidden.EntityNodeIDs) == 0 ||
			own.EntityNodeIDs[0] != hidden.EntityNodeIDs[0] {
			t.Fatal("fixture no longer shares entities")
		}
		entity := own.EntityNodeIDs[0]
		snapshot, err := w.f.base.Snapshot(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		w.f.clock.Set(snapshot.AsOf.Add(time.Second))
		attacker = w.f.labelIngester(t, "attacker")
		bindings, valid, err := w.client.NodeGate().BindGraphEvidence(attacker,
			shoal.ID(snapshot.ID), snapshot.AsOf,
			[]interaction.EvidenceReference{nodeEvidence(t, w.f.base, entity)})
		if err != nil || !valid {
			t.Fatalf("BindGraphEvidence: valid = %v, err = %v", valid, err)
		}
		binding, err := authorized.DecodeGraphEvidenceBinding(bindings[0])
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range binding.Entries() {
			if entry.DocumentID != ownIngest.Document.ID ||
				entry.RevisionID != ownIngest.Revision.ID {
				t.Fatalf("the binding names %s@%s, not the recorder's own owner",
					entry.DocumentID, entry.RevisionID)
			}
		}
		requireVerdicts(t, "the recorder", gateVerdicts(t, w.client, attacker,
			evidencelabels.Graph{NodeIDs: []shoal.ID{entity}, Binding: bindings[0]}), true)
	})
}

// TestLegacyFleetEvidenceIgnoresAnUnderstatedRecordTime is the review's
// repro, kept for good: open R_a at t0, secret R_b at t0+1m, an action at
// t0+90s whose executor reports an evidence snapshot time of t0, then a
// relabel to open R_c. A legacy record's time is executor-supplied and was
// not always checked against a corpus frontier, so the gate takes none:
// legacy evidence is held to every registered revision, closed to the
// non-holder whatever time the record claims. Evidence the same action
// records with a binding names R_b and is closed for the same reason.
func TestLegacyFleetEvidenceIgnoresAnUnderstatedRecordTime(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newBindingWorld(t, newFixture(t), store)
		const uri = "file:///fleet/understated.txt"
		t0 := w.f.clock.Now()
		at := func(offset time.Duration) {
			w.f.clock.Set(t0.Add(offset))
			w.holder = w.f.bindingDecision(t, "holder", "secret")
			w.reader = w.f.bindingDecision(t, "reader")
		}
		first, err := w.client.Ingest(w.as(w.holder), labelledSource(uri, "an open start", ""))
		if err != nil {
			t.Fatal(err)
		}
		at(time.Minute)
		if _, err := w.client.Ingest(w.as(w.holder),
			labelledSource(uri, "a secret middle", "secret")); err != nil {
			t.Fatal(err)
		}
		at(90 * time.Second)
		w.resnapshot()
		used := w.bindFleetEvidence(w.client, w.as(w.holder),
			nodeEvidence(t, w.f.base, first.Document.ID))[0]
		declassify, err := authorized.WithDeclassificationReason(
			w.as(w.holder), "ticket:SEC-705")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.client.Ingest(declassify,
			labelledSource(uri, "an open end", "")); err != nil {
			t.Fatal(err)
		}
		at(3 * time.Minute)
		// Not vacuous: the document is open to the reader now.
		if _, err := w.client.Document(w.as(w.reader), first.Document.ID, ""); err != nil {
			t.Fatalf("the relabelled document is not readable: %v", err)
		}
		requireVerdicts(t, "reader", gateVerdicts(t, w.client, w.as(w.reader),
			used.legacy(), used.bound()), false, false)
		requireVerdicts(t, "holder", gateVerdicts(t, w.client, w.as(w.holder),
			used.legacy(), used.bound()), true, true)
	})
}

// TestLegacyFleetEvidenceIsHeldToEveryRevision: evidence with no binding is
// held to every registered revision, so even a revision labelled after the
// record closes it (fail closed; availability only), while the same evidence
// recorded with a binding names only the open revision it used and stays
// open once the document is relabelled open again.
func TestLegacyFleetEvidenceIsHeldToEveryRevision(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newBindingWorld(t, newFixture(t), store)
		const uri = "file:///fleet/legacy.txt"
		first, err := w.client.Ingest(w.as(w.holder), labelledSource(uri, "an open start", ""))
		if err != nil {
			t.Fatal(err)
		}
		w.resnapshot()
		recorded := w.bindFleetEvidence(w.client, w.as(w.holder),
			nodeEvidence(t, w.f.base, first.Document.ID))[0]
		at := func(offset time.Duration) {
			w.f.clock.Set(recorded.asOf.Add(offset))
			w.holder = w.f.bindingDecision(t, "holder", "secret")
			w.reader = w.f.bindingDecision(t, "reader")
		}
		requireVerdicts(t, "reader before", gateVerdicts(t, w.client, w.as(w.reader),
			recorded.bound(), recorded.legacy()), true, true)
		at(time.Minute)
		if _, err := w.client.Ingest(w.as(w.holder),
			labelledSource(uri, "a secret middle", "secret")); err != nil {
			t.Fatal(err)
		}
		at(2 * time.Minute)
		declassify, err := authorized.WithDeclassificationReason(
			w.as(w.holder), "ticket:SEC-705")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.client.Ingest(declassify,
			labelledSource(uri, "an open end", "")); err != nil {
			t.Fatal(err)
		}
		at(3 * time.Minute)
		requireVerdicts(t, "reader", gateVerdicts(t, w.client, w.as(w.reader),
			recorded.bound(), recorded.legacy()), true, false)
		requireVerdicts(t, "holder", gateVerdicts(t, w.client, w.as(w.holder),
			recorded.bound(), recorded.legacy()), true, true)
	})
}

// TestACorruptBindingDeniesEvenWhereLegacyWouldAdmit: a binding that does not
// decode is denied, not treated as a legacy record, even for a holder and
// even when the same batch loads the document's history for a legacy
// reference that the holder is admitted by.
func TestACorruptBindingDeniesEvenWhereLegacyWouldAdmit(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newFleetWorld(t, store)
		w.relabel(w.client)
		for _, corrupt := range [][]byte{
			append(append([]byte(nil), w.closed.binding...), 0),
			{0xff},
			w.closed.binding[:len(w.closed.binding)-1],
		} {
			damaged := w.closed.bound()
			damaged.Binding = corrupt
			requireVerdicts(t, "holder", gateVerdicts(t, w.client, w.as(w.holder),
				w.closed.legacy(), damaged, w.closed.bound()), true, false, true)
		}
	})
}

// TestGraphEvidenceBindingDecodesEveryVersionEverWritten pins the exact bytes
// of every binding version a fleet record may hold. Fleet records keep their
// bindings for life, so a change to the encoding must add a version and keep
// decoding these.
func TestGraphEvidenceBindingDecodesEveryVersionEverWritten(t *testing.T) {
	for _, stored := range []struct {
		version string
		data    []byte
		entries []authorized.GraphBindingEntry
	}{{
		version: "1",
		data: []byte{
			1, 2, // version, entry count
			1, 1, 1, 'n', 1, 'd', 1, 'r', // node, document, "n", "d", "r"
			2, 4, 1, 'e', 0, 0, // edge, application, "e"
		},
		entries: []authorized.GraphBindingEntry{
			{Object: authorized.GraphBindingNode, ID: "n",
				Kind: authorized.RegistrationDocument, DocumentID: "d", RevisionID: "r"},
			{Object: authorized.GraphBindingEdge, ID: "e",
				Kind: authorized.RegistrationApplication},
		},
	}} {
		binding, err := authorized.DecodeGraphEvidenceBinding(stored.data)
		if err != nil {
			t.Fatalf("version %s no longer decodes: %v", stored.version, err)
		}
		entries := binding.Entries()
		if len(entries) != len(stored.entries) {
			t.Fatalf("version %s: %+v, want %+v", stored.version, entries, stored.entries)
		}
		for index := range entries {
			if entries[index] != stored.entries[index] {
				t.Fatalf("version %s: %+v, want %+v", stored.version, entries, stored.entries)
			}
		}
	}
	// The current encoder still writes version 1 exactly.
	binding, err := authorized.NewGraphEvidenceBinding(
		authorized.GraphBindingEntry{Object: authorized.GraphBindingEdge, ID: "e",
			Kind: authorized.RegistrationApplication},
		authorized.GraphBindingEntry{Object: authorized.GraphBindingNode, ID: "n",
			Kind: authorized.RegistrationDocument, DocumentID: "d", RevisionID: "r"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := binding.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{1, 2, 1, 1, 1, 'n', 1, 'd', 1, 'r', 2, 4, 1, 'e', 0, 0}; string(encoded) != string(want) {
		t.Fatalf("Encode = %v, want %v", encoded, want)
	}
}

// TestFleetBindingIsWhatTheCatalogNamedAtEitherRead: the resolver's binding
// is the union of the registrations the catalog named before the evidence
// was validated and after. Each read alone misses one of them.
func TestFleetBindingIsWhatTheCatalogNamedAtEitherRead(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newBindingWorld(t, newFixture(t), store)
		inner, ok := trustedSnapshotValidator(w.f.base).(authorized.EvidenceSnapshotValidator)
		if !ok {
			t.Fatal("the corpus does not validate evidence snapshots")
		}
		moving := &movingCatalog{
			PolicyStore: store, nodeID: w.closed.Document.ID,
			revisionID: "revision-registered-mid-recording",
		}
		validator := &relabelAfterValidation{inner: inner, after: func() {
			moving.moved.Store(true)
		}}
		selector := w.f.staticSelector(t, w.f.sourceA, w.f.policyA)
		recorder, err := authorized.NewClient(authorized.Config{
			Base:              w.f.base,
			InteractionWriter: trustedInteractionWriter(w.f.base),
			InteractionReader: trustedInteractionReader(w.f.base),
			SnapshotValidator: validator,
			FoldStore:         trustedFoldStore(w.f.base),
			Resolver:          w.f.authority.Resolver(),
			PolicySelector:    selector,
			PolicyStore:       moving,
			GenerationReader:  w.f.reader,
			Clock:             w.f.clock.Now,
		})
		if err != nil {
			t.Fatal(err)
		}
		recorded := w.bindFleetEvidence(recorder, w.as(w.holder),
			nodeEvidence(t, w.f.base, w.closed.Document.ID))[0]
		if !moving.moved.Load() {
			t.Fatal("the catalog did not move during the recording")
		}
		binding, err := authorized.DecodeGraphEvidenceBinding(recorded.binding)
		if err != nil {
			t.Fatal(err)
		}
		revisions := map[shoal.ID]bool{}
		for _, entry := range binding.Entries() {
			revisions[entry.RevisionID] = true
		}
		if len(revisions) != 2 || !revisions[moving.revisionID] ||
			!revisions[w.closed.Revision.ID] {
			t.Fatalf("recorded %+v, want both revisions", binding.Entries())
		}
	})
}

// TestBindGraphEvidenceRefusesWhatGraphEvidenceValidRefuses: the resolver
// validates the evidence as the record-time check always did, and records
// nothing for a refused reference.
func TestBindGraphEvidenceRefusesWhatGraphEvidenceValidRefuses(t *testing.T) {
	w := newBindingWorld(t, newFixture(t), authorized.NewMemoryPolicyStore())
	reference := nodeEvidence(t, w.f.base, w.closed.Document.ID)
	bindings, valid, err := w.client.NodeGate().BindGraphEvidence(w.as(w.holder),
		"snapshot-never-taken", w.snapshot.AsOf, []interaction.EvidenceReference{reference})
	if err != nil || valid || bindings != nil {
		t.Fatalf("an untrusted snapshot: bindings = %v, valid = %v, err = %v",
			bindings, valid, err)
	}
	reference.NodeIDs = []shoal.ID{""}
	if _, _, err := w.client.NodeGate().BindGraphEvidence(w.as(w.holder),
		shoal.ID(w.snapshot.ID), w.snapshot.AsOf,
		[]interaction.EvidenceReference{reference}); !shoal.IsErrorCode(
		err, shoal.ErrorInvalidArgument) {
		t.Fatalf("an empty node ID: err = %v, want invalid argument", err)
	}
	// A reference naming no graph member has nothing to bind.
	empty := interaction.EvidenceReference{}
	bindings, valid, err = w.client.NodeGate().BindGraphEvidence(w.as(w.holder),
		shoal.ID(w.snapshot.ID), w.snapshot.AsOf, []interaction.EvidenceReference{empty})
	if err != nil || !valid || len(bindings) != 1 || bindings[0] != nil {
		t.Fatalf("a reference with no members: bindings = %v, valid = %v, err = %v",
			bindings, valid, err)
	}
}

// TestGraphEvidenceBindingEncodingIsStrict: a stored binding decodes only
// exactly as Encode wrote it.
func TestGraphEvidenceBindingEncodingIsStrict(t *testing.T) {
	document := authorized.GraphBindingEntry{
		Object: authorized.GraphBindingNode, ID: "node-a",
		Kind: authorized.RegistrationDocument, DocumentID: "doc", RevisionID: "rev",
	}
	edge := authorized.GraphBindingEntry{
		Object: authorized.GraphBindingEdge, ID: "edge-a",
		Kind: authorized.RegistrationApplication,
	}
	binding, err := authorized.NewGraphEvidenceBinding(edge, document, edge)
	if err != nil {
		t.Fatal(err)
	}
	if entries := binding.Entries(); len(entries) != 2 ||
		entries[0] != document || entries[1] != edge {
		t.Fatalf("canonical entries %+v", entries)
	}
	encoded, err := binding.Encode()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := authorized.DecodeGraphEvidenceBinding(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := decoded.Encode(); string(again) != string(encoded) {
		t.Fatal("the binding does not round-trip")
	}
	if _, err := (authorized.GraphEvidenceBinding{}).Encode(); err == nil {
		t.Fatal("the empty binding encoded")
	}
	single, err := authorized.NewGraphEvidenceBinding(document)
	if err != nil {
		t.Fatal(err)
	}
	singleEncoded, _ := single.Encode()
	edgeOnly, _ := authorized.NewGraphEvidenceBinding(edge)
	edgeEncoded, _ := edgeOnly.Encode()
	// Two entries out of canonical order: edge first, then the node.
	reordered := append([]byte{1, 2}, edgeEncoded[2:]...)
	reordered = append(reordered, singleEncoded[2:]...)
	for name, data := range map[string][]byte{
		"empty input":     nil,
		"unknown version": append([]byte{2}, encoded[1:]...),
		"no entries":      {1, 0},
		"truncated":       encoded[:len(encoded)-1],
		"trailing byte":   append(append([]byte(nil), encoded...), 0),
		"out of order":    reordered,
		// A materialized node naming a revision: well formed, invalid entry.
		"invalid entry": {1, 1, 1, 3, 1, 'x', 1, 'd', 1, 'r'},
		"duplicated":    append([]byte{1, 2}, append(append([]byte(nil), singleEncoded[2:]...), singleEncoded[2:]...)...),
	} {
		if _, err := authorized.DecodeGraphEvidenceBinding(data); err == nil {
			t.Fatalf("%s decoded", name)
		}
	}
	for name, entry := range map[string]authorized.GraphBindingEntry{
		"no ID": {Object: authorized.GraphBindingNode,
			Kind: authorized.RegistrationMaterialized},
		"unknown object": {Object: 9, ID: "x", Kind: authorized.RegistrationMaterialized},
		"application node": {Object: authorized.GraphBindingNode, ID: "x",
			Kind: authorized.RegistrationApplication},
		"unknown kind": {Object: authorized.GraphBindingEdge, ID: "x", Kind: 9},
		"document with no revision": {Object: authorized.GraphBindingNode, ID: "x",
			Kind: authorized.RegistrationDocument, DocumentID: "doc"},
		"materialization identity as a revision": {Object: authorized.GraphBindingNode,
			ID: "x", Kind: authorized.RegistrationMaterialized,
			DocumentID: "materialization", RevisionID: "rev"},
	} {
		if _, err := authorized.NewGraphEvidenceBinding(entry); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}
