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

// Fleet graph evidence over a derived (latent) edge is decided as the
// interaction plane decides it (#744, the fleet half of #719): the edge is
// resolved through its trusted derived assertion, admitted when both of its
// endpoints are, and bound by its endpoints' revisions.

import (
	"context"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/explorer/evidencelabels"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// requireDerivedBinding: the binding names exactly the derived edge's two
// endpoints at the given revisions, and no entry for the edge itself.
func requireDerivedBinding(
	t *testing.T, evidence fleetEvidence, endpoints ...explorer.IngestResult,
) {
	t.Helper()
	binding, err := authorized.DecodeGraphEvidenceBinding(evidence.binding)
	if err != nil {
		t.Fatal(err)
	}
	want := map[shoal.ID]authorized.GraphBindingEntry{}
	for _, endpoint := range endpoints {
		want[endpoint.Document.ID] = authorized.GraphBindingEntry{
			Object: authorized.GraphBindingNode, ID: endpoint.Document.ID,
			Kind:       authorized.RegistrationDocument,
			DocumentID: endpoint.Document.ID, RevisionID: endpoint.Revision.ID,
		}
	}
	entries := binding.Entries()
	if len(entries) != len(want) {
		t.Fatalf("derived edge binding %+v, want %+v", entries, want)
	}
	for _, entry := range entries {
		if want[entry.ID] != entry {
			t.Fatalf("derived edge binding entry %+v, want %+v", entry, want[entry.ID])
		}
	}
}

// registeredThenDerived serves, while serving is set, one extra edge
// registration under an ID the corpus holds as a derived edge; clearing it
// removes the registration, so the ID resolves only as derived.
type registeredThenDerived struct {
	authorized.PolicyStore
	registration authorized.EdgeRegistration
	serving      *bool
}

func (s registeredThenDerived) Edges(
	ctx context.Context, edgeIDs []shoal.ID,
) (map[shoal.ID]authorized.EdgeRegistration, error) {
	resolved, err := s.PolicyStore.Edges(ctx, edgeIDs)
	if err != nil || !*s.serving {
		return resolved, err
	}
	for _, id := range edgeIDs {
		if id == s.registration.Edge.ID {
			resolved[id] = s.registration
		}
	}
	return resolved, nil
}

// TestFleetEdgeRecordedRegisteredThenDerivedDenies: evidence recorded while
// an edge ID resolved a registration carries that registration's edge
// entry. Once the registration is gone and the ID resolves only as a derived
// edge, the entry's rule could no longer be enforced, so the evidence is
// denied, to the holder too. Unreachable while derived and registered IDs
// are disjoint; the store is injected to reach it. Not vacuous: the same
// evidence is visible while the registration is served, and evidence bound
// after the removal is visible.
func TestFleetEdgeRecordedRegisteredThenDerivedDenies(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newDerivedWorld(t, store)
		w.connect(w.client, "edge-derived-fleet-shadow",
			w.source.Document.ID, w.open.Document.ID)
		shadowID := w.edgeID(w.client, "edge-derived-fleet-shadow")
		registered, err := store.Edges(context.Background(), []shoal.ID{shadowID})
		if err != nil {
			t.Fatal(err)
		}
		registration, ok := registered[shadowID]
		if !ok || registration.Kind != authorized.RegistrationApplication {
			t.Fatalf("no application registration for the shadow edge: %+v", registered)
		}
		registration.Edge.ID = w.toOpen.ID
		serving := true
		client := w.newClient(registeredThenDerived{
			PolicyStore: store, registration: registration, serving: &serving,
		})
		w.resnapshot()
		holder := w.as(w.holder)
		recorded := w.bindFleetEvidence(client, holder, w.evidence(t, w.toOpen))[0]
		binding, err := authorized.DecodeGraphEvidenceBinding(recorded.binding)
		if err != nil {
			t.Fatal(err)
		}
		hasEdgeEntry := false
		for _, entry := range binding.Entries() {
			hasEdgeEntry = hasEdgeEntry || (entry.Object == authorized.GraphBindingEdge &&
				entry.ID == w.toOpen.ID && entry.Kind == authorized.RegistrationApplication)
		}
		if !hasEdgeEntry {
			t.Fatalf("the binding has no application entry for the edge: %+v", binding.Entries())
		}
		requireVerdicts(t, "while registered", gateVerdicts(t, client, holder,
			recorded.bound()), true)
		serving = false
		later := w.bindFleetEvidence(client, holder, w.evidence(t, w.toOpen))[0]
		requireDerivedBinding(t, later, w.source, w.open)
		requireVerdicts(t, "once only derived", gateVerdicts(t, client, holder,
			recorded.bound(), later.bound()), false, true)
	})
}

// pathBlindValidator is the trusted corpus as snapshot validator, except
// that it accepts every evidence path.
type pathBlindValidator struct{ *explorer.Explorer }

func (pathBlindValidator) ValidateEvidenceSnapshot(
	context.Context, shoal.ID, time.Time, []shoal.ID, []shoal.ID,
	[]interaction.EvidenceReference,
) error {
	return nil
}

func (w *derivedWorld) clientWithSnapshotValidator(
	t *testing.T, validator authorized.SnapshotValidator,
) *authorized.Client {
	t.Helper()
	selector := w.f.staticSelector(t, w.f.sourceA, w.f.policyA)
	client, err := authorized.NewClient(authorized.Config{
		Base:                   w.f.base,
		VectorScorer:           trustedVectorScorer(w.f.base),
		InteractionWriter:      trustedInteractionWriter(w.f.base),
		InteractionReader:      trustedInteractionReader(w.f.base),
		SnapshotValidator:      validator,
		DerivedAssertionReader: trustedDerivedAssertionReader(w.f.base),
		FoldStore:              trustedFoldStore(w.f.base),
		Resolver:               w.f.authority.Resolver(),
		PolicySelector:         selector,
		EdgePolicySelector:     selector,
		PolicyStore:            w.store,
		GenerationReader:       w.f.reader,
		Clock:                  w.f.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// servedMaterialized serves one node's registration as materialized, with
// no document revision, keeping its rule.
type servedMaterialized struct {
	authorized.PolicyStore
	nodeID shoal.ID
}

func (s servedMaterialized) Nodes(
	ctx context.Context, nodeIDs []shoal.ID,
) (map[shoal.ID]authorized.NodeRegistration, error) {
	resolved, err := s.PolicyStore.Nodes(ctx, nodeIDs)
	if err != nil {
		return nil, err
	}
	if registration, ok := resolved[s.nodeID]; ok {
		registration.Kind = authorized.RegistrationMaterialized
		registration.DocumentID, registration.RevisionID = "", ""
		resolved[s.nodeID] = registration
	}
	return resolved, nil
}

// neverDerived is fleet evidence over a derived edge the corpus has never
// held, between two nodes the reader may read.
func (w *derivedWorld) neverDerived() evidencelabels.Graph {
	return evidencelabels.Graph{
		NodeIDs: []shoal.ID{w.source.Document.ID, w.open.Document.ID},
		EdgeIDs: []shoal.ID{"assertion:derived-never-existed"},
	}
}

// TestFleetEvidenceOverAVisibleDerivedEdge is the issue's repro: the
// reader, shown a derived edge and its assertion, records fleet evidence
// over it, and the evidence is visible to the reader and the holder, bound
// and unbound. Before #744 it could not be recorded (BindGraphEvidence
// refused it as an unregistered edge) and was invisible to everyone.
func TestFleetEvidenceOverAVisibleDerivedEdge(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newDerivedWorld(t, store)
		valid, err := w.client.NodeGate().GraphEvidenceValid(w.as(w.reader),
			shoal.ID(w.snapshot.ID), w.snapshot.AsOf,
			[]interaction.EvidenceReference{w.evidence(t, w.toOpen)})
		if err != nil || !valid {
			t.Fatalf("GraphEvidenceValid over the visible derived edge = %v, %v", valid, err)
		}
		recorded := w.bindFleetEvidence(w.client, w.as(w.reader), w.evidence(t, w.toOpen))
		requireDerivedBinding(t, recorded[0], w.source, w.open)
		graphs := []evidencelabels.Graph{recorded[0].bound(), recorded[0].legacy()}
		requireVerdicts(t, "reader", gateVerdicts(t, w.client, w.as(w.reader), graphs...),
			true, true)
		requireVerdicts(t, "holder", gateVerdicts(t, w.client, w.as(w.holder), graphs...),
			true, true)
	})
}

// TestFleetEvidenceOverAHiddenDerivedEdgeLooksLikeAbsence: the holder's
// fleet evidence over the derived edge to the hidden target is refused to the
// reader exactly as evidence over a derived edge that never existed: a false
// verdict and no error, bound or unbound. So is a path that names the open
// target in the hidden edge's place, which every node of is readable, and a
// trusted assertion whose derivation was computed to or from the hidden
// target. At record time the misrouted and tampered paths are invalid, as
// one that never existed is. The holder sees the true path.
func TestFleetEvidenceOverAHiddenDerivedEdgeLooksLikeAbsence(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newDerivedWorld(t, store)
		recorded := w.bindFleetEvidence(w.client, w.as(w.holder), w.evidence(t, w.toHidden))
		requireDerivedBinding(t, recorded[0], w.source, w.hidden)
		misrouted := evidencelabels.Graph{
			NodeIDs: []shoal.ID{w.source.Document.ID, w.open.Document.ID},
			EdgeIDs: []shoal.ID{w.toHidden.ID},
		}
		misroutedBound := misrouted
		misroutedBound.Binding = recorded[0].binding
		requireVerdicts(t, "reader", gateVerdicts(t, w.client, w.as(w.reader),
			recorded[0].bound(), recorded[0].legacy(), misrouted, misroutedBound,
			w.neverDerived()),
			false, false, false, false, false)
		requireVerdicts(t, "holder", gateVerdicts(t, w.client, w.as(w.holder),
			recorded[0].bound(), recorded[0].legacy(), w.neverDerived()),
			true, true, false)

		gate := w.client.NodeGate()
		bind := func(client *authorized.Client, reference interaction.EvidenceReference) (bool, error) {
			bindings, valid, err := client.NodeGate().BindGraphEvidence(w.as(w.reader),
				shoal.ID(w.snapshot.ID), w.snapshot.AsOf,
				[]interaction.EvidenceReference{reference})
			if valid != (bindings != nil) {
				t.Fatalf("valid = %v with bindings %v", valid, bindings)
			}
			return valid, err
		}
		for name, reference := range map[string]interaction.EvidenceReference{
			"never existed": w.fabricatedEvidence(t,
				"assertion:derived-never-existed", w.open.Document.ID),
			"misrouted": w.fabricatedEvidence(t, w.toHidden.ID, w.open.Document.ID),
		} {
			if valid, err := bind(w.client, reference); err != nil || valid {
				t.Fatalf("%s: BindGraphEvidence = %v, %v; want invalid", name, valid, err)
			}
			if valid, err := gate.GraphEvidenceValid(w.as(w.reader),
				shoal.ID(w.snapshot.ID), w.snapshot.AsOf,
				[]interaction.EvidenceReference{reference}); err != nil || valid {
				t.Fatalf("%s: GraphEvidenceValid = %v, %v; want invalid", name, valid, err)
			}
		}

		// The path must join as the trusted assertion builds the edge even
		// where the snapshot validator would not catch it: through a
		// validator that accepts every path, the misrouted path is still
		// invalid and the true one valid.
		pathBlind := w.clientWithSnapshotValidator(t, pathBlindValidator{w.f.base})
		if valid, err := bind(pathBlind,
			w.fabricatedEvidence(t, w.toHidden.ID, w.open.Document.ID)); err != nil || valid {
			t.Fatalf("misrouted, path-blind validator: BindGraphEvidence = %v, %v", valid, err)
		}
		if valid, err := bind(pathBlind, w.evidence(t, w.toHidden)); err != nil || !valid {
			t.Fatalf("true path, path-blind validator: BindGraphEvidence = %v, %v", valid, err)
		}

		// Trusted assertions between two readable nodes whose derivation
		// covers the hidden target do not admit the edge.
		trusted, err := w.f.base.DerivedAssertions(context.Background(),
			[]shoal.ID{w.toOpen.ID})
		if err != nil {
			t.Fatal(err)
		}
		open := trusted[w.toOpen.ID]
		overHidden := derivedAssertionOver(t, open, w.source.Document.ID, w.hidden.Document.ID)
		fromHidden := derivedAssertionOver(t, open, w.hidden.Document.ID, w.open.Document.ID)
		tampered := w.tamperedClient(t, map[shoal.ID]ontology.Assertion{
			overHidden.ID(): overHidden, fromHidden.ID(): fromHidden,
		})
		for name, edgeID := range map[string]shoal.ID{
			"derivation to the hidden target":   overHidden.ID(),
			"derivation from the hidden target": fromHidden.ID(),
		} {
			planted := evidencelabels.Graph{
				NodeIDs: []shoal.ID{w.source.Document.ID, w.open.Document.ID},
				EdgeIDs: []shoal.ID{edgeID},
			}
			requireVerdicts(t, name, gateVerdicts(t, tampered, w.as(w.holder), planted), false)
			if valid, err := bind(tampered,
				w.fabricatedEvidence(t, edgeID, w.open.Document.ID)); err != nil || valid {
				t.Fatalf("%s: BindGraphEvidence = %v, %v; want invalid", name, valid, err)
			}
		}
		// An endpoint off the revision path is still decided by its current
		// rule: served as a materialized node, the hidden target has no
		// history to hold the misrouted path to, and its own rule refuses
		// it.
		offPath := w.newClient(servedMaterialized{PolicyStore: store, nodeID: w.hidden.Document.ID})
		requireVerdicts(t, "an endpoint off the revision path",
			gateVerdicts(t, offPath, w.as(w.reader), misrouted, evidencelabels.Graph{
				NodeIDs: []shoal.ID{w.source.Document.ID, w.open.Document.ID},
				EdgeIDs: []shoal.ID{w.toOpen.ID},
			}), false, true)

		// The control: the tampered client still admits the genuine edge.
		requireVerdicts(t, "tampered client, genuine edge",
			gateVerdicts(t, tampered, w.as(w.reader), w.neverDerived(), evidencelabels.Graph{
				NodeIDs: []shoal.ID{w.source.Document.ID, w.open.Document.ID},
				EdgeIDs: []shoal.ID{w.toOpen.ID},
			}), false, true)
	})
}

// TestFleetDerivedEdgeEvidenceIsHeldToItsEndpointRevisions: fleet evidence
// the holder recorded over the derived edge to the labelled target R1 stays
// closed to the reader after the target is relabelled, bound or unbound,
// because its binding names R1 through the endpoint entries; a binding
// missing either endpoint covers nothing and denies even the holder. Not
// vacuous: evidence over the same derived edge recorded after the relabel
// names R2, and the reader sees it.
func TestFleetDerivedEdgeEvidenceIsHeldToItsEndpointRevisions(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newDerivedWorld(t, store)
		before := w.bindFleetEvidence(w.client, w.as(w.holder), w.evidence(t, w.toHidden))[0]
		requireDerivedBinding(t, before, w.source, w.hidden)

		ctx, err := authorized.WithDeclassificationReason(w.as(w.holder), "ticket:SEC-744")
		if err != nil {
			t.Fatal(err)
		}
		relabelled, err := w.client.Ingest(ctx,
			labelledSource(derivedHiddenURI, "the derived target, declassified", ""))
		if err != nil {
			t.Fatal(err)
		}
		if relabelled.Document.ID != w.hidden.Document.ID ||
			relabelled.Revision.ID == w.hidden.Revision.ID {
			t.Fatalf("relabel did not make a new revision of the target: %+v", relabelled)
		}
		w.resnapshot()
		if after := w.derivedEdge(t, w.hidden.Document.ID); after.ID != w.toHidden.ID {
			t.Fatalf("the relabel changed the derived edge: %s, was %s", after.ID, w.toHidden.ID)
		}
		if !w.readerShown(t, w.toHidden.ID) {
			t.Fatal("the relabel did not open the derived edge to the reader")
		}

		requireVerdicts(t, "reader, recorded over R1", gateVerdicts(t, w.client, w.as(w.reader),
			before.bound(), before.legacy()), false, false)
		requireVerdicts(t, "holder, recorded over R1", gateVerdicts(t, w.client, w.as(w.holder),
			before.bound(), before.legacy()), true, true)
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
		requireVerdicts(t, "holder, an endpoint uncovered", gateVerdicts(t, w.client, w.as(w.holder),
			rebind(t, before, without(w.hidden.Document.ID)),
			rebind(t, before, without(w.source.Document.ID))), false, false)

		later := w.bindFleetEvidence(w.client, w.as(w.holder), w.evidence(t, w.toHidden))[0]
		requireDerivedBinding(t, later, w.source, relabelled)
		requireVerdicts(t, "reader, recorded over R2", gateVerdicts(t, w.client, w.as(w.reader),
			later.bound(), before.bound()), true, false)

		// The derived edge's endpoints are members even when the graph's
		// own nodes do not name them: a path naming the open target in the
		// relabelled target's place is held to the target's history when
		// unbound, and a binding that covers only the nodes it names (here
		// the open edge's) leaves the target uncovered. Both deny, though
		// every current rule now admits the reader.
		openLater := w.bindFleetEvidence(w.client, w.as(w.holder), w.evidence(t, w.toOpen))[0]
		misrouted := evidencelabels.Graph{
			NodeIDs: []shoal.ID{w.source.Document.ID, w.open.Document.ID},
			EdgeIDs: []shoal.ID{w.toHidden.ID},
		}
		misroutedBound := misrouted
		misroutedBound.Binding = openLater.binding
		requireVerdicts(t, "reader, misrouted after the relabel",
			gateVerdicts(t, w.client, w.as(w.reader),
				misrouted, misroutedBound, openLater.bound()), false, false, true)
	})
}

// TestUnavailableDerivedReaderDoesNotRevealHiddenFleetEdges: with the
// trusted derived assertion reader failing, the fleet gate fails the same
// way for a registered edge the reader may not read as for an edge that
// never existed, at read and at record time. Consulting the reader only for
// unregistered edges would answer false for the hidden edge and fail the
// other, telling the reader the hidden edge exists.
func TestUnavailableDerivedReaderDoesNotRevealHiddenFleetEdges(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newDerivedWorld(t, store)
		w.connect(w.client, "edge-derived-fleet-hidden-application",
			w.source.Document.ID, w.hidden.Document.ID)
		w.resnapshot()
		hiddenEdge := graph.Edge{
			ID:   w.edgeID(w.client, "edge-derived-fleet-hidden-application"),
			From: w.source.Document.ID, To: w.hidden.Document.ID,
		}
		hidden := exactAuthorizedGraphEvidenceWith(t, w.f.base, hiddenEdge, nil)
		never := w.fabricatedEvidence(t, "assertion:derived-never-existed", w.open.Document.ID)
		asGraph := func(reference interaction.EvidenceReference) []evidencelabels.Graph {
			return []evidencelabels.Graph{{NodeIDs: reference.NodeIDs, EdgeIDs: reference.EdgeIDs}}
		}
		// The control: through the working reader the hidden edge is a
		// plain false, and valid to record.
		requireVerdicts(t, "working reader", gateVerdicts(t, w.client, w.as(w.reader),
			asGraph(hidden)...), false)

		client := w.clientWithDerivedReader(t, failingDerivedAssertions{})
		gate := client.NodeGate()
		_, neverErr := gate.GraphsVisibleToReader(w.as(w.reader), asGraph(never))
		_, hiddenErr := gate.GraphsVisibleToReader(w.as(w.reader), asGraph(hidden))
		if !shoal.IsErrorCode(neverErr, shoal.ErrorUnavailable) {
			t.Fatalf("read, a never-existing edge = %v, want unavailable", neverErr)
		}
		if hiddenErr == nil || hiddenErr.Error() != neverErr.Error() {
			t.Fatalf("read, a hidden registered edge = %v, but a never-existing one = %v",
				hiddenErr, neverErr)
		}
		bind := func(reference interaction.EvidenceReference) error {
			_, _, err := gate.BindGraphEvidence(w.as(w.reader),
				shoal.ID(w.snapshot.ID), w.snapshot.AsOf,
				[]interaction.EvidenceReference{reference})
			return err
		}
		neverErr, hiddenErr = bind(never), bind(hidden)
		if !shoal.IsErrorCode(neverErr, shoal.ErrorUnavailable) {
			t.Fatalf("record, a never-existing edge = %v, want unavailable", neverErr)
		}
		if hiddenErr == nil || hiddenErr.Error() != neverErr.Error() {
			t.Fatalf("record, a hidden registered edge = %v, but a never-existing one = %v",
				hiddenErr, neverErr)
		}
	})
}
