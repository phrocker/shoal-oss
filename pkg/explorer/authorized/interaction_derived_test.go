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

// A session touching a derived (latent) edge is authorized as reads admit
// the edge (#719): by its trusted derived assertion and both endpoints. A
// reader shown the edge records over it and reads the record back; one who
// may not read an endpoint is refused exactly as for an edge that never
// existed.

import (
	"context"
	"fmt"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	derivedSourceURI = "file:///derived/source.txt"
	derivedOpenURI   = "file:///derived/open-target.txt"
	derivedHiddenURI = "file:///derived/hidden-target.txt"

	derivedNeverSession shoal.ID = "interaction.session_derived_never_written"
)

// derivedWorld: the holder of "secret" ingests a source, an open target and
// a target labelled "secret", and a latent link joins the source to each.
// The reader may read the source and the open target only.
type derivedWorld struct {
	*citationWorld
	source, open, hidden explorer.IngestResult
	// toOpen and toHidden are the derived edges source -> open and
	// source -> hidden, as the base graph builds them.
	toOpen, toHidden graph.Edge
}

func newDerivedWorld(t *testing.T, store authorized.PolicyStore) *derivedWorld {
	t.Helper()
	w := &derivedWorld{citationWorld: &citationWorld{
		bindingWorld: newBindingWorld(t, newFixture(t), store),
	}}
	holder := w.as(w.holder)
	var err error
	for _, document := range []struct {
		into          *explorer.IngestResult
		uri, contents string
		label         string
	}{
		{&w.source, derivedSourceURI, "the derived source", ""},
		{&w.open, derivedOpenURI, "the open derived target", ""},
		{&w.hidden, derivedHiddenURI, "the hidden derived target", "secret"},
	} {
		*document.into, err = w.client.Ingest(holder,
			labelledSource(document.uri, document.contents, document.label))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.f.base.PutLatentLinkCells(w.f.admin(t), []explorer.LatentLinkCell{
		authorizedLatentCell(w.source.Document.ID, w.open.Document.ID, 1),
		authorizedLatentCell(w.source.Document.ID, w.hidden.Document.ID, 2),
	}); err != nil {
		t.Fatal(err)
	}
	w.resnapshot()
	w.toOpen = w.derivedEdge(t, w.open.Document.ID)
	w.toHidden = w.derivedEdge(t, w.hidden.Document.ID)
	// The premise of #719: neither edge has a policy-store registration.
	registered, err := store.Edges(context.Background(),
		[]shoal.ID{w.toOpen.ID, w.toHidden.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(registered) != 0 {
		t.Fatalf("derived edges are registered: %+v", registered)
	}
	// And the reader is shown the open edge and its assertion, not the
	// hidden one.
	shown, err := w.client.BoundedNeighborhood(w.as(w.reader),
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{w.source.Document.ID},
			Depth:   1, Fanout: 8, MaxNodes: 8, MaxScannedEdges: 64,
			EdgeTypes: []string{authorizedLatentEdgeType(t)},
			Direction: explorer.GraphDirectionOutgoing,
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(shown.Neighborhood.Edges) != 1 ||
		shown.Neighborhood.Edges[0].ID != w.toOpen.ID ||
		len(shown.Neighborhood.Assertions) != 1 ||
		shown.Neighborhood.Assertions[0].ID() != w.toOpen.ID {
		t.Fatalf("the reader is not shown exactly the open derived edge: %+v",
			shown.Neighborhood)
	}
	return w
}

// derivedEdge is the base graph's derived edge from the source to target.
func (w *derivedWorld) derivedEdge(t *testing.T, target shoal.ID) graph.Edge {
	t.Helper()
	around, err := w.f.base.Neighborhood(context.Background(),
		explorer.NeighborhoodRequest{
			NodeIDs: []shoal.ID{w.source.Document.ID}, Depth: 1,
			EdgeTypes: []string{authorizedLatentEdgeType(t)},
		})
	if err != nil {
		t.Fatal(err)
	}
	for _, edge := range around.Edges {
		if edge.To == target {
			return edge
		}
	}
	t.Fatalf("no derived edge to %s", target)
	return graph.Edge{}
}

func derivedCitation(edgeID shoal.ID) []interaction.AssertionReference {
	return []interaction.AssertionReference{{
		AssertionID: edgeID, EdgeID: edgeID, Origin: ontology.AssertionDerived,
	}}
}

// derivedSession is a session as decision seeded with graph evidence.
func (w *derivedWorld) derivedSession(
	t *testing.T, decision auth.Decision, id shoal.ID,
	evidence interaction.EvidenceReference,
) interaction.Session {
	t.Helper()
	return w.citationSession(t, decision, id, evidence)
}

func (w *derivedWorld) evidence(
	t *testing.T, edge graph.Edge,
) interaction.EvidenceReference {
	t.Helper()
	return exactAuthorizedGraphEvidenceWith(t, w.f.base, edge, derivedCitation(edge.ID))
}

// fabricatedEvidence is a well-formed graph path over an edge from the
// source to target that the corpus has never held.
func (w *derivedWorld) fabricatedEvidence(
	t *testing.T, edgeID, target shoal.ID,
) interaction.EvidenceReference {
	t.Helper()
	around, err := w.f.base.Neighborhood(context.Background(),
		explorer.NeighborhoodRequest{
			NodeIDs: []shoal.ID{w.source.Document.ID}, Depth: 1,
		})
	if err != nil {
		t.Fatal(err)
	}
	nodes := map[shoal.ID]graph.Node{}
	for _, node := range around.Nodes {
		nodes[node.ID] = node
	}
	edge := w.toHidden
	edge.ID, edge.To = edgeID, target
	anchor, err := inference.NewGraphAnchorWithAssertions(graph.Path{
		Nodes: []graph.Node{nodes[w.source.Document.ID], nodes[target]},
		Edges: []graph.Edge{edge},
	}, derivedCitation(edgeID))
	if err != nil {
		t.Fatal(err)
	}
	reference, err := anchor.EvidenceReference()
	if err != nil {
		t.Fatal(err)
	}
	return reference
}

// requireReadable: decision reads the session through every read path.
func (w *derivedWorld) requireReadable(
	t *testing.T, client *authorized.Client, decision auth.Decision, id shoal.ID,
) {
	t.Helper()
	ctx := w.as(decision)
	if _, err := client.InteractionRecord(ctx, id); err != nil {
		t.Fatalf("InteractionRecord(%s): %v", id, err)
	}
	if _, err := client.Interaction(ctx, id); err != nil {
		t.Fatalf("Interaction(%s): %v", id, err)
	}
	if _, err := client.InteractionSubgraph(ctx, id); err != nil {
		t.Fatalf("InteractionSubgraph(%s): %v", id, err)
	}
	records, err := client.InteractionRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !listsSession(records, id) {
		t.Fatalf("InteractionRecords omits %s", id)
	}
}

// requireRefused: decision is refused the session exactly as one never
// written, through every read path.
func (w *derivedWorld) requireRefused(
	t *testing.T, client *authorized.Client, decision auth.Decision, id shoal.ID,
	when string,
) {
	t.Helper()
	ctx := w.as(decision)
	_, never := client.InteractionRecord(ctx, derivedNeverSession)
	_, err := client.InteractionRecord(ctx, id)
	assertSameRefusal(t, when+": InteractionRecord", err, never)
	_, never = client.Interaction(ctx, derivedNeverSession)
	_, err = client.Interaction(ctx, id)
	assertSameRefusal(t, when+": Interaction", err, never)
	_, never = client.InteractionSubgraph(ctx, derivedNeverSession)
	_, err = client.InteractionSubgraph(ctx, id)
	assertSameRefusal(t, when+": InteractionSubgraph", err, never)
	records, err := client.InteractionRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if listsSession(records, id) {
		t.Fatalf("%s: InteractionRecords lists %s", when, id)
	}
}

// TestRecordingOverAVisibleDerivedEdge is the issue's repro: the reader,
// shown a derived edge and its assertion, records a session over it through
// every recording path and reads it back.
//
// The edge's target is labelled "secret" in the base but registered, as a
// pre-#570 build did, under its bare source rule. The label migration then
// tightens that rule in the catalog alone. (Re-ingesting the target would
// change its node, which the session's graph evidence anchors, and close the
// session to everyone; the migration leaves the base and the session valid,
// so only authorization can refuse it.) Afterwards the reader is refused
// the session exactly as one never written, and the holder still reads it.
func TestRecordingOverAVisibleDerivedEdge(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		legacy := &legacyStore{PolicyStore: store}
		w := newDerivedWorld(t, legacy)
		legacy.setStrip(true)
		migrated, err := w.client.Ingest(w.as(w.holder), labelledSource(
			"file:///derived/migrated-target.txt",
			"the derived target labelled the old way", "secret"))
		if err != nil {
			t.Fatal(err)
		}
		legacy.setStrip(false)
		if err := w.f.base.PutLatentLinkCells(w.f.admin(t), []explorer.LatentLinkCell{
			authorizedLatentCell(w.source.Document.ID, migrated.Document.ID, 3),
		}); err != nil {
			t.Fatal(err)
		}
		w.resnapshot()
		edge := w.derivedEdge(t, migrated.Document.ID)
		if !w.readerShown(t, edge.ID) {
			t.Fatal("the reader is not shown the legacy-labelled derived edge")
		}
		ids := make([]shoal.ID, 0, len(recordingPaths))
		for index, path := range recordingPaths {
			id := shoal.ID(fmt.Sprintf("interaction.session_derived_open_%d", index))
			reader := w.pathDecision(t, path, "reader")
			session := w.derivedSession(t, reader, id, w.evidence(t, edge))
			if err := path.record(w.client, w.as(reader), session); err != nil {
				t.Fatalf("%s: recording over the visible derived edge: %v",
					path.name, err)
			}
			ids = append(ids, id)
		}
		for _, id := range ids {
			w.requireReadable(t, w.client, w.reader, id)
		}
		if record := migrate(t, w.client); record.Tightened == 0 {
			t.Fatalf("the migration tightened nothing: %+v", record)
		}
		w.resnapshot()
		if w.readerShown(t, edge.ID) {
			t.Fatal("the migration did not close the derived edge to the reader")
		}
		for _, id := range ids {
			w.requireRefused(t, w.client, w.reader, id, "after the migration")
			w.requireReadable(t, w.client, w.holder, id)
		}
	})
}

// readerShown reports whether the reader is shown the derived edge from
// the source.
func (w *derivedWorld) readerShown(t *testing.T, edgeID shoal.ID) bool {
	t.Helper()
	shown, err := w.client.BoundedNeighborhood(w.as(w.reader),
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{w.source.Document.ID},
			Depth:   1, Fanout: 8, MaxNodes: 8, MaxScannedEdges: 64,
			EdgeTypes: []string{authorizedLatentEdgeType(t)},
			Direction: explorer.GraphDirectionOutgoing,
		})
	if err != nil {
		t.Fatal(err)
	}
	return hasEdge(shown.Neighborhood, edgeID)
}

// TestRecordingOverAHiddenDerivedEdgeIsNeverWritten: the reader, who may
// not read the hidden target, records over the derived edge to it: along
// its true path, and along a path that names the open target instead, so
// that every touched node is one the reader may read. Both are refused
// exactly as an edge that never existed, through every recording path, and
// nothing is written; the holder records the true path.
func TestRecordingOverAHiddenDerivedEdgeIsNeverWritten(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newDerivedWorld(t, store)
		hidden := w.evidence(t, w.toHidden)
		misrouted := w.fabricatedEvidence(t, w.toHidden.ID, w.open.Document.ID)
		never := w.fabricatedEvidence(t,
			"assertion:derived-never-existed", w.open.Document.ID)
		for index, path := range recordingPaths {
			id := func(kind string) shoal.ID {
				return shoal.ID(fmt.Sprintf("interaction.session_derived_%s_%d", kind, index))
			}
			reader := w.pathDecision(t, path, "reader")
			neverErr := path.record(w.client, w.as(reader),
				w.derivedSession(t, reader, id("never"), never))
			if !shoal.IsErrorCode(neverErr, shoal.ErrorNotFound) {
				t.Fatalf("%s: a never-existing edge = %v, want not_found",
					path.name, neverErr)
			}
			for kind, evidence := range map[string]interaction.EvidenceReference{
				"hidden": hidden, "misrouted": misrouted,
			} {
				err := path.record(w.client, w.as(reader),
					w.derivedSession(t, reader, id(kind), evidence))
				assertSameRefusal(t, path.name+": the "+kind+" derived edge", err, neverErr)
				requireNotPersisted(t, w.f.base, id(kind))
			}
			requireNotPersisted(t, w.f.base, id("never"))
			holder := w.pathDecision(t, path, "holder", "secret")
			if err := path.record(w.client, w.as(holder),
				w.derivedSession(t, holder, id("holder"), hidden)); err != nil {
				t.Fatalf("%s: the holder recording the same evidence: %v", path.name, err)
			}
		}
	})
}

// TestDerivedEdgeSessionIsHeldToItsEndpointRevisions: a session over a
// derived edge is bound (#695) to the revisions of both of the edge's
// endpoints, so a later reader is held to the revisions the session saw.
func TestDerivedEdgeSessionIsHeldToItsEndpointRevisions(t *testing.T) {
	const session shoal.ID = "interaction.session_derived_bound"
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newDerivedWorld(t, store)
		path := recordingPaths[0]
		holder := w.pathDecision(t, path, "holder", "secret")
		if err := path.record(w.client, w.as(holder), w.derivedSession(
			t, holder, session, w.evidence(t, w.toHidden))); err != nil {
			t.Fatal(err)
		}
		bound, err := store.InteractionRevisions(context.Background(),
			[]shoal.ID{session})
		if err != nil {
			t.Fatal(err)
		}
		for _, endpoint := range []explorer.IngestResult{w.source, w.hidden} {
			want := authorized.RevisionReference{
				DocumentID: endpoint.Document.ID, RevisionID: endpoint.Revision.ID,
			}
			found := false
			for _, reference := range bound[session] {
				found = found || reference == want
			}
			if !found {
				t.Fatalf("binding %v omits the endpoint revision %+v",
					bound[session], want)
			}
		}
		w.requireReadable(t, w.client, w.holder, session)
		w.requireRefused(t, w.client, w.reader, session, "the reader")
	})
}

func hasEdge(neighborhood explorer.Neighborhood, edgeID shoal.ID) bool {
	for _, edge := range neighborhood.Edges {
		if edge.ID == edgeID {
			return true
		}
	}
	return false
}

// TestDerivedEdgeSessionTranslatesItsOutputLabels: a session over the
// derived edge to the labelled target, stamped with the target's free-form
// label as the chat path stamps it, has the label translated over the
// touched nodes and the edge's endpoints (#570). A derived edge does not
// leave the translation incomplete, which would keep the free-form term and
// hide the session from everyone: the holder reads it back and the reader
// does not.
func TestDerivedEdgeSessionTranslatesItsOutputLabels(t *testing.T) {
	const session shoal.ID = "interaction.session_derived_labelled"
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newDerivedWorld(t, store)
		path := recordingPaths[0]
		holder := w.pathDecision(t, path, "holder", "secret")
		recorded := w.derivedSession(t, holder, session, w.evidence(t, w.toHidden))
		recorded.RequiredVisibility = []string{"secret"}
		if err := path.record(w.client, w.as(holder), recorded); err != nil {
			t.Fatal(err)
		}
		w.requireReadable(t, w.client, w.holder, session)
		w.requireRefused(t, w.client, w.reader, session, "the reader")
	})
}

// tamperedDerivedAssertions returns, besides the corpus's own, assertions
// the test plants under chosen IDs.
type tamperedDerivedAssertions struct {
	authorized.DerivedAssertionReader
	planted map[shoal.ID]ontology.Assertion
}

func (r tamperedDerivedAssertions) DerivedAssertions(
	ctx context.Context, ids []shoal.ID,
) (map[shoal.ID]ontology.Assertion, error) {
	found, err := r.DerivedAssertionReader.DerivedAssertions(ctx, ids)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if assertion, ok := r.planted[id]; ok {
			found[id] = assertion
		}
	}
	return found, nil
}

// derivedAssertionOver is the open edge's derived assertion with its
// derivation recomputed between source and target instead.
func derivedAssertionOver(
	t *testing.T, like ontology.Assertion, source, target shoal.ID,
) ontology.Assertion {
	t.Helper()
	original, ok := like.Evidence()[0].Derivation()
	if !ok {
		t.Fatal("fixture assertion has no derivation")
	}
	derivation, err := ontology.NewAssertionDerivation(
		original.EmbeddingModel(), original.EmbeddingModelVersion(),
		original.SimilarityMetric(), original.Threshold(),
		original.TessellationCell(), original.Score(),
		source, target, original.IteratorName(), original.IteratorOptions())
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := ontology.NewDerivationEvidenceRef(
		derivation, like.Evidence()[0].Metadata())
	if err != nil {
		t.Fatal(err)
	}
	return newDerivedAssertion(t, like, evidence)
}

func newDerivedAssertion(
	t *testing.T, like ontology.Assertion, evidence ontology.EvidenceRef,
) ontology.Assertion {
	t.Helper()
	object, ok := like.Object().ReferenceValue()
	if !ok {
		t.Fatal("fixture assertion has no reference object")
	}
	value, err := ontology.NewReferenceValue(object)
	if err != nil {
		t.Fatal(err)
	}
	var options []ontology.AssertionOption
	if subjectType, ok := like.SubjectType(); ok {
		options = append(options, ontology.WithAssertionSubjectType(subjectType))
	}
	if objectType, ok := like.ObjectType(); ok {
		options = append(options, ontology.WithAssertionObjectType(objectType))
	}
	if identity, ok := like.Ontology(); ok {
		options = append(options, ontology.WithAssertionOntology(identity))
	}
	assertion, err := ontology.NewAssertion(
		like.Subject(), like.Predicate(), value, ontology.AssertionDerived,
		like.Confidence(), []ontology.EvidenceRef{evidence},
		like.Provenance(), like.Metadata(), options...)
	if err != nil {
		t.Fatal(err)
	}
	return assertion
}

func (w *derivedWorld) tamperedClient(
	t *testing.T, planted map[shoal.ID]ontology.Assertion,
) *authorized.Client {
	t.Helper()
	return w.clientWithDerivedReader(t, tamperedDerivedAssertions{
		DerivedAssertionReader: trustedDerivedAssertionReader(w.f.base),
		planted:                planted,
	})
}

func (w *derivedWorld) clientWithDerivedReader(
	t *testing.T, derived authorized.DerivedAssertionReader,
) *authorized.Client {
	t.Helper()
	selector := w.f.staticSelector(t, w.f.sourceA, w.f.policyA)
	client, err := authorized.NewClient(authorized.Config{
		Base:                   w.f.base,
		VectorScorer:           trustedVectorScorer(w.f.base),
		InteractionWriter:      trustedInteractionWriter(w.f.base),
		InteractionReader:      trustedInteractionReader(w.f.base),
		SnapshotValidator:      trustedSnapshotValidator(w.f.base),
		DerivedAssertionReader: derived,
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

// TestDerivedEdgeDerivationMustCoverOnlyItsEndpoints: a trusted derived
// assertion between two nodes the reader may read, whose derivation was
// computed to or from the hidden target, or which is planted under another
// assertion's ID, does not admit a recording; it is refused exactly as an
// edge that never existed. The derivation is attributable to the edge's own
// endpoints only.
func TestDerivedEdgeDerivationMustCoverOnlyItsEndpoints(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newDerivedWorld(t, store)
		trusted, err := w.f.base.DerivedAssertions(context.Background(),
			[]shoal.ID{w.toOpen.ID})
		if err != nil {
			t.Fatal(err)
		}
		open, ok := trusted[w.toOpen.ID]
		if !ok {
			t.Fatal("the open derived assertion is not held")
		}
		overHidden := derivedAssertionOver(t, open,
			w.source.Document.ID, w.hidden.Document.ID)
		fromHidden := derivedAssertionOver(t, open,
			w.hidden.Document.ID, w.open.Document.ID)
		// The control: the same construction over the edge's own
		// endpoints reproduces the corpus's assertion, ID included.
		if again := derivedAssertionOver(t, open,
			w.source.Document.ID, w.open.Document.ID); again.ID() != open.ID() {
			t.Fatalf("reconstruction changed the assertion: %s vs %s",
				again.ID(), open.ID())
		}
		const misfiled shoal.ID = "assertion:derived-misfiled"
		client := w.tamperedClient(t, map[shoal.ID]ontology.Assertion{
			overHidden.ID(): overHidden,
			fromHidden.ID(): fromHidden,
			misfiled:        open,
		})
		path := recordingPaths[0]
		reader := w.pathDecision(t, path, "reader")
		record := func(id shoal.ID, evidence interaction.EvidenceReference) error {
			return path.record(client, w.as(reader),
				w.derivedSession(t, reader, id, evidence))
		}
		never := record("interaction.session_derived_tamper_never",
			w.fabricatedEvidence(t, "assertion:derived-never-existed", w.open.Document.ID))
		for name, edgeID := range map[string]shoal.ID{
			"derivation to the hidden target":   overHidden.ID(),
			"derivation from the hidden target": fromHidden.ID(),
			"assertion under another ID":        misfiled,
		} {
			id := shoal.ID("interaction.session_derived_tamper_" + string(edgeID[len("assertion:"):]))
			err := record(id, w.fabricatedEvidence(t, edgeID, w.open.Document.ID))
			assertSameRefusal(t, name, err, never)
			if !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
				t.Fatalf("%s: refusal = %v, want not_found", name, err)
			}
			requireNotPersisted(t, w.f.base, id)
		}
		// The control: the genuine edge records through the same client.
		if err := record("interaction.session_derived_tamper_control",
			w.evidence(t, w.toOpen)); err != nil {
			t.Fatal(err)
		}
	})
}

// failingDerivedAssertions is a trusted derived assertion reader that is
// unavailable.
type failingDerivedAssertions struct{}

func (failingDerivedAssertions) DerivedAssertions(
	context.Context, []shoal.ID,
) (map[shoal.ID]ontology.Assertion, error) {
	return nil, shoal.NewError(shoal.ErrorUnavailable, "derived assertions unavailable")
}

// TestUnavailableDerivedReaderDoesNotRevealHiddenEdges: with the trusted
// derived assertion reader failing, a recording over a registered edge the
// reader may not read fails exactly as one over an edge that never
// existed. Consulting the reader only for unregistered edges would refuse
// the hidden edge and fail the other, telling the recorder the hidden edge
// exists.
func TestUnavailableDerivedReaderDoesNotRevealHiddenEdges(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		w := newDerivedWorld(t, store)
		w.connect(w.client, "edge-derived-hidden-application",
			w.source.Document.ID, w.hidden.Document.ID)
		w.resnapshot()
		hiddenEdge := graph.Edge{
			ID:   w.edgeID(w.client, "edge-derived-hidden-application"),
			From: w.source.Document.ID, To: w.hidden.Document.ID,
		}
		hidden := exactAuthorizedGraphEvidenceWith(t, w.f.base, hiddenEdge, nil)
		never := w.fabricatedEvidence(t,
			"assertion:derived-never-existed", w.open.Document.ID)
		client := w.clientWithDerivedReader(t, failingDerivedAssertions{})
		path := recordingPaths[0]
		reader := w.pathDecision(t, path, "reader")
		neverErr := path.record(client, w.as(reader), w.derivedSession(
			t, reader, "interaction.session_derived_unavailable_never", never))
		hiddenErr := path.record(client, w.as(reader), w.derivedSession(
			t, reader, "interaction.session_derived_unavailable_hidden", hidden))
		if !shoal.IsErrorCode(neverErr, shoal.ErrorUnavailable) {
			t.Fatalf("a never-existing edge = %v, want unavailable", neverErr)
		}
		if hiddenErr == nil || hiddenErr.Error() != neverErr.Error() {
			t.Fatalf("a hidden registered edge = %v, but a never-existing one = %v",
				hiddenErr, neverErr)
		}
		// The control: through the working reader the hidden edge is refused.
		err := path.record(w.client, w.as(reader), w.derivedSession(
			t, reader, "interaction.session_derived_available_hidden", hidden))
		if !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			t.Fatalf("a hidden registered edge through the working reader = %v", err)
		}
	})
}
