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

import (
	"context"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// boundWorld is two documents on two sources, both matching one term, with an
// application edge from the source-A document to the source-B one, so every
// read path has something on the far side of a bound to reach.
type boundWorld struct {
	f       *fixture
	docA    explorer.IngestResult
	docB    explorer.IngestResult
	boundA  *retrieval.SourceBound
	boundB  *retrieval.SourceBound
	edgeAB  shoal.ID
	nodesA  map[shoal.ID]struct{}
	nodesB  map[shoal.ID]struct{}
	readsAB context.Context
}

const boundTerm = "lighthouse"

func newBoundWorld(t *testing.T) *boundWorld {
	t.Helper()
	f := newFixture(t)
	admin := f.admin(t)
	docA, err := f.clientA.Ingest(admin, explorer.Source{
		URI: "file:///a.txt", Title: "A", MediaType: explorer.MediaTypeText,
		Content: "the " + boundTerm + " on the northern cape",
	})
	if err != nil {
		t.Fatalf("ingest A: %v", err)
	}
	docB, err := f.clientB.Ingest(admin, explorer.Source{
		URI: "file:///b.txt", Title: "B", MediaType: explorer.MediaTypeText,
		Content: "the " + boundTerm + " on the southern reef",
	})
	if err != nil {
		t.Fatalf("ingest B: %v", err)
	}
	edgeID, err := f.clientA.ApplicationEdgeID(admin, "bound_probe")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.clientA.Connect(admin, graph.Edge{
		ID: edgeID, From: docA.Document.ID, To: docB.Document.ID,
		Type: "references", Weight: 1,
	}); err != nil {
		t.Fatalf("connect A to B: %v", err)
	}
	world := &boundWorld{
		f: f, docA: docA, docB: docB, edgeAB: edgeID,
		boundA: &retrieval.SourceBound{Scopes: []retrieval.BoundScope{
			{SourceID: f.sourceA, PolicyID: f.policyA},
		}},
		boundB: &retrieval.SourceBound{Scopes: []retrieval.BoundScope{
			{SourceID: f.sourceB, PolicyID: f.policyB},
		}},
		nodesA:  documentNodeIDs(t, f, admin, docA),
		nodesB:  documentNodeIDs(t, f, admin, docB),
		readsAB: admin,
	}
	return world
}

func documentNodeIDs(
	t *testing.T, f *fixture, ctx context.Context, doc explorer.IngestResult,
) map[shoal.ID]struct{} {
	t.Helper()
	view, err := f.clientA.Document(ctx, doc.Document.ID, "")
	if err != nil {
		t.Fatalf("read %s: %v", doc.Document.ID, err)
	}
	ids := map[shoal.ID]struct{}{view.Document.ID: {}}
	var walk func(explorer.SectionView)
	walk = func(section explorer.SectionView) {
		ids[section.Section.ID] = struct{}{}
		for _, span := range section.Spans {
			ids[span.ID] = struct{}{}
		}
		for _, child := range section.Children {
			walk(child)
		}
	}
	walk(view.Root)
	return ids
}

// citedDocuments is every document a response draws on: each citation and
// each node of each evidence path.
func citedDocuments(
	w *boundWorld, response retrieval.Response,
) map[shoal.ID]struct{} {
	cited := map[shoal.ID]struct{}{}
	for _, result := range response.Results {
		for _, evidence := range result.Evidence {
			cited[evidence.Citation.DocumentID] = struct{}{}
			for _, node := range evidence.Path.Nodes {
				if _, ok := w.nodesA[node.ID]; ok {
					cited[w.docA.Document.ID] = struct{}{}
				}
				if _, ok := w.nodesB[node.ID]; ok {
					cited[w.docB.Document.ID] = struct{}{}
				}
			}
		}
	}
	return cited
}

var boundModes = []retrieval.Mode{
	retrieval.ModeLexical, retrieval.ModeTree, retrieval.ModeGraph,
}

// TestRetrieveWithinBoundDrawsNoEvidenceFromAnotherSource is #560's core
// claim at the retrieval seam: for a principal that may read both sources, a
// bound on one returns evidence — citations and path nodes alike — drawn from
// that source only, while the same request unbounded reaches both.
func TestRetrieveWithinBoundDrawsNoEvidenceFromAnotherSource(t *testing.T) {
	w := newBoundWorld(t)
	request := retrieval.Request{Text: boundTerm, TopK: 10, Modes: boundModes}

	unbounded, err := w.f.clientA.Retrieve(w.readsAB, request)
	if err != nil {
		t.Fatal(err)
	}
	cited := citedDocuments(w, unbounded)
	if _, ok := cited[w.docA.Document.ID]; !ok {
		t.Fatal("control: unbounded retrieval must reach source A")
	}
	if _, ok := cited[w.docB.Document.ID]; !ok {
		t.Fatal("control: unbounded retrieval must reach source B")
	}

	for name, test := range map[string]struct {
		bound *retrieval.SourceBound
		want  shoal.ID
	}{
		"bound to A": {bound: w.boundA, want: w.docA.Document.ID},
		"bound to B": {bound: w.boundB, want: w.docB.Document.ID},
	} {
		t.Run(name, func(t *testing.T) {
			bounded := request
			bounded.Bound = test.bound
			response, err := w.f.clientA.Retrieve(w.readsAB, bounded)
			if err != nil {
				t.Fatal(err)
			}
			if len(response.Results) == 0 {
				t.Fatal("a bound on a readable source must still retrieve it")
			}
			for documentID := range citedDocuments(w, response) {
				if documentID != test.want {
					t.Fatalf("bounded retrieval drew on %s, want only %s",
						documentID, test.want)
				}
			}
		})
	}
}

// TestRetrieveBoundRevealsNoHiddenCounts pins the #398 property under a
// bound: the suppressed count is decided before the bound, so it is the same
// whatever the bound names — including a source the caller cannot read, or
// one that does not exist — and a bound that reaches nothing is an empty
// answer rather than an error. Readable content outside the bound is not
// counted either.
func TestRetrieveBoundRevealsNoHiddenCounts(t *testing.T) {
	w := newBoundWorld(t)
	alice := w.f.alice(t) // source A only
	request := retrieval.Request{Text: boundTerm, TopK: 10}
	_, control, err := w.f.clientA.RetrieveWithDisclosure(alice, request)
	if err != nil {
		t.Fatal(err)
	}
	if control.Suppressed == 0 {
		t.Fatal("control: alice must be denied the source-B document")
	}
	absent := &retrieval.SourceBound{Scopes: []retrieval.BoundScope{{
		SourceID: []byte("source-absent"), PolicyID: []byte("policy-absent"),
	}}}
	for name, test := range map[string]struct {
		bound *retrieval.SourceBound
		empty bool
	}{
		"own source":        {bound: w.boundA},
		"unreadable source": {bound: w.boundB, empty: true},
		"absent source":     {bound: absent, empty: true},
		"no scopes":         {bound: &retrieval.SourceBound{}, empty: true},
	} {
		t.Run(name, func(t *testing.T) {
			bounded := request
			bounded.Bound = test.bound
			response, disclosure, err := w.f.clientA.RetrieveWithDisclosure(
				alice, bounded)
			if err != nil {
				t.Fatalf("a bound must narrow, not fail: %v", err)
			}
			if disclosure != control {
				t.Fatalf("disclosure under bound = %#v, unbounded = %#v",
					disclosure, control)
			}
			if test.empty != (len(response.Results) == 0) {
				t.Fatalf("results = %d, want empty = %v",
					len(response.Results), test.empty)
			}
		})
	}

	// A caller who may read both sources is denied nothing, and the document
	// the bound leaves out is not counted as withheld.
	_, both, err := w.f.clientA.RetrieveWithDisclosure(w.readsAB, retrieval.Request{
		Text: boundTerm, TopK: 10, Bound: w.boundA,
	})
	if err != nil {
		t.Fatal(err)
	}
	if both.Suppressed != 0 {
		t.Fatalf("bound-excluded readable content was counted: %#v", both)
	}
}

// TestRetrieveBoundOnlyNarrows pins that a bound never grants: naming a
// source the decision does not permit adds nothing, and a bound covering
// everything the caller reads changes nothing.
func TestRetrieveBoundOnlyNarrows(t *testing.T) {
	w := newBoundWorld(t)
	alice := w.f.alice(t)
	request := retrieval.Request{Text: boundTerm, TopK: 10}
	unbounded, err := w.f.clientA.Retrieve(alice, request)
	if err != nil {
		t.Fatal(err)
	}
	both := request
	both.Bound = &retrieval.SourceBound{Scopes: append(
		append([]retrieval.BoundScope(nil), w.boundA.Scopes...),
		w.boundB.Scopes...)}
	widened, err := w.f.clientA.Retrieve(alice, both)
	if err != nil {
		t.Fatal(err)
	}
	if len(widened.Results) != len(unbounded.Results) {
		t.Fatalf("bound over both sources returned %d results, unbounded %d",
			len(widened.Results), len(unbounded.Results))
	}
	for documentID := range citedDocuments(w, widened) {
		if documentID == w.docB.Document.ID {
			t.Fatal("a bound naming source B granted alice its document")
		}
	}
}

// TestBoundedReadsRefuseOutOfBoundContentAsAbsent pins the direct reads: a
// document outside the bound has exactly the error a document that does not
// exist has, and a graph expansion neither returns an out-of-bound node or
// edge nor accepts an out-of-bound seed.
func TestBoundedReadsRefuseOutOfBoundContentAsAbsent(t *testing.T) {
	w := newBoundWorld(t)
	ctx := w.readsAB

	if _, err := w.f.clientA.DocumentWithin(ctx, w.boundA, w.docA.Document.ID, ""); err != nil {
		t.Fatalf("in-bound document: %v", err)
	}
	_, outside := w.f.clientA.DocumentWithin(ctx, w.boundA, w.docB.Document.ID, "")
	_, absent := w.f.clientA.DocumentWithin(
		ctx, w.boundA, shoal.ID("doc_00000000000000000000000000000000"), "")
	if !shoal.IsErrorCode(outside, shoal.ErrorNotFound) ||
		outside == nil || absent == nil || outside.Error() != absent.Error() {
		t.Fatalf("out-of-bound = %v, absent = %v; want the same not-found", outside, absent)
	}

	request := explorer.BoundedNeighborhoodRequest{
		NodeIDs: []shoal.ID{w.docA.Document.ID}, Depth: 2, Fanout: 64,
		MaxNodes: 64, MaxScannedEdges: 4096,
		Direction: explorer.GraphDirectionBoth,
	}
	control, err := w.f.clientA.BoundedNeighborhood(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !hasNode(control.Neighborhood, w.docB.Document.ID) {
		t.Fatal("control: the unbounded expansion must reach the source-B document")
	}
	bounded, err := w.f.clientA.BoundedNeighborhoodWithin(ctx, w.boundA, request)
	if err != nil {
		t.Fatal(err)
	}
	if !hasNode(bounded.Neighborhood, w.docA.Document.ID) {
		t.Fatal("the bounded expansion must keep its in-bound seed")
	}
	for _, node := range bounded.Neighborhood.Nodes {
		if _, ok := w.nodesB[node.ID]; ok || node.ID == w.docB.Document.ID {
			t.Fatalf("bounded expansion returned source-B node %s", node.ID)
		}
	}
	for _, edge := range bounded.Neighborhood.Edges {
		if edge.ID == w.edgeAB {
			t.Fatal("bounded expansion returned the edge into source B")
		}
		if _, ok := w.nodesB[edge.To]; ok {
			t.Fatalf("bounded expansion returned edge %s into source B", edge.ID)
		}
	}

	// A second source-A document reachable only through the source-B one is
	// not part of the bounded neighborhood, though it is itself in bound.
	far, err := w.f.clientA.Ingest(ctx, explorer.Source{
		URI: "file:///a-far.txt", Title: "A far", MediaType: explorer.MediaTypeText,
		Content: "a far shore",
	})
	if err != nil {
		t.Fatal(err)
	}
	edgeBA, err := w.f.clientB.ApplicationEdgeID(ctx, "bound_probe_back")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.f.clientB.Connect(ctx, graph.Edge{
		ID: edgeBA, From: w.docB.Document.ID, To: far.Document.ID,
		Type: "references", Weight: 1,
	}); err != nil {
		t.Fatalf("connect B to the far A document: %v", err)
	}
	// Depth three reaches the far document's own structure too, whose edges
	// are themselves in bound: only reachability from the seed through kept
	// edges can leave them out.
	request.Direction = explorer.GraphDirectionOutgoing
	request.Depth = 3
	control, err = w.f.clientA.BoundedNeighborhood(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !hasNode(control.Neighborhood, far.Document.ID) {
		t.Fatal("control: the unbounded expansion must reach the far A document through B")
	}
	bounded, err = w.f.clientA.BoundedNeighborhoodWithin(ctx, w.boundA, request)
	if err != nil {
		t.Fatal(err)
	}
	if hasNode(bounded.Neighborhood, far.Document.ID) {
		t.Fatal("a node reached only through out-of-bound content must not be returned")
	}
	request.Direction = explorer.GraphDirectionBoth
	request.Depth = 2

	request.NodeIDs = []shoal.ID{w.docB.Document.ID}
	_, outsideSeed := w.f.clientA.BoundedNeighborhoodWithin(ctx, w.boundA, request)
	if !shoal.IsErrorCode(outsideSeed, shoal.ErrorNotFound) {
		t.Fatalf("out-of-bound seed = %v, want not-found", outsideSeed)
	}
}

// TestBoundKeepsLabelledContentOnItsSource pins the label half of the bound's
// meaning: a document labelled on a bound source is within the bound for a
// reader holding the label, and still hidden — and counted exactly as
// unbounded — for one who does not.
func TestBoundKeepsLabelledContentOnItsSource(t *testing.T) {
	f := newFixture(t)
	const term = "quartermaster"
	if _, err := f.clientA.Ingest(f.labelIngester(t, "ingester", "secret"),
		labelledSource("file:///secret.txt", "the "+term+" ledger", "secret")); err != nil {
		t.Fatal(err)
	}
	bound := &retrieval.SourceBound{Scopes: []retrieval.BoundScope{
		{SourceID: f.sourceA, PolicyID: f.policyA},
	}}
	request := retrieval.Request{Text: term, TopK: 10, Bound: bound}

	cleared := f.context(t, f.decision(t, "cleared",
		[][]byte{f.sourceA},
		[][]byte{f.policyA, labelPolicy(t, f.sourceA, "secret")},
		labelReaderOperations))
	response, err := f.clientA.Retrieve(cleared, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) == 0 {
		t.Fatal("a document labelled on the bound source must be within the bound")
	}

	uncleared := f.context(t, f.decision(t, "uncleared",
		[][]byte{f.sourceA}, [][]byte{f.policyA}, labelReaderOperations))
	unbounded := request
	unbounded.Bound = nil
	_, control, err := f.clientA.RetrieveWithDisclosure(uncleared, unbounded)
	if err != nil {
		t.Fatal(err)
	}
	hidden, disclosure, err := f.clientA.RetrieveWithDisclosure(uncleared, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(hidden.Results) != 0 {
		t.Fatal("a bound must not reveal a document the reader's labels hide")
	}
	if disclosure != control || control.Suppressed == 0 {
		t.Fatalf("disclosure under bound = %#v, unbounded = %#v", disclosure, control)
	}
}

// TestUnboundedRetrieversRefuseABound pins that a retriever which cannot
// interpret a bound refuses it instead of silently retrieving unbounded.
func TestUnboundedRetrieversRefuseABound(t *testing.T) {
	f := newFixture(t)
	_, err := f.base.Retrieve(context.Background(), retrieval.Request{
		Text: "anything", TopK: 1,
		Bound: &retrieval.SourceBound{Scopes: []retrieval.BoundScope{
			{SourceID: f.sourceA, PolicyID: f.policyA},
		}},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("base engine with a bound = %v, want unavailable", err)
	}
}
