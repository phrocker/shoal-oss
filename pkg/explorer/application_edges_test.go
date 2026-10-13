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

package explorer_test

// Application edges (Connect) may attach to extracted entities and to
// materialized graph nodes, and must stay in the graph across every later
// rebuild (#724). Before #724 the rebuild filtered application edges before
// the extracted and materialized nodes were placed, so every such edge
// vanished at the first ingest, extraction, interaction, fold,
// materialization or reopen after Connect.

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/extraction"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type applicationEdgeWorld struct {
	t         *testing.T
	dir       string
	corpus    *explorer.Explorer
	skill     explorer.IngestResult
	other     explorer.IngestResult
	extracted []shoal.ID
	// extractedTools counts the skill entity's own provides_tool relations.
	extractedTools int
	team           shoal.ID
	person         shoal.ID
	edges          []graph.Edge
}

// newApplicationEdgeWorld ingests a skill document and a plain document,
// extracts the skill document, materializes a two-node graph, and connects
// application edges between every pair of endpoint classes.
func newApplicationEdgeWorld(t *testing.T) *applicationEdgeWorld {
	t.Helper()
	ctx := context.Background()
	w := &applicationEdgeWorld{t: t, dir: t.TempDir()}
	corpus, err := explorer.Open(w.dir)
	if err != nil {
		t.Fatal(err)
	}
	w.corpus = corpus
	t.Cleanup(func() { _ = w.corpus.Close() })
	w.skill = w.ingest("file:///SKILL.md", sampleSkillMarkdown)
	w.other = w.ingest("file:///other.md", "# Other\n\nplain words\n")
	w.extract()
	around := w.neighborhood(w.skill.Document.ID, 2, nil)
	// The skill entity, which states provides_tool relations, comes first.
	for _, edge := range around.Edges {
		if edge.Type == "provides_tool" {
			w.extracted = append(w.extracted, edge.From)
			break
		}
	}
	if len(w.extracted) != 1 {
		t.Fatal("extraction produced no provides_tool relation")
	}
	for _, node := range around.Nodes {
		if node.Properties[extraction.GraphPropertyOntologyConceptID] != "" &&
			node.ID != w.extracted[0] {
			w.extracted = append(w.extracted, node.ID)
		}
	}
	if len(w.extracted) < 2 {
		t.Fatalf("extraction produced %d entities, want at least 2", len(w.extracted))
	}
	w.extractedTools = countEdgesOfType(
		w.neighborhood(w.extracted[0], 1, []string{"provides_tool"}).Edges, "provides_tool")
	if w.extractedTools == 0 {
		t.Fatal("skill entity has no extracted provides_tool edges")
	}
	snapshot, err := corpus.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := corpus.MaterializeGraph(ctx, graphRequest(snapshot))
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range materialized.GraphNodes {
		switch node.Kind {
		case "team":
			w.team = node.ID
		case "person":
			w.person = node.ID
		}
	}
	if w.team == "" || w.person == "" {
		t.Fatalf("materialized nodes = %#v", materialized.GraphNodes)
	}

	entity, peer := w.extracted[0], w.extracted[1]
	document := w.other.Document.ID
	// The fixture behind #724: many edges of one type on one entity.
	for i := 0; i < 24; i++ {
		w.connect(fmt.Sprintf("app/entity-to-document/%02d", i), entity, document, "provides_tool")
	}
	w.connect("app/document-to-entity", document, entity, "mentions")
	w.connect("app/entity-to-entity", entity, peer, "related")
	w.connect("app/materialized-to-document", w.team, document, "documented_by")
	w.connect("app/document-to-materialized", document, w.person, "about")
	w.connect("app/entity-to-materialized", entity, w.team, "owned_by")
	w.connect("app/document-to-document", w.skill.Document.ID, document, "see_also")
	return w
}

func (w *applicationEdgeWorld) ingest(uri, content string) explorer.IngestResult {
	w.t.Helper()
	result, err := w.corpus.Ingest(context.Background(), explorer.Source{
		URI: uri, MediaType: explorer.MediaTypeMarkdown, Content: content,
	})
	if err != nil {
		w.t.Fatal(err)
	}
	return result
}

func (w *applicationEdgeWorld) extract() {
	w.t.Helper()
	if _, err := w.corpus.ExtractDocument(context.Background(), explorer.ExtractionRequest{
		DocumentID: w.skill.Document.ID, RevisionID: w.skill.Revision.ID,
		Version: skillsOntologyVersion(w.t),
	}); err != nil {
		w.t.Fatal(err)
	}
}

func (w *applicationEdgeWorld) connect(id string, from, to shoal.ID, edgeType string) {
	w.t.Helper()
	w.connectEdge(graph.Edge{
		ID: shoal.ID(id), From: from, To: to, Type: edgeType, Weight: 1,
	})
}

func (w *applicationEdgeWorld) connectEdge(edge graph.Edge) {
	w.t.Helper()
	if err := w.corpus.Connect(context.Background(), edge); err != nil {
		w.t.Fatalf("Connect(%s): %v", edge.ID, err)
	}
	w.edges = append(w.edges, edge)
}

func (w *applicationEdgeWorld) neighborhood(
	seed shoal.ID, depth uint32, types []string,
) explorer.Neighborhood {
	w.t.Helper()
	around, err := w.corpus.Neighborhood(context.Background(), explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{seed}, Depth: depth, EdgeTypes: types,
	})
	if err != nil {
		w.t.Fatal(err)
	}
	return around
}

// requireAllEdges checks every connected edge is in the graph, with its
// content, around both of its endpoints, through the plain and the bounded
// neighborhood.
func (w *applicationEdgeWorld) requireAllEdges(stage string) {
	w.t.Helper()
	ctx := context.Background()
	if got := countEdgesOfType(w.neighborhood(w.extracted[0], 1, []string{"provides_tool"}).Edges,
		"provides_tool"); got != 24+w.extractedTools {
		w.t.Errorf("%s: entity provides_tool edges = %d, want 24 connected + %d extracted",
			stage, got, w.extractedTools)
	}
	for _, edge := range w.edges {
		for _, seed := range []shoal.ID{edge.From, edge.To} {
			around := w.neighborhood(seed, 1, []string{edge.Type})
			if !containsEdge(around.Edges, edge) {
				w.t.Errorf("%s: edge %s missing around %s", stage, edge.ID, seed)
			}
		}
		bounded, err := w.corpus.BoundedNeighborhood(ctx, explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{edge.From}, Depth: 1, EdgeTypes: []string{edge.Type},
			Fanout: 64, MaxNodes: 64, MaxScannedEdges: 256,
		})
		if err != nil {
			w.t.Fatal(err)
		}
		if !containsEdge(bounded.Neighborhood.Edges, edge) {
			w.t.Errorf("%s: edge %s missing from the bounded neighborhood", stage, edge.ID)
		}
	}
	ids := make([]shoal.ID, len(w.edges))
	for i, edge := range w.edges {
		ids[i] = edge.ID
	}
	connected, err := w.corpus.ConnectedEdges(ctx, ids)
	if err != nil {
		w.t.Fatal(err)
	}
	if len(connected) != len(ids) {
		w.t.Errorf("%s: stored application edges = %d, want %d", stage, len(connected), len(ids))
	}
}

func containsEdge(edges []graph.Edge, want graph.Edge) bool {
	for _, edge := range edges {
		if edge.ID == want.ID {
			return edge.From == want.From && edge.To == want.To &&
				edge.Type == want.Type && edge.Weight == want.Weight
		}
	}
	return false
}

func TestApplicationEdgesOnDerivedNodesSurviveEveryRebuild(t *testing.T) {
	ctx := context.Background()
	w := newApplicationEdgeWorld(t)
	w.requireAllEdges("after connect")

	w.ingest("file:///third.md", "# Third\n\nmore words\n")
	w.requireAllEdges("after ingest")

	w.extract()
	w.requireAllEdges("after re-extract")

	recordedSession(t, w.corpus, "interaction.session_ops",
		[]shoal.ID{w.other.Document.ID}, nil)
	w.requireAllEdges("after interaction")

	if _, err := w.corpus.FoldInteractions(ctx, explorer.FoldRequest{
		SessionIDs:    []shoal.ID{"interaction.session_ops"},
		SummaryDigest: interaction.Digest("an out-of-band summary"),
	}); err != nil {
		t.Fatal(err)
	}
	w.requireAllEdges("after fold")

	snapshot, err := w.corpus.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	another := graphRequest(snapshot)
	another.Namespace = []byte("another")
	another.MutationID = "mutation-2"
	if _, err := w.corpus.MaterializeGraph(ctx, another); err != nil {
		t.Fatal(err)
	}
	w.requireAllEdges("after materialization")

	if err := w.corpus.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := explorer.Open(w.dir)
	if err != nil {
		t.Fatal(err)
	}
	w.corpus = reopened
	w.requireAllEdges("after reopen")
}

// TestConnectRefusesOverwritingStoredApplicationEdge pins that an edge ID is
// written once even when the graph does not show the edge: here its source
// section was superseded by a new revision. Before #724 Connect consulted
// only the graph, so a hidden edge could be overwritten with new content.
func TestConnectRefusesOverwritingStoredApplicationEdge(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	corpus, err := explorer.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = corpus.Close() }()
	first, err := corpus.Ingest(ctx, explorer.Source{
		URI: "file:///moving.md", MediaType: explorer.MediaTypeMarkdown,
		Content: "# Moving\n\nfirst revision\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err := corpus.Ingest(ctx, explorer.Source{
		URI: "file:///target.md", MediaType: explorer.MediaTypeMarkdown,
		Content: "# Target\n\nstays\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	var section shoal.ID
	for _, node := range neighborhoodNodes(t, corpus, first.Document.ID) {
		if node.ID != first.Document.ID {
			section = node.ID
			break
		}
	}
	if section == "" {
		t.Fatal("first revision has no section or span node")
	}
	edge := graph.Edge{
		ID: "app/superseded", From: section, To: target.Document.ID,
		Type: "cites", Weight: 1,
	}
	if err := corpus.Connect(ctx, edge); err != nil {
		t.Fatal(err)
	}
	if _, err := corpus.Ingest(ctx, explorer.Source{
		URI: "file:///moving.md", MediaType: explorer.MediaTypeMarkdown,
		Content: "# Moving Elsewhere\n\nsecond revision\n",
	}); err != nil {
		t.Fatal(err)
	}
	for _, node := range neighborhoodNodes(t, corpus, target.Document.ID) {
		if node.ID == section {
			t.Fatal("fixture no longer supersedes the section: the edge is still shown")
		}
	}

	for _, reopen := range []bool{false, true} {
		if reopen {
			if err := corpus.Close(); err != nil {
				t.Fatal(err)
			}
			if corpus, err = explorer.Open(dir); err != nil {
				t.Fatal(err)
			}
		}
		changed := edge
		changed.Weight = 2
		changed.From = target.Document.ID
		changed.To = first.Document.ID
		if err := corpus.Connect(ctx, changed); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
			t.Fatalf("reopen=%v: overwriting a hidden application edge = %v, want conflict", reopen, err)
		}
		around, err := corpus.Neighborhood(ctx, explorer.NeighborhoodRequest{
			NodeIDs: []shoal.ID{target.Document.ID}, Depth: 1, EdgeTypes: []string{"cites"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(around.Edges) != 0 {
			t.Fatalf("reopen=%v: refused content reached the graph: %#v", reopen, around.Edges)
		}
		connected, err := corpus.ConnectedEdges(ctx, []shoal.ID{edge.ID})
		if err != nil || !connected[edge.ID] {
			t.Fatalf("reopen=%v: stored edge lost: %v %v", reopen, connected, err)
		}
	}
}

func neighborhoodNodes(t *testing.T, corpus *explorer.Explorer, seed shoal.ID) []graph.Node {
	t.Helper()
	around, err := corpus.Neighborhood(context.Background(), explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{seed}, Depth: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	return around.Nodes
}

// TestDerivedWritesWinOverStoredApplicationEdges pins the precedence when an
// ingest, an extraction or a materialization derives an edge under the ID of
// a stored application edge with different content (#724): the derived write
// commits and is served, the application edge stays stored but withheld, and
// a shown application edge that is withheld is reported. Refusing the derived
// write instead would keep the squatted document or extraction out forever,
// and failing the rebuild would make the whole graph unreadable.
func TestDerivedWritesWinOverStoredApplicationEdges(t *testing.T) {
	ctx := context.Background()
	t.Run("extraction", func(t *testing.T) {
		corpus, err := explorer.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = corpus.Close() }()
		skill, err := corpus.Ingest(ctx, explorer.Source{
			URI: "file:///SKILL.md", MediaType: explorer.MediaTypeMarkdown,
			Content: sampleSkillMarkdown,
		})
		if err != nil {
			t.Fatal(err)
		}
		other, err := corpus.Ingest(ctx, explorer.Source{
			URI: "file:///other.md", MediaType: explorer.MediaTypeMarkdown,
			Content: "# Other\n",
		})
		if err != nil {
			t.Fatal(err)
		}
		planned, err := corpus.PlanExtractDocument(ctx, explorer.ExtractionRequest{
			DocumentID: skill.Document.ID, RevisionID: skill.Revision.ID,
			Version: skillsOntologyVersion(t),
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(planned.GraphEdges) == 0 {
			t.Fatal("planned extraction has no edges")
		}
		taken := planned.GraphEdges[0].ID
		if err := corpus.Connect(ctx, graph.Edge{
			ID: taken, From: skill.Document.ID, To: other.Document.ID,
			Type: "squatter", Weight: 1,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := corpus.CommitExtraction(ctx, planned); err != nil {
			t.Fatalf("extraction over an application edge ID = %v, want it committed", err)
		}
		requireDerivedEdgeWins(t, corpus, planned.GraphEdges[0], []shoal.ID{taken})
	})
	// The materialization and ingest cases squat on the ID with an edge the
	// graph no longer shows (its source revision was superseded): nothing is
	// withheld, since the application edge is not shown anyway.
	t.Run("materialization", func(t *testing.T) {
		corpus, err := explorer.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = corpus.Close() }()
		probe, err := explorer.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = probe.Close() }()
		snapshot, err := probe.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		planned, err := probe.PlanGraphMaterialization(ctx, graphRequest(snapshot))
		if err != nil {
			t.Fatal(err)
		}
		squatHiddenEdge(t, corpus, planned.GraphEdges[0].ID)
		snapshot, err = corpus.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := corpus.MaterializeGraph(ctx, graphRequest(snapshot)); err != nil {
			t.Fatalf("materialization over an application edge ID = %v, want it committed", err)
		}
		requireDerivedEdgeWins(t, corpus, planned.GraphEdges[0], nil)
	})
	t.Run("materialization over a shown edge", func(t *testing.T) {
		corpus, err := explorer.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = corpus.Close() }()
		left, err := corpus.Ingest(ctx, explorer.Source{
			URI: "file:///left.md", MediaType: explorer.MediaTypeMarkdown, Content: "# Left\n",
		})
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := corpus.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		planned, err := corpus.PlanGraphMaterialization(ctx, graphRequest(snapshot))
		if err != nil {
			t.Fatal(err)
		}
		taken := planned.GraphEdges[0].ID
		if err := corpus.Connect(ctx, graph.Edge{
			ID: taken, From: left.Document.ID, To: left.Document.ID, Type: "squatter", Weight: 1,
		}); err != nil {
			t.Fatal(err)
		}
		if snapshot, err = corpus.Snapshot(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := corpus.MaterializeGraph(ctx, graphRequest(snapshot)); err != nil {
			t.Fatalf("materialization over a shown application edge = %v, want it committed", err)
		}
		requireDerivedEdgeWins(t, corpus, planned.GraphEdges[0], []shoal.ID{taken})
	})
	t.Run("ingest", func(t *testing.T) {
		source := explorer.Source{
			URI: "file:///late.md", MediaType: explorer.MediaTypeMarkdown,
			Content: "# Late\n\narrives after the squatter\n",
		}
		probe, err := explorer.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = probe.Close() }()
		probed, err := probe.Ingest(ctx, source)
		if err != nil {
			t.Fatal(err)
		}
		around, err := probe.Neighborhood(ctx, explorer.NeighborhoodRequest{
			NodeIDs: []shoal.ID{probed.Document.ID}, Depth: 1,
		})
		if err != nil || len(around.Edges) == 0 {
			t.Fatalf("probe document has no intrinsic edges: %v", err)
		}
		corpus, err := explorer.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = corpus.Close() }()
		squatHiddenEdge(t, corpus, around.Edges[0].ID)
		if _, err := corpus.Ingest(ctx, source); err != nil {
			t.Fatalf("ingest over an application edge ID = %v, want it committed", err)
		}
		requireDerivedEdgeWins(t, corpus, around.Edges[0], nil)
	})
}

// requireDerivedEdgeWins checks the graph serves derived under its ID, the
// application edge squatting it stays stored, every document is readable,
// and the reported collisions are exactly collisions.
func requireDerivedEdgeWins(
	t *testing.T, corpus *explorer.Explorer, derived graph.Edge, collisions []shoal.ID,
) {
	t.Helper()
	ctx := context.Background()
	requireGraphReadable(t, corpus)
	around, err := corpus.Neighborhood(ctx, explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{derived.From}, Depth: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !containsEdge(around.Edges, derived) {
		t.Fatalf("derived edge %s is not served with its own content: %#v", derived.ID, around.Edges)
	}
	connected, err := corpus.ConnectedEdges(ctx, []shoal.ID{derived.ID})
	if err != nil || !connected[derived.ID] {
		t.Fatalf("the squatting application edge is no longer stored: %v %v", connected, err)
	}
	reported, err := corpus.ApplicationEdgeCollisions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(reported, collisions) {
		t.Fatalf("reported collisions = %v, want %v", reported, collisions)
	}
}

// squatHiddenEdge connects an application edge under id from a section that
// a new revision then supersedes, so the graph no longer shows it.
func squatHiddenEdge(t *testing.T, corpus *explorer.Explorer, id shoal.ID) {
	t.Helper()
	ctx := context.Background()
	moving, err := corpus.Ingest(ctx, explorer.Source{
		URI: "file:///squatter.md", MediaType: explorer.MediaTypeMarkdown,
		Content: "# Squatter\n\nfirst\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	var section shoal.ID
	for _, node := range neighborhoodNodes(t, corpus, moving.Document.ID) {
		if node.ID != moving.Document.ID {
			section = node.ID
		}
	}
	if err := corpus.Connect(ctx, graph.Edge{
		ID: id, From: section, To: moving.Document.ID, Type: "squatter", Weight: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := corpus.Ingest(ctx, explorer.Source{
		URI: "file:///squatter.md", MediaType: explorer.MediaTypeMarkdown,
		Content: "# Squatter Moved\n\nsecond\n",
	}); err != nil {
		t.Fatal(err)
	}
	for _, node := range neighborhoodNodes(t, corpus, moving.Document.ID) {
		if node.ID == section {
			t.Fatal("fixture no longer supersedes the squatter's section")
		}
	}
}

func requireGraphReadable(t *testing.T, corpus *explorer.Explorer) {
	t.Helper()
	documents, err := corpus.Documents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, summary := range documents {
		if _, err := corpus.Neighborhood(context.Background(), explorer.NeighborhoodRequest{
			NodeIDs: []shoal.ID{summary.Document.ID}, Depth: 1,
		}); err != nil {
			t.Fatalf("graph broken after the refusal: %v", err)
		}
	}
}
