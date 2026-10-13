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

package explorer

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/extraction"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestRebuildApplicationEdgeIDCollisions pins the precedence between a
// stored application edge and an intrinsic, extracted or derived assertion
// edge sharing its ID (#724). Connect refuses a taken ID, so only stored
// state holds such a collision (an edge connected before #699 namespaced
// authorized edge IDs, under an ID later derived). Identical content is one
// edge. Different content never fails the rebuild: the derived edge keeps its
// ID and content, the application edge stays stored but is withheld, and the
// withheld ID is reported to operators.
func TestRebuildApplicationEdgeIDCollisions(t *testing.T) {
	hex := strings.Repeat("0", 63)
	entity := shoal.ID("inferred-entity:" + hex + "1")
	tool := shoal.ID("inferred-entity:" + hex + "2")
	relation := shoal.ID("inferred-edge:" + hex + "3")
	relationProperties := shoal.Metadata{"ontology_relationship_id": "relationship:provides"}
	canonicalRelation := graph.Edge{
		ID: relation, From: entity, To: tool, Type: "provides_tool", Weight: 1,
		Properties: relationProperties,
	}

	setup := func(t *testing.T) (*Explorer, IngestResult, graph.Edge) {
		t.Helper()
		corpus, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = corpus.Close() })
		document, err := corpus.Ingest(context.Background(), Source{
			URI: "file:///a.md", MediaType: MediaTypeMarkdown, Content: "# zeta\n\nbody\n",
		})
		if err != nil {
			t.Fatal(err)
		}
		node := func(nodeID shoal.ID, key string) graph.Node {
			return graph.Node{
				ID: nodeID, Kind: "ontology:skill", Labels: []string{"skill", key},
				Properties: shoal.Metadata{
					extraction.GraphPropertyOntologyConceptID:  "concept:skill",
					extraction.GraphPropertyOntologyConceptKey: "skill",
					extraction.GraphPropertyEntityKey:          key,
				},
			}
		}
		corpus.mu.Lock()
		defer corpus.mu.Unlock()
		corpus.extractions["extraction-a"] = &persistedExtraction{
			ID: "extraction-a", DocumentID: document.Document.ID,
			RevisionID:       document.Revision.ID,
			OntologySchemaID: "schema", OntologyVersionID: "version",
			PublishedAt: time.Unix(1700000000, 0).UTC(),
			Nodes:       []graph.Node{node(entity, "zeta"), node(tool, "zeta-cli")},
			Edges:       []graph.Edge{canonicalRelation},
		}
		if err := corpus.rebuildCurrentGraphLocked(); err != nil {
			t.Fatal(err)
		}
		record := corpus.documents[document.Document.ID][document.Revision.ID]
		if len(record.Edges) == 0 {
			t.Fatal("document has no intrinsic edges")
		}
		return corpus, document, record.Edges[0]
	}
	// store places edge as a stored application edge and rebuilds.
	store := func(t *testing.T, corpus *Explorer, edge graph.Edge) {
		t.Helper()
		corpus.mu.Lock()
		defer corpus.mu.Unlock()
		corpus.edges[edge.ID] = persistedEdge{Edge: edge, PublishedAt: time.Now().UTC()}
		if err := corpus.rebuildCurrentGraphLocked(); err != nil {
			t.Fatalf("rebuild over a stored application edge = %v", err)
		}
	}
	requireServed := func(t *testing.T, corpus *Explorer, want graph.Edge, collisions []shoal.ID) {
		t.Helper()
		corpus.mu.RLock()
		got, ok := corpus.graphEdges[want.ID]
		corpus.mu.RUnlock()
		if !ok || !edgesEqual(got, want) {
			t.Fatalf("graph edge %s = %+v, %v, want %+v", want.ID, got, ok, want)
		}
		reported, err := corpus.ApplicationEdgeCollisions(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(reported, collisions) {
			t.Fatalf("reported collisions = %v, want %v", reported, collisions)
		}
	}

	t.Run("application edge to extracted nodes", func(t *testing.T) {
		corpus, document, _ := setup(t)
		edge := graph.Edge{
			ID: "app/entity", From: document.Document.ID, To: entity, Type: "mentions", Weight: 1,
		}
		store(t, corpus, edge)
		requireServed(t, corpus, edge, nil)
	})
	t.Run("identical to an extracted edge", func(t *testing.T) {
		corpus, _, _ := setup(t)
		store(t, corpus, canonicalRelation)
		requireServed(t, corpus, canonicalRelation, nil)
	})
	t.Run("differs from an extracted edge", func(t *testing.T) {
		corpus, _, _ := setup(t)
		squatter := canonicalRelation
		squatter.Weight = 0.5
		store(t, corpus, squatter)
		requireServed(t, corpus, canonicalRelation, []shoal.ID{relation})
	})
	t.Run("differs from an intrinsic edge", func(t *testing.T) {
		// Before #724 the application edge silently replaced the intrinsic one.
		corpus, _, intrinsic := setup(t)
		squatter := intrinsic
		squatter.Type = "squatter"
		store(t, corpus, squatter)
		requireServed(t, corpus, intrinsic, []shoal.ID{intrinsic.ID})
		// The report clears once the application edge no longer collides.
		corpus.mu.Lock()
		delete(corpus.edges, squatter.ID)
		err := corpus.rebuildCurrentGraphLocked()
		corpus.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		requireServed(t, corpus, intrinsic, nil)
	})
	t.Run("differs from a derived assertion edge", func(t *testing.T) {
		ctx := context.Background()
		corpus, err := Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = corpus.Close() })
		source := ingestLatentReadPathDocument(t, corpus, "source")
		target := ingestLatentReadPathDocument(t, corpus, "target")
		if err := corpus.PutLatentLinkCells(ctx, []LatentLinkCell{
			latentReadPathCell(source.Document.ID, target.Document.ID, 42, false),
		}); err != nil {
			t.Fatal(err)
		}
		around, err := corpus.Neighborhood(ctx, NeighborhoodRequest{
			NodeIDs: []shoal.ID{source.Document.ID}, Depth: 1,
			EdgeTypes: []string{latentReadPathEdgeType(t)},
		})
		if err != nil || len(around.Edges) != 1 {
			t.Fatalf("latent edge = %+v, %v", around.Edges, err)
		}
		derived := around.Edges[0]
		squatter := graph.Edge{
			ID: derived.ID, From: source.Document.ID, To: target.Document.ID,
			Type: "squatter", Weight: 1,
		}
		store(t, corpus, squatter)
		requireServed(t, corpus, derived, []shoal.ID{derived.ID})
	})
}

// TestLegacyIntrinsicSquatStaysReadableAcrossReopen is the #730 review
// repro: before #699 an authorized Connect accepted any edge ID, and a
// document's root contains edge ID is derived from its URI, so an
// application edge could hold the intrinsic ID of a document ingested later.
// A rebuild that failed on that collision left graphErr set for good: every
// graph read failed, every later ingest committed durably yet returned
// Conflict, and the squatted URI could never get a new revision. Now the
// intrinsic edge wins, the collision is reported, and everything keeps
// working across a durable reopen.
func TestLegacyIntrinsicSquatStaysReadableAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	squattedID := func(uri string) shoal.ID {
		document := SourceDocumentID(uri)
		root := shoal.ID(stableID("section", string(document), "root"))
		return containsEdge(document, root).ID
	}
	corpus, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	a, err := corpus.Ingest(ctx, Source{
		URI: "file:///a.md", MediaType: MediaTypeMarkdown, Content: "# alpha\n\nalpha body\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	record := corpus.documents[a.Document.ID][a.Revision.ID]
	squat := graph.Edge{
		ID: squattedID("file:///b.md"), From: record.Nodes[0].ID, To: record.Nodes[1].ID,
		Type: "related", Weight: 1,
	}
	if err := corpus.Connect(ctx, squat); err != nil {
		t.Fatalf("squatting Connect: %v", err)
	}
	if _, err := corpus.Ingest(ctx, Source{
		URI: "file:///b.md", MediaType: MediaTypeMarkdown, Content: "# beta\n\nbeta body\n",
	}); err != nil {
		t.Fatalf("ingest of the squatted document = %v", err)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}

	corpus, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = corpus.Close() }()
	b := SourceDocumentID("file:///b.md")
	requireIntrinsicWins := func(stage string) {
		t.Helper()
		for _, seed := range []shoal.ID{a.Document.ID, b} {
			if _, err := corpus.Neighborhood(ctx, NeighborhoodRequest{
				NodeIDs: []shoal.ID{seed}, Depth: 1,
			}); err != nil {
				t.Fatalf("%s: neighborhood of %s = %v", stage, seed, err)
			}
		}
		if _, err := corpus.BoundedNeighborhood(ctx, BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{b}, Depth: 1, Fanout: 8, MaxNodes: 8,
		}); err != nil {
			t.Fatalf("%s: bounded neighborhood = %v", stage, err)
		}
		corpus.mu.RLock()
		served, ok := corpus.graphEdges[squat.ID]
		corpus.mu.RUnlock()
		if !ok || served.Type != "contains" || served.From != b {
			t.Fatalf("%s: squatted ID serves %+v, %v, want b.md's contains edge", stage, served, ok)
		}
		reported, err := corpus.ApplicationEdgeCollisions(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(reported, []shoal.ID{squat.ID}) {
			t.Fatalf("%s: reported collisions = %v, want [%s]", stage, reported, squat.ID)
		}
	}
	requireIntrinsicWins("after reopen")

	if _, err := corpus.Ingest(ctx, Source{
		URI: "file:///c.md", MediaType: MediaTypeMarkdown, Content: "# gamma\n\ngamma\n",
	}); err != nil {
		t.Fatalf("unrelated ingest = %v", err)
	}
	second, err := corpus.Ingest(ctx, Source{
		URI: "file:///b.md", MediaType: MediaTypeMarkdown, Content: "# beta2\n\nbeta two\n",
	})
	if err != nil || second.Disposition != IngestApplied {
		t.Fatalf("new revision of the squatted document = %+v, %v", second.Disposition, err)
	}
	documents, err := corpus.Documents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, summary := range documents {
		if summary.Document.ID == b && summary.Revision.ID != second.Revision.ID {
			t.Fatalf("b.md current revision = %s, want %s", summary.Revision.ID, second.Revision.ID)
		}
	}
	if err := corpus.Connect(ctx, freshApplicationEdge(a.Document.ID, b)); err != nil {
		t.Fatalf("fresh Connect = %v", err)
	}
	// The squatter keeps its ID: a retry with other content conflicts.
	retry := squat
	retry.Weight = 2
	if err := corpus.Connect(ctx, retry); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("overwriting the withheld squatter = %v, want conflict", err)
	}
	requireIntrinsicWins("after later writes")
}

// freshApplicationEdge returns a fresh application edge between two documents.
func freshApplicationEdge(from, to shoal.ID) graph.Edge {
	return graph.Edge{ID: "fresh", From: from, To: to, Type: "x", Weight: 1}
}
