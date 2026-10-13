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
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/extraction"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestRebuildApplicationEdgeIDCollisions pins the precedence between an
// application edge and an intrinsic or extracted edge sharing its ID (#724).
// Every write path refuses to create such a collision, so only stored state
// from before #724 holds one; a rebuild over it never lets either edge
// silently displace the other. Identical content is one edge; different
// content fails the rebuild with a conflict.
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
	store := func(corpus *Explorer, edge graph.Edge) error {
		corpus.mu.Lock()
		defer corpus.mu.Unlock()
		corpus.edges[edge.ID] = persistedEdge{Edge: edge, PublishedAt: time.Now().UTC()}
		return corpus.rebuildCurrentGraphLocked()
	}

	t.Run("application edge to extracted nodes", func(t *testing.T) {
		corpus, document, _ := setup(t)
		edge := graph.Edge{
			ID: "app/entity", From: document.Document.ID, To: entity, Type: "mentions", Weight: 1,
		}
		if err := store(corpus, edge); err != nil {
			t.Fatal(err)
		}
		corpus.mu.RLock()
		got, ok := corpus.graphEdges[edge.ID]
		corpus.mu.RUnlock()
		if !ok || !edgesEqual(got, edge) {
			t.Fatalf("application edge to an extracted entity = %+v, %v", got, ok)
		}
	})
	t.Run("identical to an extracted edge", func(t *testing.T) {
		corpus, _, _ := setup(t)
		if err := store(corpus, canonicalRelation); err != nil {
			t.Fatalf("identical content conflicts: %v", err)
		}
	})
	t.Run("differs from an extracted edge", func(t *testing.T) {
		corpus, _, _ := setup(t)
		squatter := canonicalRelation
		squatter.Weight = 0.5
		if err := store(corpus, squatter); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
			t.Fatalf("rebuild over a differing extracted edge = %v, want conflict", err)
		}
	})
	t.Run("differs from an intrinsic edge", func(t *testing.T) {
		// Before #724 the application edge silently replaced the intrinsic one.
		corpus, _, intrinsic := setup(t)
		squatter := intrinsic
		squatter.Type = "squatter"
		if err := store(corpus, squatter); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
			t.Fatalf("rebuild over a differing intrinsic edge = %v, want conflict", err)
		}
	})
}
