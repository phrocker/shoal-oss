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
	"slices"
	"testing"

	"github.com/phrocker/shoal-oss/internal/devbackfill"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestApplicationRegistrationOfASpanEdgeLosesToTheIntrinsicEdge pins that
// every parent and child pair of the parse counts as the document's own, a
// section and its span as well as the document and its root section: an
// application registration identical to a section-to-span contains edge loses
// that edge's ID to the intrinsic edge, as for the root edge, rather than
// refusing the document.
func TestApplicationRegistrationOfASpanEdgeLosesToTheIntrinsicEdge(t *testing.T) {
	const uri = "file:///span-edge/e.md"
	const content = "# epsilon\n\nepsilon body words\n"
	for _, kind := range policyStoreKinds {
		t.Run(kind.name, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			store, _ := kind.open(t)
			client := f.newClient(t, f.base, store, f.sourceA, f.policyA, nil)
			e, err := f.base.Ingest(ctx, explorer.Source{
				URI: uri, MediaType: explorer.MediaTypeMarkdown, Content: content,
			})
			if err != nil {
				t.Fatal(err)
			}
			spanEdge := sectionSpanEdge(t, f, e.Document.ID)
			legacyConnect(t, f, store, spanEdge, f.sourceRule(t, f.sourceB, f.policyB))
			if _, err := client.BackfillExistingDocumentsForDevelopment(
				f.alice(t), devbackfill.NewCapability()); err != nil {
				t.Fatalf("backfill = %v", err)
			}
			registration, ok, err := store.Edge(ctx, spanEdge.ID)
			if err != nil || !ok || registration.Kind != authorized.RegistrationDocument ||
				registration.DocumentID != e.Document.ID {
				t.Fatalf("span edge registered as %+v, %v, %v", registration, ok, err)
			}
			response, err := client.Retrieve(f.alice(t), retrieval.Request{
				Text: "epsilon body", TopK: 5,
			})
			if err != nil || len(response.Results) == 0 {
				t.Fatalf("Retrieve = %d results, %v", len(response.Results), err)
			}
		})
	}
}

// sectionSpanEdge returns one of a stored document's section-to-span
// contains edges, read from the base graph.
func sectionSpanEdge(t *testing.T, f *fixture, documentID shoal.ID) graph.Edge {
	t.Helper()
	around, err := f.base.Neighborhood(context.Background(), explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{documentID}, Depth: 4, EdgeTypes: []string{"contains"},
	})
	if err != nil {
		t.Fatal(err)
	}
	kinds := make(map[shoal.ID]string, len(around.Nodes))
	for _, node := range around.Nodes {
		kinds[node.ID] = node.Kind
	}
	index := slices.IndexFunc(around.Edges, func(edge graph.Edge) bool {
		return kinds[edge.From] == "section" && kinds[edge.To] == "span"
	})
	if index < 0 {
		t.Fatal("document has no section-to-span edge")
	}
	return around.Edges[index]
}
