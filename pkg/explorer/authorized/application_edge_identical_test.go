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

// TestIdenticalApplicationRegistrationOfARootEdgeLosesToTheIntrinsicEdge
// pins #732's exact-content case: an application registration holding a
// document's root contains edge with that edge's very content (a pre-#699
// Connect of it before the catalog registered the document, as a legacy
// corpus or the development backfill has). The intrinsic edge still wins it:
// it stays in the revision's intrinsic edges under the document's rule, and
// the application registration is withheld and reported. Leaving it out
// would register the revision one intrinsic edge short, and then every
// retrieval that hydrates the document fails its consistency check, for
// every reader of it.
func TestIdenticalApplicationRegistrationOfARootEdgeLosesToTheIntrinsicEdge(t *testing.T) {
	const bURI = "file:///identical/b.md"
	const bContent = "# beta\n\nbeta body words\n"
	for _, kind := range policyStoreKinds {
		t.Run(kind.name, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			store, reopen := kind.open(t)
			client := f.newClient(t, f.base, store, f.sourceA, f.policyA, nil)
			if _, err := client.Ingest(f.alice(t), explorer.Source{
				URI: "file:///identical/c.md", MediaType: explorer.MediaTypeMarkdown,
				Content: "# gamma\n\ngamma body words\n",
			}); err != nil {
				t.Fatal(err)
			}
			b, err := f.base.Ingest(ctx, explorer.Source{
				URI: bURI, MediaType: explorer.MediaTypeMarkdown, Content: bContent,
			})
			if err != nil {
				t.Fatal(err)
			}
			root := rootContainsEdge(t, bURI, bContent)
			if !explorer.IsDocumentContainsEdge(root) {
				t.Fatalf("root edge %+v is not a document contains edge", root)
			}
			legacyConnect(t, f, store, root, f.sourceRule(t, f.sourceB, f.policyB))
			if _, err := client.BackfillExistingDocumentsForDevelopment(
				f.alice(t), devbackfill.NewCapability()); err != nil {
				t.Fatalf("backfill = %v", err)
			}

			require := func(stage string, store authorized.PolicyStore) {
				t.Helper()
				client := f.newClient(t, f.base, store, f.sourceA, f.policyA, nil)
				registration, ok, err := store.Edge(ctx, root.ID)
				if err != nil || !ok || registration.Kind != authorized.RegistrationDocument ||
					registration.DocumentID != b.Document.ID {
					t.Fatalf("%s: root edge registered as %+v, %v, %v", stage, registration, ok, err)
				}
				current, ok, err := store.CurrentRevision(ctx, b.Document.ID)
				if err != nil || !ok || !slices.ContainsFunc(current.IntrinsicEdges,
					func(edge graph.Edge) bool { return edge.ID == root.ID }) {
					t.Fatalf("%s: root edge missing from b.md's intrinsic edges: %+v, %v, %v",
						stage, current.IntrinsicEdges, ok, err)
				}
				if _, ok := neighborhoodEdge(t, client, f.alice(t), b.Document.ID, root.ID); !ok {
					t.Fatalf("%s: b.md's reader lost its root edge", stage)
				}
				if got := reportedCollisions(t, store); !slices.Equal(got, []shoal.ID{root.ID}) {
					t.Fatalf("%s: catalog reports %v, want [%s]", stage, got, root.ID)
				}
				// An unscoped retrieval hydrates every document the reader
				// can see, b.md included.
				for _, text := range []string{"beta body", "gamma body"} {
					response, err := client.Retrieve(f.alice(t), retrieval.Request{
						Text: text, TopK: 5,
					})
					if err != nil {
						t.Fatalf("%s: Retrieve(%q) = %v", stage, text, err)
					}
					if len(response.Results) == 0 {
						t.Fatalf("%s: Retrieve(%q) found nothing", stage, text)
					}
				}
			}
			require("after backfill", store)
			store = reopen()
			require("after reopen", store)
			client = f.newClient(t, f.base, store, f.sourceA, f.policyA, nil)
			if _, err := client.Ingest(f.alice(t), explorer.Source{
				URI: bURI, MediaType: explorer.MediaTypeMarkdown, Content: "# beta\n\nbeta body two\n",
			}); err != nil {
				t.Fatalf("new revision of b.md = %v", err)
			}
			require("after a new revision", store)
		})
	}
}
