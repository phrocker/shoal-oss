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

	"github.com/phrocker/shoal-oss/internal/devbackfill"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestApplicationContainsEdgeOutsideTheParseRefusesOnlyItsDocument pins the
// containment of #732's precedence: an application registration of the exact
// document contains shape between two of a document's nodes that are not
// parent and child (the document node and a span) is read as intrinsic from
// the base graph, but it is not one of the parse's pairs. Registering it as
// intrinsic would make retrieval validation reject the document in every
// retrieval that hydrates it, an unscoped one of every reader of it. So the
// revision is refused, through the backfill and through Ingest alike, as
// before #732, and retrieval over the other documents keeps working.
func TestApplicationContainsEdgeOutsideTheParseRefusesOnlyItsDocument(t *testing.T) {
	const bURI = "file:///outside-parse/b.md"
	const bContent = "# beta\n\nbeta body words\n"
	const dURI = "file:///outside-parse/d.md"
	const dContent = "# delta\n\ndelta body words\n"
	for _, kind := range policyStoreKinds {
		t.Run(kind.name, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			store, reopen := kind.open(t)
			client := f.newClient(t, f.base, store, f.sourceA, f.policyA, nil)
			if _, err := client.Ingest(f.alice(t), explorer.Source{
				URI: "file:///outside-parse/c.md", MediaType: explorer.MediaTypeMarkdown,
				Content: "# gamma\n\ngamma body words\n",
			}); err != nil {
				t.Fatal(err)
			}
			ruleB := f.sourceRule(t, f.sourceB, f.policyB)

			// The backfill: b.md is stored but not yet registered.
			b, err := f.base.Ingest(ctx, explorer.Source{
				URI: bURI, MediaType: explorer.MediaTypeMarkdown, Content: bContent,
			})
			if err != nil {
				t.Fatal(err)
			}
			bOutside := outsideParseEdge(t, f, b.Document.ID)
			legacyConnect(t, f, store, bOutside, ruleB)
			if _, err := client.BackfillExistingDocumentsForDevelopment(
				f.alice(t), devbackfill.NewCapability(),
			); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
				t.Fatalf("backfill = %v, want conflict", err)
			}

			// Ingest: d.md is registered, then the application edge
			// appears, and a re-ingest reads it as intrinsic.
			d, err := client.Ingest(f.alice(t), explorer.Source{
				URI: dURI, MediaType: explorer.MediaTypeMarkdown, Content: dContent,
			})
			if err != nil {
				t.Fatal(err)
			}
			dOutside := outsideParseEdge(t, f, d.Document.ID)
			legacyConnect(t, f, store, dOutside, ruleB)
			if _, err := client.Ingest(f.alice(t), explorer.Source{
				URI: dURI, MediaType: explorer.MediaTypeMarkdown, Content: dContent,
			}); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
				t.Fatalf("re-ingest of d.md = %v, want conflict", err)
			}

			require := func(stage string, store authorized.PolicyStore) {
				t.Helper()
				client := f.newClient(t, f.base, store, f.sourceA, f.policyA, nil)
				if _, registered, err := store.CurrentRevision(ctx, b.Document.ID); err != nil || registered {
					t.Fatalf("%s: b.md registered = %v, %v", stage, registered, err)
				}
				current, ok, err := store.CurrentRevision(ctx, d.Document.ID)
				if err != nil || !ok || current.RevisionID != d.Revision.ID {
					t.Fatalf("%s: d.md current = %+v, %v, %v", stage, current, ok, err)
				}
				for _, edge := range current.IntrinsicEdges {
					if edge.ID == dOutside.ID {
						t.Fatalf("%s: the outside edge is registered as intrinsic", stage)
					}
				}
				for _, outside := range []graph.Edge{bOutside, dOutside} {
					registration, ok, err := store.Edge(ctx, outside.ID)
					if err != nil || !ok || registration.Kind != authorized.RegistrationApplication {
						t.Fatalf("%s: outside edge registered as %+v, %v, %v",
							stage, registration, ok, err)
					}
				}
				// An unscoped retrieval hydrates every document alice can
				// see, d.md included.
				for _, text := range []string{"gamma body", "delta body"} {
					response, err := client.Retrieve(f.alice(t), retrieval.Request{
						Text: text, TopK: 5,
					})
					if err != nil || len(response.Results) == 0 {
						t.Fatalf("%s: Retrieve(%q) = %d results, %v",
							stage, text, len(response.Results), err)
					}
				}
				if _, ok := neighborhoodEdge(t, client, f.alice(t), d.Document.ID, dOutside.ID); ok {
					t.Fatalf("%s: d.md's reader is served the outside edge", stage)
				}
			}
			require("after the refusals", store)
			store = reopen()
			require("after reopen", store)
		})
	}
}

// outsideParseEdge returns an edge of the exact document contains shape from
// a document's node to one of its spans, which are never parent and child.
func outsideParseEdge(
	t *testing.T, f *fixture, documentID shoal.ID,
) graph.Edge {
	t.Helper()
	outside := explorer.DocumentContainsEdge(documentID, sectionSpanEdge(t, f, documentID).To)
	if !explorer.IsDocumentContainsEdge(outside) {
		t.Fatalf("%+v is not of the document contains shape", outside)
	}
	return outside
}
