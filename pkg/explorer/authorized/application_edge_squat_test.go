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

// A pre-#699 application edge holding a document's intrinsic edge ID (#732).
// Before #699 an authorized Connect accepted any edge ID, and a document's
// root contains edge ID is derived from its URI. #724 let the intrinsic edge
// win the ID in the base graph; these tests pin that the policy catalog
// agrees: the squatted document's ingest succeeds, its intrinsic edge is
// governed by the document's rule alone, and the application edge is
// withheld from everyone and reported.

import (
	"context"
	"slices"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type applicationEdgeCollisionReporter interface {
	ApplicationEdgeCollisions(context.Context) ([]shoal.ID, error)
}

// rootContainsEdge returns the contains edge from a document ingested at uri
// to its root section, read from a scratch corpus: the ID depends on the URI
// only, which is what let a pre-#699 Connect take it before the document
// existed.
func rootContainsEdge(t *testing.T, uri, content string) graph.Edge {
	t.Helper()
	ctx := context.Background()
	scratch, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scratch.Close() }()
	if _, err := scratch.Ingest(ctx, explorer.Source{
		URI: uri, MediaType: explorer.MediaTypeMarkdown, Content: content,
	}); err != nil {
		t.Fatal(err)
	}
	document := explorer.SourceDocumentID(uri)
	around, err := scratch.Neighborhood(ctx, explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{document}, Depth: 1, EdgeTypes: []string{"contains"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, edge := range around.Edges {
		if edge.From == document {
			return edge
		}
	}
	t.Fatal("scratch document has no root contains edge")
	return graph.Edge{}
}

func (f *fixture) sourceRule(t *testing.T, source, policy []byte) authorized.AccessRule {
	t.Helper()
	selected, err := auth.NewPolicy(auth.PolicyConfig{
		AuthorizationDomain: f.domain, SourceID: source, GrantPolicyID: policy, Epoch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	rule, err := authorized.NewAccessRule(selected)
	if err != nil {
		t.Fatal(err)
	}
	return rule
}

// legacyConnect writes an application edge the way an authorized Connect did
// before #699: any ID, no revision binding.
func legacyConnect(
	t *testing.T, f *fixture, store authorized.PolicyStore, edge graph.Edge,
	rule authorized.AccessRule,
) {
	t.Helper()
	ctx := context.Background()
	registration := authorized.EdgeRegistration{
		Edge: edge, Rule: rule, Kind: authorized.RegistrationApplication,
	}
	if err := store.ReserveEdge(ctx, registration); err != nil {
		t.Fatalf("reserve the legacy edge: %v", err)
	}
	if err := f.base.Connect(ctx, edge); err != nil {
		t.Fatalf("base Connect of the legacy edge: %v", err)
	}
	if err := store.PutEdge(ctx, registration); err != nil {
		t.Fatalf("register the legacy edge: %v", err)
	}
}

// edgeShowingBase serves extra in every neighborhood holding both of its
// endpoints, as a base that did not withhold it would.
type edgeShowingBase struct {
	*explorer.Explorer
	extra graph.Edge
}

func (b *edgeShowingBase) Neighborhood(
	ctx context.Context, request explorer.NeighborhoodRequest,
) (explorer.Neighborhood, error) {
	result, err := b.Explorer.Neighborhood(ctx, request)
	if err != nil {
		return result, err
	}
	if len(request.EdgeTypes) > 0 && !slices.Contains(request.EdgeTypes, b.extra.Type) {
		return result, nil
	}
	from, to := false, false
	for _, node := range result.Nodes {
		from = from || node.ID == b.extra.From
		to = to || node.ID == b.extra.To
	}
	if from && to {
		result.Edges = append(result.Edges, b.extra)
	}
	return result, nil
}

func neighborhoodEdge(
	t *testing.T, client *authorized.Client, ctx context.Context, seed, edgeID shoal.ID,
) (graph.Edge, bool) {
	t.Helper()
	around, err := client.Neighborhood(ctx, explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{seed}, Depth: 1,
	})
	if shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		return graph.Edge{}, false
	}
	if err != nil {
		t.Fatalf("neighborhood of %s: %v", seed, err)
	}
	for _, edge := range around.Edges {
		if edge.ID == edgeID {
			return edge, true
		}
	}
	return graph.Edge{}, false
}

func reportedCollisions(t *testing.T, store authorized.PolicyStore) []shoal.ID {
	t.Helper()
	reporter, ok := store.(applicationEdgeCollisionReporter)
	if !ok {
		t.Fatalf("%T reports no application edge collisions", store)
	}
	reported, err := reporter.ApplicationEdgeCollisions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return reported
}

// TestAuthorizedIngestOfALegacySquattedDocument is the #732 repro: a
// pre-#699 application edge holds b.md's root contains edge ID, under a rule
// (source B) b.md's readers (source A) do not hold. The ingest of b.md
// succeeds, b.md is readable by its readers, its intrinsic edge is served
// under b.md's rule (the application rule does not govern it), the
// application edge is withheld from everyone, even a reader of both rules
// and even if the base showed it, and both the catalog and the base report
// the collision. All of it holds across a reopen, a later revision of b.md
// and a refused retry of the squatter.
func TestAuthorizedIngestOfALegacySquattedDocument(t *testing.T) {
	const bURI = "file:///squat/b.md"
	for _, kind := range policyStoreKinds {
		t.Run(kind.name, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			store, reopen := kind.open(t)
			client := f.newClient(t, f.base, store, f.sourceA, f.policyA, nil)
			a, err := client.Ingest(f.alice(t), explorer.Source{
				URI: "file:///squat/a.md", MediaType: explorer.MediaTypeMarkdown,
				Content: "# alpha\n\nalpha body\n",
			})
			if err != nil {
				t.Fatal(err)
			}
			aRoot := rootContainsEdge(t, "file:///squat/a.md", "# alpha\n\nalpha body\n")
			intrinsic := rootContainsEdge(t, bURI, "# beta\n\nbeta body\n")
			squat := graph.Edge{
				ID: intrinsic.ID, From: a.Document.ID, To: aRoot.To,
				Type: "related", Weight: 1,
			}
			legacyConnect(t, f, store, squat, f.sourceRule(t, f.sourceB, f.policyB))

			b, err := client.Ingest(f.alice(t), explorer.Source{
				URI: bURI, MediaType: explorer.MediaTypeMarkdown, Content: "# beta\n\nbeta body\n",
			})
			if err != nil {
				t.Fatalf("ingest of the squatted document = %v", err)
			}

			require := func(stage string, store authorized.PolicyStore) {
				t.Helper()
				client := f.newClient(t, f.base, store, f.sourceA, f.policyA, nil)
				if _, err := client.Document(f.alice(t), b.Document.ID, ""); err != nil {
					t.Fatalf("%s: b.md unreadable to its reader: %v", stage, err)
				}
				if _, err := client.Document(f.bob(t), b.Document.ID, ""); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
					t.Fatalf("%s: b.md read by a non-reader = %v", stage, err)
				}
				// Only b.md's rule governs the intrinsic edge: alice holds
				// it and not the application edge's rule.
				served, ok := neighborhoodEdge(t, client, f.alice(t), b.Document.ID, intrinsic.ID)
				if !ok || served.Type != "contains" || served.From != b.Document.ID {
					t.Fatalf("%s: intrinsic edge served as %+v, %v", stage, served, ok)
				}
				registration, ok, err := store.Edge(ctx, intrinsic.ID)
				if err != nil || !ok || registration.Kind != authorized.RegistrationDocument ||
					registration.DocumentID != b.Document.ID ||
					registration.Edge.Type != "contains" {
					t.Fatalf("%s: catalog resolves the ID to %+v, %v, %v", stage, registration, ok, err)
				}
				// The application edge is withheld for everyone, even for a
				// reader of both rules, and even if the base showed it.
				showing := f.newClient(t, &edgeShowingBase{Explorer: f.base, extra: squat},
					store, f.sourceA, f.policyA, nil)
				for name, reader := range map[string]context.Context{
					"alice": f.alice(t), "bob": f.bob(t), "admin": f.admin(t),
				} {
					for _, c := range []*authorized.Client{client, showing} {
						if edge, ok := neighborhoodEdge(t, c, reader, a.Document.ID, squat.ID); ok &&
							edge.Type == squat.Type {
							t.Fatalf("%s: %s is served the withheld application edge", stage, name)
						}
					}
				}
				if got := reportedCollisions(t, store); !slices.Equal(got, []shoal.ID{squat.ID}) {
					t.Fatalf("%s: catalog reports %v, want [%s]", stage, got, squat.ID)
				}
				baseReported, err := f.base.ApplicationEdgeCollisions(ctx)
				if err != nil || !slices.Equal(baseReported, []shoal.ID{squat.ID}) {
					t.Fatalf("%s: base reports %v, %v", stage, baseReported, err)
				}
			}
			require("after ingest", store)
			store = reopen()
			require("after reopen", store)

			client = f.newClient(t, f.base, store, f.sourceA, f.policyA, nil)
			second, err := client.Ingest(f.alice(t), explorer.Source{
				URI: bURI, MediaType: explorer.MediaTypeMarkdown, Content: "# beta\n\nbeta two\n",
			})
			if err != nil || second.Disposition != explorer.IngestApplied {
				t.Fatalf("new revision of the squatted document = %+v, %v", second.Disposition, err)
			}
			require("after a new revision", store)
			// The squatter can never take its ID back while the intrinsic
			// edge is current.
			retry := authorized.EdgeRegistration{
				Edge: squat, Rule: f.sourceRule(t, f.sourceB, f.policyB),
				Kind: authorized.RegistrationApplication,
			}
			if err := store.ReserveEdge(ctx, retry); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
				t.Fatalf("reserve the squatter again = %v, want conflict", err)
			}
			if err := store.PutEdge(ctx, retry); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
				t.Fatalf("register the squatter again = %v, want conflict", err)
			}
			store = reopen()
			require("after a second reopen", store)
		})
	}
}

// TestAuthorizedIngestKeepsAnApplicationContainsEdgeUnderItsOwnRule pins the
// guard on the other side: the client reads a revision's intrinsic edges
// from the base graph, which shows a stored application edge of type
// "contains" between the document's own nodes. Such an edge must stay
// governed by its application registration, never be registered under the
// document's rule, and never refuse the revision. One whose base content
// diverged from its registration fails the ingest closed rather than be
// registered as intrinsic.
func TestAuthorizedIngestKeepsAnApplicationContainsEdgeUnderItsOwnRule(t *testing.T) {
	const uri = "file:///app-contains/b.md"
	for _, kind := range policyStoreKinds {
		t.Run(kind.name, func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t)
			store, reopen := kind.open(t)
			client := f.newClient(t, f.base, store, f.sourceA, f.policyA, nil)
			b, err := client.Ingest(f.alice(t), explorer.Source{
				URI: uri, MediaType: explorer.MediaTypeMarkdown, Content: "# beta\n\nbeta body\n",
			})
			if err != nil {
				t.Fatal(err)
			}
			root := rootContainsEdge(t, uri, "# beta\n\nbeta body\n")
			application := graph.Edge{
				ID: "legacy-application-contains", From: b.Document.ID, To: root.To,
				Type: "contains", Weight: 1,
			}
			legacyConnect(t, f, store, application, f.sourceRule(t, f.sourceB, f.policyB))

			if _, err := client.Ingest(f.alice(t), explorer.Source{
				URI: uri, MediaType: explorer.MediaTypeMarkdown, Content: "# beta\n\nbeta two\n",
			}); err != nil {
				t.Fatalf("new revision beside an application contains edge = %v", err)
			}
			require := func(stage string, store authorized.PolicyStore) {
				t.Helper()
				client := f.newClient(t, f.base, store, f.sourceA, f.policyA, nil)
				registration, ok, err := store.Edge(ctx, application.ID)
				if err != nil || !ok || registration.Kind != authorized.RegistrationApplication {
					t.Fatalf("%s: application contains edge registered as %+v, %v, %v",
						stage, registration, ok, err)
				}
				current, ok, err := store.CurrentRevision(ctx, b.Document.ID)
				if err != nil || !ok {
					t.Fatalf("%s: current revision = %v, %v", stage, ok, err)
				}
				for _, edge := range current.IntrinsicEdges {
					if edge.ID == application.ID {
						t.Fatalf("%s: application edge registered as intrinsic", stage)
					}
				}
				if _, ok := neighborhoodEdge(t, client, f.alice(t), b.Document.ID, application.ID); ok {
					t.Fatalf("%s: the document's reader is served the application edge", stage)
				}
				if _, ok := neighborhoodEdge(t, client, f.admin(t), b.Document.ID, application.ID); !ok {
					t.Fatalf("%s: a reader of both rules lost the application edge", stage)
				}
				if got := reportedCollisions(t, store); len(got) != 0 {
					t.Fatalf("%s: catalog reports %v, want none", stage, got)
				}
			}
			require("after ingest", store)
			store = reopen()
			require("after reopen", store)

			// The base shows the application edge with content other than
			// its registration: it is refused, not registered as intrinsic.
			divergent := application
			divergent.Weight = 0.5
			client = f.newClient(t, &replacingBase{Explorer: f.base, edge: divergent},
				store, f.sourceA, f.policyA, nil)
			if _, err := client.Ingest(f.alice(t), explorer.Source{
				URI: uri, MediaType: explorer.MediaTypeMarkdown, Content: "# beta\n\nbeta three\n",
			}); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
				t.Fatalf("ingest beside a divergent application contains edge = %v, want conflict", err)
			}
			registration, ok, err := store.Edge(ctx, application.ID)
			if err != nil || !ok || registration.Kind != authorized.RegistrationApplication {
				t.Fatalf("divergent edge registered as %+v, %v, %v", registration, ok, err)
			}
		})
	}
}

// replacingBase serves edge in place of the base edge with its ID.
type replacingBase struct {
	*explorer.Explorer
	edge graph.Edge
}

func (b *replacingBase) Neighborhood(
	ctx context.Context, request explorer.NeighborhoodRequest,
) (explorer.Neighborhood, error) {
	result, err := b.Explorer.Neighborhood(ctx, request)
	for i := range result.Edges {
		if result.Edges[i].ID == b.edge.ID {
			result.Edges[i] = b.edge
		}
	}
	return result, err
}
