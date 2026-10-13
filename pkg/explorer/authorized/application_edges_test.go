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

// Application edges (Connect) on extracted entities and materialized nodes
// stay in the base graph, the authorized graph and the policy catalog across
// every rebuild (#724). Before #724 the base rebuild dropped them, while the
// catalog kept their registrations, so every such edge vanished from both
// graphs at the first write after Connect.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// policyStoreKind opens a memory or a durable policy store; reopen returns
// the same memory store, or closes and reopens the durable one.
type policyStoreKind struct {
	name string
	open func(t *testing.T) (store authorized.PolicyStore, reopen func() authorized.PolicyStore)
}

var policyStoreKinds = []policyStoreKind{
	{"memory", func(t *testing.T) (authorized.PolicyStore, func() authorized.PolicyStore) {
		store := authorized.NewMemoryPolicyStore()
		return store, func() authorized.PolicyStore { return store }
	}},
	{"durable", func(t *testing.T) (authorized.PolicyStore, func() authorized.PolicyStore) {
		dir := t.TempDir()
		store, err := authorized.OpenDurablePolicyStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		current := store
		t.Cleanup(func() { _ = current.Close() })
		return store, func() authorized.PolicyStore {
			if err := current.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := authorized.OpenDurablePolicyStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			current = reopened
			return reopened
		}
	}},
}

type authorizedEdgeWorld struct {
	t      *testing.T
	f      *fixture
	store  authorized.PolicyStore
	client *authorized.Client
	skill  explorer.IngestResult
	other  explorer.IngestResult
	edges  []graph.Edge
}

func (w *authorizedEdgeWorld) ingest(uri, content string) explorer.IngestResult {
	w.t.Helper()
	result, err := w.client.Ingest(w.f.admin(w.t), explorer.Source{
		URI: uri, MediaType: explorer.MediaTypeMarkdown, Content: content,
	})
	if err != nil {
		w.t.Fatal(err)
	}
	return result
}

func (w *authorizedEdgeWorld) extract() explorer.ExtractionResult {
	w.t.Helper()
	extracted, err := w.client.ExtractDocument(w.f.bob(w.t), explorer.ExtractionRequest{
		DocumentID: w.skill.Document.ID, RevisionID: w.skill.Revision.ID,
		Version: authorizedSkillsOntologyVersion(w.t),
	})
	if err != nil {
		w.t.Fatal(err)
	}
	return extracted
}

func (w *authorizedEdgeWorld) materialize(namespace string) explorer.GraphMaterializationResult {
	w.t.Helper()
	snapshot, err := w.f.base.Snapshot(context.Background())
	if err != nil {
		w.t.Fatal(err)
	}
	result, err := w.client.MaterializeGraph(w.f.admin(w.t), explorer.GraphMaterializationRequest{
		Namespace: []byte(namespace), MutationID: shoal.ID("mutation-" + namespace),
		SourceID: w.f.sourceB, PolicyID: w.f.policyB,
		ExpectedSnapshot: snapshot,
		Nodes: []explorer.GraphNodeSpec{
			{Key: []byte("team"), Kind: "team", Properties: shoal.Metadata{"name": "Team"}},
			{Key: []byte("person"), Kind: "person", Properties: shoal.Metadata{"name": "Person"}},
		},
	})
	if err != nil {
		w.t.Fatal(err)
	}
	return result
}

func (w *authorizedEdgeWorld) connect(local string, from, to shoal.ID, edgeType string) {
	w.t.Helper()
	edge := graph.Edge{
		ID:   appEdgeID(w.t, w.client, w.f.admin(w.t), shoal.ID(local)),
		From: from, To: to, Type: edgeType, Weight: 1,
	}
	if err := w.client.Connect(w.f.admin(w.t), edge); err != nil {
		w.t.Fatalf("Connect(%s): %v", local, err)
	}
	w.edges = append(w.edges, edge)
}

// requireAllEdges checks every connected edge is in the base graph, in the
// authorized graph for a reader of its source, and in the catalog.
func (w *authorizedEdgeWorld) requireAllEdges(stage string) {
	w.t.Helper()
	ctx := context.Background()
	ids := make([]shoal.ID, len(w.edges))
	for i, edge := range w.edges {
		ids[i] = edge.ID
		request := explorer.NeighborhoodRequest{
			NodeIDs: []shoal.ID{edge.From}, Depth: 1, EdgeTypes: []string{edge.Type},
		}
		base, err := w.f.base.Neighborhood(ctx, request)
		if err != nil {
			w.t.Fatal(err)
		}
		if !hasEdgeContent(base.Edges, edge) {
			w.t.Errorf("%s: edge %s missing from the base graph", stage, edge.ID)
		}
		visible, err := w.client.Neighborhood(w.f.bob(w.t), request)
		if err != nil {
			w.t.Fatal(err)
		}
		if !hasEdgeContent(visible.Edges, edge) {
			w.t.Errorf("%s: edge %s missing from the authorized graph", stage, edge.ID)
		}
	}
	registrations, err := w.store.Edges(ctx, ids)
	if err != nil {
		w.t.Fatal(err)
	}
	for _, edge := range w.edges {
		registration, ok := registrations[edge.ID]
		if !ok || registration.Kind != authorized.RegistrationApplication ||
			!hasEdgeContent([]graph.Edge{registration.Edge}, edge) {
			w.t.Errorf("%s: catalog registration of %s = %+v, %v", stage, edge.ID, registration, ok)
		}
	}
}

func hasEdgeContent(edges []graph.Edge, want graph.Edge) bool {
	for _, edge := range edges {
		if edge.ID == want.ID {
			return edge.From == want.From && edge.To == want.To &&
				edge.Type == want.Type && edge.Weight == want.Weight
		}
	}
	return false
}

// recordAndFold records one interaction over the plain document through the
// authorized client and folds it.
func (w *authorizedEdgeWorld) recordAndFold() {
	w.t.Helper()
	f := w.f
	view, err := w.client.Document(f.admin(w.t), w.other.Document.ID, w.other.Revision.ID)
	if err != nil {
		w.t.Fatal(err)
	}
	snapshot, err := f.base.Snapshot(context.Background())
	if err != nil {
		w.t.Fatal(err)
	}
	f.clock.Set(snapshot.AsOf.Add(time.Second))
	decision := f.decision(w.t, "interaction-writer",
		[][]byte{f.sourceB}, [][]byte{f.policyB}, allOperations)
	ctx := f.context(w.t, decision)
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		w.t.Fatal(err)
	}
	session := interaction.Session{
		ID:                       interaction.DerivedID("session", "application-edges"),
		RecordedAt:               f.clock.Now(),
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		SeedNodeIDs:              []shoal.ID{firstSpanID(w.t, view)},
	}
	if err := w.client.RecordInteraction(ctx, session); err != nil {
		w.t.Fatal(err)
	}
	w.requireAllEdges("after interaction")
	if _, err := w.client.FoldInteractions(ctx, explorer.FoldRequest{
		SessionIDs: []shoal.ID{session.ID},
	}); err != nil {
		w.t.Fatal(err)
	}
	w.requireAllEdges("after fold")
}

func TestAuthorizedApplicationEdgesOnDerivedNodesSurviveEveryRebuild(t *testing.T) {
	for _, kind := range policyStoreKinds {
		t.Run(kind.name, func(t *testing.T) {
			f := newFixture(t)
			store, reopen := kind.open(t)
			w := &authorizedEdgeWorld{t: t, f: f, store: store}
			w.client = f.newClient(t, f.base, store, f.sourceB, f.policyB, nil)
			w.skill = w.ingest("file:///app-edges/SKILL.md", authorizedSkillMarkdown)
			w.other = w.ingest("file:///app-edges/other.md", "# Other\n\nplain words\n")
			extracted := w.extract()
			if len(extracted.EntityNodeIDs) < 2 {
				t.Fatalf("extraction produced %d entities", len(extracted.EntityNodeIDs))
			}
			entity, peer := extracted.EntityNodeIDs[0], extracted.EntityNodeIDs[1]
			materialized := w.materialize("app-edges")
			team, person := materialized.GraphNodes[0].ID, materialized.GraphNodes[1].ID
			document := w.other.Document.ID
			for i := 0; i < 24; i++ {
				w.connect(fmt.Sprintf("entity-to-document-%02d", i), entity, document, "provides_tool")
			}
			w.connect("document-to-entity", document, entity, "mentions")
			w.connect("entity-to-entity", entity, peer, "related")
			w.connect("materialized-to-document", team, document, "documented_by")
			w.connect("document-to-materialized", document, person, "about")
			w.connect("entity-to-materialized", entity, team, "owned_by")
			w.connect("document-to-document", w.skill.Document.ID, document, "see_also")
			w.requireAllEdges("after connect")

			w.ingest("file:///app-edges/third.md", "# Third\n\nmore words\n")
			w.requireAllEdges("after ingest")
			w.extract()
			w.requireAllEdges("after re-extract")
			w.recordAndFold()
			w.materialize("app-edges-two")
			w.requireAllEdges("after materialization")

			// Reopen the base and the catalog.
			if err := f.base.Close(); err != nil {
				t.Fatal(err)
			}
			base, err := explorer.Open(f.dataDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = base.Close() })
			f.base = base
			w.store = reopen()
			w.client = f.newClient(t, base, w.store, f.sourceB, f.policyB, nil)
			w.requireAllEdges("after reopen")
		})
	}
}

// countingBase counts the Connect calls that reach the base.
type countingBase struct {
	*explorer.Explorer
	mu       sync.Mutex
	connects int
}

func (b *countingBase) Connect(ctx context.Context, edge graph.Edge) error {
	b.mu.Lock()
	b.connects++
	b.mu.Unlock()
	return b.Explorer.Connect(ctx, edge)
}

// TestAuthorizedConnectRefusesDifferentContentAtTheCatalog pins that the
// catalog reservation, not only the base, refuses a second Connect of an ID
// with different content: the base is never asked, and neither the
// registration nor the base edge changes.
func TestAuthorizedConnectRefusesDifferentContentAtTheCatalog(t *testing.T) {
	for _, kind := range policyStoreKinds {
		t.Run(kind.name, func(t *testing.T) {
			f := newFixture(t)
			store, _ := kind.open(t)
			base := &countingBase{Explorer: f.base}
			client := f.newClient(t, base, store, f.sourceB, f.policyB, nil)
			w := &authorizedEdgeWorld{t: t, f: f, store: store, client: client}
			w.skill = w.ingest("file:///catalog-refusal/SKILL.md", authorizedSkillMarkdown)
			w.other = w.ingest("file:///catalog-refusal/other.md", "# Other\n")
			entity := w.extract().EntityNodeIDs[0]
			w.connect("entity-to-document", entity, w.other.Document.ID, "provides_tool")
			original := w.edges[0]
			if base.connects != 1 {
				t.Fatalf("base Connect calls = %d, want 1", base.connects)
			}
			for _, changed := range []graph.Edge{
				{ID: original.ID, From: original.From, To: original.To, Type: original.Type, Weight: 2},
				{ID: original.ID, From: original.To, To: original.From, Type: original.Type, Weight: 1},
			} {
				err := client.Connect(f.admin(t), changed)
				if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
					t.Fatalf("Connect with different content = %v, want conflict", err)
				}
			}
			if base.connects != 1 {
				t.Fatalf("a refused Connect reached the base: %d calls", base.connects)
			}
			// The identical retry is a no-op that succeeds.
			if err := client.Connect(f.admin(t), original); err != nil {
				t.Fatalf("identical retry = %v", err)
			}
			w.ingest("file:///catalog-refusal/third.md", "# Third\n")
			w.requireAllEdges("after the refused overwrites")
		})
	}
}

// divergentBase serves one edge with content other than what the catalog
// registered, as a base edge overwritten while #724 hid it would be.
type divergentBase struct {
	*explorer.Explorer
	edgeID shoal.ID
}

func (b *divergentBase) rewrite(edges []graph.Edge) {
	for i := range edges {
		if edges[i].ID == b.edgeID {
			edges[i].Weight = 0.25
		}
	}
}

func (b *divergentBase) Neighborhood(
	ctx context.Context, request explorer.NeighborhoodRequest,
) (explorer.Neighborhood, error) {
	result, err := b.Explorer.Neighborhood(ctx, request)
	b.rewrite(result.Edges)
	return result, err
}

func (b *divergentBase) BoundedNeighborhood(
	ctx context.Context, request explorer.BoundedNeighborhoodRequest,
) (explorer.BoundedNeighborhood, error) {
	result, err := b.Explorer.BoundedNeighborhood(ctx, request)
	b.rewrite(result.Neighborhood.Edges)
	return result, err
}

// TestAuthorizedReadsWithholdAnEdgeThatDivergesFromItsRegistration defines
// what reads do with an application edge whose base content differs from its
// catalog registration: before #724 a base Connect could overwrite an edge
// the graph hid, and the fix makes it reappear. The edge fails closed: every
// authorized read withholds it, as though it did not exist, and serves
// neither the base's content nor the registration's. Every other edge is
// served as before, so one divergent edge never fails the whole read.
func TestAuthorizedReadsWithholdAnEdgeThatDivergesFromItsRegistration(t *testing.T) {
	for _, kind := range policyStoreKinds {
		t.Run(kind.name, func(t *testing.T) {
			f := newFixture(t)
			store, _ := kind.open(t)
			w := &authorizedEdgeWorld{t: t, f: f, store: store}
			w.client = f.newClient(t, f.base, store, f.sourceB, f.policyB, nil)
			w.skill = w.ingest("file:///divergent/SKILL.md", authorizedSkillMarkdown)
			w.other = w.ingest("file:///divergent/other.md", "# Other\n")
			entity := w.extract().EntityNodeIDs[0]
			w.connect("divergent", entity, w.other.Document.ID, "provides_tool")
			w.connect("faithful", entity, w.other.Document.ID, "provides_tool")
			divergent, faithful := w.edges[0], w.edges[1]

			base := &divergentBase{Explorer: f.base, edgeID: divergent.ID}
			client := f.newClient(t, base, store, f.sourceB, f.policyB, nil)
			around, err := client.Neighborhood(f.bob(t), explorer.NeighborhoodRequest{
				NodeIDs: []shoal.ID{entity}, Depth: 1, EdgeTypes: []string{"provides_tool"},
			})
			if err != nil {
				t.Fatalf("a divergent edge failed the read: %v", err)
			}
			bounded, err := client.BoundedNeighborhood(f.bob(t), explorer.BoundedNeighborhoodRequest{
				NodeIDs: []shoal.ID{entity}, Depth: 1, EdgeTypes: []string{"provides_tool"},
				Fanout: 64, MaxNodes: 64, MaxScannedEdges: 256,
			})
			if err != nil {
				t.Fatalf("a divergent edge failed the bounded read: %v", err)
			}
			for name, edges := range map[string][]graph.Edge{
				"neighborhood": around.Edges, "bounded": bounded.Neighborhood.Edges,
			} {
				for _, edge := range edges {
					if edge.ID == divergent.ID {
						t.Errorf("%s served the divergent edge: %+v", name, edge)
					}
				}
				if !hasEdgeContent(edges, faithful) {
					t.Errorf("%s lost the faithful edge", name)
				}
			}
			if _, err := client.Neighborhood(f.bob(t), explorer.NeighborhoodRequest{
				NodeIDs: []shoal.ID{w.other.Document.ID}, Depth: 1,
			}); err != nil {
				t.Fatalf("read from the other endpoint failed: %v", err)
			}
		})
	}
}

// TestReappearingApplicationEdgeToRelabelledEntityStaysClosed pins that an
// application edge to an extracted entity is governed by its catalog
// registration and its Connect binding (#703), not by whether the base graph
// showed it: an edge that #724 hid while its entity's document was labelled
// reappears closed to a reader without the label. Neither the relabel nor
// the label migration reads the base graph to find what to narrow, so the
// edges they narrow are the same whether or not the graph showed them.
func TestReappearingApplicationEdgeToRelabelledEntityStaysClosed(t *testing.T) {
	t.Run("relabel", func(t *testing.T) {
		withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
			w := newExtractedEdgeWorld(t, store)
			public := w.ingest("file:///reappear/public.md", "# Public\n\nopen words\n", nil)
			const uri = "file:///reappear/SKILL.md"
			body := skillBody("delta", "delta-cli")
			entity := w.extract(w.ingest(uri, body, nil)).EntityNodeIDs[0]
			edgeID := appEdgeID(t, w.client, w.owner, "public-to-entity")
			if err := w.client.Connect(w.owner, graph.Edge{
				ID: edgeID, From: public.Document.ID, To: entity, Type: "mentions", Weight: 1,
			}); err != nil {
				t.Fatal(err)
			}
			if !w.edgeVisible(w.reader, public.Document.ID, edgeID) {
				t.Fatal("edge not visible before the relabel")
			}
			w.ingest(uri, body+"\nNow secret.\n", shoal.Metadata{"shoal.visibility": "secret"})
			// Any later write rebuilds the base graph, which before #724
			// dropped the edge and now shows it again.
			w.ingest("file:///reappear/later.md", "# Later\n", nil)
			if !baseShowsEdge(t, w.f, public.Document.ID, edgeID) {
				t.Fatal("the base graph does not show the edge")
			}
			if w.edgeVisible(w.f.labelIngester(t, "reader"), public.Document.ID, edgeID) {
				t.Fatal("reader without the label sees an edge to the relabelled document's entity")
			}
		})
	})
	t.Run("label migration", func(t *testing.T) {
		withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
			f := newFixture(t)
			legacy := &legacyStore{PolicyStore: store}
			selector := f.staticSelector(t, f.sourceA, f.policyA)
			client := f.labelClient(t, f.base, legacy, selector, selector)
			owner := f.labelIngester(t, "owner", "secret")
			public, err := client.Ingest(owner, explorer.Source{
				URI: "file:///reappear-legacy/public.md", MediaType: explorer.MediaTypeMarkdown,
				Content: "# Public\n",
			})
			if err != nil {
				t.Fatal(err)
			}
			legacy.setStrip(true)
			document, err := client.Ingest(owner, explorer.Source{
				URI: "file:///reappear-legacy/SKILL.md", MediaType: explorer.MediaTypeMarkdown,
				Content:  authorizedSkillMarkdown,
				Metadata: shoal.Metadata{interaction.PropertyVisibility: "secret"},
			})
			if err != nil {
				t.Fatal(err)
			}
			extracted, err := client.ExtractDocument(owner, explorer.ExtractionRequest{
				DocumentID: document.Document.ID, RevisionID: document.Revision.ID,
				Version: authorizedSkillsOntologyVersion(t),
			})
			if err != nil {
				t.Fatal(err)
			}
			entity := extracted.EntityNodeIDs[0]
			edgeID := appEdgeID(t, client, owner, "legacy-public-to-entity")
			if err := client.Connect(owner, graph.Edge{
				ID: edgeID, From: public.Document.ID, To: entity, Type: "mentions", Weight: 1,
			}); err != nil {
				t.Fatal(err)
			}
			legacy.setStrip(false)
			visible := func(ctx context.Context) bool {
				t.Helper()
				around, err := client.Neighborhood(ctx, explorer.NeighborhoodRequest{
					NodeIDs: []shoal.ID{public.Document.ID}, Depth: 1,
				})
				if err != nil {
					t.Fatal(err)
				}
				for _, edge := range around.Edges {
					if edge.ID == edgeID {
						return true
					}
				}
				return false
			}
			if !visible(f.labelIngester(t, "before")) {
				t.Fatal("legacy edge not open before the migration")
			}
			migrate(t, client)
			if _, err := client.Ingest(owner, explorer.Source{
				URI: "file:///reappear-legacy/later.md", MediaType: explorer.MediaTypeMarkdown,
				Content: "# Later\n",
			}); err != nil {
				t.Fatal(err)
			}
			if !baseShowsEdge(t, f, public.Document.ID, edgeID) {
				t.Fatal("the base graph does not show the edge")
			}
			if visible(f.labelIngester(t, "reader")) {
				t.Fatal("reader without the label sees an edge to the migrated document's entity")
			}
		})
	})
}

func baseShowsEdge(t *testing.T, f *fixture, from, edgeID shoal.ID) bool {
	t.Helper()
	around, err := f.base.Neighborhood(context.Background(), explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{from}, Depth: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, edge := range around.Edges {
		if edge.ID == edgeID {
			return true
		}
	}
	return false
}
