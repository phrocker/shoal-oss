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

package harness

import (
	"context"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestExplorerToolHostUsesServerBoundedNeighbors(t *testing.T) {
	pack, _, _ := fixture(t)
	client := &boundedClientStub{}
	host := &ExplorerToolHost{Client: client, BoundedClient: client}
	request, err := NewNeighborsRequest("node-a", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	result, err := host.Neighbors(context.Background(), ToolContext{
		requestID:     "request",
		contextPackID: pack.ID(),
		context:       pack,
		correlation:   "neighbors",
		snapshot:      pack.Snapshot(),
		auth:          pack.Authorization(),
	}, request)
	if err != nil {
		t.Fatal(err)
	}
	if client.last.Fanout != 2 || client.last.MaxNodes != 3 ||
		client.last.Direction != explorer.GraphDirectionOutgoing {
		t.Fatalf("bounds not propagated: %#v", client.last)
	}
	if result.Snapshot() != pack.Snapshot() || result.Authorization() != pack.Authorization() {
		t.Fatal("pins changed")
	}
	anchors := result.Anchors()
	if len(anchors) != 1 {
		t.Fatalf("anchors = %d", len(anchors))
	}
	path, ok := anchors[0].Path()
	if !ok || path.Nodes[0].ID != "node-a" || len(path.Edges) != 1 {
		t.Fatalf("unexpected path: %#v", path)
	}
}

type boundedClientStub struct {
	last explorer.BoundedNeighborhoodRequest
}

func (b *boundedClientStub) Retrieve(context.Context, retrieval.Request) (retrieval.Response, error) {
	return retrieval.Response{}, nil
}

func (b *boundedClientStub) Ingest(context.Context, explorer.Source) (explorer.IngestResult, error) {
	return explorer.IngestResult{}, nil
}

func (b *boundedClientStub) Documents(context.Context) ([]explorer.DocumentSummary, error) {
	return nil, nil
}

func (b *boundedClientStub) Document(context.Context, shoal.ID, shoal.ID) (explorer.DocumentView, error) {
	return explorer.DocumentView{}, nil
}

func (b *boundedClientStub) Connect(context.Context, graph.Edge) error { return nil }

func (b *boundedClientStub) Neighborhood(context.Context, explorer.NeighborhoodRequest) (explorer.Neighborhood, error) {
	return explorer.Neighborhood{}, nil
}

func (b *boundedClientStub) Snapshot(context.Context) (explorer.Snapshot, error) {
	return explorer.Snapshot{}, nil
}

func (b *boundedClientStub) BoundedNeighborhood(
	_ context.Context,
	request explorer.BoundedNeighborhoodRequest,
) (explorer.BoundedNeighborhood, error) {
	b.last = request
	return explorer.BoundedNeighborhood{Neighborhood: explorer.Neighborhood{
		Nodes: []graph.Node{
			{ID: "node-a", Kind: "entity"},
			{ID: "node-b", Kind: "entity"},
		},
		Edges: []graph.Edge{{
			ID: "edge-a-b", From: "node-a", To: "node-b",
			Type: "related", Weight: 1,
		}},
	}}, nil
}
