// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package explorer

import (
	"context"
	"math"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestBoundedNeighborhoodCountsSelfEdgeOnce(t *testing.T) {
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	ctx := context.Background()
	first, err := corpus.Ingest(ctx, Source{
		URI: "file:///first.txt", MediaType: MediaTypeText, Content: "first",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := corpus.Ingest(ctx, Source{
		URI: "file:///second.txt", MediaType: MediaTypeText, Content: "second",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := corpus.Connect(ctx, graph.Edge{
		ID: "a-self", From: first.Document.ID, To: first.Document.ID,
		Type: "self", Weight: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := corpus.Connect(ctx, graph.Edge{
		ID: "b-neighbor", From: first.Document.ID, To: second.Document.ID,
		Type: "neighbor", Weight: 1,
	}); err != nil {
		t.Fatal(err)
	}
	result, err := corpus.BoundedNeighborhood(ctx, BoundedNeighborhoodRequest{
		NodeIDs: []shoal.ID{first.Document.ID}, Depth: 1, Fanout: 2,
		MaxNodes: 2, Direction: GraphDirectionOutgoing,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Neighborhood.Edges) != 2 {
		t.Fatalf("edges = %+v", result.Neighborhood.Edges)
	}
	if _, err := corpus.BoundedNeighborhood(ctx, BoundedNeighborhoodRequest{
		NodeIDs: []shoal.ID{first.Document.ID}, Depth: 1, Fanout: 1,
		MaxNodes: math.MaxUint32,
	}); err != nil {
		t.Fatalf("large max_nodes should not drive allocation: %v", err)
	}
}

func TestConnectClonesRetainedEdgeProperties(t *testing.T) {
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	ctx := context.Background()
	first, err := corpus.Ingest(ctx, Source{
		URI: "file:///first.txt", MediaType: MediaTypeText, Content: "first",
	})
	if err != nil {
		t.Fatal(err)
	}
	properties := shoal.Metadata{"state": "persisted"}
	if err := corpus.Connect(ctx, graph.Edge{
		ID: "self", From: first.Document.ID, To: first.Document.ID,
		Type: "self", Weight: 1, Properties: properties,
	}); err != nil {
		t.Fatal(err)
	}
	properties["state"] = "mutated"
	if _, err := corpus.Ingest(ctx, Source{
		URI: "file:///second.txt", MediaType: MediaTypeText, Content: "second",
	}); err != nil {
		t.Fatal(err)
	}
	result, err := corpus.BoundedNeighborhood(ctx, BoundedNeighborhoodRequest{
		NodeIDs: []shoal.ID{first.Document.ID}, Depth: 1, Fanout: 10, MaxNodes: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, edge := range result.Neighborhood.Edges {
		if edge.ID == "self" && edge.Properties["state"] != "persisted" {
			t.Fatalf("retained edge properties = %+v", edge.Properties)
		}
		if edge.ID == "self" {
			found = true
		}
	}
	if !found {
		t.Fatal("retained edge not found")
	}
}
