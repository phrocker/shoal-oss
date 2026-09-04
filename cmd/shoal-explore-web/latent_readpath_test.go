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

package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestLatentAssertionsUseStartedEmbeddedWorkspaceReadPath(t *testing.T) {
	data := t.TempDir()
	sourceID := seedStartedLatentWorkspace(t, data)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output := &lockedBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{
			"-data", data,
			"-listen", "127.0.0.1:0",
			"-dev-auth",
		}, output)
	}()
	baseURL := waitForListeningURL(t, output)
	getMeta(t, baseURL)

	request, err := json.Marshal(webapi.NeighborhoodRequest{
		NodeIDs: []shoal.ID{sourceID},
		Depth:   1, Fanout: 8, MaxNodes: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	body := postJSON(t, baseURL+"/api/v1/neighborhood", string(request))
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode neighborhood %s: %v", body, err)
	}
	neighborhood, ok := decoded["neighborhood"].(map[string]any)
	if !ok {
		t.Fatalf("neighborhood response missing graph: %s", body)
	}
	assertions, ok := neighborhood["assertions"].([]any)
	if !ok || len(assertions) != 1 {
		t.Fatalf("started workspace assertions = %v in %s", neighborhood["assertions"], body)
	}
	assertion, ok := assertions[0].(map[string]any)
	if !ok || assertion["origin"] != "derived" {
		t.Fatalf("assertion did not preserve derived origin: %v", assertions[0])
	}
	evidence, ok := assertion["evidence"].([]any)
	if !ok || len(evidence) != 1 {
		t.Fatalf("derived assertion evidence missing: %v", assertion)
	}
	derivation, ok := evidence[0].(map[string]any)["derivation"].(map[string]any)
	if !ok || derivation["iterator_name"] != explorer.LatentLinkDefaultIteratorName {
		t.Fatalf("derivation did not survive HTTP read path: %v", evidence[0])
	}
	var typed webapi.NeighborhoodResponse
	if err := json.Unmarshal(body, &typed); err != nil {
		t.Fatalf("decode typed neighborhood %s: %v", body, err)
	}
	assertionID := typed.Neighborhood.Assertions[0].ID()
	producerRequest, err := json.Marshal(webapi.NeighborhoodRequest{
		NodeIDs: []shoal.ID{assertionID},
		Depth:   1, Fanout: 8, MaxNodes: 8,
		EdgeTypes: []string{graph.EdgeTypeProduced},
	})
	if err != nil {
		t.Fatal(err)
	}
	body = postJSON(t, baseURL+"/api/v1/neighborhood", string(producerRequest))
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode producer neighborhood %s: %v", body, err)
	}
	neighborhood, ok = decoded["neighborhood"].(map[string]any)
	if !ok {
		t.Fatalf("producer neighborhood response missing graph: %s", body)
	}
	nodes, ok := neighborhood["nodes"].([]any)
	if !ok || len(nodes) != 2 {
		t.Fatalf("producer provenance nodes = %v in %s", neighborhood["nodes"], body)
	}
	edges, ok := neighborhood["edges"].([]any)
	if !ok || len(edges) != 1 {
		t.Fatalf("producer provenance edges = %v in %s", neighborhood["edges"], body)
	}
	producedEdge, ok := edges[0].(map[string]any)
	if !ok || producedEdge["type"] != graph.EdgeTypeProduced {
		t.Fatalf("producer derivation edge did not survive HTTP read path: %v", edges[0])
	}
	assertions, ok = neighborhood["assertions"].([]any)
	if !ok || len(assertions) != 1 {
		t.Fatalf("producer assertion list = %v in %s", neighborhood["assertions"], body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
}

func seedStartedLatentWorkspace(t *testing.T, data string) shoal.ID {
	t.Helper()
	ctx := context.Background()
	corpus, err := explorer.Open(data)
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	created := time.Date(2026, time.September, 4, 12, 0, 0, 0, time.UTC)
	var sourceID, targetID shoal.ID
	for index, source := range []explorer.Source{
		{
			URI: "file:///latent-source.txt", Title: "latent source",
			MediaType: explorer.MediaTypeText, Content: "latent source",
			Metadata: shoal.Metadata{"id": "source"},
		},
		{
			URI: "file:///latent-target.txt", Title: "latent target",
			MediaType: explorer.MediaTypeText, Content: "latent target",
			Metadata: shoal.Metadata{"id": "target"},
		},
	} {
		result, err := corpus.IngestWithOptions(
			ctx, source, explorer.IngestOptions{CreatedAt: created},
		)
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			sourceID = result.Document.ID
		} else {
			targetID = result.Document.ID
		}
	}
	if err := corpus.PutLatentLinkCells(ctx, []explorer.LatentLinkCell{{
		Row:             []byte("cell-a:" + string(sourceID)),
		ColumnFamily:    []byte("link"),
		ColumnQualifier: []byte(targetID),
		Timestamp:       42,
		Value:           []byte("0.91"),
	}}); err != nil {
		t.Fatal(err)
	}
	return sourceID
}
