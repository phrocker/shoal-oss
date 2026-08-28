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

package webapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestEmbeddedServiceBoundsAndSnapshot(t *testing.T) {
	service, corpus, first, second := testService(t)
	defer corpus.Close()
	ctx := context.Background()
	documents, err := service.Documents(ctx, webapi.DocumentsRequest{
		Page: webapi.PageRequest{Limit: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(documents.Documents) != 1 || documents.NextCursor == "" ||
		documents.Snapshot.ID == "" || documents.Snapshot.AsOf.IsZero() {
		t.Fatalf("documents = %+v", documents)
	}
	next, err := service.Documents(ctx, webapi.DocumentsRequest{
		Snapshot: documents.Snapshot,
		Page:     webapi.PageRequest{Limit: 1, Cursor: documents.NextCursor},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Documents) != 1 || next.Documents[0].Document.ID == documents.Documents[0].Document.ID {
		t.Fatalf("next page = %+v", next)
	}
	_, err = service.Documents(ctx, webapi.DocumentsRequest{
		Page: webapi.PageRequest{Limit: webapi.MaxPageSize + 1},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("page bound error = %v", err)
	}
	_, err = service.Retrieve(ctx, webapi.RetrievalRequest{
		Snapshot: documents.Snapshot,
		Query:    retrieval.Request{Text: "bounded", TopK: webapi.MaxTopK + 1},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("top-k bound error = %v", err)
	}
	neighborhood, err := service.Neighborhood(ctx, webapi.NeighborhoodRequest{
		Snapshot: documents.Snapshot,
		NodeIDs:  []shoal.ID{first.Document.ID, second.Document.ID},
		Depth:    2, Fanout: 1, MaxNodes: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(neighborhood.Neighborhood.Nodes) > 2 ||
		len(neighborhood.Neighborhood.Edges) > 1 {
		t.Fatalf("unbounded neighborhood = %+v", neighborhood)
	}
	nodeIDs := map[shoal.ID]bool{}
	for _, node := range neighborhood.Neighborhood.Nodes {
		nodeIDs[node.ID] = true
	}
	if !nodeIDs[first.Document.ID] || !nodeIDs[second.Document.ID] {
		t.Fatalf("requested seeds were not reserved: %+v", neighborhood)
	}
	firstPage, err := service.Neighborhood(ctx, webapi.NeighborhoodRequest{
		Snapshot: documents.Snapshot,
		NodeIDs:  []shoal.ID{first.Document.ID, first.Document.ID},
		Depth:    1, Fanout: 1, MaxNodes: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if firstPage.NextCursor == "" || len(firstPage.Neighborhood.Edges) != 1 {
		t.Fatalf("first graph page = %+v", firstPage)
	}
	secondPage, err := service.Neighborhood(ctx, webapi.NeighborhoodRequest{
		Snapshot: documents.Snapshot,
		NodeIDs:  []shoal.ID{first.Document.ID, first.Document.ID},
		Depth:    1, Fanout: 1, MaxNodes: 10, Cursor: firstPage.NextCursor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Neighborhood.Edges) != 1 ||
		secondPage.Neighborhood.Edges[0].ID == firstPage.Neighborhood.Edges[0].ID {
		t.Fatalf("second graph page = %+v", secondPage)
	}
	path, err := service.Path(ctx, webapi.PathRequest{
		Snapshot: documents.Snapshot, From: first.Document.ID,
		To: second.Document.ID, MaxDepth: 2, Fanout: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := path.Path.Validate(); err != nil {
		t.Fatalf("path: %v", err)
	}
}

func TestSnapshotChangesWhenGraphChanges(t *testing.T) {
	service, corpus, first, second := testService(t)
	defer corpus.Close()
	ctx := context.Background()
	documents, err := service.Documents(ctx, webapi.DocumentsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err := corpus.Connect(ctx, graph.Edge{
		ID: "reverse-edge", From: second.Document.ID, To: first.Document.ID,
		Type: "references", Weight: 1,
	}); err != nil {
		t.Fatal(err)
	}
	_, err = service.Neighborhood(ctx, webapi.NeighborhoodRequest{
		Snapshot: documents.Snapshot, NodeIDs: []shoal.ID{first.Document.ID},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("stale graph snapshot error = %v", err)
	}
}

func TestPathFanoutCountsOnlyOutgoingEdges(t *testing.T) {
	service, corpus, first, second := testService(t)
	defer corpus.Close()
	ctx := context.Background()
	firstView, err := corpus.Document(ctx, first.Document.ID, first.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	secondView, err := corpus.Document(ctx, second.Document.ID, second.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	from := firstSpanID(t, firstView.Root)
	to := firstSpanID(t, secondView.Root)
	if err := corpus.Connect(ctx, graph.Edge{
		ID: "a-incoming", From: to, To: from, Type: "noise", Weight: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := corpus.Connect(ctx, graph.Edge{
		ID: "z-outgoing", From: from, To: to, Type: "path", Weight: 1,
	}); err != nil {
		t.Fatal(err)
	}
	documents, err := service.Documents(ctx, webapi.DocumentsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	path, err := service.Path(ctx, webapi.PathRequest{
		Snapshot: documents.Snapshot, From: from, To: to,
		MaxDepth: 1, Fanout: 1, EdgeTypes: []string{"noise", "path"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(path.Path.Edges) != 1 || path.Path.Edges[0].ID != "z-outgoing" {
		t.Fatalf("path = %+v", path.Path)
	}
}

func TestHTTPIDsAreBinarySafeAndReversible(t *testing.T) {
	rawID := shoal.ID([]byte{0xff, 0x00, 'x'})
	encoded, err := json.Marshal(webapi.NeighborhoodResponse{
		Neighborhood: explorer.Neighborhood{
			Nodes: []graph.Node{{
				ID: rawID, Kind: "binary",
				Properties: shoal.Metadata{string([]byte{0xfe}): string([]byte{0xfd})},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("\ufffd")) {
		t.Fatalf("JSON replaced opaque ID bytes: %s", encoded)
	}
	var wire struct {
		Neighborhood struct {
			Nodes []struct {
				ID string `json:"id"`
			} `json:"nodes"`
		} `json:"neighborhood"`
	}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	requestJSON, err := json.Marshal(map[string]any{
		"node_ids":  []string{wire.Neighborhood.Nodes[0].ID},
		"depth":     1,
		"fanout":    1,
		"max_nodes": 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	var request webapi.NeighborhoodRequest
	if err := json.Unmarshal(requestJSON, &request); err != nil {
		t.Fatal(err)
	}
	if len(request.NodeIDs) != 1 || request.NodeIDs[0] != rawID {
		t.Fatalf("ID round trip = %q", request.NodeIDs)
	}
}

func TestSnapshotFrontierJSONRoundTrip(t *testing.T) {
	snapshot := webapi.Snapshot{
		ID: "snapshot", AsOf: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Frontier: math.MaxUint64,
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"frontier":"18446744073709551615"`)) {
		t.Fatalf("frontier is not an exact string: %s", encoded)
	}
	var decoded webapi.Snapshot
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != snapshot {
		t.Fatalf("snapshot round trip = %+v", decoded)
	}
}

func TestEvidenceSerializationPreservesCitationAndExplanation(t *testing.T) {
	service, corpus, _, _ := testService(t)
	defer corpus.Close()
	documents, err := service.Documents(context.Background(), webapi.DocumentsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.Retrieve(context.Background(), webapi.RetrievalRequest{
		Snapshot: documents.Snapshot,
		Query: retrieval.Request{
			Text: "bounded retries", TopK: 2,
			Modes:   []retrieval.Mode{retrieval.ModeLexical, retrieval.ModeTree},
			Explain: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Retrieval struct {
			Results []struct {
				ID       string `json:"id"`
				Evidence []struct {
					Quote    string `json:"quote"`
					Citation struct {
						DocumentID string `json:"document_id"`
					} `json:"citation"`
				} `json:"evidence"`
				Explanation json.RawMessage `json:"explanation"`
			} `json:"results"`
		} `json:"retrieval"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Retrieval.Results) == 0 ||
		len(decoded.Retrieval.Results[0].Evidence) == 0 {
		t.Fatalf("missing evidence: %s", encoded)
	}
	evidence := decoded.Retrieval.Results[0].Evidence[0]
	if evidence.Quote == "" || evidence.Citation.DocumentID == "" ||
		len(decoded.Retrieval.Results[0].Explanation) == 0 {
		t.Fatalf("lost evidence fields: %+v", decoded)
	}
}

func TestHTTPHandlerValidationAndWorkspace(t *testing.T) {
	service, corpus, _, _ := testService(t)
	defer corpus.Close()
	handler, err := webapi.NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK ||
		!strings.HasPrefix(response.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("workspace response = %s %s", response.Status, response.Header)
	}

	response, err = http.Post(
		server.URL+"/api/v1/documents", "text/plain",
		bytes.NewBufferString(`{"page":{"limit":1}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("plain-text request status = %s", response.Status)
	}

	response, err = http.Post(
		server.URL+"/api/v1/documents", "application/json",
		bytes.NewBufferString(`{"page":{"limit":1},"unexpected":true}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("validation status = %s", response.Status)
	}

	response, err = http.Post(
		server.URL+"/api/v1/documents", "application/json",
		bytes.NewBufferString(`{"page":{"limit":1}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("documents status = %s", response.Status)
	}
	if response.Header.Get("Content-Security-Policy") == "" {
		t.Fatal("missing content security policy")
	}
}

func testService(
	t *testing.T,
) (*webapi.EmbeddedService, *explorer.Explorer, explorer.IngestResult, explorer.IngestResult) {
	t.Helper()
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := corpus.IngestWithOptions(ctx, explorer.Source{
		URI: "file:///guide.md", MediaType: explorer.MediaTypeMarkdown,
		Content: "# Guide\n\nUse bounded retries.\n\n## Details\n\nKeep exact evidence.",
	}, explorer.IngestOptions{CreatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		corpus.Close()
		t.Fatal(err)
	}
	second, err := corpus.IngestWithOptions(ctx, explorer.Source{
		URI: "file:///service.md", MediaType: explorer.MediaTypeMarkdown,
		Content: "# Service\n\nThe service consumes the guide.",
	}, explorer.IngestOptions{CreatedAt: time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		corpus.Close()
		t.Fatal(err)
	}
	if err := corpus.Connect(ctx, graph.Edge{
		ID: "guide-to-service", From: first.Document.ID, To: second.Document.ID,
		Type: "informs", Weight: 1,
	}); err != nil {
		corpus.Close()
		t.Fatal(err)
	}
	service, err := webapi.NewEmbeddedService(corpus)
	if err != nil {
		corpus.Close()
		t.Fatal(err)
	}
	return service, corpus, first, second
}

func firstSpanID(t *testing.T, view explorer.SectionView) shoal.ID {
	t.Helper()
	if len(view.Spans) > 0 {
		return view.Spans[0].ID
	}
	for _, child := range view.Children {
		if id := firstSpanIDOptional(child); id != "" {
			return id
		}
	}
	t.Fatal("document has no span")
	return ""
}

func firstSpanIDOptional(view explorer.SectionView) shoal.ID {
	if len(view.Spans) > 0 {
		return view.Spans[0].ID
	}
	for _, child := range view.Children {
		if id := firstSpanIDOptional(child); id != "" {
			return id
		}
	}
	return ""
}
