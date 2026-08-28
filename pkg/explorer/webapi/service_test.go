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
		Snapshot: documents.Snapshot, NodeIDs: []shoal.ID{first.Document.ID},
		Depth: 2, Fanout: 1, MaxNodes: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(neighborhood.Neighborhood.Nodes) > 2 ||
		len(neighborhood.Neighborhood.Edges) > 1 {
		t.Fatalf("unbounded neighborhood = %+v", neighborhood)
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
	var decoded webapi.RetrievalResponse
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Retrieval.Results) == 0 ||
		len(decoded.Retrieval.Results[0].Evidence) == 0 {
		t.Fatalf("missing evidence: %s", encoded)
	}
	evidence := decoded.Retrieval.Results[0].Evidence[0]
	if evidence.Quote == "" || evidence.Citation.DocumentID == "" ||
		decoded.Retrieval.Results[0].Explanation == nil {
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
