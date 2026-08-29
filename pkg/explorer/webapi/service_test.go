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
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/model"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestServiceContractParity(t *testing.T) {
	service, corpus, first, second := testService(t)
	defer corpus.Close()
	assertServiceBoundsAndSnapshot(t, "embedded", service, first, second)

	remote, cleanup, first, second := testRemoteService(t)
	defer cleanup()
	assertServiceBoundsAndSnapshot(t, "remote", remote, first, second)
}

func TestRemoteServiceMatchesEmbeddedResponses(t *testing.T) {
	embedded, corpus, _, _ := testService(t)
	defer corpus.Close()
	remote, cleanup := serveRemoteService(t, embedded)
	defer cleanup()
	ctx := context.Background()

	documentsRequest := webapi.DocumentsRequest{Page: webapi.PageRequest{Limit: 1}}
	embeddedDocuments, err := embedded.Documents(ctx, documentsRequest)
	if err != nil {
		t.Fatal(err)
	}
	remoteDocuments, err := remote.Documents(ctx, documentsRequest)
	if err != nil {
		t.Fatal(err)
	}
	assertEqualJSON(t, "documents", embeddedDocuments, remoteDocuments)

	documentRequest := webapi.DocumentRequest{
		Snapshot:   embeddedDocuments.Snapshot,
		DocumentID: embeddedDocuments.Documents[0].Document.ID,
		RevisionID: embeddedDocuments.Documents[0].Revision.ID,
	}
	embeddedDocument, err := embedded.Document(ctx, documentRequest)
	if err != nil {
		t.Fatal(err)
	}
	remoteDocument, err := remote.Document(ctx, documentRequest)
	if err != nil {
		t.Fatal(err)
	}
	assertEqualJSON(t, "document", embeddedDocument, remoteDocument)

	retrievalRequest := webapi.RetrievalRequest{
		Snapshot: embeddedDocuments.Snapshot,
		Query: retrieval.Request{
			Text: "bounded retries", TopK: 2,
			Modes: []retrieval.Mode{retrieval.ModeLexical, retrieval.ModeTree},
			AsOf:  embeddedDocuments.Snapshot.AsOf, Explain: true,
		},
	}
	embeddedRetrieval, err := embedded.Retrieve(ctx, retrievalRequest)
	if err != nil {
		t.Fatal(err)
	}
	remoteRetrieval, err := remote.Retrieve(ctx, retrievalRequest)
	if err != nil {
		t.Fatal(err)
	}
	assertEqualJSON(t, "retrieval", embeddedRetrieval, remoteRetrieval)
}

func assertServiceBoundsAndSnapshot(
	t *testing.T,
	name string,
	service webapi.Service,
	first explorer.IngestResult,
	second explorer.IngestResult,
) {
	t.Helper()
	ctx := context.Background()
	documents, err := service.Documents(ctx, webapi.DocumentsRequest{
		Page: webapi.PageRequest{Limit: 1},
	})
	if err != nil {
		t.Fatalf("%s documents: %v", name, err)
	}
	if len(documents.Documents) != 1 || documents.NextCursor == "" ||
		documents.Snapshot.ID == "" || documents.Snapshot.AsOf.IsZero() {
		t.Fatalf("%s documents = %+v", name, documents)
	}
	next, err := service.Documents(ctx, webapi.DocumentsRequest{
		Snapshot: documents.Snapshot,
		Page:     webapi.PageRequest{Limit: 1, Cursor: documents.NextCursor},
	})
	if err != nil {
		t.Fatalf("%s next documents: %v", name, err)
	}
	if len(next.Documents) != 1 || next.Documents[0].Document.ID == documents.Documents[0].Document.ID {
		t.Fatalf("%s next page = %+v", name, next)
	}
	_, err = service.Documents(ctx, webapi.DocumentsRequest{
		Page: webapi.PageRequest{Limit: webapi.MaxPageSize + 1},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("%s page bound error = %v", name, err)
	}
	_, err = service.Retrieve(ctx, webapi.RetrievalRequest{
		Snapshot: documents.Snapshot,
		Query:    retrieval.Request{Text: "bounded", TopK: webapi.MaxTopK + 1},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("%s top-k bound error = %v", name, err)
	}
	_, err = service.Retrieve(ctx, webapi.RetrievalRequest{
		Query: retrieval.Request{
			Text: "bounded", TopK: 1, AsOf: documents.Snapshot.AsOf,
		},
	})
	if err != nil {
		t.Fatalf("%s unpinned retrieval as_of: %v", name, err)
	}
	neighborhood, err := service.Neighborhood(ctx, webapi.NeighborhoodRequest{
		Snapshot: documents.Snapshot,
		NodeIDs:  []shoal.ID{first.Document.ID, second.Document.ID},
		Depth:    2, Fanout: 1, MaxNodes: 2,
	})
	if err != nil {
		t.Fatalf("%s neighborhood: %v", name, err)
	}
	if len(neighborhood.Neighborhood.Nodes) > 2 ||
		len(neighborhood.Neighborhood.Edges) > 1 {
		t.Fatalf("%s unbounded neighborhood = %+v", name, neighborhood)
	}
	nodeIDs := map[shoal.ID]bool{}
	for _, node := range neighborhood.Neighborhood.Nodes {
		nodeIDs[node.ID] = true
	}
	if !nodeIDs[first.Document.ID] || !nodeIDs[second.Document.ID] {
		t.Fatalf("%s requested seeds were not reserved: %+v", name, neighborhood)
	}
	firstPage, err := service.Neighborhood(ctx, webapi.NeighborhoodRequest{
		Snapshot: documents.Snapshot,
		NodeIDs:  []shoal.ID{first.Document.ID, first.Document.ID},
		Depth:    1, Fanout: 10, MaxNodes: 2,
	})
	if err != nil {
		t.Fatalf("%s first graph page: %v", name, err)
	}
	if firstPage.NextCursor == "" || len(firstPage.Neighborhood.Edges) != 1 {
		t.Fatalf("%s first graph page = %+v", name, firstPage)
	}
	secondPage, err := service.Neighborhood(ctx, webapi.NeighborhoodRequest{
		Snapshot: documents.Snapshot,
		NodeIDs:  []shoal.ID{first.Document.ID, first.Document.ID},
		Depth:    1, Fanout: 10, MaxNodes: 2, Cursor: firstPage.NextCursor,
	})
	if err != nil {
		t.Fatalf("%s second graph page: %v", name, err)
	}
	if len(secondPage.Neighborhood.Edges) != 1 ||
		secondPage.Neighborhood.Edges[0].ID == firstPage.Neighborhood.Edges[0].ID {
		t.Fatalf("%s second graph page = %+v", name, secondPage)
	}
	path, err := service.Path(ctx, webapi.PathRequest{
		Snapshot: documents.Snapshot, From: first.Document.ID,
		To: second.Document.ID, MaxDepth: 2, Fanout: 2,
	})
	if err != nil {
		t.Fatalf("%s path: %v", name, err)
	}
	if err := path.Path.Validate(); err != nil {
		t.Fatalf("%s path: %v", name, err)
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
	server := httptest.NewUnstartedServer(nil)
	handler, err := webapi.NewHandler(service, server.Listener.Addr().String())
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	server.Config.Handler = handler
	server.Start()
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

	request, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/meta", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = "attacker.example"
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("forged host status = %s", response.Status)
	}
}

func TestMetadataAdvertisesCapabilities(t *testing.T) {
	service, corpus, _, _ := testService(t)
	defer corpus.Close()
	server := httptest.NewUnstartedServer(nil)
	handler, err := webapi.NewHandler(service, server.Listener.Addr().String())
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	server.Config.Handler = handler
	server.Start()
	defer server.Close()

	response, err := http.Get(server.URL + "/api/v1/meta")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("meta status = %s", response.Status)
	}
	var metadata webapi.MetadataResponse
	if err := json.NewDecoder(response.Body).Decode(&metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.MaxPageSize != webapi.MaxPageSize ||
		metadata.MaxResponseBytes != webapi.MaxResponseBytes ||
		!metadata.Capabilities.Supports(webapi.CapabilityRetrieve) ||
		!metadata.Capabilities.Supports(webapi.CapabilityPath) ||
		metadata.Capabilities.Supports(webapi.CapabilityVector) {
		t.Fatalf("metadata = %+v", metadata)
	}
}

func TestMetadataAdvertisesVectorOnlyWhenAvailable(t *testing.T) {
	dataDir := t.TempDir()
	corpus, err := explorer.OpenWithOptions(dataDir, explorer.Options{
		Embedder: model.FakeEmbedder{Model: "webapi", Dimensions: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := corpus.Ingest(context.Background(), explorer.Source{
		URI:       "file:///vector.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "vector capability is available",
	}); err != nil {
		t.Fatal(err)
	}
	service, err := webapi.NewEmbeddedService(corpus)
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := service.Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.Supports(webapi.CapabilityVector) {
		t.Fatalf("vector capabilities = %+v", capabilities)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := explorer.OpenWithOptions(dataDir, explorer.Options{
		Embedder: model.FakeEmbedder{Model: "other", Dimensions: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	service, err = webapi.NewEmbeddedService(reopened)
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err = service.Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.Supports(webapi.CapabilityVector) {
		t.Fatalf("mismatched vector capabilities = %+v", capabilities)
	}
}

func TestRemoteServiceCapabilityNegotiation(t *testing.T) {
	pathCalled := false
	retrieveCalled := false
	upstream := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/api/v1/meta":
			writeJSON(t, writer, webapi.MetadataResponse{
				MaxPageSize: webapi.MaxPageSize, MaxTopK: webapi.MaxTopK,
				MaxDepth: webapi.MaxDepth, MaxFanout: webapi.MaxFanout,
				MaxNodes: webapi.MaxNodes,
				Capabilities: webapi.Capabilities{
					Documents: true, Document: true, Retrieve: true,
					Neighborhood: true, Path: false,
				},
			})
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/path":
			pathCalled = true
			http.Error(writer, "path should not be called", http.StatusInternalServerError)
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/retrieve":
			retrieveCalled = true
			http.Error(writer, "retrieve should not be called", http.StatusInternalServerError)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer upstream.Close()
	service, err := webapi.NewRemoteService(upstream.URL, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}

	capabilities, err := service.Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.Supports(webapi.CapabilityPath) {
		t.Fatalf("capabilities = %+v", capabilities)
	}
	_, err = service.Path(context.Background(), webapi.PathRequest{
		From: "from", To: "to", MaxDepth: 1, Fanout: 1,
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("unsupported path error = %v", err)
	}
	if pathCalled {
		t.Fatal("remote path endpoint was called")
	}
	_, err = service.Retrieve(context.Background(), webapi.RetrievalRequest{
		Query: retrieval.Request{
			Text: "query", Modes: []retrieval.Mode{retrieval.ModeVector},
		},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) || retrieveCalled {
		t.Fatalf("unsupported vector retrieve error = %v, called = %t", err, retrieveCalled)
	}

	documentsCalled := false
	missingCapabilities := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/api/v1/meta":
			writeJSON(t, writer, map[string]any{
				"max_page_size": webapi.MaxPageSize,
				"max_top_k":     webapi.MaxTopK,
				"max_depth":     webapi.MaxDepth,
				"max_fanout":    webapi.MaxFanout,
				"max_nodes":     webapi.MaxNodes,
			})
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/documents":
			documentsCalled = true
			http.Error(writer, "documents should not be called", http.StatusInternalServerError)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer missingCapabilities.Close()
	missing, err := webapi.NewRemoteService(
		missingCapabilities.URL, missingCapabilities.Client())
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err = missing.Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.Supports(webapi.CapabilityDocuments) {
		t.Fatalf("missing capabilities defaulted open: %+v", capabilities)
	}
	_, err = missing.Documents(context.Background(), webapi.DocumentsRequest{
		Page: webapi.PageRequest{Limit: 1},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) || documentsCalled {
		t.Fatalf("missing capabilities error = %v, documentsCalled = %t", err, documentsCalled)
	}

	insufficientBounds := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		if request.Method == http.MethodGet && request.URL.Path == "/api/v1/meta" {
			writeJSON(t, writer, webapi.MetadataResponse{
				MaxPageSize: webapi.MaxPageSize - 1, MaxTopK: webapi.MaxTopK,
				MaxDepth: webapi.MaxDepth, MaxFanout: webapi.MaxFanout,
				MaxNodes: webapi.MaxNodes, Capabilities: webapi.AllCapabilities(),
			})
			return
		}
		http.NotFound(writer, request)
	}))
	defer insufficientBounds.Close()
	insufficient, err := webapi.NewRemoteService(
		insufficientBounds.URL, insufficientBounds.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = insufficient.Capabilities(context.Background())
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("insufficient bounds error = %v", err)
	}
}

func TestRemoteServiceAllowsScopedVectorWhenGlobalCapabilityIsUnavailable(t *testing.T) {
	retrieveCalled := false
	now := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	upstream := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/api/v1/meta":
			writeJSON(t, writer, webapi.MetadataResponse{
				MaxPageSize: webapi.MaxPageSize,
				MaxTopK:     webapi.MaxTopK,
				MaxDepth:    webapi.MaxDepth,
				MaxFanout:   webapi.MaxFanout,
				MaxNodes:    webapi.MaxNodes,
				Capabilities: webapi.Capabilities{
					Documents: true, Document: true, Retrieve: true,
					Neighborhood: true, Path: true, Vector: false,
				},
			})
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/retrieve":
			retrieveCalled = true
			writeJSON(t, writer, webapi.RetrievalResponse{
				Snapshot: webapi.Snapshot{
					ID: "snapshot", AsOf: now, Frontier: 1,
				},
				Retrieval: retrieval.Response{},
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer upstream.Close()
	service, err := webapi.NewRemoteService(upstream.URL, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Retrieve(context.Background(), webapi.RetrievalRequest{
		Query: retrieval.Request{
			Text:  "query",
			Modes: []retrieval.Mode{retrieval.ModeVector},
			Scope: retrieval.Scope{DocumentIDs: []shoal.ID{
				"document",
			}},
		},
	})
	if err != nil {
		t.Fatalf("scoped vector retrieve: %v", err)
	}
	if !retrieveCalled {
		t.Fatal("scoped vector request did not reach remote endpoint")
	}
}

func TestRemoteServiceErrorMappingAndLocalBounds(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/api/v1/meta":
			writeJSON(t, writer, webapi.MetadataResponse{
				MaxPageSize: webapi.MaxPageSize, MaxTopK: webapi.MaxTopK,
				MaxDepth: webapi.MaxDepth, MaxFanout: webapi.MaxFanout,
				MaxNodes: webapi.MaxNodes, Capabilities: webapi.AllCapabilities(),
			})
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/documents":
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusBadRequest)
			writeJSON(t, writer, map[string]any{
				"code":    shoal.ErrorInvalidArgument,
				"message": "invalid_argument: remote invalid argument",
			})
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/retrieve":
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusGatewayTimeout)
			writeJSON(t, writer, map[string]any{
				"code":    shoal.ErrorDeadline,
				"message": "deadline_exceeded: remote deadline",
			})
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/path":
			writer.WriteHeader(http.StatusGatewayTimeout)
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/neighborhood":
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusInternalServerError)
			writeJSON(t, writer, map[string]any{
				"code":    shoal.ErrorNotFound,
				"message": "not_found: contradictory status",
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	service, err := webapi.NewRemoteService(upstream.URL, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}

	_, err = service.Retrieve(context.Background(), webapi.RetrievalRequest{
		Query: retrieval.Request{Text: "bounded", TopK: webapi.MaxTopK + 1},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("local top-k bound error = %v", err)
	}
	_, err = service.Path(context.Background(), webapi.PathRequest{
		From: "from", To: "to", MaxDepth: 1, Fanout: 1,
		EdgeTypes: []string{string([]byte{0xff})},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("local path edge-type error = %v", err)
	}
	_, err = service.Documents(context.Background(), webapi.DocumentsRequest{
		Page: webapi.PageRequest{Limit: 1},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("remote invalid-argument error = %v", err)
	}
	if got, want := err.Error(), "invalid_argument: remote invalid argument"; got != want {
		t.Fatalf("remote error = %q, want %q", got, want)
	}
	_, err = service.Retrieve(context.Background(), webapi.RetrievalRequest{
		Query: retrieval.Request{Text: "bounded", TopK: 1},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorDeadline) ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("remote deadline error = %v", err)
	}
	_, err = service.Path(context.Background(), webapi.PathRequest{
		From: "from", To: "to", MaxDepth: 1, Fanout: 1,
	})
	if !shoal.IsErrorCode(err, shoal.ErrorDeadline) ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("remote deadline fallback error = %v", err)
	}
	_, err = service.Neighborhood(context.Background(), webapi.NeighborhoodRequest{
		NodeIDs: []shoal.ID{"node"}, Depth: 1, Fanout: 1, MaxNodes: 1,
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) ||
		shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("contradictory error mapping = %v", err)
	}

	upstream.Close()
	_, err = service.Documents(context.Background(), webapi.DocumentsRequest{
		Page: webapi.PageRequest{Limit: 1},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("unavailable error = %v", err)
	}
}

func TestRemoteServiceRejectsNonconformingResponses(t *testing.T) {
	snapshot := webapi.Snapshot{
		ID: "snapshot", AsOf: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		Frontier: 7,
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/api/v1/meta":
			writeJSON(t, writer, webapi.MetadataResponse{
				MaxPageSize: webapi.MaxPageSize, MaxTopK: webapi.MaxTopK,
				MaxDepth: webapi.MaxDepth, MaxFanout: webapi.MaxFanout,
				MaxNodes: webapi.MaxNodes, Capabilities: webapi.AllCapabilities(),
			})
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/documents":
			writeJSON(t, writer, webapi.DocumentsResponse{
				Snapshot: snapshot,
				Documents: []explorer.DocumentSummary{
					documentSummary("one", snapshot.AsOf),
					documentSummary("two", snapshot.AsOf),
				},
			})
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/document":
			var body webapi.DocumentRequest
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			view := documentView("one", snapshot.AsOf)
			if body.DocumentID == "blank-source" {
				view = documentView("blank-source", snapshot.AsOf)
				view.SourceURI = ""
			}
			writeJSON(t, writer, webapi.DocumentResponse{
				Snapshot: snapshot,
				Document: view,
			})
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/neighborhood":
			writeJSON(t, writer, webapi.NeighborhoodResponse{
				Snapshot: snapshot,
				Neighborhood: explorer.Neighborhood{Nodes: []graph.Node{
					{ID: "one", Kind: "test"}, {ID: "two", Kind: "test"},
				}},
			})
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/path":
			var body struct {
				EdgeTypes []string `json:"edge_types"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			from := shoal.ID("other")
			if len(body.EdgeTypes) > 0 {
				from = "from"
			}
			if len(body.EdgeTypes) > 0 && body.EdgeTypes[0] == "duplicate" {
				writeJSON(t, writer, webapi.PathResponse{
					Snapshot: snapshot,
					Path: graph.Path{
						Nodes: []graph.Node{{ID: "from"}, {ID: "mid"}, {ID: "to"}},
						Edges: []graph.Edge{
							{ID: "edge", From: "from", To: "mid", Type: "duplicate", Weight: 1},
							{ID: "edge", From: "mid", To: "to", Type: "duplicate", Weight: 1},
						},
					},
				})
				return
			}
			writeJSON(t, writer, webapi.PathResponse{
				Snapshot: snapshot,
				Path: graph.Path{
					Nodes: []graph.Node{{ID: from}, {ID: "to"}},
					Edges: []graph.Edge{{
						ID: "edge", From: from, To: "to", Type: "test", Weight: 1,
					}},
				},
			})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer upstream.Close()
	service, err := webapi.NewRemoteService(upstream.URL, upstream.Client())
	if err != nil {
		t.Fatal(err)
	}

	_, err = service.Documents(context.Background(), webapi.DocumentsRequest{
		Page: webapi.PageRequest{Limit: 1},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("oversized document page error = %v", err)
	}
	_, err = service.Documents(context.Background(), webapi.DocumentsRequest{
		Snapshot: webapi.Snapshot{
			ID: "other", AsOf: snapshot.AsOf, Frontier: snapshot.Frontier,
		},
		Page: webapi.PageRequest{Limit: 10},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("mismatched snapshot error = %v", err)
	}
	_, err = service.Document(context.Background(), webapi.DocumentRequest{
		Snapshot: snapshot, DocumentID: "two",
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("mismatched document error = %v", err)
	}
	_, err = service.Document(context.Background(), webapi.DocumentRequest{
		Snapshot: snapshot, DocumentID: "blank-source",
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("blank document source URI error = %v", err)
	}
	_, err = service.Neighborhood(context.Background(), webapi.NeighborhoodRequest{
		Snapshot: snapshot, NodeIDs: []shoal.ID{"one"}, Depth: 1,
		Fanout: 1, MaxNodes: 1,
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("oversized neighborhood error = %v", err)
	}
	_, err = service.Neighborhood(context.Background(), webapi.NeighborhoodRequest{
		Snapshot: snapshot, NodeIDs: []shoal.ID{"one"}, Depth: 1,
		Fanout: 1, MaxNodes: 2,
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("disconnected neighborhood error = %v", err)
	}
	_, err = service.Path(context.Background(), webapi.PathRequest{
		Snapshot: snapshot, From: "from", To: "to", MaxDepth: 1, Fanout: 1,
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("mismatched path error = %v", err)
	}
	_, err = service.Path(context.Background(), webapi.PathRequest{
		Snapshot: snapshot, From: "from", To: "to", MaxDepth: 1, Fanout: 1,
		EdgeTypes: []string{"allowed"},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("excluded path edge type error = %v", err)
	}
	_, err = service.Path(context.Background(), webapi.PathRequest{
		Snapshot: snapshot, From: "from", To: "to", MaxDepth: 2, Fanout: 2,
		EdgeTypes: []string{"duplicate"},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("duplicate path edge error = %v", err)
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

func testRemoteService(
	t *testing.T,
) (*webapi.RemoteService, func(), explorer.IngestResult, explorer.IngestResult) {
	t.Helper()
	embedded, corpus, first, second := testService(t)
	remote, serverCleanup := serveRemoteService(t, embedded)
	cleanup := func() {
		serverCleanup()
		corpus.Close()
	}
	return remote, cleanup, first, second
}

func serveRemoteService(
	t *testing.T,
	service webapi.Service,
) (*webapi.RemoteService, func()) {
	t.Helper()
	server := httptest.NewUnstartedServer(nil)
	handler, err := webapi.NewHandler(service, server.Listener.Addr().String())
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	server.Config.Handler = handler
	server.Start()
	remote, err := webapi.NewRemoteService(server.URL, server.Client())
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	return remote, server.Close
}

func writeJSON(t *testing.T, writer http.ResponseWriter, value any) {
	t.Helper()
	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Fatal(err)
	}
}

func assertEqualJSON(t *testing.T, name string, left, right any) {
	t.Helper()
	leftJSON, err := json.Marshal(left)
	if err != nil {
		t.Fatalf("%s left JSON: %v", name, err)
	}
	rightJSON, err := json.Marshal(right)
	if err != nil {
		t.Fatalf("%s right JSON: %v", name, err)
	}
	if !bytes.Equal(leftJSON, rightJSON) {
		t.Fatalf("%s mismatch:\nleft:  %s\nright: %s", name, leftJSON, rightJSON)
	}
}

func documentSummary(id string, createdAt time.Time) explorer.DocumentSummary {
	documentID := shoal.ID(id)
	revisionID := shoal.ID(id + "-revision")
	return explorer.DocumentSummary{
		Document: document.Document{
			ID: documentID, RevisionID: revisionID,
			Title: id, RootSectionID: shoal.ID(id + "-root"),
		},
		Revision: document.Revision{
			ID: revisionID, DocumentID: documentID, CreatedAt: createdAt,
		},
		SourceURI: "file:///" + id,
	}
}

func documentView(id string, createdAt time.Time) explorer.DocumentView {
	summary := documentSummary(id, createdAt)
	return explorer.DocumentView{
		Document:  summary.Document,
		Revision:  summary.Revision,
		SourceURI: summary.SourceURI,
		Root: explorer.SectionView{Section: document.Section{
			ID: summary.Document.RootSectionID, DocumentID: summary.Document.ID,
			RevisionID: summary.Revision.ID, Heading: id,
		}},
	}
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
