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

package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/explorer/workspace"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestStreamableHTTPAuthenticatesEveryRequestAndIsolatesSessions(t *testing.T) {
	service := &stubService{}
	server, authority := newTestServer(t, service, nil)
	var observedSubjects []shoal.ID
	service.documents = func(
		ctx context.Context, _ webapi.DocumentsRequest,
	) (webapi.DocumentsResponse, error) {
		decision, err := authority.Resolver().Resolve(ctx)
		if err != nil {
			return webapi.DocumentsResponse{}, err
		}

		observedSubjects = append(observedSubjects, decision.Subject())
		return webapi.DocumentsResponse{}, nil
	}
	var requestNumber atomic.Uint64
	authenticator := webapi.AuthenticatorFunc(func(request *http.Request) (auth.Decision, error) {
		token := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
		switch token {
		case "alice", "bob":
			return httpTestDecision(
				t, shoal.ID(token),
				shoal.ID("http-"+strconv.FormatUint(requestNumber.Add(1), 10)),
				time.Now().Add(time.Hour),
			), nil
		case "alice-revoked":
			return httpTestDecisionWithGeneration(
				t, "alice",
				shoal.ID("http-"+strconv.FormatUint(requestNumber.Add(1), 10)),
				time.Now().Add(time.Hour), 2,
			), nil
		case "alice-expired":
			return httpTestDecision(
				t, "alice",
				shoal.ID("http-"+strconv.FormatUint(requestNumber.Add(1), 10)),
				time.Now().Add(-time.Minute),
			), nil
		default:
			return auth.Decision{}, shoal.NewError(
				shoal.ErrorUnauthorized, "bad token")
		}
	})

	httpServer := httptest.NewUnstartedServer(nil)
	mcpHandler, err := NewHTTPHandler(HTTPConfig{
		Server: server,
		AllowedOrigins: []string{
			"http://" + httpServer.Listener.Addr().String(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := webapi.NewAuthenticatedHandler(
		service, authenticator, authority.Binder(),
		httpServer.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := outer.MountAuthenticated("/mcp", mcpHandler); err != nil {
		t.Fatal(err)
	}
	httpServer.Config.Handler = outer
	httpServer.Start()
	t.Cleanup(httpServer.Close)

	aliceSession, initializeBody := initializeHTTP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "alice", `1.2300e+04`)
	if !bytes.Contains(initializeBody, []byte(`"id":1.2300e+04`)) {
		t.Fatalf("initialize ID was not preserved: %s", initializeBody)
	}
	initialized := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "alice",
		aliceSession, ProtocolVersion,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	if initialized.StatusCode != http.StatusAccepted {
		t.Fatalf("initialized status = %d", initialized.StatusCode)
	}
	assertEmptyBody(t, initialized)

	listed := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "alice",
		aliceSession, ProtocolVersion,
		`{"jsonrpc":"2.0","id":"list","method":"tools/list"}`)
	if listed.StatusCode != http.StatusOK {
		t.Fatalf("tools/list status = %d", listed.StatusCode)
	}
	_ = decodeHTTPResponse(t, listed)

	crossCaller := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "bob",
		aliceSession, ProtocolVersion,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	if crossCaller.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-caller status = %d, want 404", crossCaller.StatusCode)
	}
	revoked := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "alice-revoked",
		aliceSession, ProtocolVersion,
		`{"jsonrpc":"2.0","id":20,"method":"tools/list"}`)
	if revoked.StatusCode != http.StatusNotFound {
		t.Fatalf("changed authorization status = %d, want 404", revoked.StatusCode)
	}
	revoked.Body.Close()
	expired := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "alice-expired",
		aliceSession, ProtocolVersion,
		`{"jsonrpc":"2.0","id":21,"method":"tools/list"}`)
	if expired.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired authorization status = %d, want 401", expired.StatusCode)
	}
	expired.Body.Close()

	bobSession, _ := initializeHTTP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "bob", `3`)
	beforeInitialized := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "bob",
		bobSession, ProtocolVersion,
		`{"jsonrpc":"2.0","id":4,"method":"tools/list"}`)
	response := decodeHTTPResponse(t, beforeInitialized)
	if response.Error == nil || response.Error.Code != codeInvalidRequest {
		t.Fatalf("pre-initialized response = %+v", response)
	}
	assertEmptyBody(t, postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "bob",
		bobSession, ProtocolVersion,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	for _, call := range []struct {
		token   string
		session string
		id      int
	}{
		{"alice", aliceSession, 30},
		{"bob", bobSession, 31},
	} {
		called := postHTTPMCP(
			t, httpServer.Client(), httpServer.URL+"/mcp", call.token,
			call.session, ProtocolVersion,
			`{"jsonrpc":"2.0","id":`+strconv.Itoa(call.id)+
				`,"method":"tools/call","params":{`+
				`"name":"shoal.documents","arguments":{}}}`)
		decoded := decodeHTTPResponse(t, called)
		if decoded.Error != nil || decodeToolResult(t, decoded).IsError {
			t.Fatalf("%s documents response = %+v", call.token, decoded)
		}
	}
	if len(observedSubjects) != 2 ||
		observedSubjects[0] != "alice" ||
		observedSubjects[1] != "bob" {
		t.Fatalf("observed subjects = %q", observedSubjects)
	}

	badVersion := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "alice",
		aliceSession, "",
		`{"jsonrpc":"2.0","id":5,"method":"ping"}`)
	if badVersion.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing protocol version status = %d", badVersion.StatusCode)
	}
	badVersion.Body.Close()

	badOriginRequest := newHTTPMCPRequest(
		t, http.MethodPost, httpServer.URL+"/mcp", "alice",
		aliceSession, ProtocolVersion,
		`{"jsonrpc":"2.0","id":6,"method":"ping"}`)
	badOriginRequest.Header.Set("Origin", "https://attacker.invalid")
	authBeforeBadOrigin := requestNumber.Load()
	badOrigin, err := httpServer.Client().Do(badOriginRequest)
	if err != nil {
		t.Fatal(err)
	}
	if badOrigin.StatusCode != http.StatusForbidden {
		t.Fatalf("bad origin status = %d", badOrigin.StatusCode)
	}
	badOrigin.Body.Close()
	if requestNumber.Load() != authBeforeBadOrigin {
		t.Fatal("bad Origin reached the authenticator")
	}
	normalizedBadOriginRequest := newHTTPMCPRequest(
		t, http.MethodPost, httpServer.URL+"/mcp/../mcp", "alice",
		aliceSession, ProtocolVersion,
		`{"jsonrpc":"2.0","id":60,"method":"ping"}`)
	normalizedBadOriginRequest.Header.Set(
		"Origin", "https://attacker.invalid")
	authBeforeNormalizedOrigin := requestNumber.Load()
	noRedirectClient := *httpServer.Client()
	noRedirectClient.CheckRedirect = func(
		*http.Request, []*http.Request,
	) error {
		return http.ErrUseLastResponse
	}
	normalizedBadOrigin, err := noRedirectClient.Do(
		normalizedBadOriginRequest)
	if err != nil {
		t.Fatal(err)
	}
	if normalizedBadOrigin.StatusCode != http.StatusForbidden {
		t.Fatalf("normalized bad origin status = %d",
			normalizedBadOrigin.StatusCode)
	}
	normalizedBadOrigin.Body.Close()
	if requestNumber.Load() != authBeforeNormalizedOrigin {
		t.Fatal("normalized bad Origin reached the authenticator")
	}

	noAuthRequest := newHTTPMCPRequest(
		t, http.MethodPost, httpServer.URL+"/mcp", "",
		aliceSession, ProtocolVersion,
		`{"jsonrpc":"2.0","id":7,"method":"ping"}`)
	noAuth, err := httpServer.Client().Do(noAuthRequest)
	if err != nil {
		t.Fatal(err)
	}
	if noAuth.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing auth status = %d", noAuth.StatusCode)
	}
	noAuth.Body.Close()

	badAcceptRequest := newHTTPMCPRequest(
		t, http.MethodPost, httpServer.URL+"/mcp", "alice",
		aliceSession, ProtocolVersion,
		`{"jsonrpc":"2.0","id":9,"method":"ping"}`)
	badAcceptRequest.Header.Set("Accept", "application/json")
	badAccept, err := httpServer.Client().Do(badAcceptRequest)
	if err != nil {
		t.Fatal(err)
	}
	if badAccept.StatusCode != http.StatusNotAcceptable {
		t.Fatalf("bad Accept status = %d", badAccept.StatusCode)
	}
	badAccept.Body.Close()

	badContentRequest := newHTTPMCPRequest(
		t, http.MethodPost, httpServer.URL+"/mcp", "alice",
		aliceSession, ProtocolVersion,
		`{"jsonrpc":"2.0","id":10,"method":"ping"}`)
	badContentRequest.Header.Set("Content-Type", "text/plain")
	badContent, err := httpServer.Client().Do(badContentRequest)
	if err != nil {
		t.Fatal(err)
	}
	if badContent.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("bad Content-Type status = %d", badContent.StatusCode)
	}
	badContent.Body.Close()

	badHostRequest := newHTTPMCPRequest(
		t, http.MethodPost, httpServer.URL+"/mcp", "alice",
		aliceSession, ProtocolVersion,
		`{"jsonrpc":"2.0","id":11,"method":"ping"}`)
	badHostRequest.Host = "attacker.invalid"
	badHost, err := httpServer.Client().Do(badHostRequest)
	if err != nil {
		t.Fatal(err)
	}
	if badHost.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("bad Host status = %d", badHost.StatusCode)
	}
	badHost.Body.Close()

	get := newHTTPMCPRequest(
		t, http.MethodGet, httpServer.URL+"/mcp", "alice",
		aliceSession, ProtocolVersion, "")
	get.Header.Set("Accept", "text/event-stream")
	getResponse, err := httpServer.Client().Do(get)
	if err != nil {
		t.Fatal(err)
	}
	if getResponse.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", getResponse.StatusCode)
	}
	getResponse.Body.Close()
	getWithoutSession := newHTTPMCPRequest(
		t, http.MethodGet, httpServer.URL+"/mcp", "alice", "", "", "")
	getWithoutSession.Header.Set("Accept", "text/event-stream")
	getWithoutSessionResponse, err := httpServer.Client().Do(getWithoutSession)
	if err != nil {
		t.Fatal(err)
	}
	if getWithoutSessionResponse.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("sessionless GET status = %d, want 405",
			getWithoutSessionResponse.StatusCode)
	}
	getWithoutSessionResponse.Body.Close()

	deleteRequest := newHTTPMCPRequest(
		t, http.MethodDelete, httpServer.URL+"/mcp", "alice",
		aliceSession, ProtocolVersion, "")
	deleted, err := httpServer.Client().Do(deleteRequest)
	if err != nil {
		t.Fatal(err)
	}
	if deleted.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d", deleted.StatusCode)
	}
	deleted.Body.Close()
	afterDelete := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "alice",
		aliceSession, ProtocolVersion,
		`{"jsonrpc":"2.0","id":8,"method":"ping"}`)
	if afterDelete.StatusCode != http.StatusNotFound {
		t.Fatalf("deleted session status = %d, want 404", afterDelete.StatusCode)
	}
	afterDelete.Body.Close()
}

func TestHTTPToolCallPersistsAuthorizedInteractionAcrossRestart(t *testing.T) {
	corpusDir := t.TempDir()
	corpus, err := explorer.Open(corpusDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := corpus.EnsureInteractionSink(context.Background()); err != nil {
		t.Fatal(err)
	}
	authority := auth.NewAuthority()
	sourceID := []byte("source")
	policyID := []byte("policy")
	selector, err := authorized.NewStaticPolicySelector(sourceID, policyID)
	if err != nil {
		t.Fatal(err)
	}
	store := authorized.NewMemoryPolicyStore()
	client, err := authorized.NewClient(authorized.Config{
		Base: corpus, Resolver: authority.Resolver(),
		InteractionWriter: corpus, InteractionReader: corpus,
		SnapshotValidator: corpus,
		PolicySelector:    selector, PolicyStore: store,
		GenerationReader: httpGenerationReader{
			domain: []byte("domain"), generation: 1,
		},
		Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	alice := httpScopedDecision(
		t, "alice", "ingest-request", time.Now().Add(time.Hour),
		sourceID, policyID)
	aliceContext, err := authority.Binder().Bind(context.Background(), alice)
	if err != nil {
		t.Fatal(err)
	}
	ingested, err := client.Ingest(aliceContext, explorer.Source{
		URI: "file:///recorded.md", MediaType: explorer.MediaTypeMarkdown,
		Content: "# Recorded\n\nDurable MCP interaction.\n",
		Metadata: shoal.Metadata{
			interaction.PropertyVisibility: "restricted",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := webapi.NewEmbeddedService(client)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(Config{
		Service: service, Authority: authority,
		Decisions:       DecisionProviderFunc(authority.Resolver().Resolve),
		InteractionSink: client, Snapshots: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	var requestNumber atomic.Uint64
	authenticator := webapi.AuthenticatorFunc(func(request *http.Request) (auth.Decision, error) {
		if request.Header.Get("Authorization") != "Bearer alice" {
			return auth.Decision{}, shoal.NewError(
				shoal.ErrorUnauthorized, "bad token")
		}
		return httpScopedDecision(
			t, "alice",
			shoal.ID("http-"+strconv.FormatUint(requestNumber.Add(1), 10)),
			time.Now().Add(time.Hour), sourceID, policyID,
		), nil
	})
	httpServer := httptest.NewUnstartedServer(nil)
	transport, err := NewHTTPHandler(HTTPConfig{
		Server: server,
		AllowedOrigins: []string{
			"http://" + httpServer.Listener.Addr().String(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := webapi.NewAuthenticatedHandler(
		service, authenticator, authority.Binder(),
		httpServer.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := outer.MountAuthenticated("/mcp", transport); err != nil {
		t.Fatal(err)
	}
	httpServer.Config.Handler = outer
	httpServer.Start()

	session, _ := initializeHTTP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "alice", "1")
	initialized := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "alice",
		session, ProtocolVersion,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	assertEmptyBody(t, initialized)
	listed := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "alice",
		session, ProtocolVersion,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call",`+
			`"params":{"name":"shoal.documents","arguments":{}}}`)
	response := decodeHTTPResponse(t, listed)
	if response.Error != nil || decodeToolResult(t, response).IsError {
		t.Fatalf("documents response = %+v", response)
	}

	readDecision := httpScopedDecision(
		t, "alice", "read-interactions", time.Now().Add(time.Hour),
		sourceID, policyID)
	readContext, err := authority.Binder().Bind(
		context.Background(), readDecision)
	if err != nil {
		t.Fatal(err)
	}
	summaries, err := client.Interactions(readContext)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 {
		t.Fatalf("interaction summaries = %d, want 1", len(summaries))
	}
	recorded, err := client.Interaction(readContext, summaries[0].SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if recorded.Operation != interaction.OperationToolCall ||
		recorded.AuthorizationOperation != string(auth.OperationList) ||
		recorded.Actor.SubjectID != "alice" ||
		recorded.Reason.Code != "audit_purpose" ||
		recorded.Reason.Digest !=
			interaction.Digest("test HTTP MCP request") ||
		len(recorded.Turns) != 1 ||
		recorded.Turns[0].ToolCall == nil ||
		!containsObservedID(
			recorded.Turns[0].ToolCall.RetrievedNodeIDs,
			ingested.Document.ID,
		) {
		t.Fatalf("recorded interaction = %+v", recorded)
	}
	if summaries[0].Visibility != "restricted" {
		t.Fatalf("recorded visibility = %q", summaries[0].Visibility)
	}
	for _, node := range mustInteractionSubgraph(
		t, client, readContext, summaries[0].SessionID).Nodes {
		if node.Kind == interaction.KindInference {
			t.Fatal("generic MCP call synthesized an inference node")
		}
	}

	httpServer.Close()
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := explorer.Open(corpusDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	persisted, err := reopened.Interactions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(persisted) != 1 ||
		persisted[0].SessionID != summaries[0].SessionID {
		t.Fatalf("persisted interactions = %+v", persisted)
	}
}

func TestStreamableHTTPBindsWorkspaceRevisionDimensionsAndLimits(t *testing.T) {
	var observedTopK atomic.Uint32
	service := &stubService{
		retrieve: func(
			_ context.Context, request webapi.RetrievalRequest,
		) (webapi.RetrievalResponse, error) {
			observedTopK.Store(request.Query.TopK)
			return webapi.RetrievalResponse{
				Snapshot: webapi.Snapshot{
					ID: "workspace-snapshot", AsOf: time.Now().UTC(),
				},
			}, nil
		},
	}
	server, authority := newTestServer(t, service, nil)
	generations := httpGenerationReader{
		domain: []byte("domain"), generation: 1,
	}
	settingsStore, err := workspace.OpenDurableStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = settingsStore.Close() })
	settingsProvider, err := workspace.NewProvider(
		settingsStore,
		workspace.ProviderOptions{
			Resolver: authority.Resolver(), GenerationReader: generations,
			Clock: time.Now,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var requestNumber atomic.Uint64
	authenticator := webapi.AuthenticatorFunc(func(
		*http.Request,
	) (auth.Decision, error) {
		return workspaceHTTPDecision(
			t,
			shoal.ID("workspace-request-"+
				strconv.FormatUint(requestNumber.Add(1), 10)),
		), nil
	})
	create := func(id shoal.ID, mutation shoal.ID, topK uint32, output uint64) {
		t.Helper()
		decision := workspaceHTTPDecision(t, mutation)
		ctx, bindErr := authority.Binder().Bind(context.Background(), decision)
		if bindErr != nil {
			t.Fatal(bindErr)
		}
		_, updateErr := settingsProvider.Update(ctx, id, workspace.UpdateRequest{
			ExpectedRevision: 0,
			MutationID:       mutation,
			Narrowing: workspace.UpdateNarrowing{
				Budgets: workspace.Budgets{
					RetrievalTopK: &topK, OutputBytes: &output,
				},
			},
		})
		if updateErr != nil {
			t.Fatal(updateErr)
		}
	}
	const outputLimit = uint64(4096)
	workspaceA := shoal.ID("workspace-a")
	workspaceB := shoal.ID("workspace-b")
	create(workspaceA, "workspace-a-create", 3, outputLimit)
	create(workspaceB, "workspace-b-create", 4, outputLimit)
	workspaceAHeader := base64.RawURLEncoding.EncodeToString(
		[]byte(workspaceA))
	workspaceBHeader := base64.RawURLEncoding.EncodeToString(
		[]byte(workspaceB))

	httpServer := httptest.NewUnstartedServer(nil)
	mcpHandler, err := NewHTTPHandler(HTTPConfig{
		Server: server,
		AllowedOrigins: []string{
			"http://" + httpServer.Listener.Addr().String(),
		},
		RequireWorkspaceSettings: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := webapi.NewAuthenticatedHandler(
		service, authenticator, authority.Binder(),
		httpServer.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := outer.SetWorkspaceSettingsProvider(settingsProvider); err != nil {
		t.Fatal(err)
	}
	if err := outer.MountAuthenticated("/mcp", mcpHandler); err != nil {
		t.Fatal(err)
	}
	httpServer.Config.Handler = outer
	httpServer.Start()
	t.Cleanup(httpServer.Close)

	missing := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "ignored", "", "",
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{`+
			`"protocolVersion":"`+ProtocolVersion+`","capabilities":{},`+
			`"clientInfo":{"name":"test","version":"1"}}}`)
	if missing.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing workspace status = %d, want 400", missing.StatusCode)
	}
	missing.Body.Close()

	session, initializedBody := initializeHTTPWithWorkspace(
		t, httpServer.Client(), httpServer.URL+"/mcp",
		"ignored", workspaceAHeader, "2")
	var initialized Response
	if err := json.Unmarshal(initializedBody, &initialized); err != nil {
		t.Fatal(err)
	}
	var result InitializeResult
	if err := json.Unmarshal(initialized.Result, &result); err != nil {
		t.Fatal(err)
	}
	metadata, ok := result.Meta["shoal.workspace"].(map[string]any)
	if !ok || metadata["id"] != workspaceAHeader ||
		metadata["revision"] != float64(1) {
		t.Fatalf("workspace initialize metadata = %#v", result.Meta)
	}
	dimensions, ok := metadata["cache_dimensions"].(map[string]any)
	if !ok || len(dimensions) == 0 {
		t.Fatalf("workspace cache dimensions = %#v", metadata)
	}
	assertEmptyBody(t, postHTTPMCPWithWorkspace(
		t, httpServer.Client(), httpServer.URL+"/mcp", "ignored",
		workspaceAHeader, session, ProtocolVersion,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`))

	crossWorkspace := postHTTPMCPWithWorkspace(
		t, httpServer.Client(), httpServer.URL+"/mcp", "ignored",
		workspaceBHeader, session, ProtocolVersion,
		`{"jsonrpc":"2.0","id":3,"method":"tools/list"}`)
	if crossWorkspace.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-workspace status = %d, want 404",
			crossWorkspace.StatusCode)
	}
	crossWorkspace.Body.Close()

	retrieved := postHTTPMCPWithWorkspace(
		t, httpServer.Client(), httpServer.URL+"/mcp", "ignored",
		workspaceAHeader, session, ProtocolVersion,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call",`+
			`"params":{"name":"shoal.retrieve","arguments":{`+
			`"query":{"text":"bounded","top_k":50,"modes":["lexical"]}}}}`)
	if response := decodeHTTPResponse(t, retrieved); response.Error != nil ||
		decodeToolResult(t, response).IsError {
		t.Fatalf("workspace retrieval failed: %+v", response)
	}
	if observedTopK.Load() != 3 {
		t.Fatalf("effective retrieval top_k = %d, want 3",
			observedTopK.Load())
	}
	mcpHandler.sessionsMu.Lock()
	sessionState := mcpHandler.sessions[session]
	mcpHandler.sessionsMu.Unlock()
	if sessionState == nil ||
		sessionState.dispatcher.outputBudget != outputLimit ||
		sessionState.dispatcher.contextBudget != int(outputLimit) {
		t.Fatalf("workspace session limits were not bound")
	}

	decision := workspaceHTTPDecision(t, "workspace-a-update")
	ctx, err := authority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	topK := uint32(2)
	output := outputLimit
	if _, err := settingsProvider.Update(ctx, workspaceA, workspace.UpdateRequest{
		ExpectedRevision: 1,
		MutationID:       "workspace-a-update",
		Narrowing: workspace.UpdateNarrowing{
			Budgets: workspace.Budgets{
				RetrievalTopK: &topK, OutputBytes: &output,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	stale := postHTTPMCPWithWorkspace(
		t, httpServer.Client(), httpServer.URL+"/mcp", "ignored",
		workspaceAHeader, session, ProtocolVersion,
		`{"jsonrpc":"2.0","id":5,"method":"tools/list"}`)
	if stale.StatusCode != http.StatusNotFound {
		t.Fatalf("stale workspace session status = %d, want 404",
			stale.StatusCode)
	}
	stale.Body.Close()
}

func TestHTTPResponseLimitCoversEnvelopeAndMarksToolCallIndeterminate(
	t *testing.T,
) {
	handler := &HTTPHandler{}
	response := newResponse(
		json.RawMessage(`"exact-id"`),
		map[string]string{"payload": strings.Repeat("x", 4096)},
	)
	writer := httptest.NewRecorder()
	const limit = int64(256)
	handler.writeBoundedHTTPResponse(
		writer, http.StatusOK, response, limit, true,
	)
	if writer.Code != http.StatusServiceUnavailable {
		t.Fatalf("overflow status = %d, want 503", writer.Code)
	}
	if writer.Header().Get(webapi.CommitOutcomeHeader) !=
		webapi.CommitOutcomeIndeterminate {
		t.Fatalf("overflow commit outcome = %q",
			writer.Header().Get(webapi.CommitOutcomeHeader))
	}
	if int64(writer.Body.Len()) > limit {
		t.Fatalf("overflow response bytes = %d, limit = %d",
			writer.Body.Len(), limit)
	}
	var failure Response
	if err := json.Unmarshal(writer.Body.Bytes(), &failure); err != nil {
		t.Fatalf("decode bounded overflow response: %v", err)
	}
	if string(failure.ID) != `"exact-id"` || failure.Error == nil {
		t.Fatalf("bounded overflow response = %+v", failure)
	}
	if got := httpResponseLimit(workspaceBinding{
		limits: workspace.Limits{OutputBytes: uint64(limit)},
	}, true); got != limit {
		t.Fatalf("workspace HTTP response limit = %d, want %d", got, limit)
	}
}

func TestStreamableHTTPCancellationReachesActiveTool(t *testing.T) {
	started := make(chan struct{})
	service := &stubService{
		documents: func(
			ctx context.Context, _ webapi.DocumentsRequest,
		) (webapi.DocumentsResponse, error) {
			close(started)
			<-ctx.Done()
			return webapi.DocumentsResponse{}, ctx.Err()
		},
	}
	server, authority := newTestServer(t, service, nil)
	authenticator := webapi.AuthenticatorFunc(func(*http.Request) (auth.Decision, error) {
		return httpTestDecision(
			t, "caller", shoal.ID("request-"+strconv.FormatInt(time.Now().UnixNano(), 10)),
			time.Now().Add(time.Hour),
		), nil
	})
	httpServer := httptest.NewUnstartedServer(nil)
	transport, err := NewHTTPHandler(HTTPConfig{
		Server: server,
		AllowedOrigins: []string{
			"http://" + httpServer.Listener.Addr().String(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := webapi.NewAuthenticatedHandler(
		service, authenticator, authority.Binder(),
		httpServer.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := outer.MountAuthenticated("/mcp", transport); err != nil {
		t.Fatal(err)
	}
	httpServer.Config.Handler = outer
	httpServer.Start()
	t.Cleanup(httpServer.Close)
	session, _ := initializeHTTP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "caller", "1")
	assertEmptyBody(t, postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "caller",
		session, ProtocolVersion,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`))

	result := make(chan *http.Response, 1)
	go func() {
		result <- postHTTPMCP(
			t, httpServer.Client(), httpServer.URL+"/mcp", "caller",
			session, ProtocolVersion,
			`{"jsonrpc":"2.0","id":1e2,"method":"tools/call",`+
				`"params":{"name":"shoal.documents","arguments":{}}}`)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("tool call did not start")
	}
	cancelled := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "caller",
		session, ProtocolVersion,
		`{"jsonrpc":"2.0","method":"notifications/cancelled",`+
			`"params":{"requestId":100}}`)
	if cancelled.StatusCode != http.StatusAccepted {
		t.Fatalf("cancellation status = %d", cancelled.StatusCode)
	}
	assertEmptyBody(t, cancelled)
	select {
	case response := <-result:
		decoded := decodeHTTPResponse(t, response)
		toolResult := decodeToolResult(t, decoded)
		if !toolResult.IsError ||
			!strings.Contains(string(toolResult.StructuredContent), "canceled") {
			t.Fatalf("cancelled tool result = %s", toolResult.StructuredContent)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled tool call did not finish")
	}
}

func TestClosedHTTPSessionRejectsLateRegistration(t *testing.T) {
	session := &httpSession{active: make(map[string]context.CancelFunc)}
	session.close()
	if _, _, err := session.register(
		context.Background(), json.RawMessage("1"),
	); !errors.Is(err, errHTTPSessionClosed) {
		t.Fatalf("late registration error = %v", err)
	}
}

func TestHTTPOriginNormalizationHandlesDefaultPorts(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{"https://Workspace.Example:443", "https://workspace.example"},
		{"http://Workspace.Example:80", "http://workspace.example"},
		{"https://Workspace.Example:8443", "https://workspace.example:8443"},
		{"http://[::1]:80", "http://[::1]"},
	}
	for _, test := range tests {
		got, err := normalizeHTTPOrigin(test.raw)
		if err != nil {
			t.Fatalf("normalize %q: %v", test.raw, err)
		}
		if got != test.want {
			t.Fatalf("normalize %q = %q, want %q",
				test.raw, got, test.want)
		}
	}
}

func TestRecordedStopReasonsPreserveCancellationAndDeadline(t *testing.T) {
	if got := toolStopReason(context.Canceled); got != "canceled" {
		t.Fatalf("canceled stop reason = %q", got)
	}
	if got := toolStopReason(context.DeadlineExceeded); got != "deadline_exceeded" {
		t.Fatalf("deadline stop reason = %q", got)
	}
}

func TestStreamableHTTPRevalidatesAuthorizationBeforeResponse(t *testing.T) {
	service := &stubService{
		documents: func(
			context.Context, webapi.DocumentsRequest,
		) (webapi.DocumentsResponse, error) {
			time.Sleep(100 * time.Millisecond)
			return webapi.DocumentsResponse{}, nil
		},
	}
	server, authority := newTestServer(t, service, nil)
	authenticator := webapi.AuthenticatorFunc(func(*http.Request) (auth.Decision, error) {
		return httpTestDecision(
			t, "short-lived",
			shoal.ID("request-"+strconv.FormatInt(time.Now().UnixNano(), 10)),
			time.Now().Add(50*time.Millisecond),
		), nil
	})
	httpServer := httptest.NewUnstartedServer(nil)
	transport, err := NewHTTPHandler(HTTPConfig{
		Server: server,
		AllowedOrigins: []string{
			"http://" + httpServer.Listener.Addr().String(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	outer, err := webapi.NewAuthenticatedHandler(
		service, authenticator, authority.Binder(),
		httpServer.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := outer.MountAuthenticated("/mcp", transport); err != nil {
		t.Fatal(err)
	}
	httpServer.Config.Handler = outer
	httpServer.Start()
	t.Cleanup(httpServer.Close)
	session, _ := initializeHTTP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "ignored", "1")
	assertEmptyBody(t, postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "ignored",
		session, ProtocolVersion,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	response := postHTTPMCP(
		t, httpServer.Client(), httpServer.URL+"/mcp", "ignored",
		session, ProtocolVersion,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call",`+
			`"params":{"name":"shoal.documents","arguments":{}}}`)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired active response status = %d, want 401",
			response.StatusCode)
	}
	response.Body.Close()
}

func TestGenericToolRecordingPreservesFullRetrievalAndFailureSemantics(t *testing.T) {
	const resultCount = 50
	results := make([]retrieval.Result, resultCount)
	for index := range results {
		results[index].ID = shoal.ID("node-" + strconv.Itoa(index))
	}
	service := &stubService{
		retrieve: func(
			context.Context, webapi.RetrievalRequest,
		) (webapi.RetrievalResponse, error) {
			return webapi.RetrievalResponse{
				Snapshot: webapi.Snapshot{
					ID: "snapshot", AsOf: time.Now().UTC(),
				},
				Retrieval: retrieval.Response{Results: results},
			}, nil
		},
	}
	sink := &testInteractionSink{}
	recorder, err := interaction.NewRecorder(context.Background(), sink)
	if err != nil {
		t.Fatal(err)
	}
	authority := auth.NewAuthority()
	server, err := NewServer(Config{
		Service: service, Authority: authority,
		Decisions: DecisionProviderFunc(func(context.Context) (auth.Decision, error) {
			return testDecision(t), nil
		}),
		Recorder: recorder,
		Snapshots: testSnapshotProvider{snapshot: explorer.Snapshot{
			ID: "snapshot", AsOf: time.Now().UTC(),
		}},
		requestIDFactory: sequentialRequestIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	makeReady(t, server)
	response := callToolRequest(
		t, server, ToolRetrieve,
		`{"query":{"text":"all results","top_k":50}}`)
	if result := decodeToolResult(t, response); result.IsError {
		t.Fatalf("retrieve failed: %s", result.StructuredContent)
	}
	if len(sink.sessions) != 2 {
		t.Fatalf("recorded sessions = %d, want 2", len(sink.sessions))
	}
	var retrievalSession, toolSession interaction.Session
	for _, session := range sink.sessions {
		switch session.Operation {
		case interaction.OperationRetrieval:
			retrievalSession = session
		case interaction.OperationToolCall:
			toolSession = session
		}
	}
	if retrievalSession.ID == "" ||
		len(retrievalSession.SeedNodeIDs) != resultCount {
		t.Fatalf("retrieval session = %+v", retrievalSession)
	}
	if toolSession.ID == "" ||
		toolSession.Actor.SubjectID != "" ||
		toolSession.Actor.ActorID != "" ||
		toolSession.Actor.ClientID != "" ||
		len(toolSession.Actor.OnBehalfOf) != 0 ||
		toolSession.Reason != (interaction.Reason{}) ||
		toolSession.QueryDigest == "" ||
		len(toolSession.Turns) != 1 ||
		toolSession.Turns[0].ToolCall == nil ||
		len(toolSession.Turns[0].ToolCall.RetrievedNodeIDs) != resultCount {
		t.Fatalf("recorded tool session = %+v", toolSession)
	}
	if !containsObservedID(
		toolSession.Turns[0].ToolCall.RetrievedNodeIDs,
		shoal.ID("node-"+strconv.Itoa(resultCount-1)),
	) {
		t.Fatal("retrieved source set was truncated")
	}

	readCalls := 0
	failingSink := &testInteractionSink{
		err: errors.New("record unavailable"),
	}
	failingRecorder, err := interaction.NewRecorder(
		context.Background(), failingSink)
	if err != nil {
		t.Fatal(err)
	}
	server, err = NewServer(Config{
		Service: &stubService{
			documents: func(
				context.Context, webapi.DocumentsRequest,
			) (webapi.DocumentsResponse, error) {
				readCalls++
				return webapi.DocumentsResponse{}, nil
			},
		},
		Authority: authority,
		Decisions: DecisionProviderFunc(func(context.Context) (auth.Decision, error) {
			return testDecision(t), nil
		}),
		Recorder: failingRecorder,
		Snapshots: testSnapshotProvider{snapshot: explorer.Snapshot{
			ID: "snapshot", AsOf: time.Now().UTC(),
		}},
		requestIDFactory: sequentialRequestIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	makeReady(t, server)
	response = callToolRequest(t, server, ToolDocuments, `{}`)
	if result := decodeToolResult(t, response); !result.IsError {
		t.Fatal("unrecorded read-only tool call succeeded")
	}
	if readCalls != 1 {
		t.Fatalf("read calls = %d, want 1", readCalls)
	}

	mutating := &allOptionalService{
		stubService: &stubService{}, calls: make(map[string]int),
	}
	admissionSink := &testInteractionSink{
		err: errors.New("admission storage unavailable"),
	}
	admissionRecorder, err := interaction.NewRecorder(
		context.Background(), admissionSink)
	if err != nil {
		t.Fatal(err)
	}
	server, err = NewServer(Config{
		Service: mutating, Authority: authority,
		Decisions: DecisionProviderFunc(func(context.Context) (auth.Decision, error) {
			return testDecision(t), nil
		}),
		Recorder: admissionRecorder,
		Snapshots: testSnapshotProvider{snapshot: explorer.Snapshot{
			ID: "snapshot", AsOf: time.Now().UTC(),
		}},
		requestIDFactory: sequentialRequestIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	makeReady(t, server)
	response = callToolRequest(
		t, server, ToolIngest,
		`{"files":[{"name":"note.txt","content":"aGVsbG8="}]}`)
	if result := decodeToolResult(t, response); !result.IsError {
		t.Fatal("admission recording failure succeeded")
	}
	if mutating.calls[ToolIngest] != 0 {
		t.Fatal("mutation ran before admission was durably recorded")
	}

	fixedSink := &testInteractionSink{}
	fixedRecorder, err := interaction.NewRecorder(
		context.Background(), fixedSink)
	if err != nil {
		t.Fatal(err)
	}
	fixedTime := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	successfulMutation := &allOptionalService{
		stubService: &stubService{}, calls: make(map[string]int),
	}
	server, err = NewServer(Config{
		Service: successfulMutation, Authority: authority,
		Decisions: DecisionProviderFunc(func(context.Context) (auth.Decision, error) {
			return testDecision(t), nil
		}),
		Recorder: fixedRecorder,
		Snapshots: testSnapshotProvider{snapshot: explorer.Snapshot{
			ID: "snapshot", AsOf: fixedTime,
		}},
		requestIDFactory: sequentialRequestIDs(),
		interactionClock: func() time.Time {
			return fixedTime
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	makeReady(t, server)
	response = callToolRequest(
		t, server, ToolIngest,
		`{"files":[{"name":"note.txt","content":"aGVsbG8="}]}`)
	if result := decodeToolResult(t, response); result.IsError {
		t.Fatalf("recorded mutation failed: %s", result.StructuredContent)
	}
	if len(fixedSink.sessions) != 2 ||
		fixedSink.sessions[0].ID == fixedSink.sessions[1].ID ||
		fixedSink.sessions[0].StopReason != "admitted" ||
		fixedSink.sessions[1].StopReason != "succeeded" {
		t.Fatalf("mutation receipts = %+v", fixedSink.sessions)
	}

	postSink := &sequencedInteractionSink{failAt: 2}
	postRecorder, err := interaction.NewRecorder(context.Background(), postSink)
	if err != nil {
		t.Fatal(err)
	}
	server, err = NewServer(Config{
		Service: mutating, Authority: authority,
		Decisions: DecisionProviderFunc(func(context.Context) (auth.Decision, error) {
			return testDecision(t), nil
		}),
		Recorder: postRecorder,
		Snapshots: testSnapshotProvider{snapshot: explorer.Snapshot{
			ID: "snapshot", AsOf: time.Now().UTC(),
		}},
		requestIDFactory: sequentialRequestIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	makeReady(t, server)
	response = callToolRequest(
		t, server, ToolIngest,
		`{"files":[{"name":"note.txt","content":"aGVsbG8="}]}`)
	result := decodeToolResult(t, response)
	if !result.IsError ||
		!strings.Contains(string(result.StructuredContent), `"code":"indeterminate"`) ||
		!strings.Contains(string(result.StructuredContent), "verify current state") {
		t.Fatalf("post-effect recording result = %s", result.StructuredContent)
	}
	if mutating.calls[ToolIngest] != 1 {
		t.Fatalf("mutation calls = %d, want 1", mutating.calls[ToolIngest])
	}

	deniedMutation := &allOptionalService{
		stubService: &stubService{}, calls: make(map[string]int),
	}
	server, err = NewServer(Config{
		Service: deniedMutation, Authority: authority,
		Decisions: DecisionProviderFunc(func(context.Context) (auth.Decision, error) {
			return decisionWithExpiry(t, time.Now().Add(-time.Minute)), nil
		}),
		Recorder: postRecorder,
		Snapshots: testSnapshotProvider{snapshot: explorer.Snapshot{
			ID: "snapshot", AsOf: time.Now().UTC(),
		}},
		requestIDFactory: sequentialRequestIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	makeReady(t, server)
	response = callToolRequest(
		t, server, ToolIngest,
		`{"files":[{"name":"note.txt","content":"aGVsbG8="}]}`)
	if result := decodeToolResult(t, response); !result.IsError {
		t.Fatal("unauthorized mutation succeeded")
	}
	if deniedMutation.calls[ToolIngest] != 0 {
		t.Fatal("mutation executed before authorization")
	}
}

func TestOptionalActionRequiresExactAuthorizationAndRecordsIt(t *testing.T) {
	calls := 0
	provider := optionalProvider{
		tool: Tool{
			Name: "fleet.action", Description: "Execute one test action.",
			InputSchema: json.RawMessage(
				`{"type":"object","additionalProperties":false}`),
		},
		operation: auth.OperationInvoke,
		call: func(context.Context, json.RawMessage) (any, error) {
			calls++
			return struct {
				Accepted bool `json:"accepted"`
			}{Accepted: true}, nil
		},
	}
	authority := auth.NewAuthority()
	deniedRecorder, err := interaction.NewRecorder(
		context.Background(), &testInteractionSink{})
	if err != nil {
		t.Fatal(err)
	}
	denied, err := NewServer(Config{
		Service: &stubService{}, Authority: authority,
		Decisions: DecisionProviderFunc(func(context.Context) (auth.Decision, error) {
			return testDecision(t), nil
		}),
		Recorder: deniedRecorder,
		Snapshots: testSnapshotProvider{snapshot: explorer.Snapshot{
			ID: "snapshot", AsOf: time.Now().UTC(),
		}},
		OptionalTools:    []OptionalToolProvider{provider},
		requestIDFactory: sequentialRequestIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	makeReady(t, denied)
	if result := decodeToolResult(
		t, callToolRequest(t, denied, "fleet.action", `{}`),
	); !result.IsError {
		t.Fatal("action without invoke authorization succeeded")
	}
	if calls != 0 {
		t.Fatal("unauthorized action reached provider")
	}

	sink := &testInteractionSink{}
	recorder, err := interaction.NewRecorder(context.Background(), sink)
	if err != nil {
		t.Fatal(err)
	}
	allowed, err := NewServer(Config{
		Service: &stubService{}, Authority: authority,
		Decisions: DecisionProviderFunc(func(context.Context) (auth.Decision, error) {
			return actionOnlyDecision(t, auth.OperationInvoke), nil
		}),
		Recorder: recorder,
		Snapshots: testSnapshotProvider{snapshot: explorer.Snapshot{
			ID: "snapshot", AsOf: time.Now().UTC(),
		}},
		OptionalTools:    []OptionalToolProvider{provider},
		requestIDFactory: sequentialRequestIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	makeReady(t, allowed)
	if result := decodeToolResult(
		t, callToolRequest(t, allowed, "fleet.action", `{}`),
	); result.IsError {
		t.Fatalf("authorized action failed: %s", result.StructuredContent)
	}
	if calls != 1 || len(sink.sessions) != 2 {
		t.Fatalf("action calls=%d records=%d", calls, len(sink.sessions))
	}
	for _, session := range sink.sessions {
		if session.Operation != interaction.OperationToolCall ||
			session.AuthorizationOperation != string(auth.OperationInvoke) ||
			len(session.TouchedNodeIDs()) != 0 {
			t.Fatalf("action interaction = %+v", session)
		}
	}
}

type sequencedInteractionSink struct {
	calls  int
	failAt int
}

func (*sequencedInteractionSink) EnsureInteractionSink(context.Context) error {
	return nil
}

func (s *sequencedInteractionSink) RecordInteraction(
	ctx context.Context, session interaction.Session,
) error {
	_, err := s.RecordInteractionResult(ctx, session)
	return err
}

func (s *sequencedInteractionSink) RecordInteractionResult(
	_ context.Context, session interaction.Session,
) (interaction.Session, error) {
	s.calls++
	if s.calls == s.failAt {
		return interaction.Session{}, errors.New("outcome storage unavailable")
	}
	return session, nil
}

func initializeHTTP(
	t *testing.T,
	client *http.Client,
	endpoint string,
	token string,
	id string,
) (string, []byte) {
	t.Helper()
	response := postHTTPMCP(
		t, client, endpoint, token, "", "",
		`{"jsonrpc":"2.0","id":`+id+`,"method":"initialize","params":{`+
			`"protocolVersion":"`+ProtocolVersion+`","capabilities":{},`+
			`"clientInfo":{"name":"test","version":"1"}}}`)
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("initialize status = %d: %s", response.StatusCode, body)
	}
	session := response.Header.Get(SessionHeader)
	if session == "" {
		t.Fatal("initialize response omitted MCP-Session-Id")
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return session, body
}

func initializeHTTPWithWorkspace(
	t *testing.T,
	client *http.Client,
	endpoint string,
	token string,
	workspaceID string,
	id string,
) (string, []byte) {
	t.Helper()
	request := newHTTPMCPRequest(
		t, http.MethodPost, endpoint, token, "", "",
		`{"jsonrpc":"2.0","id":`+id+`,"method":"initialize","params":{`+
			`"protocolVersion":"`+ProtocolVersion+`","capabilities":{},`+
			`"clientInfo":{"name":"test","version":"1"}}}`)
	request.Header.Set(webapi.WorkspaceIDHeader, workspaceID)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("initialize status = %d: %s", response.StatusCode, body)
	}
	session := response.Header.Get(SessionHeader)
	if session == "" {
		t.Fatal("initialize response omitted MCP-Session-Id")
	}
	return session, body
}

func postHTTPMCP(
	t *testing.T,
	client *http.Client,
	endpoint string,
	token string,
	session string,
	version string,
	body string,
) *http.Response {
	t.Helper()
	request := newHTTPMCPRequest(
		t, http.MethodPost, endpoint, token, session, version, body)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func postHTTPMCPWithWorkspace(
	t *testing.T,
	client *http.Client,
	endpoint string,
	token string,
	workspaceID string,
	session string,
	version string,
	body string,
) *http.Response {
	t.Helper()
	request := newHTTPMCPRequest(
		t, http.MethodPost, endpoint, token, session, version, body)
	request.Header.Set(webapi.WorkspaceIDHeader, workspaceID)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func newHTTPMCPRequest(
	t *testing.T,
	method string,
	endpoint string,
	token string,
	session string,
	version string,
	body string,
) *http.Request {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request, err := http.NewRequest(method, endpoint, reader)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if session != "" {
		request.Header.Set(SessionHeader, session)
	}
	if version != "" {
		request.Header.Set(ProtocolVersionHeader, version)
	}
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(
			"Accept", "application/json, text/event-stream")
	}
	return request
}

func decodeHTTPResponse(t *testing.T, response *http.Response) Response {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Response
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode HTTP response %s: %v", body, err)
	}
	return decoded
}

func assertEmptyBody(t *testing.T, response *http.Response) {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != 0 {
		t.Fatalf("body = %q, want empty", body)
	}
}

func httpTestDecision(
	t *testing.T,
	subject shoal.ID,
	requestID shoal.ID,
	expires time.Time,
) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: subject, Actor: "http-client",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations: []auth.Operation{
			auth.OperationList, auth.OperationRead, auth.OperationRetrieve,
			auth.OperationNeighborhood, auth.OperationIngest,
			auth.OperationConnect, auth.OperationValidate,
		},
		PermittedSourceIDs:    [][]byte{[]byte("source")},
		PermittedPolicyIDs:    [][]byte{[]byte("policy")},
		PolicyGeneration:      1,
		AuthenticationExpires: expires,
		RequestID:             requestID,
		AuditPurpose:          "test HTTP MCP request",
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

func workspaceHTTPDecision(t *testing.T, requestID shoal.ID) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "workspace-user", Actor: "http-client",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations: []auth.Operation{
			auth.OperationIngest,
			auth.OperationList,
			auth.OperationRead,
			auth.OperationRetrieve,
			auth.OperationConnect,
			auth.OperationNeighborhood,
			auth.OperationValidate,
			auth.OperationWorkspaceSettingsRead,
			auth.OperationWorkspaceSettingsWrite,
		},
		PermittedSourceIDs:    [][]byte{[]byte("source")},
		PermittedPolicyIDs:    [][]byte{[]byte("policy")},
		PolicyGeneration:      1,
		AuthenticationExpires: time.Now().Add(time.Hour),
		RequestID:             requestID,
		AuditPurpose:          "test workspace MCP request",
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

func actionOnlyDecision(
	t *testing.T, operation auth.Operation,
) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "action-subject", Actor: "action-actor",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations: []auth.Operation{
			operation,
			auth.OperationValidate,
		},
		PermittedSourceIDs:    [][]byte{[]byte("source")},
		PermittedPolicyIDs:    [][]byte{[]byte("policy")},
		PolicyGeneration:      1,
		AuthenticationExpires: time.Now().Add(time.Hour),
		RequestID:             "action-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

func httpTestDecisionWithGeneration(
	t *testing.T,
	subject shoal.ID,
	requestID shoal.ID,
	expires time.Time,
	generation int64,
) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: subject, Actor: "http-client",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations: []auth.Operation{
			auth.OperationList, auth.OperationRead, auth.OperationRetrieve,
			auth.OperationNeighborhood, auth.OperationIngest,
			auth.OperationConnect, auth.OperationValidate,
		},
		PermittedSourceIDs:    [][]byte{[]byte("source")},
		PermittedPolicyIDs:    [][]byte{[]byte("policy")},
		PolicyGeneration:      generation,
		AuthenticationExpires: expires,
		RequestID:             requestID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

func httpScopedDecision(
	t *testing.T,
	subject shoal.ID,
	requestID shoal.ID,
	expires time.Time,
	sourceID []byte,
	policyID []byte,
) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: subject, Actor: "http-client",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations: []auth.Operation{
			auth.OperationList, auth.OperationRead, auth.OperationRetrieve,
			auth.OperationNeighborhood, auth.OperationIngest,
			auth.OperationConnect, auth.OperationValidate,
		},
		PermittedSourceIDs:    [][]byte{sourceID},
		PermittedPolicyIDs:    [][]byte{policyID},
		PolicyGeneration:      1,
		AuthenticationExpires: expires,
		RequestID:             requestID,
		AuditPurpose:          "test HTTP MCP request",
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

type httpGenerationReader struct {
	domain     []byte
	generation int64
}

func (r httpGenerationReader) CurrentPolicyGeneration(
	ctx context.Context, domain []byte,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !bytes.Equal(domain, r.domain) {
		return 0, nil
	}
	return r.generation, nil
}

func mustInteractionSubgraph(
	t *testing.T,
	client *authorized.Client,
	ctx context.Context,
	sessionID shoal.ID,
) explorer.Neighborhood {
	t.Helper()
	subgraph, err := client.InteractionSubgraph(ctx, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	return subgraph
}

func containsObservedID(values []shoal.ID, want shoal.ID) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
