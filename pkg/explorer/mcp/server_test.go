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
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestServeNegotiatesVersionAndEnforcesInitializationOrdering(t *testing.T) {
	server, _ := newTestServer(t, &stubService{}, nil)
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":1e2,"method":"initialize","params":{"protocolVersion":"2099-01-01","capabilities":{},"clientInfo":{"name":"test","title":"Test Client","version":"1","description":"test","icons":[{"src":"data:image/png;base64,AA==","mimeType":"image/png","sizes":["16x16"]}],"websiteUrl":"https://example.test"}}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":"after","method":"tools/list"}`,
	}, "\n") + "\n"
	var output bytes.Buffer
	if err := server.Serve(context.Background(), strings.NewReader(input), &output); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	lines := nonemptyLines(output.String())
	if len(lines) != 4 {
		t.Fatalf("responses = %d, want 4:\n%s", len(lines), output.String())
	}
	var before Response
	mustUnmarshal(t, lines[0], &before)
	if before.Error == nil || before.Error.Code != codeInvalidRequest {
		t.Fatalf("pre-initialize response = %+v", before)
	}
	if !strings.Contains(lines[1], `"id":1e2`) {
		t.Fatalf("initialize did not preserve raw numeric ID: %s", lines[1])
	}
	var initialized Response
	mustUnmarshal(t, lines[1], &initialized)
	if initialized.Error != nil {
		t.Fatalf("initialize error = %+v", initialized.Error)
	}
	var result InitializeResult
	mustUnmarshal(t, string(initialized.Result), &result)
	if result.ProtocolVersion != ProtocolVersion {
		t.Fatalf("negotiated version = %q, want %q", result.ProtocolVersion, ProtocolVersion)
	}
	if result.Capabilities.Tools == nil || result.Capabilities.Tools.ListChanged {
		t.Fatalf("tools capability = %+v", result.Capabilities.Tools)
	}
	if !strings.Contains(result.Instructions, "recording is deferred") {
		t.Fatalf("instructions claim unsupported recording: %q", result.Instructions)
	}
	var awaitingNotification Response
	mustUnmarshal(t, lines[2], &awaitingNotification)
	if awaitingNotification.Error == nil ||
		awaitingNotification.Error.Code != codeInvalidRequest {
		t.Fatalf("pre-notification response = %+v", awaitingNotification)
	}
	var after Response
	mustUnmarshal(t, lines[3], &after)
	if after.Error != nil {
		t.Fatalf("post-initialization tools/list error = %+v", after.Error)
	}
}

func TestInitializedNotificationIsSilentAndCannotRunEarly(t *testing.T) {
	server, _ := newTestServer(t, &stubService{}, nil)
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`,
	}, "\n") + "\n"
	var output bytes.Buffer
	if err := server.Serve(context.Background(), strings.NewReader(input), &output); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	lines := nonemptyLines(output.String())
	if len(lines) != 1 {
		t.Fatalf("responses = %d, want 1: %s", len(lines), output.String())
	}
	var response Response
	mustUnmarshal(t, lines[0], &response)
	if response.Error == nil || response.Error.Code != codeInvalidRequest {
		t.Fatalf("response = %+v", response)
	}
}

func TestPingIsAvailableThroughoutHandshake(t *testing.T) {
	server, _ := newTestServer(t, &stubService{}, nil)
	ping := func(id string) {
		response := server.handle(context.Background(), Request{
			JSONRPC: jsonRPCVersion, ID: json.RawMessage(id), Method: "ping",
		})
		if response == nil || response.Error != nil || string(response.Result) != "{}" {
			t.Fatalf("ping response = %+v", response)
		}
	}
	ping("1")
	makeReady(t, server)
	ping("2")

	repeated := server.handle(context.Background(), Request{
		JSONRPC: jsonRPCVersion, ID: json.RawMessage("3"),
		Method: "initialize",
		Params: json.RawMessage(
			`{"protocolVersion":"` + ProtocolVersion +
				`","capabilities":{},"clientInfo":{"name":"test","version":"1"}}`),
	})
	if repeated == nil || repeated.Error == nil ||
		repeated.Error.Code != codeInvalidRequest {
		t.Fatalf("repeated initialize = %+v", repeated)
	}
}

func TestToolAdvertisementTracksOptionalProviders(t *testing.T) {
	base := &stubService{}
	server, _ := newTestServer(t, base, nil)
	makeReady(t, server)
	names := listedToolNames(t, server)
	assertStrings(t, names, []string{
		ToolDocument,
		ToolDocuments,
		ToolNeighborhood,
		ToolPath,
		ToolRetrieve,
	})

	optional := optionalProvider{
		tool: Tool{
			Name: "shoal.compress_context", Description: "Compress context.",
			InputSchema: json.RawMessage(
				`{"type":"object","additionalProperties":false}`),
		},
	}
	server, _ = newTestServer(t, base, []OptionalToolProvider{optional})
	makeReady(t, server)
	names = listedToolNames(t, server)
	assertStrings(t, names, []string{
		"shoal.compress_context",
		ToolDocument,
		ToolDocuments,
		ToolNeighborhood,
		ToolPath,
		ToolRetrieve,
	})
	for _, absent := range []string{ToolIngest, ToolExtract, ToolRecompute} {
		if containsString(names, absent) {
			t.Fatalf("unimplemented optional tool %q was advertised", absent)
		}
	}
}

func TestChangeToolRequiresServiceChangeProvider(t *testing.T) {
	server, _ := newTestServer(t, &stubService{}, nil)
	makeReady(t, server)
	names := listedToolNames(t, server)
	if containsString(names, ToolChanges) {
		t.Fatalf("change tool advertised for service without ChangeProvider")
	}
	response := callToolRequest(t, server, ToolChanges, `{}`)
	if response.Error == nil || response.Error.Code != codeInvalidParams {
		t.Fatalf("absent change tool response = %+v", response)
	}

	server, _ = newTestServer(
		t, &changesService{stubService: &stubService{}}, nil)
	makeReady(t, server)
	names = listedToolNames(t, server)
	if !containsString(names, ToolChanges) {
		t.Fatalf("change tool omitted for service implementing ChangeProvider")
	}
	response = callToolRequest(t, server, ToolChanges, `{}`)
	if response.Error != nil {
		t.Fatalf("change tool protocol error = %+v", response.Error)
	}
	result := decodeToolResult(t, response)
	if result.IsError {
		t.Fatalf("change tool error = %s", result.StructuredContent)
	}
	if !strings.Contains(string(result.StructuredContent), `"next_cursor":"next"`) {
		t.Fatalf("change result = %s", result.StructuredContent)
	}

	if _, ok := any(&webapi.RemoteService{}).(webapi.ChangeProvider); ok {
		t.Fatal("RemoteService unexpectedly implements ChangeProvider")
	}
}

func TestMandatoryToolsDispatchToServiceReadPaths(t *testing.T) {
	called := make(map[string]int)
	service := &stubService{
		documents: func(
			context.Context,
			webapi.DocumentsRequest,
		) (webapi.DocumentsResponse, error) {
			called[ToolDocuments]++
			return webapi.DocumentsResponse{}, nil
		},
		document: func(
			context.Context,
			webapi.DocumentRequest,
		) (webapi.DocumentResponse, error) {
			called[ToolDocument]++
			return webapi.DocumentResponse{}, nil
		},
		retrieve: func(
			context.Context,
			webapi.RetrievalRequest,
		) (webapi.RetrievalResponse, error) {
			called[ToolRetrieve]++
			return webapi.RetrievalResponse{}, nil
		},
		neighborhood: func(
			context.Context,
			webapi.NeighborhoodRequest,
		) (webapi.NeighborhoodResponse, error) {
			called[ToolNeighborhood]++
			return webapi.NeighborhoodResponse{}, nil
		},
		path: func(
			context.Context,
			webapi.PathRequest,
		) (webapi.PathResponse, error) {
			called[ToolPath]++
			return webapi.PathResponse{}, nil
		},
	}
	server, _ := newTestServer(t, service, nil)
	makeReady(t, server)
	calls := []struct {
		name      string
		arguments string
	}{
		{ToolDocuments, `{}`},
		{ToolDocument, `{"snapshot":{},"document_id":"ZG9j"}`},
		{ToolRetrieve, `{"snapshot":{},"query":{"text":"query"}}`},
		{ToolNeighborhood, `{"snapshot":{},"node_ids":["bm9kZQ"]}`},
		{ToolPath, `{"snapshot":{},"from":"ZnJvbQ","to":"dG8"}`},
	}
	for _, call := range calls {
		response := callToolRequest(t, server, call.name, call.arguments)
		if response.Error != nil {
			t.Fatalf("%s protocol error = %+v", call.name, response.Error)
		}
		if result := decodeToolResult(t, response); result.IsError {
			t.Fatalf("%s tool error = %s", call.name, result.StructuredContent)
		}
	}
	for _, call := range calls {
		if called[call.name] != 1 {
			t.Fatalf("%s calls = %d, want 1", call.name, called[call.name])
		}
	}
}

func TestOptionalProviderAbsenceFailsClosed(t *testing.T) {
	calls := 0
	server, _ := newTestServer(t, &stubService{}, nil)
	server.decisions = DecisionProviderFunc(func(context.Context) (auth.Decision, error) {
		calls++
		return testDecision(t), nil
	})
	makeReady(t, server)
	names := listedToolNames(t, server)
	if containsString(names, ToolChanges) {
		t.Fatalf("change tool advertised without ChangeProvider")
	}
	response := callToolRequest(t, server, ToolChanges, `{}`)
	if response.Error == nil || response.Error.Code != codeInvalidParams {
		t.Fatalf("absent optional tool response = %+v", response)
	}
	if calls != 1 {
		t.Fatalf("decision calls = %d, want 1", calls)
	}
}

func TestToolResultsAreStructuredAndExecutionErrorsAreNotProtocolErrors(t *testing.T) {
	var documentsErr error
	service := &stubService{
		documents: func(
			context.Context,
			webapi.DocumentsRequest,
		) (webapi.DocumentsResponse, error) {
			if documentsErr != nil {
				return webapi.DocumentsResponse{}, documentsErr
			}
			return webapi.DocumentsResponse{
				Snapshot:  webapi.Snapshot{ID: "snapshot", Frontier: 7},
				Documents: nil,
			}, nil
		},
	}
	server, _ := newTestServer(t, service, nil)
	makeReady(t, server)

	response := callToolRequest(t, server, ToolDocuments, `{}`)
	if response.Error != nil {
		t.Fatalf("success protocol error = %+v", response.Error)
	}
	result := decodeToolResult(t, response)
	if result.IsError {
		t.Fatalf("success marked as error: %s", result.StructuredContent)
	}
	var success map[string]any
	mustUnmarshal(t, string(result.StructuredContent), &success)
	if success["snapshot"] == nil || len(result.Content) != 1 {
		t.Fatalf("structured success = %#v, content = %#v", success, result.Content)
	}

	documentsErr = shoal.NewError(shoal.ErrorNotFound, "document set not found")
	response = callToolRequest(t, server, ToolDocuments, `{}`)
	if response.Error != nil {
		t.Fatalf("tool failure became protocol error = %+v", response.Error)
	}
	result = decodeToolResult(t, response)
	if !result.IsError {
		t.Fatalf("tool failure not marked as error: %s", result.StructuredContent)
	}
	var failure structuredToolFailure
	mustUnmarshal(t, string(result.StructuredContent), &failure)
	if failure.Error.Code != string(shoal.ErrorNotFound) ||
		failure.Error.Message != "document set not found" {
		t.Fatalf("structured failure = %+v", failure)
	}

	documentsErr = errors.New("private backend detail")
	response = callToolRequest(t, server, ToolDocuments, `{}`)
	result = decodeToolResult(t, response)
	if strings.Contains(string(result.StructuredContent), "private backend detail") {
		t.Fatalf("unsanitized tool error: %s", result.StructuredContent)
	}
}

func TestEveryToolCallBindsFreshDecisionAndRequestID(t *testing.T) {
	authority := auth.NewAuthority()
	resolver := authority.Resolver()
	var decisions int
	var observed []shoal.ID
	service := &stubService{
		documents: func(
			ctx context.Context,
			_ webapi.DocumentsRequest,
		) (webapi.DocumentsResponse, error) {
			decision, err := resolver.Resolve(ctx)
			if err != nil {
				return webapi.DocumentsResponse{}, err
			}
			observed = append(observed, decision.RequestID())
			if decision.Subject() != "subject" || decision.Actor() != "actor" {
				t.Fatalf("bound identity = %q/%q", decision.Subject(), decision.Actor())
			}
			return webapi.DocumentsResponse{}, nil
		},
	}
	server, err := NewServer(Config{
		Service: service, Authority: authority,
		Decisions: DecisionProviderFunc(func(context.Context) (auth.Decision, error) {
			decisions++
			return testDecision(t), nil
		}),
		requestIDFactory: sequentialRequestIDs(),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	makeReady(t, server)
	for range 2 {
		response := callToolRequest(t, server, ToolDocuments, `{}`)
		if response.Error != nil || decodeToolResult(t, response).IsError {
			t.Fatalf("tools/call failed: %+v", response)
		}
	}
	if decisions != 2 {
		t.Fatalf("decision resolutions = %d, want 2", decisions)
	}
	assertIDs(t, observed, []shoal.ID{"request-1", "request-2"})
	if observed[0] == testDecision(t).RequestID() {
		t.Fatalf("template request ID was reused")
	}
}

func TestToolArgumentValidationIsStructured(t *testing.T) {
	serviceCalls := 0
	service := &stubService{
		documents: func(
			context.Context,
			webapi.DocumentsRequest,
		) (webapi.DocumentsResponse, error) {
			serviceCalls++
			return webapi.DocumentsResponse{}, nil
		},
	}
	server, _ := newTestServer(t, service, nil)
	makeReady(t, server)
	response := callToolRequest(t, server, ToolDocuments, `{"unexpected":true}`)
	if response.Error != nil {
		t.Fatalf("argument error became protocol error = %+v", response.Error)
	}
	result := decodeToolResult(t, response)
	if !result.IsError ||
		!strings.Contains(string(result.StructuredContent), "invalid_argument") {
		t.Fatalf("argument result = %+v", result)
	}
	if serviceCalls != 0 {
		t.Fatalf("service called %d times for invalid arguments", serviceCalls)
	}

	params := json.RawMessage(`{"name":"shoal.documents","arguments":[]}`)
	responsePointer := server.handle(context.Background(), Request{
		JSONRPC: jsonRPCVersion, ID: json.RawMessage("9"),
		Method: "tools/call", Params: params,
	})
	if responsePointer == nil || responsePointer.Error == nil ||
		responsePointer.Error.Code != codeInvalidParams {
		t.Fatalf("malformed call response = %+v", responsePointer)
	}
}

type stubService struct {
	documents func(
		context.Context,
		webapi.DocumentsRequest,
	) (webapi.DocumentsResponse, error)
	document func(
		context.Context,
		webapi.DocumentRequest,
	) (webapi.DocumentResponse, error)
	retrieve func(
		context.Context,
		webapi.RetrievalRequest,
	) (webapi.RetrievalResponse, error)
	neighborhood func(
		context.Context,
		webapi.NeighborhoodRequest,
	) (webapi.NeighborhoodResponse, error)
	path func(
		context.Context,
		webapi.PathRequest,
	) (webapi.PathResponse, error)
}

func (s *stubService) Documents(
	ctx context.Context,
	request webapi.DocumentsRequest,
) (webapi.DocumentsResponse, error) {
	if s.documents != nil {
		return s.documents(ctx, request)
	}
	return webapi.DocumentsResponse{}, nil
}

func (s *stubService) Document(
	ctx context.Context,
	request webapi.DocumentRequest,
) (webapi.DocumentResponse, error) {
	if s.document != nil {
		return s.document(ctx, request)
	}
	return webapi.DocumentResponse{}, nil
}

func (s *stubService) Retrieve(
	ctx context.Context,
	request webapi.RetrievalRequest,
) (webapi.RetrievalResponse, error) {
	if s.retrieve != nil {
		return s.retrieve(ctx, request)
	}
	return webapi.RetrievalResponse{}, nil
}

func (s *stubService) Neighborhood(
	ctx context.Context,
	request webapi.NeighborhoodRequest,
) (webapi.NeighborhoodResponse, error) {
	if s.neighborhood != nil {
		return s.neighborhood(ctx, request)
	}
	return webapi.NeighborhoodResponse{}, nil
}

func (s *stubService) Path(
	ctx context.Context,
	request webapi.PathRequest,
) (webapi.PathResponse, error) {
	if s.path != nil {
		return s.path(ctx, request)
	}
	return webapi.PathResponse{}, nil
}

type changesService struct {
	*stubService
}

func (*changesService) Changes(
	context.Context,
	webapi.ChangesRequest,
) (webapi.ChangesResponse, error) {
	return webapi.ChangesResponse{NextCursor: "next"}, nil
}

type optionalProvider struct {
	tool Tool
}

func (p optionalProvider) Tool() Tool {
	return p.tool
}

func (optionalProvider) Call(
	context.Context,
	json.RawMessage,
) (any, error) {
	return struct {
		Compressed bool `json:"compressed"`
	}{Compressed: true}, nil
}

func newTestServer(
	t *testing.T,
	service webapi.Service,
	optional []OptionalToolProvider,
) (*Server, *auth.Authority) {
	t.Helper()
	authority := auth.NewAuthority()
	server, err := NewServer(Config{
		Service: service, Authority: authority,
		Decisions: DecisionProviderFunc(func(context.Context) (auth.Decision, error) {
			return testDecision(t), nil
		}),
		OptionalTools:    optional,
		requestIDFactory: sequentialRequestIDs(),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return server, authority
}

func testDecision(t *testing.T) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "subject", Actor: "actor",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations: []auth.Operation{
			auth.OperationList,
			auth.OperationRead,
			auth.OperationRetrieve,
			auth.OperationNeighborhood,
			auth.OperationIngest,
			auth.OperationConnect,
			auth.OperationValidate,
		},
		PermittedSourceIDs:    [][]byte{[]byte("source")},
		PermittedPolicyIDs:    [][]byte{[]byte("policy")},
		PolicyGeneration:      1,
		AuthenticationExpires: time.Now().Add(time.Hour),
		RequestID:             "template-request",
	})
	if err != nil {
		t.Fatalf("NewDecision: %v", err)
	}
	return decision
}

func sequentialRequestIDs() func() (shoal.ID, error) {
	next := 0
	return func() (shoal.ID, error) {
		next++
		return shoal.ID("request-" + string(rune('0'+next))), nil
	}
}

func makeReady(t *testing.T, server *Server) {
	t.Helper()
	params := json.RawMessage(
		`{"protocolVersion":"` + ProtocolVersion +
			`","capabilities":{},"clientInfo":{"name":"test","version":"1"}}`)
	response := server.handle(context.Background(), Request{
		JSONRPC: jsonRPCVersion, ID: json.RawMessage("1"),
		Method: "initialize", Params: params,
	})
	if response == nil || response.Error != nil {
		t.Fatalf("initialize = %+v", response)
	}
	if response := server.handle(context.Background(), Request{
		JSONRPC: jsonRPCVersion, Method: "notifications/initialized",
	}); response != nil {
		t.Fatalf("initialized notification returned %+v", response)
	}
}

func listedToolNames(t *testing.T, server *Server) []string {
	t.Helper()
	response := server.handle(context.Background(), Request{
		JSONRPC: jsonRPCVersion, ID: json.RawMessage("2"),
		Method: "tools/list",
	})
	if response == nil || response.Error != nil {
		t.Fatalf("tools/list = %+v", response)
	}
	var result ListToolsResult
	mustUnmarshal(t, string(response.Result), &result)
	names := make([]string, len(result.Tools))
	for index := range result.Tools {
		names[index] = result.Tools[index].Name
	}
	return names
}

func callToolRequest(
	t *testing.T,
	server *Server,
	name string,
	arguments string,
) Response {
	t.Helper()
	params, err := json.Marshal(CallToolParams{
		Name: name, Arguments: json.RawMessage(arguments),
	})
	if err != nil {
		t.Fatalf("marshal call params: %v", err)
	}
	response := server.handle(context.Background(), Request{
		JSONRPC: jsonRPCVersion, ID: json.RawMessage(`"call"`),
		Method: "tools/call", Params: params,
	})
	if response == nil {
		t.Fatal("tools/call returned no response")
	}
	return *response
}

func decodeToolResult(t *testing.T, response Response) ToolResult {
	t.Helper()
	var result ToolResult
	mustUnmarshal(t, string(response.Result), &result)
	return result
}

func mustUnmarshal(t *testing.T, value string, target any) {
	t.Helper()
	if err := json.Unmarshal([]byte(value), target); err != nil {
		t.Fatalf("unmarshal %q: %v", value, err)
	}
}

func nonemptyLines(value string) []string {
	var result []string
	for _, line := range strings.Split(value, "\n") {
		if strings.TrimSpace(line) != "" {
			result = append(result, line)
		}
	}
	return result
}

func assertStrings(t *testing.T, actual, expected []string) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("values = %q, want %q", actual, expected)
	}
	for index := range expected {
		if actual[index] != expected[index] {
			t.Fatalf("values = %q, want %q", actual, expected)
		}
	}
}

func assertIDs(t *testing.T, actual, expected []shoal.ID) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("IDs = %q, want %q", actual, expected)
	}
	for index := range expected {
		if actual[index] != expected[index] {
			t.Fatalf("IDs = %q, want %q", actual, expected)
		}
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
