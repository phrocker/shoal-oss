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

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/mcp"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestParseCommandConfigFailsClosedAndSupportsEnvironment(t *testing.T) {
	emptyEnvironment := func(string) string { return "" }
	if _, err := parseCommandConfig(nil, io.Discard, emptyEnvironment); err == nil ||
		!strings.Contains(err.Error(), "identity subject") {
		t.Fatalf("missing identity error = %v", err)
	}
	if _, err := parseCommandConfig(
		[]string{"-dev-auth", "-identity-subject", "mixed"},
		io.Discard,
		emptyEnvironment,
	); err == nil || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("mixed identity error = %v", err)
	}
	if _, err := parseCommandConfig(
		[]string{
			"-identity-subject", "subject",
			"-identity-actor", "actor",
			"-identity-domain", "domain",
			"-identity-source", "source",
			"-identity-policy", "policy",
			"-identity-operations", "read,unknown",
		},
		io.Discard,
		emptyEnvironment,
	); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("invalid operation error = %v", err)
	}
	if _, _, err := resolveStatePaths(
		"", filepath.Join("state", "corpus"), filepath.Join("state", "corpus", "policy"),
	); err == nil || !strings.Contains(err.Error(), "separate siblings") {
		t.Fatalf("nested state directory error = %v", err)
	}

	root := t.TempDir()
	environment := map[string]string{
		"SHOAL_MCP_STATE_DIR":              root,
		"SHOAL_MCP_IDENTITY_SUBJECT":       "configured-subject",
		"SHOAL_MCP_IDENTITY_ACTOR":         "configured-actor",
		"SHOAL_MCP_IDENTITY_DOMAIN":        "configured-domain",
		"SHOAL_MCP_IDENTITY_SOURCE":        "configured-source",
		"SHOAL_MCP_IDENTITY_POLICY":        "configured-policy",
		"SHOAL_MCP_IDENTITY_OPERATIONS":    "list,read",
		"SHOAL_MCP_IDENTITY_GENERATION":    "2",
		"SHOAL_MCP_IDENTITY_LIFETIME":      "10m",
		"SHOAL_MCP_IDENTITY_AUDIT_PURPOSE": "configured stdio identity",
	}
	config, err := parseCommandConfig(
		nil, io.Discard, func(name string) string { return environment[name] })
	if err != nil {
		t.Fatal(err)
	}
	if config.corpusDir != filepath.Join(root, "corpus") ||
		config.policyDir != filepath.Join(root, "policy") {
		t.Fatalf("state paths = %q, %q", config.corpusDir, config.policyDir)
	}
	if config.identity.subject != "configured-subject" ||
		config.identity.policyGeneration != 2 ||
		config.identity.lifetime != 10*time.Minute {
		t.Fatalf("identity config = %+v", config.identity)
	}

	invalidBool := func(name string) string {
		if name == "SHOAL_MCP_DEV_AUTH" {
			return "sometimes"
		}
		return ""
	}
	if _, err := parseCommandConfig(nil, io.Discard, invalidBool); err == nil ||
		!strings.Contains(err.Error(), "must be a boolean") {
		t.Fatalf("invalid environment boolean error = %v", err)
	}
}

func TestStdioSmokeUsesEmbeddedExplorerAndKeepsStdoutPure(t *testing.T) {
	stateDir := t.TempDir()
	content := base64.StdEncoding.EncodeToString(
		[]byte("# MCP smoke\n\nThe embedded MCP workspace is available.\n"))
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"` +
			mcp.ProtocolVersion +
			`","capabilities":{},"clientInfo":{"name":"smoke","version":"1"}}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"shoal.ingest","arguments":{"files":[{"name":"smoke.md","content":"` +
			content + `"}]}}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"shoal.documents","arguments":{}}}`,
	}, "\n") + "\n"
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWith(
		context.Background(),
		[]string{"-state-dir", stateDir, "-dev-auth"},
		strings.NewReader(input),
		&stdout,
		&stderr,
		runtimeDependencies{
			getenv: func(string) string { return "" },
			build:  buildApplication,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q", stderr.String())
	}
	lines := nonemptyLines(stdout.String())
	if len(lines) != 4 {
		t.Fatalf("stdout responses = %d, want 4:\n%s", len(lines), stdout.String())
	}
	responses := make([]mcp.Response, len(lines))
	for index, line := range lines {
		if err := json.Unmarshal([]byte(line), &responses[index]); err != nil {
			t.Fatalf("stdout line %d is not JSON-RPC: %q: %v", index, line, err)
		}
		if responses[index].JSONRPC != "2.0" {
			t.Fatalf("stdout line %d JSON-RPC version = %q", index, responses[index].JSONRPC)
		}
		if responses[index].Error != nil {
			t.Fatalf("stdout line %d protocol error = %+v", index, responses[index].Error)
		}
	}

	var initialized mcp.InitializeResult
	mustDecode(t, responses[0].Result, &initialized)
	for _, text := range []string{
		"launcher-configured identity",
		"future HTTP transport",
		"recording is not implemented",
	} {
		if !strings.Contains(initialized.Instructions, text) {
			t.Fatalf("instructions missing %q: %s", text, initialized.Instructions)
		}
	}

	var listed mcp.ListToolsResult
	mustDecode(t, responses[1].Result, &listed)
	names := make(map[string]bool, len(listed.Tools))
	for _, tool := range listed.Tools {
		names[tool.Name] = true
	}
	for _, name := range []string{
		mcp.ToolDocuments,
		mcp.ToolDocument,
		mcp.ToolRetrieve,
		mcp.ToolNeighborhood,
		mcp.ToolPath,
		mcp.ToolIngest,
		mcp.ToolExtract,
		mcp.ToolRecompute,
		mcp.ToolChanges,
	} {
		if !names[name] {
			t.Fatalf("implemented tool %q was not advertised: %v", name, names)
		}
	}

	var ingested mcp.ToolResult
	mustDecode(t, responses[2].Result, &ingested)
	if ingested.IsError {
		t.Fatalf("ingest failed: %s", ingested.StructuredContent)
	}
	var documentsResult mcp.ToolResult
	mustDecode(t, responses[3].Result, &documentsResult)
	if documentsResult.IsError {
		t.Fatalf("documents failed: %s", documentsResult.StructuredContent)
	}
	var documents webapi.DocumentsResponse
	mustDecode(t, documentsResult.StructuredContent, &documents)
	if len(documents.Documents) != 1 ||
		documents.Documents[0].Document.Title != "smoke.md" {
		t.Fatalf("documents = %+v", documents.Documents)
	}
}

func TestProcessIdentityIsBoundFreshForEveryToolCall(t *testing.T) {
	authority := auth.NewAuthority()
	identityConfig, err := configureIdentity(identityOptions{
		development:      true,
		policyGeneration: 1,
		lifetime:         time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := newProcessIdentity(identityConfig, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	service := &requestIDService{resolver: authority.Resolver()}
	server, err := mcp.NewServer(mcp.Config{
		Service: service, Authority: authority, Decisions: identity,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"` +
			mcp.ProtocolVersion +
			`","capabilities":{},"clientInfo":{"name":"identity-test","version":"1"}}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"shoal.documents","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"shoal.documents","arguments":{}}}`,
	}, "\n") + "\n"
	var output bytes.Buffer
	if err := server.Serve(
		context.Background(), strings.NewReader(input), &output); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if len(service.requestIDs) != 2 {
		t.Fatalf("request IDs = %v", service.requestIDs)
	}
	if service.requestIDs[0] == service.requestIDs[1] {
		t.Fatalf("tool calls shared request ID %q", service.requestIDs[0])
	}
	for _, requestID := range service.requestIDs {
		if !strings.HasPrefix(string(requestID), "mcp-") ||
			requestID == processTemplateRequestID {
			t.Fatalf("tool call request ID = %q", requestID)
		}
	}
}

func TestUnregisteredExistingCorpusIsRefused(t *testing.T) {
	root := t.TempDir()
	corpusDir := filepath.Join(root, "corpus")
	corpus, err := explorer.Open(corpusDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := corpus.Ingest(context.Background(), explorer.Source{
		URI:       "file:///unregistered.md",
		MediaType: explorer.MediaTypeMarkdown,
		Content:   "# Unregistered\n",
	}); err != nil {
		t.Fatal(err)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}
	identity, err := configureIdentity(identityOptions{
		development:      true,
		policyGeneration: 1,
		lifetime:         time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, closeWorkspace, err := openWorkspace(
		context.Background(),
		commandConfig{
			corpusDir: corpusDir,
			policyDir: filepath.Join(root, "policy"),
			identity:  identity,
		},
		auth.NewAuthority(),
		time.Now,
	)
	if err == nil {
		_ = closeWorkspace()
		t.Fatal("unregistered corpus was served")
	}
	if !strings.Contains(err.Error(), "no authorization registrations") {
		t.Fatalf("refusal error = %v", err)
	}
}

func TestApplicationRejectsTypedNilAndPropagatesServeAndCloseErrors(t *testing.T) {
	var typedNil *fakeServer
	if _, err := newApplication(typedNil, func() error { return nil }); err == nil {
		t.Fatal("typed-nil server was accepted")
	}
	serveFailure := errors.New("serve failed")
	closeFailure := errors.New("close failed")
	var closed atomic.Int32
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWith(
		context.Background(),
		[]string{"-dev-auth", "-state-dir", t.TempDir()},
		strings.NewReader(""),
		&stdout,
		&stderr,
		runtimeDependencies{
			getenv: func(string) string { return "" },
			build: func(
				context.Context,
				commandConfig,
			) (*application, error) {
				return newApplication(
					&fakeServer{err: serveFailure},
					func() error {
						closed.Add(1)
						return closeFailure
					},
				)
			},
		},
	)
	if !errors.Is(err, serveFailure) || !errors.Is(err, closeFailure) {
		t.Fatalf("run error = %v", err)
	}
	if closed.Load() != 1 {
		t.Fatalf("close calls = %d", closed.Load())
	}
}

func TestCancellationClosesStdinAndWorkspaceGracefully(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer := io.Pipe()
	defer writer.Close()
	stateDir := t.TempDir()
	started := make(chan struct{})
	var closed atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- runWith(
			ctx,
			[]string{"-dev-auth", "-state-dir", stateDir},
			reader,
			io.Discard,
			io.Discard,
			runtimeDependencies{
				getenv: func(string) string { return "" },
				build: func(
					context.Context,
					commandConfig,
				) (*application, error) {
					return newApplication(
						&blockingServer{started: started},
						func() error {
							closed.Add(1)
							return nil
						},
					)
				},
			},
		)
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("canceled run error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled run did not return")
	}
	if closed.Load() != 1 {
		t.Fatalf("close calls = %d", closed.Load())
	}
}

type requestIDService struct {
	resolver   auth.Resolver
	mu         sync.Mutex
	requestIDs []shoal.ID
}

func (s *requestIDService) Documents(
	ctx context.Context,
	_ webapi.DocumentsRequest,
) (webapi.DocumentsResponse, error) {
	decision, err := s.resolver.Resolve(ctx)
	if err != nil {
		return webapi.DocumentsResponse{}, err
	}
	s.mu.Lock()
	s.requestIDs = append(s.requestIDs, decision.RequestID())
	s.mu.Unlock()
	return webapi.DocumentsResponse{}, nil
}

func (*requestIDService) Document(
	context.Context,
	webapi.DocumentRequest,
) (webapi.DocumentResponse, error) {
	return webapi.DocumentResponse{}, nil
}

func (*requestIDService) Retrieve(
	context.Context,
	webapi.RetrievalRequest,
) (webapi.RetrievalResponse, error) {
	return webapi.RetrievalResponse{}, nil
}

func (*requestIDService) Neighborhood(
	context.Context,
	webapi.NeighborhoodRequest,
) (webapi.NeighborhoodResponse, error) {
	return webapi.NeighborhoodResponse{}, nil
}

func (*requestIDService) Path(
	context.Context,
	webapi.PathRequest,
) (webapi.PathResponse, error) {
	return webapi.PathResponse{}, nil
}

type fakeServer struct {
	err error
}

func (s *fakeServer) Serve(context.Context, io.Reader, io.Writer) error {
	return s.err
}

type blockingServer struct {
	started chan<- struct{}
}

func (s *blockingServer) Serve(
	_ context.Context,
	input io.Reader,
	_ io.Writer,
) error {
	close(s.started)
	var one [1]byte
	_, err := input.Read(one[:])
	return err
}

func nonemptyLines(value string) []string {
	raw := strings.Split(value, "\n")
	lines := make([]string, 0, len(raw))
	for _, line := range raw {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func mustDecode(t *testing.T, raw []byte, value any) {
	t.Helper()
	if err := json.Unmarshal(raw, value); err != nil {
		t.Fatalf("decode %s: %v", string(raw), err)
	}
}
