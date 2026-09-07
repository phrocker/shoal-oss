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
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestBuildScenarioIsDeterministicAndComplete(t *testing.T) {
	value := validConfig("https://shoal.example.test")
	if err := value.validate(); err != nil {
		t.Fatal(err)
	}
	first, err := buildScenario(value)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildScenario(value)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("scenario generation is not deterministic")
	}
	if len(first.Plans) != 2 {
		t.Fatalf("plans = %d", len(first.Plans))
	}
	kinds := make(map[string]int)
	workItems := 0
	activities := make(map[string]struct{})
	actors := make(map[string]struct{})
	for _, plan := range first.Plans {
		for _, file := range plan.Files {
			var envelope struct {
				Schema  string            `json:"schema"`
				Kind    string            `json:"kind"`
				Records []json.RawMessage `json:"records"`
			}
			if err := json.Unmarshal(file.Content, &envelope); err != nil {
				t.Fatalf("decode %s: %v", file.Name, err)
			}
			if envelope.Schema != scenarioSchema {
				t.Fatalf("%s schema = %q", file.Name, envelope.Schema)
			}
			kinds[envelope.Kind] += len(envelope.Records)
			switch envelope.Kind {
			case "work_item":
				workItems += len(envelope.Records)
			case "activity":
				for _, raw := range envelope.Records {
					var activity struct {
						OccurredAt string `json:"occurred_at"`
					}
					if err := json.Unmarshal(raw, &activity); err != nil {
						t.Fatal(err)
					}
					activities[activity.OccurredAt[:10]] = struct{}{}
					var actor struct {
						ActorID string `json:"actor_id"`
					}
					if err := json.Unmarshal(raw, &actor); err != nil {
						t.Fatal(err)
					}
					actors[actor.ActorID] = struct{}{}
				}
			}
		}
	}
	if kinds["team"] != 1 || kinds["person"] != 2 ||
		kinds["simulated_agent"] != 2 {
		t.Fatalf("fixture kinds = %#v", kinds)
	}
	if workItems != workItemCount {
		t.Fatalf("work items = %d", workItems)
	}
	if len(activities) != activityDays {
		t.Fatalf("activity dates = %d", len(activities))
	}
	for _, actor := range []string{
		"person-a", "person-b", "agent-planning", "agent-delivery",
	} {
		if _, ok := actors[actor]; !ok {
			t.Fatalf("activity does not include actor %q: %#v", actor, actors)
		}
	}
}

func TestProvisionIsIdempotentAndUsesDistinctOwners(t *testing.T) {
	state := newProvisionServerState()
	server := httptest.NewServer(http.HandlerFunc(state.serveHTTP))
	defer server.Close()
	value := validConfig(server.URL)
	if err := value.validate(); err != nil {
		t.Fatal(err)
	}
	generated, err := buildScenario(value)
	if err != nil {
		t.Fatal(err)
	}
	tokens := map[string]string{
		"SHOAL_DEMO_TOKEN_A": "token-a",
		"SHOAL_DEMO_TOKEN_B": "token-b",
	}
	provision := provisioner{
		client: server.Client(),
		getenv: func(name string) string { return tokens[name] },
	}
	first, err := provision.provision(context.Background(), value, generated)
	if err != nil {
		t.Fatal(err)
	}
	second, err := provision.provision(context.Background(), value, generated)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.UserSummaries) != 2 || len(second.UserSummaries) != 2 {
		t.Fatalf("summaries = %#v %#v", first, second)
	}
	for _, user := range first.UserSummaries {
		for name, disposition := range user.Files {
			if disposition != "applied" {
				t.Fatalf("first %s disposition = %q", name, disposition)
			}
		}
	}
	for _, user := range second.UserSummaries {
		for name, disposition := range user.Files {
			if disposition != "unchanged" {
				t.Fatalf("second %s disposition = %q", name, disposition)
			}
		}
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.settings) != 2 {
		t.Fatalf("workspace settings = %d", len(state.settings))
	}
	if len(state.files) != 9 {
		t.Fatalf("fixture files = %d", len(state.files))
	}
	if state.identityCalls["token-a"] != 2 ||
		state.identityCalls["token-b"] != 2 {
		t.Fatalf("identity calls = %#v", state.identityCalls)
	}
	for token, workspaces := range state.tokenWorkspaces {
		if len(workspaces) != 1 {
			t.Fatalf("%s used workspaces %#v", token, workspaces)
		}
	}
	if reflect.DeepEqual(
		state.tokenWorkspaces["token-a"], state.tokenWorkspaces["token-b"]) {
		t.Fatal("two users used the same workspace header")
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*config)
		want   string
	}{
		{
			name: "public HTTP",
			mutate: func(value *config) {
				value.BaseURL = "http://shoal.example.test"
			},
			want: "plain HTTP",
		},
		{
			name: "one user",
			mutate: func(value *config) {
				value.Users = value.Users[:1]
			},
			want: "exactly two",
		},
		{
			name: "duplicate subjects",
			mutate: func(value *config) {
				value.Users[1].OIDCSubject = value.Users[0].OIDCSubject
			},
			want: "distinct oidc_subject_id",
		},
		{
			name: "duplicate workspaces",
			mutate: func(value *config) {
				value.Users[1].WorkspaceID = value.Users[0].WorkspaceID
			},
			want: "distinct workspace_id",
		},
		{
			name: "bad token environment",
			mutate: func(value *config) {
				value.Users[0].TokenEnv = "token-a"
			},
			want: "uppercase environment variable",
		},
		{
			name: "non UTC start",
			mutate: func(value *config) {
				value.ScenarioStart = "2026-09-01T09:00:00-04:00"
			},
			want: "UTC timestamp",
		},
		{
			name: "budget exceeds bound",
			mutate: func(value *config) {
				value.Workspace.GraphDepth = 5
			},
			want: "graph_depth",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := validConfig("https://shoal.example.test")
			test.mutate(&value)
			err := value.validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadConfigRejectsUnknownAndTrailingValues(t *testing.T) {
	value := validConfig("https://shoal.example.test")
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string][]byte{
		"unknown": bytes.Replace(
			encoded, []byte(`"base_url"`),
			[]byte(`"unknown":true,"base_url"`), 1),
		"trailing": append(append([]byte(nil), encoded...), []byte(` {}`)...),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, content, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadConfig(path); err == nil {
				t.Fatal("invalid config was accepted")
			}
		})
	}
}

func TestCheckedInExamplesAreValidAndContainNoCredential(t *testing.T) {
	configPath := filepath.Join(
		"..", "..", "deploy", "shoal-explore-web",
		"demo-scenario.json.example",
	)
	if _, err := loadConfig(configPath); err != nil {
		t.Fatalf("example scenario config: %v", err)
	}
	mcpPath := filepath.Join("..", "..", ".vscode", "mcp.json.example")
	content, err := os.ReadFile(mcpPath)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(content, &document); err != nil {
		t.Fatalf("example MCP config is not JSON: %v", err)
	}
	text := string(content)
	for _, want := range []string{
		"${env:SHOAL_MCP_URL}",
		"Bearer ${env:SHOAL_MCP_BEARER_TOKEN}",
		"${env:SHOAL_MCP_WORKSPACE_ID}",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("example MCP config missing %q", want)
		}
	}
}

func validConfig(baseURL string) config {
	return config{
		BaseURL:       baseURL,
		OIDCIssuer:    "https://identity.example.test",
		ScenarioStart: "2026-08-24T00:00:00Z",
		Team: teamConfig{
			ID: "demo-team", Name: "Example Delivery Team",
		},
		Workspace: workspaceConfig{
			RetrievalTopK: 20, GraphDepth: 3, GraphFanout: 20,
			GraphNodes: 100, OutputBytes: 1048576,
		},
		Users: []userConfig{
			{
				PersonID: "person-a", DisplayName: "Alex Morgan",
				TeamRole: "maintainer", OIDCSubject: "subject-a",
				WorkspaceID: "demo-workspace-a", TokenEnv: "SHOAL_DEMO_TOKEN_A",
				Agent: agentConfig{
					ID: "agent-planning", Name: "Planning Agent",
					Capability: "planning",
					Skills:     []string{"triage", "dependency-analysis"},
				},
			},
			{
				PersonID: "person-b", DisplayName: "Sam Rivera",
				TeamRole: "developer", OIDCSubject: "subject-b",
				WorkspaceID: "demo-workspace-b", TokenEnv: "SHOAL_DEMO_TOKEN_B",
				Agent: agentConfig{
					ID: "agent-delivery", Name: "Delivery Agent",
					Capability: "delivery",
					Skills:     []string{"validation", "release-readiness"},
				},
			},
		},
	}
}

type provisionServerState struct {
	mu              sync.Mutex
	settings        map[string][]byte
	files           map[string][]byte
	identityCalls   map[string]int
	tokenWorkspaces map[string]map[string]struct{}
}

func newProvisionServerState() *provisionServerState {
	return &provisionServerState{
		settings: make(map[string][]byte), files: make(map[string][]byte),
		identityCalls: make(map[string]int),
		tokenWorkspaces: map[string]map[string]struct{}{
			"token-a": {}, "token-b": {},
		},
	}
}

func (s *provisionServerState) serveHTTP(
	writer http.ResponseWriter,
	request *http.Request,
) {
	token := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
	switch {
	case request.Method == http.MethodGet &&
		request.URL.Path == "/api/v1/identity":
		s.identity(writer, token)
	case request.Method == http.MethodPut &&
		strings.HasPrefix(request.URL.Path, "/api/v1/workspaces/"):
		s.workspace(writer, request, token)
	case request.Method == http.MethodPost &&
		request.URL.Path == "/api/v1/ingest":
		s.ingest(writer, request, token)
	default:
		http.NotFound(writer, request)
	}
}

func (s *provisionServerState) identity(
	writer http.ResponseWriter,
	token string,
) {
	subjects := map[string]string{
		"token-a": "oidc:https://identity.example.test#subject-a",
		"token-b": "oidc:https://identity.example.test#subject-b",
	}
	subject, ok := subjects[token]
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	s.mu.Lock()
	s.identityCalls[token]++
	s.mu.Unlock()
	writeTestJSON(writer, http.StatusOK, map[string]any{
		"authenticated": true, "subject": subject,
		"operations": []string{
			"ingest", "workspace_settings_read", "workspace_settings_write",
		},
	})
}

func (s *provisionServerState) workspace(
	writer http.ResponseWriter,
	request *http.Request,
	token string,
) {
	if token != "token-a" && token != "token-b" {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	key := request.URL.Path
	s.mu.Lock()
	existing, found := s.settings[key]
	if found && !bytes.Equal(existing, body) {
		s.mu.Unlock()
		http.Error(writer, "divergent replay", http.StatusConflict)
		return
	}
	s.settings[key] = append([]byte(nil), body...)
	s.mu.Unlock()
	status := http.StatusCreated
	if found {
		status = http.StatusOK
	}
	writeTestJSON(writer, status, map[string]any{"revision": 1})
}

func (s *provisionServerState) ingest(
	writer http.ResponseWriter,
	request *http.Request,
	token string,
) {
	workspaceID := request.Header.Get("Shoal-Workspace-ID")
	if token == "" || workspaceID == "" ||
		request.Header.Get("X-Shoal-Workspace-Request") != "1" {
		http.Error(writer, "missing headers", http.StatusUnauthorized)
		return
	}
	reader, err := request.MultipartReader()
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	type result struct {
		Name        string `json:"name"`
		Disposition string `json:"disposition"`
	}
	results := []result{}
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		content, err := io.ReadAll(part)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		name := part.FileName()
		key := token + "/" + name
		s.mu.Lock()
		existing, found := s.files[key]
		if found && !bytes.Equal(existing, content) {
			s.mu.Unlock()
			http.Error(writer, "divergent upload", http.StatusConflict)
			return
		}
		s.files[key] = append([]byte(nil), content...)
		s.tokenWorkspaces[token][workspaceID] = struct{}{}
		s.mu.Unlock()
		disposition := "applied"
		if found {
			disposition = "unchanged"
		}
		results = append(results, result{
			Name: name, Disposition: disposition,
		})
	}
	writeTestJSON(writer, http.StatusOK, map[string]any{"files": results})
}

func writeTestJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func TestWorkspaceHeaderEncoding(t *testing.T) {
	value := validConfig("https://shoal.example.test")
	if err := value.validate(); err != nil {
		t.Fatal(err)
	}
	want := base64.RawURLEncoding.EncodeToString([]byte("demo-workspace-a"))
	if value.Users[0].workspaceKey != want {
		t.Fatalf("workspace key = %q, want %q", value.Users[0].workspaceKey, want)
	}
}

func TestIngestMultipartStaysWithinPublicFileBound(t *testing.T) {
	value := validConfig("https://shoal.example.test")
	if err := value.validate(); err != nil {
		t.Fatal(err)
	}
	generated, err := buildScenario(value)
	if err != nil {
		t.Fatal(err)
	}
	for index, plan := range generated.Plans {
		if len(plan.Files) > 8 {
			t.Fatalf("plan %d contains %d files", index, len(plan.Files))
		}
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		for _, file := range plan.Files {
			part, err := writer.CreateFormFile("files", file.Name)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fmt.Fprint(part, string(file.Content)); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
