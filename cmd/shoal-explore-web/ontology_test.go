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
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestOntologyFileConfiguresWebWorkspaceEndpoint(t *testing.T) {
	data := t.TempDir()
	ontologyPath := writeWorkspaceOntologyFile(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output := &lockedBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{
			"-data", data,
			"-listen", "127.0.0.1:0",
			"-dev-auth",
			"-ontology-file", ontologyPath,
		}, output)
	}()

	baseURL := waitForListeningURL(t, output)
	getMeta(t, baseURL)
	body := getOntologyJSON(t, baseURL+"/api/v1/ontology")
	var ontology map[string]any
	if err := json.Unmarshal(body, &ontology); err != nil {
		t.Fatalf("decode ontology %s: %v", string(body), err)
	}
	if configured, _ := ontology["configured"].(bool); !configured {
		t.Fatalf("ontology endpoint was not configured by -ontology-file: %s", string(body))
	}
	if concepts, ok := ontology["concepts"].([]any); !ok || len(concepts) != 2 {
		t.Fatalf("ontology concepts were not served from -ontology-file: %s", string(body))
	}
	if relationships, ok := ontology["relationships"].([]any); !ok || len(relationships) != 1 {
		t.Fatalf("ontology relationships were not served from -ontology-file: %s", string(body))
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

func TestOntologyProposalLifecycleUsesStartedEmbeddedWorkspace(t *testing.T) {
	data := t.TempDir()
	ontologyPath := writeWorkspaceOntologyFile(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output := &lockedBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{
			"-data", data,
			"-listen", "127.0.0.1:0",
			"-dev-auth",
			"-ontology-file", ontologyPath,
		}, output)
	}()
	baseURL := waitForListeningURL(t, output)
	getMeta(t, baseURL)

	created := postOntologyProposalJSON(t, baseURL, "/api/v1/ontology/proposals", []byte(`{
  "rationale": "exercise startup proposal wiring",
  "proposed_version": {
    "version": "v2",
    "properties": [
      {"key": "name", "name": "Name", "value_type": "string"},
      {"key": "role", "name": "Role", "value_type": "string"}
    ],
    "concepts": [
      {"key": "person", "name": "Person", "properties": ["name"]},
      {"key": "organization", "name": "Organization", "properties": ["name"]}
    ],
    "relationships": [
      {
        "key": "member_of",
        "name": "Member of",
        "from_concepts": ["person"],
        "to_concepts": ["organization"],
        "properties": ["role"],
        "directed": true
      }
    ]
  }
}`))
	proposal, ok := created["proposal"].(map[string]any)
	if !ok || proposal["state"] != "draft" {
		t.Fatalf("started workspace proposal creation = %v", created)
	}
	proposalID, _ := proposal["id"].(string)
	submitted := postOntologyProposalJSON(
		t, baseURL, "/api/v1/ontology/proposals/"+proposalID+"/transition",
		[]byte(`{"state":"submitted","note":"ready for review"}`),
	)
	updated, ok := submitted["proposal"].(map[string]any)
	if !ok || updated["state"] != "submitted" {
		t.Fatalf("started workspace proposal transition = %v", submitted)
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

func writeWorkspaceOntologyFile(t *testing.T) string {
	t.Helper()
	ontologyPath := filepath.Join(t.TempDir(), "ontology.json")
	if err := os.WriteFile(ontologyPath, []byte(`{
  "schema": {"key": "workspace", "name": "Workspace"},
  "version": {"version": "v1", "created_at": "2026-09-04T00:00:00Z"},
  "properties": [
    {"key": "name", "name": "Name", "value_type": "string", "constraints": [
      {"kind": "required"}
    ]},
    {"key": "role", "name": "Role", "value_type": "string"}
  ],
  "concepts": [
    {"key": "person", "name": "Person", "properties": ["name"]},
    {"key": "organization", "name": "Organization", "properties": ["name"]}
  ],
  "relationships": [
    {
      "key": "member_of",
      "name": "Member of",
      "from_concepts": ["person"],
      "to_concepts": ["organization"],
      "properties": ["role"],
      "directed": true
    }
  ]
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	return ontologyPath
}

func getOntologyJSON(t *testing.T, url string) []byte {
	t.Helper()
	client := http.Client{Timeout: 5 * time.Second}
	response, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %s: %s", url, response.Status, data)
	}
	return data
}

func postOntologyProposalJSON(
	t *testing.T,
	baseURL string,
	path string,
	body []byte,
) map[string]any {
	t.Helper()
	client := http.Client{Timeout: 5 * time.Second}
	response, err := client.Post(
		baseURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		t.Fatalf("POST %s returned %s: %s", path, response.Status, data)
	}
	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode %s response %s: %v", path, data, err)
	}
	return decoded
}
