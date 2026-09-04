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
