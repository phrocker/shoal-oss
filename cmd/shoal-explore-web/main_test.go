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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
)

func TestDocumentedWebWorkspaceStartServesMeta(t *testing.T) {
	data := t.TempDir()
	corpus, err := explorer.Open(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := corpus.Ingest(context.Background(), explorer.Source{
		URI: "file:///web-demo.md", MediaType: explorer.MediaTypeMarkdown,
		Content: "# Web Demo\n\nThe web workspace serves Explorer metadata.\n",
	}); err != nil {
		t.Fatal(err)
	}
	if err := corpus.Close(); err != nil {
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
		}, output)
	}()

	baseURL := waitForListeningURL(t, output)
	body := getMeta(t, baseURL)
	var meta map[string]any
	if err := json.Unmarshal(body, &meta); err != nil {
		t.Fatalf("decode meta %s: %v", string(body), err)
	}
	for _, field := range []string{
		"max_page_size", "max_top_k", "max_depth", "max_fanout", "max_nodes",
	} {
		if _, ok := meta[field]; !ok {
			t.Fatalf("meta missing %q: %s", field, string(body))
		}
	}
	body = postJSON(t, baseURL+"/api/v1/documents", `{"page":{"limit":2}}`)
	var documents map[string]any
	if err := json.Unmarshal(body, &documents); err != nil {
		t.Fatalf("decode documents %s: %v", string(body), err)
	}
	snapshot, ok := documents["snapshot"].(map[string]any)
	if !ok {
		t.Fatalf("documents missing snapshot: %s", string(body))
	}
	for _, field := range []string{"id", "as_of", "frontier"} {
		if _, ok := snapshot[field]; !ok {
			t.Fatalf("snapshot missing %q: %s", field, string(body))
		}
	}
	if got, ok := documents["documents"].([]any); !ok || len(got) != 1 {
		t.Fatalf("documents response = %s", string(body))
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

func TestDefaultListenAddressIsDocumented(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "..", "docs", "explorer-demo-walkthrough.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(source, []byte("-listen 127.0.0.1:8080")) ||
		!bytes.Contains(source, []byte("The default listen address is also `127.0.0.1:8080`")) {
		t.Fatalf("walkthrough does not document the default listen address")
	}
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func waitForListeningURL(t *testing.T, output *lockedBuffer) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		line := output.String()
		if index := strings.Index(line, "http://"); index >= 0 {
			return strings.TrimSpace(line[index:])
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server did not report listen address: %q", output.String())
	return ""
}

func getMeta(t *testing.T, baseURL string) []byte {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	client := http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		response, err := client.Get(baseURL + "/api/v1/meta")
		if err == nil {
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if response.StatusCode == http.StatusOK && readErr == nil && closeErr == nil {
				return body
			}
			lastErr = readErr
			if closeErr != nil {
				lastErr = closeErr
			}
			if response.StatusCode != http.StatusOK {
				lastErr = errStatus(response.Status)
			}
		} else {
			lastErr = err
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("GET /api/v1/meta failed: %v", lastErr)
	return nil
}

func postJSON(t *testing.T, url string, body string) []byte {
	t.Helper()
	client := http.Client{Timeout: 5 * time.Second}
	response, err := client.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("POST %s returned %s: %s", url, response.Status, string(data))
	}
	return data
}

type errStatus string

func (e errStatus) Error() string { return string(e) }
