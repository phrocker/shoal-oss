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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
)

func TestIngestListQueryWorkflow(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "guide.md")
	if err := os.WriteFile(source, []byte(
		"# Guide\n\nUse exponential backoff for retries.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(dir, "data")
	var output bytes.Buffer
	if err := run(context.Background(), []string{
		"ingest", "-data", data, "-file", source,
	}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"Disposition": "applied"`) {
		t.Fatalf("ingest output = %s", output.String())
	}
	output.Reset()
	if err := run(context.Background(), []string{
		"query", "-data", data, "-text", "exponential backoff",
	}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Use exponential backoff for retries.") {
		t.Fatalf("query output = %s", output.String())
	}
}

func TestDocumentedDemoWorkflow(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	data := filepath.Join(dir, "explorer-demo")
	guideFile := filepath.Join("..", "..", "docs", "explorer-demo-guide.md")
	releaseFile := filepath.Join("..", "..", "docs", "explorer-demo-release.md")

	var output bytes.Buffer
	if err := run(ctx, []string{
		"ingest", "-data", data, "-file", guideFile,
	}, &output); err != nil {
		t.Fatal(err)
	}
	guideIngest := decodeJSON[explorer.IngestResult](t, output.Bytes())
	if guideIngest.Disposition != explorer.IngestApplied ||
		guideIngest.Document.Title != "Shoal Explorer Demo Guide" ||
		guideIngest.SectionCount != 5 ||
		guideIngest.SpanCount != 4 {
		t.Fatalf("guide ingest = %+v", guideIngest)
	}

	output.Reset()
	if err := run(ctx, []string{
		"ingest", "-data", data, "-file", releaseFile,
	}, &output); err != nil {
		t.Fatal(err)
	}
	releaseIngest := decodeJSON[explorer.IngestResult](t, output.Bytes())
	if releaseIngest.Disposition != explorer.IngestApplied ||
		releaseIngest.Document.Title != "Explorer Release Checklist" ||
		releaseIngest.SectionCount != 3 ||
		releaseIngest.SpanCount != 2 {
		t.Fatalf("release ingest = %+v", releaseIngest)
	}

	output.Reset()
	if err := run(ctx, []string{
		"ingest", "-data", data, "-file", guideFile,
	}, &output); err != nil {
		t.Fatal(err)
	}
	reingest := decodeJSON[explorer.IngestResult](t, output.Bytes())
	if reingest.Disposition != explorer.IngestUnchanged ||
		reingest.Document.ID != guideIngest.Document.ID ||
		reingest.Revision.ID != guideIngest.Revision.ID {
		t.Fatalf("idempotent guide ingest = %+v", reingest)
	}

	output.Reset()
	if err := run(ctx, []string{"list", "-data", data}, &output); err != nil {
		t.Fatal(err)
	}
	documents := decodeJSON[[]explorer.DocumentSummary](t, output.Bytes())
	if len(documents) != 2 ||
		documents[0].Document.Title != "Explorer Release Checklist" ||
		documents[1].Document.Title != "Shoal Explorer Demo Guide" {
		t.Fatalf("documents = %+v", documents)
	}

	output.Reset()
	if err := run(ctx, []string{
		"outline", "-data", data, "-document", string(guideIngest.Document.ID),
	}, &output); err != nil {
		t.Fatal(err)
	}
	view := decodeJSON[explorer.DocumentView](t, output.Bytes())
	if view.Document.ID != guideIngest.Document.ID ||
		view.Root.Section.Heading != "Shoal Explorer Demo Guide" {
		t.Fatalf("guide outline root = %+v", view.Root.Section)
	}
	requireHeadings(t, view.Root,
		"Shoal Explorer Demo Guide",
		"Ingested knowledge",
		"Promotion gate",
		"Relationship target",
	)

	output.Reset()
	if err := run(ctx, []string{
		"query", "-data", data,
		"-text", "canary validation exact citation",
		"-top", "2", "-modes", "lexical,tree,graph", "-explain=true",
	}, &output); err != nil {
		t.Fatal(err)
	}
	response := decodeJSON[retrieval.Response](t, output.Bytes())
	if len(response.Results) == 0 {
		t.Fatalf("query returned no results: %+v", response)
	}
	guideContent, err := os.ReadFile(guideFile)
	if err != nil {
		t.Fatal(err)
	}
	expectedQuote := extractDemoQuote(t, string(guideContent))
	first := response.Results[0]
	if len(first.Evidence) != 1 || first.Evidence[0].Quote != expectedQuote {
		t.Fatalf("evidence = %+v", first.Evidence)
	}
	citation := first.Evidence[0].Citation
	if citation.DocumentID != guideIngest.Document.ID ||
		citation.RevisionID != guideIngest.Revision.ID ||
		citation.SectionID == "" ||
		citation.SpanID == "" {
		t.Fatalf("citation = %+v", citation)
	}
	if got := string(guideContent[citation.Range.Start.Offset:citation.Range.End.Offset]); got != expectedQuote {
		t.Fatalf("citation slice = %q, want %q", got, expectedQuote)
	}
	if first.Explanation == nil {
		t.Fatalf("missing explanation: %+v", first)
	}
	for _, mode := range []retrieval.Mode{
		retrieval.ModeLexical, retrieval.ModeTree, retrieval.ModeGraph,
	} {
		if first.Explanation.Scores[string(mode)] <= 0 {
			t.Fatalf("missing %s score in %+v", mode, first.Explanation)
		}
	}

	output.Reset()
	if err := run(ctx, []string{
		"connect", "-data", data,
		"-id", "demo-edge-guide-supports-release",
		"-from", string(guideIngest.Document.ID),
		"-to", string(releaseIngest.Document.ID),
		"-type", "supports",
	}, &output); err != nil {
		t.Fatal(err)
	}
	edge := decodeJSON[graph.Edge](t, output.Bytes())
	if edge.ID != "demo-edge-guide-supports-release" ||
		edge.From != guideIngest.Document.ID ||
		edge.To != releaseIngest.Document.ID ||
		edge.Type != "supports" ||
		edge.Weight != 1 {
		t.Fatalf("edge = %+v", edge)
	}

	output.Reset()
	if err := run(ctx, []string{
		"neighbors", "-data", data,
		"-node", string(guideIngest.Document.ID),
		"-depth", "1", "-edge-types", "supports",
	}, &output); err != nil {
		t.Fatal(err)
	}
	neighborhood := decodeJSON[explorer.Neighborhood](t, output.Bytes())
	if len(neighborhood.Nodes) != 2 ||
		len(neighborhood.Edges) != 1 ||
		neighborhood.Edges[0].ID != edge.ID {
		t.Fatalf("neighborhood = %+v", neighborhood)
	}
	titles := map[string]bool{}
	for _, node := range neighborhood.Nodes {
		titles[node.Properties["title"]] = true
	}
	if !titles["Shoal Explorer Demo Guide"] ||
		!titles["Explorer Release Checklist"] {
		t.Fatalf("neighborhood titles = %+v", titles)
	}
}

func decodeJSON[T any](t *testing.T, data []byte) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decode %T from %s: %v", value, string(data), err)
	}
	return value
}

func requireHeadings(t *testing.T, root explorer.SectionView, headings ...string) {
	t.Helper()
	found := make(map[string]bool)
	var visit func(explorer.SectionView)
	visit = func(section explorer.SectionView) {
		found[section.Section.Heading] = true
		for _, child := range section.Children {
			visit(child)
		}
	}
	visit(root)
	for _, heading := range headings {
		if !found[heading] {
			t.Fatalf("missing heading %q in %+v", heading, root)
		}
	}
}

func extractDemoQuote(t *testing.T, source string) string {
	t.Helper()
	start := strings.Index(source, "Run the canary validation")
	if start < 0 {
		t.Fatal("demo quote start not found")
	}
	end := strings.Index(source[start:], "\n### Relationship target")
	if end < 0 {
		t.Fatal("demo quote end not found")
	}
	return strings.TrimRight(source[start:start+end], "\r\n")
}
