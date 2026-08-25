/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package explorer_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const sampleMarkdown = `# Operations Guide

Use bounded retries for transient failures.

## Retry policy

Retry requests three times with exponential backoff.

### Timeouts

Each attempt has a five second timeout.
`

func TestIngestExploreRetrievePersists(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	corpus, err := explorer.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	source := explorer.Source{
		URI:       "file:///operations.md",
		MediaType: explorer.MediaTypeMarkdown,
		Content:   sampleMarkdown,
	}
	first, err := corpus.Ingest(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if first.Disposition != explorer.IngestApplied {
		t.Fatalf("first disposition = %q", first.Disposition)
	}
	second, err := corpus.Ingest(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if second.Disposition != explorer.IngestUnchanged ||
		second.Revision.ID != first.Revision.ID {
		t.Fatalf("idempotent ingest = %+v", second)
	}

	view, err := corpus.Document(ctx, first.Document.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	if view.Root.Section.Heading != "Operations Guide" ||
		len(view.Root.Children) != 1 ||
		view.Root.Children[0].Section.Heading != "Operations Guide" ||
		len(view.Root.Children[0].Children) != 1 ||
		view.Root.Children[0].Children[0].Section.Heading != "Retry policy" {
		t.Fatalf("unexpected outline: %+v", view.Root)
	}

	response, err := corpus.Retrieve(ctx, retrieval.Request{
		Text: "exponential backoff",
		TopK: 3,
		Modes: []retrieval.Mode{
			retrieval.ModeLexical, retrieval.ModeTree, retrieval.ModeGraph,
		},
		Explain: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 {
		t.Fatalf("results = %+v", response.Results)
	}
	result := response.Results[0]
	if len(result.Evidence) != 1 ||
		result.Evidence[0].Quote != "Retry requests three times with exponential backoff." {
		t.Fatalf("evidence = %+v", result.Evidence)
	}
	citation := result.Evidence[0].Citation
	if err := citation.Validate(); err != nil {
		t.Fatalf("citation: %v", err)
	}
	start, end := citation.Range.Start.Offset, citation.Range.End.Offset
	if got := sampleMarkdown[int(start):int(end)]; got != result.Evidence[0].Quote {
		t.Fatalf("citation slice = %q, quote = %q", got, result.Evidence[0].Quote)
	}
	if err := result.Evidence[0].Path.Validate(); err != nil {
		t.Fatalf("path: %v", err)
	}
	if result.Explanation == nil ||
		result.Explanation.Scores[string(retrieval.ModeGraph)] == 0 {
		t.Fatalf("explanation = %+v", result.Explanation)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := explorer.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	documents, err := reopened.Documents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(documents) != 1 || documents[0].Revision.ID != first.Revision.ID {
		t.Fatalf("reopened documents = %+v", documents)
	}
	reopenedResponse, err := reopened.Retrieve(ctx, retrieval.Request{
		Text: "bounded retries", TopK: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reopenedResponse.Results) != 1 {
		t.Fatalf("reopened results = %+v", reopenedResponse.Results)
	}
}

func TestCrossDocumentNeighborhoodPersists(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	corpus, err := explorer.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	first, err := corpus.Ingest(ctx, explorer.Source{
		URI: "file:///a.txt", MediaType: explorer.MediaTypeText,
		Content: "Service A calls Service B.",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := corpus.Ingest(ctx, explorer.Source{
		URI: "file:///b.txt", MediaType: explorer.MediaTypeText,
		Content: "Service B stores account records.",
	})
	if err != nil {
		t.Fatal(err)
	}
	edge := graph.Edge{
		ID: "service-dependency", From: first.Document.ID, To: second.Document.ID,
		Type: "depends_on", Weight: 1,
	}
	if err := corpus.Connect(ctx, edge); err != nil {
		t.Fatal(err)
	}
	if err := corpus.Connect(ctx, edge); err != nil {
		t.Fatalf("idempotent connect: %v", err)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}
	corpus, err = explorer.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	neighborhood, err := corpus.Neighborhood(ctx, explorer.NeighborhoodRequest{
		NodeIDs:   []shoal.ID{first.Document.ID},
		Depth:     1,
		EdgeTypes: []string{"depends_on"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(neighborhood.Nodes) != 2 || len(neighborhood.Edges) != 1 ||
		neighborhood.Edges[0].ID != edge.ID {
		t.Fatalf("neighborhood = %+v", neighborhood)
	}
}

func TestVectorModeFailsExplicitly(t *testing.T) {
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	_, err = corpus.Retrieve(context.Background(), retrieval.Request{
		Text: "query", Modes: []retrieval.Mode{retrieval.ModeVector},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("expected unavailable, got %v", err)
	}
}

func TestRevisionIdentityPreservesComponentBoundaries(t *testing.T) {
	ctx := context.Background()
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()

	first, err := corpus.Ingest(ctx, explorer.Source{
		URI: "file:///collision.txt", Title: "a\x00b",
		MediaType: explorer.MediaTypeText, Content: "c",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := corpus.Ingest(ctx, explorer.Source{
		URI: "file:///collision.txt", Title: "a",
		MediaType: explorer.MediaTypeText, Content: "b\x00c",
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision.ID == second.Revision.ID {
		t.Fatalf("distinct component sequences produced revision %q", first.Revision.ID)
	}
	if second.Disposition != explorer.IngestApplied {
		t.Fatalf("second disposition = %q", second.Disposition)
	}
}

func TestRetrieveAcceptsMaximumTopK(t *testing.T) {
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	if _, err := corpus.Ingest(context.Background(), explorer.Source{
		URI: "file:///topk.txt", MediaType: explorer.MediaTypeText,
		Content: "maximum result limit",
	}); err != nil {
		t.Fatal(err)
	}
	response, err := corpus.Retrieve(context.Background(), retrieval.Request{
		Text: "maximum", TopK: ^uint32(0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 {
		t.Fatalf("results = %+v", response.Results)
	}
}

func TestRequestIdentityPreservesScopeBoundariesAndAllFields(t *testing.T) {
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	ctx := context.Background()
	base := retrieval.Request{
		Text:  "query",
		TopK:  1,
		Modes: []retrieval.Mode{retrieval.ModeLexical},
		Scope: retrieval.Scope{
			DocumentIDs: []shoal.ID{"a b", "c"},
			NodeIDs:     []shoal.ID{"node"},
		},
		AsOf: time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC),
	}
	baseResponse, err := corpus.Retrieve(ctx, base)
	if err != nil {
		t.Fatal(err)
	}

	scopeCollision := base
	scopeCollision.Scope.DocumentIDs = []shoal.ID{"a", "b c"}
	explainChange := base
	explainChange.Explain = true
	topKChange := base
	topKChange.TopK = 2
	modeChange := base
	modeChange.Modes = []retrieval.Mode{retrieval.ModeTree}
	nodeChange := base
	nodeChange.Scope.NodeIDs = []shoal.ID{"other"}
	asOfChange := base
	asOfChange.AsOf = base.AsOf.Add(time.Nanosecond)

	for name, request := range map[string]retrieval.Request{
		"scope boundaries": scopeCollision,
		"explain":          explainChange,
		"top k":            topKChange,
		"modes":            modeChange,
		"node scope":       nodeChange,
		"as of":            asOfChange,
	} {
		response, err := corpus.Retrieve(ctx, request)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if response.RequestID == baseResponse.RequestID {
			t.Errorf("%s did not change request ID %q", name, response.RequestID)
		}
	}
}

func TestExplanationReportsWeightedModeContribution(t *testing.T) {
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	if _, err := corpus.Ingest(context.Background(), explorer.Source{
		URI: "file:///weighted.md", MediaType: explorer.MediaTypeMarkdown,
		Content: "# Heading\n\nEvidence without the relationship term.\n",
	}); err != nil {
		t.Fatal(err)
	}
	response, err := corpus.Retrieve(context.Background(), retrieval.Request{
		Text: "contains", TopK: 1,
		Modes:   []retrieval.Mode{retrieval.ModeGraph},
		Explain: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 {
		t.Fatalf("results = %+v", response.Results)
	}
	result := response.Results[0]
	if result.Explanation == nil {
		t.Fatal("missing explanation")
	}
	if got := result.Explanation.Scores[string(retrieval.ModeGraph)]; got != result.Score {
		t.Fatalf("graph contribution = %v, result score = %v", got, result.Score)
	}
	if result.Score != shoal.Score(0.25) {
		t.Fatalf("weighted graph score = %v", result.Score)
	}
}

func TestMarkdownHeadingsPreserveHashesAndIgnoreFences(t *testing.T) {
	ctx := context.Background()
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()

	result, err := corpus.Ingest(ctx, explorer.Source{
		URI:       "file:///languages.md",
		MediaType: explorer.MediaTypeMarkdown,
		Content:   "# C#\n\n```markdown\n# Not a section\n```\n\n## Runtime ###\n\nDetails.\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := corpus.Document(ctx, result.Document.ID, result.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Document.Title != "C#" {
		t.Fatalf("document title = %q", view.Document.Title)
	}
	if len(view.Root.Children) != 1 {
		t.Fatalf("root children = %+v", view.Root.Children)
	}
	top := view.Root.Children[0]
	if top.Section.Heading != "C#" || len(top.Children) != 1 ||
		top.Children[0].Section.Heading != "Runtime" {
		t.Fatalf("outline = %+v", view.Root)
	}
	if got := top.Spans[0].Text; !strings.Contains(got, "# Not a section") {
		t.Fatalf("fenced content span = %q", got)
	}
}
