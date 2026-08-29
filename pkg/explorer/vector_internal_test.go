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

package explorer

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/model"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestVectorAvailabilityCachesProbeForEmptyCorpus(t *testing.T) {
	ctx := context.Background()
	embedder := &countingEmbedder{}
	corpus, err := OpenWithOptions(t.TempDir(), Options{Embedder: embedder})
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	for i := 0; i < 2; i++ {
		available, err := corpus.VectorAvailable(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !available {
			t.Fatal("configured empty corpus should be vector-capable")
		}
	}
	if embedder.calls != 1 {
		t.Fatalf("capability probes = %d, want 1", embedder.calls)
	}
}

func TestVectorAvailabilityCoalescesConcurrentProbes(t *testing.T) {
	ctx := context.Background()
	embedder := &countingEmbedder{delay: 10 * time.Millisecond}
	corpus, err := OpenWithOptions(t.TempDir(), Options{Embedder: embedder})
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	var wait sync.WaitGroup
	errorsSeen := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			available, err := corpus.VectorAvailable(ctx)
			if err == nil && !available {
				err = shoal.NewError(shoal.ErrorInternal, "vector unavailable")
			}
			errorsSeen <- err
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	if embedder.calls != 1 {
		t.Fatalf("coalesced capability probes = %d, want 1", embedder.calls)
	}
}

func TestEmbeddingAggregateBoundRejectsUnpersistableIndex(t *testing.T) {
	err := validateEmbeddingAggregateBound(int(maxExplorerEmbeddingBytes/4)+1, 1)
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("aggregate bound error = %v", err)
	}
}

func TestEmbeddingSpanCountBoundStopsBeforeProviderCalls(t *testing.T) {
	embedder := &countingEmbedder{}
	corpus, err := OpenWithOptions(t.TempDir(), Options{Embedder: embedder})
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	_, err = corpus.embedParsedSpans(
		context.Background(),
		make([]document.Span, maxExplorerEmbeddingSpans+1),
	)
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("span count bound error = %v", err)
	}
	if embedder.calls != 0 {
		t.Fatalf("provider calls before span bound = %d", embedder.calls)
	}
}

func TestEmbeddingProvenanceStringsAreBounded(t *testing.T) {
	ctx := context.Background()
	embedder := &countingEmbedder{
		model:    strings.Repeat("x", shoal.MaxSemanticStringBytes+1),
		identity: "counting-oversized-provenance",
	}
	corpus, err := OpenWithOptions(t.TempDir(), Options{Embedder: embedder})
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	_, err = corpus.Ingest(ctx, Source{
		URI:       "file:///oversized-provenance.txt",
		MediaType: MediaTypeText,
		Content:   "oversized provenance",
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("oversized provenance error = %v", err)
	}

	_, err = OpenWithOptions(t.TempDir(), Options{
		Embedder: &countingEmbedder{
			identity: strings.Repeat("i", shoal.MaxSemanticStringBytes+1),
		},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("oversized identity error = %v", err)
	}
}

func TestEmbeddingSpaceMismatchStopsAfterFirstProviderCall(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	corpus, err := OpenWithOptions(dataDir, Options{
		Embedder: model.FakeEmbedder{Model: "space-a", Dimensions: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := corpus.Ingest(ctx, Source{
		URI:       "file:///space-a.txt",
		MediaType: MediaTypeText,
		Content:   "existing vector space",
	}); err != nil {
		t.Fatal(err)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}

	embedder := &countingEmbedder{model: "space-b", dimensions: 16}
	reopened, err := OpenWithOptions(dataDir, Options{Embedder: embedder})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	_, err = reopened.Ingest(ctx, Source{
		URI:       "file:///space-b.md",
		MediaType: MediaTypeMarkdown,
		Content:   "# Mismatch\n\nfirst paragraph\n\nsecond paragraph\n\nthird paragraph",
	})
	if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("space mismatch error = %v", err)
	}
	if embedder.calls != 1 {
		t.Fatalf("provider calls before mismatch = %d, want 1", embedder.calls)
	}
}

func TestVectorAvailabilityIncludesHistoricalEmbeddingSpace(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	corpus, err := OpenWithOptions(dataDir, Options{
		Embedder: model.FakeEmbedder{Model: "historical-a", Dimensions: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := corpus.Ingest(ctx, Source{
		URI:       "file:///historical-space.md",
		MediaType: MediaTypeMarkdown,
		Content:   "# Historical\n\nold embedded span",
	}); err != nil {
		t.Fatal(err)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}
	legacy, err := Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Ingest(ctx, Source{
		URI:       "file:///historical-space.md",
		MediaType: MediaTypeMarkdown,
		Content:   "# Historical\n",
	}); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenWithOptions(dataDir, Options{
		Embedder: model.FakeEmbedder{Model: "historical-b", Dimensions: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	available, err := reopened.VectorAvailable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if available {
		t.Fatal("historical embedding mismatch advertised as available")
	}
}

func TestVectorRetrievalRejectsStaleEmbeddings(t *testing.T) {
	ctx := context.Background()
	corpus, err := OpenWithOptions(t.TempDir(), Options{
		Embedder: model.FakeEmbedder{Model: "stale", Dimensions: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	ingested, err := corpus.Ingest(ctx, Source{
		URI:       "file:///stale.txt",
		MediaType: MediaTypeText,
		Content:   "stale vector quote",
	})
	if err != nil {
		t.Fatal(err)
	}

	corpus.mu.Lock()
	record, err := latestRevision(corpus.documents[ingested.Document.ID])
	if err != nil {
		corpus.mu.Unlock()
		t.Fatal(err)
	}
	record.Embeddings.Spans[0].TextDigest = "stale"
	corpus.mu.Unlock()

	available, err := corpus.VectorAvailable(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if available {
		t.Fatal("stale embeddings advertised as vector-capable")
	}
	_, err = corpus.Retrieve(ctx, retrieval.Request{
		Text:  "stale vector quote",
		Modes: []retrieval.Mode{retrieval.ModeVector},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("stale vector error = %v", err)
	}
}

func TestClosedExplorerDoesNotCallEmbedderForVectorRequests(t *testing.T) {
	ctx := context.Background()
	embedder := &countingEmbedder{}
	corpus, err := OpenWithOptions(t.TempDir(), Options{Embedder: embedder})
	if err != nil {
		t.Fatal(err)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = corpus.Retrieve(ctx, retrieval.Request{
		Text:  "closed",
		Modes: []retrieval.Mode{retrieval.ModeVector},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("closed retrieve error = %v", err)
	}
	_, err = corpus.VectorScores(ctx, VectorScoreRequest{Text: "closed"})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("closed vector scores error = %v", err)
	}
	if embedder.calls != 0 {
		t.Fatalf("embedder called after close %d times", embedder.calls)
	}
}

type countingEmbedder struct {
	calls      int
	model      string
	dimensions int
	identity   string
	delay      time.Duration
}

func (e *countingEmbedder) CacheIdentity() (string, error) {
	if e.identity != "" {
		return e.identity, nil
	}
	return "counting-embedder-v1:" + e.model, nil
}

func (e *countingEmbedder) Embed(
	ctx context.Context, req model.EmbedRequest,
) (model.EmbedResult, error) {
	if err := ctx.Err(); err != nil {
		return model.EmbedResult{}, err
	}
	if e.delay > 0 {
		timer := time.NewTimer(e.delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return model.EmbedResult{}, ctx.Err()
		}
	}
	e.calls++
	dimensions := e.dimensions
	if dimensions == 0 {
		dimensions = 2
	}
	vector := make([]float32, dimensions)
	vector[0] = 1
	modelName := e.model
	if modelName == "" {
		modelName = "deterministic"
	}
	return model.EmbedResult{
		Vector: vector,
		Provenance: model.Provenance{
			Provider: "counting",
			Model:    modelName,
		},
	}, nil
}
