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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/model"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	maxExplorerEmbeddingDimensions = 16 * 1024
	maxExplorerEmbeddingBytes      = 64 * 1024 * 1024
	maxExplorerEmbeddingSpans      = 4096
	vectorAvailabilityTTL          = time.Minute
	vectorCapabilityProbeText      = "shoal vector capability probe"
)

type vectorAvailabilityCache struct {
	checkedAt time.Time
	available bool
}

type embeddingSpaceCache struct {
	provenance persistedEmbeddingProvenance
	found      bool
}

// VectorScoreRequest asks the embedded Explorer to recompute vector scores for
// exact citations already selected by a trusted wrapper.
type VectorScoreRequest struct {
	Text      string
	Citations []document.Citation
}

// VectorAvailable reports whether the current embedded corpus can serve vector
// retrieval without silently ignoring stale or missing embeddings.
func (e *Explorer) VectorAvailable(ctx context.Context) (bool, error) {
	if err := contextError(ctx); err != nil {
		return false, err
	}
	now := time.Now().UTC()
	e.mu.RLock()
	if err := e.requireOpen(); err != nil {
		e.mu.RUnlock()
		return false, err
	}
	if !e.vectorAvailability.checkedAt.IsZero() &&
		now.Sub(e.vectorAvailability.checkedAt) < vectorAvailabilityTTL {
		available := e.vectorAvailability.available
		e.mu.RUnlock()
		return available, nil
	}
	if e.embedder == nil {
		e.mu.RUnlock()
		if err := e.cacheVectorAvailability(false, now); err != nil {
			return false, err
		}
		return false, nil
	}
	e.mu.RUnlock()

	e.vectorProbeMu.Lock()
	defer e.vectorProbeMu.Unlock()

	now = time.Now().UTC()
	e.mu.RLock()
	if err := e.requireOpen(); err != nil {
		e.mu.RUnlock()
		return false, err
	}
	if !e.vectorAvailability.checkedAt.IsZero() &&
		now.Sub(e.vectorAvailability.checkedAt) < vectorAvailabilityTTL {
		available := e.vectorAvailability.available
		e.mu.RUnlock()
		return available, nil
	}
	if e.embedder == nil {
		e.mu.RUnlock()
		if err := e.cacheVectorAvailability(false, now); err != nil {
			return false, err
		}
		return false, nil
	}
	space, found, corpusReady, err := e.vectorAvailabilitySnapshotLocked()
	if err != nil {
		e.mu.RUnlock()
		return false, err
	}
	e.mu.RUnlock()

	if !corpusReady {
		if err := e.cacheVectorAvailability(false, now); err != nil {
			return false, err
		}
		return false, nil
	}
	provenance, _, err := e.embedQuery(ctx, vectorCapabilityProbeText)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorCanceled) ||
			shoal.IsErrorCode(err, shoal.ErrorDeadline) {
			return false, err
		}
		if err := e.cacheVectorAvailability(false, now); err != nil {
			return false, err
		}
		return false, nil
	}
	available := !found || provenance == space
	if err := e.cacheVectorAvailability(available, now); err != nil {
		return false, err
	}
	return available, nil
}

func (e *Explorer) vectorAvailabilitySnapshotLocked() (
	persistedEmbeddingProvenance, bool, bool, error,
) {
	space, found := e.embeddingSpace.provenance, e.embeddingSpace.found
	corpusReady := true
	for _, revisions := range e.documents {
		record, err := latestRevision(revisions)
		if err != nil {
			return persistedEmbeddingProvenance{}, false, false, err
		}
		if record == nil || len(record.Spans) == 0 {
			continue
		}
		if _, err := recordEmbeddingMap(record); err != nil {
			corpusReady = false
			break
		}
		current := record.Embeddings.Provenance
		if space != current {
			corpusReady = false
			break
		}
	}
	return space, found, corpusReady, nil
}

// VectorScores recomputes vector scores for exact stored span citations. It is
// intentionally citation-addressed so authorization wrappers can project first
// and then verify ranking without receiving raw embeddings.
func (e *Explorer) VectorScores(
	ctx context.Context,
	request VectorScoreRequest,
) (map[shoal.ID]shoal.Score, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	for _, citation := range request.Citations {
		if err := citation.Validate(); err != nil {
			return nil, err
		}
	}
	e.mu.RLock()
	if err := e.requireOpen(); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	e.mu.RUnlock()
	queryProvenance, queryVector, err := e.embedQuery(ctx, request.Text)
	if err != nil {
		return nil, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.requireOpen(); err != nil {
		return nil, err
	}
	embeddingMaps := make(
		map[documentRevisionKey]map[shoal.ID]persistedSpanEmbedding)
	spanMaps := make(map[documentRevisionKey]map[shoal.ID]document.Span)
	scores := make(map[shoal.ID]shoal.Score, len(request.Citations))
	for _, citation := range request.Citations {
		key := documentRevisionKey{
			documentID: citation.DocumentID,
			revisionID: citation.RevisionID,
		}
		record := e.documents[key.documentID][key.revisionID]
		if record == nil {
			return nil, shoal.NewError(
				shoal.ErrorNotFound, "vector score citation revision not found")
		}
		embeddings := embeddingMaps[key]
		if embeddings == nil {
			if record.Embeddings == nil {
				return nil, shoal.NewError(
					shoal.ErrorUnavailable,
					"vector scoring requires embeddings for every cited span",
				)
			}
			if record.Embeddings.Provenance != queryProvenance {
				return nil, incompatibleEmbeddingSpaceError(
					queryProvenance,
					record.Embeddings.Provenance,
				)
			}
			embeddings, err = recordEmbeddingMap(record)
			if err != nil {
				return nil, err
			}
			embeddingMaps[key] = embeddings
		}
		spans := spanMaps[key]
		if spans == nil {
			spans = make(map[shoal.ID]document.Span, len(record.Spans))
			for _, span := range record.Spans {
				spans[span.ID] = span
			}
			spanMaps[key] = spans
		}
		span, ok := spans[citation.SpanID]
		if !ok || span.SectionID != citation.SectionID ||
			span.Range != citation.Range {
			return nil, shoal.NewError(
				shoal.ErrorNotFound, "vector score citation span not found")
		}
		embedding, ok := embeddings[span.ID]
		if !ok || !embeddingMatchesSpan(embedding, span) {
			return nil, shoal.NewError(
				shoal.ErrorUnavailable,
				"stored span embeddings are stale or incomplete",
			)
		}
		score, err := vectorScore(queryVector, embedding.Vector)
		if err != nil {
			return nil, err
		}
		scores[span.ID] = score
	}
	return scores, nil
}

func (e *Explorer) cacheVectorAvailability(
	available bool,
	checkedAt time.Time,
) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return shoal.NewError(shoal.ErrorUnavailable, "explorer is closed")
	}
	e.vectorAvailability = vectorAvailabilityCache{
		checkedAt: checkedAt,
		available: available,
	}
	return nil
}

func (e *Explorer) invalidateVectorAvailabilityLocked() {
	e.vectorAvailability = vectorAvailabilityCache{}
}

func (e *Explorer) embedParsedSpans(
	ctx context.Context, spans []document.Span,
) (*persistedEmbeddingSet, error) {
	if e.embedder == nil || len(spans) == 0 {
		return nil, nil
	}
	if len(spans) > maxExplorerEmbeddingSpans {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"source has too many spans for embedded vector indexing",
		)
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	identity, err := e.embeddingIdentity()
	if err != nil {
		return nil, err
	}
	first, provenance, err := e.embedSpan(ctx, spans[0], identity)
	if err != nil {
		return nil, err
	}
	if err := validateEmbeddingAggregateBound(
		len(spans), provenance.Dimensions,
	); err != nil {
		return nil, err
	}
	e.mu.RLock()
	if err := e.ensureEmbeddingSpaceCompatibleLocked(provenance); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	e.mu.RUnlock()
	embedded := &persistedEmbeddingSet{
		Provenance: provenance,
		Spans:      make([]persistedSpanEmbedding, len(spans)),
	}
	embedded.Spans[0] = first
	for index := 1; index < len(spans); index++ {
		embedding, current, err := e.embedSpan(ctx, spans[index], identity)
		if err != nil {
			return nil, err
		}
		if current != provenance {
			return nil, shoal.NewError(
				shoal.ErrorConflict,
				"embedding provider returned incompatible provenance within one revision",
			)
		}
		embedded.Spans[index] = embedding
	}
	return embedded, nil
}

func (e *Explorer) embedSpan(
	ctx context.Context,
	span document.Span,
	identity string,
) (persistedSpanEmbedding, persistedEmbeddingProvenance, error) {
	result, err := e.embedder.Embed(ctx, model.EmbedRequest{Text: span.Text})
	if err != nil {
		return persistedSpanEmbedding{}, persistedEmbeddingProvenance{},
			embeddingProviderError("embed source span", err)
	}
	provenance, vector, err := normalizedEmbeddingResult(result, identity)
	if err != nil {
		return persistedSpanEmbedding{}, persistedEmbeddingProvenance{}, err
	}
	return persistedSpanEmbedding{
		SpanID:     span.ID,
		TextDigest: spanTextDigest(span),
		Range:      span.Range,
		Vector:     vector,
	}, provenance, nil
}

func (e *Explorer) embedQuery(
	ctx context.Context, text string,
) (persistedEmbeddingProvenance, []float32, error) {
	if e.embedder == nil {
		return persistedEmbeddingProvenance{}, nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"vector retrieval is not configured for the embedded Explorer",
		)
	}
	identity, err := e.embeddingIdentity()
	if err != nil {
		return persistedEmbeddingProvenance{}, nil, err
	}
	result, err := e.embedder.Embed(ctx, model.EmbedRequest{Text: text})
	if err != nil {
		return persistedEmbeddingProvenance{}, nil,
			embeddingProviderError("embed retrieval query", err)
	}
	provenance, vector, err := normalizedEmbeddingResult(result, identity)
	if err != nil {
		return persistedEmbeddingProvenance{}, nil, err
	}
	return provenance, vector, nil
}

func normalizedEmbeddingResult(
	result model.EmbedResult, identity string,
) (persistedEmbeddingProvenance, []float32, error) {
	provider := strings.TrimSpace(result.Provenance.Provider)
	modelName := strings.TrimSpace(result.Provenance.Model)
	if provider == "" || modelName == "" ||
		!utf8.ValidString(provider) || !utf8.ValidString(modelName) ||
		len(provider) > shoal.MaxSemanticStringBytes ||
		len(modelName) > shoal.MaxSemanticStringBytes {
		return persistedEmbeddingProvenance{}, nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"embedding provider did not return stable provenance",
		)
	}
	if len(result.Vector) == 0 ||
		len(result.Vector) > maxExplorerEmbeddingDimensions {
		return persistedEmbeddingProvenance{}, nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"embedding provider returned an unsupported vector dimension",
		)
	}
	vector := append([]float32(nil), result.Vector...)
	var norm float64
	for _, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return persistedEmbeddingProvenance{}, nil, shoal.NewError(
				shoal.ErrorUnavailable,
				"embedding provider returned a non-finite vector value",
			)
		}
		norm += float64(value) * float64(value)
	}
	if norm == 0 {
		return persistedEmbeddingProvenance{}, nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"embedding provider returned a zero vector",
		)
	}
	return persistedEmbeddingProvenance{
		Provider: provider, Model: modelName,
		Identity: identity, Dimensions: len(vector),
	}, vector, nil
}

func (e *Explorer) embeddingIdentity() (string, error) {
	return embeddingIdentityFor(e.embedder)
}

func embeddingIdentityFor(embedder model.Embedder) (string, error) {
	provider, ok := embedder.(model.EmbeddingSpaceIdentityProvider)
	if !ok {
		return "", shoal.NewError(
			shoal.ErrorInvalidArgument,
			"embedding provider must expose a stable embedding space identity",
		)
	}
	identity, err := provider.EmbeddingSpaceIdentity()
	if err != nil {
		return "", embeddingProviderError("identify embedding space", err)
	}
	identity = strings.TrimSpace(identity)
	if identity == "" || !utf8.ValidString(identity) ||
		len(identity) > shoal.MaxSemanticStringBytes {
		return "", shoal.NewError(
			shoal.ErrorInvalidArgument,
			"embedding provider identity is invalid",
		)
	}
	return identity, nil
}

func (e *Explorer) ensureEmbeddingSpaceCompatibleLocked(
	provenance persistedEmbeddingProvenance,
) error {
	if err := validateEmbeddingProvenance(provenance); err != nil {
		return err
	}
	existing, ok, err := e.validatedCachedEmbeddingSpaceLocked()
	if err != nil {
		return err
	}
	if ok && existing != provenance {
		return incompatibleEmbeddingSpaceError(existing, provenance)
	}
	return nil
}

func (e *Explorer) validatedCachedEmbeddingSpaceLocked() (
	persistedEmbeddingProvenance, bool, error,
) {
	if !e.embeddingSpace.found {
		return persistedEmbeddingProvenance{}, false, nil
	}
	if err := validateEmbeddingProvenance(e.embeddingSpace.provenance); err != nil {
		return persistedEmbeddingProvenance{}, false, err
	}
	return e.embeddingSpace.provenance, true, nil
}

func (e *Explorer) embeddingSpaceLocked() (
	persistedEmbeddingProvenance, bool, error,
) {
	var space persistedEmbeddingProvenance
	found := false
	for _, revisions := range e.documents {
		for _, record := range revisions {
			if record.Embeddings == nil {
				continue
			}
			if err := validateEmbeddingSet(record); err != nil {
				return persistedEmbeddingProvenance{}, false, err
			}
			current := record.Embeddings.Provenance
			if !found {
				space = current
				found = true
				continue
			}
			if space != current {
				return persistedEmbeddingProvenance{}, false,
					incompatibleEmbeddingSpaceError(space, current)
			}
		}
	}
	return space, found, nil
}

func validateEmbeddingSet(record *persistedDocument) error {
	if record == nil || record.Embeddings == nil {
		return nil
	}
	if err := validateEmbeddingProvenance(record.Embeddings.Provenance); err != nil {
		return err
	}
	if err := validateEmbeddingAggregateBound(
		len(record.Embeddings.Spans),
		record.Embeddings.Provenance.Dimensions,
	); err != nil {
		return shoal.WrapError(
			shoal.ErrorInternal, "stored span embeddings exceed aggregate bound", err)
	}
	seen := make(map[shoal.ID]struct{}, len(record.Embeddings.Spans))
	for _, embedding := range record.Embeddings.Spans {
		if _, duplicate := seen[embedding.SpanID]; duplicate {
			return shoal.NewError(
				shoal.ErrorInternal,
				"stored span embeddings contain duplicate span IDs",
			)
		}
		seen[embedding.SpanID] = struct{}{}
		if embedding.TextDigest == "" || len(embedding.Vector) !=
			record.Embeddings.Provenance.Dimensions {
			return shoal.NewError(
				shoal.ErrorInternal,
				"stored span embedding is incomplete",
			)
		}
		if err := embedding.Range.Validate(); err != nil {
			return shoal.WrapError(
				shoal.ErrorInternal, "stored span embedding range is invalid", err)
		}
		var norm float64
		for _, value := range embedding.Vector {
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return shoal.NewError(
					shoal.ErrorInternal,
					"stored span embedding contains a non-finite vector value",
				)
			}
			norm += float64(value) * float64(value)
		}
		if norm == 0 {
			return shoal.NewError(
				shoal.ErrorInternal,
				"stored span embedding has zero magnitude",
			)
		}
	}
	return nil
}

func validateEmbeddingProvenance(provenance persistedEmbeddingProvenance) error {
	if strings.TrimSpace(provenance.Provider) == "" ||
		strings.TrimSpace(provenance.Model) == "" ||
		!utf8.ValidString(provenance.Provider) ||
		!utf8.ValidString(provenance.Model) ||
		len(provenance.Provider) > shoal.MaxSemanticStringBytes ||
		len(provenance.Model) > shoal.MaxSemanticStringBytes ||
		strings.TrimSpace(provenance.Identity) == "" ||
		!utf8.ValidString(provenance.Identity) ||
		len(provenance.Identity) > shoal.MaxSemanticStringBytes ||
		provenance.Dimensions <= 0 ||
		provenance.Dimensions > maxExplorerEmbeddingDimensions {
		return shoal.NewError(
			shoal.ErrorInternal, "stored embedding provenance is invalid")
	}
	return nil
}

func validateEmbeddingAggregateBound(spanCount, dimensions int) error {
	if spanCount < 0 || dimensions < 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "embedding aggregate bound is invalid")
	}
	bytes := uint64(spanCount) * uint64(dimensions) * 4
	if bytes > maxExplorerEmbeddingBytes {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"span embeddings exceed the embedded vector index bound",
		)
	}
	return nil
}

func recordEmbeddingMap(
	record *persistedDocument,
) (map[shoal.ID]persistedSpanEmbedding, error) {
	if record == nil || record.Embeddings == nil {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"vector retrieval requires embeddings for every eligible span",
		)
	}
	if err := validateEmbeddingSet(record); err != nil {
		return nil, err
	}
	bySpan := make(map[shoal.ID]persistedSpanEmbedding, len(record.Embeddings.Spans))
	for _, embedding := range record.Embeddings.Spans {
		bySpan[embedding.SpanID] = embedding
	}
	for _, span := range record.Spans {
		embedding, ok := bySpan[span.ID]
		if !ok || !embeddingMatchesSpan(embedding, span) {
			return nil, shoal.NewError(
				shoal.ErrorUnavailable,
				"stored span embeddings are stale or incomplete",
			)
		}
	}
	return bySpan, nil
}

func embeddingMatchesSpan(embedding persistedSpanEmbedding, span document.Span) bool {
	return embedding.SpanID == span.ID &&
		embedding.TextDigest == spanTextDigest(span) &&
		embedding.Range == span.Range
}

func spanTextDigest(span document.Span) string {
	sum := sha256.Sum256([]byte(span.Text))
	return hex.EncodeToString(sum[:])
}

func vectorScore(query, candidate []float32) (shoal.Score, error) {
	if len(query) == 0 || len(query) != len(candidate) {
		return 0, shoal.NewError(
			shoal.ErrorConflict, "embedding vector dimensions do not match")
	}
	var dot, queryNorm, candidateNorm float64
	for i := range query {
		q, c := float64(query[i]), float64(candidate[i])
		dot += q * c
		queryNorm += q * q
		candidateNorm += c * c
	}
	if queryNorm == 0 || candidateNorm == 0 {
		return 0, shoal.NewError(
			shoal.ErrorUnavailable,
			"embedding vector has zero magnitude",
		)
	}
	cosine := dot / (math.Sqrt(queryNorm) * math.Sqrt(candidateNorm))
	if math.IsNaN(cosine) || math.IsInf(cosine, 0) {
		return 0, shoal.NewError(
			shoal.ErrorUnavailable,
			"embedding vector score is non-finite",
		)
	}
	if cosine > 1 {
		cosine = 1
	}
	if cosine < -1 {
		cosine = -1
	}
	return shoal.Score((cosine + 1) / 2), nil
}

func embeddingProviderError(operation string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, model.ErrCanceled):
		return shoal.WrapError(shoal.ErrorCanceled, operation, err)
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, model.ErrTimeout):
		return shoal.WrapError(shoal.ErrorDeadline, operation, err)
	case errors.Is(err, model.ErrInvalidConfig), errors.Is(err, model.ErrInvalidRequest):
		return shoal.WrapError(shoal.ErrorInvalidArgument, operation, err)
	default:
		return shoal.WrapError(shoal.ErrorUnavailable, operation, err)
	}
}

func incompatibleEmbeddingSpaceError(
	existing, incoming persistedEmbeddingProvenance,
) error {
	return shoal.NewError(
		shoal.ErrorConflict,
		fmt.Sprintf(
			"embedding space mismatch: existing %s/%s/%s/%d, incoming %s/%s/%s/%d",
			existing.Provider,
			existing.Model,
			existing.Identity,
			existing.Dimensions,
			incoming.Provider,
			incoming.Model,
			incoming.Identity,
			incoming.Dimensions,
		),
	)
}
