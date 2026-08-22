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

package code

import (
	"context"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// IngestRequest is an immutable, idempotent request bound to both the exact
// parse bytes and the result validated for those bytes.
type IngestRequest struct {
	idempotencyKey ID
	parseRequest   ParseRequest
	parseResult    ParseResult
}

// NewIngestRequest validates result with ParseResult.ValidateFor and derives
// the canonical idempotency key.
func NewIngestRequest(
	parseRequest ParseRequest, result ParseResult) (IngestRequest, error) {
	if err := result.ValidateFor(parseRequest); err != nil {
		return IngestRequest{}, err
	}
	key, err := expectedIngestionKey(result)
	if err != nil {
		return IngestRequest{}, err
	}
	return IngestRequest{
		idempotencyKey: key,
		parseRequest:   parseRequest.clone(),
		parseResult:    cloneParseResult(result),
	}, nil
}

func (r IngestRequest) Validate() error {
	if err := r.parseResult.ValidateFor(r.parseRequest); err != nil {
		return err
	}
	expected, err := expectedIngestionKey(r.parseResult)
	if err != nil {
		return err
	}
	if r.idempotencyKey != expected {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "ingestion idempotency key does not match parse result")
	}
	return nil
}

func (r IngestRequest) IdempotencyKey() ID {
	return r.idempotencyKey
}

func (r IngestRequest) ParseRequest() ParseRequest {
	return r.parseRequest.clone()
}

func (r IngestRequest) ParseResult() ParseResult {
	return cloneParseResult(r.parseResult)
}

// ArtifactKind identifies a public materialization surface without coupling
// parser output to a storage representation.
type ArtifactKind string

const (
	ArtifactDocument ArtifactKind = "document"
	ArtifactGraph    ArtifactKind = "graph"
)

// ArtifactRef identifies a document or graph artifact created by ingestion.
type ArtifactRef struct {
	kind       ArtifactKind
	identifier shoal.ID
}

// NewArtifactRef creates a materialization reference.
func NewArtifactRef(kind ArtifactKind, identifier shoal.ID) (ArtifactRef, error) {
	reference := ArtifactRef{kind: kind, identifier: identifier}
	if err := reference.Validate(); err != nil {
		return ArtifactRef{}, err
	}
	return reference, nil
}

func (r ArtifactRef) Validate() error {
	switch r.kind {
	case ArtifactDocument, ArtifactGraph:
	default:
		return shoal.NewError(shoal.ErrorInvalidArgument, "invalid artifact kind")
	}
	if !requiredExact(string(r.identifier)) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "artifact identifier is required")
	}
	return nil
}

func (r ArtifactRef) Kind() ArtifactKind {
	return r.kind
}

func (r ArtifactRef) Identifier() shoal.ID {
	return r.identifier
}

// IngestDisposition reports whether the request created materializations or
// matched an already-applied idempotency key.
type IngestDisposition string

const (
	IngestApplied   IngestDisposition = "applied"
	IngestUnchanged IngestDisposition = "unchanged"
)

// IngestResult reports materializations for one exact ingestion request.
type IngestResult struct {
	idempotencyKey ID
	source         Source
	disposition    IngestDisposition
	artifacts      []ArtifactRef
}

// NewIngestResult creates and validates an ingestion result tied to request.
func NewIngestResult(request IngestRequest, disposition IngestDisposition,
	artifacts []ArtifactRef) (IngestResult, error) {
	result := IngestResult{
		idempotencyKey: request.IdempotencyKey(),
		source:         request.parseRequest.Source(),
		disposition:    disposition,
		artifacts:      append([]ArtifactRef(nil), artifacts...),
	}
	if err := result.ValidateFor(request); err != nil {
		return IngestResult{}, err
	}
	return result, nil
}

func (r IngestResult) ValidateFor(request IngestRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if r.idempotencyKey != request.IdempotencyKey() ||
		!r.source.Equal(request.parseRequest.Source()) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "ingestion result does not match request")
	}
	switch r.disposition {
	case IngestApplied, IngestUnchanged:
	default:
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "invalid ingestion disposition")
	}
	seen := make(map[string]struct{}, len(r.artifacts))
	for _, artifact := range r.artifacts {
		if err := artifact.Validate(); err != nil {
			return err
		}
		key := string(artifact.Kind()) + "\x00" + string(artifact.Identifier())
		if _, duplicate := seen[key]; duplicate {
			return shoal.NewError(
				shoal.ErrorInvalidArgument, "duplicate ingestion artifact")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func (r IngestResult) IdempotencyKey() ID {
	return r.idempotencyKey
}

func (r IngestResult) Source() Source {
	return r.source
}

func (r IngestResult) Disposition() IngestDisposition {
	return r.disposition
}

func (r IngestResult) Artifacts() []ArtifactRef {
	return append([]ArtifactRef(nil), r.artifacts...)
}

// Ingest is implemented by adapters that materialize parser-neutral results.
// Repeating a request must not create duplicate artifacts; implementations
// return IngestUnchanged with the same artifact identities after the first
// successful application.
type Ingest interface {
	Ingest(context.Context, IngestRequest) (IngestResult, error)
}

// Ingester is the conventional name for an Ingest implementation.
type Ingester = Ingest

func expectedIngestionKey(result ParseResult) (ID, error) {
	source := result.Source()
	language := result.Language()
	parser := result.Parser()
	return deriveID(
		"ingest",
		source.ID().String(),
		language.ID(),
		language.Version(),
		language.Dialect(),
		parser.Name(),
		parser.Version(),
		parser.ConfigurationHash().String(),
	)
}

func cloneParseResult(result ParseResult) ParseResult {
	result.roots = cloneIDs(result.roots)
	result.nodes = cloneNodes(result.nodes)
	result.symbols = cloneSymbols(result.symbols)
	result.externals = cloneExternals(result.externals)
	result.relationships = cloneRelationships(result.relationships)
	result.diagnostics = append([]Diagnostic(nil), result.diagnostics...)
	return result
}
