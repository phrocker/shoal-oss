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

// Package explorer provides an opinionated document and graph exploration
// experience over Shoal's transport-neutral knowledge contracts.
package explorer

import (
	"context"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	MediaTypeMarkdown = "text/markdown"
	MediaTypeText     = "text/plain"
)

// Source is one textual source revision to ingest.
type Source struct {
	URI       string
	Title     string
	MediaType string
	Content   string
	Metadata  shoal.Metadata
}

// IngestDisposition reports whether ingestion created a revision or found the
// same immutable revision already present.
type IngestDisposition string

const (
	IngestApplied   IngestDisposition = "applied"
	IngestUnchanged IngestDisposition = "unchanged"
)

// IngestResult identifies the durable artifacts created for one source.
type IngestResult struct {
	Disposition  IngestDisposition
	Document     document.Document
	Revision     document.Revision
	SectionCount int
	SpanCount    int
}

// DocumentSummary is the current revision shown in corpus listings.
type DocumentSummary struct {
	Document  document.Document
	Revision  document.Revision
	SourceURI string
}

// SectionView combines one section with its directly attributable spans and
// nested sections.
type SectionView struct {
	Section  document.Section
	Spans    []document.Span
	Children []SectionView
}

// DocumentView is a revision-specific, navigable document.
type DocumentView struct {
	Document  document.Document
	Revision  document.Revision
	SourceURI string
	Root      SectionView
}

// NeighborhoodRequest bounds a graph expansion around one or more nodes.
type NeighborhoodRequest struct {
	NodeIDs   []shoal.ID
	Depth     uint32
	EdgeTypes []string
}

// Neighborhood is a deterministic graph subgraph.
type Neighborhood struct {
	Nodes []graph.Node
	Edges []graph.Edge
}

// Client is the product-facing Explorer contract. Implementations can be
// embedded or remote without changing document, graph, or retrieval values.
type Client interface {
	retrieval.Retriever
	Ingest(context.Context, Source) (IngestResult, error)
	Documents(context.Context) ([]DocumentSummary, error)
	Document(context.Context, shoal.ID, shoal.ID) (DocumentView, error)
	Connect(context.Context, graph.Edge) error
	Neighborhood(context.Context, NeighborhoodRequest) (Neighborhood, error)
}
