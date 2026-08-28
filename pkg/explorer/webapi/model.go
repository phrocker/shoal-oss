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

// Package webapi defines the transport-neutral Explorer workspace service and
// its standard-library HTTP transport.
package webapi

import (
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	DefaultPageSize      uint32 = 25
	MaxPageSize          uint32 = 100
	MaxTopK              uint32 = 50
	DefaultDepth         uint32 = 1
	MaxDepth             uint32 = 4
	DefaultFanout        uint32 = 12
	MaxFanout            uint32 = 50
	DefaultMaxNodes      uint32 = 100
	MaxNodes             uint32 = 250
	MaxEdgeTypes         uint32 = 64
	MaxEvidencePerResult uint32 = 32
	MaxResponseBytes     uint64 = 64 << 20
)

// Capability identifies one browser-visible feature without revealing where or
// how the backing Explorer service executes it.
type Capability string

const (
	CapabilityDocuments    Capability = "documents"
	CapabilityDocument     Capability = "document"
	CapabilityRetrieve     Capability = "retrieve"
	CapabilityNeighborhood Capability = "neighborhood"
	CapabilityPath         Capability = "path"
)

// Capabilities advertises only stable logical features supported by a backend.
type Capabilities struct {
	Documents    bool `json:"documents"`
	Document     bool `json:"document"`
	Retrieve     bool `json:"retrieve"`
	Neighborhood bool `json:"neighborhood"`
	Path         bool `json:"path"`
}

// AllCapabilities returns the complete feature set implemented by the embedded
// service. Remote services must advertise their supported features explicitly.
func AllCapabilities() Capabilities {
	return Capabilities{
		Documents: true, Document: true, Retrieve: true,
		Neighborhood: true, Path: true,
	}
}

// Supports reports whether the feature is currently available.
func (c Capabilities) Supports(capability Capability) bool {
	switch capability {
	case CapabilityDocuments:
		return c.Documents
	case CapabilityDocument:
		return c.Document
	case CapabilityRetrieve:
		return c.Retrieve
	case CapabilityNeighborhood:
		return c.Neighborhood
	case CapabilityPath:
		return c.Path
	default:
		return false
	}
}

// Snapshot pins every workspace request to one logical corpus view.
type Snapshot struct {
	ID       string    `json:"id"`
	AsOf     time.Time `json:"as_of"`
	Frontier uint64    `json:"frontier"`
}

// PageRequest carries bounded cursor pagination.
type PageRequest struct {
	Limit  uint32 `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

// DocumentsRequest lists a page from a pinned corpus snapshot.
type DocumentsRequest struct {
	Snapshot Snapshot    `json:"snapshot,omitempty"`
	Page     PageRequest `json:"page,omitempty"`
}

// DocumentsResponse returns document summaries without exposing storage
// topology or implementation details.
type DocumentsResponse struct {
	Snapshot   Snapshot                   `json:"snapshot"`
	Documents  []explorer.DocumentSummary `json:"documents"`
	NextCursor string                     `json:"next_cursor,omitempty"`
}

// DocumentRequest fetches one immutable hierarchy.
type DocumentRequest struct {
	Snapshot   Snapshot `json:"snapshot"`
	DocumentID shoal.ID `json:"document_id"`
	RevisionID shoal.ID `json:"revision_id,omitempty"`
}

// DocumentResponse contains the hierarchy and the snapshot that produced it.
type DocumentResponse struct {
	Snapshot Snapshot              `json:"snapshot"`
	Document explorer.DocumentView `json:"document"`
}

// RetrievalRequest runs a bounded retrieval plan.
type RetrievalRequest struct {
	Snapshot Snapshot          `json:"snapshot"`
	Query    retrieval.Request `json:"query"`
}

// RetrievalResponse preserves exact citations and explanations.
type RetrievalResponse struct {
	Snapshot  Snapshot           `json:"snapshot"`
	Retrieval retrieval.Response `json:"retrieval"`
}

// NeighborhoodRequest asks for a bounded graph expansion.
type NeighborhoodRequest struct {
	Snapshot  Snapshot   `json:"snapshot"`
	NodeIDs   []shoal.ID `json:"node_ids"`
	Depth     uint32     `json:"depth,omitempty"`
	Fanout    uint32     `json:"fanout,omitempty"`
	MaxNodes  uint32     `json:"max_nodes,omitempty"`
	EdgeTypes []string   `json:"edge_types,omitempty"`
	Cursor    string     `json:"cursor,omitempty"`
}

// NeighborhoodResponse returns only the bounded subgraph requested.
type NeighborhoodResponse struct {
	Snapshot     Snapshot              `json:"snapshot"`
	Neighborhood explorer.Neighborhood `json:"neighborhood"`
	Truncated    bool                  `json:"truncated"`
	NextCursor   string                `json:"next_cursor,omitempty"`
}

// PathRequest asks for one bounded directed path.
type PathRequest struct {
	Snapshot  Snapshot `json:"snapshot"`
	From      shoal.ID `json:"from"`
	To        shoal.ID `json:"to"`
	MaxDepth  uint32   `json:"max_depth,omitempty"`
	Fanout    uint32   `json:"fanout,omitempty"`
	EdgeTypes []string `json:"edge_types,omitempty"`
}

// PathResponse contains the selected explanation path.
type PathResponse struct {
	Snapshot Snapshot   `json:"snapshot"`
	Path     graph.Path `json:"path"`
}

// MetadataResponse advertises server-enforced public bounds.
type MetadataResponse struct {
	MaxPageSize      uint32       `json:"max_page_size"`
	MaxTopK          uint32       `json:"max_top_k"`
	MaxDepth         uint32       `json:"max_depth"`
	MaxFanout        uint32       `json:"max_fanout"`
	MaxNodes         uint32       `json:"max_nodes"`
	MaxEdgeTypes     uint32       `json:"max_edge_types"`
	MaxResponseBytes uint64       `json:"max_response_bytes"`
	Capabilities     Capabilities `json:"capabilities"`
}
