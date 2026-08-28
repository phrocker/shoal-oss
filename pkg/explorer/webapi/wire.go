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

package webapi

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type wireDocument struct {
	ID            string       `json:"id"`
	RevisionID    string       `json:"revision_id"`
	Title         string       `json:"title"`
	RootSectionID string       `json:"root_section_id"`
	Metadata      wireMetadata `json:"metadata,omitempty"`
}

type wireRevision struct {
	ID            string       `json:"id"`
	DocumentID    string       `json:"document_id"`
	CreatedAt     time.Time    `json:"created_at"`
	SourceVersion string       `json:"source_version"`
	Metadata      wireMetadata `json:"metadata,omitempty"`
}

type wireSection struct {
	ID         string       `json:"id"`
	DocumentID string       `json:"document_id"`
	RevisionID string       `json:"revision_id"`
	ParentID   string       `json:"parent_id,omitempty"`
	Order      uint32       `json:"order"`
	Heading    string       `json:"heading"`
	Range      wireRange    `json:"range"`
	Metadata   wireMetadata `json:"metadata,omitempty"`
}

type wireSpan struct {
	ID         string       `json:"id"`
	DocumentID string       `json:"document_id"`
	RevisionID string       `json:"revision_id"`
	SectionID  string       `json:"section_id"`
	Order      uint32       `json:"order"`
	Range      wireRange    `json:"range"`
	Text       string       `json:"text"`
	Metadata   wireMetadata `json:"metadata,omitempty"`
}

type wireSectionView struct {
	Section  wireSection       `json:"section"`
	Spans    []wireSpan        `json:"spans"`
	Children []wireSectionView `json:"children"`
}

type wireNode struct {
	ID         string       `json:"id"`
	Kind       string       `json:"kind,omitempty"`
	Labels     []string     `json:"labels,omitempty"`
	Properties wireMetadata `json:"properties,omitempty"`
}

type wireEdge struct {
	ID         string       `json:"id"`
	From       string       `json:"from"`
	To         string       `json:"to"`
	Type       string       `json:"type"`
	Weight     shoal.Score  `json:"weight"`
	Properties wireMetadata `json:"properties,omitempty"`
}

type wirePath struct {
	Nodes []wireNode `json:"nodes"`
	Edges []wireEdge `json:"edges"`
}

type wirePosition struct {
	Offset int64 `json:"offset"`
	Page   int32 `json:"page,omitempty"`
}

type wireRange struct {
	Start wirePosition `json:"start"`
	End   wirePosition `json:"end"`
}

type wireExplanation struct {
	Modes   []retrieval.Mode       `json:"modes"`
	Summary string                 `json:"summary"`
	Scores  map[string]shoal.Score `json:"scores"`
}

type wireCitation struct {
	DocumentID string    `json:"document_id"`
	RevisionID string    `json:"revision_id"`
	SectionID  string    `json:"section_id,omitempty"`
	SpanID     string    `json:"span_id,omitempty"`
	Range      wireRange `json:"range"`
}

type wireNeighborhood struct {
	Nodes []wireNode `json:"nodes"`
	Edges []wireEdge `json:"edges"`
}

type wireMetadataEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type wireMetadata []wireMetadataEntry

// MarshalJSON keeps the uint64 frontier exact in JavaScript clients.
func (s Snapshot) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		ID       string    `json:"id"`
		AsOf     time.Time `json:"as_of"`
		Frontier string    `json:"frontier"`
	}{s.ID, s.AsOf, strconv.FormatUint(s.Frontier, 10)})
}

// UnmarshalJSON parses the decimal frontier string without float conversion.
func (s *Snapshot) UnmarshalJSON(data []byte) error {
	var wire struct {
		ID       string    `json:"id"`
		AsOf     time.Time `json:"as_of"`
		Frontier string    `json:"frontier"`
	}
	if err := strictUnmarshal(data, &wire); err != nil {
		return err
	}
	var frontier uint64
	var err error
	if wire.Frontier != "" {
		frontier, err = strconv.ParseUint(wire.Frontier, 10, 64)
		if err != nil {
			return fmt.Errorf("frontier must be a decimal uint64 string")
		}
	}
	*s = Snapshot{ID: wire.ID, AsOf: wire.AsOf, Frontier: frontier}
	return nil
}

func (r DocumentRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Snapshot   Snapshot `json:"snapshot"`
		DocumentID string   `json:"document_id"`
		RevisionID string   `json:"revision_id,omitempty"`
	}{
		Snapshot: r.Snapshot, DocumentID: encodeID(r.DocumentID),
		RevisionID: encodeOptionalID(r.RevisionID),
	})
}

func (r RetrievalRequest) MarshalJSON() ([]byte, error) {
	documentIDs := make([]string, 0, len(r.Query.Scope.DocumentIDs))
	for _, id := range r.Query.Scope.DocumentIDs {
		documentIDs = append(documentIDs, encodeID(id))
	}
	nodeIDs := make([]string, 0, len(r.Query.Scope.NodeIDs))
	for _, id := range r.Query.Scope.NodeIDs {
		nodeIDs = append(nodeIDs, encodeID(id))
	}
	return json.Marshal(struct {
		Snapshot Snapshot `json:"snapshot"`
		Query    struct {
			Text  string           `json:"text"`
			TopK  uint32           `json:"top_k,omitempty"`
			Modes []retrieval.Mode `json:"modes,omitempty"`
			Scope struct {
				DocumentIDs []string `json:"document_ids,omitempty"`
				NodeIDs     []string `json:"node_ids,omitempty"`
			} `json:"scope,omitempty"`
			AsOf    time.Time `json:"as_of,omitempty"`
			Explain bool      `json:"explain,omitempty"`
		} `json:"query"`
	}{
		Snapshot: r.Snapshot,
		Query: struct {
			Text  string           `json:"text"`
			TopK  uint32           `json:"top_k,omitempty"`
			Modes []retrieval.Mode `json:"modes,omitempty"`
			Scope struct {
				DocumentIDs []string `json:"document_ids,omitempty"`
				NodeIDs     []string `json:"node_ids,omitempty"`
			} `json:"scope,omitempty"`
			AsOf    time.Time `json:"as_of,omitempty"`
			Explain bool      `json:"explain,omitempty"`
		}{
			Text: r.Query.Text, TopK: r.Query.TopK, Modes: r.Query.Modes,
			Scope: struct {
				DocumentIDs []string `json:"document_ids,omitempty"`
				NodeIDs     []string `json:"node_ids,omitempty"`
			}{DocumentIDs: documentIDs, NodeIDs: nodeIDs},
			AsOf: r.Query.AsOf, Explain: r.Query.Explain,
		},
	})
}

func (r NeighborhoodRequest) MarshalJSON() ([]byte, error) {
	nodeIDs := make([]string, 0, len(r.NodeIDs))
	for _, id := range r.NodeIDs {
		nodeIDs = append(nodeIDs, encodeID(id))
	}
	return json.Marshal(struct {
		Snapshot  Snapshot `json:"snapshot"`
		NodeIDs   []string `json:"node_ids"`
		Depth     uint32   `json:"depth,omitempty"`
		Fanout    uint32   `json:"fanout,omitempty"`
		MaxNodes  uint32   `json:"max_nodes,omitempty"`
		EdgeTypes []string `json:"edge_types,omitempty"`
		Cursor    string   `json:"cursor,omitempty"`
	}{
		Snapshot: r.Snapshot, NodeIDs: nodeIDs, Depth: r.Depth,
		Fanout: r.Fanout, MaxNodes: r.MaxNodes, EdgeTypes: r.EdgeTypes,
		Cursor: r.Cursor,
	})
}

func (r PathRequest) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Snapshot  Snapshot `json:"snapshot"`
		From      string   `json:"from"`
		To        string   `json:"to"`
		MaxDepth  uint32   `json:"max_depth,omitempty"`
		Fanout    uint32   `json:"fanout,omitempty"`
		EdgeTypes []string `json:"edge_types,omitempty"`
	}{
		Snapshot: r.Snapshot, From: encodeID(r.From), To: encodeID(r.To),
		MaxDepth: r.MaxDepth, Fanout: r.Fanout, EdgeTypes: r.EdgeTypes,
	})
}

func (r DocumentsResponse) MarshalJSON() ([]byte, error) {
	documents := make([]any, 0, len(r.Documents))
	for _, summary := range r.Documents {
		documents = append(documents, struct {
			Document  wireDocument `json:"document"`
			Revision  wireRevision `json:"revision"`
			SourceURI string       `json:"source_uri"`
		}{
			Document: wireDocumentValue(summary.Document),
			Revision: wireRevisionValue(summary.Revision), SourceURI: summary.SourceURI,
		})
	}
	return json.Marshal(struct {
		Snapshot   Snapshot `json:"snapshot"`
		Documents  []any    `json:"documents"`
		NextCursor string   `json:"next_cursor,omitempty"`
	}{r.Snapshot, documents, r.NextCursor})
}

func (r *DocumentsResponse) UnmarshalJSON(data []byte) error {
	var wire struct {
		Snapshot  Snapshot `json:"snapshot"`
		Documents []struct {
			Document  wireDocument `json:"document"`
			Revision  wireRevision `json:"revision"`
			SourceURI string       `json:"source_uri"`
		} `json:"documents"`
		NextCursor string `json:"next_cursor,omitempty"`
	}
	if err := strictUnmarshal(data, &wire); err != nil {
		return err
	}
	documents := make([]explorer.DocumentSummary, 0, len(wire.Documents))
	for _, item := range wire.Documents {
		documentValue, err := documentValue(item.Document)
		if err != nil {
			return fmt.Errorf("documents.document: %w", err)
		}
		revisionValue, err := revisionValue(item.Revision)
		if err != nil {
			return fmt.Errorf("documents.revision: %w", err)
		}
		documents = append(documents, explorer.DocumentSummary{
			Document: documentValue, Revision: revisionValue, SourceURI: item.SourceURI,
		})
	}
	*r = DocumentsResponse{
		Snapshot: wire.Snapshot, Documents: documents, NextCursor: wire.NextCursor,
	}
	return nil
}

func (r DocumentResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Snapshot Snapshot `json:"snapshot"`
		Document any      `json:"document"`
	}{r.Snapshot, struct {
		Document  wireDocument    `json:"document"`
		Revision  wireRevision    `json:"revision"`
		SourceURI string          `json:"source_uri"`
		Root      wireSectionView `json:"root"`
	}{
		Document:  wireDocumentValue(r.Document.Document),
		Revision:  wireRevisionValue(r.Document.Revision),
		SourceURI: r.Document.SourceURI, Root: wireSectionViewValue(r.Document.Root),
	}})
}

func (r *DocumentResponse) UnmarshalJSON(data []byte) error {
	var wire struct {
		Snapshot Snapshot `json:"snapshot"`
		Document struct {
			Document  wireDocument    `json:"document"`
			Revision  wireRevision    `json:"revision"`
			SourceURI string          `json:"source_uri"`
			Root      wireSectionView `json:"root"`
		} `json:"document"`
	}
	if err := strictUnmarshal(data, &wire); err != nil {
		return err
	}
	documentValue, err := documentValue(wire.Document.Document)
	if err != nil {
		return fmt.Errorf("document.document: %w", err)
	}
	revisionValue, err := revisionValue(wire.Document.Revision)
	if err != nil {
		return fmt.Errorf("document.revision: %w", err)
	}
	root, err := sectionViewValue(wire.Document.Root)
	if err != nil {
		return fmt.Errorf("document.root: %w", err)
	}
	*r = DocumentResponse{
		Snapshot: wire.Snapshot,
		Document: explorer.DocumentView{
			Document: documentValue, Revision: revisionValue,
			SourceURI: wire.Document.SourceURI, Root: root,
		},
	}
	return nil
}

func (r RetrievalResponse) MarshalJSON() ([]byte, error) {
	results := make([]any, 0, len(r.Retrieval.Results))
	for _, result := range r.Retrieval.Results {
		evidence := make([]any, 0, len(result.Evidence))
		for _, item := range result.Evidence {
			evidence = append(evidence, struct {
				Citation any         `json:"citation"`
				Quote    string      `json:"quote"`
				Path     wirePath    `json:"path"`
				Score    shoal.Score `json:"score"`
			}{
				Citation: wireCitationValue(item.Citation), Quote: item.Quote,
				Path: wirePathValue(item.Path), Score: item.Score,
			})
		}
		results = append(results, struct {
			ID          string           `json:"id"`
			Score       shoal.Score      `json:"score"`
			Evidence    []any            `json:"evidence"`
			Explanation *wireExplanation `json:"explanation,omitempty"`
		}{
			encodeID(result.ID), result.Score, evidence,
			wireExplanationValue(result.Explanation),
		})
	}
	return json.Marshal(struct {
		Snapshot  Snapshot `json:"snapshot"`
		Retrieval any      `json:"retrieval"`
	}{r.Snapshot, struct {
		RequestID string `json:"request_id,omitempty"`
		Results   []any  `json:"results"`
	}{encodeOptionalID(r.Retrieval.RequestID), results}})
}

func (r *RetrievalResponse) UnmarshalJSON(data []byte) error {
	var wire struct {
		Snapshot  Snapshot `json:"snapshot"`
		Retrieval struct {
			RequestID string `json:"request_id,omitempty"`
			Results   []struct {
				ID       string      `json:"id"`
				Score    shoal.Score `json:"score"`
				Evidence []struct {
					Citation wireCitation `json:"citation"`
					Quote    string       `json:"quote"`
					Path     wirePath     `json:"path"`
					Score    shoal.Score  `json:"score"`
				} `json:"evidence"`
				Explanation *wireExplanation `json:"explanation,omitempty"`
			} `json:"results"`
		} `json:"retrieval"`
	}
	if err := strictUnmarshal(data, &wire); err != nil {
		return err
	}
	requestID, err := decodeOptionalID(wire.Retrieval.RequestID)
	if err != nil {
		return fmt.Errorf("retrieval.request_id: %w", err)
	}
	results := make([]retrieval.Result, 0, len(wire.Retrieval.Results))
	for _, item := range wire.Retrieval.Results {
		id, err := decodeID(item.ID)
		if err != nil {
			return fmt.Errorf("retrieval.results.id: %w", err)
		}
		evidence := make([]retrieval.Evidence, 0, len(item.Evidence))
		for _, evidenceItem := range item.Evidence {
			citation, err := citationValue(evidenceItem.Citation)
			if err != nil {
				return fmt.Errorf("retrieval.results.evidence.citation: %w", err)
			}
			path, err := pathValue(evidenceItem.Path)
			if err != nil {
				return fmt.Errorf("retrieval.results.evidence.path: %w", err)
			}
			evidence = append(evidence, retrieval.Evidence{
				Citation: citation, Quote: evidenceItem.Quote,
				Path: path, Score: evidenceItem.Score,
			})
		}
		results = append(results, retrieval.Result{
			ID: id, Score: item.Score, Evidence: evidence,
			Explanation: explanationValue(item.Explanation),
		})
	}
	*r = RetrievalResponse{
		Snapshot:  wire.Snapshot,
		Retrieval: retrieval.Response{RequestID: requestID, Results: results},
	}
	return nil
}

func (r NeighborhoodResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Snapshot     Snapshot `json:"snapshot"`
		Neighborhood any      `json:"neighborhood"`
		Truncated    bool     `json:"truncated"`
		NextCursor   string   `json:"next_cursor,omitempty"`
	}{r.Snapshot, wireNeighborhoodValue(r.Neighborhood), r.Truncated, r.NextCursor})
}

func (r *NeighborhoodResponse) UnmarshalJSON(data []byte) error {
	var wire struct {
		Snapshot     Snapshot         `json:"snapshot"`
		Neighborhood wireNeighborhood `json:"neighborhood"`
		Truncated    bool             `json:"truncated"`
		NextCursor   string           `json:"next_cursor,omitempty"`
	}
	if err := strictUnmarshal(data, &wire); err != nil {
		return err
	}
	neighborhood, err := neighborhoodValue(wire.Neighborhood)
	if err != nil {
		return err
	}
	*r = NeighborhoodResponse{
		Snapshot: wire.Snapshot, Neighborhood: neighborhood,
		Truncated: wire.Truncated, NextCursor: wire.NextCursor,
	}
	return nil
}

func (r PathResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Snapshot Snapshot `json:"snapshot"`
		Path     wirePath `json:"path"`
	}{r.Snapshot, wirePathValue(r.Path)})
}

func (r *PathResponse) UnmarshalJSON(data []byte) error {
	var wire struct {
		Snapshot Snapshot `json:"snapshot"`
		Path     wirePath `json:"path"`
	}
	if err := strictUnmarshal(data, &wire); err != nil {
		return err
	}
	path, err := pathValue(wire.Path)
	if err != nil {
		return err
	}
	*r = PathResponse{Snapshot: wire.Snapshot, Path: path}
	return nil
}

func (r *DocumentRequest) UnmarshalJSON(data []byte) error {
	var wire struct {
		Snapshot   Snapshot `json:"snapshot"`
		DocumentID string   `json:"document_id"`
		RevisionID string   `json:"revision_id,omitempty"`
	}
	if err := strictUnmarshal(data, &wire); err != nil {
		return err
	}
	documentID, err := decodeID(wire.DocumentID)
	if err != nil {
		return fmt.Errorf("document_id: %w", err)
	}
	revisionID, err := decodeOptionalID(wire.RevisionID)
	if err != nil {
		return fmt.Errorf("revision_id: %w", err)
	}
	*r = DocumentRequest{
		Snapshot: wire.Snapshot, DocumentID: documentID, RevisionID: revisionID,
	}
	return nil
}

func (r *NeighborhoodRequest) UnmarshalJSON(data []byte) error {
	var wire struct {
		Snapshot  Snapshot `json:"snapshot"`
		NodeIDs   []string `json:"node_ids"`
		Depth     uint32   `json:"depth,omitempty"`
		Fanout    uint32   `json:"fanout,omitempty"`
		MaxNodes  uint32   `json:"max_nodes,omitempty"`
		EdgeTypes []string `json:"edge_types,omitempty"`
		Cursor    string   `json:"cursor,omitempty"`
	}
	if err := strictUnmarshal(data, &wire); err != nil {
		return err
	}
	nodeIDs, err := decodeIDs(wire.NodeIDs)
	if err != nil {
		return fmt.Errorf("node_ids: %w", err)
	}
	*r = NeighborhoodRequest{
		Snapshot: wire.Snapshot, NodeIDs: nodeIDs, Depth: wire.Depth,
		Fanout: wire.Fanout, MaxNodes: wire.MaxNodes, EdgeTypes: wire.EdgeTypes,
		Cursor: wire.Cursor,
	}
	return nil
}

func (r *PathRequest) UnmarshalJSON(data []byte) error {
	var wire struct {
		Snapshot  Snapshot `json:"snapshot"`
		From      string   `json:"from"`
		To        string   `json:"to"`
		MaxDepth  uint32   `json:"max_depth,omitempty"`
		Fanout    uint32   `json:"fanout,omitempty"`
		EdgeTypes []string `json:"edge_types,omitempty"`
	}
	if err := strictUnmarshal(data, &wire); err != nil {
		return err
	}
	from, err := decodeID(wire.From)
	if err != nil {
		return fmt.Errorf("from: %w", err)
	}
	to, err := decodeID(wire.To)
	if err != nil {
		return fmt.Errorf("to: %w", err)
	}
	*r = PathRequest{
		Snapshot: wire.Snapshot, From: from, To: to, MaxDepth: wire.MaxDepth,
		Fanout: wire.Fanout, EdgeTypes: wire.EdgeTypes,
	}
	return nil
}

func (r *RetrievalRequest) UnmarshalJSON(data []byte) error {
	var wire struct {
		Snapshot Snapshot `json:"snapshot"`
		Query    struct {
			Text  string           `json:"text"`
			TopK  uint32           `json:"top_k,omitempty"`
			Modes []retrieval.Mode `json:"modes,omitempty"`
			Scope struct {
				DocumentIDs []string `json:"document_ids,omitempty"`
				NodeIDs     []string `json:"node_ids,omitempty"`
			} `json:"scope,omitempty"`
			AsOf    time.Time `json:"as_of,omitempty"`
			Explain bool      `json:"explain,omitempty"`
		} `json:"query"`
	}
	if err := strictUnmarshal(data, &wire); err != nil {
		return err
	}
	documentIDs, err := decodeIDs(wire.Query.Scope.DocumentIDs)
	if err != nil {
		return fmt.Errorf("query.scope.document_ids: %w", err)
	}
	nodeIDs, err := decodeIDs(wire.Query.Scope.NodeIDs)
	if err != nil {
		return fmt.Errorf("query.scope.node_ids: %w", err)
	}
	*r = RetrievalRequest{
		Snapshot: wire.Snapshot,
		Query: retrieval.Request{
			Text: wire.Query.Text, TopK: wire.Query.TopK, Modes: wire.Query.Modes,
			Scope: retrieval.Scope{DocumentIDs: documentIDs, NodeIDs: nodeIDs},
			AsOf:  wire.Query.AsOf, Explain: wire.Query.Explain,
		},
	}
	return nil
}

func wireDocumentValue(value document.Document) wireDocument {
	return wireDocument{
		ID: encodeID(value.ID), RevisionID: encodeID(value.RevisionID),
		Title: value.Title, RootSectionID: encodeID(value.RootSectionID),
		Metadata: wireMetadataValue(value.Metadata),
	}
}

func wireRevisionValue(value document.Revision) wireRevision {
	return wireRevision{
		ID: encodeID(value.ID), DocumentID: encodeID(value.DocumentID),
		CreatedAt: value.CreatedAt, SourceVersion: value.SourceVersion,
		Metadata: wireMetadataValue(value.Metadata),
	}
}

func wireSectionViewValue(value explorer.SectionView) wireSectionView {
	view := wireSectionView{
		Section:  wireSectionValue(value.Section),
		Spans:    make([]wireSpan, 0, len(value.Spans)),
		Children: make([]wireSectionView, 0, len(value.Children)),
	}
	for _, span := range value.Spans {
		view.Spans = append(view.Spans, wireSpanValue(span))
	}
	for _, child := range value.Children {
		view.Children = append(view.Children, wireSectionViewValue(child))
	}
	return view
}

func wireSectionValue(value document.Section) wireSection {
	return wireSection{
		ID: encodeID(value.ID), DocumentID: encodeID(value.DocumentID),
		RevisionID: encodeID(value.RevisionID), ParentID: encodeOptionalID(value.ParentID),
		Order: value.Order, Heading: value.Heading,
		Range: wireRangeValue(value.Range), Metadata: wireMetadataValue(value.Metadata),
	}
}

func wireSpanValue(value document.Span) wireSpan {
	return wireSpan{
		ID: encodeID(value.ID), DocumentID: encodeID(value.DocumentID),
		RevisionID: encodeID(value.RevisionID), SectionID: encodeID(value.SectionID),
		Order: value.Order, Range: wireRangeValue(value.Range),
		Text: value.Text, Metadata: wireMetadataValue(value.Metadata),
	}
}

func wireNeighborhoodValue(value explorer.Neighborhood) wireNeighborhood {
	nodes := make([]wireNode, 0, len(value.Nodes))
	edges := make([]wireEdge, 0, len(value.Edges))
	for _, node := range value.Nodes {
		nodes = append(nodes, wireNodeValue(node))
	}
	for _, edge := range value.Edges {
		edges = append(edges, wireEdgeValue(edge))
	}
	return wireNeighborhood{Nodes: nodes, Edges: edges}
}

func wirePathValue(value graph.Path) wirePath {
	nodes := make([]wireNode, 0, len(value.Nodes))
	edges := make([]wireEdge, 0, len(value.Edges))
	for _, node := range value.Nodes {
		nodes = append(nodes, wireNodeValue(node))
	}
	for _, edge := range value.Edges {
		edges = append(edges, wireEdgeValue(edge))
	}
	return wirePath{Nodes: nodes, Edges: edges}
}

func wireNodeValue(value graph.Node) wireNode {
	return wireNode{
		ID: encodeID(value.ID), Kind: value.Kind,
		Labels: value.Labels, Properties: wireMetadataValue(value.Properties),
	}
}

func wireEdgeValue(value graph.Edge) wireEdge {
	return wireEdge{
		ID: encodeID(value.ID), From: encodeID(value.From), To: encodeID(value.To),
		Type: value.Type, Weight: value.Weight,
		Properties: wireMetadataValue(value.Properties),
	}
}

func wireCitationValue(value document.Citation) wireCitation {
	return wireCitation{
		DocumentID: encodeID(value.DocumentID), RevisionID: encodeID(value.RevisionID),
		SectionID: encodeOptionalID(value.SectionID),
		SpanID:    encodeOptionalID(value.SpanID), Range: wireRangeValue(value.Range),
	}
}

func wireRangeValue(value document.SourceRange) wireRange {
	return wireRange{
		Start: wirePosition{Offset: value.Start.Offset, Page: value.Start.Page},
		End:   wirePosition{Offset: value.End.Offset, Page: value.End.Page},
	}
}

func wireExplanationValue(value *retrieval.Explanation) *wireExplanation {
	if value == nil {
		return nil
	}
	return &wireExplanation{
		Modes: value.Modes, Summary: value.Summary, Scores: value.Scores,
	}
}

func wireMetadataValue(value shoal.Metadata) wireMetadata {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	metadata := make(wireMetadata, 0, len(keys))
	for _, key := range keys {
		metadata = append(metadata, wireMetadataEntry{
			Key:   base64.RawURLEncoding.EncodeToString([]byte(key)),
			Value: base64.RawURLEncoding.EncodeToString([]byte(value[key])),
		})
	}
	return metadata
}

func documentValue(value wireDocument) (document.Document, error) {
	id, err := decodeID(value.ID)
	if err != nil {
		return document.Document{}, fmt.Errorf("id: %w", err)
	}
	revisionID, err := decodeID(value.RevisionID)
	if err != nil {
		return document.Document{}, fmt.Errorf("revision_id: %w", err)
	}
	rootSectionID, err := decodeID(value.RootSectionID)
	if err != nil {
		return document.Document{}, fmt.Errorf("root_section_id: %w", err)
	}
	metadata, err := metadataValue(value.Metadata)
	if err != nil {
		return document.Document{}, fmt.Errorf("metadata: %w", err)
	}
	return document.Document{
		ID: id, RevisionID: revisionID, Title: value.Title,
		RootSectionID: rootSectionID, Metadata: metadata,
	}, nil
}

func revisionValue(value wireRevision) (document.Revision, error) {
	id, err := decodeID(value.ID)
	if err != nil {
		return document.Revision{}, fmt.Errorf("id: %w", err)
	}
	documentID, err := decodeID(value.DocumentID)
	if err != nil {
		return document.Revision{}, fmt.Errorf("document_id: %w", err)
	}
	metadata, err := metadataValue(value.Metadata)
	if err != nil {
		return document.Revision{}, fmt.Errorf("metadata: %w", err)
	}
	return document.Revision{
		ID: id, DocumentID: documentID, CreatedAt: value.CreatedAt,
		SourceVersion: value.SourceVersion, Metadata: metadata,
	}, nil
}

func sectionViewValue(value wireSectionView) (explorer.SectionView, error) {
	section, err := sectionValue(value.Section)
	if err != nil {
		return explorer.SectionView{}, fmt.Errorf("section: %w", err)
	}
	spans := make([]document.Span, 0, len(value.Spans))
	for _, item := range value.Spans {
		span, err := spanValue(item)
		if err != nil {
			return explorer.SectionView{}, fmt.Errorf("spans: %w", err)
		}
		spans = append(spans, span)
	}
	children := make([]explorer.SectionView, 0, len(value.Children))
	for _, item := range value.Children {
		child, err := sectionViewValue(item)
		if err != nil {
			return explorer.SectionView{}, fmt.Errorf("children: %w", err)
		}
		children = append(children, child)
	}
	return explorer.SectionView{Section: section, Spans: spans, Children: children}, nil
}

func sectionValue(value wireSection) (document.Section, error) {
	id, err := decodeID(value.ID)
	if err != nil {
		return document.Section{}, fmt.Errorf("id: %w", err)
	}
	documentID, err := decodeID(value.DocumentID)
	if err != nil {
		return document.Section{}, fmt.Errorf("document_id: %w", err)
	}
	revisionID, err := decodeID(value.RevisionID)
	if err != nil {
		return document.Section{}, fmt.Errorf("revision_id: %w", err)
	}
	parentID, err := decodeOptionalID(value.ParentID)
	if err != nil {
		return document.Section{}, fmt.Errorf("parent_id: %w", err)
	}
	metadata, err := metadataValue(value.Metadata)
	if err != nil {
		return document.Section{}, fmt.Errorf("metadata: %w", err)
	}
	return document.Section{
		ID: id, DocumentID: documentID, RevisionID: revisionID,
		ParentID: parentID, Order: value.Order, Heading: value.Heading,
		Range: sourceRangeValue(value.Range), Metadata: metadata,
	}, nil
}

func spanValue(value wireSpan) (document.Span, error) {
	id, err := decodeID(value.ID)
	if err != nil {
		return document.Span{}, fmt.Errorf("id: %w", err)
	}
	documentID, err := decodeID(value.DocumentID)
	if err != nil {
		return document.Span{}, fmt.Errorf("document_id: %w", err)
	}
	revisionID, err := decodeID(value.RevisionID)
	if err != nil {
		return document.Span{}, fmt.Errorf("revision_id: %w", err)
	}
	sectionID, err := decodeID(value.SectionID)
	if err != nil {
		return document.Span{}, fmt.Errorf("section_id: %w", err)
	}
	metadata, err := metadataValue(value.Metadata)
	if err != nil {
		return document.Span{}, fmt.Errorf("metadata: %w", err)
	}
	return document.Span{
		ID: id, DocumentID: documentID, RevisionID: revisionID,
		SectionID: sectionID, Order: value.Order, Range: sourceRangeValue(value.Range),
		Text: value.Text, Metadata: metadata,
	}, nil
}

func neighborhoodValue(value wireNeighborhood) (explorer.Neighborhood, error) {
	nodes := make([]graph.Node, 0, len(value.Nodes))
	for _, item := range value.Nodes {
		node, err := nodeValue(item)
		if err != nil {
			return explorer.Neighborhood{}, fmt.Errorf("nodes: %w", err)
		}
		nodes = append(nodes, node)
	}
	edges := make([]graph.Edge, 0, len(value.Edges))
	for _, item := range value.Edges {
		edge, err := edgeValue(item)
		if err != nil {
			return explorer.Neighborhood{}, fmt.Errorf("edges: %w", err)
		}
		edges = append(edges, edge)
	}
	return explorer.Neighborhood{Nodes: nodes, Edges: edges}, nil
}

func pathValue(value wirePath) (graph.Path, error) {
	nodes := make([]graph.Node, 0, len(value.Nodes))
	for _, item := range value.Nodes {
		node, err := nodeValue(item)
		if err != nil {
			return graph.Path{}, fmt.Errorf("nodes: %w", err)
		}
		nodes = append(nodes, node)
	}
	edges := make([]graph.Edge, 0, len(value.Edges))
	for _, item := range value.Edges {
		edge, err := edgeValue(item)
		if err != nil {
			return graph.Path{}, fmt.Errorf("edges: %w", err)
		}
		edges = append(edges, edge)
	}
	return graph.Path{Nodes: nodes, Edges: edges}, nil
}

func nodeValue(value wireNode) (graph.Node, error) {
	id, err := decodeID(value.ID)
	if err != nil {
		return graph.Node{}, fmt.Errorf("id: %w", err)
	}
	metadata, err := metadataValue(value.Properties)
	if err != nil {
		return graph.Node{}, fmt.Errorf("properties: %w", err)
	}
	return graph.Node{
		ID: id, Kind: value.Kind, Labels: value.Labels, Properties: metadata,
	}, nil
}

func edgeValue(value wireEdge) (graph.Edge, error) {
	id, err := decodeID(value.ID)
	if err != nil {
		return graph.Edge{}, fmt.Errorf("id: %w", err)
	}
	from, err := decodeID(value.From)
	if err != nil {
		return graph.Edge{}, fmt.Errorf("from: %w", err)
	}
	to, err := decodeID(value.To)
	if err != nil {
		return graph.Edge{}, fmt.Errorf("to: %w", err)
	}
	metadata, err := metadataValue(value.Properties)
	if err != nil {
		return graph.Edge{}, fmt.Errorf("properties: %w", err)
	}
	return graph.Edge{
		ID: id, From: from, To: to, Type: value.Type,
		Weight: value.Weight, Properties: metadata,
	}, nil
}

func citationValue(value wireCitation) (document.Citation, error) {
	documentID, err := decodeID(value.DocumentID)
	if err != nil {
		return document.Citation{}, fmt.Errorf("document_id: %w", err)
	}
	revisionID, err := decodeID(value.RevisionID)
	if err != nil {
		return document.Citation{}, fmt.Errorf("revision_id: %w", err)
	}
	sectionID, err := decodeOptionalID(value.SectionID)
	if err != nil {
		return document.Citation{}, fmt.Errorf("section_id: %w", err)
	}
	spanID, err := decodeOptionalID(value.SpanID)
	if err != nil {
		return document.Citation{}, fmt.Errorf("span_id: %w", err)
	}
	return document.Citation{
		DocumentID: documentID, RevisionID: revisionID,
		SectionID: sectionID, SpanID: spanID,
		Range: sourceRangeValue(value.Range),
	}, nil
}

func sourceRangeValue(value wireRange) document.SourceRange {
	return document.SourceRange{
		Start: document.SourcePosition{
			Offset: value.Start.Offset, Page: value.Start.Page,
		},
		End: document.SourcePosition{
			Offset: value.End.Offset, Page: value.End.Page,
		},
	}
}

func explanationValue(value *wireExplanation) *retrieval.Explanation {
	if value == nil {
		return nil
	}
	return &retrieval.Explanation{
		Modes: value.Modes, Summary: value.Summary, Scores: value.Scores,
	}
}

func metadataValue(value wireMetadata) (shoal.Metadata, error) {
	if len(value) == 0 {
		return nil, nil
	}
	metadata := make(shoal.Metadata, len(value))
	for _, item := range value {
		key, err := base64.RawURLEncoding.DecodeString(item.Key)
		if err != nil {
			return nil, fmt.Errorf("key must be unpadded base64url")
		}
		decodedValue, err := base64.RawURLEncoding.DecodeString(item.Value)
		if err != nil {
			return nil, fmt.Errorf("value must be unpadded base64url")
		}
		if _, duplicate := metadata[string(key)]; duplicate {
			return nil, fmt.Errorf("metadata contains duplicate keys")
		}
		metadata[string(key)] = string(decodedValue)
	}
	return metadata, nil
}

func encodeID(value shoal.ID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func encodeOptionalID(value shoal.ID) string {
	if value == "" {
		return ""
	}
	return encodeID(value)
}

func decodeID(value string) (shoal.ID, error) {
	if value == "" {
		return "", fmt.Errorf("is required")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", fmt.Errorf("must be unpadded base64url")
	}
	return shoal.ID(decoded), nil
}

func decodeOptionalID(value string) (shoal.ID, error) {
	if value == "" {
		return "", nil
	}
	return decodeID(value)
}

func decodeIDs(values []string) ([]shoal.ID, error) {
	result := make([]shoal.ID, 0, len(values))
	for _, value := range values {
		id, err := decodeID(value)
		if err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, nil
}

func strictUnmarshal(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(value)
}
