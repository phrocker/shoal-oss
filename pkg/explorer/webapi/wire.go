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

func (r NeighborhoodResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Snapshot     Snapshot `json:"snapshot"`
		Neighborhood any      `json:"neighborhood"`
		Truncated    bool     `json:"truncated"`
		NextCursor   string   `json:"next_cursor,omitempty"`
	}{r.Snapshot, wireNeighborhoodValue(r.Neighborhood), r.Truncated, r.NextCursor})
}

func (r PathResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Snapshot Snapshot `json:"snapshot"`
		Path     wirePath `json:"path"`
	}{r.Snapshot, wirePathValue(r.Path)})
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

func wireNeighborhoodValue(value explorer.Neighborhood) any {
	nodes := make([]wireNode, 0, len(value.Nodes))
	edges := make([]wireEdge, 0, len(value.Edges))
	for _, node := range value.Nodes {
		nodes = append(nodes, wireNodeValue(node))
	}
	for _, edge := range value.Edges {
		edges = append(edges, wireEdgeValue(edge))
	}
	return struct {
		Nodes []wireNode `json:"nodes"`
		Edges []wireEdge `json:"edges"`
	}{nodes, edges}
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

func wireCitationValue(value document.Citation) any {
	return struct {
		DocumentID string    `json:"document_id"`
		RevisionID string    `json:"revision_id"`
		SectionID  string    `json:"section_id,omitempty"`
		SpanID     string    `json:"span_id,omitempty"`
		Range      wireRange `json:"range"`
	}{
		encodeID(value.DocumentID), encodeID(value.RevisionID),
		encodeOptionalID(value.SectionID), encodeOptionalID(value.SpanID),
		wireRangeValue(value.Range),
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
