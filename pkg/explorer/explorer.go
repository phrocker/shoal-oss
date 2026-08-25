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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/internal/cclient"
	"github.com/phrocker/shoal-oss/internal/engine"
	"github.com/phrocker/shoal-oss/internal/iterrt"
	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	explorerTable = "_shoal_explorer"
	recordCF      = "record"
	recordCQ      = "v1"
	documentRow   = "document/"
	edgeRow       = "edge/"
)

// Explorer is a durable embedded implementation of the Explorer and Retriever
// contracts. Its API contains no engine, cell, or storage-format types.
type Explorer struct {
	mu        sync.RWMutex
	engine    *engine.Engine
	documents map[shoal.ID]map[shoal.ID]*persistedDocument
	edges     map[shoal.ID]graph.Edge
	closed    bool
}

type persistedDocument struct {
	Document document.Document
	Revision document.Revision
	Source   Source
	Sections []document.Section
	Spans    []document.Span
	Nodes    []graph.Node
	Edges    []graph.Edge
}

type persistedEdge struct {
	Edge graph.Edge
}

// Open opens or creates a local Explorer corpus rooted at dir.
func Open(dir string) (*Explorer, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "data directory is required")
	}
	eng, err := engine.Open(dir, engine.Options{})
	if err != nil {
		return nil, shoal.WrapError(shoal.ErrorUnavailable, "open explorer storage", err)
	}
	found := false
	for _, table := range eng.TableNames() {
		if table == explorerTable {
			found = true
			break
		}
	}
	if !found {
		if err := eng.CreateTable(explorerTable, engine.TableOptions{}); err != nil {
			_ = eng.Close()
			return nil, shoal.WrapError(shoal.ErrorInternal, "create explorer table", err)
		}
	}
	explorer := &Explorer{
		engine:    eng,
		documents: make(map[shoal.ID]map[shoal.ID]*persistedDocument),
		edges:     make(map[shoal.ID]graph.Edge),
	}
	if err := explorer.load(); err != nil {
		_ = eng.Close()
		return nil, err
	}
	return explorer, nil
}

// Close flushes and closes the local corpus.
func (e *Explorer) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	if err := e.engine.Close(); err != nil {
		return shoal.WrapError(shoal.ErrorInternal, "close explorer storage", err)
	}
	return nil
}

// Ingest parses and durably stores one immutable text or Markdown revision.
func (e *Explorer) Ingest(ctx context.Context, source Source) (IngestResult, error) {
	if err := contextError(ctx); err != nil {
		return IngestResult{}, err
	}
	parsed, err := parseSource(source, time.Now())
	if err != nil {
		return IngestResult{}, err
	}
	record := &persistedDocument{
		Document: parsed.document,
		Revision: parsed.revision,
		Source:   parsed.source,
		Sections: parsed.sections,
		Spans:    parsed.spans,
		Nodes:    parsed.nodes,
		Edges:    parsed.edges,
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.requireOpen(); err != nil {
		return IngestResult{}, err
	}
	if revisions := e.documents[record.Document.ID]; revisions != nil {
		if existing := revisions[record.Revision.ID]; existing != nil {
			return ingestResult(existing, IngestUnchanged), nil
		}
	}
	if err := e.writeJSON(
		documentRow+string(record.Document.ID)+"/"+string(record.Revision.ID), record); err != nil {
		return IngestResult{}, err
	}
	if e.documents[record.Document.ID] == nil {
		e.documents[record.Document.ID] = make(map[shoal.ID]*persistedDocument)
	}
	e.documents[record.Document.ID][record.Revision.ID] = record
	return ingestResult(record, IngestApplied), nil
}

// Documents lists the newest revision of every document.
func (e *Explorer) Documents(ctx context.Context) ([]DocumentSummary, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.requireOpen(); err != nil {
		return nil, err
	}
	summaries := make([]DocumentSummary, 0, len(e.documents))
	for _, revisions := range e.documents {
		record := latestRevision(revisions, time.Time{})
		if record == nil {
			continue
		}
		summaries = append(summaries, DocumentSummary{
			Document:  cloneDocument(record.Document),
			Revision:  cloneRevision(record.Revision),
			SourceURI: record.Source.URI,
		})
	}
	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].Document.Title == summaries[j].Document.Title {
			return summaries[i].Document.ID < summaries[j].Document.ID
		}
		return summaries[i].Document.Title < summaries[j].Document.Title
	})
	return summaries, nil
}

// Document returns a navigable document revision. An empty revisionID selects
// the newest revision.
func (e *Explorer) Document(
	ctx context.Context, documentID, revisionID shoal.ID,
) (DocumentView, error) {
	if err := contextError(ctx); err != nil {
		return DocumentView{}, err
	}
	if documentID == "" {
		return DocumentView{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "document ID is required")
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.requireOpen(); err != nil {
		return DocumentView{}, err
	}
	revisions := e.documents[documentID]
	var record *persistedDocument
	if revisionID == "" {
		record = latestRevision(revisions, time.Time{})
	} else {
		record = revisions[revisionID]
	}
	if record == nil {
		return DocumentView{}, shoal.NewError(shoal.ErrorNotFound, "document revision not found")
	}
	root, err := buildSectionView(record)
	if err != nil {
		return DocumentView{}, err
	}
	return DocumentView{
		Document:  cloneDocument(record.Document),
		Revision:  cloneRevision(record.Revision),
		SourceURI: record.Source.URI,
		Root:      root,
	}, nil
}

// Connect adds an application-defined edge between existing document,
// section, or span nodes.
func (e *Explorer) Connect(ctx context.Context, edge graph.Edge) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := validateEdge(edge); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.requireOpen(); err != nil {
		return err
	}
	nodes, allEdges := e.currentGraph(time.Time{})
	if _, ok := nodes[edge.From]; !ok {
		return shoal.NewError(shoal.ErrorNotFound, "edge source node not found")
	}
	if _, ok := nodes[edge.To]; !ok {
		return shoal.NewError(shoal.ErrorNotFound, "edge target node not found")
	}
	if existing, ok := allEdges[edge.ID]; ok {
		if edgesEqual(existing, edge) {
			return nil
		}
		return shoal.NewError(shoal.ErrorConflict, "edge ID already has different content")
	}
	if err := e.writeJSON(edgeRow+string(edge.ID), persistedEdge{Edge: edge}); err != nil {
		return err
	}
	e.edges[edge.ID] = cloneEdge(edge)
	return nil
}

// Neighborhood expands both incoming and outgoing graph relationships.
func (e *Explorer) Neighborhood(
	ctx context.Context, request NeighborhoodRequest,
) (Neighborhood, error) {
	if err := contextError(ctx); err != nil {
		return Neighborhood{}, err
	}
	if len(request.NodeIDs) == 0 {
		return Neighborhood{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "at least one graph node ID is required")
	}
	if request.Depth == 0 {
		request.Depth = 1
	}
	if request.Depth > 16 {
		return Neighborhood{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "graph depth cannot exceed 16")
	}
	typeFilter := make(map[string]struct{}, len(request.EdgeTypes))
	for _, edgeType := range request.EdgeTypes {
		if !utf8.ValidString(edgeType) || strings.TrimSpace(edgeType) == "" {
			return Neighborhood{}, shoal.NewError(
				shoal.ErrorInvalidArgument, "edge types must be non-empty UTF-8")
		}
		typeFilter[edgeType] = struct{}{}
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.requireOpen(); err != nil {
		return Neighborhood{}, err
	}
	nodes, edges := e.currentGraph(time.Time{})
	seen := make(map[shoal.ID]struct{})
	frontier := make(map[shoal.ID]struct{})
	for _, id := range request.NodeIDs {
		if _, ok := nodes[id]; !ok {
			return Neighborhood{}, shoal.NewError(shoal.ErrorNotFound, "graph node not found")
		}
		seen[id] = struct{}{}
		frontier[id] = struct{}{}
	}
	selectedEdges := make(map[shoal.ID]graph.Edge)
	for depth := uint32(0); depth < request.Depth && len(frontier) > 0; depth++ {
		next := make(map[shoal.ID]struct{})
		for _, edge := range edges {
			if len(typeFilter) > 0 {
				if _, ok := typeFilter[edge.Type]; !ok {
					continue
				}
			}
			_, from := frontier[edge.From]
			_, to := frontier[edge.To]
			if !from && !to {
				continue
			}
			selectedEdges[edge.ID] = edge
			for _, id := range []shoal.ID{edge.From, edge.To} {
				if _, exists := seen[id]; !exists {
					seen[id] = struct{}{}
					next[id] = struct{}{}
				}
			}
		}
		frontier = next
	}
	result := Neighborhood{
		Nodes: make([]graph.Node, 0, len(seen)),
		Edges: make([]graph.Edge, 0, len(selectedEdges)),
	}
	for id := range seen {
		result.Nodes = append(result.Nodes, cloneNode(nodes[id]))
	}
	for _, edge := range selectedEdges {
		result.Edges = append(result.Edges, cloneEdge(edge))
	}
	sort.Slice(result.Nodes, func(i, j int) bool { return result.Nodes[i].ID < result.Nodes[j].ID })
	sort.Slice(result.Edges, func(i, j int) bool { return result.Edges[i].ID < result.Edges[j].ID })
	return result, nil
}

func (e *Explorer) load() error {
	scanner, err := e.engine.Scan(explorerTable, iterrt.InfiniteRange(), engine.ScanOptions{
		ColumnFamilies: [][]byte{[]byte(recordCF)}, ColumnFamiliesInclusive: true,
	})
	if err != nil {
		return shoal.WrapError(shoal.ErrorInternal, "scan explorer records", err)
	}
	defer scanner.Close()
	for scanner.Next() {
		key := scanner.Key()
		if !bytes.Equal(key.ColumnQualifier, []byte(recordCQ)) {
			if err := scanner.Advance(); err != nil {
				return shoal.WrapError(shoal.ErrorInternal, "advance explorer scan", err)
			}
			continue
		}
		row := string(key.Row)
		switch {
		case strings.HasPrefix(row, documentRow):
			var record persistedDocument
			if err := json.Unmarshal(scanner.Value(), &record); err != nil {
				return shoal.WrapError(shoal.ErrorInternal, "decode explorer document", err)
			}
			if record.Document.ID == "" || record.Revision.ID == "" {
				return shoal.NewError(shoal.ErrorInternal, "stored explorer document is incomplete")
			}
			if e.documents[record.Document.ID] == nil {
				e.documents[record.Document.ID] = make(map[shoal.ID]*persistedDocument)
			}
			copy := record
			e.documents[record.Document.ID][record.Revision.ID] = &copy
		case strings.HasPrefix(row, edgeRow):
			var record persistedEdge
			if err := json.Unmarshal(scanner.Value(), &record); err != nil {
				return shoal.WrapError(shoal.ErrorInternal, "decode explorer edge", err)
			}
			if err := validateEdge(record.Edge); err != nil {
				return shoal.WrapError(shoal.ErrorInternal, "stored explorer edge is invalid", err)
			}
			e.edges[record.Edge.ID] = record.Edge
		}
		if err := scanner.Advance(); err != nil {
			return shoal.WrapError(shoal.ErrorInternal, "advance explorer scan", err)
		}
	}
	return nil
}

func (e *Explorer) writeJSON(row string, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return shoal.WrapError(shoal.ErrorInternal, "encode explorer record", err)
	}
	mutation, err := cclient.NewMutation([]byte(row))
	if err != nil {
		return shoal.WrapError(shoal.ErrorInternal, "create explorer mutation", err)
	}
	mutation.PutLatest([]byte(recordCF), []byte(recordCQ), nil, encoded)
	if err := e.engine.Write(explorerTable, []*cclient.Mutation{mutation}); err != nil {
		return shoal.WrapError(shoal.ErrorUnavailable, "write explorer record", err)
	}
	return nil
}

func (e *Explorer) requireOpen() error {
	if e.closed {
		return shoal.NewError(shoal.ErrorUnavailable, "explorer is closed")
	}
	return nil
}

func (e *Explorer) currentGraph(
	asOf time.Time,
) (map[shoal.ID]graph.Node, map[shoal.ID]graph.Edge) {
	nodes := make(map[shoal.ID]graph.Node)
	edges := make(map[shoal.ID]graph.Edge)
	for _, revisions := range e.documents {
		record := latestRevision(revisions, asOf)
		if record == nil {
			continue
		}
		for _, node := range record.Nodes {
			nodes[node.ID] = node
		}
		for _, edge := range record.Edges {
			edges[edge.ID] = edge
		}
	}
	for id, edge := range e.edges {
		if _, from := nodes[edge.From]; !from {
			continue
		}
		if _, to := nodes[edge.To]; !to {
			continue
		}
		edges[id] = edge
	}
	return nodes, edges
}

func latestRevision(
	revisions map[shoal.ID]*persistedDocument, asOf time.Time,
) *persistedDocument {
	var latest *persistedDocument
	for _, record := range revisions {
		if !asOf.IsZero() && record.Revision.CreatedAt.After(asOf) {
			continue
		}
		if latest == nil || record.Revision.CreatedAt.After(latest.Revision.CreatedAt) ||
			(record.Revision.CreatedAt.Equal(latest.Revision.CreatedAt) &&
				record.Revision.ID > latest.Revision.ID) {
			latest = record
		}
	}
	return latest
}

func buildSectionView(record *persistedDocument) (SectionView, error) {
	sections := make(map[shoal.ID]document.Section, len(record.Sections))
	children := make(map[shoal.ID][]shoal.ID)
	spans := make(map[shoal.ID][]document.Span)
	for _, section := range record.Sections {
		if _, duplicate := sections[section.ID]; duplicate {
			return SectionView{}, shoal.NewError(
				shoal.ErrorInternal, "stored document has duplicate sections")
		}
		sections[section.ID] = section
		if section.ParentID != "" {
			children[section.ParentID] = append(children[section.ParentID], section.ID)
		}
	}
	for _, span := range record.Spans {
		if _, ok := sections[span.SectionID]; !ok {
			return SectionView{}, shoal.NewError(
				shoal.ErrorInternal, "stored span has no section")
		}
		spans[span.SectionID] = append(spans[span.SectionID], span)
	}
	visiting := make(map[shoal.ID]bool)
	var build func(shoal.ID) (SectionView, error)
	build = func(id shoal.ID) (SectionView, error) {
		section, ok := sections[id]
		if !ok {
			return SectionView{}, shoal.NewError(
				shoal.ErrorInternal, "stored section hierarchy is incomplete")
		}
		if visiting[id] {
			return SectionView{}, shoal.NewError(
				shoal.ErrorInternal, "stored section hierarchy contains a cycle")
		}
		visiting[id] = true
		view := SectionView{
			Section: cloneSection(section),
			Spans:   append([]document.Span(nil), spans[id]...),
		}
		for i := range view.Spans {
			view.Spans[i] = cloneSpan(view.Spans[i])
		}
		sort.Slice(view.Spans, func(i, j int) bool {
			return view.Spans[i].Order < view.Spans[j].Order
		})
		childIDs := append([]shoal.ID(nil), children[id]...)
		sort.Slice(childIDs, func(i, j int) bool {
			return sections[childIDs[i]].Order < sections[childIDs[j]].Order
		})
		for _, childID := range childIDs {
			child, err := build(childID)
			if err != nil {
				return SectionView{}, err
			}
			view.Children = append(view.Children, child)
		}
		delete(visiting, id)
		return view, nil
	}
	if _, ok := sections[record.Document.RootSectionID]; !ok {
		return SectionView{}, shoal.NewError(
			shoal.ErrorInternal, "stored document root is missing")
	}
	return build(record.Document.RootSectionID)
}

func ingestResult(record *persistedDocument, disposition IngestDisposition) IngestResult {
	return IngestResult{
		Disposition:  disposition,
		Document:     cloneDocument(record.Document),
		Revision:     cloneRevision(record.Revision),
		SectionCount: len(record.Sections),
		SpanCount:    len(record.Spans),
	}
}

func validateEdge(edge graph.Edge) error {
	if edge.ID == "" || edge.From == "" || edge.To == "" ||
		strings.TrimSpace(edge.Type) == "" {
		return shoal.NewError(shoal.ErrorInvalidArgument, "edge is structurally incomplete")
	}
	for _, value := range []string{
		string(edge.ID), string(edge.From), string(edge.To), edge.Type,
	} {
		if !utf8.ValidString(value) {
			return shoal.NewError(shoal.ErrorInvalidArgument, "edge values must be valid UTF-8")
		}
	}
	if math.IsNaN(float64(edge.Weight)) || math.IsInf(float64(edge.Weight), 0) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "edge weight must be finite")
	}
	if err := validateMetadata(edge.Properties); err != nil {
		return err
	}
	return nil
}

func edgesEqual(left, right graph.Edge) bool {
	if left.ID != right.ID || left.From != right.From || left.To != right.To ||
		left.Type != right.Type || left.Weight != right.Weight ||
		len(left.Properties) != len(right.Properties) {
		return false
	}
	for key, value := range left.Properties {
		if right.Properties[key] != value {
			return false
		}
	}
	return true
}

func cloneNode(node graph.Node) graph.Node {
	node.Labels = append([]string(nil), node.Labels...)
	node.Properties = cloneMetadata(node.Properties)
	return node
}

func cloneEdge(edge graph.Edge) graph.Edge {
	edge.Properties = cloneMetadata(edge.Properties)
	return edge
}

func cloneDocument(value document.Document) document.Document {
	value.Metadata = cloneMetadata(value.Metadata)
	return value
}

func cloneRevision(value document.Revision) document.Revision {
	value.Metadata = cloneMetadata(value.Metadata)
	return value
}

func cloneSection(value document.Section) document.Section {
	value.Metadata = cloneMetadata(value.Metadata)
	return value
}

func cloneSpan(value document.Span) document.Span {
	value.Metadata = cloneMetadata(value.Metadata)
	return value
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return shoal.NewError(shoal.ErrorInvalidArgument, "context is required")
	}
	switch err := ctx.Err(); {
	case errors.Is(err, context.Canceled):
		return shoal.WrapError(shoal.ErrorCanceled, "operation canceled", err)
	case errors.Is(err, context.DeadlineExceeded):
		return shoal.WrapError(shoal.ErrorDeadline, "operation deadline exceeded", err)
	default:
		return nil
	}
}

var _ Client = (*Explorer)(nil)
