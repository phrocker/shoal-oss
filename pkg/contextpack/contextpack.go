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

// Package contextpack constructs bounded, verified inference context from
// Explorer retrieval, document, and graph contracts. It performs no model or
// provider calls.
package contextpack

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	DefaultMaxResults         = 256
	DefaultMaxAnchors         = 1024
	DefaultMaxDocuments       = 256
	DefaultMaxSections        = 4096
	DefaultMaxGraphNodes      = 4096
	DefaultMaxGraphEdges      = 8192
	DefaultMaxPathNodes       = 256
	DefaultMaxContextBytes    = 4 * 1024 * 1024
	DefaultMaxQuoteBytes      = 1024 * 1024
	DefaultMaxProvenanceBytes = 64 * 1024
	DefaultMaxHierarchyDepth  = 16

	metadataBuilderKey   = "shoal.context.builder"
	metadataRequestKey   = "shoal.context.retrieval_request_id"
	metadataRetrievalKey = "shoal.context.retrieval_identity"
	metadataPolicyKey    = "shoal.context.policy_id"
	builderVersion       = "explorer-context/v1"
)

// Reader is the stable, authorization-enforcing Explorer seam needed for
// hydration. Implementations must return absent and unauthorized resources
// with the same externally observable semantics.
type Reader interface {
	Document(context.Context, shoal.ID, shoal.ID) (explorer.DocumentView, error)
	Neighborhood(context.Context, explorer.NeighborhoodRequest) (explorer.Neighborhood, error)
}

// Limits are fail-closed construction ceilings. Zero fields select defaults.
type Limits struct {
	MaxResults         int
	MaxAnchors         int
	MaxDocuments       int
	MaxSections        int
	MaxGraphNodes      int
	MaxGraphEdges      int
	MaxPathNodes       int
	MaxContextBytes    int
	MaxQuoteBytes      int
	MaxProvenanceBytes int
	MaxHierarchyDepth  uint32
}

// Pins are trusted snapshot, policy, authorization, and optional ontology
// identities supplied by the caller. The builder never derives authorization.
type Pins struct {
	Snapshot      inference.SnapshotPin
	Authorization inference.AuthPin
	PolicyID      shoal.ID
	Ontology      *inference.OntologyIdentity
}

// InitialRequest contains one retrieval operation and optional pre-hydrated
// public views. Missing views are fetched through Builder.Reader.
type InitialRequest struct {
	Request       retrieval.Request
	Response      retrieval.Response
	Documents     []explorer.DocumentView
	Neighborhoods []explorer.Neighborhood
	Selection     EvidenceSelection
	Pins          Pins
	Metadata      shoal.Metadata
}

// EvidenceSelection chooses which exact retrieval evidence variants enter the
// pack. A zero value includes both document citations and graph paths.
type EvidenceSelection struct {
	Documents bool
	Paths     bool
}

// OpenSectionRequest explicitly selects sections and a bounded descendant
// depth. Depth zero includes only the selected sections.
type OpenSectionRequest struct {
	DocumentID shoal.ID
	RevisionID shoal.ID
	SectionIDs []shoal.ID
	Depth      uint32
}

// ExpandNeighborsRequest explicitly selects graph seeds and a bounded
// neighborhood. No implicit crawling is performed.
type ExpandNeighborsRequest struct {
	NodeIDs   []shoal.ID
	Depth     uint32
	EdgeTypes []string
}

// Builder creates immutable inference packs.
type Builder struct {
	Reader Reader
	Limits Limits
}

// Build creates an initial context pack from exact retrieval evidence.
func (b Builder) Build(ctx context.Context, input InitialRequest) (inference.ContextPack, error) {
	if err := contextError(ctx); err != nil {
		return inference.ContextPack{}, err
	}
	limits, err := normalizeLimits(b.Limits)
	if err != nil {
		return inference.ContextPack{}, err
	}
	request, err := input.Request.Normalize()
	if err != nil {
		return inference.ContextPack{}, err
	}
	if !request.AsOf.IsZero() && !request.AsOf.Equal(input.Pins.Snapshot.AsOf()) {
		return inference.ContextPack{}, invalid(
			"retrieval as_of does not match the trusted snapshot pin")
	}
	response := cloneResponse(input.Response)
	retrieval.SortResults(&response)
	if err := response.ValidateFor(request); err != nil {
		return inference.ContextPack{}, fmt.Errorf("context retrieval response: %w", err)
	}
	if len(response.Results) == 0 {
		return inference.ContextPack{}, invalid("context retrieval returned no evidence")
	}
	if len(response.Results) > limits.MaxResults {
		return inference.ContextPack{}, invalid("context retrieval exceeds the result bound")
	}

	verifier, err := newVerifier(ctx, b.Reader, limits, input.Documents, input.Neighborhoods)
	if err != nil {
		return inference.ContextPack{}, err
	}
	selection := input.Selection
	if !selection.Documents && !selection.Paths {
		selection.Documents = true
		selection.Paths = true
	}
	anchors := make([]inference.EvidenceAnchor, 0)
	for _, result := range response.Results {
		for _, evidence := range result.Evidence {
			documentAnchor, err := verifier.documentAnchor(evidence.Citation, evidence.Quote)
			if err != nil {
				return inference.ContextPack{}, fmt.Errorf(
					"result %q document evidence: %w", result.ID, err)
			}
			if selection.Documents {
				anchors = append(anchors, documentAnchor)
			}
			if pathPresent(evidence.Path) {
				graphAnchor, err := verifier.graphAnchor(evidence.Path)
				if err != nil {
					return inference.ContextPack{}, fmt.Errorf(
						"result %q graph evidence: %w", result.ID, err)
				}
				if selection.Paths {
					anchors = append(anchors, graphAnchor)
				}
			}
		}
	}
	anchors, err = canonicalAnchors(anchors, limits.MaxAnchors)
	if err != nil {
		return inference.ContextPack{}, err
	}
	metadata, err := provenanceMetadata(
		input.Metadata, request, response, input.Pins.PolicyID, limits)
	if err != nil {
		return inference.ContextPack{}, err
	}
	return buildPack(request.Text, anchors, input.Pins, metadata, limits)
}

// OpenSection returns a new pack containing the existing verified evidence and
// exact span-backed citations from explicitly selected sections.
func (b Builder) OpenSection(
	ctx context.Context,
	pack inference.ContextPack,
	request OpenSectionRequest,
) (inference.ContextPack, error) {
	if err := contextError(ctx); err != nil {
		return inference.ContextPack{}, err
	}
	if err := pack.Validate(); err != nil {
		return inference.ContextPack{}, err
	}
	limits, err := normalizeLimits(b.Limits)
	if err != nil {
		return inference.ContextPack{}, err
	}
	if b.Reader == nil {
		return inference.ContextPack{}, invalid("section hydration requires an Explorer reader")
	}
	if err := shoal.ValidateRequiredID("document ID", request.DocumentID); err != nil {
		return inference.ContextPack{}, err
	}
	if err := shoal.ValidateRequiredID("revision ID", request.RevisionID); err != nil {
		return inference.ContextPack{}, err
	}
	if request.Depth > limits.MaxHierarchyDepth {
		return inference.ContextPack{}, invalid("section expansion exceeds the hierarchy depth bound")
	}
	if err := validateUniqueIDs("section ID", request.SectionIDs); err != nil {
		return inference.ContextPack{}, err
	}
	if len(request.SectionIDs) == 0 {
		return inference.ContextPack{}, invalid("section expansion requires explicit section IDs")
	}
	view, err := b.Reader.Document(ctx, request.DocumentID, request.RevisionID)
	if err != nil {
		return inference.ContextPack{}, err
	}
	verifier, err := newVerifier(ctx, b.Reader, limits, []explorer.DocumentView{view}, nil)
	if err != nil {
		return inference.ContextPack{}, err
	}
	if err := verifier.verifyExisting(pack.Evidence()); err != nil {
		return inference.ContextPack{}, err
	}
	key := documentKey{request.DocumentID, request.RevisionID}
	index := verifier.documents[key]
	if index == nil {
		return inference.ContextPack{}, invalid("hydrated document identity does not match request")
	}
	selected, err := index.selectSections(request.SectionIDs, request.Depth, limits)
	if err != nil {
		return inference.ContextPack{}, err
	}
	anchors := pack.Evidence()
	for _, sectionID := range selected {
		for _, span := range index.spansBySection[sectionID] {
			citation := document.Citation{
				DocumentID: span.DocumentID,
				RevisionID: span.RevisionID,
				SectionID:  span.SectionID,
				SpanID:     span.ID,
				Range:      span.Range,
			}
			anchor, err := verifier.documentAnchor(citation, span.Text)
			if err != nil {
				return inference.ContextPack{}, err
			}
			anchors = append(anchors, anchor)
		}
	}
	anchors, err = canonicalAnchors(anchors, limits.MaxAnchors)
	if err != nil {
		return inference.ContextPack{}, err
	}
	return rebuildPack(pack, anchors, limits)
}

// ExpandNeighbors returns a new pack containing verified existing evidence and
// exact one-node or one-edge paths from an explicitly requested neighborhood.
func (b Builder) ExpandNeighbors(
	ctx context.Context,
	pack inference.ContextPack,
	request ExpandNeighborsRequest,
) (inference.ContextPack, error) {
	if err := contextError(ctx); err != nil {
		return inference.ContextPack{}, err
	}
	if err := pack.Validate(); err != nil {
		return inference.ContextPack{}, err
	}
	limits, err := normalizeLimits(b.Limits)
	if err != nil {
		return inference.ContextPack{}, err
	}
	if b.Reader == nil {
		return inference.ContextPack{}, invalid("graph hydration requires an Explorer reader")
	}
	if err := validateUniqueIDs("graph node ID", request.NodeIDs); err != nil {
		return inference.ContextPack{}, err
	}
	if len(request.NodeIDs) == 0 {
		return inference.ContextPack{}, invalid("graph expansion requires explicit node IDs")
	}
	normalized, err := (explorer.NeighborhoodRequest{
		NodeIDs: request.NodeIDs, Depth: request.Depth, EdgeTypes: request.EdgeTypes,
	}).Normalize()
	if err != nil {
		return inference.ContextPack{}, err
	}
	if int(normalized.Depth) > limits.MaxPathNodes-1 {
		return inference.ContextPack{}, invalid("graph expansion exceeds the path bound")
	}
	neighborhood, err := b.Reader.Neighborhood(ctx, normalized)
	if err != nil {
		return inference.ContextPack{}, err
	}
	verifier, err := newVerifier(ctx, b.Reader, limits, nil, []explorer.Neighborhood{neighborhood})
	if err != nil {
		return inference.ContextPack{}, err
	}
	if err := verifier.verifyExisting(pack.Evidence()); err != nil {
		return inference.ContextPack{}, err
	}
	anchors := pack.Evidence()
	for _, seed := range normalized.NodeIDs {
		node, ok := verifier.nodes[seed]
		if !ok {
			return inference.ContextPack{}, invalid("hydrated neighborhood omitted a requested node")
		}
		anchor, err := verifier.graphAnchor(graph.Path{Nodes: []graph.Node{node}})
		if err != nil {
			return inference.ContextPack{}, err
		}
		anchors = append(anchors, anchor)
	}
	for _, edge := range sortedEdges(neighborhood.Edges) {
		from, fromOK := verifier.nodes[edge.From]
		to, toOK := verifier.nodes[edge.To]
		if !fromOK || !toOK {
			return inference.ContextPack{}, invalid("hydrated neighborhood edge has a missing endpoint")
		}
		anchor, err := verifier.graphAnchor(graph.Path{
			Nodes: []graph.Node{from, to},
			Edges: []graph.Edge{edge},
		})
		if err != nil {
			return inference.ContextPack{}, err
		}
		anchors = append(anchors, anchor)
	}
	anchors, err = canonicalAnchors(anchors, limits.MaxAnchors)
	if err != nil {
		return inference.ContextPack{}, err
	}
	return rebuildPack(pack, anchors, limits)
}

type verifier struct {
	ctx       context.Context
	reader    Reader
	limits    Limits
	documents map[documentKey]*documentIndex
	nodes     map[shoal.ID]graph.Node
	edges     map[shoal.ID]graph.Edge
	sections  int
}

type documentKey struct {
	documentID shoal.ID
	revisionID shoal.ID
}

type documentIndex struct {
	view           explorer.DocumentView
	sections       map[shoal.ID]document.Section
	spans          map[shoal.ID]document.Span
	children       map[shoal.ID][]shoal.ID
	spansBySection map[shoal.ID][]document.Span
}

func newVerifier(
	ctx context.Context,
	reader Reader,
	limits Limits,
	views []explorer.DocumentView,
	neighborhoods []explorer.Neighborhood,
) (*verifier, error) {
	v := &verifier{
		ctx: ctx, reader: reader, limits: limits,
		documents: make(map[documentKey]*documentIndex),
		nodes:     make(map[shoal.ID]graph.Node),
		edges:     make(map[shoal.ID]graph.Edge),
	}
	for _, view := range views {
		if err := v.addDocument(view); err != nil {
			return nil, err
		}
	}
	for _, neighborhood := range neighborhoods {
		if err := v.addNeighborhood(neighborhood); err != nil {
			return nil, err
		}
	}
	return v, nil
}

func (v *verifier) documentAnchor(
	citation document.Citation,
	quote string,
) (inference.EvidenceAnchor, error) {
	if err := contextError(v.ctx); err != nil {
		return inference.EvidenceAnchor{}, err
	}
	if err := citation.Validate(); err != nil {
		return inference.EvidenceAnchor{}, err
	}
	key := documentKey{citation.DocumentID, citation.RevisionID}
	index := v.documents[key]
	if index == nil {
		if v.reader == nil {
			return inference.EvidenceAnchor{}, invalid(
				"citation hydration is required for verification")
		}
		view, err := v.reader.Document(v.ctx, citation.DocumentID, citation.RevisionID)
		if err != nil {
			return inference.EvidenceAnchor{}, err
		}
		if err := v.addDocument(view); err != nil {
			return inference.EvidenceAnchor{}, err
		}
		index = v.documents[key]
		if index == nil {
			return inference.EvidenceAnchor{}, invalid(
				"hydrated document identity does not match citation")
		}
	}
	if err := index.validateCitation(citation, quote); err != nil {
		return inference.EvidenceAnchor{}, err
	}
	return inference.NewDocumentAnchor(citation, quote)
}

func (v *verifier) graphAnchor(path graph.Path) (inference.EvidenceAnchor, error) {
	if err := contextError(v.ctx); err != nil {
		return inference.EvidenceAnchor{}, err
	}
	if len(path.Nodes) > v.limits.MaxPathNodes {
		return inference.EvidenceAnchor{}, invalid("graph path exceeds the builder path bound")
	}
	if err := path.Validate(); err != nil {
		return inference.EvidenceAnchor{}, err
	}
	missing := false
	nodeIDs := make([]shoal.ID, 0, len(path.Nodes))
	for _, node := range path.Nodes {
		nodeIDs = append(nodeIDs, node.ID)
		if existing, ok := v.nodes[node.ID]; !ok || !canonicalEqual(existing, node) {
			missing = true
		}
	}
	for _, edge := range path.Edges {
		if existing, ok := v.edges[edge.ID]; !ok || !canonicalEqual(existing, edge) {
			missing = true
		}
	}
	if missing {
		if v.reader == nil {
			return inference.EvidenceAnchor{}, invalid(
				"graph hydration is required for verification")
		}
		neighborhood, err := v.reader.Neighborhood(v.ctx, explorer.NeighborhoodRequest{
			NodeIDs: nodeIDs,
			Depth:   1,
		})
		if err != nil {
			return inference.EvidenceAnchor{}, err
		}
		if err := v.addNeighborhood(neighborhood); err != nil {
			return inference.EvidenceAnchor{}, err
		}
	}
	for _, node := range path.Nodes {
		existing, ok := v.nodes[node.ID]
		if !ok || !canonicalEqual(existing, node) {
			return inference.EvidenceAnchor{}, invalid(
				"graph path node does not match hydrated Explorer data")
		}
	}
	for _, edge := range path.Edges {
		existing, ok := v.edges[edge.ID]
		if !ok || !canonicalEqual(existing, edge) {
			return inference.EvidenceAnchor{}, invalid(
				"graph path edge does not match hydrated Explorer data")
		}
	}
	return inference.NewGraphAnchor(path)
}

func (v *verifier) verifyExisting(anchors []inference.EvidenceAnchor) error {
	for _, anchor := range anchors {
		switch anchor.Kind() {
		case inference.AnchorDocument:
			citation, quote, ok := anchor.Document()
			if !ok {
				return invalid("document anchor variant is unavailable")
			}
			if _, err := v.documentAnchor(citation, quote); err != nil {
				return fmt.Errorf("existing document anchor %q: %w", anchor.ID(), err)
			}
		case inference.AnchorGraph:
			path, ok := anchor.Path()
			if !ok {
				return invalid("graph anchor variant is unavailable")
			}
			if _, err := v.graphAnchor(path); err != nil {
				return fmt.Errorf("existing graph anchor %q: %w", anchor.ID(), err)
			}
		default:
			return invalid("unknown evidence anchor kind")
		}
	}
	return nil
}

func (v *verifier) addDocument(view explorer.DocumentView) error {
	index, err := indexDocument(view)
	if err != nil {
		return err
	}
	key := documentKey{view.Document.ID, view.Revision.ID}
	if existing := v.documents[key]; existing != nil {
		if !canonicalEqual(existing.view, view) {
			return invalid("duplicate hydrated document has different content")
		}
		return nil
	}
	if len(v.documents)+1 > v.limits.MaxDocuments {
		return invalid("hydrated documents exceed the document bound")
	}
	if v.sections+len(index.sections) > v.limits.MaxSections {
		return invalid("hydrated documents exceed the section bound")
	}
	v.sections += len(index.sections)
	v.documents[key] = index
	return nil
}

func (v *verifier) addNeighborhood(neighborhood explorer.Neighborhood) error {
	localNodes := make(map[shoal.ID]graph.Node, len(neighborhood.Nodes))
	for _, node := range neighborhood.Nodes {
		if err := node.Validate(); err != nil {
			return err
		}
		if existing, ok := localNodes[node.ID]; ok {
			if !canonicalEqual(existing, node) {
				return invalid("duplicate hydrated graph node has different content")
			}
			continue
		}
		localNodes[node.ID] = node
	}
	localEdges := make(map[shoal.ID]graph.Edge, len(neighborhood.Edges))
	for _, edge := range neighborhood.Edges {
		if err := edge.Validate(); err != nil {
			return err
		}
		if _, ok := localNodes[edge.From]; !ok {
			return invalid("hydrated graph edge source is missing")
		}
		if _, ok := localNodes[edge.To]; !ok {
			return invalid("hydrated graph edge target is missing")
		}
		if existing, ok := localEdges[edge.ID]; ok {
			if !canonicalEqual(existing, edge) {
				return invalid("duplicate hydrated graph edge has different content")
			}
			continue
		}
		localEdges[edge.ID] = edge
	}
	for id, node := range localNodes {
		if existing, ok := v.nodes[id]; ok {
			if !canonicalEqual(existing, node) {
				return invalid("hydrated graph node conflicts with prior content")
			}
			continue
		}
		if len(v.nodes)+1 > v.limits.MaxGraphNodes {
			return invalid("hydrated graph exceeds the node bound")
		}
		v.nodes[id] = cloneNode(node)
	}
	for id, edge := range localEdges {
		if existing, ok := v.edges[id]; ok {
			if !canonicalEqual(existing, edge) {
				return invalid("hydrated graph edge conflicts with prior content")
			}
			continue
		}
		if len(v.edges)+1 > v.limits.MaxGraphEdges {
			return invalid("hydrated graph exceeds the edge bound")
		}
		v.edges[id] = cloneEdge(edge)
	}
	return nil
}

func indexDocument(view explorer.DocumentView) (*documentIndex, error) {
	if err := view.Document.Validate(); err != nil {
		return nil, err
	}
	if err := view.Revision.Validate(); err != nil {
		return nil, err
	}
	if view.Document.RevisionID != view.Revision.ID ||
		view.Revision.DocumentID != view.Document.ID {
		return nil, invalid("hydrated document and revision ownership do not match")
	}
	index := &documentIndex{
		view:           cloneView(view),
		sections:       make(map[shoal.ID]document.Section),
		spans:          make(map[shoal.ID]document.Span),
		children:       make(map[shoal.ID][]shoal.ID),
		spansBySection: make(map[shoal.ID][]document.Span),
	}
	var walk func(explorer.SectionView, shoal.ID) error
	walk = func(sectionView explorer.SectionView, expectedParent shoal.ID) error {
		section := sectionView.Section
		if err := section.Validate(); err != nil {
			return err
		}
		if section.DocumentID != view.Document.ID ||
			section.RevisionID != view.Revision.ID ||
			section.ParentID != expectedParent {
			return invalid("hydrated section ownership or parent does not match document")
		}
		if _, duplicate := index.sections[section.ID]; duplicate {
			return invalid("hydrated document has duplicate section IDs")
		}
		index.sections[section.ID] = section
		if expectedParent != "" {
			index.children[expectedParent] = append(index.children[expectedParent], section.ID)
		}
		for _, span := range sectionView.Spans {
			if err := span.Validate(); err != nil {
				return err
			}
			if span.DocumentID != view.Document.ID ||
				span.RevisionID != view.Revision.ID ||
				span.SectionID != section.ID {
				return invalid("hydrated span ownership does not match section")
			}
			if !rangeContains(section.Range, span.Range) {
				return invalid("hydrated span range is outside its section")
			}
			if span.Range.End.Offset-span.Range.Start.Offset != int64(len(span.Text)) {
				return invalid("hydrated span text length does not match its range")
			}
			if _, duplicate := index.spans[span.ID]; duplicate {
				return invalid("hydrated document has duplicate span IDs")
			}
			index.spans[span.ID] = span
			index.spansBySection[section.ID] = append(index.spansBySection[section.ID], span)
		}
		for _, child := range sectionView.Children {
			if !rangeContains(section.Range, child.Section.Range) {
				return invalid("hydrated child section range is outside its parent")
			}
			if err := walk(child, section.ID); err != nil {
				return err
			}
		}
		return nil
	}
	if view.Root.Section.ID != view.Document.RootSectionID {
		return nil, invalid("hydrated document root section does not match document")
	}
	if err := walk(view.Root, ""); err != nil {
		return nil, err
	}
	for id := range index.children {
		sort.Slice(index.children[id], func(i, j int) bool {
			return shoal.CompareID(index.children[id][i], index.children[id][j]) < 0
		})
	}
	for id := range index.spansBySection {
		sort.Slice(index.spansBySection[id], func(i, j int) bool {
			return shoal.CompareID(
				index.spansBySection[id][i].ID,
				index.spansBySection[id][j].ID,
			) < 0
		})
	}
	return index, nil
}

func (d *documentIndex) validateCitation(citation document.Citation, quote string) error {
	if !utf8.ValidString(quote) || len(quote) == 0 {
		return invalid("citation quote must be nonempty valid UTF-8")
	}
	if citation.DocumentID != d.view.Document.ID ||
		citation.RevisionID != d.view.Revision.ID {
		return invalid("citation ownership does not match hydrated revision")
	}
	if citation.SectionID != "" {
		section, ok := d.sections[citation.SectionID]
		if !ok {
			return shoal.NewError(shoal.ErrorNotFound, "cited section was not found")
		}
		if !rangeContains(section.Range, citation.Range) {
			return invalid("citation range is outside cited section")
		}
	}
	if citation.SpanID != "" {
		span, ok := d.spans[citation.SpanID]
		if !ok {
			return shoal.NewError(shoal.ErrorNotFound, "cited span was not found")
		}
		if citation.SectionID != "" && span.SectionID != citation.SectionID {
			return invalid("cited span does not belong to cited section")
		}
		return validateQuoteInSpan(span, citation.Range, quote)
	}
	for _, span := range d.spansBySection[citation.SectionID] {
		if rangeContains(span.Range, citation.Range) {
			return validateQuoteInSpan(span, citation.Range, quote)
		}
	}
	return invalid("section citation cannot be verified from one public span")
}

func (d *documentIndex) selectSections(
	ids []shoal.ID,
	depth uint32,
	limits Limits,
) ([]shoal.ID, error) {
	selected := make(map[shoal.ID]struct{})
	frontier := append([]shoal.ID(nil), ids...)
	for _, id := range frontier {
		if _, ok := d.sections[id]; !ok {
			return nil, shoal.NewError(shoal.ErrorNotFound, "requested section was not found")
		}
	}
	for level := uint32(0); len(frontier) > 0 && level <= depth; level++ {
		next := make([]shoal.ID, 0)
		for _, id := range frontier {
			selected[id] = struct{}{}
			if level < depth {
				next = append(next, d.children[id]...)
			}
		}
		if len(selected)+len(next) > limits.MaxSections {
			return nil, invalid("section expansion exceeds the section bound")
		}
		frontier = next
	}
	result := make([]shoal.ID, 0, len(selected))
	for id := range selected {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool {
		return shoal.CompareID(result[i], result[j]) < 0
	})
	return result, nil
}

func validateQuoteInSpan(span document.Span, quotedRange document.SourceRange, quote string) error {
	if !rangeContains(span.Range, quotedRange) {
		return invalid("citation range is outside cited span")
	}
	start := quotedRange.Start.Offset - span.Range.Start.Offset
	end := quotedRange.End.Offset - span.Range.Start.Offset
	if start < 0 || end < start || end > int64(len(span.Text)) {
		return invalid("citation range exceeds hydrated span text")
	}
	if !utf8.ValidString(span.Text) ||
		(start > 0 && start < int64(len(span.Text)) && !utf8.RuneStart(span.Text[start])) ||
		(end > 0 && end < int64(len(span.Text)) && !utf8.RuneStart(span.Text[end])) {
		return invalid("citation range is not a UTF-8 boundary")
	}
	if span.Text[start:end] != quote {
		return invalid("citation quote does not match hydrated source bytes")
	}
	return nil
}

func provenanceMetadata(
	input shoal.Metadata,
	request retrieval.Request,
	response retrieval.Response,
	policyID shoal.ID,
	limits Limits,
) (shoal.Metadata, error) {
	if err := shoal.ValidateRequiredID("policy ID", policyID); err != nil {
		return nil, err
	}
	metadata := cloneMetadata(input)
	if metadata == nil {
		metadata = make(shoal.Metadata)
	}
	for _, key := range []string{
		metadataBuilderKey, metadataRequestKey, metadataRetrievalKey, metadataPolicyKey,
	} {
		if _, exists := metadata[key]; exists {
			return nil, invalid("context metadata uses a reserved builder key")
		}
	}
	identity, err := retrievalIdentity(request, response)
	if err != nil {
		return nil, err
	}
	metadata[metadataBuilderKey] = builderVersion
	metadata[metadataRequestKey] = string(response.RequestID)
	metadata[metadataRetrievalKey] = identity
	metadata[metadataPolicyKey] = string(policyID)
	if metadataBytes(metadata) > limits.MaxProvenanceBytes {
		return nil, invalid("context metadata and provenance exceed the byte bound")
	}
	if err := shoal.ValidateMetadata("context metadata", metadata); err != nil {
		return nil, err
	}
	return metadata, nil
}

func retrievalIdentity(request retrieval.Request, response retrieval.Response) (string, error) {
	type evidenceIdentity struct {
		Citation document.Citation `json:"citation"`
		Quote    string            `json:"quote"`
		Path     graph.Path        `json:"path"`
		Score    shoal.Score       `json:"score"`
	}
	type resultIdentity struct {
		ID          shoal.ID               `json:"id"`
		Score       shoal.Score            `json:"score"`
		Evidence    []evidenceIdentity     `json:"evidence"`
		Explanation *retrieval.Explanation `json:"explanation,omitempty"`
	}
	results := make([]resultIdentity, 0, len(response.Results))
	for _, result := range response.Results {
		item := resultIdentity{
			ID: result.ID, Score: result.Score, Explanation: cloneExplanation(result.Explanation),
		}
		for _, evidence := range result.Evidence {
			item.Evidence = append(item.Evidence, evidenceIdentity{
				Citation: evidence.Citation,
				Quote:    evidence.Quote,
				Path:     clonePath(evidence.Path),
				Score:    evidence.Score,
			})
		}
		results = append(results, item)
	}
	payload := struct {
		Version   string            `json:"version"`
		Request   retrieval.Request `json:"request"`
		RequestID shoal.ID          `json:"request_id"`
		Results   []resultIdentity  `json:"results"`
	}{
		Version: builderVersion, Request: request,
		RequestID: response.RequestID, Results: results,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", shoal.WrapError(shoal.ErrorInvalidArgument, "encode retrieval identity", err)
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func buildPack(
	query string,
	anchors []inference.EvidenceAnchor,
	pins Pins,
	metadata shoal.Metadata,
	limits Limits,
) (inference.ContextPack, error) {
	if err := enforcePackBounds(query, anchors, metadata, limits); err != nil {
		return inference.ContextPack{}, err
	}
	return inference.NewContextPack(
		query, anchors, pins.Ontology, pins.Snapshot, pins.Authorization, metadata)
}

func rebuildPack(
	pack inference.ContextPack,
	anchors []inference.EvidenceAnchor,
	limits Limits,
) (inference.ContextPack, error) {
	var ontology *inference.OntologyIdentity
	if value, ok := pack.Ontology(); ok {
		ontology = &value
	}
	return buildPack(pack.Query(), anchors, Pins{
		Snapshot: pack.Snapshot(), Authorization: pack.Authorization(), Ontology: ontology,
	}, pack.Metadata(), limits)
}

func canonicalAnchors(
	input []inference.EvidenceAnchor,
	maximum int,
) ([]inference.EvidenceAnchor, error) {
	byID := make(map[shoal.ID]inference.EvidenceAnchor, len(input))
	for _, anchor := range input {
		if err := anchor.Validate(); err != nil {
			return nil, err
		}
		if existing, duplicate := byID[anchor.ID()]; duplicate {
			if !anchorsEqual(existing, anchor) {
				return nil, invalid("duplicate anchor identity has different content")
			}
			continue
		}
		byID[anchor.ID()] = anchor
	}
	if len(byID) > maximum {
		return nil, invalid("context evidence exceeds the anchor bound")
	}
	result := make([]inference.EvidenceAnchor, 0, len(byID))
	for _, anchor := range byID {
		result = append(result, anchor)
	}
	sort.Slice(result, func(i, j int) bool {
		return shoal.CompareID(result[i].ID(), result[j].ID()) < 0
	})
	return result, nil
}

func anchorsEqual(left, right inference.EvidenceAnchor) bool {
	if left.ID() != right.ID() || left.Kind() != right.Kind() {
		return false
	}
	if left.Kind() == inference.AnchorDocument {
		leftCitation, leftQuote, leftOK := left.Document()
		rightCitation, rightQuote, rightOK := right.Document()
		return leftOK && rightOK &&
			canonicalEqual(leftCitation, rightCitation) && leftQuote == rightQuote
	}
	leftPath, leftOK := left.Path()
	rightPath, rightOK := right.Path()
	return leftOK && rightOK && canonicalEqual(leftPath, rightPath)
}

func enforcePackBounds(
	query string,
	anchors []inference.EvidenceAnchor,
	metadata shoal.Metadata,
	limits Limits,
) error {
	if len(anchors) > limits.MaxAnchors {
		return invalid("context evidence exceeds the anchor bound")
	}
	quoteBytes := 0
	graphNodes := 0
	graphEdges := 0
	contextBytes := len(query) + metadataBytes(metadata)
	for _, anchor := range anchors {
		contextBytes += len(anchor.ID()) + len(anchor.Kind())
		if citation, quote, ok := anchor.Document(); ok {
			quoteBytes += len(quote)
			contextBytes += len(quote) + citationBytes(citation)
		} else if path, ok := anchor.Path(); ok {
			if len(path.Nodes) > limits.MaxPathNodes {
				return invalid("context graph path exceeds the path bound")
			}
			graphNodes += len(path.Nodes)
			graphEdges += len(path.Edges)
			if graphNodes > limits.MaxGraphNodes {
				return invalid("context evidence exceeds the graph node bound")
			}
			if graphEdges > limits.MaxGraphEdges {
				return invalid("context evidence exceeds the graph edge bound")
			}
			encoded, err := json.Marshal(path)
			if err != nil {
				return shoal.WrapError(shoal.ErrorInvalidArgument, "encode graph path", err)
			}
			contextBytes += len(encoded)
		}
		if quoteBytes > limits.MaxQuoteBytes {
			return invalid("context evidence exceeds the total quote byte bound")
		}
		if contextBytes > limits.MaxContextBytes {
			return invalid("context pack exceeds the builder byte bound")
		}
	}
	return nil
}

func normalizeLimits(input Limits) (Limits, error) {
	limits := input
	defaultInt(&limits.MaxResults, DefaultMaxResults)
	defaultInt(&limits.MaxAnchors, DefaultMaxAnchors)
	defaultInt(&limits.MaxDocuments, DefaultMaxDocuments)
	defaultInt(&limits.MaxSections, DefaultMaxSections)
	defaultInt(&limits.MaxGraphNodes, DefaultMaxGraphNodes)
	defaultInt(&limits.MaxGraphEdges, DefaultMaxGraphEdges)
	defaultInt(&limits.MaxPathNodes, DefaultMaxPathNodes)
	defaultInt(&limits.MaxContextBytes, DefaultMaxContextBytes)
	defaultInt(&limits.MaxQuoteBytes, DefaultMaxQuoteBytes)
	defaultInt(&limits.MaxProvenanceBytes, DefaultMaxProvenanceBytes)
	if limits.MaxHierarchyDepth == 0 {
		limits.MaxHierarchyDepth = DefaultMaxHierarchyDepth
	}
	for name, value := range map[string]int{
		"result": limits.MaxResults, "anchor": limits.MaxAnchors,
		"document": limits.MaxDocuments, "section": limits.MaxSections,
		"graph node": limits.MaxGraphNodes, "graph edge": limits.MaxGraphEdges,
		"path node": limits.MaxPathNodes, "context byte": limits.MaxContextBytes,
		"quote byte": limits.MaxQuoteBytes, "provenance byte": limits.MaxProvenanceBytes,
	} {
		if value <= 0 {
			return Limits{}, invalid(name + " limit must be positive")
		}
	}
	if limits.MaxResults > int(retrieval.MaxTopK) {
		return Limits{}, invalid("result limit exceeds the retrieval contract")
	}
	if limits.MaxAnchors > inference.MaxEvidenceAnchors {
		return Limits{}, invalid("anchor limit exceeds the inference contract")
	}
	if limits.MaxPathNodes > inference.MaxPathNodes {
		return Limits{}, invalid("path limit exceeds the inference contract")
	}
	if limits.MaxContextBytes > inference.MaxContextPackBytes {
		return Limits{}, invalid("context byte limit exceeds the inference contract")
	}
	if limits.MaxHierarchyDepth > 16 {
		return Limits{}, invalid("hierarchy depth limit exceeds the Explorer contract")
	}
	return limits, nil
}

func defaultInt(value *int, fallback int) {
	if *value == 0 {
		*value = fallback
	}
}

func validateUniqueIDs(name string, ids []shoal.ID) error {
	seen := make(map[shoal.ID]struct{}, len(ids))
	for _, id := range ids {
		if err := shoal.ValidateRequiredID(name, id); err != nil {
			return err
		}
		if _, duplicate := seen[id]; duplicate {
			return invalid("duplicate " + name)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func cloneResponse(response retrieval.Response) retrieval.Response {
	cloned := retrieval.Response{RequestID: response.RequestID}
	cloned.Results = make([]retrieval.Result, len(response.Results))
	for i, result := range response.Results {
		cloned.Results[i] = retrieval.Result{
			ID: result.ID, Score: result.Score, Explanation: cloneExplanation(result.Explanation),
		}
		cloned.Results[i].Evidence = make([]retrieval.Evidence, len(result.Evidence))
		for j, evidence := range result.Evidence {
			cloned.Results[i].Evidence[j] = retrieval.Evidence{
				Citation: evidence.Citation, Quote: evidence.Quote,
				Path: clonePath(evidence.Path), Score: evidence.Score,
			}
		}
	}
	return cloned
}

func cloneExplanation(explanation *retrieval.Explanation) *retrieval.Explanation {
	if explanation == nil {
		return nil
	}
	cloned := &retrieval.Explanation{
		Modes:   append([]retrieval.Mode(nil), explanation.Modes...),
		Summary: explanation.Summary,
		Scores:  make(map[string]shoal.Score, len(explanation.Scores)),
	}
	for key, value := range explanation.Scores {
		cloned.Scores[key] = value
	}
	return cloned
}

func cloneView(view explorer.DocumentView) explorer.DocumentView {
	encoded, _ := json.Marshal(view)
	var cloned explorer.DocumentView
	_ = json.Unmarshal(encoded, &cloned)
	return cloned
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

func clonePath(path graph.Path) graph.Path {
	cloned := graph.Path{
		Nodes: make([]graph.Node, len(path.Nodes)),
		Edges: make([]graph.Edge, len(path.Edges)),
	}
	for i, node := range path.Nodes {
		cloned.Nodes[i] = cloneNode(node)
	}
	for i, edge := range path.Edges {
		cloned.Edges[i] = cloneEdge(edge)
	}
	return cloned
}

func cloneMetadata(metadata shoal.Metadata) shoal.Metadata {
	if metadata == nil {
		return nil
	}
	cloned := make(shoal.Metadata, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

func sortedEdges(edges []graph.Edge) []graph.Edge {
	result := make([]graph.Edge, len(edges))
	for i, edge := range edges {
		result[i] = cloneEdge(edge)
	}
	sort.Slice(result, func(i, j int) bool {
		return shoal.CompareID(result[i].ID, result[j].ID) < 0
	})
	return result
}

func canonicalEqual(left, right any) bool {
	leftBytes, leftErr := json.Marshal(left)
	rightBytes, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftBytes, rightBytes)
}

func metadataBytes(metadata shoal.Metadata) int {
	total := 0
	for key, value := range metadata {
		total += len(key) + len(value)
	}
	return total
}

func citationBytes(citation document.Citation) int {
	return len(citation.DocumentID) + len(citation.RevisionID) +
		len(citation.SectionID) + len(citation.SpanID) +
		len(strconv.FormatInt(citation.Range.Start.Offset, 10)) +
		len(strconv.FormatInt(citation.Range.End.Offset, 10))
}

func rangeContains(outer, inner document.SourceRange) bool {
	return outer.Start.Offset <= inner.Start.Offset && inner.End.Offset <= outer.End.Offset
}

func pathPresent(path graph.Path) bool {
	return len(path.Nodes) > 0 || len(path.Edges) > 0
}

func contextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return shoal.WrapError(shoal.ErrorUnavailable, "context construction canceled", err)
	}
	return nil
}

func invalid(message string) error {
	return shoal.NewError(shoal.ErrorInvalidArgument, strings.TrimSpace(message))
}
