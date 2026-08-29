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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type rankedSpan struct {
	record      *persistedDocument
	span        document.Span
	score       shoal.Score
	lexical     shoal.Score
	tree        shoal.Score
	graph       shoal.Score
	vector      shoal.Score
	path        graph.Path
	activeModes []retrieval.Mode
}

type pathEdge struct {
	from shoal.ID
	to   shoal.ID
}

// Retrieve searches the newest eligible document revisions and returns exact
// span citations with a navigable evidence path.
func (e *Explorer) Retrieve(
	ctx context.Context, request retrieval.Request,
) (retrieval.Response, error) {
	return e.retrieve(ctx, request)
}

func (e *Explorer) retrieve(
	ctx context.Context, request retrieval.Request,
) (retrieval.Response, error) {
	if err := contextError(ctx); err != nil {
		return retrieval.Response{}, err
	}
	normalized, err := request.Normalize()
	if err != nil {
		return retrieval.Response{}, err
	}
	request = normalized
	if !request.AsOf.IsZero() {
		return retrieval.Response{}, shoal.NewError(
			shoal.ErrorUnavailable,
			"as-of retrieval requires publication-frontier semantics",
		)
	}
	if err := request.ValidateSeedPlan(false); err != nil {
		return retrieval.Response{}, err
	}
	analyzer := retrieval.UnicodeTermAnalyzer{}
	scorer := retrieval.CoverageFusionScorer{}
	modes := request.Modes
	hasVector := request.HasMode(retrieval.ModeVector)
	hasTermMode := request.HasMode(retrieval.ModeLexical) ||
		request.HasMode(retrieval.ModeTree) ||
		request.HasMode(retrieval.ModeGraph)
	queryTerms := analyzer.Analyze(request.Text)
	if hasTermMode && len(queryTerms) == 0 {
		return retrieval.Response{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "retrieval text has no searchable terms")
	}
	var (
		queryProvenance persistedEmbeddingProvenance
		queryVector     []float32
	)
	if hasVector {
		e.mu.RLock()
		if err := e.requireOpen(); err != nil {
			e.mu.RUnlock()
			return retrieval.Response{}, err
		}
		e.mu.RUnlock()
		queryProvenance, queryVector, err = e.embedQuery(ctx, request.Text)
		if err != nil {
			return retrieval.Response{}, err
		}
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.requireOpen(); err != nil {
		return retrieval.Response{}, err
	}
	if hasVector {
		if err := e.ensureEmbeddingSpaceCompatibleLocked(queryProvenance); err != nil {
			return retrieval.Response{}, err
		}
	}
	documentScope := idSet(request.Scope.DocumentIDs)
	nodeScope := idSet(request.Scope.NodeIDs)
	var ranked []rankedSpan
	for documentID, revisions := range e.documents {
		if len(documentScope) > 0 {
			if _, ok := documentScope[documentID]; !ok {
				continue
			}
		}
		record, err := latestRevision(revisions)
		if err != nil {
			return retrieval.Response{}, err
		}
		if record == nil {
			continue
		}
		sectionByID := make(map[shoal.ID]document.Section, len(record.Sections))
		for _, section := range record.Sections {
			sectionByID[section.ID] = section
		}
		nodes := make(map[shoal.ID]graph.Node, len(record.Nodes))
		edges := make(map[pathEdge]graph.Edge, len(record.Edges))
		for _, node := range record.Nodes {
			nodes[node.ID] = node
		}
		for _, edge := range record.Edges {
			edges[pathEdgeKey(edge.From, edge.To)] = edge
		}
		if len(nodeScope) > 0 {
			recordEligible := false
			for _, span := range record.Spans {
				if spanInNodeScope(record, sectionByID, span, nodeScope) {
					recordEligible = true
					break
				}
			}
			if !recordEligible {
				continue
			}
		}
		var spanEmbeddings map[shoal.ID]persistedSpanEmbedding
		if hasVector && len(record.Spans) > 0 {
			if record.Embeddings == nil {
				return retrieval.Response{}, shoal.NewError(
					shoal.ErrorUnavailable,
					"vector retrieval requires embeddings for every eligible span",
				)
			}
			if record.Embeddings.Provenance != queryProvenance {
				return retrieval.Response{}, incompatibleEmbeddingSpaceError(
					queryProvenance,
					record.Embeddings.Provenance,
				)
			}
			spanEmbeddings, err = recordEmbeddingMap(record)
			if err != nil {
				return retrieval.Response{}, err
			}
		}
		for _, span := range record.Spans {
			if len(nodeScope) > 0 &&
				!spanInNodeScope(record, sectionByID, span, nodeScope) {
				continue
			}
			lexical := scorer.Coverage(queryTerms, analyzer.Analyze(span.Text))
			headingText := record.Document.Title
			for sectionID := span.SectionID; sectionID != ""; {
				section := sectionByID[sectionID]
				headingText += " " + section.Heading
				sectionID = section.ParentID
			}
			treeScore := scorer.Coverage(queryTerms, analyzer.Analyze(headingText))
			graphScore := shoal.Score(0)
			path, err := evidencePath(record, sectionByID, nodes, edges, span)
			if err != nil {
				return retrieval.Response{}, err
			}
			if len(path.Edges) > 0 {
				graphScore = scorer.Coverage(
					queryTerms, analyzer.Analyze(pathSearchText(path, span.Text)))
			}
			vectorScoreValue := shoal.Score(0)
			if hasVector {
				embedding, ok := spanEmbeddings[span.ID]
				if !ok || !embeddingMatchesSpan(embedding, span) {
					return retrieval.Response{}, shoal.NewError(
						shoal.ErrorUnavailable,
						"stored span embeddings are stale or incomplete",
					)
				}
				vectorScoreValue, err = vectorScore(queryVector, embedding.Vector)
				if err != nil {
					return retrieval.Response{}, err
				}
			}
			score := scorer.CombinedScore(modes, retrieval.ComponentScores{
				Lexical: lexical,
				Tree:    treeScore,
				Graph:   graphScore,
				Vector:  vectorScoreValue,
			})
			if score <= 0 {
				continue
			}
			ranked = append(ranked, rankedSpan{
				record: record, span: span, score: score,
				lexical: lexical, tree: treeScore, graph: graphScore,
				vector: vectorScoreValue,
				path:   path, activeModes: modes,
			})
		}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		return shoal.CompareID(ranked[i].span.ID, ranked[j].span.ID) < 0
	})
	topK := request.TopK
	if uint64(topK) < uint64(len(ranked)) {
		ranked = ranked[:int(topK)]
	}

	response := retrieval.Response{
		RequestID: requestID(request),
		Results:   make([]retrieval.Result, 0, len(ranked)),
	}
	for _, match := range ranked {
		result := retrieval.Result{
			ID: match.span.ID, Score: match.score,
			Evidence: []retrieval.Evidence{{
				Citation: document.Citation{
					DocumentID: match.span.DocumentID,
					RevisionID: match.span.RevisionID,
					SectionID:  match.span.SectionID,
					SpanID:     match.span.ID,
					Range:      match.span.Range,
				},
				Quote: match.span.Text, Path: match.path, Score: match.score,
			}},
		}
		if request.Explain {
			scores := map[string]shoal.Score{}
			for _, mode := range match.activeModes {
				scores[string(mode)] = scorer.ModeScore(mode, retrieval.ComponentScores{
					Lexical: match.lexical,
					Tree:    match.tree,
					Graph:   match.graph,
					Vector:  match.vector,
				})
			}
			result.Explanation = &retrieval.Explanation{
				Modes: append([]retrieval.Mode(nil), match.activeModes...),
				Summary: "ranked source span using analyzer " + analyzer.Version() +
					" and scorer " + scorer.Version(),
				Scores: scores,
			}
		}
		response.Results = append(response.Results, result)
	}
	if err := response.ValidateFor(request); err != nil {
		return retrieval.Response{}, shoal.WrapError(
			shoal.ErrorInternal, "embedded retrieval produced an invalid response", err)
	}
	return response, nil
}

func requestID(request retrieval.Request) shoal.ID {
	parts := []string{
		"text", request.Text,
		"top_k", strconv.FormatUint(uint64(request.TopK), 10),
		"modes", strconv.Itoa(len(request.Modes)),
	}
	for _, mode := range request.Modes {
		parts = append(parts, string(mode))
	}
	parts = append(parts, "document_ids", strconv.Itoa(len(request.Scope.DocumentIDs)))
	for _, id := range request.Scope.DocumentIDs {
		parts = append(parts, string(id))
	}
	parts = append(parts, "node_ids", strconv.Itoa(len(request.Scope.NodeIDs)))
	for _, id := range request.Scope.NodeIDs {
		parts = append(parts, string(id))
	}
	asOf := ""
	if !request.AsOf.IsZero() {
		asOf = request.AsOf.UTC().Format(time.RFC3339Nano)
	}
	parts = append(parts,
		"as_of", asOf,
		"explain", strconv.FormatBool(request.Explain),
	)
	return shoal.ID(stableID("request", parts...))
}

func evidencePath(
	record *persistedDocument,
	sections map[shoal.ID]document.Section,
	nodes map[shoal.ID]graph.Node,
	edges map[pathEdge]graph.Edge,
	span document.Span,
) (graph.Path, error) {
	var sectionIDs []shoal.ID
	for sectionID := span.SectionID; sectionID != ""; {
		section, ok := sections[sectionID]
		if !ok {
			return graph.Path{}, shoal.NewError(
				shoal.ErrorInternal, "stored evidence section is missing")
		}
		sectionIDs = append(sectionIDs, sectionID)
		sectionID = section.ParentID
	}
	pathIDs := []shoal.ID{record.Document.ID}
	for i := len(sectionIDs) - 1; i >= 0; i-- {
		pathIDs = append(pathIDs, sectionIDs[i])
	}
	pathIDs = append(pathIDs, span.ID)
	path := graph.Path{
		Nodes: make([]graph.Node, 0, len(pathIDs)),
		Edges: make([]graph.Edge, 0, len(pathIDs)-1),
	}
	for i, id := range pathIDs {
		node, ok := nodes[id]
		if !ok {
			return graph.Path{}, shoal.NewError(
				shoal.ErrorInternal, "stored evidence graph node is missing")
		}
		path.Nodes = append(path.Nodes, cloneNode(node))
		if i == 0 {
			continue
		}
		edge, ok := edges[pathEdgeKey(pathIDs[i-1], id)]
		if !ok {
			return graph.Path{}, shoal.NewError(
				shoal.ErrorInternal, "stored evidence graph edge is missing")
		}
		path.Edges = append(path.Edges, cloneEdge(edge))
	}
	if err := path.Validate(); err != nil {
		return graph.Path{}, shoal.WrapError(
			shoal.ErrorInternal, "stored evidence path is invalid", err)
	}
	return path, nil
}

func spanInNodeScope(
	record *persistedDocument,
	sections map[shoal.ID]document.Section,
	span document.Span,
	scope map[shoal.ID]struct{},
) bool {
	if _, ok := scope[record.Document.ID]; ok {
		return true
	}
	if _, ok := scope[span.ID]; ok {
		return true
	}
	for sectionID := span.SectionID; sectionID != ""; {
		if _, ok := scope[sectionID]; ok {
			return true
		}
		section := sections[sectionID]
		sectionID = section.ParentID
	}
	return false
}

func pathSearchText(path graph.Path, quote string) string {
	var text strings.Builder
	text.WriteString(quote)
	for _, node := range path.Nodes {
		for _, label := range node.Labels {
			text.WriteByte(' ')
			text.WriteString(label)
		}
		for _, key := range []string{"title", "heading"} {
			if value := node.Properties[key]; value != "" {
				text.WriteByte(' ')
				text.WriteString(value)
			}
		}
	}
	for _, edge := range path.Edges {
		text.WriteByte(' ')
		text.WriteString(edge.Type)
	}
	return text.String()
}

func idSet(ids []shoal.ID) map[shoal.ID]struct{} {
	if len(ids) == 0 {
		return nil
	}
	result := make(map[shoal.ID]struct{}, len(ids))
	for _, id := range ids {
		result[id] = struct{}{}
	}
	return result
}

func pathEdgeKey(from, to shoal.ID) pathEdge {
	return pathEdge{from: from, to: to}
}
