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
	"fmt"
	"sort"
	"strings"
	"unicode"

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
	path        graph.Path
	activeModes []retrieval.Mode
}

// Retrieve searches the newest eligible document revisions and returns exact
// span citations with a navigable evidence path.
func (e *Explorer) Retrieve(
	ctx context.Context, request retrieval.Request,
) (retrieval.Response, error) {
	if err := contextError(ctx); err != nil {
		return retrieval.Response{}, err
	}
	if err := request.Validate(); err != nil {
		return retrieval.Response{}, err
	}
	modes := append([]retrieval.Mode(nil), request.Modes...)
	if len(modes) == 0 {
		modes = []retrieval.Mode{retrieval.ModeLexical, retrieval.ModeTree}
	}
	for _, mode := range modes {
		if mode == retrieval.ModeVector {
			return retrieval.Response{}, shoal.NewError(
				shoal.ErrorUnavailable,
				"vector retrieval is not configured for the embedded Explorer")
		}
	}
	queryTerms := uniqueTerms(request.Text)
	if len(queryTerms) == 0 {
		return retrieval.Response{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "retrieval text has no searchable terms")
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.requireOpen(); err != nil {
		return retrieval.Response{}, err
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
		record := latestRevision(revisions, request.AsOf)
		if record == nil {
			continue
		}
		sectionByID := make(map[shoal.ID]document.Section, len(record.Sections))
		for _, section := range record.Sections {
			sectionByID[section.ID] = section
		}
		nodes := make(map[shoal.ID]graph.Node, len(record.Nodes))
		edges := make(map[string]graph.Edge, len(record.Edges))
		for _, node := range record.Nodes {
			nodes[node.ID] = node
		}
		for _, edge := range record.Edges {
			edges[pathEdgeKey(edge.From, edge.To)] = edge
		}
		for _, span := range record.Spans {
			if len(nodeScope) > 0 &&
				!spanInNodeScope(record, sectionByID, span, nodeScope) {
				continue
			}
			lexical := termCoverage(queryTerms, uniqueTerms(span.Text))
			headingText := record.Document.Title
			for sectionID := span.SectionID; sectionID != ""; {
				section := sectionByID[sectionID]
				headingText += " " + section.Heading
				sectionID = section.ParentID
			}
			treeScore := termCoverage(queryTerms, uniqueTerms(headingText))
			graphScore := shoal.Score(0)
			path, err := evidencePath(record, sectionByID, nodes, edges, span)
			if err != nil {
				return retrieval.Response{}, err
			}
			if len(path.Edges) > 0 {
				graphScore = termCoverage(
					queryTerms, uniqueTerms(pathSearchText(path, span.Text)))
			}
			score := combinedScore(modes, lexical, treeScore, graphScore)
			if score <= 0 {
				continue
			}
			ranked = append(ranked, rankedSpan{
				record: record, span: span, score: score,
				lexical: lexical, tree: treeScore, graph: graphScore,
				path: path, activeModes: modes,
			})
		}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		if ranked[i].record.Document.ID != ranked[j].record.Document.ID {
			return ranked[i].record.Document.ID < ranked[j].record.Document.ID
		}
		return ranked[i].span.Range.Start.Offset < ranked[j].span.Range.Start.Offset
	})
	topK := int(request.TopK)
	if topK == 0 {
		topK = 10
	}
	if len(ranked) > topK {
		ranked = ranked[:topK]
	}

	response := retrieval.Response{
		RequestID: shoal.ID(stableID(
			"request", request.Text, fmt.Sprint(request.TopK),
			fmt.Sprint(modes), fmt.Sprint(request.Scope), request.AsOf.UTC().String())),
		Results: make([]retrieval.Result, 0, len(ranked)),
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
				switch mode {
				case retrieval.ModeLexical:
					scores[string(mode)] = match.lexical
				case retrieval.ModeTree:
					scores[string(mode)] = match.tree
				case retrieval.ModeGraph:
					scores[string(mode)] = match.graph
				}
			}
			result.Explanation = &retrieval.Explanation{
				Modes:   append([]retrieval.Mode(nil), match.activeModes...),
				Summary: "ranked source span using configured Explorer strategies",
				Scores:  scores,
			}
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

func combinedScore(
	modes []retrieval.Mode, lexical, treeScore, graphScore shoal.Score,
) shoal.Score {
	var score shoal.Score
	for _, mode := range modes {
		switch mode {
		case retrieval.ModeLexical:
			score += lexical
		case retrieval.ModeTree:
			score += treeScore*0.35 + lexical*0.65
		case retrieval.ModeGraph:
			score += graphScore*0.25 + lexical*0.75
		}
	}
	return score / shoal.Score(len(modes))
}

func evidencePath(
	record *persistedDocument,
	sections map[shoal.ID]document.Section,
	nodes map[shoal.ID]graph.Node,
	edges map[string]graph.Edge,
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

func uniqueTerms(value string) map[string]struct{} {
	terms := make(map[string]struct{})
	var current strings.Builder
	flush := func() {
		if current.Len() > 0 {
			terms[current.String()] = struct{}{}
			current.Reset()
		}
	}
	for _, r := range strings.ToLower(value) {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			current.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	return terms
}

func termCoverage(query, candidate map[string]struct{}) shoal.Score {
	if len(query) == 0 {
		return 0
	}
	matched := 0
	for term := range query {
		if _, ok := candidate[term]; ok {
			matched++
		}
	}
	return shoal.Score(matched) / shoal.Score(len(query))
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

func pathEdgeKey(from, to shoal.ID) string {
	return string(from) + "\x00" + string(to)
}
