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
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Service is independent of HTTP and storage topology so another transport can
// implement the same browser contract against distributed Shoal.
type Service interface {
	Documents(context.Context, DocumentsRequest) (DocumentsResponse, error)
	Document(context.Context, DocumentRequest) (DocumentResponse, error)
	Retrieve(context.Context, RetrievalRequest) (RetrievalResponse, error)
	Neighborhood(context.Context, NeighborhoodRequest) (NeighborhoodResponse, error)
	Path(context.Context, PathRequest) (PathResponse, error)
}

// EmbeddedService adapts the public Explorer client to the workspace service.
type EmbeddedService struct {
	client explorer.Client
}

// NewEmbeddedService creates a local service without exposing the embedded
// engine through the service contract.
func NewEmbeddedService(client explorer.Client) (*EmbeddedService, error) {
	if client == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "explorer client is required")
	}
	return &EmbeddedService{client: client}, nil
}

func (s *EmbeddedService) Documents(
	ctx context.Context, request DocumentsRequest,
) (DocumentsResponse, error) {
	documents, snapshot, err := s.current(ctx, request.Snapshot)
	if err != nil {
		return DocumentsResponse{}, err
	}
	limit, err := normalizeLimit(request.Page.Limit)
	if err != nil {
		return DocumentsResponse{}, err
	}
	offset, err := decodeCursor(request.Page.Cursor, snapshot.ID)
	if err != nil {
		return DocumentsResponse{}, err
	}
	if offset > len(documents) {
		return DocumentsResponse{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "document cursor is outside the snapshot")
	}
	end := offset + int(limit)
	if end > len(documents) {
		end = len(documents)
	}
	response := DocumentsResponse{
		Snapshot:  snapshot,
		Documents: append([]explorer.DocumentSummary(nil), documents[offset:end]...),
	}
	if end < len(documents) {
		response.NextCursor = encodeCursor(snapshot.ID, end)
	}
	return response, nil
}

func (s *EmbeddedService) Document(
	ctx context.Context, request DocumentRequest,
) (DocumentResponse, error) {
	_, snapshot, err := s.current(ctx, request.Snapshot)
	if err != nil {
		return DocumentResponse{}, err
	}
	view, err := s.client.Document(ctx, request.DocumentID, request.RevisionID)
	if err != nil {
		return DocumentResponse{}, err
	}
	return DocumentResponse{Snapshot: snapshot, Document: view}, nil
}

func (s *EmbeddedService) Retrieve(
	ctx context.Context, request RetrievalRequest,
) (RetrievalResponse, error) {
	_, snapshot, err := s.current(ctx, request.Snapshot)
	if err != nil {
		return RetrievalResponse{}, err
	}
	query, err := request.Query.Normalize()
	if err != nil {
		return RetrievalResponse{}, err
	}
	if query.TopK > MaxTopK {
		return RetrievalResponse{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "workspace top_k exceeds the server bound")
	}
	if !query.AsOf.IsZero() && !query.AsOf.Equal(snapshot.AsOf) {
		return RetrievalResponse{}, shoal.NewError(
			shoal.ErrorConflict, "retrieval as_of does not match the pinned snapshot")
	}
	// The embedded backend is already pinned by the snapshot identity and does
	// not implement historical publication-frontier reads.
	query.AsOf = time.Time{}
	response, err := s.client.Retrieve(ctx, query)
	if err != nil {
		return RetrievalResponse{}, err
	}
	if err := response.ValidateFor(query); err != nil {
		return RetrievalResponse{}, shoal.WrapError(
			shoal.ErrorInternal, "invalid retrieval response", err)
	}
	return RetrievalResponse{Snapshot: snapshot, Retrieval: response}, nil
}

func (s *EmbeddedService) Neighborhood(
	ctx context.Context, request NeighborhoodRequest,
) (NeighborhoodResponse, error) {
	_, snapshot, err := s.current(ctx, request.Snapshot)
	if err != nil {
		return NeighborhoodResponse{}, err
	}
	depth, fanout, maxNodes, err := normalizeGraphBounds(
		request.Depth, request.Fanout, request.MaxNodes)
	if err != nil {
		return NeighborhoodResponse{}, err
	}
	if len(request.NodeIDs) == 0 {
		return NeighborhoodResponse{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "at least one graph node ID is required")
	}
	if uint32(len(request.NodeIDs)) > maxNodes {
		return NeighborhoodResponse{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "graph seeds exceed max_nodes")
	}

	nodes := make(map[shoal.ID]graph.Node)
	edges := make(map[shoal.ID]graph.Edge)
	frontier := append([]shoal.ID(nil), request.NodeIDs...)
	for level := uint32(0); level <= depth && len(frontier) > 0; level++ {
		next := make([]shoal.ID, 0)
		for _, seed := range frontier {
			if uint32(len(nodes)) >= maxNodes {
				break
			}
			part, err := s.client.Neighborhood(ctx, explorer.NeighborhoodRequest{
				NodeIDs: []shoal.ID{seed}, Depth: 1, EdgeTypes: request.EdgeTypes,
			})
			if err != nil {
				return NeighborhoodResponse{}, err
			}
			sort.Slice(part.Edges, func(i, j int) bool {
				return shoal.CompareID(part.Edges[i].ID, part.Edges[j].ID) < 0
			})
			nodeByID := make(map[shoal.ID]graph.Node, len(part.Nodes))
			for _, node := range part.Nodes {
				nodeByID[node.ID] = node
			}
			if node, ok := nodeByID[seed]; ok {
				nodes[seed] = node
			}
			if level == depth {
				continue
			}
			var used uint32
			for _, edge := range part.Edges {
				if edge.From != seed && edge.To != seed {
					continue
				}
				if used >= fanout {
					break
				}
				other := edge.To
				if other == seed {
					other = edge.From
				}
				node, ok := nodeByID[other]
				if !ok {
					continue
				}
				if _, exists := nodes[other]; !exists {
					if uint32(len(nodes)) >= maxNodes {
						break
					}
					nodes[other] = node
					next = append(next, other)
				}
				edges[edge.ID] = edge
				used++
			}
		}
		frontier = deduplicateIDs(next)
	}
	result := explorer.Neighborhood{
		Nodes: make([]graph.Node, 0, len(nodes)),
		Edges: make([]graph.Edge, 0, len(edges)),
	}
	for _, node := range nodes {
		result.Nodes = append(result.Nodes, node)
	}
	for _, edge := range edges {
		if _, from := nodes[edge.From]; !from {
			continue
		}
		if _, to := nodes[edge.To]; !to {
			continue
		}
		result.Edges = append(result.Edges, edge)
	}
	sort.Slice(result.Nodes, func(i, j int) bool {
		return shoal.CompareID(result.Nodes[i].ID, result.Nodes[j].ID) < 0
	})
	sort.Slice(result.Edges, func(i, j int) bool {
		return shoal.CompareID(result.Edges[i].ID, result.Edges[j].ID) < 0
	})
	return NeighborhoodResponse{
		Snapshot: snapshot, Neighborhood: result,
		Truncated: uint32(len(nodes)) >= maxNodes,
	}, nil
}

func (s *EmbeddedService) Path(
	ctx context.Context, request PathRequest,
) (PathResponse, error) {
	_, snapshot, err := s.current(ctx, request.Snapshot)
	if err != nil {
		return PathResponse{}, err
	}
	if err := shoal.ValidateRequiredID("path source node ID", request.From); err != nil {
		return PathResponse{}, err
	}
	if err := shoal.ValidateRequiredID("path target node ID", request.To); err != nil {
		return PathResponse{}, err
	}
	depth, fanout, _, err := normalizeGraphBounds(request.MaxDepth, request.Fanout, MaxNodes)
	if err != nil {
		return PathResponse{}, err
	}
	if request.From == request.To {
		part, err := s.client.Neighborhood(ctx, explorer.NeighborhoodRequest{
			NodeIDs: []shoal.ID{request.From}, Depth: 1,
		})
		if err != nil {
			return PathResponse{}, err
		}
		for _, node := range part.Nodes {
			if node.ID == request.From {
				return PathResponse{
					Snapshot: snapshot, Path: graph.Path{Nodes: []graph.Node{node}},
				}, nil
			}
		}
	}

	type predecessor struct {
		from shoal.ID
		edge graph.Edge
	}
	nodes := make(map[shoal.ID]graph.Node)
	previous := make(map[shoal.ID]predecessor)
	seen := map[shoal.ID]struct{}{request.From: {}}
	frontier := []shoal.ID{request.From}
	found := false
	for level := uint32(0); level < depth && len(frontier) > 0 && !found; level++ {
		next := make([]shoal.ID, 0)
		for _, seed := range frontier {
			part, err := s.client.Neighborhood(ctx, explorer.NeighborhoodRequest{
				NodeIDs: []shoal.ID{seed}, Depth: 1, EdgeTypes: request.EdgeTypes,
			})
			if err != nil {
				return PathResponse{}, err
			}
			for _, node := range part.Nodes {
				nodes[node.ID] = node
			}
			sort.Slice(part.Edges, func(i, j int) bool {
				return shoal.CompareID(part.Edges[i].ID, part.Edges[j].ID) < 0
			})
			var used uint32
			for _, edge := range part.Edges {
				if edge.From != seed || used >= fanout {
					continue
				}
				used++
				if _, exists := seen[edge.To]; exists {
					continue
				}
				seen[edge.To] = struct{}{}
				previous[edge.To] = predecessor{from: seed, edge: edge}
				next = append(next, edge.To)
				if edge.To == request.To {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		frontier = next
	}
	if !found {
		return PathResponse{}, shoal.NewError(
			shoal.ErrorNotFound, "no directed path within the server bounds")
	}
	nodeIDs := []shoal.ID{request.To}
	pathEdges := make([]graph.Edge, 0)
	for current := request.To; current != request.From; {
		step := previous[current]
		pathEdges = append(pathEdges, step.edge)
		current = step.from
		nodeIDs = append(nodeIDs, current)
	}
	reverseIDs(nodeIDs)
	reverseEdges(pathEdges)
	path := graph.Path{Edges: pathEdges, Nodes: make([]graph.Node, 0, len(nodeIDs))}
	for _, id := range nodeIDs {
		node, ok := nodes[id]
		if !ok {
			return PathResponse{}, shoal.NewError(
				shoal.ErrorInternal, "path backend omitted a node")
		}
		path.Nodes = append(path.Nodes, node)
	}
	if err := path.Validate(); err != nil {
		return PathResponse{}, shoal.WrapError(shoal.ErrorInternal, "invalid path", err)
	}
	return PathResponse{Snapshot: snapshot, Path: path}, nil
}

func (s *EmbeddedService) current(
	ctx context.Context, requested Snapshot,
) ([]explorer.DocumentSummary, Snapshot, error) {
	documents, err := s.client.Documents(ctx)
	if err != nil {
		return nil, Snapshot{}, err
	}
	hash := sha256.New()
	asOf := time.Unix(0, 0).UTC()
	for _, summary := range documents {
		_, _ = fmt.Fprintf(hash, "%s\x00%s\x00", summary.Document.ID, summary.Revision.ID)
		created := summary.Revision.CreatedAt.UTC()
		if created.After(asOf) {
			asOf = created
		}
	}
	snapshot := Snapshot{ID: hex.EncodeToString(hash.Sum(nil)), AsOf: asOf}
	if requested.ID != "" && requested.ID != snapshot.ID {
		return nil, Snapshot{}, shoal.NewError(
			shoal.ErrorConflict, "requested snapshot is no longer current")
	}
	if !requested.AsOf.IsZero() && !requested.AsOf.Equal(snapshot.AsOf) {
		return nil, Snapshot{}, shoal.NewError(
			shoal.ErrorConflict, "requested as_of does not match the snapshot")
	}
	return documents, snapshot, nil
}

func normalizeLimit(limit uint32) (uint32, error) {
	if limit == 0 {
		return DefaultPageSize, nil
	}
	if limit > MaxPageSize {
		return 0, shoal.NewError(
			shoal.ErrorInvalidArgument, "page limit exceeds the server bound")
	}
	return limit, nil
}

func normalizeGraphBounds(depth, fanout, maxNodes uint32) (uint32, uint32, uint32, error) {
	if depth == 0 {
		depth = DefaultDepth
	}
	if fanout == 0 {
		fanout = DefaultFanout
	}
	if maxNodes == 0 {
		maxNodes = DefaultMaxNodes
	}
	if depth > MaxDepth {
		return 0, 0, 0, shoal.NewError(
			shoal.ErrorInvalidArgument, "graph depth exceeds the server bound")
	}
	if fanout > MaxFanout {
		return 0, 0, 0, shoal.NewError(
			shoal.ErrorInvalidArgument, "graph fanout exceeds the server bound")
	}
	if maxNodes > MaxNodes {
		return 0, 0, 0, shoal.NewError(
			shoal.ErrorInvalidArgument, "graph max_nodes exceeds the server bound")
	}
	return depth, fanout, maxNodes, nil
}

func encodeCursor(snapshot string, offset int) string {
	value := snapshot + ":" + strconv.Itoa(offset)
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeCursor(cursor, snapshot string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	value, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, shoal.NewError(shoal.ErrorInvalidArgument, "invalid document cursor")
	}
	prefix := snapshot + ":"
	if !strings.HasPrefix(string(value), prefix) {
		return 0, shoal.NewError(
			shoal.ErrorConflict, "document cursor belongs to another snapshot")
	}
	offset, err := strconv.Atoi(strings.TrimPrefix(string(value), prefix))
	if err != nil || offset < 0 {
		return 0, shoal.NewError(shoal.ErrorInvalidArgument, "invalid document cursor")
	}
	return offset, nil
}

func deduplicateIDs(values []shoal.ID) []shoal.ID {
	seen := make(map[shoal.ID]struct{}, len(values))
	result := make([]shoal.ID, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func reverseIDs(values []shoal.ID) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func reverseEdges(values []graph.Edge) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

var _ Service = (*EmbeddedService)(nil)
