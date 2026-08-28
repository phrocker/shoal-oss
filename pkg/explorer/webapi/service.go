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
	"encoding/base64"
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
	client explorer.BoundedClient
}

// NewEmbeddedService creates a local service without exposing the embedded
// engine through the service contract.
func NewEmbeddedService(client explorer.BoundedClient) (*EmbeddedService, error) {
	if client == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "explorer client is required")
	}
	return &EmbeddedService{client: client}, nil
}

func (s *EmbeddedService) Documents(
	ctx context.Context, request DocumentsRequest,
) (DocumentsResponse, error) {
	snapshot, err := s.pin(ctx, request.Snapshot)
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
	documents, err := s.client.Documents(ctx)
	if err != nil {
		return DocumentsResponse{}, err
	}
	if err := s.confirmSnapshot(ctx, snapshot); err != nil {
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
	snapshot, err := s.pin(ctx, request.Snapshot)
	if err != nil {
		return DocumentResponse{}, err
	}
	view, err := s.client.Document(ctx, request.DocumentID, request.RevisionID)
	if err != nil {
		return DocumentResponse{}, err
	}
	if err := s.confirmSnapshot(ctx, snapshot); err != nil {
		return DocumentResponse{}, err
	}
	return DocumentResponse{Snapshot: snapshot, Document: view}, nil
}

func (s *EmbeddedService) Retrieve(
	ctx context.Context, request RetrievalRequest,
) (RetrievalResponse, error) {
	snapshot, err := s.pin(ctx, request.Snapshot)
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
	if err := s.confirmSnapshot(ctx, snapshot); err != nil {
		return RetrievalResponse{}, err
	}
	return RetrievalResponse{Snapshot: snapshot, Retrieval: response}, nil
}

func (s *EmbeddedService) Neighborhood(
	ctx context.Context, request NeighborhoodRequest,
) (NeighborhoodResponse, error) {
	snapshot, err := s.pin(ctx, request.Snapshot)
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

	result, err := s.client.BoundedNeighborhood(ctx, explorer.BoundedNeighborhoodRequest{
		NodeIDs: request.NodeIDs, Depth: depth, Fanout: fanout,
		MaxNodes: maxNodes, EdgeTypes: request.EdgeTypes,
	})
	if err != nil {
		return NeighborhoodResponse{}, err
	}
	if err := s.confirmSnapshot(ctx, snapshot); err != nil {
		return NeighborhoodResponse{}, err
	}
	return NeighborhoodResponse{
		Snapshot: snapshot, Neighborhood: result.Neighborhood,
		Truncated: result.Truncated,
	}, nil
}

func (s *EmbeddedService) Path(
	ctx context.Context, request PathRequest,
) (PathResponse, error) {
	snapshot, err := s.pin(ctx, request.Snapshot)
	if err != nil {
		return PathResponse{}, err
	}
	if err := shoal.ValidateRequiredID("path source node ID", request.From); err != nil {
		return PathResponse{}, err
	}
	if err := shoal.ValidateRequiredID("path target node ID", request.To); err != nil {
		return PathResponse{}, err
	}
	depth, fanout, maxNodes, err := normalizeGraphBounds(
		request.MaxDepth, request.Fanout, MaxNodes)
	if err != nil {
		return PathResponse{}, err
	}
	bounded, err := s.client.BoundedNeighborhood(ctx, explorer.BoundedNeighborhoodRequest{
		NodeIDs: []shoal.ID{request.From}, Depth: depth, Fanout: fanout,
		MaxNodes: maxNodes, EdgeTypes: request.EdgeTypes,
		Direction: explorer.GraphDirectionOutgoing,
	})
	if err != nil {
		return PathResponse{}, err
	}
	path, err := directedPath(
		bounded.Neighborhood, request.From, request.To, depth)
	if err != nil {
		return PathResponse{}, err
	}
	if err := path.Validate(); err != nil {
		return PathResponse{}, shoal.WrapError(shoal.ErrorInternal, "invalid path", err)
	}
	if err := s.confirmSnapshot(ctx, snapshot); err != nil {
		return PathResponse{}, err
	}
	return PathResponse{Snapshot: snapshot, Path: path}, nil
}

func (s *EmbeddedService) pin(
	ctx context.Context, requested Snapshot,
) (Snapshot, error) {
	before, err := s.client.Snapshot(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot := fromExplorerSnapshot(before)
	if err := validateRequestedSnapshot(requested, snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func (s *EmbeddedService) confirmSnapshot(ctx context.Context, expected Snapshot) error {
	current, err := s.client.Snapshot(ctx)
	if err != nil {
		return err
	}
	if fromExplorerSnapshot(current) != expected {
		return shoal.NewError(
			shoal.ErrorConflict, "corpus changed while serving the snapshot")
	}
	return nil
}

func validateRequestedSnapshot(requested, current Snapshot) error {
	if requested.ID != "" && requested.ID != current.ID {
		return shoal.NewError(
			shoal.ErrorConflict, "requested snapshot is no longer current")
	}
	if !requested.AsOf.IsZero() && !requested.AsOf.Equal(current.AsOf) {
		return shoal.NewError(
			shoal.ErrorConflict, "requested as_of does not match the snapshot")
	}
	if requested.Frontier != 0 && requested.Frontier != current.Frontier {
		return shoal.NewError(
			shoal.ErrorConflict, "requested frontier does not match the snapshot")
	}
	return nil
}

func fromExplorerSnapshot(snapshot explorer.Snapshot) Snapshot {
	return Snapshot{
		ID: snapshot.ID, AsOf: snapshot.AsOf, Frontier: snapshot.Frontier,
	}
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

func directedPath(
	neighborhood explorer.Neighborhood, from, to shoal.ID, maxDepth uint32,
) (graph.Path, error) {
	nodes := make(map[shoal.ID]graph.Node, len(neighborhood.Nodes))
	for _, node := range neighborhood.Nodes {
		nodes[node.ID] = node
	}
	if _, ok := nodes[from]; !ok {
		return graph.Path{}, shoal.NewError(shoal.ErrorNotFound, "path source node not found")
	}
	if from == to {
		return graph.Path{Nodes: []graph.Node{nodes[from]}}, nil
	}
	outgoing := make(map[shoal.ID][]graph.Edge)
	for _, edge := range neighborhood.Edges {
		outgoing[edge.From] = append(outgoing[edge.From], edge)
	}
	for id := range outgoing {
		sort.Slice(outgoing[id], func(i, j int) bool {
			return shoal.CompareID(outgoing[id][i].ID, outgoing[id][j].ID) < 0
		})
	}
	type predecessor struct {
		from shoal.ID
		edge graph.Edge
	}
	previous := make(map[shoal.ID]predecessor)
	seen := map[shoal.ID]struct{}{from: {}}
	frontier := []shoal.ID{from}
	found := false
	for level := uint32(0); level < maxDepth && len(frontier) > 0 && !found; level++ {
		next := make([]shoal.ID, 0)
		for _, id := range frontier {
			for _, edge := range outgoing[id] {
				if _, ok := seen[edge.To]; ok {
					continue
				}
				seen[edge.To] = struct{}{}
				previous[edge.To] = predecessor{from: id, edge: edge}
				next = append(next, edge.To)
				if edge.To == to {
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
		return graph.Path{}, shoal.NewError(
			shoal.ErrorNotFound, "no directed path within the server bounds")
	}
	nodeIDs := []shoal.ID{to}
	pathEdges := make([]graph.Edge, 0)
	for current := to; current != from; {
		step := previous[current]
		pathEdges = append(pathEdges, step.edge)
		current = step.from
		nodeIDs = append(nodeIDs, current)
	}
	for left, right := 0, len(nodeIDs)-1; left < right; left, right = left+1, right-1 {
		nodeIDs[left], nodeIDs[right] = nodeIDs[right], nodeIDs[left]
	}
	for left, right := 0, len(pathEdges)-1; left < right; left, right = left+1, right-1 {
		pathEdges[left], pathEdges[right] = pathEdges[right], pathEdges[left]
	}
	path := graph.Path{Edges: pathEdges, Nodes: make([]graph.Node, 0, len(nodeIDs))}
	for _, id := range nodeIDs {
		path.Nodes = append(path.Nodes, nodes[id])
	}
	return path, nil
}

var _ Service = (*EmbeddedService)(nil)
