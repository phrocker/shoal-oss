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
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
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

// IngestProvider is an optional service extension for mutable browser uploads.
type IngestProvider interface {
	Ingest(context.Context, IngestRequest) (IngestResponse, error)
}

// CapabilityProvider is an optional service extension for dynamic feature
// negotiation. Services that do not implement it are treated as capable for
// stable non-vector operations, while vector fails closed.
type CapabilityProvider interface {
	Capabilities(context.Context) (Capabilities, error)
}

// MetadataProvider is an optional service extension for backends that can
// negotiate both feature availability and public bounds.
type MetadataProvider interface {
	Metadata(context.Context) (MetadataResponse, error)
}

type vectorAvailabilityProvider interface {
	VectorAvailable(context.Context) (bool, error)
}

// suppressionCountingRetriever is implemented by an authorized backing client
// that can report, alongside a retrieval, how many current documents
// authorization withheld from the caller. It is optional: a client that does
// not implement it is treated as withholding nothing, so the signal fails
// closed to "0 withheld" rather than fabricating a count.
type suppressionCountingRetriever interface {
	RetrieveWithSuppressed(
		context.Context, retrieval.Request,
	) (retrieval.Response, uint32, error)
}

// suppressionCountingLister is the Documents-listing counterpart to
// suppressionCountingRetriever and is optional under the same fail-closed rule.
type suppressionCountingLister interface {
	DocumentsWithSuppressed(
		context.Context,
	) ([]explorer.DocumentSummary, uint32, error)
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

func (s *EmbeddedService) Capabilities(ctx context.Context) (Capabilities, error) {
	capabilities := AllCapabilities()
	capabilities.Vector = false
	provider, ok := s.client.(vectorAvailabilityProvider)
	if !ok {
		return capabilities, nil
	}
	available, err := provider.VectorAvailable(ctx)
	if err != nil {
		return Capabilities{}, err
	}
	capabilities.Vector = available
	return capabilities, nil
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
	documents, suppressed, err := s.documentsCountingSuppressed(ctx)
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
		Snapshot:   snapshot,
		Documents:  append([]explorer.DocumentSummary(nil), documents[offset:end]...),
		Suppressed: suppressed,
	}
	if end < len(documents) {
		response.NextCursor = encodeCursor(snapshot.ID, end)
	}
	return response, nil
}

// documentsCountingSuppressed lists the authorized documents and, when the
// backing client can report it, the corpus-wide number of current documents
// authorization withheld from this identity. See retrieveCountingSuppressed for
// the amplification risk of disclosing this count; the same caveat applies to
// the document listing, whose withheld count reveals roughly how much of the
// shared corpus the identity cannot see.
func (s *EmbeddedService) documentsCountingSuppressed(
	ctx context.Context,
) ([]explorer.DocumentSummary, uint32, error) {
	if counter, ok := s.client.(suppressionCountingLister); ok {
		return counter.DocumentsWithSuppressed(ctx)
	}
	documents, err := s.client.Documents(ctx)
	return documents, 0, err
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
	response, suppressed, err := s.retrieveCountingSuppressed(ctx, query)
	if err != nil {
		return RetrievalResponse{}, err
	}
	if err := validateRetrievalResponse(response, query); err != nil {
		return RetrievalResponse{}, shoal.WrapError(
			shoal.ErrorInternal, "invalid retrieval response", err)
	}
	if err := s.confirmSnapshot(ctx, snapshot); err != nil {
		return RetrievalResponse{}, err
	}
	return RetrievalResponse{
		Snapshot: snapshot, Retrieval: response, Suppressed: suppressed,
	}, nil
}

// retrieveCountingSuppressed runs the authorized retrieval and, when the backing
// client can report it, the number of current documents authorization withheld
// from this identity and therefore never searched.
//
// Amplification risk. This count is a real disclosure. Because a caller can
// re-run retrieval as the corpus changes, and vary the request, and watch the
// number move, the count is a coarse oracle for the existence and rough volume
// of content the caller is not allowed to read. The count is the entire leak —
// no identifiers, labels, or snippets accompany it — but it is still a leak. It
// is emitted deliberately, on an explicit product decision, so a short or empty
// answer can never be silently mistaken for "nothing exists". A future policy
// control may need to coarsen or suppress this count for sensitive
// authorization domains; that control is intentionally not built here.
func (s *EmbeddedService) retrieveCountingSuppressed(
	ctx context.Context, query retrieval.Request,
) (retrieval.Response, uint32, error) {
	if counter, ok := s.client.(suppressionCountingRetriever); ok {
		return counter.RetrieveWithSuppressed(ctx, query)
	}
	response, err := s.client.Retrieve(ctx, query)
	return response, 0, err
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
	normalizedBase, err := (explorer.NeighborhoodRequest{
		NodeIDs: request.NodeIDs, Depth: depth, EdgeTypes: request.EdgeTypes,
	}).Normalize()
	if err != nil {
		return NeighborhoodResponse{}, err
	}
	request.NodeIDs = normalizedBase.NodeIDs
	request.EdgeTypes = normalizedBase.EdgeTypes
	if err := validateEdgeTypes(request.EdgeTypes); err != nil {
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
	normalizedRequest := request
	normalizedRequest.Depth = depth
	normalizedRequest.Fanout = fanout
	normalizedRequest.MaxNodes = maxNodes
	afterEdgeID, err := decodeGraphCursor(
		request.Cursor, snapshot.ID, normalizedRequest)
	if err != nil {
		return NeighborhoodResponse{}, err
	}

	result, err := s.client.BoundedNeighborhood(ctx, explorer.BoundedNeighborhoodRequest{
		NodeIDs: request.NodeIDs, Depth: depth, Fanout: fanout,
		MaxNodes: maxNodes, EdgeTypes: request.EdgeTypes,
		AfterEdgeID: afterEdgeID,
	})
	if err != nil {
		return NeighborhoodResponse{}, err
	}
	if err := s.confirmSnapshot(ctx, snapshot); err != nil {
		return NeighborhoodResponse{}, err
	}
	response := NeighborhoodResponse{
		Snapshot: snapshot, Neighborhood: result.Neighborhood,
		Truncated: result.Truncated,
	}
	if result.Continuation {
		response.NextCursor = encodeGraphCursor(
			snapshot.ID, normalizedRequest, result.NextAfterEdgeID)
	}
	return response, nil
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
	normalized, err := (explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{request.From}, Depth: depth, EdgeTypes: request.EdgeTypes,
	}).Normalize()
	if err != nil {
		return PathResponse{}, err
	}
	request.EdgeTypes = normalized.EdgeTypes
	if err := validateEdgeTypes(request.EdgeTypes); err != nil {
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

func validateEdgeTypes(edgeTypes []string) error {
	if uint32(len(edgeTypes)) > MaxEdgeTypes {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "edge type count exceeds the server bound")
	}
	return nil
}

func validateRetrievalResponse(response retrieval.Response, query retrieval.Request) error {
	if err := response.ValidateFor(query); err != nil {
		return err
	}
	for _, result := range response.Results {
		if uint32(len(result.Evidence)) > MaxEvidencePerResult {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"retrieval evidence count exceeds the server bound",
			)
		}
	}
	return nil
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

type graphCursorPayload struct {
	Snapshot  string `json:"snapshot"`
	Signature string `json:"signature"`
	After     string `json:"after"`
}

func encodeGraphCursor(
	snapshot string, request NeighborhoodRequest, after shoal.ID,
) string {
	encoded, _ := json.Marshal(graphCursorPayload{
		Snapshot: snapshot, Signature: graphRequestSignature(request),
		After: encodeID(after),
	})
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func decodeGraphCursor(
	cursor, snapshot string, request NeighborhoodRequest,
) (shoal.ID, error) {
	if cursor == "" {
		return "", nil
	}
	if len(request.NodeIDs) != 1 || request.Depth != 1 {
		return "", shoal.NewError(
			shoal.ErrorInvalidArgument,
			"graph cursors require one seed and depth 1",
		)
	}
	encoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", shoal.NewError(shoal.ErrorInvalidArgument, "invalid graph cursor")
	}
	var payload graphCursorPayload
	if err := json.Unmarshal(encoded, &payload); err != nil {
		return "", shoal.NewError(shoal.ErrorInvalidArgument, "invalid graph cursor")
	}
	if payload.Snapshot != snapshot {
		return "", shoal.NewError(
			shoal.ErrorConflict, "graph cursor belongs to another snapshot")
	}
	if payload.Signature != graphRequestSignature(request) {
		return "", shoal.NewError(
			shoal.ErrorInvalidArgument, "graph cursor does not match the request")
	}
	after, err := decodeOptionalID(payload.After)
	if err != nil {
		return "", shoal.NewError(shoal.ErrorInvalidArgument, "invalid graph cursor")
	}
	return after, nil
}

func graphRequestSignature(request NeighborhoodRequest) string {
	value := struct {
		NodeIDs   []string `json:"node_ids"`
		Depth     uint32   `json:"depth"`
		Fanout    uint32   `json:"fanout"`
		MaxNodes  uint32   `json:"max_nodes"`
		EdgeTypes []string `json:"edge_types"`
	}{
		Depth: request.Depth, Fanout: request.Fanout,
		MaxNodes: request.MaxNodes, EdgeTypes: request.EdgeTypes,
	}
	for _, id := range request.NodeIDs {
		value.NodeIDs = append(value.NodeIDs, encodeID(id))
	}
	encoded, _ := json.Marshal(value)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
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
