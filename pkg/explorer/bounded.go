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

package explorer

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"sort"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Snapshot returns the cached logical corpus frontier.
func (e *Explorer) Snapshot(ctx context.Context) (Snapshot, error) {
	if err := contextError(ctx); err != nil {
		return Snapshot{}, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.requireOpen(); err != nil {
		return Snapshot{}, err
	}
	return e.snapshot, nil
}

// BoundedNeighborhood expands the cached adjacency index without scanning or
// materializing an unbounded one-hop neighborhood.
func (e *Explorer) BoundedNeighborhood(
	ctx context.Context, request BoundedNeighborhoodRequest,
) (BoundedNeighborhood, error) {
	if err := contextError(ctx); err != nil {
		return BoundedNeighborhood{}, err
	}
	if request.Depth == 0 || request.Fanout == 0 || request.MaxNodes == 0 {
		return BoundedNeighborhood{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "bounded graph limits must be nonzero")
	}
	if request.Direction == "" {
		request.Direction = GraphDirectionBoth
	}
	switch request.Direction {
	case GraphDirectionBoth, GraphDirectionOutgoing, GraphDirectionIncoming:
	default:
		return BoundedNeighborhood{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "unknown graph direction")
	}
	normalized, err := (NeighborhoodRequest{
		NodeIDs: request.NodeIDs, Depth: request.Depth, EdgeTypes: request.EdgeTypes,
	}).Normalize()
	if err != nil {
		return BoundedNeighborhood{}, err
	}
	if uint32(len(normalized.NodeIDs)) > request.MaxNodes {
		return BoundedNeighborhood{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "graph seeds exceed max_nodes")
	}
	typeFilter := make(map[string]struct{}, len(normalized.EdgeTypes))
	for _, edgeType := range normalized.EdgeTypes {
		typeFilter[edgeType] = struct{}{}
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.requireOpen(); err != nil {
		return BoundedNeighborhood{}, err
	}
	if e.graphErr != nil {
		return BoundedNeighborhood{}, e.graphErr
	}
	nodes := make(map[shoal.ID]graph.Node)
	seen := make(map[shoal.ID]struct{})
	frontier := make([]shoal.ID, 0, len(normalized.NodeIDs))
	for _, id := range normalized.NodeIDs {
		node, ok := e.graphNodes[id]
		if !ok {
			return BoundedNeighborhood{}, shoal.NewError(
				shoal.ErrorNotFound, "graph node not found")
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		nodes[id] = cloneNode(node)
		frontier = append(frontier, id)
	}
	edges := make(map[shoal.ID]graph.Edge)
	truncated := false
	for level := uint32(0); level < normalized.Depth && len(frontier) > 0; level++ {
		next := make([]shoal.ID, 0)
		for _, seed := range frontier {
			edgeIDs := e.adjacency[seed]
			examined := uint32(0)
			for _, edgeID := range edgeIDs {
				edge := e.graphEdges[edgeID]
				if request.Direction == GraphDirectionOutgoing && edge.From != seed {
					continue
				}
				if request.Direction == GraphDirectionIncoming && edge.To != seed {
					continue
				}
				if examined >= request.Fanout {
					truncated = true
					break
				}
				examined++
				if len(typeFilter) > 0 {
					if _, ok := typeFilter[edge.Type]; !ok {
						continue
					}
				}
				other := edge.To
				if other == seed {
					other = edge.From
				}
				if _, exists := seen[other]; !exists {
					if uint32(len(seen)) >= request.MaxNodes {
						truncated = true
						continue
					}
					seen[other] = struct{}{}
					nodes[other] = cloneNode(e.graphNodes[other])
					next = append(next, other)
				}
				edges[edge.ID] = cloneEdge(edge)
			}
		}
		frontier = next
	}
	result := Neighborhood{
		Nodes: make([]graph.Node, 0, len(nodes)),
		Edges: make([]graph.Edge, 0, len(edges)),
	}
	for _, node := range nodes {
		result.Nodes = append(result.Nodes, node)
	}
	for _, edge := range edges {
		result.Edges = append(result.Edges, edge)
	}
	sort.Slice(result.Nodes, func(i, j int) bool {
		return shoal.CompareID(result.Nodes[i].ID, result.Nodes[j].ID) < 0
	})
	sort.Slice(result.Edges, func(i, j int) bool {
		return shoal.CompareID(result.Edges[i].ID, result.Edges[j].ID) < 0
	})
	return BoundedNeighborhood{Neighborhood: result, Truncated: truncated}, nil
}

func (e *Explorer) refreshSnapshotLocked() {
	hash := sha256.New()
	documentIDs := make([]shoal.ID, 0, len(e.documents))
	for id := range e.documents {
		documentIDs = append(documentIDs, id)
	}
	sort.Slice(documentIDs, func(i, j int) bool {
		return shoal.CompareID(documentIDs[i], documentIDs[j]) < 0
	})
	asOf := e.openedAt
	for _, id := range documentIDs {
		record, err := latestRevision(e.documents[id])
		if err != nil || record == nil {
			continue
		}
		writeSnapshotString(hash, string(record.Document.ID))
		writeSnapshotString(hash, string(record.Revision.ID))
		if record.PublishedAt.After(asOf) {
			asOf = record.PublishedAt
		}
	}
	nodeIDs := make([]shoal.ID, 0, len(e.graphNodes))
	for id := range e.graphNodes {
		nodeIDs = append(nodeIDs, id)
	}
	sort.Slice(nodeIDs, func(i, j int) bool {
		return shoal.CompareID(nodeIDs[i], nodeIDs[j]) < 0
	})
	for _, id := range nodeIDs {
		node := e.graphNodes[id]
		writeSnapshotString(hash, string(node.ID))
		writeSnapshotString(hash, node.Kind)
		for _, label := range node.Labels {
			writeSnapshotString(hash, label)
		}
		writeSnapshotMetadata(hash, node.Properties)
	}
	edgeIDs := make([]shoal.ID, 0, len(e.graphEdges))
	for id := range e.graphEdges {
		edgeIDs = append(edgeIDs, id)
	}
	sort.Slice(edgeIDs, func(i, j int) bool {
		return shoal.CompareID(edgeIDs[i], edgeIDs[j]) < 0
	})
	for _, id := range edgeIDs {
		edge := e.graphEdges[id]
		writeSnapshotString(hash, string(edge.ID))
		writeSnapshotString(hash, string(edge.From))
		writeSnapshotString(hash, string(edge.To))
		writeSnapshotString(hash, edge.Type)
		var weight [8]byte
		binary.BigEndian.PutUint64(weight[:], math.Float64bits(float64(edge.Weight)))
		_, _ = hash.Write(weight[:])
		writeSnapshotMetadata(hash, edge.Properties)
		if record, ok := e.edges[id]; ok && record.PublishedAt.After(asOf) {
			asOf = record.PublishedAt
		}
	}
	sum := hash.Sum(nil)
	e.snapshot = Snapshot{
		ID: hex.EncodeToString(sum), AsOf: asOf.UTC(),
		Frontier: binary.BigEndian.Uint64(sum[:8]),
	}
}

type snapshotWriter interface {
	Write([]byte) (int, error)
}

func writeSnapshotString(writer snapshotWriter, value string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = writer.Write([]byte(value))
}

func writeSnapshotMetadata(writer snapshotWriter, metadata shoal.Metadata) {
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		writeSnapshotString(writer, key)
		writeSnapshotString(writer, metadata[key])
	}
}
