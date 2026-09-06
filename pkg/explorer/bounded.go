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
	"encoding/json"
	"math"
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Snapshot returns the cached logical corpus frontier.
func (e *Explorer) Snapshot(ctx context.Context) (Snapshot, error) {
	if err := contextError(ctx); err != nil {
		return Snapshot{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.requireOpen(); err != nil {
		return Snapshot{}, err
	}
	if err := e.ensureGraphLocked(); err != nil {
		return Snapshot{}, err
	}
	if err := e.registerSnapshotLocked(e.snapshot); err != nil {
		return Snapshot{}, err
	}
	return e.snapshot, nil
}

// ValidateSnapshot verifies that a snapshot pin names a genuine corpus
// frontier observed from this Explorer instance. Historical frontiers remain
// valid after unrelated content publications during the process lifetime.
func (e *Explorer) ValidateSnapshot(
	ctx context.Context, id shoal.ID, asOf time.Time, nodeIDs []shoal.ID,
) error {
	return e.ValidateEvidenceSnapshot(ctx, id, asOf, nodeIDs, nil, nil)
}

// ValidateEvidenceSnapshot verifies exact source nodes and edges against the
// canonical state captured by a trusted snapshot. Reusing an ID after changing
// content, revision, labels, endpoints, or edge properties fails closed.
func (e *Explorer) ValidateEvidenceSnapshot(
	ctx context.Context,
	id shoal.ID,
	asOf time.Time,
	nodeIDs []shoal.ID,
	edgeIDs []shoal.ID,
	assertions []interaction.AssertionReference,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := shoal.ValidateRequiredID("snapshot ID", id); err != nil {
		return err
	}
	if asOf.IsZero() {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "snapshot time is required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.requireOpen(); err != nil {
		return err
	}
	if err := e.ensureGraphLocked(); err != nil {
		return err
	}
	return e.validateEvidenceSnapshotLocked(
		id, asOf, nodeIDs, edgeIDs, assertions)
}

func (e *Explorer) validateEvidenceSnapshotLocked(
	id shoal.ID,
	asOf time.Time,
	nodeIDs []shoal.ID,
	edgeIDs []shoal.ID,
	assertions []interaction.AssertionReference,
) error {
	record, ok := e.snapshotHistory[string(id)]
	if !ok || !record.AsOf.Equal(asOf.UTC()) {
		return shoal.NewError(
			shoal.ErrorConflict, "snapshot pin is not a trusted corpus frontier")
	}
	nodeState, edgeState, err := e.snapshotStateLocked(id)
	if err != nil {
		return err
	}
	for _, nodeID := range nodeIDs {
		node, current := e.graphNodes[nodeID]
		expected, present := nodeState[nodeID]
		actual, digestErr := snapshotObjectDigest(node)
		if !current || interaction.IsInteractionKind(node.Kind) || !present ||
			expected == "" || digestErr != nil || actual != expected {
			return shoal.NewError(
				shoal.ErrorConflict,
				"interaction source does not match the pinned snapshot",
			)
		}
	}
	for _, edgeID := range edgeIDs {
		edge, current := e.graphEdges[edgeID]
		expected, present := edgeState[edgeID]
		actual, digestErr := snapshotObjectDigest(edge)
		if !current || interaction.IsInteractionID(edge.ID) || !present ||
			expected == "" || digestErr != nil || actual != expected {
			return shoal.NewError(
				shoal.ErrorConflict,
				"interaction source edge does not match the pinned snapshot",
			)
		}
	}
	for _, reference := range assertions {
		if err := e.validateAssertionReferenceLocked(reference); err != nil {
			return err
		}
	}
	return nil
}

func (e *Explorer) validateAssertionReferenceLocked(
	reference interaction.AssertionReference,
) error {
	if err := shoal.ValidateRequiredID(
		"interaction assertion ID", reference.AssertionID); err != nil {
		return err
	}
	if err := shoal.ValidateRequiredID(
		"interaction assertion edge ID", reference.EdgeID); err != nil {
		return err
	}
	var assertion ontology.Assertion
	found := false
	for _, candidate := range e.graphAssertions {
		if candidate.ID() == reference.AssertionID {
			assertion = candidate
			found = true
			break
		}
	}
	if !found || assertion.Origin() != reference.Origin {
		return shoal.NewError(
			shoal.ErrorConflict,
			"interaction assertion does not match current authoritative data",
		)
	}
	edge, ok := e.graphEdges[reference.EdgeID]
	if !ok {
		return shoal.NewError(
			shoal.ErrorConflict,
			"interaction assertion edge is no longer present",
		)
	}
	const (
		assertionEdgeIDProperty = "shoal.graph.edge_id"
		assertionIDProperty     = "ontology.assertion.id"
	)
	target, hasTarget := assertion.Object().ReferenceValue()
	matches := shoal.ID(
		assertion.Metadata()[assertionEdgeIDProperty],
	) == reference.EdgeID ||
		(hasTarget && assertion.ID() == reference.EdgeID &&
			edge.From == assertion.Subject() &&
			edge.To == target &&
			edge.Type == string(assertion.Predicate()) &&
			math.Float64bits(float64(edge.Weight)) ==
				math.Float64bits(float64(assertion.Confidence()))) ||
		shoal.ID(edge.Properties[assertionIDProperty]) == assertion.ID()
	if !matches {
		return shoal.NewError(
			shoal.ErrorConflict,
			"interaction assertion is not authoritative for its source edge",
		)
	}
	return nil
}

func (e *Explorer) registerSnapshotLocked(snapshot Snapshot) error {
	id := shoal.ID(snapshot.ID)
	currentNodes, currentEdges, err := e.currentSourceStateLocked()
	if err != nil {
		return err
	}
	nodeStates, removedNodes := snapshotObjectDelta(
		e.latestSnapshotNodeDigests, currentNodes)
	edgeStates, removedEdges := snapshotObjectDelta(
		e.latestSnapshotEdgeDigests, currentEdges)
	record := persistedSnapshot{
		ID: id, AsOf: snapshot.AsOf.UTC(), ParentID: e.latestSnapshotID,
		AddedNodeIDs:   snapshotStateIDs(nodeStates),
		RemovedNodeIDs: removedNodes,
		NodeStates:     nodeStates,
		RemovedEdgeIDs: removedEdges,
		EdgeStates:     edgeStates,
	}
	if existing, ok := e.snapshotHistory[snapshot.ID]; ok {
		nodes, edges, err := e.snapshotStateLocked(id)
		if err == nil && existing.AsOf.Equal(record.AsOf) &&
			snapshotObjectMapsEqual(nodes, currentNodes) &&
			snapshotObjectMapsEqual(edges, currentEdges) {
			e.latestSnapshotID = id
			e.setLatestSnapshotState(currentNodes, currentEdges)
			return nil
		}
		return shoal.NewError(
			shoal.ErrorInternal,
			"snapshot ID has conflicting observation times",
		)
	}
	if e.readOnly {
		e.snapshotHistory[snapshot.ID] = record
		e.latestSnapshotID = id
		e.setLatestSnapshotState(currentNodes, currentEdges)
		return nil
	}
	accepted, err := e.conditionalInteractionRecord(
		snapshotRecordRow(id),
		embeddedRecordSnapshot,
		record,
		recordCQV2,
		false,
	)
	if err != nil {
		return err
	}
	if !accepted {
		var winner persistedSnapshot
		found, err := e.lookupEmbeddedRecord(
			snapshotRecordRow(id), embeddedRecordSnapshot, &winner)
		if err != nil {
			return err
		}
		winner.AsOf = winner.AsOf.UTC()
		if !found || winner.ID != id || !winner.AsOf.Equal(record.AsOf) {
			return shoal.NewError(
				shoal.ErrorConflict,
				"snapshot ID is already registered with different content",
			)
		}
		e.snapshotHistory[snapshot.ID] = winner
		nodes, edges, err := e.snapshotStateLocked(id)
		if err != nil ||
			!snapshotObjectMapsEqual(nodes, currentNodes) ||
			!snapshotObjectMapsEqual(edges, currentEdges) {
			return shoal.NewError(
				shoal.ErrorConflict,
				"snapshot ID is already registered with different membership",
			)
		}
		record = winner
	}
	e.snapshotHistory[snapshot.ID] = record
	e.latestSnapshotID = id
	e.setLatestSnapshotState(currentNodes, currentEdges)
	return nil
}

func snapshotObjectMapsEqual(
	left, right map[shoal.ID]string,
) bool {
	if len(left) != len(right) {
		return false
	}
	for id, digest := range left {
		if right[id] != digest {
			return false
		}
	}
	return true
}

func (e *Explorer) restoreLatestSnapshotLocked() error {
	var latest persistedSnapshot
	found := false
	for _, record := range e.snapshotHistory {
		if !found || record.AsOf.After(latest.AsOf) ||
			(record.AsOf.Equal(latest.AsOf) &&
				shoal.CompareID(record.ID, latest.ID) > 0) {
			latest = record
			found = true
		}
	}
	if !found {
		return nil
	}
	nodes, edges, err := e.snapshotStateLocked(latest.ID)
	if err != nil {
		return err
	}
	e.latestSnapshotID = latest.ID
	e.setLatestSnapshotState(nodes, edges)
	return nil
}

func (e *Explorer) currentSourceStateLocked() (
	map[shoal.ID]string,
	map[shoal.ID]string,
	error,
) {
	nodes := make(map[shoal.ID]string, len(e.graphNodes))
	for id, node := range e.graphNodes {
		if !interaction.IsInteractionKind(node.Kind) {
			digest, err := snapshotObjectDigest(node)
			if err != nil {
				return nil, nil, err
			}
			nodes[id] = digest
		}
	}
	edges := make(map[shoal.ID]string, len(e.graphEdges))
	for id, edge := range e.graphEdges {
		from, fromOK := e.graphNodes[edge.From]
		to, toOK := e.graphNodes[edge.To]
		if interaction.IsInteractionID(id) || !fromOK || !toOK ||
			interaction.IsInteractionKind(from.Kind) ||
			interaction.IsInteractionKind(to.Kind) {
			continue
		}
		digest, err := snapshotObjectDigest(edge)
		if err != nil {
			return nil, nil, err
		}
		edges[id] = digest
	}
	return nodes, edges, nil
}

func snapshotObjectDelta(
	previous, current map[shoal.ID]string,
) ([]persistedSnapshotObject, []shoal.ID) {
	var states []persistedSnapshotObject
	var removed []shoal.ID
	for id, digest := range current {
		if previous[id] != digest {
			states = append(states, persistedSnapshotObject{
				ID: id, Digest: digest,
			})
		}
	}
	for id := range previous {
		if _, ok := current[id]; !ok {
			removed = append(removed, id)
		}
	}
	sort.Slice(states, func(i, j int) bool {
		return shoal.CompareID(states[i].ID, states[j].ID) < 0
	})
	sort.Slice(removed, func(i, j int) bool {
		return shoal.CompareID(removed[i], removed[j]) < 0
	})
	return states, removed
}

func snapshotStateIDs(states []persistedSnapshotObject) []shoal.ID {
	ids := make([]shoal.ID, len(states))
	for index, state := range states {
		ids[index] = state.ID
	}
	return ids
}

func (e *Explorer) snapshotMembershipLocked(
	id shoal.ID,
) (map[shoal.ID]struct{}, error) {
	nodes, _, err := e.snapshotStateLocked(id)
	if err != nil {
		return nil, err
	}
	membership := make(map[shoal.ID]struct{}, len(nodes))
	for nodeID := range nodes {
		membership[nodeID] = struct{}{}
	}
	return membership, nil
}

func (e *Explorer) snapshotStateLocked(
	id shoal.ID,
) (map[shoal.ID]string, map[shoal.ID]string, error) {
	var chain []persistedSnapshot
	seen := make(map[shoal.ID]struct{})
	for id != "" {
		if _, duplicate := seen[id]; duplicate {
			return nil, nil, shoal.NewError(
				shoal.ErrorInternal, "snapshot history contains a cycle")
		}
		seen[id] = struct{}{}
		record, ok := e.snapshotHistory[string(id)]
		if !ok {
			return nil, nil, shoal.NewError(
				shoal.ErrorInternal, "snapshot history is incomplete")
		}
		chain = append(chain, record)
		id = record.ParentID
	}
	nodes := make(map[shoal.ID]string)
	edges := make(map[shoal.ID]string)
	for index := len(chain) - 1; index >= 0; index-- {
		for _, nodeID := range chain[index].RemovedNodeIDs {
			delete(nodes, nodeID)
		}
		if len(chain[index].NodeStates) == 0 {
			for _, nodeID := range chain[index].AddedNodeIDs {
				nodes[nodeID] = ""
			}
		} else {
			for _, state := range chain[index].NodeStates {
				nodes[state.ID] = state.Digest
			}
		}
		for _, edgeID := range chain[index].RemovedEdgeIDs {
			delete(edges, edgeID)
		}
		for _, state := range chain[index].EdgeStates {
			edges[state.ID] = state.Digest
		}
	}
	return nodes, edges, nil
}

func (e *Explorer) setLatestSnapshotState(
	nodes, edges map[shoal.ID]string,
) {
	e.latestSnapshotNodeDigests = nodes
	e.latestSnapshotEdgeDigests = edges
}

func snapshotObjectDigest(value any) (string, error) {
	switch object := value.(type) {
	case graph.Node:
		object.Labels = append([]string(nil), object.Labels...)
		sort.Strings(object.Labels)
		if len(object.Labels) == 0 {
			object.Labels = nil
		}
		if len(object.Properties) == 0 {
			object.Properties = nil
		}
		value = object
	case graph.Edge:
		if len(object.Properties) == 0 {
			object.Properties = nil
		}
		value = object
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
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

	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.requireOpen(); err != nil {
		return BoundedNeighborhood{}, err
	}
	if err := e.ensureGraphLocked(); err != nil {
		return BoundedNeighborhood{}, err
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
	explicit := idSet(normalized.NodeIDs)
	truncated := false
	cursorEligible := len(normalized.NodeIDs) == 1 && normalized.Depth == 1
	nextAfter := request.AfterEdgeID
	continuation := false
	stopExpansion := false
	for level := uint32(0); level < normalized.Depth && len(frontier) > 0; level++ {
		next := make([]shoal.ID, 0)
		for _, seed := range frontier {
			after := shoal.ID("")
			if level == 0 && len(normalized.NodeIDs) == 1 {
				after = request.AfterEdgeID
			}
			edgeIDs, limited := e.boundedEdgeIDs(
				seed, request.Direction, request.Fanout, after)
			if limited {
				truncated = true
				if cursorEligible {
					continuation = true
				}
			}
			for _, edgeID := range edgeIDs {
				edge := e.graphEdges[edgeID]
				if len(typeFilter) > 0 {
					if _, ok := typeFilter[edge.Type]; !ok {
						if cursorEligible {
							nextAfter = edgeID
						}
						continue
					}
				}
				if excludedInteractionEdge(e.graphNodes, explicit, edge) {
					if cursorEligible {
						nextAfter = edgeID
					}
					continue
				}
				other := edge.To
				if other == seed {
					other = edge.From
				}
				if _, exists := seen[other]; !exists {
					if uint32(len(seen)) >= request.MaxNodes {
						truncated = true
						if cursorEligible && nextAfter != request.AfterEdgeID {
							continuation = true
						}
						stopExpansion = true
						break
					}
					seen[other] = struct{}{}
					nodes[other] = cloneNode(e.graphNodes[other])
					next = append(next, other)
				}
				edges[edge.ID] = cloneEdge(edge)
				if cursorEligible {
					nextAfter = edgeID
				}
			}
			if stopExpansion {
				break
			}
		}
		if stopExpansion {
			break
		}
		frontier = next
	}
	result := Neighborhood{
		Nodes:      make([]graph.Node, 0, len(nodes)),
		Edges:      make([]graph.Edge, 0, len(edges)),
		Assertions: make([]ontology.Assertion, 0, len(edges)),
	}
	for _, node := range nodes {
		result.Nodes = append(result.Nodes, node)
	}
	for _, edge := range edges {
		result.Edges = append(result.Edges, edge)
	}
	result.Assertions = e.assertionsForEdgesLocked(edges)
	sort.Slice(result.Nodes, func(i, j int) bool {
		return shoal.CompareID(result.Nodes[i].ID, result.Nodes[j].ID) < 0
	})
	sort.Slice(result.Edges, func(i, j int) bool {
		return shoal.CompareID(result.Edges[i].ID, result.Edges[j].ID) < 0
	})
	if cursorEligible && nextAfter == request.AfterEdgeID {
		continuation = false
	}
	if !continuation {
		nextAfter = ""
	}
	return BoundedNeighborhood{
		Neighborhood: result, Truncated: truncated,
		NextAfterEdgeID: nextAfter, Continuation: continuation,
	}, nil
}

func (e *Explorer) boundedEdgeIDs(
	nodeID shoal.ID, direction GraphDirection, fanout uint32, after shoal.ID,
) ([]shoal.ID, bool) {
	switch direction {
	case GraphDirectionOutgoing:
		return limitEdgeIDsAfter(e.outgoing[nodeID], fanout, after)
	case GraphDirectionIncoming:
		return limitEdgeIDsAfter(e.incoming[nodeID], fanout, after)
	}
	outgoing := afterEdge(e.outgoing[nodeID], after)
	incoming := afterEdge(e.incoming[nodeID], after)
	result := make([]shoal.ID, 0)
	left, right := 0, 0
	for uint32(len(result)) <= fanout && (left < len(outgoing) || right < len(incoming)) {
		var next shoal.ID
		switch {
		case right >= len(incoming):
			next, left = outgoing[left], left+1
		case left >= len(outgoing):
			next, right = incoming[right], right+1
		case shoal.CompareID(outgoing[left], incoming[right]) <= 0:
			next, left = outgoing[left], left+1
		default:
			next, right = incoming[right], right+1
		}
		if len(result) == 0 || result[len(result)-1] != next {
			result = append(result, next)
		}
	}
	if uint32(len(result)) > fanout {
		return result[:fanout], true
	}
	return result, false
}

func limitEdgeIDsAfter(values []shoal.ID, fanout uint32, after shoal.ID) ([]shoal.ID, bool) {
	values = afterEdge(values, after)
	if uint32(len(values)) <= fanout {
		return values, false
	}
	return values[:fanout], true
}

func afterEdge(values []shoal.ID, after shoal.ID) []shoal.ID {
	if after == "" {
		return values
	}
	index := sort.Search(len(values), func(index int) bool {
		return shoal.CompareID(values[index], after) > 0
	})
	return values[index:]
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
	writeSnapshotString(hash, "documents")
	writeSnapshotCount(hash, len(documentIDs))
	asOf := e.snapshotAnchor
	for _, id := range documentIDs {
		record, err := latestRevision(e.documents[id])
		if err != nil || record == nil {
			continue
		}
		writeSnapshotString(hash, "document")
		writeSnapshotString(hash, string(record.Document.ID))
		writeSnapshotString(hash, string(record.Revision.ID))
		if record.PublishedAt.After(asOf) {
			asOf = record.PublishedAt
		}
	}
	nodeIDs := make([]shoal.ID, 0, len(e.graphNodes))
	for id, node := range e.graphNodes {
		// The snapshot is the content frontier. Interaction records are
		// written while inference is being served, so including them would
		// invalidate every concurrent reader's pinned snapshot.
		if interaction.IsInteractionKind(node.Kind) {
			continue
		}
		nodeIDs = append(nodeIDs, id)
	}
	sort.Slice(nodeIDs, func(i, j int) bool {
		return shoal.CompareID(nodeIDs[i], nodeIDs[j]) < 0
	})
	writeSnapshotString(hash, "nodes")
	writeSnapshotCount(hash, len(nodeIDs))
	for _, id := range nodeIDs {
		node := e.graphNodes[id]
		writeSnapshotString(hash, "node")
		writeSnapshotString(hash, string(node.ID))
		writeSnapshotString(hash, node.Kind)
		writeSnapshotCount(hash, len(node.Labels))
		for _, label := range node.Labels {
			writeSnapshotString(hash, label)
		}
		writeSnapshotMetadata(hash, node.Properties)
	}
	edgeIDs := make([]shoal.ID, 0, len(e.graphEdges))
	for id, edge := range e.graphEdges {
		if interaction.IsInteractionEdgeType(edge.Type) {
			continue
		}
		edgeIDs = append(edgeIDs, id)
	}
	sort.Slice(edgeIDs, func(i, j int) bool {
		return shoal.CompareID(edgeIDs[i], edgeIDs[j]) < 0
	})
	writeSnapshotString(hash, "edges")
	writeSnapshotCount(hash, len(edgeIDs))
	for _, id := range edgeIDs {
		edge := e.graphEdges[id]
		writeSnapshotString(hash, "edge")
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
	for _, record := range e.extractions {
		if record != nil && record.PublishedAt.After(asOf) {
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
	writeSnapshotCount(writer, len(keys))
	for _, key := range keys {
		writeSnapshotString(writer, key)
		writeSnapshotString(writer, metadata[key])
	}
}

func writeSnapshotCount(writer snapshotWriter, count int) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(count))
	_, _ = writer.Write(encoded[:])
}
