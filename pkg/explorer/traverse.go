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
	"time"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Cross-session provenance traversal.
//
// These are explicit, kind-scoped entry points, exactly like Interactions and
// InteractionSubgraph. Nothing here is reachable from Retrieve or from
// expanding a source node's neighborhood: walking provenance is an operator
// and auditor capability, never something an inference can do to itself.

// InteractionTouch is one recorded interaction that touched a source node,
// with the retrieved/cited distinction preserved. A node can be both: what a
// model was shown and what it cited are different questions, and only the
// larger set bounds exposure.
type InteractionTouch struct {
	// InteractionID is the interaction.session or interaction.fold node that
	// touched the source node.
	InteractionID shoal.ID
	Kind          string
	Retrieved     bool
	Cited         bool
	RecordedAt    time.Time
	Visibility    string
}

// InteractionOverlap is another recorded interaction that touched at least one
// of the same source nodes as the one asked about.
type InteractionOverlap struct {
	InteractionID   shoal.ID
	Kind            string
	SharedNodeIDs   []shoal.ID
	SharedRetrieved int
	SharedCited     int
	RecordedAt      time.Time
	Visibility      string
}

// InteractionsTouching lists every recorded session and fold that retrieved or
// cited a given source node.
//
// It refuses an interaction node: provenance is walked from source evidence
// outward, and treating a derived node as the subject of the walk is the same
// mistake as treating it as source evidence.
func (e *Explorer) InteractionsTouching(
	ctx context.Context, nodeID shoal.ID,
) ([]InteractionTouch, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := shoal.ValidateRequiredID("source node ID", nodeID); err != nil {
		return nil, err
	}
	if interaction.IsInteractionID(nodeID) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"cross-session traversal starts from a source node, "+
				"not from an interaction node",
		)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.requireOpen(); err != nil {
		return nil, err
	}
	if err := e.ensureGraphLocked(); err != nil {
		return nil, err
	}
	if node, ok := e.graphNodes[nodeID]; ok &&
		interaction.IsInteractionKind(node.Kind) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"cross-session traversal starts from a source node, "+
				"not from an interaction node",
		)
	}
	var touches []InteractionTouch
	e.eachLiveInteractionLocked(func(view interactionView) {
		touched := interaction.TouchedNodes(view.nodes, view.edges)
		retrieved := containsID(touched.RetrievedNodeIDs, nodeID)
		cited := containsID(touched.CitedNodeIDs, nodeID)
		if !retrieved && !cited {
			return
		}
		touches = append(touches, InteractionTouch{
			InteractionID: view.id,
			Kind:          view.kind,
			Retrieved:     retrieved,
			Cited:         cited,
			RecordedAt:    view.recordedAt,
			Visibility:    view.visibility,
		})
	})
	sort.Slice(touches, func(i, j int) bool {
		return shoal.CompareID(
			touches[i].InteractionID, touches[j].InteractionID,
		) < 0
	})
	return touches, nil
}

// RelatedInteractions walks provenance across sessions: given one recorded
// session or fold, it returns every other recorded interaction that touched at
// least one of the same source nodes, and which nodes those were.
//
// The walk goes interaction to source node to interaction. It never traverses
// an interaction-to-interaction edge other than a fold's own membership, so a
// session can never be reached as though it were evidence for another.
func (e *Explorer) RelatedInteractions(
	ctx context.Context, interactionID shoal.ID,
) ([]InteractionOverlap, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := shoal.ValidateRequiredID(
		"interaction ID", interactionID,
	); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.requireOpen(); err != nil {
		return nil, err
	}
	if err := e.ensureGraphLocked(); err != nil {
		return nil, err
	}
	origin, ok := e.interactionViewLocked(interactionID)
	if !ok {
		return nil, shoal.NewError(
			shoal.ErrorNotFound, "interaction not found")
	}
	if e.subgraphVisibilityIsStaleLocked(
		origin.nodes, origin.edges, origin.visibility) {
		return nil, staleDerivedVisibilityError()
	}
	originTouched := interaction.TouchedNodes(origin.nodes, origin.edges)
	originRetrieved := idSet(originTouched.RetrievedNodeIDs)
	originCited := idSet(originTouched.CitedNodeIDs)
	if len(originRetrieved) == 0 && len(originCited) == 0 {
		return []InteractionOverlap{}, nil
	}

	overlaps := make([]InteractionOverlap, 0)
	e.eachLiveInteractionLocked(func(view interactionView) {
		if view.id == interactionID {
			return
		}
		touched := interaction.TouchedNodes(view.nodes, view.edges)
		shared := make(map[shoal.ID]struct{})
		sharedRetrieved := 0
		sharedCited := 0
		for _, id := range touched.RetrievedNodeIDs {
			if _, ok := originRetrieved[id]; !ok {
				if _, ok := originCited[id]; !ok {
					continue
				}
			}
			shared[id] = struct{}{}
			sharedRetrieved++
		}
		for _, id := range touched.CitedNodeIDs {
			if _, ok := originRetrieved[id]; !ok {
				if _, ok := originCited[id]; !ok {
					continue
				}
			}
			shared[id] = struct{}{}
			sharedCited++
		}
		if len(shared) == 0 {
			return
		}
		sharedIDs := make([]shoal.ID, 0, len(shared))
		for id := range shared {
			sharedIDs = append(sharedIDs, id)
		}
		sort.Slice(sharedIDs, func(i, j int) bool {
			return shoal.CompareID(sharedIDs[i], sharedIDs[j]) < 0
		})
		overlaps = append(overlaps, InteractionOverlap{
			InteractionID:   view.id,
			Kind:            view.kind,
			SharedNodeIDs:   sharedIDs,
			SharedRetrieved: sharedRetrieved,
			SharedCited:     sharedCited,
			RecordedAt:      view.recordedAt,
			Visibility:      view.visibility,
		})
	})
	sort.Slice(overlaps, func(i, j int) bool {
		return shoal.CompareID(
			overlaps[i].InteractionID, overlaps[j].InteractionID,
		) < 0
	})
	return overlaps, nil
}

// interactionView is the read-only shape shared by a recorded session and a
// fold for traversal purposes.
type interactionView struct {
	id         shoal.ID
	kind       string
	nodes      []graph.Node
	edges      []graph.Edge
	recordedAt time.Time
	visibility string
}

// eachLiveInteractionLocked visits every session and fold that has not been
// deleted. Callers must hold the lock; the slices handed to visit are the
// stored records and must not be retained or mutated.
func (e *Explorer) eachLiveInteractionLocked(visit func(interactionView)) {
	for _, record := range e.interactions {
		if record.Deleted {
			continue
		}
		if e.subgraphVisibilityIsStaleLocked(
			record.Nodes, record.Edges, record.Visibility) {
			continue
		}
		visit(interactionView{
			id:         record.SessionID,
			kind:       interaction.KindSession,
			nodes:      record.Nodes,
			edges:      record.Edges,
			recordedAt: record.RecordedAt,
			visibility: record.Visibility,
		})
	}
	for _, record := range e.folds {
		if record.Deleted {
			continue
		}
		if e.subgraphVisibilityIsStaleLocked(
			record.Nodes, record.Edges, record.Visibility) {
			continue
		}
		visit(interactionView{
			id:         record.FoldID,
			kind:       interaction.KindFold,
			nodes:      record.Nodes,
			edges:      record.Edges,
			recordedAt: record.FoldedAt,
			visibility: record.Visibility,
		})
	}
}

// subgraphVisibilityIsStaleLocked reports whether a live record's stored
// visibility no longer equals what its touched source nodes require now, or can
// no longer be derived at all. Provenance traversal withholds stale records so
// a tightening re-ingest cannot leave a previously derived record disclosed
// under its now under-labelled stored visibility. See issue #273. The caller
// must hold at least e.mu.RLock.
func (e *Explorer) subgraphVisibilityIsStaleLocked(
	nodes []graph.Node, edges []graph.Edge, stored string,
) bool {
	current, err := e.currentSubgraphVisibilityLocked(nodes, edges)
	return err != nil || current != stored
}

func (e *Explorer) interactionViewLocked(id shoal.ID) (interactionView, bool) {
	if record, ok := e.interactions[id]; ok && !record.Deleted {
		return interactionView{
			id:         record.SessionID,
			kind:       interaction.KindSession,
			nodes:      record.Nodes,
			edges:      record.Edges,
			recordedAt: record.RecordedAt,
			visibility: record.Visibility,
		}, true
	}
	if record, ok := e.folds[id]; ok && !record.Deleted {
		return interactionView{
			id:         record.FoldID,
			kind:       interaction.KindFold,
			nodes:      record.Nodes,
			edges:      record.Edges,
			recordedAt: record.FoldedAt,
			visibility: record.Visibility,
		}, true
	}
	return interactionView{}, false
}

func containsID(ids []shoal.ID, target shoal.ID) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}
