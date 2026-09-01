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

// InteractionSummary describes one recorded or tombstoned interaction session
// without materializing its subgraph.
type InteractionSummary struct {
	SessionID  shoal.ID
	RecordedAt time.Time
	Visibility string
	NodeCount  int
	EdgeCount  int
	Deleted    bool
	DeletedAt  time.Time
}

type persistedInteraction struct {
	SessionID  shoal.ID
	Nodes      []graph.Node
	Edges      []graph.Edge
	Visibility string
	RecordedAt time.Time
	Deleted    bool
	DeletedAt  time.Time
}

type persistedInteractionSink struct {
	CheckedAt time.Time
}

// EnsureInteractionSink verifies at setup time that this corpus can durably
// record interactions. Capture is part of serving an inference, so a corpus
// that cannot accept an interaction record must refuse to serve one at all,
// with a clear diagnostic here rather than an opaque failure at first write.
func (e *Explorer) EnsureInteractionSink(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.requireOpen(); err != nil {
		return err
	}
	if e.readOnly {
		return shoal.NewError(
			shoal.ErrorUnavailable,
			"corpus is open read-only and cannot record interactions; "+
				"inference requires a writable interaction sink",
		)
	}
	if err := e.writeRecord(
		interactionSinkRow,
		embeddedRecordInteractionSink,
		persistedInteractionSink{CheckedAt: time.Now().UTC()},
	); err != nil {
		return shoal.WrapError(
			shoal.ErrorUnavailable,
			"corpus has no writable interaction sink; "+
				"inference requires a writable interaction sink",
			err,
		)
	}
	return nil
}

// RecordInteraction durably stores one interaction session as reserved
// interaction.* nodes and edges in this corpus.
//
// The record's visibility is the conjunction of every visibility label of
// every source node the session touched, retrieved as well as cited. It is
// never derived from the asker's grants. If any touched node cannot be
// resolved, nothing is written.
func (e *Explorer) RecordInteraction(
	ctx context.Context, session interaction.Session,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if err := session.Validate(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.requireOpen(); err != nil {
		return err
	}
	if err := e.requireWritableLocked(); err != nil {
		return err
	}
	if err := e.ensureGraphLocked(); err != nil {
		return err
	}
	if existing, ok := e.interactions[session.ID]; ok {
		if existing.Deleted {
			return shoal.NewError(
				shoal.ErrorConflict,
				"interaction session ID was explicitly deleted and cannot be reused",
			)
		}
		return shoal.NewError(
			shoal.ErrorConflict, "interaction session ID already exists")
	}
	// Sessions and folds are distinct maps but share one node namespace in the
	// corpus graph, so an ID taken by either would silently overwrite the other
	// during a graph rebuild and leave two records claiming one identity.
	if _, ok := e.folds[session.ID]; ok {
		return shoal.NewError(
			shoal.ErrorConflict,
			"interaction session ID is already used by a fold",
		)
	}
	subgraph, err := session.Subgraph(e.visibilityResolverLocked())
	if err != nil {
		return err
	}
	record := persistedInteraction{
		SessionID:  session.ID,
		Nodes:      subgraph.Nodes,
		Edges:      subgraph.Edges,
		Visibility: interaction.Expression(subgraph.Visibility),
		RecordedAt: session.RecordedAt.UTC(),
	}
	if err := validatePersistedInteraction(record); err != nil {
		return err
	}
	if err := e.writeRecord(
		interactionRecordRow(session.ID), embeddedRecordInteraction, record,
	); err != nil {
		return err
	}
	e.interactions[session.ID] = &record
	return e.rebuildCurrentGraphLocked()
}

// DeleteInteraction removes one interaction session's nodes and edges and
// leaves a tombstone node in their place. Retention is explicit deletion only;
// there is no TTL, and a deletion is itself auditable.
func (e *Explorer) DeleteInteraction(
	ctx context.Context, sessionID shoal.ID,
) (interaction.Tombstone, error) {
	if err := contextError(ctx); err != nil {
		return interaction.Tombstone{}, err
	}
	if err := shoal.ValidateRequiredID(
		"interaction session ID", sessionID,
	); err != nil {
		return interaction.Tombstone{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.requireOpen(); err != nil {
		return interaction.Tombstone{}, err
	}
	if err := e.requireWritableLocked(); err != nil {
		return interaction.Tombstone{}, err
	}
	existing, ok := e.interactions[sessionID]
	if !ok {
		return interaction.Tombstone{}, shoal.NewError(
			shoal.ErrorNotFound, "interaction session not found")
	}
	if existing.Deleted {
		return interaction.Tombstone{}, shoal.NewError(
			shoal.ErrorConflict, "interaction session is already deleted")
	}
	// A session that a live fold summarizes cannot be deleted out from under
	// it, because the fold would keep a rehydratable copy of provenance the
	// operator just asked to destroy. Delete the fold first.
	if folds := e.foldsReferencingLocked(sessionID); len(folds) > 0 {
		return interaction.Tombstone{}, shoal.NewError(
			shoal.ErrorConflict,
			"interaction session is folded into a summary; "+
				"delete the fold before deleting the session",
		)
	}
	visibility, err := interaction.ParseVisibility(existing.Visibility)
	if err != nil {
		return interaction.Tombstone{}, err
	}
	tombstone := interaction.Tombstone{
		SessionID:  sessionID,
		DeletedAt:  time.Now().UTC(),
		NodeCount:  len(existing.Nodes),
		EdgeCount:  len(existing.Edges),
		Visibility: visibility,
	}
	node, err := tombstone.Node()
	if err != nil {
		return interaction.Tombstone{}, err
	}
	record := persistedInteraction{
		SessionID:  sessionID,
		Nodes:      []graph.Node{node},
		Visibility: existing.Visibility,
		RecordedAt: existing.RecordedAt,
		Deleted:    true,
		DeletedAt:  tombstone.DeletedAt,
	}
	if err := validatePersistedInteraction(record); err != nil {
		return interaction.Tombstone{}, err
	}
	if err := e.writeRecord(
		interactionRecordRow(sessionID), embeddedRecordInteraction, record,
	); err != nil {
		return interaction.Tombstone{}, err
	}
	e.interactions[sessionID] = &record
	if err := e.rebuildCurrentGraphLocked(); err != nil {
		return interaction.Tombstone{}, err
	}
	return tombstone, nil
}

// Interactions lists recorded interaction sessions. This is the explicit
// kind-scoped query; interaction records are never returned by Retrieve and
// are never discovered by graph expansion from a source node.
func (e *Explorer) Interactions(ctx context.Context) ([]InteractionSummary, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.requireOpen(); err != nil {
		return nil, err
	}
	summaries := make([]InteractionSummary, 0, len(e.interactions))
	for _, record := range e.interactions {
		summaries = append(summaries, InteractionSummary{
			SessionID:  record.SessionID,
			RecordedAt: record.RecordedAt,
			Visibility: record.Visibility,
			NodeCount:  len(record.Nodes),
			EdgeCount:  len(record.Edges),
			Deleted:    record.Deleted,
			DeletedAt:  record.DeletedAt,
		})
	}
	sort.Slice(summaries, func(i, j int) bool {
		return shoal.CompareID(summaries[i].SessionID, summaries[j].SessionID) < 0
	})
	return summaries, nil
}

// InteractionSubgraph returns one recorded session's nodes and edges. This is
// the explicit traversal entry point; it is never reached implicitly.
func (e *Explorer) InteractionSubgraph(
	ctx context.Context, sessionID shoal.ID,
) (Neighborhood, error) {
	if err := contextError(ctx); err != nil {
		return Neighborhood{}, err
	}
	if err := shoal.ValidateRequiredID(
		"interaction session ID", sessionID,
	); err != nil {
		return Neighborhood{}, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if err := e.requireOpen(); err != nil {
		return Neighborhood{}, err
	}
	record, ok := e.interactions[sessionID]
	if !ok {
		return Neighborhood{}, shoal.NewError(
			shoal.ErrorNotFound, "interaction session not found")
	}
	result := Neighborhood{
		Nodes: make([]graph.Node, 0, len(record.Nodes)),
		Edges: make([]graph.Edge, 0, len(record.Edges)),
	}
	for _, node := range record.Nodes {
		result.Nodes = append(result.Nodes, cloneNode(node))
	}
	for _, edge := range record.Edges {
		result.Edges = append(result.Edges, cloneEdge(edge))
	}
	return result, nil
}

// visibilityResolverLocked resolves the declared visibility labels of a source
// node. It fails closed: an unknown node, or an interaction node presented as
// if it were source evidence, produces an error rather than an empty label set.
func (e *Explorer) visibilityResolverLocked() interaction.VisibilityResolver {
	return func(id shoal.ID) ([]string, error) {
		node, ok := e.graphNodes[id]
		if !ok {
			return nil, shoal.NewError(
				shoal.ErrorUnavailable,
				"interaction touched node "+string(id)+
					", which is no longer in the corpus graph, so its visibility "+
					"cannot be derived; this usually means the node was removed or "+
					"replaced by a concurrent re-ingest between retrieval and "+
					"recording. The request fails by design rather than recording "+
					"an interaction with an under-labelled visibility. Retry the "+
					"request against the updated corpus",
			)
		}
		if interaction.IsInteractionKind(node.Kind) {
			return nil, shoal.NewError(
				shoal.ErrorUnavailable,
				"interaction cannot treat another interaction node as source "+
					"evidence: node "+string(id)+" has kind "+node.Kind,
			)
		}
		return interaction.NodeVisibility(node)
	}
}

func (e *Explorer) requireWritableLocked() error {
	if e.readOnly {
		return shoal.NewError(
			shoal.ErrorUnavailable, "corpus is open read-only")
	}
	return nil
}

func validatePersistedInteraction(record persistedInteraction) error {
	if err := shoal.ValidateRequiredID(
		"interaction session ID", record.SessionID,
	); err != nil {
		return err
	}
	if record.RecordedAt.IsZero() {
		return shoal.NewError(
			shoal.ErrorInternal, "stored interaction time is missing")
	}
	if _, err := interaction.ParseVisibility(record.Visibility); err != nil {
		return err
	}
	if record.Deleted && record.DeletedAt.IsZero() {
		return shoal.NewError(
			shoal.ErrorInternal, "stored interaction deletion time is missing")
	}
	if len(record.Nodes) == 0 {
		return shoal.NewError(
			shoal.ErrorInternal, "stored interaction has no nodes")
	}
	for _, node := range record.Nodes {
		if err := node.Validate(); err != nil {
			return err
		}
		if !interaction.IsInteractionKind(node.Kind) {
			return shoal.NewError(
				shoal.ErrorInternal,
				"stored interaction record contains a non-interaction node",
			)
		}
	}
	for _, edge := range record.Edges {
		if err := validatePersistedEdge(edge); err != nil {
			return err
		}
		if !interaction.IsInteractionEdgeType(edge.Type) {
			return shoal.NewError(
				shoal.ErrorInternal,
				"stored interaction record contains a non-interaction edge",
			)
		}
	}
	return nil
}
