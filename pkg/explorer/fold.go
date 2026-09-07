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
	"slices"
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// FoldRequest asks for a content-addressed fold over recorded interaction
// sessions.
//
// The request names sessions only. The provenance that is folded is read from
// what those sessions actually recorded, never from the caller, so a caller
// cannot understate what a session was shown and thereby widen the fold's
// visibility.
type FoldRequest struct {
	SessionIDs []shoal.ID
	// SummaryDigest is the SHA-256 digest, lowercase hex, of the summary text
	// this fold stands for. The summary text is held out-of-band and is never
	// persisted: it is derived from evidence, and an interaction record may
	// carry identities, digests, counts, and node IDs only. May be empty for a
	// purely structural fold.
	SummaryDigest string
}

// FoldResult describes a materialized fold.
type FoldResult struct {
	FoldID shoal.ID
	// Created is false when the same input had already been folded. Fold
	// identity is content-addressed, so refolding the same sessions with the
	// same summary digest is idempotent rather than a conflict.
	Created        bool
	Visibility     string
	FoldedAt       time.Time
	MemberCount    int
	RetrievedCount int
	CitedCount     int
}

// FoldSummary describes one stored fold without materializing its subgraph.
type FoldSummary struct {
	FoldID        shoal.ID
	FoldedAt      time.Time
	Visibility    string
	SummaryDigest string
	MemberCount   int
	NodeCount     int
	EdgeCount     int
	Deleted       bool
	DeletedAt     time.Time
}

type FoldSummaryPage struct {
	Folds     []FoldSummary
	NextAfter shoal.ID
}

const MaxFoldSummaryPageSize uint32 = 1000

type persistedFold struct {
	FoldID             shoal.ID
	Members            []interaction.FoldMember
	SummaryDigest      string
	Nodes              []graph.Node
	Edges              []graph.Edge
	Visibility         string
	SourceEdgeIDs      []shoal.ID
	RequiredVisibility []string
	FoldedAt           time.Time
	Deleted            bool
	DeletedAt          time.Time
}

// FoldInteractions folds one or more recorded sessions into a single derived
// summary vertex, following the public fold semantics of phrocker/sag.
//
// The fold is an interaction.fold node in the reserved namespace, so it is
// excluded from retrieval and from graph expansion exactly as a session is: a
// model can never retrieve a fold, and can never cite one as though it were a
// source document.
//
// The fold's visibility is the conjunction of every folded session's own
// visibility and every label of every source node those sessions touched,
// retrieved as well as cited. Folding therefore only ever narrows visibility.
// Publishing a redacted public summary is a separate, explicit, reviewed
// action and is deliberately not implemented here.
//
// Folding is an operator action, not part of serving an inference, so it never
// sits on the request latency path.
func (e *Explorer) FoldInteractions(
	ctx context.Context, request FoldRequest,
) (FoldResult, error) {
	if err := contextError(ctx); err != nil {
		return FoldResult{}, err
	}
	sessionIDs, err := normalizeFoldSessionIDs(request.SessionIDs)
	if err != nil {
		return FoldResult{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.requireOpen(); err != nil {
		return FoldResult{}, err
	}
	if err := e.requireWritableLocked(); err != nil {
		return FoldResult{}, err
	}
	if err := e.ensureGraphLocked(); err != nil {
		return FoldResult{}, err
	}

	members := make([]interaction.FoldMember, 0, len(sessionIDs))
	var sourceEdgeIDs []shoal.ID
	var requiredVisibility []string
	for _, sessionID := range sessionIDs {
		record, ok := e.interactions[sessionID]
		if !ok {
			return FoldResult{}, shoal.NewError(
				shoal.ErrorNotFound, "interaction session not found")
		}
		if record.Deleted {
			return FoldResult{}, shoal.NewError(
				shoal.ErrorConflict,
				"interaction session was explicitly deleted and cannot be folded",
			)
		}
		visibility, err := interaction.ParseVisibility(record.Visibility)
		if err != nil {
			return FoldResult{}, err
		}
		touched := interaction.TouchedNodes(record.Nodes, record.Edges)
		members = append(members, interaction.FoldMember{
			SessionID:        sessionID,
			RetrievedNodeIDs: touched.RetrievedNodeIDs,
			CitedNodeIDs:     touched.CitedNodeIDs,
			Visibility:       visibility,
		})
		sourceEdgeIDs = append(
			sourceEdgeIDs, record.Session.TouchedEdgeIDs()...)
		requiredVisibility = append(
			requiredVisibility, record.Session.RequiredVisibility...)
	}
	sourceEdgeIDs = dedupeExplorerIDs(sourceEdgeIDs)
	requiredVisibility, err = interaction.Conjoin(requiredVisibility)
	if err != nil {
		return FoldResult{}, err
	}

	fold := interaction.Fold{
		Members:       members,
		SummaryDigest: request.SummaryDigest,
		FoldedAt:      time.Now().UTC(),
	}
	subgraph, err := fold.Subgraph(e.visibilityResolverLocked())
	if err != nil {
		return FoldResult{}, err
	}
	canonical, err := fold.Canonical()
	if err != nil {
		return FoldResult{}, err
	}
	if _, exists := e.folds[subgraph.ID]; !exists {
		if err := e.reconcilePersistedFoldLocked(subgraph.ID); err != nil {
			return FoldResult{}, err
		}
	}
	if existing, ok := e.folds[subgraph.ID]; ok {
		return foldIdempotentResult(*existing, subgraph)
	}
	// A fold and a session must never share an identity: both are written into
	// the same corpus node namespace, so a collision would drop one of them at
	// the next graph rebuild.
	if _, ok := e.interactions[subgraph.ID]; ok {
		return FoldResult{}, shoal.NewError(
			shoal.ErrorConflict,
			"fold identity is already used by an interaction session",
		)
	}
	reservedNodes := append([]graph.Node(nil), subgraph.Nodes...)
	reservedNodes = append(reservedNodes, graph.Node{
		ID: interaction.TombstoneID(subgraph.ID),
	})
	if err := e.requireInteractionGraphIDsAvailableLocked(
		reservedNodes, subgraph.Edges,
	); err != nil {
		return FoldResult{}, err
	}
	record := persistedFold{
		FoldID:             subgraph.ID,
		Members:            canonical.Members,
		SummaryDigest:      canonical.SummaryDigest,
		Nodes:              subgraph.Nodes,
		Edges:              subgraph.Edges,
		Visibility:         interaction.Expression(subgraph.Visibility),
		SourceEdgeIDs:      sourceEdgeIDs,
		RequiredVisibility: requiredVisibility,
		FoldedAt:           fold.FoldedAt,
	}
	if err := validatePersistedFold(record); err != nil {
		return FoldResult{}, err
	}
	accepted, err := e.createInteractionRecord(
		foldRecordRow(record.FoldID), embeddedRecordFold, record,
	)
	if err != nil {
		return FoldResult{}, err
	}
	if !accepted {
		if err := e.reconcilePersistedFoldLocked(record.FoldID); err != nil {
			return FoldResult{}, err
		}
		existing, ok := e.folds[record.FoldID]
		if !ok {
			return FoldResult{}, shoal.NewError(
				shoal.ErrorUnavailable,
				"fold create was rejected without a durable winner",
			)
		}
		return foldIdempotentResult(*existing, subgraph)
	}
	e.reserveInteractionRecordGraphIDsLocked(
		record.FoldID, record.Nodes, record.Edges)
	e.foldOrder = insertOrderedID(e.foldOrder, record.FoldID)
	e.folds[record.FoldID] = &record
	if err := e.rebuildCurrentGraphLocked(); err != nil {
		return FoldResult{}, MarkCommittedInteraction(err)
	}
	return FoldResult{
		FoldID:         record.FoldID,
		Created:        true,
		Visibility:     record.Visibility,
		FoldedAt:       record.FoldedAt,
		MemberCount:    len(record.Members),
		RetrievedCount: len(subgraph.RetrievedNodeIDs),
		CitedCount:     len(subgraph.CitedNodeIDs),
	}, nil
}

// RehydrateFold unfolds a stored fold back into the provenance it replaced.
//
// Rehydration is lossless with respect to provenance: every folded session is
// returned with its retrieved set and its cited set kept apart, exactly as the
// session recorded them. Collapsing the two would understate what the model
// was shown and make the visibility conjunction unsound.
//
// It fails closed. A fold whose stored content no longer hashes to its own
// identity, or a deleted fold, is refused rather than partially returned.
func (e *Explorer) RehydrateFold(
	ctx context.Context, foldID shoal.ID,
) (interaction.Fold, error) {
	if err := contextError(ctx); err != nil {
		return interaction.Fold{}, err
	}
	if err := shoal.ValidateRequiredID("fold ID", foldID); err != nil {
		return interaction.Fold{}, err
	}
	if err := e.acquireReadWithGraph(); err != nil {
		return interaction.Fold{}, err
	}
	defer e.mu.RUnlock()
	record, ok := e.folds[foldID]
	if !ok {
		return interaction.Fold{}, shoal.NewError(
			shoal.ErrorNotFound, "fold not found")
	}
	if record.Deleted {
		return interaction.Fold{}, shoal.NewError(
			shoal.ErrorConflict, "fold was explicitly deleted")
	}
	current, err := e.currentFoldVisibilityLocked(record)
	if err != nil {
		return interaction.Fold{}, err
	}
	if !visibilityCovered(record.Visibility, current) {
		return interaction.Fold{}, staleDerivedVisibilityError()
	}
	fold := interaction.Fold{
		Members:       cloneFoldMembers(record.Members),
		SummaryDigest: record.SummaryDigest,
		FoldedAt:      record.FoldedAt,
	}
	derived, err := fold.ID()
	if err != nil {
		return interaction.Fold{}, err
	}
	if derived != foldID {
		return interaction.Fold{}, shoal.NewError(
			shoal.ErrorInternal,
			"stored fold does not hash to its own identity",
		)
	}
	return fold, nil
}

// Folds lists stored folds. Like Interactions, this is an explicit
// kind-scoped query: a fold is never returned by Retrieve and never
// discovered by expanding a source node's neighborhood.
func (e *Explorer) Folds(ctx context.Context) ([]FoldSummary, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := e.acquireReadWithGraph(); err != nil {
		return nil, err
	}
	defer e.mu.RUnlock()
	summaries := make([]FoldSummary, 0, len(e.folds))
	for _, record := range e.folds {
		if !record.Deleted {
			current, err := e.currentFoldVisibilityLocked(record)
			if err != nil || !visibilityCovered(record.Visibility, current) {
				// Fail closed at read time: a live fold whose evidence was
				// reclassified to a stricter label after it was folded is
				// withheld rather than served under its stale, now
				// under-labelled visibility. A merely loosened source still
				// covers the stored label and is kept. See issue #273.
				continue
			}
		}
		summaries = append(summaries, FoldSummary{
			FoldID:        record.FoldID,
			FoldedAt:      record.FoldedAt,
			Visibility:    record.Visibility,
			SummaryDigest: record.SummaryDigest,
			MemberCount:   len(record.Members),
			NodeCount:     len(record.Nodes),
			EdgeCount:     len(record.Edges),
			Deleted:       record.Deleted,
			DeletedAt:     record.DeletedAt,
		})
	}
	sort.Slice(summaries, func(i, j int) bool {
		return shoal.CompareID(summaries[i].FoldID, summaries[j].FoldID) < 0
	})
	return summaries, nil
}

// FoldsPage returns a bounded ID-ordered slice of fold summaries.
func (e *Explorer) FoldsPage(
	ctx context.Context, after shoal.ID, limit uint32,
) (FoldSummaryPage, error) {
	if err := contextError(ctx); err != nil {
		return FoldSummaryPage{}, err
	}
	if err := shoal.ValidateOptionalID("fold page cursor", after); err != nil {
		return FoldSummaryPage{}, err
	}
	if limit == 0 || limit > MaxFoldSummaryPageSize {
		return FoldSummaryPage{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "fold page limit is outside its bound")
	}
	if err := e.acquireReadWithGraph(); err != nil {
		return FoldSummaryPage{}, err
	}
	defer e.mu.RUnlock()
	start := sort.Search(len(e.foldOrder), func(index int) bool {
		return shoal.CompareID(e.foldOrder[index], after) > 0
	})
	ids := e.foldOrder[start:]
	page := FoldSummaryPage{Folds: make([]FoldSummary, 0, limit)}
	maxScanned := int(limit) * 8
	for index, id := range ids {
		if index >= maxScanned || len(page.Folds) >= int(limit) {
			page.NextAfter = ids[index-1]
			break
		}
		record := e.folds[id]
		if !record.Deleted {
			current, err := e.currentFoldVisibilityLocked(record)
			if err != nil || !visibilityCovered(record.Visibility, current) {
				continue
			}
		}
		page.Folds = append(page.Folds, FoldSummary{
			FoldID:        record.FoldID,
			FoldedAt:      record.FoldedAt,
			Visibility:    record.Visibility,
			SummaryDigest: record.SummaryDigest,
			MemberCount:   len(record.Members),
			NodeCount:     len(record.Nodes),
			EdgeCount:     len(record.Edges),
			Deleted:       record.Deleted,
			DeletedAt:     record.DeletedAt,
		})
		if index+1 < len(ids) && len(page.Folds) == int(limit) {
			page.NextAfter = id
		}
	}
	return page, nil
}

// FoldSubgraph returns one stored fold's node and edges. This is the explicit
// traversal entry point for a fold; it is never reached implicitly.
func (e *Explorer) FoldSubgraph(
	ctx context.Context, foldID shoal.ID,
) (Neighborhood, error) {
	if err := contextError(ctx); err != nil {
		return Neighborhood{}, err
	}
	if err := shoal.ValidateRequiredID("fold ID", foldID); err != nil {
		return Neighborhood{}, err
	}
	if err := e.acquireReadWithGraph(); err != nil {
		return Neighborhood{}, err
	}
	defer e.mu.RUnlock()
	record, ok := e.folds[foldID]
	if !ok {
		return Neighborhood{}, shoal.NewError(shoal.ErrorNotFound, "fold not found")
	}
	if !record.Deleted {
		current, err := e.currentFoldVisibilityLocked(record)
		if err != nil {
			return Neighborhood{}, err
		}
		if !visibilityCovered(record.Visibility, current) {
			return Neighborhood{}, staleDerivedVisibilityError()
		}
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

// DeleteFold removes a fold's node and edges and leaves an
// interaction.tombstone in their place, matching the retention rule for
// sessions: explicit deletion only, no TTL, and the deletion is itself
// auditable.
func (e *Explorer) DeleteFold(
	ctx context.Context, foldID shoal.ID,
) (interaction.Tombstone, error) {
	if err := contextError(ctx); err != nil {
		return interaction.Tombstone{}, err
	}
	if err := shoal.ValidateRequiredID("fold ID", foldID); err != nil {
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
	if err := e.reconcilePersistedFoldLocked(foldID); err != nil {
		return interaction.Tombstone{}, err
	}
	existing, ok := e.folds[foldID]
	if !ok {
		return interaction.Tombstone{}, shoal.NewError(
			shoal.ErrorNotFound, "fold not found")
	}
	if existing.Deleted {
		return interaction.Tombstone{}, shoal.NewError(
			shoal.ErrorConflict, "fold is already deleted")
	}
	visibility, err := interaction.ParseVisibility(existing.Visibility)
	if err != nil {
		return interaction.Tombstone{}, err
	}
	tombstone := interaction.Tombstone{
		SessionID:  foldID,
		DeletedAt:  time.Now().UTC(),
		NodeCount:  len(existing.Nodes),
		EdgeCount:  len(existing.Edges),
		Visibility: visibility,
	}
	node, err := tombstone.Node()
	if err != nil {
		return interaction.Tombstone{}, err
	}
	if existingNode, exists := e.graphNodes[node.ID]; exists {
		return interaction.Tombstone{}, shoal.NewError(
			shoal.ErrorConflict,
			"fold tombstone ID collides with existing graph node "+
				string(existingNode.ID),
		)
	}
	// The members are dropped, not retained beside the tombstone: a deleted
	// fold must not keep a rehydratable copy of what it folded.
	record := persistedFold{
		FoldID:     foldID,
		Nodes:      []graph.Node{node},
		Visibility: existing.Visibility,
		FoldedAt:   existing.FoldedAt,
		Deleted:    true,
		DeletedAt:  tombstone.DeletedAt,
	}
	if err := validatePersistedFold(record); err != nil {
		return interaction.Tombstone{}, err
	}
	accepted, err := e.deleteInteractionRecord(
		foldRecordRow(foldID), embeddedRecordFold, record,
	)
	if err != nil {
		return interaction.Tombstone{}, err
	}
	if !accepted {
		if err := e.reconcilePersistedFoldLocked(foldID); err != nil {
			return interaction.Tombstone{}, err
		}
		return interaction.Tombstone{}, shoal.NewError(
			shoal.ErrorConflict,
			"fold deletion lost a concurrent durable race",
		)
	}
	e.reserveInteractionRecordGraphIDsLocked(
		record.FoldID, record.Nodes, record.Edges)
	e.foldOrder = insertOrderedID(e.foldOrder, foldID)
	e.folds[foldID] = &record
	if err := e.rebuildCurrentGraphLocked(); err != nil {
		return interaction.Tombstone{}, MarkCommittedInteraction(err)
	}
	return tombstone, nil
}

// foldsReferencingLocked reports the live folds that fold a given session.
func (e *Explorer) foldsReferencingLocked(sessionID shoal.ID) []shoal.ID {
	var referencing []shoal.ID
	for _, record := range e.folds {
		if record.Deleted {
			continue
		}
		for _, member := range record.Members {
			if member.SessionID == sessionID {
				referencing = append(referencing, record.FoldID)
				break
			}
		}
	}
	sort.Slice(referencing, func(i, j int) bool {
		return shoal.CompareID(referencing[i], referencing[j]) < 0
	})
	return referencing
}

func foldIdempotentResult(
	existing persistedFold, subgraph interaction.FoldSubgraph,
) (FoldResult, error) {
	if existing.Deleted {
		return FoldResult{}, shoal.NewError(
			shoal.ErrorConflict,
			"fold identity was explicitly deleted and cannot be reused",
		)
	}
	// Identity covers the folded provenance but not the visibility derived
	// from it, so re-folding the same members against a corpus whose labels
	// moved fails here rather than quietly returning a stale visibility.
	//
	// Read paths (Folds, RehydrateFold, FoldSubgraph) and the interaction read
	// paths (Interactions, InteractionSubgraph) now re-derive visibility from
	// current source labels on every read and fail closed when the stored value
	// no longer matches, so a later metadata-only re-ingest that tightens a
	// source label no longer leaves an already-written fold or session
	// under-labelled and readable. See issue #273. Tombstones are exempt: a
	// deleted record carries its original visibility by design.
	if existing.Visibility != interaction.Expression(subgraph.Visibility) {
		return FoldResult{}, shoal.NewError(
			shoal.ErrorConflict,
			"fold already exists with a different derived visibility",
		)
	}
	return FoldResult{
		FoldID:         existing.FoldID,
		Created:        false,
		Visibility:     existing.Visibility,
		FoldedAt:       existing.FoldedAt,
		MemberCount:    len(existing.Members),
		RetrievedCount: len(subgraph.RetrievedNodeIDs),
		CitedCount:     len(subgraph.CitedNodeIDs),
	}, nil
}

func normalizeFoldSessionIDs(ids []shoal.ID) ([]shoal.ID, error) {
	if len(ids) == 0 {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "fold requires at least one session ID")
	}
	if len(ids) > interaction.MaxFoldMembers {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "fold exceeds the public member bound")
	}
	seen := make(map[shoal.ID]struct{}, len(ids))
	normalized := make([]shoal.ID, 0, len(ids))
	for _, id := range ids {
		if err := shoal.ValidateRequiredID(
			"interaction session ID", id,
		); err != nil {
			return nil, err
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		normalized = append(normalized, id)
	}
	sort.Slice(normalized, func(i, j int) bool {
		return shoal.CompareID(normalized[i], normalized[j]) < 0
	})
	return normalized, nil
}

func cloneFoldMembers(members []interaction.FoldMember) []interaction.FoldMember {
	if len(members) == 0 {
		return nil
	}
	cloned := make([]interaction.FoldMember, 0, len(members))
	for _, member := range members {
		cloned = append(cloned, interaction.FoldMember{
			SessionID:        member.SessionID,
			RetrievedNodeIDs: append([]shoal.ID(nil), member.RetrievedNodeIDs...),
			CitedNodeIDs:     append([]shoal.ID(nil), member.CitedNodeIDs...),
			Visibility:       append([]string(nil), member.Visibility...),
		})
	}
	return cloned
}

func validatePersistedFold(record persistedFold) error {
	if err := shoal.ValidateRequiredID("fold ID", record.FoldID); err != nil {
		return err
	}
	if record.FoldedAt.IsZero() {
		return shoal.NewError(shoal.ErrorInternal, "stored fold time is missing")
	}
	if _, err := interaction.ParseVisibility(record.Visibility); err != nil {
		return err
	}
	if !equalIDs(record.SourceEdgeIDs, dedupeExplorerIDs(record.SourceEdgeIDs)) {
		return shoal.NewError(
			shoal.ErrorInternal,
			"stored fold source edge IDs are not canonical",
		)
	}
	requiredVisibility, err := interaction.Conjoin(record.RequiredVisibility)
	if err != nil {
		return err
	}
	if !slices.Equal(record.RequiredVisibility, requiredVisibility) {
		return shoal.NewError(
			shoal.ErrorInternal,
			"stored fold required visibility is not canonical",
		)
	}
	if record.Deleted && record.DeletedAt.IsZero() {
		return shoal.NewError(
			shoal.ErrorInternal, "stored fold deletion time is missing")
	}
	if !record.Deleted && len(record.Members) == 0 {
		return shoal.NewError(shoal.ErrorInternal, "stored fold has no members")
	}
	if len(record.Nodes) == 0 {
		return shoal.NewError(shoal.ErrorInternal, "stored fold has no nodes")
	}
	for _, node := range record.Nodes {
		if err := node.Validate(); err != nil {
			return err
		}
		if !interaction.IsInteractionKind(node.Kind) {
			return shoal.NewError(
				shoal.ErrorInternal,
				"stored fold record contains a non-interaction node",
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
				"stored fold record contains a non-interaction edge",
			)
		}
	}
	return nil
}
