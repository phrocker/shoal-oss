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

package analytics

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const analyticsToolKind = "analytics"
const graphAssertionEdgeIDMetadata = "shoal.graph.edge_id"

// InteractionRecorder adapts authorized analytics records to the shared
// durable interaction recorder.
type InteractionRecorder struct {
	recorder *interaction.Recorder
	now      func() time.Time
}

// NewInteractionRecorder creates the shared-recorder adapter used by shipped
// analytics transports.
func NewInteractionRecorder(
	recorder *interaction.Recorder,
	now func() time.Time,
) (*InteractionRecorder, error) {
	if recorder == nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "interaction recorder is required")
	}
	if now == nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "analytics recorder clock is required")
	}
	if err := recorder.SetClock(now); err != nil {
		return nil, err
	}
	return &InteractionRecorder{recorder: recorder, now: now}, nil
}

// RecordAnalytics records every materialized node and exact graph edge as the
// evidence used by the analytics tool call.
func (r *InteractionRecorder) RecordAnalytics(
	ctx context.Context,
	record Record,
) (RecordingReceipt, error) {
	if r == nil || r.recorder == nil || r.now == nil {
		return RecordingReceipt{}, shoal.NewError(
			shoal.ErrorUnavailable, "analytics interaction recorder is unavailable")
	}
	if err := contextError(ctx); err != nil {
		return RecordingReceipt{}, err
	}
	if !record.Materialization.Complete || !record.Result.Scope.Complete {
		return RecordingReceipt{}, shoal.NewError(
			shoal.ErrorUnavailable, "analytics materialization is incomplete")
	}
	if record.Result.Scope.SnapshotID == "" ||
		record.Result.Scope.AuthorizationFingerprint !=
			record.Materialization.AuthorizationFingerprint.String() ||
		record.Result.Scope.PolicyGeneration !=
			record.Materialization.PolicyGeneration ||
		record.Materialization.RequestID == "" ||
		record.Materialization.AuthorizationExpiresAt.IsZero() ||
		record.Materialization.Snapshot.AsOf.IsZero() {
		return RecordingReceipt{}, shoal.NewError(
			shoal.ErrorInternal, "analytics recording pins are inconsistent")
	}
	if !ontologyMatchesMaterialization(
		record.Result.Scope.Ontology, record.Materialization) {
		return RecordingReceipt{}, shoal.NewError(
			shoal.ErrorInternal, "analytics recording ontology is inconsistent")
	}
	limits := record.Limits
	if limits == (Limits{}) {
		limits = DefaultLimits()
	}
	if err := limits.Validate(); err != nil {
		return RecordingReceipt{}, err
	}
	if err := validateAnalyticsEvidenceBytes(
		record.Materialization.Neighborhood, limits.MaxEvidenceBytes); err != nil {
		return RecordingReceipt{}, err
	}
	neighborhood := cloneNeighborhood(record.Materialization.Neighborhood)
	nodeIDs := make([]shoal.ID, len(neighborhood.Nodes))
	for index, node := range neighborhood.Nodes {
		nodeIDs[index] = node.ID
	}

	sort.Slice(nodeIDs, func(i, j int) bool {
		return shoal.CompareID(nodeIDs[i], nodeIDs[j]) < 0
	})
	edges := append([]graph.Edge(nil), neighborhood.Edges...)
	sort.Slice(edges, func(i, j int) bool {
		return shoal.CompareID(edges[i].ID, edges[j].ID) < 0
	})
	assertions := analyticsAssertionEvidence(neighborhood.Assertions)
	if uint32(len(nodeIDs)) != record.Result.Scope.NodeCount ||
		uint32(len(edges)) != record.Result.Scope.EdgeCount ||
		!resultCoversNodes(record.Result, nodeIDs) {
		return RecordingReceipt{}, shoal.NewError(
			shoal.ErrorInternal, "analytics recording evidence is inconsistent")
	}
	recordedAt := r.now().UTC()
	if recordedAt.IsZero() ||
		!recordedAt.Before(record.Materialization.AuthorizationExpiresAt) {
		return RecordingReceipt{}, shoal.NewError(
			shoal.ErrorUnauthorized, "analytics authorization expired before recording")
	}
	sessionID, err := interaction.OperationSessionID(
		interaction.OperationToolCall,
		record.Materialization.RequestID,
		recordedAt,
	)
	if err != nil {
		return RecordingReceipt{}, err
	}
	session := interaction.Session{
		ID:           sessionID,
		RecordedAt:   recordedAt,
		Operation:    interaction.OperationToolCall,
		SnapshotID:   shoal.ID(record.Result.Scope.SnapshotID),
		SnapshotAsOf: record.Materialization.Snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(
			record.Materialization.AuthorizationFingerprint.String()),
		AuthorizationExpiresAt: record.Materialization.AuthorizationExpiresAt,
		RequestID:              record.Materialization.RequestID,
		QueryDigest:            analyticsRequestDigest(record.Request),
		ResultID:               analyticsResultID(record.Result),
		StopReason:             "completed",
		SeedNodeIDs:            append([]shoal.ID(nil), record.Request.Scope.NodeIDs...),
		Turns: []interaction.Turn{{
			Index: 0, Decision: "completed",
			ToolCall: &interaction.ToolCall{
				Kind:                analyticsToolKind,
				RetrievedNodeIDs:    nodeIDs,
				RetrievedNodes:      neighborhood.Nodes,
				RetrievedEdges:      edges,
				RetrievedAssertions: assertions,
			},
		}},
		Provenance: interaction.Provenance{
			Harness:  "shoal-explorer-analytics",
			Provider: "shoal", Model: "pagerank",
			ModelVersion: "1", ToolPolicy: "analytics_read",
		},
	}
	if selected := record.Result.Scope.Ontology; selected != nil {
		session.OntologySchemaID = selected.SchemaID
		session.OntologyVersionID = selected.VersionID
	}
	persisted, err := r.recorder.Record(ctx, session)
	if err != nil {
		return RecordingReceipt{}, err
	}
	if persisted.ID != sessionID ||
		persisted.AuthorizationFingerprint != session.AuthorizationFingerprint ||
		persisted.AuthorizationExpiresAt != session.AuthorizationExpiresAt ||
		persisted.RequestID != session.RequestID ||
		persisted.OntologySchemaID != session.OntologySchemaID ||
		persisted.OntologyVersionID != session.OntologyVersionID ||
		persisted.Actor.SubjectID == "" || persisted.Actor.ActorID == "" ||
		len(persisted.RequiredVisibility) == 0 ||
		!equalIDs(persisted.TouchedNodeIDs(), nodeIDs) ||
		!equalIDs(persisted.TouchedEdgeIDs(), edgeIDs(edges)) ||
		!equalGraphNodes(recordedSourceNodes(persisted), neighborhood.Nodes) ||
		!equalGraphEdges(recordedSourceEdges(persisted), edges) ||
		!equalAssertionEvidence(
			recordedAssertionEvidence(persisted), assertions) {
		return RecordingReceipt{}, shoal.NewError(
			shoal.ErrorInternal, "analytics recorder returned inconsistent evidence")
	}
	return RecordingReceipt{InteractionID: persisted.ID}, nil
}

func validateAnalyticsEvidenceBytes(
	neighborhood explorer.Neighborhood,
	limit uint64,
) error {
	total := uint64(2)
	add := func(value any) error {
		encoded, err := json.Marshal(value)
		if err != nil {
			return shoal.WrapError(
				shoal.ErrorInternal, "encode analytics interaction evidence", err)
		}
		size := uint64(len(encoded)) + 1
		if size > limit || total > limit-size {
			return shoal.NewError(
				shoal.ErrorUnavailable,
				"analytics interaction evidence exceeds the configured byte limit",
			)
		}
		total += size
		return nil
	}
	for _, node := range neighborhood.Nodes {
		if err := add(node); err != nil {
			return err
		}
	}
	for _, edge := range neighborhood.Edges {
		if err := add(edge); err != nil {
			return err
		}
	}
	for _, assertion := range neighborhood.Assertions {
		evidence := analyticsAssertionEvidence([]ontology.Assertion{assertion})
		if len(evidence) != 1 {
			return shoal.NewError(
				shoal.ErrorInternal, "analytics assertion evidence is inconsistent")
		}
		if err := add(evidence[0]); err != nil {
			return err
		}
	}
	return nil
}

func analyticsAssertionEvidence(
	assertions []ontology.Assertion,
) []interaction.AssertionEvidence {
	if len(assertions) == 0 {
		return nil
	}
	result := make([]interaction.AssertionEvidence, 0, len(assertions))
	for _, assertion := range assertions {
		target, _ := assertion.Object().ReferenceValue()
		evidence := interaction.AssertionEvidence{
			ID: assertion.ID(), Subject: assertion.Subject(),
			Predicate: assertion.Predicate(), ObjectReference: target,
			Origin: string(assertion.Origin()), Confidence: assertion.Confidence(),
			GraphEdgeID: shoal.ID(
				assertion.Metadata()[graphAssertionEdgeIDMetadata]),
		}
		for _, item := range assertion.Evidence() {
			if derivation, ok := item.Derivation(); ok {
				evidence.DerivationID = derivation.ID()
				evidence.DerivationScore = derivation.Score()
				break
			}
		}
		result = append(result, evidence)
	}
	return result
}

func recordedAssertionEvidence(
	session interaction.Session,
) []interaction.AssertionEvidence {
	var assertions []interaction.AssertionEvidence
	for _, turn := range session.Turns {
		if turn.ToolCall != nil {
			assertions = append(
				assertions, turn.ToolCall.RetrievedAssertions...)
		}
	}
	sort.Slice(assertions, func(i, j int) bool {
		return shoal.CompareID(assertions[i].ID, assertions[j].ID) < 0
	})
	return assertions
}

func equalAssertionEvidence(
	left, right []interaction.AssertionEvidence,
) bool {
	if len(left) != len(right) {
		return false
	}
	left = append([]interaction.AssertionEvidence(nil), left...)
	right = append([]interaction.AssertionEvidence(nil), right...)
	sort.Slice(left, func(i, j int) bool {
		return shoal.CompareID(left[i].ID, left[j].ID) < 0
	})
	sort.Slice(right, func(i, j int) bool {
		return shoal.CompareID(right[i].ID, right[j].ID) < 0
	})
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func ontologyMatchesMaterialization(
	scope *OntologyScope,
	materialization Materialization,
) bool {
	selected := materialization.SelectedOntology
	if !selected.Known() {
		return scope == nil
	}
	return scope != nil &&
		scope.SchemaID == selected.SchemaID() &&
		scope.VersionID == selected.VersionID()
}

func resultCoversNodes(result Result, expected []shoal.ID) bool {
	actual := make([]shoal.ID, len(result.Nodes))
	for index, node := range result.Nodes {
		actual[index] = node.NodeID
	}
	sort.Slice(actual, func(i, j int) bool {
		return shoal.CompareID(actual[i], actual[j]) < 0
	})
	return equalIDs(actual, expected)
}

func edgeIDs(edges []graph.Edge) []shoal.ID {
	ids := make([]shoal.ID, len(edges))
	for index, edge := range edges {
		ids[index] = edge.ID
	}
	return ids
}

func recordedSourceNodes(session interaction.Session) []graph.Node {
	var nodes []graph.Node
	for _, turn := range session.Turns {
		if turn.ToolCall != nil {
			nodes = append(nodes, turn.ToolCall.RetrievedNodes...)
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		return shoal.CompareID(nodes[i].ID, nodes[j].ID) < 0
	})
	return nodes
}

func recordedSourceEdges(session interaction.Session) []graph.Edge {
	edges := append([]graph.Edge(nil), session.CitedEdges...)
	for _, turn := range session.Turns {
		if turn.ToolCall != nil {
			edges = append(edges, turn.ToolCall.RetrievedEdges...)
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		return shoal.CompareID(edges[i].ID, edges[j].ID) < 0
	})
	return edges
}

func equalGraphNodes(left, right []graph.Node) bool {
	if len(left) != len(right) {
		return false
	}
	left = append([]graph.Node(nil), left...)
	right = append([]graph.Node(nil), right...)
	sort.Slice(left, func(i, j int) bool {
		return shoal.CompareID(left[i].ID, left[j].ID) < 0
	})
	sort.Slice(right, func(i, j int) bool {
		return shoal.CompareID(right[i].ID, right[j].ID) < 0
	})
	for index := range left {
		if left[index].ID != right[index].ID ||
			left[index].Kind != right[index].Kind ||
			len(left[index].Labels) != len(right[index].Labels) ||
			len(left[index].Properties) != len(right[index].Properties) {
			return false
		}
		for labelIndex := range left[index].Labels {
			if left[index].Labels[labelIndex] != right[index].Labels[labelIndex] {
				return false
			}
		}
		for key, value := range left[index].Properties {
			other, ok := right[index].Properties[key]
			if !ok || other != value {
				return false
			}
		}
	}
	return true
}

func equalGraphEdges(left, right []graph.Edge) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].ID != right[index].ID ||
			left[index].From != right[index].From ||
			left[index].To != right[index].To ||
			left[index].Type != right[index].Type ||
			math.Float64bits(float64(left[index].Weight)) !=
				math.Float64bits(float64(right[index].Weight)) ||
			len(left[index].Properties) != len(right[index].Properties) {
			return false
		}
		for key, value := range left[index].Properties {
			other, ok := right[index].Properties[key]
			if !ok || other != value {
				return false
			}
		}
	}
	return true
}

func analyticsRequestDigest(request Request) string {
	var encoded bytes.Buffer
	writeText(&encoded, "authorized-analytics-request-v1")
	writeText(&encoded, request.SnapshotID)
	writeUint64(&encoded, uint64(len(request.Scope.NodeIDs)))
	for _, nodeID := range request.Scope.NodeIDs {
		writeBytes(&encoded, []byte(nodeID))
	}
	writeUint64(&encoded, uint64(request.Scope.Depth))
	writeText(&encoded, string(request.Scope.Direction))
	writeUint64(&encoded, uint64(request.Scope.Fanout))
	writeUint64(&encoded, uint64(request.Scope.MaxNodes))
	writeUint64(&encoded, uint64(request.Scope.MaxEdges))
	writeUint64(&encoded, uint64(request.Scope.MaxScannedEdgesPerNode))
	writeUint64(&encoded, uint64(len(request.Scope.EdgeTypes)))
	for _, edgeType := range request.Scope.EdgeTypes {
		writeText(&encoded, edgeType)
	}
	writeUint64(&encoded, math.Float64bits(request.PageRank.DampingFactor))
	writeUint64(&encoded, math.Float64bits(request.PageRank.ConvergenceTolerance))
	writeUint64(&encoded, uint64(request.PageRank.MaxIterations))
	return interaction.Digest(encoded.String())
}

func analyticsResultID(result Result) shoal.ID {
	var encoded bytes.Buffer
	writeText(&encoded, "authorized-analytics-result-v1")
	writeText(&encoded, result.Scope.SnapshotID)
	for _, node := range result.Nodes {
		writeBytes(&encoded, []byte(node.NodeID))
		writeUint64(&encoded, uint64(node.InDegree))
		writeUint64(&encoded, uint64(node.OutDegree))
		writeUint64(&encoded, math.Float64bits(node.PageRank))
		writeText(&encoded, node.WeakComponentID)
	}
	writeUint64(&encoded, uint64(result.PageRank.Iterations))
	return interaction.DerivedID(
		"analytics_result", interaction.Digest(encoded.String()))
}
