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

package fleetevents

import (
	"context"
	"encoding/hex"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// InteractionAuditor records privileged fleet actions through the existing
// durable interaction recorder without persisting event payloads or raw
// opaque object identities.
type InteractionAuditor struct {
	sink      interaction.ResultSink
	snapshots fleet.InteractionSnapshotProvider
}

func NewInteractionAuditor(
	sink interaction.ResultSink,
	snapshots fleet.InteractionSnapshotProvider,
) (*InteractionAuditor, error) {
	if interaction.IsNilResultSink(sink) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "interaction result sink is required")
	}
	if snapshots == nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "interaction snapshot provider is required")
	}
	return &InteractionAuditor{sink: sink, snapshots: snapshots}, nil
}

func (a *InteractionAuditor) RecordFleetAction(ctx context.Context, record AuditRecord) error {
	sessionID := shoal.ID(hex.EncodeToString(deriveID(
		"fleet-action-session-v1", []byte(record.Operation), record.ActionID,
		record.ObjectID, record.CorrelationID,
	)))
	snapshot, err := a.snapshots.InteractionSnapshot(ctx)
	if err != nil {
		return err
	}
	seedEvidence := fleetInteractionEvidence(record)
	seedNodeIDs := make([]shoal.ID, 0, len(seedEvidence))
	for _, evidence := range seedEvidence {
		seedNodeIDs = append(seedNodeIDs, evidence.NodeIDs...)
	}
	stopReason := "no_evidence"
	switch {
	case len(record.Evidence) > len(seedEvidence):
		stopReason = "redacted_non_node_evidence"
	case len(seedEvidence) > 0:
		stopReason = "source_node_evidence"
	}
	recordedAt := record.OccurredAt.UTC()
	if recordedAt.IsZero() || recordedAt.Before(snapshot.AsOf) {
		// Interaction receipts must not predate the snapshot whose source
		// membership they pin; older lifecycle event times are audit inputs, not
		// proof that this snapshot already existed.
		recordedAt = snapshot.AsOf.UTC()
	}
	session := interaction.Session{
		ID: sessionID, RecordedAt: recordedAt, Operation: interaction.OperationToolCall,
		AuthorizationFingerprint: shoal.ID(
			record.AuthorizationFingerprint.String()),
		AuthorizationExpiresAt: record.AuthorizationExpiresAt,
		AuthorizationOperation: string(record.Operation),
		SnapshotID:             shoal.ID(snapshot.ID), SnapshotAsOf: snapshot.AsOf,
		RequestID: record.RequestID,
		QueryDigest: interaction.Digest(
			string(record.ActionID) + "\x00" + string(record.CorrelationID)),
		SeedNodeIDs: seedNodeIDs, SeedEvidence: seedEvidence,
		StopReason: stopReason,
		Turns: []interaction.Turn{{
			Index: 0, Decision: string(record.Operation),
			ToolCall: &interaction.ToolCall{Kind: "fleet." + string(record.Operation)},
		}},
	}
	session, err = session.Canonical()
	if err != nil {
		return err
	}
	persisted, err := a.sink.RecordInteractionResult(ctx, session)
	if err != nil {
		return err
	}
	if !sameFleetReceipt(session, persisted) {
		return shoal.NewError(
			shoal.ErrorInternal, "persisted fleet interaction receipt does not match request")
	}
	return nil
}

func fleetInteractionEvidence(record AuditRecord) []interaction.EvidenceReference {
	evidence := make([]interaction.EvidenceReference, 0, len(record.Evidence))
	for _, item := range record.Evidence {
		// Only actual source graph nodes can be validated as touched interaction
		// nodes. Object, edge-only, anchor, and revision IDs stay out of
		// SeedNodeIDs so the interaction sink never tries to authorize them as
		// source nodes.
		if item.NodeID == "" {
			continue
		}
		anchorID := item.AnchorID
		if anchorID == "" {
			anchorID = shoal.ID(hex.EncodeToString(deriveID(
				"fleet-event-evidence-anchor-v1",
				record.ActionID, record.ObjectID, []byte(item.ObjectID),
				[]byte(item.NodeID), []byte(item.EdgeID),
				[]byte(item.RevisionID),
			)))
		}
		reference := interaction.EvidenceReference{
			AnchorID: anchorID,
			Kind:     interaction.EvidenceGraph,
			NodeIDs:  []shoal.ID{item.NodeID},
		}
		if item.EdgeID != "" {
			reference.EdgeIDs = []shoal.ID{item.EdgeID}
		}
		evidence = append(evidence, reference)
	}
	return evidence
}

func sameFleetReceipt(expected, persisted interaction.Session) bool {
	persisted, err := persisted.Canonical()
	if err != nil {
		return false
	}
	if persisted.ID != expected.ID ||
		persisted.Operation != interaction.OperationToolCall ||
		persisted.AuthorizationOperation != expected.AuthorizationOperation ||
		persisted.AuthorizationFingerprint != expected.AuthorizationFingerprint ||
		!persisted.AuthorizationExpiresAt.Equal(expected.AuthorizationExpiresAt) ||
		persisted.RequestID != expected.RequestID ||
		persisted.StopReason != expected.StopReason ||
		len(persisted.Turns) != 1 ||
		persisted.Turns[0].ToolCall == nil ||
		persisted.Turns[0].ToolCall.Kind != expected.Turns[0].ToolCall.Kind {
		return false
	}
	return sameFleetIDs(persisted.SeedNodeIDs, expected.SeedNodeIDs) &&
		sameFleetEvidence(persisted.SeedEvidence, expected.SeedEvidence)
}

func sameFleetIDs(left, right []shoal.ID) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func sameFleetEvidence(
	left, right []interaction.EvidenceReference,
) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].AnchorID != right[i].AnchorID ||
			left[i].Kind != right[i].Kind ||
			left[i].Citation != right[i].Citation ||
			!sameFleetIDs(left[i].NodeIDs, right[i].NodeIDs) ||
			!sameFleetIDs(left[i].EdgeIDs, right[i].EdgeIDs) ||
			len(left[i].Assertions) != len(right[i].Assertions) {
			return false
		}
		for j := range left[i].Assertions {
			if left[i].Assertions[j] != right[i].Assertions[j] {
				return false
			}
		}
	}
	return true
}
