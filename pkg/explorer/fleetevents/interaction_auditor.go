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
	"bytes"
	"context"
	"encoding/hex"
	"sort"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// InteractionAuditor records privileged fleet actions through the existing
// durable interaction recorder without persisting event payloads or raw
// opaque object identities.
type InteractionAuditor struct {
	recorder  *interaction.Recorder
	snapshots fleet.InteractionSnapshotProvider
}

func NewInteractionAuditor(
	recorder *interaction.Recorder,
	snapshots fleet.InteractionSnapshotProvider,
) (*InteractionAuditor, error) {
	if recorder == nil || snapshots == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "interaction recorder is required")
	}
	return &InteractionAuditor{recorder: recorder, snapshots: snapshots}, nil
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
	evidenceIDs := make([]shoal.ID, 0, len(record.Evidence)*5)
	for _, evidence := range record.Evidence {
		evidenceIDs = append(evidenceIDs, evidence.ObjectID)
		for _, id := range []shoal.ID{
			evidence.NodeID, evidence.EdgeID,
			evidence.AnchorID, evidence.RevisionID,
		} {
			if id != "" {
				evidenceIDs = append(evidenceIDs, id)
			}
		}
	}
	session := interaction.Session{
		ID: sessionID, RecordedAt: record.OccurredAt, Operation: interaction.OperationToolCall,
		AuthorizationFingerprint: shoal.ID(
			record.AuthorizationFingerprint.String()),
		AuthorizationExpiresAt: record.AuthorizationExpiresAt,
		AuthorizationOperation: string(record.Operation),
		SnapshotID:             shoal.ID(snapshot.ID), SnapshotAsOf: snapshot.AsOf,
		RequestID: record.RequestID,
		QueryDigest: interaction.Digest(
			string(record.ActionID) + "\x00" + string(record.CorrelationID)),
		SeedNodeIDs: evidenceIDs,
		Turns: []interaction.Turn{{
			Index: 0, Decision: string(record.Operation),
			ToolCall: &interaction.ToolCall{Kind: "fleet." + string(record.Operation)},
		}},
	}
	persisted, err := a.recorder.Record(ctx, session)
	if err != nil {
		return err
	}
	if !sameFleetReceipt(session, persisted) {
		return shoal.NewError(
			shoal.ErrorInternal, "persisted fleet interaction receipt does not match request")
	}
	return nil
}

func sameFleetReceipt(expected, persisted interaction.Session) bool {
	if persisted.ID != expected.ID ||
		persisted.Operation != interaction.OperationToolCall ||
		persisted.AuthorizationOperation != expected.AuthorizationOperation ||
		persisted.AuthorizationFingerprint != expected.AuthorizationFingerprint ||
		!persisted.AuthorizationExpiresAt.Equal(expected.AuthorizationExpiresAt) ||
		persisted.RequestID != expected.RequestID ||
		len(persisted.Turns) != 1 ||
		persisted.Turns[0].ToolCall == nil ||
		persisted.Turns[0].ToolCall.Kind != expected.Turns[0].ToolCall.Kind {
		return false
	}
	expectedEvidence := expected.TouchedNodeIDs()
	persistedEvidence := persisted.TouchedNodeIDs()
	sort.Slice(expectedEvidence, func(i, j int) bool {
		return bytes.Compare([]byte(expectedEvidence[i]), []byte(expectedEvidence[j])) < 0
	})
	sort.Slice(persistedEvidence, func(i, j int) bool {
		return bytes.Compare([]byte(persistedEvidence[i]), []byte(persistedEvidence[j])) < 0
	})
	if len(expectedEvidence) != len(persistedEvidence) {
		return false
	}
	for i := range expectedEvidence {
		if expectedEvidence[i] != persistedEvidence[i] {
			return false
		}
	}
	return true
}
