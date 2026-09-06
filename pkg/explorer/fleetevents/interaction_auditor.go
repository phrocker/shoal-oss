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
	"reflect"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// InteractionAuditor records privileged fleet actions through the existing
// durable interaction recorder without persisting event payloads or raw
// opaque object identities.
type InteractionAuditor struct {
	recorder *interaction.Recorder
}

func NewInteractionAuditor(recorder *interaction.Recorder) (*InteractionAuditor, error) {
	if recorder == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "interaction recorder is required")
	}
	return &InteractionAuditor{recorder: recorder}, nil
}

func (a *InteractionAuditor) RecordFleetAction(ctx context.Context, record AuditRecord) error {
	sessionID := interaction.DerivedID("session", hex.EncodeToString(deriveID(
		"fleet-action-session-v1", []byte(record.Operation), record.ActionID,
		record.ObjectID, record.CorrelationID,
	)))
	snapshotID := shoal.ID(hex.EncodeToString(deriveID(
		"fleet-action-snapshot-v1", record.ObjectID, []byte(record.OccurredAt.Format(time.RFC3339Nano)),
	)))
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
		SnapshotID:             snapshotID, SnapshotAsOf: record.OccurredAt,
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
		return explorer.MarkCommittedInteraction(shoal.NewError(
			shoal.ErrorInternal,
			"persisted fleet interaction receipt does not match request"))
	}
	return nil
}

func sameFleetReceipt(expected, persisted interaction.Session) bool {
	expected.RecordedAt = persisted.RecordedAt
	expected.Actor = persisted.Actor
	expected.Reason = persisted.Reason
	canonicalExpected, err := expected.Canonical()
	if err != nil {
		return false
	}
	canonicalPersisted, err := persisted.Canonical()
	if err != nil {
		return false
	}
	return reflect.DeepEqual(canonicalExpected, canonicalPersisted)
}
