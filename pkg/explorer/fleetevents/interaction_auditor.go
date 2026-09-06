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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"reflect"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// InteractionAuditor records privileged fleet actions through the existing
// durable interaction recorder without persisting event payloads or raw
// opaque object identities.
type InteractionAuditor struct {
	recorder  *interaction.Recorder
	snapshots InteractionSnapshotProvider
}

type InteractionSnapshotProvider interface {
	InteractionSnapshot(context.Context) (explorer.Snapshot, error)
}

func NewInteractionAuditor(
	recorder *interaction.Recorder, snapshots InteractionSnapshotProvider,
) (*InteractionAuditor, error) {
	if recorder == nil || snapshots == nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"interaction recorder and snapshot provider are required")
	}
	return &InteractionAuditor{recorder: recorder, snapshots: snapshots}, nil
}

func (a *InteractionAuditor) RecordFleetAction(ctx context.Context, record AuditRecord) error {
	sessionID := interaction.DerivedID("session", hex.EncodeToString(deriveID(
		"fleet-action-session-v1", []byte(record.Operation), record.ActionID,
		record.ObjectID, record.CorrelationID,
	)))
	snapshot, err := a.snapshots.InteractionSnapshot(ctx)
	if err != nil {
		return err
	}
	evidenceIDs := make([]shoal.ID, 0, len(record.Evidence))
	for _, evidence := range record.Evidence {
		if evidence.NodeID != "" {
			evidenceIDs = append(evidenceIDs, evidence.NodeID)
		}
	}
	session := interaction.Session{
		ID: sessionID, RecordedAt: record.OccurredAt, Operation: interaction.OperationToolCall,
		AuthorizationFingerprint: shoal.ID(
			record.AuthorizationFingerprint.String()),
		AuthorizationExpiresAt: record.AuthorizationExpiresAt,
		AuthorizationOperation: string(record.Operation),
		SnapshotID:             shoal.ID(snapshot.ID), SnapshotAsOf: snapshot.AsOf,
		RequestID:   record.RequestID,
		QueryDigest: interaction.Digest(hex.EncodeToString(auditDigest(record))),
		SeedNodeIDs: evidenceIDs,
		Turns: []interaction.Turn{{
			Index: 0, Decision: string(record.Operation),
			ToolCall: &interaction.ToolCall{Kind: "fleet." + string(record.Operation)},
		}},
	}
	persisted, err := a.recorder.Record(ctx, session)
	if err != nil && !interaction.IsCommittedRecord(err) {
		return err
	}
	if !sameFleetReceipt(session, persisted) {
		mismatch := explorer.MarkCommittedInteraction(shoal.NewError(
			shoal.ErrorInternal,
			"persisted fleet interaction receipt does not match request"))
		if err != nil {
			return errors.Join(err, mismatch)
		}
		return mismatch
	}
	if err != nil {
		return err
	}
	return nil
}

func sameFleetReceipt(expected, persisted interaction.Session) bool {
	expected.RecordedAt = persisted.RecordedAt
	expected.Actor = persisted.Actor
	expected.Reason = persisted.Reason
	expected.SnapshotID = persisted.SnapshotID
	expected.SnapshotAsOf = persisted.SnapshotAsOf
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

func auditDigest(record AuditRecord) []byte {
	parts := [][]byte{
		[]byte(record.Operation), record.ActionID, []byte(record.RequestID),
		record.CorrelationID, record.ObjectID, encodedUint64(uint64(len(record.Evidence))),
	}
	for _, evidence := range record.Evidence {
		parts = append(parts,
			evidence.SourceID, evidence.PolicyID, []byte(evidence.ObjectID),
			[]byte(evidence.NodeID), []byte(evidence.EdgeID),
			[]byte(evidence.AnchorID), []byte(evidence.RevisionID),
		)
		parts = append(parts, encodedUint64(uint64(evidence.Start)),
			encodedUint64(uint64(evidence.End)),
			encodedUint64(uint64(len(evidence.Visibility))))
		for _, visibility := range evidence.Visibility {
			parts = append(parts, []byte(visibility))
		}
	}
	return deriveID("fleet-action-audit-v2", parts...)
}

func encodedUint64(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}
