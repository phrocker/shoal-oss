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
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
)

type fixedAuditSnapshot struct{ at time.Time }

func (s fixedAuditSnapshot) InteractionSnapshot(
	context.Context,
) (explorer.Snapshot, error) {
	return explorer.Snapshot{
		ID: "snapshot", AsOf: s.at.UTC(), Frontier: 1,
	}, nil
}

func TestInteractionAuditorRecordsRedactedAction(t *testing.T) {
	sink := &auditSink{}
	recorder, err := interaction.NewRecorder(context.Background(), sink)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	if err := recorder.SetClock(func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	auditor, err := NewInteractionAuditor(
		recorder, fixedAuditSnapshot{at: now})
	if err != nil {
		t.Fatal(err)
	}
	err = auditor.RecordFleetAction(context.Background(), AuditRecord{
		Operation: auth.OperationEventPublish, ActionID: []byte{0, 0xff},
		RequestID: "request",
		ObjectID:  []byte("event"),
		Evidence: []Evidence{
			{
				SourceID: []byte("source-a"), PolicyID: []byte("policy-a"),
				ObjectID: "object-a", NodeID: "node-a", EdgeID: "edge-a",
				AnchorID: "anchor-a", RevisionID: "revision-a",
			},
			{SourceID: []byte("source-b"), PolicyID: []byte("policy-b"), ObjectID: "object-b"},
		},
		AuthorizationFingerprint: auth.Fingerprint{1},
		AuthorizationExpiresAt:   now.Add(time.Hour), OccurredAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.sessions) != 1 || sink.sessions[0].Operation != interaction.OperationToolCall ||
		sink.sessions[0].AuthorizationOperation != string(auth.OperationEventPublish) ||
		sink.sessions[0].Turns[0].ToolCall.Kind != "fleet.event_publish" ||
		sink.sessions[0].RequestID != "request" ||
		sink.sessions[0].AuthorizationFingerprint != "auth-sha256:0100000000000000000000000000000000000000000000000000000000000000" ||
		sink.sessions[0].Actor.SubjectID != "" ||
		sink.sessions[0].Actor.ActorID != "" ||
		sink.sessions[0].Actor.ClientID != "" ||
		len(sink.sessions[0].Actor.OnBehalfOf) != 0 ||
		sink.sessions[0].Reason != (interaction.Reason{}) ||
		len(sink.sessions[0].TouchedNodeIDs()) != 6 {
		t.Fatalf("session = %#v", sink.sessions)
	}
	if got := sink.sessions[0].TouchedNodeIDs(); got[0] != "anchor-a" ||
		got[1] != "edge-a" || got[2] != "node-a" ||
		got[3] != "object-a" || got[4] != "object-b" ||
		got[5] != "revision-a" {
		t.Fatalf("evidence = %#v", sink.sessions[0].TouchedNodeIDs())
	}
}

func TestInteractionAuditorRejectsMismatchedPersistedReceipt(t *testing.T) {
	now := time.Now().UTC()
	sink := &auditSink{mutateResult: func(session interaction.Session) interaction.Session {
		session.AuthorizationOperation = string(auth.OperationSubscriptionCreate)
		return session
	}}
	recorder, err := interaction.NewRecorder(context.Background(), sink)
	if err != nil {
		t.Fatal(err)
	}
	auditor, err := NewInteractionAuditor(
		recorder, fixedAuditSnapshot{at: now})
	if err != nil {
		t.Fatal(err)
	}
	err = auditor.RecordFleetAction(context.Background(), AuditRecord{
		Operation: auth.OperationEventPublish, ActionID: []byte("action"),
		ObjectID: []byte("event"), AuthorizationFingerprint: auth.Fingerprint{1},
		AuthorizationExpiresAt: now.Add(time.Hour), OccurredAt: now,
	})
	if err == nil {
		t.Fatal("expected mismatched receipt to fail")
	}
}

func TestInteractionAuditorRejectsNilRecorder(t *testing.T) {
	if _, err := NewInteractionAuditor(nil, fixedAuditSnapshot{}); err == nil {
		t.Fatal("nil recorder succeeded")
	}
	recorder, err := interaction.NewRecorder(
		context.Background(), &auditSink{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewInteractionAuditor(recorder, nil); err == nil {
		t.Fatal("nil snapshot provider succeeded")
	}
}

type auditSink struct {
	sessions     []interaction.Session
	mutateResult func(interaction.Session) interaction.Session
}

func (*auditSink) EnsureInteractionSink(context.Context) error { return nil }

func (s *auditSink) RecordInteraction(_ context.Context, session interaction.Session) error {
	s.sessions = append(s.sessions, session)
	return nil
}

func (s *auditSink) RecordInteractionResult(
	ctx context.Context, session interaction.Session,
) (interaction.Session, error) {
	if err := s.RecordInteraction(ctx, session); err != nil {
		return interaction.Session{}, err
	}
	result := session
	result.Actor = interaction.ActorContext{SubjectID: "trusted-subject"}
	result.Reason = interaction.Reason{Code: "audit_purpose"}
	if s.mutateResult != nil {
		result = s.mutateResult(result)
	}
	return result, nil
}
