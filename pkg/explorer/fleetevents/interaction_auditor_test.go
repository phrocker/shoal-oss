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
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	recorder, err := interaction.NewRecorder(context.Background(), sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.SetClock(func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	auditor, err := NewInteractionAuditor(
		recorder, sink, fixedAuditSnapshot{at: now})
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
		len(sink.sessions[0].TouchedNodeIDs()) != 1 ||
		len(sink.sessions[0].TouchedEdgeIDs()) != 1 {
		t.Fatalf("session = %#v", sink.sessions)
	}
	if got := sink.sessions[0].TouchedNodeIDs(); got[0] != "node-a" {
		t.Fatalf("node evidence = %#v", got)
	}
	if got := sink.sessions[0].TouchedEdgeIDs(); got[0] != "edge-a" {
		t.Fatalf("edge evidence = %#v", got)
	}
	if len(sink.sessions[0].SeedEvidence) != 1 ||
		sink.sessions[0].SeedEvidence[0].AnchorID != "anchor-a" {
		t.Fatalf("typed evidence = %#v", sink.sessions[0].SeedEvidence)
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
	if err := recorder.SetClock(func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	auditor, err := NewInteractionAuditor(
		recorder, sink, fixedAuditSnapshot{at: now})
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

func TestInteractionAuditorSeparatesPublicAndLifecycleSinks(t *testing.T) {
	now := time.Now().UTC()
	publicSink := &auditSink{}
	trustedSink := &auditSink{}
	recorder, err := interaction.NewRecorder(
		context.Background(), publicSink)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.SetClock(func() time.Time { return now }); err != nil {
		t.Fatal(err)
	}
	auditor, err := NewInteractionAuditor(
		recorder, trustedSink, fixedAuditSnapshot{at: now})
	if err != nil {
		t.Fatal(err)
	}
	record := AuditRecord{
		Operation: auth.OperationEventPublish, ActionID: []byte("public"),
		RequestID: "public-request", ObjectID: []byte("event"),
		AuthorizationFingerprint: auth.Fingerprint{1},
		AuthorizationExpiresAt:   now.Add(time.Hour), OccurredAt: now,
	}
	if err := auditor.RecordFleetAction(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(publicSink.sessions) != 1 || len(trustedSink.sessions) != 0 {
		t.Fatalf(
			"public/trusted records = %d/%d",
			len(publicSink.sessions), len(trustedSink.sessions),
		)
	}
	record.Operation = auth.OperationDispatch
	record.ActionID = []byte("lifecycle")
	record.RequestID = "lifecycle-request"
	if err := auditor.RecordFleetAction(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(publicSink.sessions) != 1 || len(trustedSink.sessions) != 1 {
		t.Fatalf(
			"public/trusted records = %d/%d",
			len(publicSink.sessions), len(trustedSink.sessions),
		)
	}
}

func TestInteractionAuditorRejectsNilRecorder(t *testing.T) {
	if _, err := NewInteractionAuditor(
		nil, &auditSink{}, fixedAuditSnapshot{},
	); err == nil {
		t.Fatal("nil recorder succeeded")
	}
	recorder, err := interaction.NewRecorder(
		context.Background(), &auditSink{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewInteractionAuditor(
		recorder, nil, fixedAuditSnapshot{},
	); err == nil {
		t.Fatal("nil trusted sink succeeded")
	}
	if _, err := NewInteractionAuditor(
		recorder, &auditSink{}, nil,
	); err == nil {
		t.Fatal("nil snapshot provider succeeded")
	}
	var snapshots *nilAuditSnapshot
	if _, err := NewInteractionAuditor(
		recorder, &auditSink{}, snapshots,
	); err == nil {
		t.Fatal("typed-nil snapshot provider succeeded")
	}
}

type nilAuditSnapshot struct{}

func (*nilAuditSnapshot) InteractionSnapshot(
	context.Context,
) (explorer.Snapshot, error) {
	panic("typed-nil snapshot provider must be rejected")
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
	if s.mutateResult != nil {
		result = s.mutateResult(result)
	}
	return result, nil
}
