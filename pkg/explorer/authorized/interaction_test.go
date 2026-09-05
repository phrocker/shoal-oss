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

package authorized_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestAuthorizedInteractionRecorderAndViews(t *testing.T) {
	f := newFixture(t)
	receipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///authorized-interaction.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "authorized interaction evidence",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Set(snapshot.AsOf.Add(time.Second))
	decision := f.decision(
		t,
		"recorder",
		[][]byte{f.sourceA},
		[][]byte{f.policyA},
		[]auth.Operation{
			auth.OperationRead,
			auth.OperationRetrieve,
			auth.OperationValidate,
		},
	)
	ctx := f.context(t, decision)
	view, err := f.clientA.Document(
		ctx, receipt.Document.ID, receipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:                       "session-authorized",
		RecordedAt:               f.clock.Now(),
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		SeedNodeIDs:              []shoal.ID{firstSpanID(t, view)},
	}
	if err := f.clientA.EnsureInteractionSink(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.clientA.RecordInteraction(ctx, session); err != nil {
		t.Fatal(err)
	}
	hydrated, err := f.clientA.Interaction(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if hydrated.AuthorizationFingerprint != session.AuthorizationFingerprint ||
		hydrated.SnapshotID != session.SnapshotID ||
		hydrated.Actor.SubjectID != decision.Subject() ||
		hydrated.Actor.ActorID != decision.Actor() {
		t.Fatalf("hydrated interaction = %+v", hydrated)
	}
	summaries, err := f.clientA.Interactions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 || summaries[0].SessionID != session.ID {
		t.Fatalf("authorized interactions = %+v", summaries)
	}

	bobDecision := f.decision(
		t,
		"other-reader",
		[][]byte{f.sourceB},
		[][]byte{f.policyB},
		[]auth.Operation{
			auth.OperationRead,
			auth.OperationRetrieve,
			auth.OperationValidate,
		},
	)
	bob := f.context(t, bobDecision)
	if _, err := f.clientB.Interaction(
		bob, session.ID,
	); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("unauthorized interaction read = %v", err)
	}
	summaries, err = f.clientB.Interactions(bob)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 0 {
		t.Fatalf("unauthorized list leaked interactions: %+v", summaries)
	}

	if _, err := f.clientB.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///authorized-interaction.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "replacement evidence under a different source policy",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.clientA.Interaction(
		ctx, session.ID,
	); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("revoked source left interaction readable: %v", err)
	}
}

func TestAuthorizedInteractionEnrichesTrustedActorDelegationAndReason(t *testing.T) {
	f := newFixture(t)
	receipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///actor-context.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "actor context evidence",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Set(snapshot.AsOf.Add(time.Second))
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject:             "human-subject",
		Actor:               "agent-actor",
		ClientID:            "mcp-client",
		OnBehalfOf:          []shoal.ID{"fleet", "delegating-agent"},
		AuthorizationDomain: f.domain,
		AllowedOperations: []auth.Operation{
			auth.OperationRead,
			auth.OperationRetrieve,
		},
		PermittedSourceIDs:    [][]byte{f.sourceA},
		PermittedPolicyIDs:    [][]byte{f.policyA},
		PolicyGeneration:      1,
		AuthenticationExpires: f.clock.Now().Add(time.Hour),
		RequestID:             "actor-context-request",
		AuditPurpose:          "fulfill grounded retrieval request",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := f.context(t, decision)
	view, err := f.clientA.Document(
		ctx, receipt.Document.ID, receipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:         "session-actor-context",
		RecordedAt: f.clock.Now(),
		Operation:  interaction.OperationRetrieval,
		Actor: interaction.ActorContext{
			SubjectID: "spoofed-subject",
			ActorID:   "spoofed-actor",
		},
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		SeedNodeIDs:              []shoal.ID{firstSpanID(t, view)},
	}
	if err := f.clientA.RecordInteraction(ctx, session); err != nil {
		t.Fatal(err)
	}
	hydrated, err := f.base.Interaction(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if hydrated.Actor.SubjectID != decision.Subject() ||
		hydrated.Actor.ActorID != decision.Actor() ||
		hydrated.Actor.ClientID != decision.ClientID() ||
		len(hydrated.Actor.OnBehalfOf) != 2 ||
		hydrated.Actor.OnBehalfOf[0] != "fleet" ||
		hydrated.Actor.OnBehalfOf[1] != "delegating-agent" ||
		hydrated.Reason.Code != "audit_purpose" ||
		hydrated.Reason.Digest !=
			interaction.Digest(decision.AuditPurpose()) {
		t.Fatalf("trusted actor enrichment = %+v", hydrated)
	}
	subgraph, err := f.base.InteractionSubgraph(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	var properties shoal.Metadata
	for _, node := range subgraph.Nodes {
		if node.Kind == interaction.KindSession {
			properties = node.Properties
			break
		}
	}
	if properties[interaction.PropertySubjectID] != "human-subject" ||
		properties[interaction.PropertyActorID] != "agent-actor" ||
		properties[interaction.PropertyClientID] != "mcp-client" ||
		properties[interaction.PropertyDelegationCount] != "2" ||
		properties[interaction.PropertyDelegationID] == "" ||
		properties[interaction.PropertyReasonCode] != "audit_purpose" ||
		properties[interaction.PropertyReasonDigest] == "" {
		t.Fatalf("actor graph properties = %+v", properties)
	}
	for _, value := range properties {
		if strings.Contains(value, decision.AuditPurpose()) {
			t.Fatal("raw audit purpose entered the interaction graph")
		}
	}
}

func TestAuthorizedInteractionRecorderRejectsWrongPin(t *testing.T) {
	f := newFixture(t)
	receipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///authorized-interaction.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "authorized interaction evidence",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Set(snapshot.AsOf.Add(time.Second))
	decision := f.decision(
		t,
		"recorder",
		[][]byte{f.sourceA},
		[][]byte{f.policyA},
		[]auth.Operation{
			auth.OperationRead,
			auth.OperationRetrieve,
			auth.OperationValidate,
		},
	)
	ctx := f.context(t, decision)
	view, err := f.clientA.Document(
		ctx, receipt.Document.ID, receipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:                       "session-wrong-pin",
		RecordedAt:               f.clock.Now(),
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: "auth-sha256:wrong",
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		SeedNodeIDs:              []shoal.ID{firstSpanID(t, view)},
	}
	if err := f.clientA.RecordInteraction(
		ctx, session,
	); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("wrong authorization pin record = %v", err)
	}
	if _, err := f.base.Interaction(
		ctx, session.ID,
	); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("rejected interaction was persisted: %v", err)
	}
}
