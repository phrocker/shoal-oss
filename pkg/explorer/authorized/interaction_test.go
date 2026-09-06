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
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type generationChangingInteractionBase struct {
	*explorer.Explorer
	after func()
}

func TestNonAnalyticsInteractionRejectsUnverifiedExactEvidence(t *testing.T) {
	f := newFixture(t)
	receipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///ordinary-interaction.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "ordinary retrieval evidence",
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
		t, "recorder", [][]byte{f.sourceA}, [][]byte{f.policyA},
		[]auth.Operation{auth.OperationRead, auth.OperationRetrieve},
	)
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		t.Fatal(err)
	}
	base := interaction.Session{
		RecordedAt:               f.clock.Now(),
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
	}
	ctx := f.context(t, decision)
	view, err := f.clientA.Document(
		ctx, receipt.Document.ID, receipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	nodeID := firstSpanID(t, view)

	ordinary := base
	ordinary.ID = "session-ordinary-retrieval"
	ordinary.SeedNodeIDs = []shoal.ID{nodeID}
	ordinary.Turns = []interaction.Turn{{
		Index: 0,
		ToolCall: &interaction.ToolCall{
			Kind: "retrieve", RetrievedNodeIDs: []shoal.ID{nodeID},
		},
	}}
	if err := f.clientA.RecordInteraction(ctx, ordinary); err != nil {
		t.Fatalf("ordinary retrieval record = %v", err)
	}

	ordinaryAnalyticsTool := base
	ordinaryAnalyticsTool.ID = "session-ordinary-analytics-tool"
	ordinaryAnalyticsTool.Provenance.ToolPolicy =
		string(auth.OperationAnalyticsRead)
	ordinaryAnalyticsTool.Turns = []interaction.Turn{{
		Index: 0,
		ToolCall: &interaction.ToolCall{
			Kind: "analytics", RetrievedNodeIDs: []shoal.ID{nodeID},
		},
	}}
	if err := f.clientA.RecordInteraction(
		ctx, ordinaryAnalyticsTool,
	); err != nil {
		t.Fatalf("ordinary analytics-named tool record = %v", err)
	}
	storedOrdinary, err := f.base.Interaction(
		context.Background(), ordinaryAnalyticsTool.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storedOrdinary.Provenance.ToolPolicy ==
		string(auth.OperationAnalyticsRead) {
		t.Fatal("ordinary interaction retained trusted analytics marker")
	}

	withNode := base
	withNode.ID = "session-exact-node"
	withNode.Turns = []interaction.Turn{{
		Index: 0,
		ToolCall: &interaction.ToolCall{
			Kind: "retrieve",
			RetrievedNodes: []graph.Node{{
				ID: "fabricated", Kind: "document",
			}},
		},
	}}
	if err := f.clientA.RecordInteraction(
		ctx, withNode,
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("non-analytics exact node record = %v", err)
	}

	first := graph.Edge{
		ID: "edge", From: "from", To: "to", Type: "related",
		Properties: shoal.Metadata{"first": ""},
	}
	second := first
	second.Properties = shoal.Metadata{"second": ""}
	withConflict := base
	withConflict.ID = "session-conflicting-edge"
	withConflict.CitedEdges = []graph.Edge{first}
	withConflict.Turns = []interaction.Turn{{
		Index: 0,
		ToolCall: &interaction.ToolCall{
			Kind: "retrieve", RetrievedEdges: []graph.Edge{second},
		},
	}}
	if err := f.clientA.RecordInteraction(
		ctx, withConflict,
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("conflicting exact edge record = %v", err)
	}
}

func (b *generationChangingInteractionBase) RecordInteractionResult(
	ctx context.Context, session interaction.Session,
) (interaction.Session, error) {
	recorded, err := b.Explorer.RecordInteractionResult(ctx, session)
	if err == nil && b.after != nil {
		b.after()
	}
	return recorded, err
}

type forgedResultInteractionBase struct {
	*explorer.Explorer
}

func (b *forgedResultInteractionBase) RecordInteractionResult(
	ctx context.Context, session interaction.Session,
) (interaction.Session, error) {
	recorded, err := b.Explorer.RecordInteractionResult(ctx, session)
	if err == nil {
		recorded.Actor.SubjectID = "forged-return"
	}
	return recorded, err
}

type committedFailureInteractionBase struct {
	*explorer.Explorer
}

func (b *committedFailureInteractionBase) RecordInteractionResult(
	ctx context.Context, session interaction.Session,
) (interaction.Session, error) {
	recorded, err := b.Explorer.RecordInteractionResult(ctx, session)
	if err != nil {
		return recorded, err
	}
	return recorded, explorer.MarkCommittedInteraction(
		shoal.NewError(shoal.ErrorUnavailable, "post-commit failure"))
}

type countingInteractionStore struct {
	authorized.PolicyStore
	nodesCalls int
}

func (s *countingInteractionStore) Nodes(
	ctx context.Context, ids []shoal.ID,
) (map[shoal.ID]authorized.NodeRegistration, error) {
	s.nodesCalls++
	return s.PolicyStore.Nodes(ctx, ids)
}

type countingInteractionBase struct {
	*explorer.Explorer
	recordCalls  int
	recordsCalls int
}

func (b *countingInteractionBase) InteractionRecord(
	ctx context.Context, id shoal.ID,
) (explorer.InteractionRecord, error) {
	b.recordCalls++
	return b.Explorer.InteractionRecord(ctx, id)
}

func (b *countingInteractionBase) InteractionRecords(
	ctx context.Context,
) ([]explorer.InteractionRecord, error) {
	b.recordsCalls++
	return b.Explorer.InteractionRecords(ctx)
}

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
	spoofedReason, err := interaction.NewReason(
		"spoofed_reason", "caller-controlled explanation")
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
		Reason:                   spoofedReason,
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		SeedNodeIDs:              []shoal.ID{firstSpanID(t, view)},
	}
	recorder, err := interaction.NewRecorder(ctx, f.clientA)
	if err != nil {
		t.Fatal(err)
	}
	returned, err := recorder.Record(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	if returned.Actor.SubjectID != decision.Subject() ||
		returned.Actor.ActorID != decision.Actor() ||
		returned.Actor.ClientID != decision.ClientID() ||
		len(returned.Actor.OnBehalfOf) != 2 ||
		returned.Reason.Code != "audit_purpose" ||
		returned.Reason.Digest !=
			interaction.Digest(decision.AuditPurpose()) {
		t.Fatalf("recorder returned untrusted metadata = %+v", returned)
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
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		t.Fatal(err)
	}
	session.ID = "session-wrong-snapshot"
	session.AuthorizationFingerprint = shoal.ID(fingerprint.String())
	session.SnapshotID = "forged-snapshot"
	if err := f.clientA.RecordInteraction(
		ctx, session,
	); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("forged snapshot record = %v", err)
	}
	session.ID = "session-expired-pin"
	session.SnapshotID = shoal.ID(snapshot.ID)
	session.SnapshotAsOf = snapshot.AsOf
	session.RecordedAt = snapshot.AsOf
	session.AuthorizationExpiresAt = snapshot.AsOf.Add(500 * time.Millisecond)
	if err := f.clientA.RecordInteraction(
		ctx, session,
	); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("expired authorization pin record = %v", err)
	}
	session.ID = "session-narrower-live-pin"
	session.RecordedAt = f.clock.Now()
	session.AuthorizationExpiresAt = f.clock.Now().Add(time.Minute)
	if err := f.clientA.RecordInteraction(ctx, session); err != nil {
		t.Fatalf("narrower authorization pin record = %v", err)
	}
	stored, err := f.base.Interaction(context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !stored.AuthorizationExpiresAt.Equal(session.AuthorizationExpiresAt) {
		t.Fatalf("stored expiry = %v, want %v",
			stored.AuthorizationExpiresAt, session.AuthorizationExpiresAt)
	}
}

func TestAuthorizedTombstoneSubgraphDoesNotLeakExistence(t *testing.T) {
	f := newFixture(t)
	receipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///deleted-interaction.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "deleted interaction evidence",
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
		"deletion-reader",
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
		ID:                       "session-deleted-authorized",
		RecordedAt:               f.clock.Now(),
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		SeedNodeIDs:              []shoal.ID{firstSpanID(t, view)},
	}
	if err := f.clientA.RecordInteraction(ctx, session); err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.DeleteInteraction(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	subgraph, err := f.clientA.InteractionSubgraph(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(subgraph.Nodes) != 1 ||
		subgraph.Nodes[0].Kind != interaction.KindTombstone {
		t.Fatalf("authorized tombstone subgraph = %+v", subgraph)
	}
	renewed, err := auth.NewDecision(auth.DecisionConfig{
		Subject:               decision.Subject(),
		Actor:                 decision.Actor(),
		ClientID:              decision.ClientID(),
		OnBehalfOf:            decision.OnBehalfOf(),
		AuthorizationDomain:   decision.AuthorizationDomain(),
		AllowedOperations:     decision.AllowedOperations(),
		PermittedSourceIDs:    decision.PermittedSourceIDs(),
		PermittedPolicyIDs:    decision.PermittedPolicyIDs(),
		PolicyGeneration:      decision.PolicyGeneration(),
		AuthenticationExpires: f.clock.Now().Add(30 * time.Minute),
		RequestID:             "renewed-deletion-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.clientA.InteractionSubgraph(
		f.context(t, renewed), session.ID,
	); err != nil {
		t.Fatalf("renewed shorter credential cannot read tombstone: %v", err)
	}
	if _, err := f.clientA.Interaction(
		ctx, session.ID,
	); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("typed deleted interaction read = %v", err)
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
	if _, err := f.clientB.InteractionSubgraph(
		f.context(t, bobDecision), session.ID,
	); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("unauthorized tombstone read leaked existence: %v", err)
	}
}

func TestAuthorizedInteractionMarksPostCommitGenerationFailure(t *testing.T) {
	f := newFixture(t)
	receipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///post-commit-generation.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "post commit generation evidence",
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
		"generation-recorder",
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
	base := &generationChangingInteractionBase{
		Explorer: f.base,
		after: func() {
			f.reader.Set(f.domain, 2)
		},
	}
	client := f.newClient(
		t, base, f.store, f.sourceA, f.policyA, nil)
	recorder, err := interaction.NewRecorder(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:                       "session-post-commit-generation",
		RecordedAt:               f.clock.Now(),
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		SeedNodeIDs:              []shoal.ID{firstSpanID(t, view)},
	}
	if _, err := recorder.Record(
		ctx, session,
	); !explorer.IsCommittedInteraction(err) {
		t.Fatalf("post-commit generation error = %v", err)
	}
	if _, err := f.base.Interaction(
		context.Background(), session.ID,
	); err != nil {
		t.Fatalf("committed interaction was not durable: %v", err)
	}
}

func TestAuthorizedInteractionRejectsForgedSinkResult(t *testing.T) {
	f := newFixture(t)
	wrapped := &forgedResultInteractionBase{Explorer: f.base}
	client := f.newClient(
		t, wrapped, f.store, f.sourceA, f.policyA, nil)
	snapshot, err := f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Set(snapshot.AsOf.Add(time.Second))
	decision := f.decision(
		t,
		"trusted-result",
		[][]byte{f.sourceA},
		[][]byte{f.policyA},
		[]auth.Operation{auth.OperationRetrieve},
	)
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:                       "session-forged-result",
		RecordedAt:               f.clock.Now(),
		Operation:                interaction.OperationRetrieval,
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
	}
	ctx := f.context(t, decision)
	if _, err := client.RecordInteractionResult(
		ctx, session,
	); !explorer.IsCommittedInteraction(err) {
		t.Fatalf("forged sink result error = %v", err)
	}
	stored, err := f.base.Interaction(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Actor.SubjectID != decision.Subject() {
		t.Fatalf("stored actor was forged: %+v", stored.Actor)
	}
}

func TestAuthorizedInteractionReadsUseBulkAndPointPaths(t *testing.T) {
	f := newFixture(t)
	receipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///bulk-interactions.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "bulk interaction evidence",
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
		"bulk-reader",
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
	for _, id := range []shoal.ID{"session-bulk-a", "session-bulk-b"} {
		if err := f.clientA.RecordInteraction(ctx, interaction.Session{
			ID:                       id,
			RecordedAt:               f.clock.Now(),
			SnapshotID:               shoal.ID(snapshot.ID),
			SnapshotAsOf:             snapshot.AsOf,
			AuthorizationFingerprint: shoal.ID(fingerprint.String()),
			AuthorizationExpiresAt:   decision.AuthenticationExpires(),
			SeedNodeIDs:              []shoal.ID{firstSpanID(t, view)},
		}); err != nil {
			t.Fatal(err)
		}
	}

	store := &countingInteractionStore{PolicyStore: f.store}
	base := &countingInteractionBase{Explorer: f.base}
	client := f.newClient(t, base, store, f.sourceA, f.policyA, nil)
	records, err := client.InteractionRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || store.nodesCalls != 1 ||
		base.recordsCalls != 1 {
		t.Fatalf(
			"records=%d node_batches=%d bulk_reads=%d",
			len(records), store.nodesCalls, base.recordsCalls,
		)
	}
	if _, err := client.InteractionSubgraph(
		ctx, "session-bulk-a",
	); err != nil {
		t.Fatal(err)
	}
	if base.recordCalls != 1 || base.recordsCalls != 1 {
		t.Fatalf("point_reads=%d bulk_reads=%d",
			base.recordCalls, base.recordsCalls)
	}
}

func TestSourceLessInteractionRequiresOriginalAuthorizationProjection(t *testing.T) {
	f := newFixture(t)
	snapshot, err := f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Set(snapshot.AsOf.Add(time.Second))
	ownerDecision := f.decision(
		t,
		"source-less-owner",
		[][]byte{f.sourceA},
		[][]byte{f.policyA},
		[]auth.Operation{
			auth.OperationRead,
			auth.OperationRetrieve,
			auth.OperationValidate,
		},
	)
	owner := f.context(t, ownerDecision)
	fingerprint, err := auth.AuthorizationFingerprint(ownerDecision)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:                       "session-source-less",
		RecordedAt:               f.clock.Now(),
		Operation:                interaction.OperationRetrieval,
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   ownerDecision.AuthenticationExpires(),
	}
	if err := f.clientA.RecordInteraction(owner, session); err != nil {
		t.Fatal(err)
	}
	if _, err := f.clientA.Interaction(owner, session.ID); err != nil {
		t.Fatalf("owner cannot read source-less interaction: %v", err)
	}

	otherDecision := f.decision(
		t,
		"source-less-other",
		[][]byte{f.sourceA},
		[][]byte{f.policyA},
		[]auth.Operation{
			auth.OperationRead,
			auth.OperationRetrieve,
			auth.OperationValidate,
		},
	)
	other := f.context(t, otherDecision)
	if _, err := f.clientA.Interaction(
		other, session.ID,
	); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("source-less point read leaked across projections: %v", err)
	}
	if _, err := f.clientA.InteractionSubgraph(
		other, session.ID,
	); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("source-less subgraph leaked across projections: %v", err)
	}
	summaries, err := f.clientA.Interactions(other)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 0 {
		t.Fatalf("source-less list leaked across projections: %+v", summaries)
	}
}

func TestInteractionReadsRequireExplicitTrustedReader(t *testing.T) {
	f := newFixture(t)
	selector, err := authorized.NewStaticPolicySelector(f.sourceA, f.policyA)
	if err != nil {
		t.Fatal(err)
	}
	client, err := authorized.NewClient(authorized.Config{
		Base:             f.base,
		VectorScorer:     f.base,
		Resolver:         f.authority.Resolver(),
		PolicySelector:   selector,
		PolicyStore:      f.store,
		GenerationReader: f.reader,
		Clock:            f.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	decision := f.decision(
		t,
		"interaction-reader",
		[][]byte{f.sourceA},
		[][]byte{f.policyA},
		[]auth.Operation{auth.OperationRead},
	)
	if _, err := client.InteractionRecords(
		f.context(t, decision),
	); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("implicit base interaction reader error = %v", err)
	}
}
