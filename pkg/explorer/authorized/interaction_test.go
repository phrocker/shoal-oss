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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestAuthorizedInteractionRejectsForgedDocumentEvidence(t *testing.T) {
	f := newFixture(t)
	firstReceipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///first-evidence.txt", MediaType: explorer.MediaTypeText,
		Content: "first evidence",
	})
	if err != nil {
		t.Fatal(err)
	}
	secondReceipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///second-evidence.txt", MediaType: explorer.MediaTypeText,
		Content: "second evidence",
	})
	if err != nil {
		t.Fatal(err)
	}
	firstView, err := f.clientA.Document(
		f.admin(t), firstReceipt.Document.ID, firstReceipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	secondView, err := f.clientA.Document(
		f.admin(t), secondReceipt.Document.ID, secondReceipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	firstSpan := firstDocumentSpan(t, firstView)
	secondSpan := firstDocumentSpan(t, secondView)
	citation := document.Citation{
		DocumentID: firstSpan.DocumentID,
		RevisionID: firstSpan.RevisionID,
		SectionID:  firstSpan.SectionID,
		SpanID:     firstSpan.ID,
		Range:      firstSpan.Range,
	}
	anchor, err := inference.NewDocumentAnchor(citation, firstSpan.Text)
	if err != nil {
		t.Fatal(err)
	}
	reference := interaction.EvidenceReference{
		AnchorID: anchor.ID(),
		Kind:     interaction.EvidenceDocument,
		Citation: citation,
		NodeIDs: []shoal.ID{
			citation.DocumentID, citation.SectionID, citation.SpanID,
		},
	}
	snapshot, err := f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Set(snapshot.AsOf.Add(time.Second))
	decision := f.decision(
		t, "forged-document-evidence",
		[][]byte{f.sourceA}, [][]byte{f.policyA},
		[]auth.Operation{
			auth.OperationRead, auth.OperationRetrieve, auth.OperationValidate,
		},
	)
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		t.Fatal(err)
	}
	baseSession := interaction.Session{
		ID:                       "forged-document-session",
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		SeedNodeIDs:              reference.NodeIDs,
		SeedEvidence:             []interaction.EvidenceReference{reference},
	}
	accepted := baseSession
	accepted.ID = "verified-document-session"
	if err := f.clientA.RecordInteraction(
		f.context(t, decision), accepted,
	); err != nil {
		t.Fatalf("verified document evidence was rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*interaction.Session)
	}{
		{
			name: "anchor",
			mutate: func(session *interaction.Session) {
				session.SeedEvidence[0].AnchorID = "forged-anchor"
			},
		},
		{
			name: "section",
			mutate: func(session *interaction.Session) {
				session.SeedEvidence[0].Citation.SectionID =
					secondSpan.SectionID
				session.SeedEvidence[0].NodeIDs = []shoal.ID{
					citation.DocumentID,
					secondSpan.SectionID,
					citation.SpanID,
				}
				session.SeedNodeIDs = session.SeedEvidence[0].NodeIDs
			},
		},
		{
			name: "span",
			mutate: func(session *interaction.Session) {
				session.SeedEvidence[0].Citation.SpanID = secondSpan.ID
				session.SeedEvidence[0].NodeIDs = []shoal.ID{
					citation.DocumentID,
					citation.SectionID,
					secondSpan.ID,
				}
				session.SeedNodeIDs = session.SeedEvidence[0].NodeIDs
			},
		},
		{
			name: "range",
			mutate: func(session *interaction.Session) {
				session.SeedEvidence[0].Citation.Range.End.Offset--
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := baseSession
			session.ID = shoal.ID(string(session.ID) + "-" + test.name)
			session.SeedNodeIDs = append(
				[]shoal.ID(nil), baseSession.SeedNodeIDs...)
			session.SeedEvidence = append(
				[]interaction.EvidenceReference(nil), baseSession.SeedEvidence...)
			session.SeedEvidence[0].NodeIDs = append(
				[]shoal.ID(nil), baseSession.SeedEvidence[0].NodeIDs...)
			test.mutate(&session)
			if err := f.clientA.RecordInteraction(
				f.context(t, decision), session,
			); err == nil {
				t.Fatal("forged document evidence was recorded")
			}
		})
	}
	records, err := f.base.InteractionRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Session.ID != accepted.ID {
		t.Fatalf("forged evidence produced durable records: %+v", records)
	}
}

type assertionInteractionBase struct {
	*explorer.Explorer
	assertion ontology.Assertion
}

func (b *assertionInteractionBase) Neighborhood(
	ctx context.Context,
	request explorer.NeighborhoodRequest,
) (explorer.Neighborhood, error) {
	result, err := b.Explorer.Neighborhood(ctx, request)
	if err == nil {
		result.Assertions = append(result.Assertions, b.assertion)
	}
	return result, err
}

func TestAuthorizedInteractionRejectsForgedAssertionEvidence(t *testing.T) {
	f := newFixture(t)
	firstReceipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///assertion-first.txt", MediaType: explorer.MediaTypeText,
		Content: "assertion first",
	})
	if err != nil {
		t.Fatal(err)
	}
	secondReceipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///assertion-second.txt", MediaType: explorer.MediaTypeText,
		Content: "assertion second",
	})
	if err != nil {
		t.Fatal(err)
	}
	firstView, err := f.clientA.Document(
		f.admin(t), firstReceipt.Document.ID, firstReceipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	secondView, err := f.clientA.Document(
		f.admin(t), secondReceipt.Document.ID, secondReceipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	firstSpan := firstDocumentSpan(t, firstView)
	secondSpan := firstDocumentSpan(t, secondView)
	firstCitation := document.Citation{
		DocumentID: firstSpan.DocumentID,
		RevisionID: firstSpan.RevisionID,
		SectionID:  firstSpan.SectionID,
		SpanID:     firstSpan.ID,
		Range:      firstSpan.Range,
	}
	evidenceRef, err := ontology.NewEvidenceRef(
		firstCitation, firstSpan.Text, nil)
	if err != nil {
		t.Fatal(err)
	}
	provenance, err := ontology.NewExtractionProvenance(
		"test", "model", "v1", "prompt", "v1", "extractor", "v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	concept, err := ontology.NewConceptDefinition(
		"endpoint", "Endpoint", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	relationship, err := ontology.NewRelationshipDefinition(
		"related", "Related", "",
		[]shoal.ID{concept.ID()}, []shoal.ID{concept.ID()}, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	object, err := ontology.NewReferenceValue(secondSpan.ID)
	if err != nil {
		t.Fatal(err)
	}
	edgeID := shoal.ID("authoritative-assertion-edge")
	assertion, err := ontology.NewAssertion(
		firstSpan.ID, relationship.ID(), object,
		ontology.AssertionInferred, 0.9,
		[]ontology.EvidenceRef{evidenceRef}, provenance,
		shoal.Metadata{"shoal.graph.edge_id": string(edgeID)})
	if err != nil {
		t.Fatal(err)
	}
	edge := graph.Edge{
		ID: edgeID, From: firstSpan.ID, To: secondSpan.ID,
		Type: "related", Weight: 0.9,
		Properties: shoal.Metadata{
			"ontology_relationship_id":  string(relationship.ID()),
			"ontology.assertion.origin": string(assertion.Origin()),
		},
	}
	if err := f.clientA.Connect(f.admin(t), edge); err != nil {
		t.Fatal(err)
	}
	base := &assertionInteractionBase{
		Explorer:  f.base,
		assertion: assertion,
	}
	client := f.newClient(
		t, base, f.store, f.sourceA, f.policyA, nil)
	raw, err := base.Neighborhood(
		context.Background(),
		explorer.NeighborhoodRequest{
			NodeIDs: []shoal.ID{firstSpan.ID, secondSpan.ID},
			Depth:   1,
		})
	if err != nil {
		t.Fatal(err)
	}
	nodesByID := make(map[shoal.ID]graph.Node, len(raw.Nodes))
	for _, node := range raw.Nodes {
		nodesByID[node.ID] = node
	}
	anchor, err := inference.NewGraphAnchor(graph.Path{
		Nodes: []graph.Node{
			nodesByID[firstSpan.ID],
			nodesByID[secondSpan.ID],
		},
		Edges: []graph.Edge{edge},
	})
	if err != nil {
		t.Fatal(err)
	}
	reference := interaction.EvidenceReference{
		AnchorID: anchor.ID(),
		Kind:     interaction.EvidenceGraph,
		NodeIDs:  []shoal.ID{firstSpan.ID, secondSpan.ID},
		EdgeIDs:  []shoal.ID{edge.ID},
		Assertions: []interaction.AssertionReference{{
			AssertionID: assertion.ID(),
			EdgeID:      edge.ID,
			Origin:      assertion.Origin(),
		}},
	}
	snapshot, err := f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Set(snapshot.AsOf.Add(time.Second))
	decision := f.decision(
		t, "forged-assertion-evidence",
		[][]byte{f.sourceA}, [][]byte{f.policyA},
		[]auth.Operation{
			auth.OperationRead, auth.OperationRetrieve, auth.OperationValidate,
		},
	)
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		t.Fatal(err)
	}
	baseSession := interaction.Session{
		ID:                       "forged-assertion-session",
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		SeedNodeIDs:              reference.NodeIDs,
		SeedEvidence:             []interaction.EvidenceReference{reference},
	}
	accepted := baseSession
	accepted.ID = "verified-assertion-session"
	if err := client.RecordInteraction(
		f.context(t, decision), accepted,
	); err != nil {
		t.Fatalf("verified assertion evidence was rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*interaction.Session)
	}{
		{
			name: "assertion-id",
			mutate: func(session *interaction.Session) {
				session.SeedEvidence[0].Assertions[0].AssertionID =
					"forged-assertion"
			},
		},
		{
			name: "assertion-origin",
			mutate: func(session *interaction.Session) {
				session.SeedEvidence[0].Assertions[0].Origin =
					ontology.AssertionExplicit
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			session := baseSession
			session.ID = shoal.ID(string(session.ID) + "-" + test.name)
			session.SeedNodeIDs = append(
				[]shoal.ID(nil), baseSession.SeedNodeIDs...)
			session.SeedEvidence = append(
				[]interaction.EvidenceReference(nil), baseSession.SeedEvidence...)
			session.SeedEvidence[0].NodeIDs = append(
				[]shoal.ID(nil), reference.NodeIDs...)
			session.SeedEvidence[0].EdgeIDs = append(
				[]shoal.ID(nil), reference.EdgeIDs...)
			session.SeedEvidence[0].Assertions = append(
				[]interaction.AssertionReference(nil), reference.Assertions...)
			test.mutate(&session)
			if err := client.RecordInteraction(
				f.context(t, decision), session,
			); err == nil {
				t.Fatal("forged assertion evidence was recorded")
			}
		})
	}
	records, err := f.base.InteractionRecords(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Session.ID != accepted.ID {
		t.Fatalf("forged assertion evidence produced records: %+v", records)
	}
}

func firstDocumentSpan(
	t *testing.T,
	view explorer.DocumentView,
) document.Span {
	t.Helper()
	var visit func(explorer.SectionView) (document.Span, bool)
	visit = func(section explorer.SectionView) (document.Span, bool) {
		if len(section.Spans) != 0 {
			return section.Spans[0], true
		}
		for _, child := range section.Children {
			if span, ok := visit(child); ok {
				return span, true
			}
		}
		return document.Span{}, false
	}
	span, ok := visit(view.Root)
	if !ok {
		t.Fatal("document has no span")
	}
	return span
}

func TestAuthorizedInteractionRequiresCurrentEdgeAuthorization(t *testing.T) {
	f := newFixture(t)
	receipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///edge-evidence.txt", MediaType: explorer.MediaTypeText,
		Content: "edge evidence",
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := f.clientA.Document(
		f.admin(t), receipt.Document.ID, receipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	spanID := firstSpanID(t, view)
	edgeSelector, err := authorized.NewStaticPolicySelector(
		f.sourceA, f.policyB)
	if err != nil {
		t.Fatal(err)
	}
	edgeClient := f.newClient(
		t, f.base, f.store, f.sourceA, f.policyA, edgeSelector)
	edge := graph.Edge{
		ID: "restricted-evidence-edge", From: spanID, To: spanID,
		Type: "related", Weight: 1,
	}
	if err := edgeClient.Connect(f.admin(t), edge); err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Set(snapshot.AsOf.Add(time.Second))
	neighborhood, err := f.base.Neighborhood(
		context.Background(),
		explorer.NeighborhoodRequest{NodeIDs: []shoal.ID{spanID}, Depth: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	var spanNode graph.Node
	for _, node := range neighborhood.Nodes {
		if node.ID == spanID {
			spanNode = node
			break
		}
	}
	anchor, err := inference.NewGraphAnchor(graph.Path{
		Nodes: []graph.Node{spanNode, spanNode},
		Edges: []graph.Edge{edge},
	})
	if err != nil {
		t.Fatal(err)
	}
	reference := interaction.EvidenceReference{
		AnchorID: anchor.ID(), Kind: interaction.EvidenceGraph,
		NodeIDs: []shoal.ID{spanID}, EdgeIDs: []shoal.ID{edge.ID},
	}
	denied := f.decision(
		t, "edge-denied",
		[][]byte{f.sourceA}, [][]byte{f.policyA},
		[]auth.Operation{
			auth.OperationRead, auth.OperationRetrieve, auth.OperationValidate,
		},
	)
	deniedFingerprint, err := auth.AuthorizationFingerprint(denied)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID: "edge-session", RecordedAt: f.clock.Now(),
		SnapshotID: shoal.ID(snapshot.ID), SnapshotAsOf: snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(deniedFingerprint.String()),
		AuthorizationExpiresAt:   denied.AuthenticationExpires(),
		SeedNodeIDs:              []shoal.ID{spanID},
		SeedEvidence:             []interaction.EvidenceReference{reference},
	}
	if err := f.clientA.RecordInteraction(
		f.context(t, denied), session,
	); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("edge-denied interaction error = %v", err)
	}
	allowed := f.decision(
		t, "edge-allowed",
		[][]byte{f.sourceA}, [][]byte{f.policyA, f.policyB},
		[]auth.Operation{
			auth.OperationRead, auth.OperationRetrieve, auth.OperationValidate,
		},
	)
	allowedFingerprint, err := auth.AuthorizationFingerprint(allowed)
	if err != nil {
		t.Fatal(err)
	}
	session.ID = "edge-session-allowed"
	session.AuthorizationFingerprint = shoal.ID(allowedFingerprint.String())
	session.AuthorizationExpiresAt = allowed.AuthenticationExpires()
	if err := edgeClient.RecordInteraction(
		f.context(t, allowed), session); err != nil {
		t.Fatal(err)
	}
}

type generationChangingInteractionBase struct {
	*explorer.Explorer
	after func()
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

type rejectingSnapshotValidator struct {
	calls int
}

func (v *rejectingSnapshotValidator) ValidateSnapshot(
	context.Context, shoal.ID, time.Time, []shoal.ID,
) error {
	v.calls++
	return shoal.NewError(
		shoal.ErrorConflict, "historical snapshot registry unavailable")
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
	f.clock.Set(snapshot.AsOf.Add(2 * time.Second))
	retry := session
	retry.RecordedAt = snapshot.AsOf.Add(-time.Hour)
	persisted, err := f.clientA.RecordInteractionResult(ctx, retry)
	if err != nil {
		t.Fatal(err)
	}
	if !persisted.RecordedAt.Equal(session.RecordedAt) {
		t.Fatalf("authorized retry time = %v, want %v",
			persisted.RecordedAt, session.RecordedAt)
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

func TestAuthorizedInteractionUsesTheRecordedToolOperation(t *testing.T) {
	f := newFixture(t)
	snapshot, err := f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Set(snapshot.AsOf.Add(time.Second))
	decision := f.decision(
		t,
		"list-only-recorder",
		[][]byte{f.sourceA},
		[][]byte{f.policyA},
		[]auth.Operation{auth.OperationList, auth.OperationValidate},
	)
	ctx := f.context(t, decision)
	snapshot, err = f.clientA.InteractionSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recorder, err := interaction.NewRecorder(ctx, f.clientA)
	if err != nil {
		t.Fatalf("list-only recorder setup: %v", err)
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:                       "session-list-only",
		RecordedAt:               f.clock.Now(),
		Operation:                interaction.OperationToolCall,
		AuthorizationOperation:   string(auth.OperationList),
		SnapshotID:               shoal.ID(snapshot.ID),
		SnapshotAsOf:             snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		Turns: []interaction.Turn{{
			Index:    0,
			ToolCall: &interaction.ToolCall{Kind: "shoal.documents"},
		}},
	}
	if _, err := recorder.Record(ctx, session); err != nil {
		t.Fatalf("list-only interaction record: %v", err)
	}
	legacy := session
	legacy.ID = "session-default-retrieve"
	legacy.AuthorizationOperation = ""
	if _, err := recorder.Record(ctx, legacy); !shoal.IsErrorCode(
		err, shoal.ErrorUnauthorized,
	) {
		t.Fatalf("default retrieve operation with list-only decision = %v", err)
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
	session.ID = "session-wrong-expiry"
	session.AuthorizationFingerprint = shoal.ID(fingerprint.String())
	session.AuthorizationExpiresAt = decision.AuthenticationExpires().Add(-time.Minute)
	if err := f.clientA.RecordInteraction(ctx, session); err != nil {
		t.Fatalf("shorter live authorization expiry record = %v", err)
	}
	session.ID = "session-wrong-snapshot"
	session.AuthorizationExpiresAt = decision.AuthenticationExpires()
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
}

func TestAuthorizedInteractionAcceptsTrustedHistoricalSnapshot(t *testing.T) {
	f := newFixture(t)
	receipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///historical-interaction.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "historical interaction evidence",
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	view, err := f.clientA.Document(
		f.alice(t), receipt.Document.ID, receipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///unrelated-later.txt",
		MediaType: explorer.MediaTypeText,
		Content:   "unrelated later publication",
	}); err != nil {
		t.Fatal(err)
	}
	current, err := f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if current.ID == snapshot.ID {
		t.Fatal("unrelated publication did not advance the snapshot")
	}
	f.clock.Set(snapshot.AsOf.Add(time.Second))
	decision := f.decision(
		t, "historical-recorder",
		[][]byte{f.sourceA}, [][]byte{f.policyA},
		[]auth.Operation{auth.OperationRetrieve},
	)
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:         interaction.DerivedID("session", "historical-snapshot"),
		RecordedAt: f.clock.Now(), Operation: interaction.OperationRetrieval,
		SnapshotID: shoal.ID(snapshot.ID), SnapshotAsOf: snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		SeedNodeIDs:              []shoal.ID{firstSpanID(t, view)},
	}
	if err := f.clientA.RecordInteraction(
		f.context(t, decision), session,
	); err != nil {
		t.Fatalf("trusted historical snapshot was rejected: %v", err)
	}
}

func TestAuthorizedExactRetryUsesTrustedDurableRecord(t *testing.T) {
	f := newFixture(t)
	snapshot, err := f.base.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Set(snapshot.AsOf.Add(time.Second))
	decision := f.decision(
		t, "retry-recorder",
		[][]byte{f.sourceA}, [][]byte{f.policyA},
		[]auth.Operation{auth.OperationRetrieve},
	)
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:         interaction.DerivedID("session", "authorized-retry"),
		RecordedAt: f.clock.Now(), Operation: interaction.OperationRetrieval,
		SnapshotID: shoal.ID(snapshot.ID), SnapshotAsOf: snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
	}
	ctx := f.context(t, decision)
	first, err := f.clientA.RecordInteractionResult(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	selector, err := authorized.NewStaticPolicySelector(f.sourceA, f.policyA)
	if err != nil {
		t.Fatal(err)
	}
	validator := &rejectingSnapshotValidator{}
	retryClient, err := authorized.NewClient(authorized.Config{
		Base: f.base, VectorScorer: f.base,
		InteractionWriter: f.base, InteractionReader: f.base,
		SnapshotValidator: validator,
		Resolver:          f.authority.Resolver(), PolicySelector: selector,
		PolicyStore: f.store, GenerationReader: f.reader, Clock: f.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	retried, err := retryClient.RecordInteractionResult(ctx, session)
	if err != nil {
		t.Fatalf("exact durable retry was rejected: %v", err)
	}
	if !reflect.DeepEqual(retried, first) {
		t.Fatalf("retry result differs: got %+v want %+v", retried, first)
	}
	if validator.calls != 0 {
		t.Fatalf("exact retry consulted snapshot validator %d times",
			validator.calls)
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

func TestAuthorizedInteractionMarksPostCommitExpiry(t *testing.T) {
	f := newFixture(t)
	receipt, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///post-commit-expiry.txt", MediaType: explorer.MediaTypeText,
		Content: "post commit expiry evidence",
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
		t, "expiry-recorder", [][]byte{f.sourceA}, [][]byte{f.policyA},
		[]auth.Operation{
			auth.OperationRead, auth.OperationRetrieve, auth.OperationValidate,
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
			f.clock.Set(decision.AuthenticationExpires())
		},
	}
	client := f.newClient(
		t, base, f.store, f.sourceA, f.policyA, nil)
	recorder, err := interaction.NewRecorder(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.SetClock(f.clock.Now); err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:         "session-post-commit-expiry",
		SnapshotID: shoal.ID(snapshot.ID), SnapshotAsOf: snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		SeedNodeIDs:              []shoal.ID{firstSpanID(t, view)},
	}
	if _, err := recorder.Record(
		ctx, session,
	); !explorer.IsCommittedInteraction(err) {
		t.Fatalf("post-commit expiry error = %v", err)
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
	if err := client.EnsureInteractionSink(
		f.context(t, decision),
	); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("implicit base interaction writer error = %v", err)
	}
}
