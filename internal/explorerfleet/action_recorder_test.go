// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestActionRecorderPreservesExactEvidenceAndTrustedResult(t *testing.T) {
	record := testActionRecord()
	record.EvidenceSnapshotID = "snapshot"
	record.EvidenceSnapshotAsOf = time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC)
	record.ExecutionFingerprint = auth.Fingerprint(sha256.Sum256([]byte("authorization")))
	record.ExecutionExpiresAt = record.Deadline
	sink := &capturingActionRecorder{record: record}
	recorder, err := NewActionRecorder(sink)
	if err != nil {
		t.Fatal(err)
	}

	record.Evidence = []fleet.EvidenceRef{
		{
			AnchorID: "anchor", Kind: interaction.EvidenceDocument,
			Citation: document.Citation{
				DocumentID: "document", RevisionID: "revision", SpanID: "span",
				Range: document.SourceRange{
					Start: document.SourcePosition{Offset: 4},
					End:   document.SourcePosition{Offset: 9},
				},
			},
			NodeIDs:    []shoal.ID{"document", "span"},
			Visibility: []string{"restricted"},
		},
		{
			AnchorID: "graph-anchor", Kind: interaction.EvidenceGraph,
			NodeIDs: []shoal.ID{"left", "right"}, EdgeIDs: []shoal.ID{"edge"},
			Assertions: []interaction.AssertionReference{{
				AssertionID: "assertion", EdgeID: "edge",
				Origin: ontology.AssertionExplicit,
			}},
			Visibility: []string{"restricted"},
		},
	}
	audit := fleet.ActionAudit{
		Phase: "effect_outcome", Operation: auth.OperationInvoke,
		Record: record,
	}
	if err := recorder.RecordAction(context.Background(), audit); err != nil {
		t.Fatal(err)
	}
	if len(sink.sessions) != 1 ||
		len(sink.sessions[0].Turns[0].ToolCall.RetrievedEvidence) != 2 {
		t.Fatalf("captured sessions = %#v", sink.sessions)
	}
	session := sink.sessions[0]
	if session.AuthorizationOperation != string(auth.OperationInvoke) {
		t.Fatalf("authorization operation = %q", session.AuthorizationOperation)
	}
	captured := session.Turns[0].ToolCall.RetrievedEvidence[0]
	if captured.AnchorID != record.Evidence[0].AnchorID ||
		captured.Kind != record.Evidence[0].Kind ||
		!reflect.DeepEqual(captured.Citation, record.Evidence[0].Citation) ||
		!reflect.DeepEqual(captured.NodeIDs, record.Evidence[0].NodeIDs) ||
		session.SnapshotID != record.EvidenceSnapshotID ||
		!session.SnapshotAsOf.Equal(record.EvidenceSnapshotAsOf) ||
		!reflect.DeepEqual(
			session.Turns[0].ToolCall.RetrievedNodeIDs,
			[]shoal.ID{"document", "left", "right", "span"}) {
		t.Fatalf("captured evidence = %#v", captured)
	}
	graph := session.Turns[0].ToolCall.RetrievedEvidence[1]
	if graph.Kind != interaction.EvidenceGraph ||
		!reflect.DeepEqual(graph.Assertions, record.Evidence[1].Assertions) ||
		!reflect.DeepEqual(graph.EdgeIDs, record.Evidence[1].EdgeIDs) {
		t.Fatalf("captured graph evidence = %#v", graph)
	}
}

func TestActionRecorderRejectsAbsentAndDivergentResult(t *testing.T) {
	var typedNil *capturingActionRecorder
	if _, err := NewActionRecorder(typedNil); !shoal.IsErrorCode(
		err, shoal.ErrorInvalidArgument,
	) {
		t.Fatalf("typed nil recorder error = %v", err)
	}
	record := testActionRecord()
	sink := &capturingActionRecorder{record: record, diverge: true}
	recorder, err := NewActionRecorder(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.RecordAction(context.Background(), fleet.ActionAudit{
		Phase: "enqueue_admission", Operation: auth.OperationDispatch,
		Record: record,
	}); !shoal.IsErrorCode(err, shoal.ErrorInternal) ||
		!explorer.IsCommittedInteraction(err) {
		t.Fatalf("divergent result error = %v", err)
	}
	if got := sink.sessions[0].AuthorizationOperation; got != "" {
		t.Fatalf("unpinned authorization operation = %q", got)
	}
}

func TestActionRecorderReturnsSinkErrorsUnchanged(t *testing.T) {
	record := testActionRecord()
	sinkErr := errors.New("ambiguous durable sink")
	recorder, err := NewActionRecorder(&capturingActionRecorder{
		record: record, err: sinkErr,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = recorder.RecordAction(context.Background(), fleet.ActionAudit{
		Phase: "effect_outcome", Operation: auth.OperationInvoke, Record: record,
	})
	if !errors.Is(err, sinkErr) {
		t.Fatalf("sink error = %v", err)
	}
}

func TestActionRecorderMarksErrorCodeOutcomeFailed(t *testing.T) {
	record := testActionRecord()
	record.Version = 2
	record.State = fleet.DispatchFailed
	record.ClaimID = []byte("claim")
	record.ClaimFence = 1
	record.ClaimLease = time.Minute
	record.ClaimLeaseUntil = record.Deadline
	record.ExecutionPolicyGeneration = 1
	record.ExecutionExpiresAt = record.Deadline
	record.EffectPossible = true
	record.ErrorCode = "executor_failure"
	sink := &capturingActionRecorder{record: record}
	recorder, err := NewActionRecorder(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.RecordAction(context.Background(), fleet.ActionAudit{
		Phase: "effect_outcome", Operation: auth.OperationInvoke, Record: record,
	}); err != nil {
		t.Fatal(err)
	}
	if !sink.sessions[0].Turns[0].Failed {
		t.Fatal("error-code outcome was not recorded as failed")
	}
}

type capturingActionRecorder struct {
	sessions []interaction.Session
	record   fleet.ActionRecord
	err      error
	diverge  bool
}

func (r *capturingActionRecorder) Record(
	_ context.Context,
	session interaction.Session,
) (interaction.Session, error) {
	if r.err != nil {
		return interaction.Session{}, r.err
	}
	session.RecordedAt = time.Date(2026, 9, 6, 9, 0, 0, 0, time.UTC)
	session.Actor = interaction.ActorContext{
		SubjectID: r.record.Subject, ActorID: r.record.Actor,
		ClientID:   r.record.ClientID,
		OnBehalfOf: append([]shoal.ID(nil), r.record.OnBehalfOf...),
	}
	canonical, err := session.Canonical()
	if err != nil {
		return interaction.Session{}, err
	}
	r.sessions = append(r.sessions, canonical)
	if r.diverge {
		canonical.ResultID = "divergent"
	}
	return canonical, nil
}
