// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestLifecycleRecorderUsesExactPinsAndTrustedResult(t *testing.T) {
	lifecycle := testLifecycle()
	sink := &trustedLifecycleRecorder{lifecycle: lifecycle}
	recorder, err := NewLifecycleRecorder(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.RecordLifecycle(context.Background(), lifecycle); err != nil {
		t.Fatal(err)
	}
	requested := sink.requests[0]
	if requested.Operation != interaction.OperationToolCall ||
		!interaction.IsSessionID(requested.ID) ||
		!requested.RecordedAt.IsZero() ||
		requested.AuthorizationOperation != string(lifecycle.Operation) ||
		requested.AuthorizationFingerprint !=
			shoal.ID(lifecycle.AuthorizationFingerprint.String()) ||
		!requested.AuthorizationExpiresAt.Equal(
			lifecycle.AuthorizationExpiresAt) ||
		requested.SnapshotID != lifecycle.SnapshotID ||
		!requested.SnapshotAsOf.Equal(lifecycle.SnapshotAsOf) ||
		!reflect.DeepEqual(requested.Actor, interaction.ActorContext{}) ||
		requested.Reason != (interaction.Reason{}) ||
		len(requested.SeedNodeIDs) != 0 ||
		len(requested.CitedNodeIDs) != 0 ||
		len(requested.Turns) != 1 ||
		requested.Turns[0].Decision !=
			"admitted:"+string(lifecycle.Operation) ||
		requested.Turns[0].ToolCall == nil ||
		requested.Turns[0].ToolCall.Kind !=
			"fleet.registry."+string(lifecycle.Operation) ||
		requested.ResultID != lifecycle.AgentID {
		t.Fatalf("requested lifecycle session = %#v", requested)
	}
}

func TestLifecycleRecorderRetryIsByteStable(t *testing.T) {
	lifecycle := testLifecycle()
	sink := &trustedLifecycleRecorder{lifecycle: lifecycle}
	recorder, err := NewLifecycleRecorder(sink)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := recorder.RecordLifecycle(
			context.Background(), lifecycle,
		); err != nil {
			t.Fatal(err)
		}
	}
	if len(sink.requests) != 2 ||
		!reflect.DeepEqual(sink.requests[0], sink.requests[1]) {
		t.Fatalf("retry sessions differ: %#v", sink.requests)
	}
	refreshed := lifecycle
	refreshed.CorrelationID = "new-correlation"
	refreshed.Actor = "new-actor"
	refreshed.OnBehalfOf = []shoal.ID{"new-delegator"}
	refreshed.AuthorizationFingerprint = auth.Fingerprint{9, 8, 7}
	refreshed.AuthorizationExpiresAt = lifecycle.AuthorizationExpiresAt.Add(time.Hour)
	refreshed.SnapshotID = "new-snapshot"
	refreshed.SnapshotAsOf = lifecycle.SnapshotAsOf.Add(time.Minute)
	refreshed.AuditPurpose = "renewed purpose"
	refreshed.Deadline += int64(time.Minute)
	if lifecycleSessionID(refreshed) != lifecycleSessionID(lifecycle) {
		t.Fatal("renewed authority minted a duplicate lifecycle receipt ID")
	}
	expectedID := interaction.DerivedID(
		"session", "fleet.lifecycle.v1", string(lifecycle.Operation),
		string(lifecycle.RequestID), string(lifecycle.AgentID),
	)
	if lifecycleSessionID(lifecycle) != expectedID {
		t.Fatalf(
			"lifecycle session ID = %q, want %q",
			lifecycleSessionID(lifecycle), expectedID,
		)
	}
}

func TestLifecycleRecorderRejectsTypedNilAndMismatchedResult(t *testing.T) {
	var typedNil *trustedLifecycleRecorder
	if _, err := NewLifecycleRecorder(typedNil); !shoal.IsErrorCode(
		err, shoal.ErrorInvalidArgument,
	) {
		t.Fatalf("typed-nil recorder = %v", err)
	}
	lifecycle := testLifecycle()
	sink := &trustedLifecycleRecorder{
		lifecycle: lifecycle,
		mutate: func(session *interaction.Session) {
			session.AuthorizationOperation = string(auth.OperationRetrieve)
		},
	}
	recorder, err := NewLifecycleRecorder(sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.RecordLifecycle(
		context.Background(), lifecycle,
	); !shoal.IsErrorCode(err, shoal.ErrorInternal) ||
		!explorer.IsCommittedInteraction(err) {
		t.Fatalf("mismatched trusted result = %v", err)
	}
}

func TestLifecycleRecorderRejectsForgedTrustedEnrichment(t *testing.T) {
	tests := map[string]func(*interaction.Session){
		"actor": func(session *interaction.Session) {
			session.Actor.ActorID = "forged-actor"
		},
		"reason": func(session *interaction.Session) {
			session.Reason, _ = interaction.NewReason(
				"audit_purpose", "forged purpose",
			)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			lifecycle := testLifecycle()
			recorder, err := NewLifecycleRecorder(&trustedLifecycleRecorder{
				lifecycle: lifecycle,
				mutate:    mutate,
			})
			if err != nil {
				t.Fatal(err)
			}
			err = recorder.RecordLifecycle(context.Background(), lifecycle)
			if !shoal.IsErrorCode(err, shoal.ErrorInternal) ||
				!explorer.IsCommittedInteraction(err) {
				t.Fatalf("forged trusted enrichment error = %v", err)
			}
		})
	}
}

func TestLifecycleRecorderPreservesCommittedAmbiguity(t *testing.T) {
	cause := context.DeadlineExceeded
	sink := &trustedLifecycleRecorder{
		lifecycle:           testLifecycle(),
		err:                 explorer.MarkCommittedInteraction(cause),
		returnResultOnError: true,
	}
	recorder, err := NewLifecycleRecorder(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = recorder.RecordLifecycle(context.Background(), testLifecycle())
	if !errors.Is(err, cause) || !explorer.IsCommittedInteraction(err) {
		t.Fatalf("committed ambiguity = %v", err)
	}
}

func TestLifecycleRecorderRejectsCommittedDivergentResult(t *testing.T) {
	cause := context.DeadlineExceeded
	sink := &trustedLifecycleRecorder{
		lifecycle:           testLifecycle(),
		err:                 explorer.MarkCommittedInteraction(cause),
		returnResultOnError: true,
		mutate: func(session *interaction.Session) {
			session.ResultID = "different-agent"
		},
	}
	recorder, err := NewLifecycleRecorder(sink)
	if err != nil {
		t.Fatal(err)
	}
	err = recorder.RecordLifecycle(context.Background(), testLifecycle())
	if !errors.Is(err, cause) || !explorer.IsCommittedInteraction(err) ||
		!shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("committed divergent result = %v", err)
	}
}

func TestLifecycleRecorderRejectsTrustedChronology(t *testing.T) {
	for name, recordedAt := range map[string]time.Time{
		"before snapshot": testLifecycle().SnapshotAsOf.Add(-time.Nanosecond),
		"at expiry":       testLifecycle().AuthorizationExpiresAt,
	} {
		t.Run(name, func(t *testing.T) {
			sink := &trustedLifecycleRecorder{
				lifecycle: testLifecycle(), recordedAt: recordedAt,
			}
			recorder, err := NewLifecycleRecorder(sink)
			if err != nil {
				t.Fatal(err)
			}
			err = recorder.RecordLifecycle(context.Background(), testLifecycle())
			if !shoal.IsErrorCode(err, shoal.ErrorInternal) ||
				!explorer.IsCommittedInteraction(err) {
				t.Fatalf("invalid chronology = %v", err)
			}
		})
	}
}

func testLifecycle() fleet.Lifecycle {
	now := time.Date(2026, 9, 6, 6, 0, 0, 0, time.UTC)
	return fleet.Lifecycle{
		Operation:     auth.OperationAgentRegister,
		RequestID:     "request",
		CorrelationID: "correlation",
		Subject:       "subject",
		Actor:         "actor",
		ClientID:      "client",
		OnBehalfOf:    []shoal.ID{"delegator"},
		AgentID:       "agent",
		Deadline:      now.Add(time.Minute).UnixNano(),
		AuthorizationFingerprint: auth.Fingerprint{
			1, 2, 3,
		},
		AuthorizationExpiresAt: now.Add(time.Hour),
		AuditPurpose:           "operate the registered agent",
		SnapshotID:             "snapshot",
		SnapshotAsOf:           now,
	}
}

type trustedLifecycleRecorder struct {
	lifecycle           fleet.Lifecycle
	requests            []interaction.Session
	mutate              func(*interaction.Session)
	err                 error
	recordedAt          time.Time
	returnResultOnError bool
}

func (r *trustedLifecycleRecorder) EnsureInteractionSink(context.Context) error {
	return nil
}

func (r *trustedLifecycleRecorder) RecordInteraction(
	ctx context.Context,
	request interaction.Session,
) error {
	_, err := r.RecordInteractionResult(ctx, request)
	return err
}

func (r *trustedLifecycleRecorder) RecordInteractionResult(
	_ context.Context,
	request interaction.Session,
) (interaction.Session, error) {
	r.requests = append(r.requests, request)
	result := request
	result.RecordedAt = r.recordedAt
	if result.RecordedAt.IsZero() {
		result.RecordedAt = r.lifecycle.SnapshotAsOf.Add(time.Second)
	}
	result.Actor = interaction.ActorContext{
		SubjectID: r.lifecycle.Subject,
		ActorID:   r.lifecycle.Actor,
		ClientID:  r.lifecycle.ClientID,
		OnBehalfOf: append(
			[]shoal.ID(nil), r.lifecycle.OnBehalfOf...),
	}
	if r.lifecycle.AuditPurpose != "" {
		result.Reason, _ = interaction.NewReason(
			"audit_purpose", r.lifecycle.AuditPurpose)
	}
	if r.mutate != nil {
		r.mutate(&result)
	}
	if r.err != nil && !r.returnResultOnError {
		return interaction.Session{}, r.err
	}
	if r.err != nil {
		return result, r.err
	}
	return result, nil
}
