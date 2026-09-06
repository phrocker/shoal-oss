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
	); !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("mismatched trusted result = %v", err)
	}
}

func TestLifecycleRecorderPreservesCommittedAmbiguity(t *testing.T) {
	cause := context.DeadlineExceeded
	sink := &trustedLifecycleRecorder{
		lifecycle: testLifecycle(),
		err:       explorer.MarkCommittedInteraction(cause),
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
	lifecycle fleet.Lifecycle
	requests  []interaction.Session
	mutate    func(*interaction.Session)
	err       error
}

func (r *trustedLifecycleRecorder) Record(
	_ context.Context,
	request interaction.Session,
) (interaction.Session, error) {
	r.requests = append(r.requests, request)
	if r.err != nil {
		return interaction.Session{}, r.err
	}
	result := request
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
	return result, nil
}
