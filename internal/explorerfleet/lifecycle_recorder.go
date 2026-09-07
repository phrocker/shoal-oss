// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"context"
	"errors"
	"reflect"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// LifecycleInteractionRecorder is the trusted interaction recorder boundary
// used by the fleet lifecycle adapter.
type LifecycleInteractionRecorder interface {
	interaction.ResultSink
}

// LifecycleRecorder converts fleet lifecycle admissions into durable,
// authorization-pinned interaction receipts.
type LifecycleRecorder struct {
	recorder LifecycleInteractionRecorder
}

// NewLifecycleRecorder constructs the production lifecycle receipt adapter.
func NewLifecycleRecorder(
	recorder LifecycleInteractionRecorder,
) (*LifecycleRecorder, error) {
	if isNilLifecycleInteractionRecorder(recorder) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet lifecycle interaction recorder is required",
		)
	}
	return &LifecycleRecorder{recorder: recorder}, nil
}

// RecordLifecycle records one stable pre-admission receipt. Actor, delegation,
// and reason are deliberately omitted from the request and accepted only from
// the trusted recorder result.
func (r *LifecycleRecorder) RecordLifecycle(
	ctx context.Context,
	lifecycle fleet.Lifecycle,
) error {
	if r == nil || isNilLifecycleInteractionRecorder(r.recorder) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet lifecycle recorder is required",
		)
	}
	if err := lifecycle.Operation.Validate(); err != nil {
		return err
	}
	requested := lifecycleSession(lifecycle)
	persisted, recordErr := r.recorder.RecordInteractionResult(ctx, requested)
	if recordErr != nil && !explorer.IsCommittedInteraction(recordErr) {
		return recordErr
	}
	expected := requested
	expected.RecordedAt = persisted.RecordedAt
	expected.Actor = interaction.ActorContext{
		SubjectID: lifecycle.Subject,
		ActorID:   lifecycle.Actor,
		ClientID:  lifecycle.ClientID,
		OnBehalfOf: append(
			[]shoal.ID(nil), lifecycle.OnBehalfOf...),
	}
	if lifecycle.AuditPurpose != "" {
		var err error
		expected.Reason, err = interaction.NewReason(
			"audit_purpose", lifecycle.AuditPurpose)
		if err != nil {
			return committedLifecycleError(recordErr, err)
		}
	}
	if persisted.RecordedAt.Before(lifecycle.SnapshotAsOf) ||
		!persisted.RecordedAt.Before(lifecycle.AuthorizationExpiresAt) {
		return committedLifecycleError(recordErr, shoal.NewError(
			shoal.ErrorInternal,
			"fleet lifecycle recorder returned an invalid trusted chronology",
		))
	}
	expected, err := expected.Canonical()
	if err != nil {
		return committedLifecycleError(recordErr, err)
	}
	persisted, err = persisted.Canonical()
	if err != nil || !reflect.DeepEqual(persisted, expected) {
		return committedLifecycleError(recordErr, shoal.NewError(
			shoal.ErrorInternal,
			"fleet lifecycle recorder returned a mismatched trusted session",
		))
	}
	return recordErr
}

func lifecycleSession(lifecycle fleet.Lifecycle) interaction.Session {
	operation := string(lifecycle.Operation)
	return interaction.Session{
		ID:                       lifecycleSessionID(lifecycle),
		Operation:                interaction.OperationToolCall,
		SnapshotID:               lifecycle.SnapshotID,
		SnapshotAsOf:             lifecycle.SnapshotAsOf.UTC(),
		AuthorizationFingerprint: shoal.ID(lifecycle.AuthorizationFingerprint.String()),
		AuthorizationExpiresAt:   lifecycle.AuthorizationExpiresAt.UTC(),
		AuthorizationOperation:   operation,
		QueryDigest: interaction.Digest(
			operation + "\x00" + string(lifecycle.AgentID),
		),
		RequestID:  lifecycle.RequestID,
		ResultID:   lifecycle.AgentID,
		StopReason: "pre_admission",
		Turns: []interaction.Turn{{
			Index:    0,
			Decision: "admitted:" + operation,
			ToolCall: &interaction.ToolCall{
				Kind: "fleet.registry." + operation,
			},
		}},
	}
}

func lifecycleSessionID(lifecycle fleet.Lifecycle) shoal.ID {
	return interaction.DerivedID(
		"session",
		"fleet.lifecycle.v1",
		string(lifecycle.Operation),
		string(lifecycle.RequestID),
		string(lifecycle.AgentID),
	)
}

func committedLifecycleError(recordErr, validationErr error) error {
	if recordErr != nil {
		return explorer.MarkCommittedInteraction(errors.Join(
			recordErr, validationErr))
	}
	return explorer.MarkCommittedInteraction(validationErr)
}

func isNilLifecycleInteractionRecorder(
	recorder LifecycleInteractionRecorder,
) bool {
	if recorder == nil {
		return true
	}
	value := reflect.ValueOf(recorder)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var _ fleet.LifecycleRecorder = (*LifecycleRecorder)(nil)
