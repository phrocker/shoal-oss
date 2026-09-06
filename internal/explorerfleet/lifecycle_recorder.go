// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"reflect"
	"strconv"

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
	persisted, err := r.recorder.RecordInteractionResult(ctx, requested)
	if err != nil {
		return err
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
		expected.Reason, err = interaction.NewReason(
			"audit_purpose", lifecycle.AuditPurpose)
		if err != nil {
			return err
		}
	}
	expected, err = expected.Canonical()
	if err != nil {
		return err
	}
	persisted, err = persisted.Canonical()
	if err != nil || !reflect.DeepEqual(persisted, expected) {
		return explorer.MarkCommittedInteraction(shoal.NewError(
			shoal.ErrorInternal,
			"fleet lifecycle recorder returned a mismatched trusted session",
		))
	}
	return nil
}

func lifecycleSession(lifecycle fleet.Lifecycle) interaction.Session {
	recordedAt := lifecycle.SnapshotAsOf.UTC()
	action := "fleet." + string(lifecycle.Operation) + ".admitted"
	return interaction.Session{
		ID:                       lifecycleSessionID(lifecycle),
		RecordedAt:               recordedAt,
		Operation:                interaction.OperationToolCall,
		SnapshotID:               lifecycle.SnapshotID,
		SnapshotAsOf:             recordedAt,
		AuthorizationFingerprint: shoal.ID(lifecycle.AuthorizationFingerprint.String()),
		AuthorizationExpiresAt:   lifecycle.AuthorizationExpiresAt.UTC(),
		AuthorizationOperation:   string(lifecycle.Operation),
		QueryDigest: interaction.Digest(
			string(lifecycle.Operation) + "\x00" + string(lifecycle.AgentID) +
				"\x00" + hex.EncodeToString(lifecycle.MutationDigest[:]),
		),
		RequestID:  lifecycle.RequestID,
		ResultID:   lifecycle.AgentID,
		StopReason: "pre_admission",
		Turns: []interaction.Turn{{
			Index:    0,
			Decision: action,
			ToolCall: &interaction.ToolCall{
				Kind: action,
			},
		}},
	}
}

func lifecycleSessionID(lifecycle fleet.Lifecycle) shoal.ID {
	digest := sha256.New()
	writeLifecycleField(digest, []byte("shoal.fleet.lifecycle.v1"))
	writeLifecycleField(digest, []byte(lifecycle.Operation))
	writeLifecycleField(digest, []byte(lifecycle.RequestID))
	writeLifecycleField(digest, []byte(lifecycle.CorrelationID))
	writeLifecycleField(digest, []byte(lifecycle.Subject))
	writeLifecycleField(digest, []byte(lifecycle.Actor))
	writeLifecycleField(digest, []byte(lifecycle.ClientID))
	for _, id := range lifecycle.OnBehalfOf {
		writeLifecycleField(digest, []byte(id))
	}
	writeLifecycleField(digest, []byte(lifecycle.AgentID))
	writeLifecycleField(digest, lifecycle.MutationDigest[:])
	writeLifecycleField(
		digest, []byte(strconv.FormatInt(lifecycle.Deadline, 10)))
	writeLifecycleField(
		digest, []byte(lifecycle.AuthorizationFingerprint.String()))
	writeLifecycleField(
		digest,
		[]byte(lifecycle.AuthorizationExpiresAt.UTC().Format(
			"2006-01-02T15:04:05.999999999Z07:00")),
	)
	writeLifecycleField(digest, []byte(lifecycle.AuditPurpose))
	writeLifecycleField(digest, []byte(lifecycle.SnapshotID))
	writeLifecycleField(
		digest,
		[]byte(lifecycle.SnapshotAsOf.UTC().Format(
			"2006-01-02T15:04:05.999999999Z07:00")),
	)
	return interaction.DerivedID(
		"session", hex.EncodeToString(digest.Sum(nil)))
}

func writeLifecycleField(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
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
