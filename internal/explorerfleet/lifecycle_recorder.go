// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"reflect"
	"strconv"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// LifecycleInteractionReader reads authoritative committed lifecycle receipts
// from the same corpus used by the result sink.
type LifecycleInteractionReader interface {
	InteractionRecord(
		context.Context, shoal.ID,
	) (explorer.InteractionRecord, error)
}

// LifecycleRecorder converts fleet lifecycle admissions into durable,
// authorization-pinned interaction receipts.
type LifecycleRecorder struct {
	record func(
		context.Context, interaction.Session,
	) (interaction.Session, error)
	read func(
		context.Context, shoal.ID,
	) (explorer.InteractionRecord, error)
}

// NewLifecycleRecorder constructs the production lifecycle receipt adapter.
func NewLifecycleRecorder(
	recorder interaction.ResultSink,
) (*LifecycleRecorder, error) {
	if isNilLifecycleInteractionRecorder(recorder) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet lifecycle interaction recorder is required",
		)
	}
	return &LifecycleRecorder{
		record: recorder.RecordInteractionResult,
	}, nil
}

// NewLifecycleRecorderWithReader constructs the production adapter with an
// authoritative same-corpus reader for refreshed retry reconciliation.
func NewLifecycleRecorderWithReader(
	recorder interaction.ResultSink,
	reader LifecycleInteractionReader,
) (*LifecycleRecorder, error) {
	if isNilLifecycleInteractionRecorder(recorder) ||
		isNilLifecycleInteractionRecorder(reader) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet lifecycle interaction recorder and reader are required",
		)
	}
	return &LifecycleRecorder{
		record: recorder.RecordInteractionResult,
		read:   reader.InteractionRecord,
	}, nil
}

// RecordLifecycle records one stable pre-admission receipt. Actor, delegation,
// and reason are deliberately omitted from the request and accepted only from
// the trusted recorder result.
func (r *LifecycleRecorder) RecordLifecycle(
	ctx context.Context,
	lifecycle fleet.Lifecycle,
) error {
	if r == nil || r.record == nil {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet lifecycle recorder is required",
		)
	}
	if err := lifecycle.Operation.Validate(); err != nil {
		return err
	}
	requested := lifecycleSession(lifecycle)
	if r.read != nil {
		legacyRequested := requested
		legacyRequested.ID = legacyLifecycleSessionID(lifecycle)
		record, readErr := r.read(
			context.WithoutCancel(ctx), legacyRequested.ID,
		)
		if readErr == nil {
			if err := validateLifecycleReplay(
				record.Session, legacyRequested, lifecycle,
			); err != nil {
				return err
			}
			return nil
		}
	}
	persisted, recordErr := r.record(ctx, requested)
	if recordErr != nil {
		if r.read != nil {
			record, readErr := r.read(
				context.WithoutCancel(ctx), requested.ID,
			)
			if readErr == nil {
				reconcileErr := validateLifecycleReplay(
					record.Session, requested, lifecycle,
				)
				if reconcileErr == nil {
					if interaction.IsCommittedRecord(recordErr) {
						return recordErr
					}
					return nil
				}
				if interaction.IsCommittedRecord(recordErr) {
					return committedLifecycleError(
						recordErr, reconcileErr,
					)
				}
			}
		}
		if !interaction.IsCommittedRecord(recordErr) || persisted.ID == "" {
			return recordErr
		}
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

func validateLifecycleReplay(
	persisted interaction.Session,
	requested interaction.Session,
	lifecycle fleet.Lifecycle,
) error {
	canonical, err := persisted.Canonical()
	if err != nil {
		return err
	}
	if canonical.Actor.SubjectID != lifecycle.Subject {
		return shoal.NewError(
			shoal.ErrorUnauthorized,
			"fleet lifecycle receipt belongs to another subject",
		)
	}
	if canonical.RecordedAt.Before(canonical.SnapshotAsOf) ||
		!canonical.RecordedAt.Before(canonical.AuthorizationExpiresAt) {
		return shoal.NewError(
			shoal.ErrorInternal,
			"stored fleet lifecycle receipt has invalid chronology",
		)
	}
	expected := requested
	expected.RecordedAt = canonical.RecordedAt
	expected.Actor = canonical.Actor
	expected.Reason = canonical.Reason
	expected.SnapshotID = canonical.SnapshotID
	expected.SnapshotAsOf = canonical.SnapshotAsOf
	expected.AuthorizationFingerprint = canonical.AuthorizationFingerprint
	expected.AuthorizationExpiresAt = canonical.AuthorizationExpiresAt
	expected, err = expected.Canonical()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(canonical, expected) {
		return shoal.NewError(
			shoal.ErrorConflict,
			"stored fleet lifecycle receipt has different mutation identity",
		)
	}
	return nil
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
			operation + "\x00" + string(lifecycle.AgentID) +
				"\x00" + hex.EncodeToString(lifecycle.MutationDigest[:]),
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
		"fleet.lifecycle.v2",
		string(lifecycle.Operation),
		string(lifecycle.RequestID),
		string(lifecycle.AgentID),
	)
}

func legacyLifecycleSessionID(lifecycle fleet.Lifecycle) shoal.ID {
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

func committedLifecycleError(recordErr, validationErr error) error {
	if recordErr != nil {
		return explorer.MarkCommittedInteraction(errors.Join(
			recordErr, validationErr))
	}
	return explorer.MarkCommittedInteraction(validationErr)
}

func isNilLifecycleInteractionRecorder(
	recorder any,
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
