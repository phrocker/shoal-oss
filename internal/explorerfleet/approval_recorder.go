// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strconv"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// ApprovalRecorder records each approval transition through the same
// result-returning interaction recorder the action audits use, pinned to the
// host's current corpus snapshot.
//
// It differs from ActionRecorder in one deliberate way. ActionRecorder expects
// the trusted sink to stamp the record's enqueuer as the actor, which is wrong
// for every caller that is not the enqueuer (#480 item 2). An approval record
// always involves two principals, so the acting one is carried on the audit and
// that is what the sink's stamp is compared against.
//
// The session identity includes the acting principal and its request ID, so
// each attempt has its own session. An approval transition is a compare-and-
// set that another principal can win; with a session keyed on the record
// version alone, the loser's audit would occupy the winner's session and the
// winner could never record — and a crash between an audit and its write would
// wedge the record for every later caller. An audit here therefore records an
// attempt; the durable approval row records the outcome.
type ApprovalRecorder struct {
	recorder  ActionInteractionRecorder
	snapshots fleet.InteractionSnapshotProvider
}

func NewApprovalRecorder(
	recorder ActionInteractionRecorder,
	snapshots fleet.InteractionSnapshotProvider,
) (*ApprovalRecorder, error) {
	if isNilActionInteractionRecorder(recorder) ||
		isNilActionSnapshotProvider(snapshots) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet approval recorder dependencies are required")
	}
	return &ApprovalRecorder{recorder: recorder, snapshots: snapshots}, nil
}

func (r *ApprovalRecorder) RecordApproval(
	ctx context.Context, audit fleet.ApprovalAudit,
) error {
	if audit.Phase == "" {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet approval audit phase is required")
	}
	if err := audit.Operation.Validate(); err != nil {
		return err
	}
	if err := audit.Record.Validate(); err != nil {
		return err
	}
	if err := shoal.ValidateRequiredID(
		"approval audit subject", audit.Subject); err != nil {
		return err
	}
	if err := shoal.ValidateRequiredID(
		"approval audit actor", audit.Actor); err != nil {
		return err
	}
	if err := shoal.ValidateRequiredID(
		"approval audit request ID", audit.RequestID); err != nil {
		return err
	}
	snapshot, err := r.snapshots.InteractionSnapshot(ctx)
	if err != nil {
		return err
	}
	identifier := "fleet." + string(audit.Operation) + "." + audit.Phase
	requested := interaction.Session{
		ID:                     approvalSessionID(audit),
		Operation:              interaction.OperationToolCall,
		AuthorizationOperation: string(audit.Operation),
		QueryDigest: interaction.Digest(
			string(audit.Operation) + ":" + audit.Phase + ":" +
				string(audit.Record.State)),
		RequestID:  audit.RequestID,
		ResultID:   shoal.ID(hex.EncodeToString(audit.Record.ID)),
		StopReason: audit.Phase,
		SnapshotID: shoal.ID(snapshot.ID), SnapshotAsOf: snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(
			audit.AuthorizationFingerprint.String()),
		AuthorizationExpiresAt: audit.AuthorizationExpiresAt,
		Turns: []interaction.Turn{{
			Index: 0, Decision: identifier,
			// A refusal and an expiry are recorded as failed turns, so an
			// activity reader sees them as the request not proceeding.
			Failed: audit.Record.State == fleet.ApprovalRefused ||
				audit.Record.State == fleet.ApprovalExpired,
			ToolCall: &interaction.ToolCall{Kind: identifier},
		}},
	}
	persisted, err := r.recorder.Record(ctx, requested)
	if err != nil {
		return err
	}
	expected := requested
	expected.RecordedAt = persisted.RecordedAt
	expected.Actor = interaction.ActorContext{
		SubjectID: audit.Subject, ActorID: audit.Actor,
		ClientID:   audit.ClientID,
		OnBehalfOf: append([]shoal.ID(nil), audit.OnBehalfOf...),
	}
	expected.Reason = persisted.Reason
	expected, err = expected.Canonical()
	if err != nil {
		return explorer.MarkCommittedInteraction(err)
	}
	persisted, err = persisted.Canonical()
	if err != nil || !reflect.DeepEqual(persisted, expected) {
		return explorer.MarkCommittedInteraction(shoal.NewError(
			shoal.ErrorInternal,
			"fleet approval recorder returned a mismatched trusted session"))
	}
	return nil
}

func approvalSessionID(audit fleet.ApprovalAudit) shoal.ID {
	digest := sha256.New()
	writeActionField(digest, []byte("shoal.fleet.approval-audit.v1"))
	writeActionField(digest, []byte(audit.Phase))
	writeActionField(digest, audit.Record.ID)
	writeActionField(
		digest, []byte(strconv.FormatUint(audit.Record.Version, 10)))
	writeActionField(digest, []byte(audit.Subject))
	writeActionField(digest, []byte(audit.Actor))
	writeActionField(digest, []byte(audit.ClientID))
	writeActionField(digest, []byte(audit.RequestID))
	return interaction.DerivedID(
		"session", hex.EncodeToString(digest.Sum(nil)))
}

var _ fleet.ApprovalRecorder = (*ApprovalRecorder)(nil)
