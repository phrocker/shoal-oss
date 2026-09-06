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

type ActionInteractionRecorder interface {
	Record(context.Context, interaction.Session) (interaction.Session, error)
}

// ActionRecorder records exact action evidence through the result-returning
// interaction recorder and rejects any sink result that changes the effect.
type ActionRecorder struct {
	recorder ActionInteractionRecorder
}

func NewActionRecorder(
	recorder ActionInteractionRecorder,
) (*ActionRecorder, error) {
	if isNilActionInteractionRecorder(recorder) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet action interaction recorder is required")
	}
	return &ActionRecorder{recorder: recorder}, nil
}

func (r *ActionRecorder) RecordAction(
	ctx context.Context,
	audit fleet.ActionAudit,
) error {
	if audit.Phase == "" {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet action audit phase is required")
	}
	if err := audit.Operation.Validate(); err != nil {
		return err
	}
	if err := audit.Record.Validate(); err != nil {
		return err
	}
	evidence := make([]interaction.EvidenceReference, len(audit.Record.Evidence))
	retrievedNodeIDs := make([]shoal.ID, 0, len(audit.Record.Evidence))
	for index, item := range audit.Record.Evidence {
		evidence[index] = interaction.EvidenceReference{
			AnchorID: item.AnchorID, Kind: item.Kind, Citation: item.Citation,
			NodeIDs: append([]shoal.ID(nil), item.NodeIDs...),
			EdgeIDs: append([]shoal.ID(nil), item.EdgeIDs...),
			Assertions: append(
				[]interaction.AssertionReference(nil), item.Assertions...),
		}
		retrievedNodeIDs = append(retrievedNodeIDs, item.NodeIDs...)
	}
	requested := interaction.Session{
		ID: actionSessionID(audit), Operation: interaction.OperationToolCall,
		AuthorizationOperation: string(audit.Operation),
		QueryDigest: interaction.Digest(
			string(audit.Operation) + ":" + audit.Phase),
		RequestID:  audit.Record.RequestID,
		ResultID:   shoal.ID(hex.EncodeToString(audit.Record.ID)),
		StopReason: audit.Phase,
		Turns: []interaction.Turn{{
			Index:    0,
			Decision: actionIdentifier(audit),
			Failed: audit.EffectError != nil ||
				audit.Record.State == fleet.DispatchFailed,
			ToolCall: &interaction.ToolCall{
				Kind:              actionIdentifier(audit),
				RetrievedNodeIDs:  retrievedNodeIDs,
				RetrievedEvidence: evidence,
			},
		}},
	}
	if len(evidence) > 0 {
		requested.SnapshotID = audit.Record.EvidenceSnapshotID
		requested.SnapshotAsOf = audit.Record.EvidenceSnapshotAsOf
		requested.AuthorizationFingerprint = shoal.ID(
			audit.Record.ExecutionFingerprint.String())
		requested.AuthorizationExpiresAt = audit.Record.ExecutionExpiresAt
	}
	persisted, err := r.recorder.Record(ctx, requested)
	if err != nil {
		return err
	}
	expected := requested
	expected.RecordedAt = persisted.RecordedAt
	expected.Actor = interaction.ActorContext{
		SubjectID: audit.Record.Subject, ActorID: audit.Record.Actor,
		ClientID:   audit.Record.ClientID,
		OnBehalfOf: append([]shoal.ID(nil), audit.Record.OnBehalfOf...),
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
			"fleet action recorder returned a mismatched trusted session"))
	}
	return nil
}

func actionSessionID(audit fleet.ActionAudit) shoal.ID {
	digest := sha256.New()
	writeActionField(digest, []byte("shoal.fleet.action-audit.v2"))
	writeActionField(digest, []byte(audit.Phase))
	writeActionField(digest, audit.Record.ID)
	writeActionField(
		digest, []byte(strconv.FormatUint(audit.Record.Version, 10)))
	return interaction.DerivedID(
		"session", hex.EncodeToString(digest.Sum(nil)))
}

func actionIdentifier(audit fleet.ActionAudit) string {
	return "fleet." + string(audit.Operation) + "." + audit.Phase
}

func writeActionField(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

func isNilActionInteractionRecorder(recorder ActionInteractionRecorder) bool {
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

var _ fleet.ActionRecorder = (*ActionRecorder)(nil)
