// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"reflect"
	"time"

	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/guard"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/transaction"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Approval records live in the dispatch table under their own row prefix and
// guard kind, so no action scan, transition scan or action read can return
// one. That separation is what keeps a held request from ever being work: the
// dispatch surfaces read "action/" rows and nothing else.
const approvalKind byte = 'P'

var (
	approvalRowPrefix = []byte("approval/")
	approvalPolicy    = []byte("fleet/approval/v1")
	approvalMagic     = []byte{'S', 'F', 'P', 1}
)

// ApprovalStore is the durable approval key space over the shared embedded
// runtime.
type ApprovalStore struct {
	runtime    DispatchRuntime
	visibility []byte
}

func NewApprovalStore(
	runtime DispatchRuntime, visibility []byte,
) (*ApprovalStore, error) {
	if runtime == nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet approval runtime is required")
	}
	if len(visibility) > coordination.MaxCoordinateBytes {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet approval visibility exceeds its bound")
	}
	return &ApprovalStore{
		runtime: runtime, visibility: append([]byte(nil), visibility...),
	}, nil
}

func (s *ApprovalStore) GetApproval(
	ctx context.Context, id []byte,
) (fleet.ApprovalRecord, error) {
	record, _, err := s.readApproval(ctx, id)
	return record, err
}

func (s *ApprovalStore) readApproval(
	ctx context.Context, id []byte,
) (fleet.ApprovalRecord, *guard.Head, error) {
	if err := dispatchID(id); err != nil {
		return fleet.ApprovalRecord{}, nil, err
	}
	head, _, err := s.runtime.ReadEntity(ctx, approvalEntity(id))
	if err != nil {
		if errors.Is(err, guard.ErrNotFound) ||
			errors.Is(err, transaction.ErrNotFound) {
			return fleet.ApprovalRecord{}, nil, fleet.ErrApprovalNotFound
		}
		return fleet.ApprovalRecord{}, nil, publicError(err)
	}
	if head == nil {
		return fleet.ApprovalRecord{}, nil, fleet.ErrApprovalNotFound
	}
	cell, ok, err := s.runtime.ReadCommittedCell(
		ctx, DispatchTable, approvalRow(id), dispatchFamily,
		dispatchQualifier, s.visibility, head.Epoch)
	if err != nil {
		return fleet.ApprovalRecord{}, nil, publicError(err)
	}
	if !ok {
		return fleet.ApprovalRecord{}, nil, fleet.ErrApprovalNotFound
	}
	record, err := decodeApproval(cell.Cell.Value)
	if err != nil {
		return fleet.ApprovalRecord{}, nil, shoal.WrapError(
			shoal.ErrorInternal, "invalid committed fleet approval", err)
	}
	return record, head, nil
}

// ApplyApproval is one compare-and-set. An identical retry of a write already
// applied returns the stored record; anything else at a version other than the
// expected one is ErrApprovalConflict.
func (s *ApprovalStore) ApplyApproval(
	ctx context.Context, mutation fleet.ApprovalMutation,
) (fleet.ApprovalRecord, error) {
	if err := dispatchID(mutation.Record.ID); err != nil {
		return fleet.ApprovalRecord{}, err
	}
	if err := dispatchID(mutation.Token); err != nil {
		return fleet.ApprovalRecord{}, err
	}
	if mutation.Record.Version != mutation.ExpectedVersion+1 {
		return fleet.ApprovalRecord{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet approval version transition is invalid")
	}
	value, err := encodeApproval(mutation.Record)
	if err != nil {
		return fleet.ApprovalRecord{}, err
	}
	canonical, err := decodeApproval(value)
	if err != nil {
		return fleet.ApprovalRecord{}, shoal.WrapError(
			shoal.ErrorInternal, "canonicalize fleet approval", err)
	}
	lpart, err := explorercoord.Partition(
		coordination.DomainID("fleet-approval"), canonical.ID)
	if err != nil {
		return fleet.ApprovalRecord{}, publicError(err)
	}
	intentGuard := explorercoord.GuardIntent{
		Entity: approvalEntity(canonical.ID), DesiredState: guard.StateLive,
		// The winner is the content, not the identity. A first write is
		// guarded absent-or-identical, and with the identity as winner two
		// different first writes at one ID would both read as identical.
		DesiredWinnerID: transitionWinner(value), LPART: lpart,
		LogicalPolicyID: approvalPolicy, RetirementGeneration: 1,
	}
	current, head, readErr := s.readApproval(ctx, canonical.ID)
	switch {
	case readErr == nil && reflect.DeepEqual(current, canonical):
		return current, nil
	case readErr == nil && current.Version != mutation.ExpectedVersion:
		return fleet.ApprovalRecord{}, fleet.ErrApprovalConflict
	case readErr == nil:
		intentGuard.Mode = guard.ModeMutate
		intentGuard.ExpectedEpoch = head.Epoch
		intentGuard.ExpectedDigest = head.LogicalDigest
	case errors.Is(readErr, fleet.ErrApprovalNotFound):
		if mutation.ExpectedVersion != 0 {
			return fleet.ApprovalRecord{}, fleet.ErrApprovalConflict
		}
		intentGuard.Mode = guard.ModeAbsentOrIdentical
	default:
		return fleet.ApprovalRecord{}, readErr
	}
	_, publishErr := s.runtime.Publish(ctx, explorercoord.Request{
		Intent: explorercoord.Intent{
			Operation: []byte("fleet.approval.apply.v1"),
			Token:     append([]byte(nil), mutation.Token...),
			Cells: []explorercoord.Cell{{
				Table: DispatchTable, Row: approvalRow(canonical.ID),
				Family: dispatchFamily, Qualifier: dispatchQualifier,
				Visibility: s.visibility, Value: value, EpochTimestamp: true,
				LPART: lpart, CopyGeneration: 1,
			}},
			Guards: []explorercoord.GuardIntent{intentGuard},
			Results: []explorercoord.ResultIdentity{{
				Kind: []byte("fleet-approval"),
				ID:   append([]byte(nil), canonical.ID...),
			}},
		},
	})
	if publishErr != nil {
		// The write may have landed. Whether it did is answered by what is
		// stored, never by the error: this record is ours if and only if it is
		// byte-for-byte what we wrote.
		resolve := context.WithoutCancel(ctx)
		for attempt := 0; attempt < 10; attempt++ {
			stored, getErr := s.GetApproval(resolve, canonical.ID)
			if getErr == nil {
				if reflect.DeepEqual(stored, canonical) {
					return stored, nil
				}
				if stored.Version >= canonical.Version {
					return fleet.ApprovalRecord{}, fleet.ErrApprovalConflict
				}
			}
			time.Sleep(time.Millisecond)
		}
		if errors.Is(publishErr, explorercoord.ErrIndeterminatePublication) {
			return fleet.ApprovalRecord{}, shoal.WrapError(
				shoal.ErrorUnavailable,
				"fleet approval publication is indeterminate", publishErr)
		}
		return fleet.ApprovalRecord{}, publicError(publishErr)
	}
	stored, err := s.GetApproval(ctx, canonical.ID)
	if err != nil {
		return fleet.ApprovalRecord{}, err
	}
	if !reflect.DeepEqual(stored, canonical) {
		return fleet.ApprovalRecord{}, fleet.ErrApprovalConflict
	}
	return stored, nil
}

func (s *ApprovalStore) ScanApprovals(
	ctx context.Context, after []byte, limit int,
) (fleet.ApprovalPage, error) {
	if len(after) > 0 {
		if err := dispatchID(after); err != nil {
			return fleet.ApprovalPage{}, err
		}
	}
	if limit <= 0 || limit > fleet.MaxApprovalListResults {
		return fleet.ApprovalPage{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "approval scan limit is outside its bound")
	}
	head, err := s.runtime.CurrentHead(ctx)
	if err != nil {
		return fleet.ApprovalPage{}, publicError(err)
	}
	var startAfter []byte
	if len(after) > 0 {
		startAfter = approvalRow(after)
	}
	page, err := s.runtime.ScanCommitted(ctx, explorercoord.CommittedScanRequest{
		Table: DispatchTable, RowPrefix: approvalRowPrefix,
		StartAfterRow: startAfter, Family: dispatchFamily,
		Qualifier: dispatchQualifier, Visibility: s.visibility,
		Frontier: head.Frontier, Limit: limit,
		MaxScanned: explorercoord.MaxCommittedScanCells,
	})
	if err != nil {
		return fleet.ApprovalPage{}, publicError(err)
	}
	result := fleet.ApprovalPage{
		Approvals: make([]fleet.ApprovalRecord, 0, len(page.Cells)),
	}
	for _, cell := range page.Cells {
		record, decodeErr := decodeApproval(cell.Cell.Value)
		if decodeErr != nil {
			return fleet.ApprovalPage{}, shoal.WrapError(
				shoal.ErrorInternal, "invalid committed fleet approval", decodeErr)
		}
		result.Approvals = append(result.Approvals, record)
	}
	if len(page.NextRow) > len(approvalRowPrefix) &&
		bytes.HasPrefix(page.NextRow, approvalRowPrefix) {
		result.Next = append(
			[]byte(nil), page.NextRow[len(approvalRowPrefix):]...)
	}
	return result, nil
}

func approvalEntity(id []byte) guard.Entity {
	return guard.Entity{
		Kind: approvalKind, ID: coordination.EntityID(append([]byte(nil), id...)),
	}
}

func approvalRow(id []byte) []byte {
	return append(append([]byte(nil), approvalRowPrefix...), id...)
}

func encodeApproval(record fleet.ApprovalRecord) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	buffer.Write(approvalMagic)
	if err := gob.NewEncoder(&buffer).Encode(record); err != nil {
		return nil, shoal.WrapError(
			shoal.ErrorInternal, "encode fleet approval", err)
	}
	if buffer.Len() > 3*fleet.MaxActionPayloadBytes {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet approval record exceeds its bound")
	}
	return buffer.Bytes(), nil
}

func decodeApproval(value []byte) (fleet.ApprovalRecord, error) {
	if len(value) < len(approvalMagic) ||
		!bytes.Equal(value[:len(approvalMagic)], approvalMagic) {
		return fleet.ApprovalRecord{}, errors.New("unknown fleet approval encoding")
	}
	var record fleet.ApprovalRecord
	reader := bytes.NewReader(value[len(approvalMagic):])
	if err := gob.NewDecoder(reader).Decode(&record); err != nil {
		return fleet.ApprovalRecord{}, err
	}
	if reader.Len() != 0 {
		return fleet.ApprovalRecord{}, errors.New("trailing fleet approval bytes")
	}
	if err := record.Validate(); err != nil {
		return fleet.ApprovalRecord{}, err
	}
	return record, nil
}

var _ fleet.ApprovalStore = (*ApprovalStore)(nil)
