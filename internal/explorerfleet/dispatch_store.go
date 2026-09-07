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

const (
	DispatchTable      = "_shoal_explorer_fleet_dispatch"
	dispatchKind  byte = 'D'
)

var (
	dispatchFamily    = []byte("r")
	dispatchQualifier = []byte("action")
	dispatchPolicy    = []byte("fleet/dispatch/v1")
	dispatchMagic     = []byte{'S', 'F', 'D', 1}
)

type DispatchStore struct {
	runtime    DispatchRuntime
	visibility []byte
}

type DispatchRuntime interface {
	Runtime
	CurrentHead(context.Context) (coordination.AllocatorHeadV1, error)
	ScanCommitted(
		context.Context,
		explorercoord.CommittedScanRequest,
	) (explorercoord.CommittedPage, error)
	ReadCommittedCell(
		context.Context,
		string,
		[]byte,
		[]byte,
		[]byte,
		[]byte,
		coordination.Epoch,
	) (explorercoord.CommittedCell, bool, error)
}

func NewDispatchStore(runtime DispatchRuntime, visibility []byte) (*DispatchStore, error) {
	if runtime == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet dispatch runtime is required")
	}
	if len(visibility) > coordination.MaxCoordinateBytes {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet dispatch visibility exceeds its bound")
	}
	return &DispatchStore{runtime: runtime, visibility: append([]byte(nil), visibility...)}, nil
}

func (s *DispatchStore) ScanActions(ctx context.Context, after []byte, limit int) (fleet.ActionPage, error) {
	if len(after) > 0 {
		if err := dispatchID(after); err != nil {
			return fleet.ActionPage{}, err
		}
	}
	if limit <= 0 || limit > fleet.MaxDispatchListResults {
		return fleet.ActionPage{}, shoal.NewError(shoal.ErrorInvalidArgument, "dispatch scan limit is outside its bound")
	}
	head, err := s.runtime.CurrentHead(ctx)
	if err != nil {
		return fleet.ActionPage{}, publicError(err)
	}
	var startAfter []byte
	if len(after) > 0 {
		startAfter = dispatchRow(after)
	}
	page, err := s.runtime.ScanCommitted(ctx, explorercoord.CommittedScanRequest{
		Table: DispatchTable, RowPrefix: []byte("action/"), StartAfterRow: startAfter,
		Family: dispatchFamily, Qualifier: dispatchQualifier, Visibility: s.visibility,
		Frontier: head.Frontier, Limit: limit,
		MaxScanned: explorercoord.MaxCommittedScanCells,
	})
	if err != nil {
		return fleet.ActionPage{}, publicError(err)
	}
	result := fleet.ActionPage{Actions: make([]fleet.ActionRecord, 0, len(page.Cells))}
	for _, cell := range page.Cells {
		record, decodeErr := decodeAction(cell.Cell.Value)
		if decodeErr != nil {
			return fleet.ActionPage{}, shoal.WrapError(shoal.ErrorInternal, "invalid committed fleet action", decodeErr)
		}
		result.Actions = append(result.Actions, record)
	}
	if len(page.NextRow) > len("action/") && bytes.HasPrefix(page.NextRow, []byte("action/")) {
		result.Next = append([]byte(nil), page.NextRow[len("action/"):]...)
	}
	return result, nil
}

func DispatchPhysicalTable() string { return DispatchTable }

func (s *DispatchStore) GetAction(ctx context.Context, id []byte) (fleet.ActionRecord, error) {
	record, _, err := s.readAction(ctx, id)
	return record, err
}

func (s *DispatchStore) readAction(
	ctx context.Context,
	id []byte,
) (fleet.ActionRecord, *guard.Head, error) {
	if err := dispatchID(id); err != nil {
		return fleet.ActionRecord{}, nil, err
	}
	head, _, err := s.runtime.ReadEntity(ctx, dispatchEntity(id))
	if err != nil {
		if errors.Is(err, guard.ErrNotFound) || errors.Is(err, transaction.ErrNotFound) {
			return fleet.ActionRecord{}, nil, fleet.ErrActionNotFound
		}
		return fleet.ActionRecord{}, nil, publicError(err)
	}
	if head == nil {
		return fleet.ActionRecord{}, nil, fleet.ErrActionNotFound
	}
	value, err := s.readCommittedAction(ctx, dispatchRow(id), head.Epoch)
	if err != nil {
		return fleet.ActionRecord{}, nil, publicError(err)
	}
	record, err := decodeAction(value)
	if err != nil {
		return fleet.ActionRecord{}, nil, shoal.WrapError(shoal.ErrorInternal, "invalid committed fleet action", err)
	}
	return record, head, nil
}

func (s *DispatchStore) readCommittedAction(
	ctx context.Context,
	row []byte,
	epoch coordination.Epoch,
) ([]byte, error) {
	cell, ok, err := s.runtime.ReadCommittedCell(
		ctx,
		DispatchTable,
		row,
		dispatchFamily,
		dispatchQualifier,
		s.visibility,
		epoch,
	)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, transaction.ErrNotFound
	}
	return append([]byte(nil), cell.Cell.Value...), nil
}

func (s *DispatchStore) ApplyAction(ctx context.Context, mutation fleet.DispatchMutation) (fleet.ActionRecord, error) {
	if err := dispatchID(mutation.Record.ID); err != nil {
		return fleet.ActionRecord{}, err
	}
	if err := dispatchID(mutation.Token); err != nil {
		return fleet.ActionRecord{}, err
	}
	if mutation.Record.Version != mutation.ExpectedVersion+1 {
		return fleet.ActionRecord{}, shoal.NewError(shoal.ErrorInvalidArgument, "fleet action version transition is invalid")
	}
	value, err := encodeAction(mutation.Record)
	if err != nil {
		return fleet.ActionRecord{}, err
	}
	canonical, err := decodeAction(value)
	if err != nil {
		return fleet.ActionRecord{}, shoal.WrapError(
			shoal.ErrorInternal, "canonicalize fleet action", err)
	}
	if current, readErr := s.GetAction(ctx, mutation.Record.ID); readErr == nil &&
		reflect.DeepEqual(current, canonical) {
		return current, nil
	}
	lpart, err := explorercoord.Partition(
		coordination.DomainID("fleet-dispatch"), canonical.ID)
	if err != nil {
		return fleet.ActionRecord{}, publicError(err)
	}
	actionGuard := explorercoord.GuardIntent{
		Entity: dispatchEntity(canonical.ID), DesiredState: guard.StateLive,
		DesiredWinnerID: append([]byte(nil), canonical.ID...), LPART: lpart,
		LogicalPolicyID: dispatchPolicy, RetirementGeneration: 1,
	}
	if mutation.ExpectedVersion == 0 {
		current, readErr := s.GetAction(ctx, canonical.ID)
		if readErr == nil {
			if bytes.Equal(current.IdempotencyKey, canonical.IdempotencyKey) &&
				reflect.DeepEqual(current, canonical) {
				return current, nil
			}
			return fleet.ActionRecord{}, fleet.ErrActionConflict
		}
		if !errors.Is(readErr, fleet.ErrActionNotFound) {
			return fleet.ActionRecord{}, readErr
		}
		actionGuard.Mode = guard.ModeAbsentOrIdentical
	} else {
		current, head, readErr := s.readAction(ctx, canonical.ID)
		if readErr != nil {
			return fleet.ActionRecord{}, readErr
		}
		if current.Version != mutation.ExpectedVersion ||
			current.ClaimFence != mutation.ExpectedFence {
			return fleet.ActionRecord{}, fleet.ErrActionConflict
		}
		actionGuard.Mode = guard.ModeMutate
		actionGuard.ExpectedEpoch = head.Epoch
		actionGuard.ExpectedDigest = head.LogicalDigest
	}
	intent := explorercoord.Intent{
		Operation: []byte("fleet.dispatch.apply.v1"),
		Token:     append([]byte(nil), mutation.Token...),
		Cells: []explorercoord.Cell{{
			Table: DispatchTable, Row: dispatchRow(canonical.ID),
			Family: dispatchFamily, Qualifier: dispatchQualifier,
			Visibility: s.visibility, Value: value, EpochTimestamp: true,
			LPART: lpart, CopyGeneration: 1,
		}},
		Guards: []explorercoord.GuardIntent{actionGuard},
		Results: []explorercoord.ResultIdentity{{
			Kind: []byte("fleet-action"), ID: append([]byte(nil), canonical.ID...),
		}},
	}
	_, publishErr := s.runtime.Publish(ctx, explorercoord.Request{Intent: intent})
	if publishErr != nil {
		resolve := context.WithoutCancel(ctx)
		for attempt := 0; attempt < 10; attempt++ {
			current, readErr := s.GetAction(resolve, canonical.ID)
			if readErr == nil {
				if reflect.DeepEqual(current, canonical) {
					return current, nil
				}
				if current.Version >= canonical.Version {
					return fleet.ActionRecord{}, fleet.ErrActionConflict
				}
			}
			time.Sleep(time.Millisecond)
		}
		if errors.Is(publishErr, explorercoord.ErrIndeterminatePublication) {
			return fleet.ActionRecord{}, shoal.WrapError(shoal.ErrorUnavailable, "fleet action publication is indeterminate", publishErr)
		}
		return fleet.ActionRecord{}, publicError(publishErr)
	}
	stored, err := s.GetAction(ctx, canonical.ID)
	if err != nil {
		return fleet.ActionRecord{}, err
	}
	if !reflect.DeepEqual(stored, canonical) {
		return fleet.ActionRecord{}, fleet.ErrActionConflict
	}
	return stored, nil
}

func dispatchID(value []byte) error {
	if len(value) == 0 || len(value) > fleet.MaxActionIDBytes {
		return shoal.NewError(shoal.ErrorInvalidArgument, "fleet action identity is outside its byte bound")
	}
	return nil
}

func dispatchEntity(id []byte) guard.Entity {
	return guard.Entity{Kind: dispatchKind, ID: coordination.EntityID(append([]byte(nil), id...))}
}

func dispatchRow(id []byte) []byte {
	return append([]byte("action/"), id...)
}

func encodeAction(record fleet.ActionRecord) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	var buffer bytes.Buffer
	buffer.Write(dispatchMagic)
	if err := gob.NewEncoder(&buffer).Encode(record); err != nil {
		return nil, shoal.WrapError(shoal.ErrorInternal, "encode fleet action", err)
	}
	if buffer.Len() > 3*fleet.MaxActionPayloadBytes {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet action record exceeds its bound")
	}
	return buffer.Bytes(), nil
}

func decodeAction(value []byte) (fleet.ActionRecord, error) {
	if len(value) < len(dispatchMagic) || !bytes.Equal(value[:len(dispatchMagic)], dispatchMagic) {
		return fleet.ActionRecord{}, errors.New("unknown fleet action encoding")
	}
	var record fleet.ActionRecord
	reader := bytes.NewReader(value[len(dispatchMagic):])
	if err := gob.NewDecoder(reader).Decode(&record); err != nil {
		return fleet.ActionRecord{}, err
	}
	if reader.Len() != 0 {
		return fleet.ActionRecord{}, errors.New("trailing fleet action bytes")
	}
	if err := record.Validate(); err != nil {
		return fleet.ActionRecord{}, err
	}
	return record, nil
}
