// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
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
	DispatchTable       = "_shoal_explorer_fleet_dispatch"
	dispatchKind   byte = 'D'
	transitionKind byte = 'T'
)

var (
	dispatchFamily    = []byte("r")
	dispatchQualifier = []byte("action")
	dispatchPolicy    = []byte("fleet/dispatch/v1")
	dispatchMagic     = []byte{'S', 'F', 'D', 1}
	transitionMagic   = []byte{'S', 'F', 'T', 1}
)

type storedTransition struct {
	Transition  fleet.ActionTransition
	CompletedAt time.Time
}

type DispatchStore struct {
	runtime    DispatchRuntime
	visibility []byte
}

type DispatchRuntime interface {
	Runtime
	CurrentHead(context.Context) (coordination.AllocatorHeadV1, error)
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
	var transition fleet.ActionTransition
	var transitionValue []byte
	if mutation.TransitionKind != "" {
		transition, err = fleet.NewActionTransition(
			mutation.Token, mutation.TransitionKind, canonical)
		if err != nil {
			return fleet.ActionRecord{}, err
		}
		transitionValue, err = encodeTransition(storedTransition{
			Transition: transition,
		})
		if err != nil {
			return fleet.ActionRecord{}, err
		}
	}
	if current, readErr := s.GetAction(ctx, mutation.Record.ID); readErr == nil &&
		reflect.DeepEqual(current, canonical) {
		if mutation.TransitionKind != "" {
			if err := s.ensureTransition(ctx, transition, transitionValue); err != nil {
				return fleet.ActionRecord{}, err
			}
		}
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
	cells := []explorercoord.Cell{{
		Table: DispatchTable, Row: dispatchRow(canonical.ID),
		Family: dispatchFamily, Qualifier: dispatchQualifier,
		Visibility: s.visibility, Value: value, EpochTimestamp: true,
		LPART: lpart, CopyGeneration: 1,
	}}
	guards := []explorercoord.GuardIntent{actionGuard}
	results := []explorercoord.ResultIdentity{{
		Kind: []byte("fleet-action"), ID: append([]byte(nil), canonical.ID...),
	}}
	if mutation.TransitionKind != "" {
		transitionGuard := explorercoord.GuardIntent{
			Entity: transitionEntity(transition.ID), Mode: guard.ModeAbsentOrIdentical,
			DesiredState:    guard.StateLive,
			DesiredWinnerID: transitionWinner(transitionValue), LPART: lpart,
			LogicalPolicyID: dispatchPolicy, RetirementGeneration: 1,
		}
		cells = append(cells, explorercoord.Cell{
			Table: DispatchTable,
			Row: transitionRow(
				transition.Record.ID, transition.Record.Version, transition.ID),
			Family: dispatchFamily, Qualifier: dispatchQualifier,
			Visibility: s.visibility, Value: transitionValue, EpochTimestamp: true,
			LPART: lpart, CopyGeneration: 1,
		})
		guards = append(guards, transitionGuard)
		results = append(results, explorercoord.ResultIdentity{
			Kind: []byte("fleet-action-transition"),
			ID:   append([]byte(nil), transition.ID...),
		})
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
	guards[0] = actionGuard
	intent := explorercoord.Intent{
		Operation: []byte("fleet.dispatch.apply.v1"),
		Token:     append([]byte(nil), mutation.Token...),
		Cells:     cells, Guards: guards, Results: results,
	}
	_, publishErr := s.runtime.Publish(ctx, explorercoord.Request{Intent: intent})
	if publishErr != nil {
		resolve := context.WithoutCancel(ctx)
		for attempt := 0; attempt < 10; attempt++ {
			current, readErr := s.GetAction(resolve, canonical.ID)
			if readErr == nil {
				if reflect.DeepEqual(current, canonical) {
					if mutation.TransitionKind != "" {
						if ensureErr := s.ensureTransition(
							resolve, transition, transitionValue,
						); ensureErr != nil {
							return fleet.ActionRecord{}, ensureErr
						}
					}
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

func (s *DispatchStore) PendingActionTransitions(
	ctx context.Context, actionID, after []byte, limit int,
) (fleet.ActionTransitionPage, error) {
	if err := dispatchID(actionID); err != nil {
		return fleet.ActionTransitionPage{}, err
	}
	if len(after) > 0 {
		if err := dispatchID(after); err != nil {
			return fleet.ActionTransitionPage{}, err
		}
	}
	if limit <= 0 || limit > fleet.MaxDispatchListResults {
		return fleet.ActionTransitionPage{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"transition scan limit is outside its bound")
	}
	head, err := s.runtime.CurrentHead(ctx)
	if err != nil {
		return fleet.ActionTransitionPage{}, publicError(err)
	}
	var startAfter []byte
	if len(after) > 0 {
		startAfter = append(transitionPrefix(actionID), after...)
	}
	prefix := transitionPrefix(actionID)
	page, err := s.runtime.ScanCommitted(ctx, explorercoord.CommittedScanRequest{
		Table: DispatchTable, RowPrefix: prefix,
		StartAfterRow: startAfter, Family: dispatchFamily,
		Qualifier: dispatchQualifier, Visibility: s.visibility,
		Frontier: head.Frontier, Limit: limit,
		MaxScanned: explorercoord.MaxCommittedScanCells,
	})
	if err != nil {
		return fleet.ActionTransitionPage{}, publicError(err)
	}
	result := fleet.ActionTransitionPage{
		Transitions: make([]fleet.ActionTransition, 0, len(page.Cells)),
	}
	for _, cell := range page.Cells {
		stored, decodeErr := decodeTransition(cell.Cell.Value)
		if decodeErr != nil {
			return fleet.ActionTransitionPage{}, shoal.WrapError(
				shoal.ErrorInternal, "invalid committed fleet transition", decodeErr)
		}
		if stored.CompletedAt.IsZero() {
			result.Transitions = append(
				result.Transitions, cloneTransition(stored.Transition))
		}
	}
	if len(page.NextRow) > len(prefix) &&
		bytes.HasPrefix(page.NextRow, prefix) {
		result.Next = append([]byte(nil), page.NextRow[len(prefix):]...)
	}
	return result, nil
}

func (s *DispatchStore) CompleteActionTransition(
	ctx context.Context, transition fleet.ActionTransition,
) error {
	if err := transition.Validate(); err != nil {
		return err
	}
	current, head, err := s.readTransition(
		ctx, transition.Record.ID, transition.Record.Version, transition.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current.Transition, transition) {
		return fleet.ErrActionConflict
	}
	if !current.CompletedAt.IsZero() {
		return nil
	}
	current.CompletedAt = transition.Record.UpdatedAt
	value, err := encodeTransition(current)
	if err != nil {
		return err
	}
	lpart, err := explorercoord.Partition(
		coordination.DomainID("fleet-dispatch"), transition.Record.ID)
	if err != nil {
		return publicError(err)
	}
	_, err = s.runtime.Publish(ctx, explorercoord.Request{Intent: explorercoord.Intent{
		Operation: []byte("fleet.dispatch.complete-transition.v1"),
		Token:     transitionCompletionToken(transition.ID),
		Cells: []explorercoord.Cell{{
			Table: DispatchTable,
			Row: transitionRow(
				transition.Record.ID, transition.Record.Version, transition.ID),
			Family: dispatchFamily, Qualifier: dispatchQualifier,
			Visibility: s.visibility, Value: value, EpochTimestamp: true,
			LPART: lpart, CopyGeneration: 1,
		}},
		Guards: []explorercoord.GuardIntent{{
			Entity: transitionEntity(transition.ID), Mode: guard.ModeMutate,
			ExpectedEpoch: head.Epoch, ExpectedDigest: head.LogicalDigest,
			DesiredState:    guard.StateLive,
			DesiredWinnerID: transitionWinner(value), LPART: lpart,
			LogicalPolicyID: dispatchPolicy, RetirementGeneration: 1,
		}},
		Results: []explorercoord.ResultIdentity{{
			Kind: []byte("fleet-action-transition-complete"),
			ID:   append([]byte(nil), transition.ID...),
		}},
	}})
	if err != nil {
		resolve := context.WithoutCancel(ctx)
		replayed, _, readErr := s.readTransition(
			resolve, transition.Record.ID, transition.Record.Version, transition.ID)
		if readErr == nil && !replayed.CompletedAt.IsZero() &&
			reflect.DeepEqual(replayed.Transition, transition) {
			return nil
		}
		return publicError(err)
	}
	return nil
}

func (s *DispatchStore) ensureTransition(
	ctx context.Context, transition fleet.ActionTransition, value []byte,
) error {
	current, _, err := s.readTransition(
		ctx, transition.Record.ID, transition.Record.Version, transition.ID)
	if err == nil {
		if reflect.DeepEqual(current.Transition, transition) {
			return nil
		}
		return fleet.ErrActionConflict
	}
	if !errors.Is(err, fleet.ErrActionNotFound) {
		return err
	}
	lpart, err := explorercoord.Partition(
		coordination.DomainID("fleet-dispatch"), transition.Record.ID)
	if err != nil {
		return publicError(err)
	}
	_, err = s.runtime.Publish(ctx, explorercoord.Request{Intent: explorercoord.Intent{
		Operation: []byte("fleet.dispatch.repair-transition.v1"),
		Token:     transitionRepairToken(transition.ID),
		Cells: []explorercoord.Cell{{
			Table: DispatchTable,
			Row: transitionRow(
				transition.Record.ID, transition.Record.Version, transition.ID),
			Family: dispatchFamily, Qualifier: dispatchQualifier,
			Visibility: s.visibility, Value: value, EpochTimestamp: true,
			LPART: lpart, CopyGeneration: 1,
		}},
		Guards: []explorercoord.GuardIntent{{
			Entity: transitionEntity(transition.ID), Mode: guard.ModeAbsentOrIdentical,
			DesiredState:    guard.StateLive,
			DesiredWinnerID: transitionWinner(value), LPART: lpart,
			LogicalPolicyID: dispatchPolicy, RetirementGeneration: 1,
		}},
		Results: []explorercoord.ResultIdentity{{
			Kind: []byte("fleet-action-transition"),
			ID:   append([]byte(nil), transition.ID...),
		}},
	}})
	return publicError(err)
}

func (s *DispatchStore) readTransition(
	ctx context.Context, actionID []byte, version uint64, id []byte,
) (storedTransition, *guard.Head, error) {
	if err := dispatchID(actionID); err != nil {
		return storedTransition{}, nil, err
	}
	if err := dispatchID(id); err != nil {
		return storedTransition{}, nil, err
	}
	head, _, err := s.runtime.ReadEntity(ctx, transitionEntity(id))
	if err != nil {
		if errors.Is(err, guard.ErrNotFound) ||
			errors.Is(err, transaction.ErrNotFound) {
			return storedTransition{}, nil, fleet.ErrActionNotFound
		}
		return storedTransition{}, nil, publicError(err)
	}
	if head == nil {
		return storedTransition{}, nil, fleet.ErrActionNotFound
	}
	cell, ok, err := s.runtime.ReadCommittedCell(
		ctx, DispatchTable,
		transitionRow(actionID, version, id),
		dispatchFamily,
		dispatchQualifier, s.visibility, head.Epoch)
	if err != nil {
		return storedTransition{}, nil, publicError(err)
	}
	if !ok {
		return storedTransition{}, nil, fleet.ErrActionNotFound
	}
	stored, err := decodeTransition(cell.Cell.Value)
	if err != nil {
		return storedTransition{}, nil, shoal.WrapError(
			shoal.ErrorInternal, "invalid committed fleet transition", err)
	}
	return stored, head, nil
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

func transitionPrefix(actionID []byte) []byte {
	result := append([]byte("transition/"), []byte(hex.EncodeToString(actionID))...)
	return append(result, '/')
}

func transitionRow(actionID []byte, version uint64, id []byte) []byte {
	row := transitionPrefix(actionID)
	var encodedVersion [8]byte
	binary.BigEndian.PutUint64(encodedVersion[:], version)
	row = append(row, encodedVersion[:]...)
	return append(row, []byte(hex.EncodeToString(id))...)
}

func transitionEntity(id []byte) guard.Entity {
	return guard.Entity{
		Kind: transitionKind,
		ID:   coordination.EntityID(hex.EncodeToString(id)),
	}
}

func transitionWinner(value []byte) []byte {
	digest := sha256.Sum256(value)
	return digest[:]
}

func transitionCompletionToken(id []byte) []byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte("fleet.dispatch.transition-complete.v1"))
	_, _ = digest.Write(id)
	return digest.Sum(nil)
}

func transitionRepairToken(id []byte) []byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte("fleet.dispatch.transition-repair.v1"))
	_, _ = digest.Write(id)
	return digest.Sum(nil)
}

func cloneTransition(value fleet.ActionTransition) fleet.ActionTransition {
	encoded, err := encodeTransition(storedTransition{Transition: value})
	if err != nil {
		return value
	}
	decoded, err := decodeTransition(encoded)
	if err != nil {
		return value
	}
	return decoded.Transition
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

func encodeTransition(value storedTransition) ([]byte, error) {
	if err := value.Transition.Validate(); err != nil {
		return nil, err
	}
	if !value.CompletedAt.IsZero() &&
		value.CompletedAt.Location() != time.UTC {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet transition completion time must be UTC")
	}
	var buffer bytes.Buffer
	buffer.Write(transitionMagic)
	if err := gob.NewEncoder(&buffer).Encode(value); err != nil {
		return nil, shoal.WrapError(
			shoal.ErrorInternal, "encode fleet action transition", err)
	}
	if buffer.Len() > 3*fleet.MaxActionPayloadBytes {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet action transition exceeds its bound")
	}
	return buffer.Bytes(), nil
}

func decodeTransition(value []byte) (storedTransition, error) {
	if len(value) < len(transitionMagic) ||
		!bytes.Equal(value[:len(transitionMagic)], transitionMagic) {
		return storedTransition{}, errors.New(
			"unknown fleet action transition encoding")
	}
	var stored storedTransition
	reader := bytes.NewReader(value[len(transitionMagic):])
	if err := gob.NewDecoder(reader).Decode(&stored); err != nil {
		return storedTransition{}, err
	}
	if reader.Len() != 0 {
		return storedTransition{}, errors.New(
			"trailing fleet action transition bytes")
	}
	if err := stored.Transition.Validate(); err != nil {
		return storedTransition{}, err
	}
	if !stored.CompletedAt.IsZero() &&
		stored.CompletedAt.Location() != time.UTC {
		return storedTransition{}, errors.New(
			"fleet action transition completion time is not UTC")
	}
	return stored, nil
}
