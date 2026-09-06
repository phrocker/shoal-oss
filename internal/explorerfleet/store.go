// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package explorerfleet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
	Table               = "_shoal_explorer_fleet"
	agentKind    byte   = 'A'
	codecVersion uint16 = 1
)

var (
	recordFamily    = []byte("r")
	recordQualifier = []byte("descriptor")
	logicalPolicy   = []byte("fleet/registry/v1")
)

type Runtime interface {
	Publish(context.Context, explorercoord.Request) (explorercoord.Result, error)
	ReadEntity(context.Context, guard.Entity) (*guard.Head, *guard.Pending, error)
	ScanCommitted(context.Context, explorercoord.CommittedScanRequest) (explorercoord.CommittedPage, error)
	CurrentHead(context.Context) (coordination.AllocatorHeadV1, error)
}

type Store struct {
	runtime    Runtime
	visibility []byte
}

func NewStore(runtime Runtime, visibility []byte) (*Store, error) {
	if isNilRuntime(runtime) {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet runtime is required")
	}
	if len(visibility) > coordination.MaxCoordinateBytes {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet visibility exceeds its bound")
	}
	return &Store{runtime: runtime, visibility: append([]byte(nil), visibility...)}, nil
}

// PhysicalTable is included in explorercoord.Config.PhysicalTables by the
// production composition owner before opening the runtime.
func PhysicalTable() string { return Table }

func (s *Store) Get(ctx context.Context, id shoal.ID) (fleet.Stored, error) {
	if err := shoal.ValidateRequiredID("agent ID", id); err != nil {
		return fleet.Stored{}, err
	}
	head, _, err := s.runtime.ReadEntity(ctx, agentEntity(id))
	if err != nil {
		return fleet.Stored{}, publicError(err)
	}
	if head == nil {
		return fleet.Stored{}, shoal.NewError(shoal.ErrorNotFound, "agent not found")
	}
	value, err := s.readCommittedCell(ctx, agentRow(id), recordQualifier, head.Epoch)
	if err != nil {
		if errors.Is(err, transaction.ErrNotFound) {
			return fleet.Stored{}, shoal.WrapError(
				shoal.ErrorInternal,
				"committed agent head has no matching durable value",
				err,
			)
		}
		return fleet.Stored{}, publicError(err)
	}
	descriptor, registrationDigest, err := decodeDescriptor(value)
	if err != nil {
		return fleet.Stored{}, shoal.WrapError(shoal.ErrorInternal, "invalid committed fleet descriptor", err)
	}
	return fleet.Stored{
		Descriptor: descriptor, RegistrationDigest: registrationDigest,
		Digest: head.LogicalDigest, Epoch: int64(head.Epoch),
	}, nil
}

func (s *Store) List(
	ctx context.Context, cursor []byte, limit int,
) (fleet.StoredPage, error) {
	if limit <= 0 || limit > fleet.MaxListResults {
		return fleet.StoredPage{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet list limit is outside its bound")
	}
	if len(cursor) > 0 && (len(cursor) != 64 || !isLowerHex(cursor)) {
		return fleet.StoredPage{}, invalidListCursor()
	}
	head, err := s.runtime.CurrentHead(ctx)
	if err != nil {
		return fleet.StoredPage{}, publicError(err)
	}
	var startAfter []byte
	if len(cursor) > 0 {
		startAfter = append([]byte("agent/"), cursor...)
	}
	page, err := s.runtime.ScanCommitted(ctx, explorercoord.CommittedScanRequest{
		Table: Table, RowPrefix: []byte("agent/"), StartAfterRow: startAfter,
		Family: recordFamily, Qualifier: recordQualifier, Visibility: s.visibility,
		Frontier: head.Frontier, Limit: limit,
		MaxScanned: explorercoord.MaxCommittedScanCells,
	})
	if err != nil {
		return fleet.StoredPage{}, publicError(err)
	}
	result := fleet.StoredPage{Entries: make([]fleet.Stored, 0, len(page.Cells))}
	for _, cell := range page.Cells {
		descriptor, registrationDigest, decodeErr := decodeDescriptor(cell.Cell.Value)
		if decodeErr != nil {
			return fleet.StoredPage{}, shoal.WrapError(
				shoal.ErrorInternal, "invalid committed fleet descriptor", decodeErr)
		}
		result.Entries = append(result.Entries, fleet.Stored{
			Descriptor: descriptor, RegistrationDigest: registrationDigest,
		})
	}
	if bytes.HasPrefix(page.NextRow, []byte("agent/")) {
		result.Next = append([]byte(nil), page.NextRow[len("agent/"):]...)
	}
	return result, nil
}

func invalidListCursor() error {
	return shoal.NewError(shoal.ErrorInvalidArgument, "fleet list cursor is invalid")
}

func isLowerHex(value []byte) bool {
	for _, current := range value {
		if !((current >= '0' && current <= '9') ||
			(current >= 'a' && current <= 'f')) {
			return false
		}
	}
	return true
}

func isNilRuntime(runtime Runtime) bool {
	if runtime == nil {
		return true
	}
	value := reflect.ValueOf(runtime)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func (s *Store) Apply(ctx context.Context, mutation fleet.Mutation) (fleet.Stored, error) {
	if err := shoal.ValidateRequiredID("agent ID", mutation.Descriptor.ID); err != nil {
		return fleet.Stored{}, err
	}
	if err := shoal.ValidateRequiredID("registration key", mutation.RegistrationKey); err != nil {
		return fleet.Stored{}, err
	}
	if mutation.ExpectedGeneration < 0 ||
		mutation.Descriptor.Generation != mutation.ExpectedGeneration+1 {
		return fleet.Stored{}, shoal.NewError(shoal.ErrorInvalidArgument, "fleet generation transition is invalid")
	}
	keyDigest := sha256.Sum256([]byte(mutation.RegistrationKey))
	value, err := encodeDescriptor(mutation.Descriptor, keyDigest)
	if err != nil {
		return fleet.Stored{}, err
	}
	if current, readErr := s.Get(ctx, mutation.Descriptor.ID); readErr == nil &&
		current.Descriptor.Generation == mutation.ExpectedGeneration+1 {
		if current.RegistrationDigest == keyDigest &&
			equivalentReplay(current.Descriptor, mutation.Descriptor) {
			return current, nil
		}
		return fleet.Stored{}, shoal.NewError(shoal.ErrorConflict, "registration key replay is divergent")
	}
	lpart, err := explorercoord.Partition(coordination.DomainID("fleet-registry"), []byte(mutation.Descriptor.ID))
	if err != nil {
		return fleet.Stored{}, publicError(err)
	}
	agentGuard := explorercoord.GuardIntent{
		Entity: agentEntity(mutation.Descriptor.ID), DesiredState: guard.StateLive,
		DesiredWinnerID: descriptorWinner(value), LPART: lpart,
		LogicalPolicyID: logicalPolicy, RetirementGeneration: 1,
	}
	if !mutation.Descriptor.RevokedAt.IsZero() {
		agentGuard.DesiredState = guard.StateTombstone
	}
	if mutation.ExpectedGeneration == 0 {
		if current, readErr := s.Get(ctx, mutation.Descriptor.ID); readErr == nil {
			if current.RegistrationDigest == keyDigest &&
				equivalentReplay(current.Descriptor, mutation.Descriptor) {
				return current, nil
			}
			return fleet.Stored{}, shoal.NewError(shoal.ErrorConflict, "agent registration conflicts with an existing descriptor")
		} else if !shoal.IsErrorCode(readErr, shoal.ErrorNotFound) {
			return fleet.Stored{}, readErr
		}
		agentGuard.Mode = guard.ModeAbsentOrIdentical
	} else {
		current, err := s.Get(ctx, mutation.Descriptor.ID)
		if err != nil {
			return fleet.Stored{}, err
		}
		if current.Descriptor.Generation != mutation.ExpectedGeneration {
			return fleet.Stored{}, shoal.NewError(shoal.ErrorConflict, "agent generation conflict")
		}
		agentGuard.Mode = guard.ModeMutate
		if agentGuard.DesiredState == guard.StateTombstone {
			agentGuard.Mode = guard.ModeRetire
		}
		agentGuard.ExpectedEpoch = coordination.Epoch(current.Epoch)
		agentGuard.ExpectedDigest = current.Digest
	}
	intent := explorercoord.Intent{
		Operation: []byte("fleet.registry.apply.v1"),
		Token:     []byte(mutation.RegistrationKey),
		Cells: []explorercoord.Cell{{
			Table: Table, Row: agentRow(mutation.Descriptor.ID),
			Family: recordFamily, Qualifier: recordQualifier,
			Visibility: s.visibility, Value: value, EpochTimestamp: true,
			LPART: lpart, CopyGeneration: 1,
		}},
		Guards:  []explorercoord.GuardIntent{agentGuard},
		Results: []explorercoord.ResultIdentity{{Kind: []byte("agent"), ID: []byte(mutation.Descriptor.ID)}},
	}
	result, err := s.runtime.Publish(ctx, explorercoord.Request{Intent: intent})
	if err != nil {
		// Resolve ambiguous publication only from the authoritative committed
		// entity head and its exact committed cell. Never infer rollback from
		// a staged physical value.
		resolveContext := context.WithoutCancel(ctx)
		for attempt := 0; attempt < 10; attempt++ {
			if current, readErr := s.Get(resolveContext, mutation.Descriptor.ID); readErr == nil {
				if current.RegistrationDigest == keyDigest &&
					equivalentReplay(
						current.Descriptor, mutation.Descriptor) {
					return current, nil
				}
				currentValue, encodeErr := encodeDescriptor(
					current.Descriptor, current.RegistrationDigest)
				if encodeErr == nil && bytes.Equal(descriptorWinner(value), descriptorWinner(currentValue)) {
					return current, nil
				}
				if current.Descriptor.Generation >= mutation.Descriptor.Generation {
					return fleet.Stored{}, shoal.NewError(shoal.ErrorConflict, "agent generation conflict")
				}
			}
			time.Sleep(time.Millisecond)
		}
		return fleet.Stored{}, publicError(err)
	}
	stored, err := s.Get(ctx, mutation.Descriptor.ID)
	if err != nil {
		if errors.Is(err, explorercoord.ErrIndeterminatePublication) {
			return fleet.Stored{}, shoal.WrapError(shoal.ErrorUnavailable, "fleet publication is indeterminate", err)
		}
		return fleet.Stored{}, err
	}
	if stored.Descriptor.Generation != mutation.Descriptor.Generation ||
		!bytes.Equal(
			descriptorWinner(value),
			descriptorWinner(mustEncodeDescriptor(
				stored.Descriptor, stored.RegistrationDigest)),
		) {
		return fleet.Stored{}, shoal.NewError(shoal.ErrorConflict, "fleet publication resolved to a divergent descriptor")
	}
	_ = result
	return stored, nil
}

func (s *Store) readCommittedCell(
	ctx context.Context,
	row, qualifier []byte,
	epoch coordination.Epoch,
) ([]byte, error) {
	page, err := s.runtime.ScanCommitted(ctx, explorercoord.CommittedScanRequest{
		Table: Table, RowPrefix: row, Family: recordFamily,
		Qualifier: qualifier, Visibility: s.visibility, Limit: 1,
		Frontier: epoch,
	})
	if err != nil {
		return nil, err
	}
	if len(page.Cells) != 1 ||
		!bytes.Equal(page.Cells[0].Cell.Coordinate.Row, row) ||
		page.Cells[0].Epoch != epoch {
		return nil, transaction.ErrNotFound
	}
	return append([]byte(nil), page.Cells[0].Cell.Value...), nil
}

func agentEntity(id shoal.ID) guard.Entity {
	return guard.Entity{Kind: agentKind, ID: coordination.EntityID([]byte(id))}
}

func agentRow(id shoal.ID) []byte {
	digest := sha256.Sum256(append(
		[]byte("shoal.fleet.registry-agent-row.v1\x00"), []byte(id)...))
	return []byte("agent/" + fmt.Sprintf("%x", digest[:]))
}
func descriptorWinner(value []byte) []byte {
	digest := sha256.Sum256(value)
	return digest[:]
}

func publicError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return shoal.WrapError(shoal.ErrorCanceled, "fleet registry request canceled", err)
	case errors.Is(err, context.DeadlineExceeded):
		return shoal.WrapError(shoal.ErrorDeadline, "fleet registry deadline exceeded", err)
	case errors.Is(err, transaction.ErrConflict), errors.Is(err, guard.ErrConflict):
		return shoal.NewError(shoal.ErrorConflict, "fleet registry conflict")
	case errors.Is(err, transaction.ErrNotFound), errors.Is(err, guard.ErrNotFound):
		return shoal.NewError(shoal.ErrorNotFound, "agent not found")
	case errors.Is(err, transaction.ErrInvalid):
		return shoal.NewError(shoal.ErrorInvalidArgument, "fleet registry request is invalid")
	case errors.Is(err, explorercoord.ErrIndeterminatePublication), errors.Is(err, guard.ErrUnknown):
		return shoal.WrapError(shoal.ErrorUnavailable, "fleet registry publication is indeterminate", err)
	default:
		return shoal.WrapError(shoal.ErrorUnavailable, "fleet registry storage is unavailable", err)
	}
}

func mustEncodeDescriptor(
	descriptor fleet.Descriptor,
	registrationDigest [sha256.Size]byte,
) []byte {
	value, _ := encodeDescriptor(descriptor, registrationDigest)
	return value
}

func encodeDescriptor(
	descriptor fleet.Descriptor,
	registrationDigest [sha256.Size]byte,
) ([]byte, error) {
	var buffer bytes.Buffer
	writeU16(&buffer, codecVersion)
	writeString(&buffer, string(descriptor.ID))
	writeI64(&buffer, descriptor.Generation)
	writeString(&buffer, string(descriptor.Subject))
	writeString(&buffer, string(descriptor.Actor))
	writeString(&buffer, string(descriptor.ParentID))
	writeBytes(&buffer, descriptor.AuthorizationDomain)
	writeU32(&buffer, uint32(len(descriptor.Scopes)))
	for _, scope := range descriptor.Scopes {
		writeBytes(&buffer, scope.SourceID)
		writeBytes(&buffer, scope.PolicyID)
	}
	writeString(&buffer, descriptor.ExecutorRef)
	writeU32(&buffer, uint32(len(descriptor.Capabilities)))
	for _, capability := range descriptor.Capabilities {
		writeString(&buffer, capability.Name)
		writeU32(&buffer, uint32(len(capability.Actions)))
		for _, action := range capability.Actions {
			writeString(&buffer, action.Name)
			writeBytes(&buffer, action.InputSchema)
			writeBytes(&buffer, action.OutputSchema)
		}
	}
	writeI64(&buffer, descriptor.LeaseExpiresAt.UnixNano())
	writeI64(&buffer, descriptor.UpdatedAt.UnixNano())
	writeI64(&buffer, timeValue(descriptor.RevokedAt))
	writeBytes(&buffer, registrationDigest[:])
	if buffer.Len() > fleet.MaxDescriptorBytes {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet descriptor exceeds its bound")
	}
	return buffer.Bytes(), nil
}

func decodeDescriptor(value []byte) (
	fleet.Descriptor,
	[sha256.Size]byte,
	error,
) {
	reader := bytes.NewReader(value)
	version, err := readU16(reader)
	if err != nil || version != codecVersion {
		return fleet.Descriptor{}, [sha256.Size]byte{},
			errors.New("unknown descriptor encoding")
	}
	var descriptor fleet.Descriptor
	if descriptor.ID, err = readID(reader); err != nil {
		return fleet.Descriptor{}, [sha256.Size]byte{}, err
	}
	if descriptor.Generation, err = readI64(reader); err != nil {
		return fleet.Descriptor{}, [sha256.Size]byte{}, err
	}
	if descriptor.Subject, err = readID(reader); err != nil {
		return fleet.Descriptor{}, [sha256.Size]byte{}, err
	}
	if descriptor.Actor, err = readID(reader); err != nil {
		return fleet.Descriptor{}, [sha256.Size]byte{}, err
	}
	if descriptor.ParentID, err = readID(reader); err != nil {
		return fleet.Descriptor{}, [sha256.Size]byte{}, err
	}
	if descriptor.AuthorizationDomain, err = readBytes(reader, 1024); err != nil {
		return fleet.Descriptor{}, [sha256.Size]byte{}, err
	}
	scopeCount, err := readU32(reader)
	if err != nil || scopeCount > fleet.MaxScopes {
		return fleet.Descriptor{}, [sha256.Size]byte{},
			errors.New("invalid scope count")
	}
	descriptor.Scopes = make([]fleet.Scope, scopeCount)
	for i := range descriptor.Scopes {
		if descriptor.Scopes[i].SourceID, err = readBytes(reader, 1024); err != nil {
			return fleet.Descriptor{}, [sha256.Size]byte{}, err
		}
		if descriptor.Scopes[i].PolicyID, err = readBytes(reader, 1024); err != nil {
			return fleet.Descriptor{}, [sha256.Size]byte{}, err
		}
	}
	if descriptor.ExecutorRef, err = readString(reader, fleet.MaxExecutorRefBytes); err != nil {
		return fleet.Descriptor{}, [sha256.Size]byte{}, err
	}
	capabilityCount, err := readU32(reader)
	if err != nil || capabilityCount > fleet.MaxCapabilities {
		return fleet.Descriptor{}, [sha256.Size]byte{},
			errors.New("invalid capability count")
	}
	descriptor.Capabilities = make([]fleet.Capability, capabilityCount)
	totalActions := 0
	for i := range descriptor.Capabilities {
		if descriptor.Capabilities[i].Name, err = readString(reader, fleet.MaxNameBytes); err != nil {
			return fleet.Descriptor{}, [sha256.Size]byte{}, err
		}
		actionCount, readErr := readU32(reader)
		if readErr != nil {
			return fleet.Descriptor{}, [sha256.Size]byte{}, readErr
		}
		totalActions += int(actionCount)
		if totalActions > fleet.MaxActions {
			return fleet.Descriptor{}, [sha256.Size]byte{},
				errors.New("invalid action count")
		}
		descriptor.Capabilities[i].Actions = make([]fleet.Action, actionCount)
		for j := range descriptor.Capabilities[i].Actions {
			action := &descriptor.Capabilities[i].Actions[j]
			if action.Name, err = readString(reader, fleet.MaxNameBytes); err != nil {
				return fleet.Descriptor{}, [sha256.Size]byte{}, err
			}
			if action.InputSchema, err = readBytes(reader, fleet.MaxSchemaBytes); err != nil {
				return fleet.Descriptor{}, [sha256.Size]byte{}, err
			}
			if action.OutputSchema, err = readBytes(reader, fleet.MaxSchemaBytes); err != nil {
				return fleet.Descriptor{}, [sha256.Size]byte{}, err
			}
		}
	}
	var lease, updated, revoked int64
	if lease, err = readI64(reader); err != nil {
		return fleet.Descriptor{}, [sha256.Size]byte{}, err
	}
	if updated, err = readI64(reader); err != nil {
		return fleet.Descriptor{}, [sha256.Size]byte{}, err
	}
	if revoked, err = readI64(reader); err != nil {
		return fleet.Descriptor{}, [sha256.Size]byte{}, err
	}
	registrationDigest, err := readBytes(reader, sha256.Size)
	if err != nil || len(registrationDigest) != sha256.Size {
		return fleet.Descriptor{}, [sha256.Size]byte{},
			errors.New("invalid registration digest")
	}
	var digest [sha256.Size]byte
	copy(digest[:], registrationDigest)
	if reader.Len() != 0 {
		return fleet.Descriptor{}, [sha256.Size]byte{},
			errors.New("trailing descriptor bytes")
	}
	descriptor.LeaseExpiresAt = time.Unix(0, lease).UTC()
	descriptor.UpdatedAt = time.Unix(0, updated).UTC()
	if revoked != 0 {
		descriptor.RevokedAt = time.Unix(0, revoked).UTC()
	}
	return descriptor, digest, nil
}

func writeU16(w io.Writer, value uint16)    { _ = binary.Write(w, binary.BigEndian, value) }
func writeU32(w io.Writer, value uint32)    { _ = binary.Write(w, binary.BigEndian, value) }
func writeI64(w io.Writer, value int64)     { _ = binary.Write(w, binary.BigEndian, value) }
func writeBytes(w io.Writer, value []byte)  { writeU32(w, uint32(len(value))); _, _ = w.Write(value) }
func writeString(w io.Writer, value string) { writeBytes(w, []byte(value)) }
func readU16(r io.Reader) (uint16, error) {
	var value uint16
	err := binary.Read(r, binary.BigEndian, &value)
	return value, err
}
func readU32(r io.Reader) (uint32, error) {
	var value uint32
	err := binary.Read(r, binary.BigEndian, &value)
	return value, err
}
func readI64(r io.Reader) (int64, error) {
	var value int64
	err := binary.Read(r, binary.BigEndian, &value)
	return value, err
}
func readBytes(r *bytes.Reader, maximum int) ([]byte, error) {
	size, err := readU32(r)
	if err != nil || uint64(size) > uint64(maximum) || uint64(size) > uint64(r.Len()) {
		return nil, errors.New("bounded byte field is invalid")
	}
	value := make([]byte, size)
	_, err = io.ReadFull(r, value)
	return value, err
}
func readString(r *bytes.Reader, maximum int) (string, error) {
	value, err := readBytes(r, maximum)
	return string(value), err
}
func readID(r *bytes.Reader) (shoal.ID, error) {
	value, err := readString(r, shoal.MaxIDBytes)
	return shoal.ID(value), err
}
func timeValue(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixNano()
}

func equivalentReplay(existing, wanted fleet.Descriptor) bool {
	existing.UpdatedAt = time.Time{}
	wanted.UpdatedAt = time.Time{}
	existingValue, existingErr := encodeDescriptor(existing, [sha256.Size]byte{})
	wantedValue, wantedErr := encodeDescriptor(wanted, [sha256.Size]byte{})
	return existingErr == nil && wantedErr == nil && bytes.Equal(existingValue, wantedValue)
}
