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
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/guard"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/transaction"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	Table                        = "_shoal_explorer_fleet"
	catalogShards                = 256
	agentKind             byte   = 'A'
	catalogKind           byte   = 'C'
	codecVersion          uint16 = 1
	reconciliationTimeout        = 5 * time.Second
)

var (
	recordFamily     = []byte("r")
	recordQualifier  = []byte("descriptor")
	catalogQualifier = []byte("catalog")
	logicalPolicy    = []byte("fleet/registry/v1")
)

type Runtime interface {
	Publish(context.Context, explorercoord.Request) (explorercoord.Result, error)
	ReadEntity(context.Context, guard.Entity) (*guard.Head, *guard.Pending, error)
	ReadCommittedCell(context.Context, string, []byte, []byte, []byte, []byte, coordination.Epoch) (explorercoord.CommittedCell, bool, error)
}

type Store struct {
	runtime               Runtime
	visibility            []byte
	reconciliationTimeout time.Duration
}

func NewStore(runtime Runtime, visibility []byte) (*Store, error) {
	if isNilDependency(runtime) {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet runtime is required")
	}
	if len(visibility) > coordination.MaxCoordinateBytes {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet visibility exceeds its bound")
	}
	return &Store{
		runtime: runtime, visibility: append([]byte(nil), visibility...),
		reconciliationTimeout: reconciliationTimeout,
	}, nil
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
	cell, found, err := s.runtime.ReadCommittedCell(
		ctx, Table, agentRow(id), recordFamily, recordQualifier,
		s.visibility, head.Epoch)
	if err != nil {
		return fleet.Stored{}, publicError(err)
	}
	if !found {
		return fleet.Stored{}, shoal.NewError(
			shoal.ErrorNotFound, "agent not found")
	}
	descriptor, err := decodeDescriptor(cell.Cell.Value)
	if err != nil {
		return fleet.Stored{}, shoal.WrapError(shoal.ErrorInternal, "invalid committed fleet descriptor", err)
	}
	return fleet.Stored{Descriptor: descriptor, Digest: head.LogicalDigest, Epoch: int64(head.Epoch)}, nil
}

func (s *Store) ListPage(
	ctx context.Context,
	cursor string,
	limit uint32,
) (fleet.StoredPage, error) {
	if limit == 0 || limit > fleet.MaxListPageSize {
		return fleet.StoredPage{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"fleet list page size is outside its bound",
		)
	}
	startShard, after, err := decodeListCursor(cursor)
	if err != nil {
		return fleet.StoredPage{}, err
	}
	result := fleet.StoredPage{
		Items: make([]fleet.StoredListItem, 0, limit),
	}
	for shard := startShard; shard < catalogShards; shard++ {
		head, _, err := s.runtime.ReadEntity(ctx, catalogEntity(byte(shard)))
		if err != nil {
			if errors.Is(err, guard.ErrNotFound) || errors.Is(err, transaction.ErrNotFound) {
				after = ""
				continue
			}
			return fleet.StoredPage{}, publicError(err)
		}
		if head == nil {
			after = ""
			continue
		}
		cell, found, err := s.runtime.ReadCommittedCell(
			ctx, Table, catalogRow(byte(shard)), recordFamily,
			catalogQualifier, s.visibility, head.Epoch)
		if err != nil {
			return fleet.StoredPage{}, publicError(err)
		}
		if !found {
			return fleet.StoredPage{}, shoal.NewError(
				shoal.ErrorInternal, "committed fleet catalog is missing")
		}
		shardIDs, err := decodeCatalog(cell.Cell.Value)
		if err != nil {
			return fleet.StoredPage{}, shoal.WrapError(
				shoal.ErrorInternal, "invalid committed fleet catalog", err)
		}
		sort.Slice(shardIDs, func(i, j int) bool {
			return shoal.CompareID(shardIDs[i], shardIDs[j]) < 0
		})
		start := 0
		if shard == startShard && after != "" {
			start = sort.Search(len(shardIDs), func(index int) bool {
				return shoal.CompareID(shardIDs[index], after) > 0
			})
		}
		for index := start; index < len(shardIDs); index++ {
			id := shardIDs[index]
			item, err := s.Get(ctx, id)
			if err != nil {
				return fleet.StoredPage{}, err
			}
			itemCursor := encodeListCursor(byte(shard), id)
			result.Items = append(result.Items, fleet.StoredListItem{
				Stored: item, Cursor: itemCursor,
			})
			if len(result.Items) == int(limit) {
				if index+1 < len(shardIDs) || shard+1 < catalogShards {
					result.NextCursor = itemCursor
				}
				return result, nil
			}
		}
		after = ""
	}
	return result, nil
}

func (s *Store) Apply(ctx context.Context, mutation fleet.Mutation) (fleet.Stored, error) {
	if err := shoal.ValidateRequiredID("agent ID", mutation.Descriptor.ID); err != nil {
		return fleet.Stored{}, err
	}
	if err := shoal.ValidateRequiredID("registration key", mutation.RegistrationKey); err != nil {
		return fleet.Stored{}, err
	}
	if mutation.ExpectedGeneration < 0 ||
		mutation.ExpectedGeneration == math.MaxInt64 ||
		mutation.Descriptor.Generation != mutation.ExpectedGeneration+1 {
		return fleet.Stored{}, shoal.NewError(shoal.ErrorInvalidArgument, "fleet generation transition is invalid")
	}
	keyDigest := sha256.Sum256([]byte(mutation.RegistrationKey))
	mutation.Descriptor.RegistrationDigest = keyDigest
	value, err := encodeDescriptor(mutation.Descriptor)
	if err != nil {
		return fleet.Stored{}, err
	}
	if current, readErr := s.Get(ctx, mutation.Descriptor.ID); readErr == nil &&
		current.Descriptor.Generation == mutation.ExpectedGeneration+1 {
		if current.Descriptor.RegistrationDigest == keyDigest &&
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
			if current.Descriptor.RegistrationDigest == keyDigest &&
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
	if mutation.ExpectedGeneration == 0 {
		if err := s.addCatalogMutation(ctx, &intent, mutation.Descriptor.ID); err != nil {
			return fleet.Stored{}, err
		}
	}
	result, err := s.runtime.Publish(ctx, explorercoord.Request{Intent: intent})
	if err != nil {
		// Resolve ambiguous publication only from the authoritative committed
		// entity head and its exact committed cell. Never infer rollback from
		// a staged physical value.
		resolveContext, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), s.reconciliationTimeout)
		defer cancel()
		for attempt := 0; attempt < 10; attempt++ {
			if current, readErr := s.Get(resolveContext, mutation.Descriptor.ID); readErr == nil {
				currentValue, encodeErr := encodeDescriptor(current.Descriptor)
				if encodeErr == nil && bytes.Equal(descriptorWinner(value), descriptorWinner(currentValue)) {
					return current, nil
				}
				if current.Descriptor.Generation >= mutation.Descriptor.Generation {
					return fleet.Stored{}, shoal.NewError(shoal.ErrorConflict, "agent generation conflict")
				}
			}
			timer := time.NewTimer(time.Millisecond)
			select {
			case <-resolveContext.Done():
				timer.Stop()
				return fleet.Stored{}, publicError(err)
			case <-timer.C:
			}
		}
		return fleet.Stored{}, publicError(err)
	}

	readbackContext, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), s.reconciliationTimeout)
	defer cancel()
	stored, err := s.Get(readbackContext, mutation.Descriptor.ID)
	if err != nil {
		return fleet.Stored{}, explorer.MarkIndeterminateCommit(
			shoal.WrapError(
				shoal.ErrorUnavailable,
				"fleet publication readback is indeterminate",
				err,
			),
		)
	}
	if stored.Descriptor.Generation != mutation.Descriptor.Generation ||
		!bytes.Equal(descriptorWinner(value), descriptorWinner(mustEncodeDescriptor(stored.Descriptor))) {
		return fleet.Stored{}, explorer.MarkIndeterminateCommit(
			shoal.NewError(
				shoal.ErrorConflict,
				"fleet publication resolved to a divergent descriptor",
			),
		)
	}
	_ = result
	return stored, nil
}

func encodeListCursor(shard byte, id shoal.ID) string {
	value := make([]byte, 1+len(id))
	value[0] = shard
	copy(value[1:], id)
	return base64.RawURLEncoding.EncodeToString(value)
}

func decodeListCursor(cursor string) (int, shoal.ID, error) {
	if cursor == "" {
		return 0, "", nil
	}
	if len(cursor) > fleet.MaxListCursorBytes {
		return 0, "", shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet list cursor exceeds its bound")
	}
	value, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || len(value) < 2 {
		return 0, "", shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet list cursor is invalid")
	}
	shard := value[0]
	id := shoal.ID(string(value[1:]))
	if err := shoal.ValidateRequiredID(
		"fleet list cursor agent ID", id,
	); err != nil || catalogShard(id) != shard {
		return 0, "", shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet list cursor is invalid")
	}
	return int(shard), id, nil
}

func (s *Store) addCatalogMutation(ctx context.Context, intent *explorercoord.Intent, id shoal.ID) error {
	shard := catalogShard(id)
	entity := catalogEntity(shard)
	head, _, err := s.runtime.ReadEntity(ctx, entity)
	var ids []shoal.ID
	var expectedEpoch coordination.Epoch
	var expectedDigest coordination.Digest
	mode := guard.ModeAbsentOrIdentical
	if err == nil && head != nil {
		cell, found, readErr := s.runtime.ReadCommittedCell(
			ctx, Table, catalogRow(shard), recordFamily,
			catalogQualifier, s.visibility, head.Epoch)
		if readErr != nil {
			return publicError(readErr)
		}
		if !found {
			return shoal.NewError(
				shoal.ErrorInternal, "committed fleet catalog is missing")
		}
		ids, readErr = decodeCatalog(cell.Cell.Value)
		if readErr != nil {
			return shoal.WrapError(shoal.ErrorInternal, "invalid committed fleet catalog", readErr)
		}
		expectedEpoch, expectedDigest, mode = head.Epoch, head.LogicalDigest, guard.ModeMutate
	} else if err != nil && !errors.Is(err, guard.ErrNotFound) && !errors.Is(err, transaction.ErrNotFound) {
		return publicError(err)
	}
	for _, existing := range ids {
		if existing == id {
			return shoal.NewError(shoal.ErrorConflict, "agent catalog identity already exists")
		}
	}
	ids = append(ids, id)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	value, err := encodeCatalog(ids)
	if err != nil {
		return err
	}
	lpart, err := explorercoord.Partition(coordination.DomainID("fleet-registry"), catalogRow(shard))
	if err != nil {
		return publicError(err)
	}
	intent.Cells = append(intent.Cells, explorercoord.Cell{
		Table: Table, Row: catalogRow(shard), Family: recordFamily,
		Qualifier: catalogQualifier, Visibility: s.visibility, Value: value,
		EpochTimestamp: true, LPART: lpart, CopyGeneration: 1,
	})
	intent.Guards = append(intent.Guards, explorercoord.GuardIntent{
		Entity: entity, Mode: mode, ExpectedEpoch: expectedEpoch,
		ExpectedDigest: expectedDigest, DesiredState: guard.StateLive,
		DesiredWinnerID: descriptorWinner(value), LPART: lpart,
		LogicalPolicyID: logicalPolicy, RetirementGeneration: 1,
	})
	return nil
}

func agentEntity(id shoal.ID) guard.Entity {
	return guard.Entity{Kind: agentKind, ID: coordination.EntityID([]byte(id))}
}

func catalogEntity(shard byte) guard.Entity {
	return guard.Entity{Kind: catalogKind, ID: coordination.EntityID(catalogRow(shard))}
}

func agentRow(id shoal.ID) []byte   { return append([]byte("agent/"), []byte(id)...) }
func catalogRow(shard byte) []byte  { return []byte(fmt.Sprintf("catalog/%02x", shard)) }
func catalogShard(id shoal.ID) byte { return sha256.Sum256([]byte(id))[0] }
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

func mustEncodeDescriptor(descriptor fleet.Descriptor) []byte {
	value, _ := encodeDescriptor(descriptor)
	return value
}

func encodeDescriptor(descriptor fleet.Descriptor) ([]byte, error) {
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
	writeBytes(&buffer, descriptor.RegistrationDigest[:])
	if buffer.Len() > fleet.MaxDescriptorBytes {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet descriptor exceeds its bound")
	}
	return buffer.Bytes(), nil
}

func decodeDescriptor(value []byte) (fleet.Descriptor, error) {
	reader := bytes.NewReader(value)
	version, err := readU16(reader)
	if err != nil || version != codecVersion {
		return fleet.Descriptor{}, errors.New("unknown descriptor encoding")
	}
	var descriptor fleet.Descriptor
	if descriptor.ID, err = readID(reader); err != nil {
		return fleet.Descriptor{}, err
	}
	if descriptor.Generation, err = readI64(reader); err != nil {
		return fleet.Descriptor{}, err
	}
	if descriptor.Subject, err = readID(reader); err != nil {
		return fleet.Descriptor{}, err
	}
	if descriptor.Actor, err = readID(reader); err != nil {
		return fleet.Descriptor{}, err
	}
	if descriptor.ParentID, err = readID(reader); err != nil {
		return fleet.Descriptor{}, err
	}
	if descriptor.AuthorizationDomain, err = readBytes(reader, 1024); err != nil {
		return fleet.Descriptor{}, err
	}
	scopeCount, err := readU32(reader)
	if err != nil || scopeCount > fleet.MaxScopes {
		return fleet.Descriptor{}, errors.New("invalid scope count")
	}
	descriptor.Scopes = make([]fleet.Scope, scopeCount)
	for i := range descriptor.Scopes {
		if descriptor.Scopes[i].SourceID, err = readBytes(reader, 1024); err != nil {
			return fleet.Descriptor{}, err
		}
		if descriptor.Scopes[i].PolicyID, err = readBytes(reader, 1024); err != nil {
			return fleet.Descriptor{}, err
		}
	}
	if descriptor.ExecutorRef, err = readString(reader, fleet.MaxExecutorRefBytes); err != nil {
		return fleet.Descriptor{}, err
	}
	capabilityCount, err := readU32(reader)
	if err != nil || capabilityCount > fleet.MaxCapabilities {
		return fleet.Descriptor{}, errors.New("invalid capability count")
	}
	descriptor.Capabilities = make([]fleet.Capability, capabilityCount)
	totalActions := 0
	for i := range descriptor.Capabilities {
		if descriptor.Capabilities[i].Name, err = readString(reader, fleet.MaxNameBytes); err != nil {
			return fleet.Descriptor{}, err
		}
		actionCount, readErr := readU32(reader)
		if readErr != nil {
			return fleet.Descriptor{}, readErr
		}
		totalActions += int(actionCount)
		if totalActions > fleet.MaxActions {
			return fleet.Descriptor{}, errors.New("invalid action count")
		}
		descriptor.Capabilities[i].Actions = make([]fleet.Action, actionCount)
		for j := range descriptor.Capabilities[i].Actions {
			action := &descriptor.Capabilities[i].Actions[j]
			if action.Name, err = readString(reader, fleet.MaxNameBytes); err != nil {
				return fleet.Descriptor{}, err
			}
			if action.InputSchema, err = readBytes(reader, fleet.MaxSchemaBytes); err != nil {
				return fleet.Descriptor{}, err
			}
			if action.OutputSchema, err = readBytes(reader, fleet.MaxSchemaBytes); err != nil {
				return fleet.Descriptor{}, err
			}
		}
	}
	var lease, updated, revoked int64
	if lease, err = readI64(reader); err != nil {
		return fleet.Descriptor{}, err
	}
	if updated, err = readI64(reader); err != nil {
		return fleet.Descriptor{}, err
	}
	if revoked, err = readI64(reader); err != nil {
		return fleet.Descriptor{}, err
	}
	registrationDigest, err := readBytes(reader, sha256.Size)
	if err != nil || len(registrationDigest) != sha256.Size {
		return fleet.Descriptor{}, errors.New("invalid registration digest")
	}
	copy(descriptor.RegistrationDigest[:], registrationDigest)
	if reader.Len() != 0 {
		return fleet.Descriptor{}, errors.New("trailing descriptor bytes")
	}
	descriptor.LeaseExpiresAt = time.Unix(0, lease).UTC()
	descriptor.UpdatedAt = time.Unix(0, updated).UTC()
	if revoked != 0 {
		descriptor.RevokedAt = time.Unix(0, revoked).UTC()
	}
	return descriptor, nil
}

func encodeCatalog(ids []shoal.ID) ([]byte, error) {
	var buffer bytes.Buffer
	writeU16(&buffer, codecVersion)
	writeU32(&buffer, uint32(len(ids)))
	for _, id := range ids {
		if err := shoal.ValidateRequiredID("agent ID", id); err != nil {
			return nil, err
		}
		writeString(&buffer, string(id))
	}
	return buffer.Bytes(), nil
}

func decodeCatalog(value []byte) ([]shoal.ID, error) {
	reader := bytes.NewReader(value)
	version, err := readU16(reader)
	if err != nil || version != codecVersion {
		return nil, errors.New("unknown catalog encoding")
	}
	count, err := readU32(reader)
	if err != nil || count > 1_000_000 {
		return nil, errors.New("invalid catalog count")
	}
	ids := make([]shoal.ID, count)
	for i := range ids {
		if ids[i], err = readID(reader); err != nil {
			return nil, err
		}
	}
	if reader.Len() != 0 {
		return nil, errors.New("trailing catalog bytes")
	}
	return ids, nil
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
	existingValue, existingErr := encodeDescriptor(existing)
	wantedValue, wantedErr := encodeDescriptor(wanted)
	return existingErr == nil && wantedErr == nil && bytes.Equal(existingValue, wantedValue)
}
