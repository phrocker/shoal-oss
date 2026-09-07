/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package workspace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash"
	"math"
	"strings"
	"sync"

	"github.com/phrocker/shoal-oss/internal/cclient"
	"github.com/phrocker/shoal-oss/internal/dirlock"
	"github.com/phrocker/shoal-oss/internal/engine"
	"github.com/phrocker/shoal-oss/internal/iterrt"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	settingsTable = "_shoal_workspace_settings"
	settingsCF    = "settings"
	settingsCQ    = "v1"

	settingsRecordMagic     = "SHOALWS1"
	settingsEnvelopeVersion = byte(1)
	settingsRecordKind      = byte(1)
	settingsEnvelopeHeader  = 8 + 1 + 1 + 8 + sha256.Size
	maxSettingsRecordBytes  = uint64(8 << 20)
)

// Store is the durable settings persistence contract used by Provider.
type Store interface {
	Load(context.Context, shoal.ID) (Settings, error)
	CompareAndSwap(
		context.Context,
		shoal.ID,
		shoal.ID,
		[]byte,
		uint64,
		shoal.ID,
		Narrowing,
	) (Settings, error)
}

// DurableStore persists one current CAS-protected settings row per workspace
// in Shoal's embedded engine.
type DurableStore struct {
	// The engine's point-read path is not concurrent with writes. Serialize the
	// one settings row operation honestly while retaining engine CAS as the
	// durable expected-version guard.
	mu               sync.Mutex
	engine           *engine.Engine
	lock             *dirlock.Lock
	conditionalWrite func(
		string, []engine.ConditionalMutation,
	) ([]bool, error)
	closed bool
}

type persistedSettings struct {
	WorkspaceID         string
	SettingsID          string
	Owner               string
	AuthorizationDomain []byte
	Revision            uint64
	LastMutationID      string
	LastMutationDigest  [sha256.Size]byte
	AllowedPresent      bool
	AllowedOperations   []string
	SourcesPresent      bool
	PermittedSources    [][]byte
	PoliciesPresent     bool
	PermittedPolicies   [][]byte
	RetrievalTopK       *uint32
	GraphDepth          *uint32
	GraphFanout         *uint32
	GraphNodes          *uint32
	OutputBytes         *uint64
	OutputPolicies      [][]byte
	OntologyPresent     bool
	OntologySchemaID    string
	OntologyVersionID   string
}

// OpenDurableStore opens or creates the workspace settings store.
func OpenDurableStore(dir string) (*DurableStore, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, invalid("settings directory is required")
	}
	lock, err := dirlock.Acquire(dir, ".shoal-workspace-settings.lock")
	if err != nil {
		message := "acquire workspace settings directory ownership"
		if errors.Is(err, dirlock.ErrLocked) {
			message = "workspace settings directory is already open"
		}
		return nil, shoal.WrapError(
			shoal.ErrorUnavailable,
			message,
			err,
		)
	}
	eng, err := engine.Open(dir, engine.Options{})
	if err != nil {
		_ = lock.Close()
		return nil, shoal.WrapError(
			shoal.ErrorUnavailable, "open workspace settings storage", err)
	}
	found := false
	for _, table := range eng.TableNames() {
		if table == settingsTable {
			found = true
			break
		}
	}
	if !found {
		if err := eng.CreateTable(settingsTable, engine.TableOptions{}); err != nil {
			_ = eng.Close()
			_ = lock.Close()
			return nil, shoal.WrapError(
				shoal.ErrorInternal, "create workspace settings table", err)
		}
	}
	return &DurableStore{
		engine:           eng,
		lock:             lock,
		conditionalWrite: eng.ConditionalWrite,
	}, nil
}

// Close flushes and closes the settings engine.
func (s *DurableStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	engineErr := s.engine.Close()
	lockErr := s.lock.Close()
	if engineErr != nil || lockErr != nil {
		return shoal.WrapError(
			shoal.ErrorInternal,
			"close workspace settings storage",
			errors.Join(engineErr, lockErr),
		)
	}
	return nil
}

// Load returns the current workspace settings revision.
func (s *DurableStore) Load(
	ctx context.Context,
	workspaceID shoal.ID,
) (Settings, error) {
	if err := validateContext(ctx); err != nil {
		return Settings{}, err
	}
	if err := shoal.ValidateRequiredID("workspace ID", workspaceID); err != nil {
		return Settings{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Settings{}, shoal.NewError(
			shoal.ErrorUnavailable, "workspace settings store is closed")
	}
	record, _, found, err := s.loadLocked(workspaceID)
	if err != nil {
		return Settings{}, err
	}
	if !found {
		return Settings{}, shoal.NewError(
			shoal.ErrorNotFound, "workspace settings not found")
	}
	settings, err := settingsFromPersisted(record)
	if err != nil {
		return Settings{}, corruptSettings(err)
	}
	return settings, nil
}

// CompareAndSwap creates revision one when expectedRevision is zero, or
// replaces exactly the expected current revision. Replaying the same mutation
// ID with the same expected revision and content returns the committed result.
func (s *DurableStore) CompareAndSwap(
	ctx context.Context,
	workspaceID, owner shoal.ID,
	authorizationDomain []byte,
	expectedRevision uint64,
	mutationID shoal.ID,
	narrowing Narrowing,
) (Settings, error) {
	if err := validateContext(ctx); err != nil {
		return Settings{}, err
	}
	if err := shoal.ValidateRequiredID("workspace ID", workspaceID); err != nil {
		return Settings{}, err
	}
	if err := shoal.ValidateRequiredID("settings owner", owner); err != nil {
		return Settings{}, err
	}
	if len(authorizationDomain) == 0 ||
		len(authorizationDomain) > auth.MaxPolicyComponentBytes {
		return Settings{}, invalid("settings authorization domain is invalid")
	}
	if err := shoal.ValidateRequiredID("settings mutation ID", mutationID); err != nil {
		return Settings{}, err
	}
	normalized, err := normalizeNarrowing(narrowing)
	if err != nil {
		return Settings{}, err
	}
	if expectedRevision >= math.MaxInt64 {
		return Settings{}, invalid("expected revision exceeds the durable version bound")
	}
	digest, err := updateDigest(
		workspaceID, owner, authorizationDomain, expectedRevision, normalized)
	if err != nil {
		return Settings{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Settings{}, shoal.NewError(
			shoal.ErrorUnavailable, "workspace settings store is closed")
	}
	current, currentBytes, found, err := s.loadLocked(workspaceID)
	if err != nil {
		return Settings{}, err
	}
	if replayed, result, err := replayResult(
		current, found, owner, authorizationDomain,
		expectedRevision, mutationID, digest,
	); replayed || err != nil {
		return result, err
	}
	if found {
		if current.Owner != string(owner) {
			return Settings{}, auth.ObjectNotFound()
		}
		if !bytes.Equal(current.AuthorizationDomain, authorizationDomain) {
			return Settings{}, auth.ObjectNotFound()
		}
		if current.Revision != expectedRevision {
			return Settings{}, versionConflict()
		}
	} else if expectedRevision != 0 {
		return Settings{}, versionConflict()
	}

	revision := expectedRevision + 1
	candidate := Settings{
		WorkspaceID: workspaceID,
		SettingsID: settingsIdentity(
			workspaceID, owner, authorizationDomain),
		Owner:               owner,
		AuthorizationDomain: append([]byte(nil), authorizationDomain...),
		Revision:            revision,
		LastMutationID:      mutationID,
		Narrowing:           normalized,
	}
	record, err := persistedFromSettings(candidate, digest)
	if err != nil {
		return Settings{}, err
	}
	encoded, err := encodeSettingsRecord(record)
	if err != nil {
		return Settings{}, shoal.WrapError(
			shoal.ErrorInternal, "encode workspace settings", err)
	}
	mutation, err := cclient.NewMutation(settingsRow(workspaceID))
	if err != nil {
		return Settings{}, shoal.WrapError(
			shoal.ErrorInternal, "create workspace settings mutation", err)
	}
	mutation.Put(
		[]byte(settingsCF), []byte(settingsCQ), nil, int64(revision), encoded)
	condition := engine.Condition{
		ColumnFamily: []byte(settingsCF), ColumnQualifier: []byte(settingsCQ),
	}
	if found {
		condition.Kind = engine.ConditionValueEquals
		condition.Value = currentBytes
	} else {
		condition.Kind = engine.ConditionAbsent
	}
	accepted, err := s.conditionalWrite(settingsTable, []engine.ConditionalMutation{{
		Mutation: mutation, Conditions: []engine.Condition{condition},
	}})
	if err != nil {
		var writeErr error = shoal.WrapError(
			shoal.ErrorUnavailable, "write workspace settings", err)
		winner, _, winnerFound, loadErr := s.loadLocked(workspaceID)
		if loadErr == nil {
			if replayed, result, replayErr := replayResult(
				winner, winnerFound, owner, authorizationDomain,
				expectedRevision, mutationID, digest,
			); replayed {
				return result, replayErr
			}
		}
		if loadErr != nil {
			writeErr = errors.Join(writeErr, loadErr)
		}
		return Settings{}, explorer.MarkIndeterminateCommit(writeErr)
	}
	if len(accepted) != 1 {
		return Settings{}, shoal.NewError(
			shoal.ErrorInternal, "workspace settings CAS returned an invalid result")
	}
	if accepted[0] {
		return candidate.clone(), nil
	}
	winner, _, winnerFound, loadErr := s.loadLocked(workspaceID)
	if loadErr != nil {
		return Settings{}, loadErr
	}
	if replayed, result, replayErr := replayResult(
		winner, winnerFound, owner, authorizationDomain,
		expectedRevision, mutationID, digest,
	); replayed || replayErr != nil {
		return result, replayErr
	}
	return Settings{}, versionConflict()
}

func (s *DurableStore) loadLocked(
	workspaceID shoal.ID,
) (persistedSettings, []byte, bool, error) {
	return s.loadRow(workspaceID)
}

func (s *DurableStore) loadRow(
	workspaceID shoal.ID,
) (persistedSettings, []byte, bool, error) {
	var encoded []byte
	err := s.engine.LookupRows(
		settingsTable,
		[][]byte{settingsRow(workspaceID)},
		engine.ScanOptions{
			ColumnFamilies:          [][]byte{[]byte(settingsCF)},
			ColumnFamiliesInclusive: true,
		},
		func(_ int, key *iterrt.Key, value []byte) {
			if encoded == nil &&
				bytes.Equal(key.ColumnFamily, []byte(settingsCF)) &&
				bytes.Equal(key.ColumnQualifier, []byte(settingsCQ)) {
				encoded = append([]byte(nil), value...)
			}
		},
	)
	if err != nil {
		return persistedSettings{}, nil, false, shoal.WrapError(
			shoal.ErrorInternal, "read workspace settings", err)
	}
	if encoded == nil {
		return persistedSettings{}, nil, false, nil
	}
	var record persistedSettings
	if err := decodeSettingsRecord(encoded, &record); err != nil {
		return persistedSettings{}, nil, false, corruptSettings(err)
	}
	if record.WorkspaceID != string(workspaceID) {
		return persistedSettings{}, nil, false, corruptSettings(
			fmt.Errorf("workspace row identity mismatch"))
	}
	if _, err := settingsFromPersisted(record); err != nil {
		return persistedSettings{}, nil, false, corruptSettings(err)
	}
	return record, encoded, true, nil
}

func replayResult(
	record persistedSettings,
	found bool,
	owner shoal.ID,
	authorizationDomain []byte,
	expectedRevision uint64,
	mutationID shoal.ID,
	digest [sha256.Size]byte,
) (bool, Settings, error) {
	if !found {
		return false, Settings{}, nil
	}
	if record.Owner != string(owner) {
		return true, Settings{}, auth.ObjectNotFound()
	}
	if !bytes.Equal(record.AuthorizationDomain, authorizationDomain) {
		return true, Settings{}, auth.ObjectNotFound()
	}
	if record.LastMutationID != string(mutationID) {
		return false, Settings{}, nil
	}
	if record.Revision != expectedRevision+1 ||
		record.LastMutationDigest != digest {
		return true, Settings{}, shoal.NewError(
			shoal.ErrorConflict, "settings mutation ID was reused with different content")
	}
	settings, err := settingsFromPersisted(record)
	if err != nil {
		return true, Settings{}, corruptSettings(err)
	}
	return true, settings, nil
}

func settingsRow(workspaceID shoal.ID) []byte {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(workspaceID))
	return []byte("workspace/" + encoded)
}

func persistedFromSettings(
	value Settings,
	digest [sha256.Size]byte,
) (persistedSettings, error) {
	if err := validateSettings(value); err != nil {
		return persistedSettings{}, err
	}
	record := persistedSettings{
		WorkspaceID: string(value.WorkspaceID),
		SettingsID:  string(value.SettingsID),
		Owner:       string(value.Owner),
		AuthorizationDomain: append(
			[]byte(nil), value.AuthorizationDomain...),
		Revision:           value.Revision,
		LastMutationID:     string(value.LastMutationID),
		LastMutationDigest: digest,
		AllowedPresent:     value.Narrowing.AllowedOperations.Present,
		SourcesPresent:     value.Narrowing.PermittedSourceIDs.Present,
		PoliciesPresent:    value.Narrowing.PermittedPolicyIDs.Present,
		RetrievalTopK:      cloneUint32(value.Narrowing.Budgets.RetrievalTopK),
		GraphDepth:         cloneUint32(value.Narrowing.Budgets.GraphDepth),
		GraphFanout:        cloneUint32(value.Narrowing.Budgets.GraphFanout),
		GraphNodes:         cloneUint32(value.Narrowing.Budgets.GraphNodes),
		OutputBytes:        cloneUint64(value.Narrowing.Budgets.OutputBytes),
		OntologyPresent:    value.Narrowing.SelectedOntology.Present,
	}
	for _, operation := range value.Narrowing.AllowedOperations.Values {
		record.AllowedOperations = append(record.AllowedOperations, string(operation))
	}
	record.PermittedSources = cloneByteSlices(
		value.Narrowing.PermittedSourceIDs.Values)
	record.PermittedPolicies = cloneByteSlices(
		value.Narrowing.PermittedPolicyIDs.Values)
	for _, policy := range value.Narrowing.OutputPolicies {
		encoded, err := policy.Encode()
		if err != nil {
			return persistedSettings{}, err
		}
		record.OutputPolicies = append(record.OutputPolicies, encoded)
	}
	if value.Narrowing.SelectedOntology.Present {
		record.OntologySchemaID = string(
			value.Narrowing.SelectedOntology.Identity.SchemaID())
		record.OntologyVersionID = string(
			value.Narrowing.SelectedOntology.Identity.VersionID())
	}
	return record, nil
}

func settingsFromPersisted(record persistedSettings) (Settings, error) {
	operations := make([]auth.Operation, 0, len(record.AllowedOperations))
	for _, value := range record.AllowedOperations {
		operation, err := auth.ParseOperation(value)
		if err != nil {
			return Settings{}, err
		}
		operations = append(operations, operation)
	}
	outputPolicies := make([]auth.Policy, 0, len(record.OutputPolicies))
	for _, value := range record.OutputPolicies {
		policy, err := auth.DecodePolicy(value)
		if err != nil {
			return Settings{}, err
		}
		outputPolicies = append(outputPolicies, policy)
	}
	var ontologySelection OntologySelection
	if record.OntologyPresent {
		identity, err := ontology.NewOntologyIdentityFromIDs(
			shoal.ID(record.OntologySchemaID),
			shoal.ID(record.OntologyVersionID),
		)
		if err != nil {
			return Settings{}, err
		}
		ontologySelection = OntologySelection{Present: true, Identity: identity}
	}
	value := Settings{
		WorkspaceID: shoal.ID(record.WorkspaceID),
		SettingsID:  shoal.ID(record.SettingsID),
		Owner:       shoal.ID(record.Owner),
		AuthorizationDomain: append(
			[]byte(nil), record.AuthorizationDomain...),
		Revision:       record.Revision,
		LastMutationID: shoal.ID(record.LastMutationID),
		Narrowing: Narrowing{
			AllowedOperations: OperationSelection{
				Present: record.AllowedPresent, Values: operations,
			},
			PermittedSourceIDs: IDSelection{
				Present: record.SourcesPresent,
				Values:  cloneByteSlices(record.PermittedSources),
			},
			PermittedPolicyIDs: IDSelection{
				Present: record.PoliciesPresent,
				Values:  cloneByteSlices(record.PermittedPolicies),
			},
			Budgets: Budgets{
				RetrievalTopK: cloneUint32(record.RetrievalTopK),
				GraphDepth:    cloneUint32(record.GraphDepth),
				GraphFanout:   cloneUint32(record.GraphFanout),
				GraphNodes:    cloneUint32(record.GraphNodes),
				OutputBytes:   cloneUint64(record.OutputBytes),
			},
			OutputPolicies:   outputPolicies,
			SelectedOntology: ontologySelection,
		},
	}
	normalized, err := normalizeNarrowing(value.Narrowing)
	if err != nil {
		return Settings{}, err
	}
	value.Narrowing = normalized
	if err := validateSettings(value); err != nil {
		return Settings{}, err
	}
	return value, nil
}

func encodeSettingsRecord(record persistedSettings) ([]byte, error) {
	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(record); err != nil {
		return nil, err
	}
	if uint64(payload.Len()) > maxSettingsRecordBytes {
		return nil, fmt.Errorf("settings payload exceeds the durable record bound")
	}
	encoded := make([]byte, settingsEnvelopeHeader+payload.Len())
	copy(encoded, settingsRecordMagic)
	encoded[8] = settingsEnvelopeVersion
	encoded[9] = settingsRecordKind
	binary.BigEndian.PutUint64(encoded[10:18], uint64(payload.Len()))
	sum := sha256.Sum256(payload.Bytes())
	copy(encoded[18:18+sha256.Size], sum[:])
	copy(encoded[settingsEnvelopeHeader:], payload.Bytes())
	return encoded, nil
}

func decodeSettingsRecord(encoded []byte, destination *persistedSettings) error {
	if len(encoded) < settingsEnvelopeHeader {
		return fmt.Errorf("settings record envelope is truncated")
	}
	if string(encoded[:8]) != settingsRecordMagic ||
		encoded[8] != settingsEnvelopeVersion ||
		encoded[9] != settingsRecordKind {
		return fmt.Errorf("settings record envelope is invalid")
	}
	length := binary.BigEndian.Uint64(encoded[10:18])
	if length > maxSettingsRecordBytes ||
		length != uint64(len(encoded)-settingsEnvelopeHeader) {
		return fmt.Errorf("settings record payload length is invalid")
	}
	payload := encoded[settingsEnvelopeHeader:]
	sum := sha256.Sum256(payload)
	if !bytes.Equal(sum[:], encoded[18:18+sha256.Size]) {
		return fmt.Errorf("settings record checksum is invalid")
	}
	decoder := gob.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return nil
}

func updateDigest(
	workspaceID, owner shoal.ID,
	authorizationDomain []byte,
	expectedRevision uint64,
	narrowing Narrowing,
) ([sha256.Size]byte, error) {
	hash := sha256.New()
	writeDigestPart(hash, []byte("shoal-workspace-settings-update-v1"))
	writeDigestPart(hash, []byte(workspaceID))
	writeDigestPart(hash, []byte(owner))
	writeDigestPart(hash, authorizationDomain)
	writeUint64(hash, expectedRevision)
	writeBool(hash, narrowing.AllowedOperations.Present)
	for _, operation := range narrowing.AllowedOperations.Values {
		writeDigestPart(hash, []byte(operation))
	}
	writeBool(hash, narrowing.PermittedSourceIDs.Present)
	for _, value := range narrowing.PermittedSourceIDs.Values {
		writeDigestPart(hash, value)
	}
	writeBool(hash, narrowing.PermittedPolicyIDs.Present)
	for _, value := range narrowing.PermittedPolicyIDs.Values {
		writeDigestPart(hash, value)
	}
	writeOptionalUint32(hash, narrowing.Budgets.RetrievalTopK)
	writeOptionalUint32(hash, narrowing.Budgets.GraphDepth)
	writeOptionalUint32(hash, narrowing.Budgets.GraphFanout)
	writeOptionalUint32(hash, narrowing.Budgets.GraphNodes)
	writeOptionalUint64(hash, narrowing.Budgets.OutputBytes)
	for _, policy := range narrowing.OutputPolicies {
		encoded, err := policy.Encode()
		if err != nil {
			return [sha256.Size]byte{}, err
		}
		writeDigestPart(hash, encoded)
	}
	writeBool(hash, narrowing.SelectedOntology.Present)
	if narrowing.SelectedOntology.Present {
		writeDigestPart(hash, []byte(
			narrowing.SelectedOntology.Identity.SchemaID()))
		writeDigestPart(hash, []byte(
			narrowing.SelectedOntology.Identity.VersionID()))
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result, nil
}

func writeDigestPart(hash hash.Hash, value []byte) {
	writeUint64(hash, uint64(len(value)))
	_, _ = hash.Write(value)
}

func writeUint64(hash hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = hash.Write(encoded[:])
}

func writeBool(hash hash.Hash, value bool) {
	if value {
		writeUint64(hash, 1)
		return
	}
	writeUint64(hash, 0)
}

func writeOptionalUint32(hash hash.Hash, value *uint32) {
	writeBool(hash, value != nil)
	if value != nil {
		writeUint64(hash, uint64(*value))
	}
}

func writeOptionalUint64(hash hash.Hash, value *uint64) {
	writeBool(hash, value != nil)
	if value != nil {
		writeUint64(hash, *value)
	}
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return invalid("context is required")
	}
	switch err := ctx.Err(); {
	case errors.Is(err, context.Canceled):
		return shoal.WrapError(shoal.ErrorCanceled, "operation canceled", err)
	case errors.Is(err, context.DeadlineExceeded):
		return shoal.WrapError(shoal.ErrorDeadline, "operation deadline exceeded", err)
	}
	return nil
}

func corruptSettings(err error) error {
	return shoal.WrapError(
		shoal.ErrorInternal, "stored workspace settings are invalid", err)
}

func versionConflict() error {
	return shoal.NewError(
		shoal.ErrorConflict, "workspace settings revision conflict")
}

func authDenied() error {
	return shoal.NewError(shoal.ErrorUnauthorized, "authorization denied")
}
