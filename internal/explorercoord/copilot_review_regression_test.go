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

package explorercoord

import (
	"context"
	"errors"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/allocator"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/guard"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/transaction"
)

func TestRuntimeRejectsUnconfiguredPhysicalTablesBeforeIntentPersistence(t *testing.T) {
	config := testRuntimeConfig(t, testDirectory(t))
	runtime, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	intent := testIntent(
		t,
		config.Domain,
		"unconfigured-table",
		"token",
		"value",
		guard.ModeAbsentOrIdentical,
		0,
		coordination.Digest{},
	)
	intent.Cells[0].Table = DefaultCoordinationTable
	if _, err := runtime.Publish(
		context.Background(),
		Request{Intent: intent},
	); !errors.Is(err, transaction.ErrInvalid) ||
		errors.Is(err, ErrIndeterminatePublication) {
		t.Fatalf("publish to coordination table = %v", err)
	}
	if pending, err := runtime.PendingPublications(
		context.Background(),
	); err != nil || pending {
		t.Fatalf("pending publication after table rejection = %v, %v", pending, err)
	}

	record := testRecordPublication("document", "revision", "value", nil)
	record.Table = DefaultCoordinationTable
	if _, err := runtime.PublishRecord(
		context.Background(),
		record,
	); !errors.Is(err, transaction.ErrInvalid) {
		t.Fatalf("record publication to coordination table = %v", err)
	}
	if pending, err := runtime.PendingPublications(
		context.Background(),
	); err != nil || pending {
		t.Fatalf("pending record publication after table rejection = %v, %v", pending, err)
	}
}

func TestRuntimeRejectsCoordinationTableAsPhysicalTargetBeforeOpen(t *testing.T) {
	config := testRuntimeConfig(t, testDirectory(t))
	config.PhysicalTables = []string{DefaultCoordinationTable}
	if runtime, err := Open(config); !errors.Is(err, transaction.ErrInvalid) {
		if runtime != nil {
			_ = runtime.Close()
		}
		t.Fatalf("open with coordination physical target = %v", err)
	}

	config.PhysicalTables = []string{"records"}
	runtime, err := Open(config)
	if err != nil {
		t.Fatalf("open after rejected configuration = %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAmbiguousIntentWriteAndFailedReadbackIsIndeterminate(t *testing.T) {
	config := testRuntimeConfig(t, testDirectory(t))
	runtime, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	faults := &ambiguousIntentStore{
		EngineStore: runtime.store,
		ambiguous:   true,
	}
	runtime.intents.store = faults
	intent := testIntent(
		t,
		config.Domain,
		"ambiguous-intent",
		"token",
		"value",
		guard.ModeAbsentOrIdentical,
		0,
		coordination.Digest{},
	)
	if _, err := runtime.Publish(
		context.Background(),
		Request{Intent: intent},
	); !errors.Is(err, ErrIndeterminatePublication) ||
		!errors.Is(err, allocator.ErrConditionalUnknown) {
		t.Fatalf("ambiguous intent publication = %v", err)
	}

	runtime.intents.store = runtime.store
	if pending, err := runtime.PendingPublications(
		context.Background(),
	); err != nil || !pending {
		t.Fatalf("pending ambiguous intent = %v, %v", pending, err)
	}
	if err := runtime.Recover(context.Background()); err != nil {
		t.Fatalf("recover ambiguous intent = %v", err)
	}
	if pending, err := runtime.PendingPublications(
		context.Background(),
	); err != nil || pending {
		t.Fatalf("pending publication after recovery = %v, %v", pending, err)
	}
}

func TestIntentLoadRejectsNoncanonicalDefaultFields(t *testing.T) {
	eng, store := openTestEngineStore(t, "coord")
	defer eng.Close()
	domain := coordination.DomainID("domain")
	intents, err := NewIntentStore(domain, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	intent := testIntent(
		t,
		domain,
		"noncanonical",
		"token",
		"value",
		guard.ModeAbsentOrIdentical,
		0,
		coordination.Digest{},
	)
	intent.Cells[0].CopyGeneration = 0
	intent.Guards[0].DesiredState = 0
	intent.Guards[0].RetirementGeneration = 0
	normalized, _, digest, err := canonicalIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Cells[0].CopyGeneration == 0 ||
		normalized.Guards[0].DesiredState == 0 ||
		normalized.Guards[0].RetirementGeneration == 0 {
		t.Fatal("intent defaults were not normalized")
	}
	txn, err := DeriveTXN(domain, intent.Operation, intent.Token)
	if err != nil {
		t.Fatal(err)
	}
	record := storedIntent{
		Version:       intentVersion,
		Domain:        append(coordination.DomainID(nil), domain...),
		TXN:           append(coordination.TXN(nil), txn...),
		LogicalDigest: digest,
		Intent:        intent,
	}
	encoded, err := encodeStoredIntent(record)
	if err != nil {
		t.Fatal(err)
	}
	coordinate := intents.intentCoordinate(txn)
	status, err := store.CompareAndMutate(
		context.Background(),
		allocator.Mutation{
			Row:        coordinate.Row,
			Conditions: []allocator.Condition{{Coordinate: coordinate, Absent: true}},
			Updates: []allocator.Update{{
				Coordinate: coordinate,
				Value:      encoded,
				Timestamp:  intentVersion,
			}},
		},
	)
	if err != nil || status != allocator.StatusAccepted {
		t.Fatalf("write noncanonical intent = %v, %v", status, err)
	}
	if _, err := intents.Load(
		context.Background(),
		txn,
	); !errors.Is(err, transaction.ErrInternal) {
		t.Fatalf("load noncanonical intent = %v", err)
	}
}

func TestRecordBindingFailuresDoNotLeaveRecoverableOrphans(t *testing.T) {
	t.Run("invalid key", func(t *testing.T) {
		config := testRuntimeConfig(t, testDirectory(t))
		runtime, err := Open(config)
		if err != nil {
			t.Fatal(err)
		}
		defer runtime.Close()

		request := testRecordPublication("document", "revision", "value", nil)
		request.RecordKey = make([]byte, coordination.MaxOpaqueIDBytes+1)
		if _, err := runtime.PublishRecord(
			context.Background(),
			request,
		); !errors.Is(err, transaction.ErrInvalid) {
			t.Fatalf("oversized record key = %v", err)
		}
		if pending, err := runtime.PendingPublications(
			context.Background(),
		); err != nil || pending {
			t.Fatalf("pending publication after invalid key = %v, %v", pending, err)
		}
	})

	t.Run("definite conflict", func(t *testing.T) {
		config := testRuntimeConfig(t, testDirectory(t))
		runtime, err := Open(config)
		if err != nil {
			t.Fatal(err)
		}
		first := testRecordPublication("document-one", "revision-one", "one", nil)
		if _, err := runtime.PublishRecord(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		second := testRecordPublication("document-two", "revision-two", "two", nil)
		second.RecordKey = append([]byte(nil), first.RecordKey...)
		if _, err := runtime.PublishRecord(
			context.Background(),
			second,
		); !errors.Is(err, transaction.ErrConflict) ||
			errors.Is(err, ErrIndeterminatePublication) {
			t.Fatalf("conflicting record binding = %v", err)
		}
		if pending, err := runtime.PendingPublications(
			context.Background(),
		); err != nil || pending {
			t.Fatalf("pending publication after binding conflict = %v, %v", pending, err)
		}
		if err := runtime.Close(); err != nil {
			t.Fatal(err)
		}

		reopened, err := Open(config)
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		head, err := reopened.RecordHead(
			context.Background(),
			second.EntityKind,
			second.EntityID,
		)
		if err != nil || head != nil {
			t.Fatalf("orphaned binding conflict was recovered = %#v, %v", head, err)
		}
	})
}

type ambiguousIntentStore struct {
	*EngineStore
	ambiguous    bool
	failReadback bool
}

func (s *ambiguousIntentStore) CompareAndMutate(
	ctx context.Context,
	mutation allocator.Mutation,
) (allocator.Status, error) {
	status, err := s.EngineStore.CompareAndMutate(ctx, mutation)
	if !s.ambiguous {
		return status, err
	}
	s.ambiguous = false
	if err != nil || status != allocator.StatusAccepted {
		return status, err
	}
	s.failReadback = true
	return allocator.StatusUnknown, allocator.ErrConditionalUnknown
}

func (s *ambiguousIntentStore) ReadExact(
	ctx context.Context,
	coordinates []allocator.Coordinate,
) ([]allocator.Cell, error) {
	if s.failReadback {
		s.failReadback = false
		return nil, errors.New("injected intent readback failure")
	}
	return s.EngineStore.ReadExact(ctx, coordinates)
}
