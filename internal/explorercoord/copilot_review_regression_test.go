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
	"bytes"
	"context"
	"errors"
	"sort"
	"testing"
	"time"

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

func TestPendingCandidateIndexIgnoresTombstonedHistory(t *testing.T) {
	eng, store := openTestEngineStore(t, "coord")
	defer eng.Close()
	domain := coordination.DomainID("domain")
	intents, err := NewIntentStore(domain, nil, store)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]storedIntent, 0, 2)
	for _, token := range []string{"first", "second"} {
		record, _, err := intents.Put(
			context.Background(),
			testIntent(
				t,
				domain,
				"pending-page",
				token,
				token,
				guard.ModeAbsentOrIdentical,
				0,
				coordination.Digest{},
			),
		)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(
			intents.intentRow(records[i].TXN),
			intents.intentRow(records[j].TXN),
		) < 0
	})
	if err := intents.Settle(
		context.Background(),
		records[0].TXN,
		records[0].LogicalDigest,
	); err != nil {
		t.Fatal(err)
	}
	intents.store = &scanFailIntentStore{EngineStore: store}
	candidates, next, err := intents.Candidates(context.Background(), nil, 1)
	if err != nil || len(candidates) != 1 ||
		!bytes.Equal(candidates[0], records[1].TXN) ||
		len(next) != 0 {
		t.Fatalf("live pending index = %#v, %x, %v", candidates, next, err)
	}
}

func TestPendingPublicationsUsesLoadedLiveIndex(t *testing.T) {
	config := testRuntimeConfig(t, testDirectory(t))
	runtime, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	intent := testIntent(
		t,
		config.Domain,
		"completed-history",
		"token",
		"value",
		guard.ModeAbsentOrIdentical,
		0,
		coordination.Digest{},
	)
	if _, err := runtime.Publish(
		context.Background(),
		Request{Intent: intent},
	); err != nil {
		t.Fatal(err)
	}
	runtime.intents.store = &scanFailIntentStore{EngineStore: runtime.store}
	if pending, err := runtime.PendingPublications(
		context.Background(),
	); err != nil || pending {
		t.Fatalf("indexed pending check = %v, %v", pending, err)
	}
}

func TestRecoveryAndPublicationUseSameLockOrder(t *testing.T) {
	config := testRuntimeConfig(t, testDirectory(t))
	runtime, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}

	runtime.recoveryMu.Lock()
	recoverDone := make(chan error, 1)
	go func() {
		_, err := runtime.RecoverPage(context.Background())
		recoverDone <- err
	}()
	time.Sleep(20 * time.Millisecond)

	publishDone := make(chan error, 1)
	go func() {
		_, err := runtime.PublishRecord(
			context.Background(),
			testRecordPublication("document", "revision", "value", nil),
		)
		publishDone <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.mu.TryLock() {
		runtime.mu.Unlock()
		if time.Now().After(deadline) {
			runtime.recoveryMu.Unlock()
			t.Fatal("publication did not acquire the runtime read lock")
		}
		time.Sleep(time.Millisecond)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- runtime.Close()
	}()
	time.Sleep(20 * time.Millisecond)
	runtime.recoveryMu.Unlock()

	for name, done := range map[string]<-chan error{
		"recovery": recoverDone,
		"publish":  publishDone,
		"close":    closeDone,
	} {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("%s deadlocked", name)
		}
	}
}

func TestRecoveryRejectsPendingIntentOutsideCurrentTableAllowlist(t *testing.T) {
	config := testRuntimeConfig(t, testDirectory(t))
	config.PhysicalTables = []string{"legacy"}
	fired := false
	config.testStageHook = func(stage recoveryStage) error {
		if stage == recoveryStageIntent && !fired {
			fired = true
			return context.Canceled
		}
		return nil
	}
	runtime, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	intent := testIntent(
		t,
		config.Domain,
		"legacy-table",
		"token",
		"value",
		guard.ModeAbsentOrIdentical,
		0,
		coordination.Digest{},
	)
	intent.Cells[0].Table = "legacy"
	if _, err := runtime.Publish(
		context.Background(),
		Request{Intent: intent},
	); !errors.Is(err, ErrIndeterminatePublication) {
		t.Fatalf("stage pending legacy intent = %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}

	config.PhysicalTables = []string{"records"}
	config.testStageHook = nil
	if reopened, err := Open(config); !errors.Is(err, transaction.ErrInvalid) {
		if reopened != nil {
			_ = reopened.Close()
		}
		t.Fatalf("recover intent outside current allowlist = %v", err)
	}
}

func TestCommittedProofPreservesOperationalReadErrors(t *testing.T) {
	config := testRuntimeConfig(t, testDirectory(t))
	runtime, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	intent := testIntent(
		t,
		config.Domain,
		"committed-proof",
		"token",
		"value",
		guard.ModeAbsentOrIdentical,
		0,
		coordination.Digest{},
	)
	result, err := runtime.Publish(
		context.Background(),
		Request{Intent: intent},
	)
	if err != nil {
		t.Fatal(err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtime.proofForEpoch(
		canceled,
		result.Epoch,
		make(map[coordination.Epoch]publicationProof),
	); !errors.Is(err, context.Canceled) ||
		errors.Is(err, transaction.ErrInternal) {
		t.Fatalf("canceled outcome read = %v", err)
	}

	runtime.intents.store = &readFailIntentStore{
		EngineStore: runtime.store,
		err:         context.Canceled,
	}
	if _, err := runtime.proofForEpoch(
		context.Background(),
		result.Epoch,
		make(map[coordination.Epoch]publicationProof),
	); !errors.Is(err, context.Canceled) ||
		errors.Is(err, transaction.ErrInternal) {
		t.Fatalf("canceled committed intent read = %v", err)
	}
}

func TestCommittedRequiresImmutableEpochOutcome(t *testing.T) {
	config := testRuntimeConfig(t, testDirectory(t))
	runtime, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	intent := testIntent(
		t,
		config.Domain,
		"committed-outcome",
		"token",
		"value",
		guard.ModeAbsentOrIdentical,
		0,
		coordination.Digest{},
	)
	result, err := runtime.Publish(
		context.Background(),
		Request{Intent: intent},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, committed, err := runtime.Committed(
		context.Background(),
		result.TXN,
		result.LogicalDigest,
	); err != nil || !committed {
		t.Fatalf("initial committed proof = %v, %v", committed, err)
	}

	row, err := coordination.OutcomeRow(config.Domain, result.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	coordinate := allocator.Coordinate{
		Row:       row,
		Family:    []byte("o"),
		Qualifier: []byte("terminal"),
	}
	cells, err := runtime.store.ReadExact(
		context.Background(),
		[]allocator.Coordinate{coordinate},
	)
	if err != nil || len(cells) != 1 {
		t.Fatalf("read outcome = %#v, %v", cells, err)
	}
	status, err := runtime.store.CompareAndMutate(
		context.Background(),
		allocator.Mutation{
			Row: row,
			Conditions: []allocator.Condition{{
				Coordinate:   coordinate,
				Value:        cells[0].Value,
				Timestamp:    cells[0].Timestamp,
				TimestampSet: true,
			}},
			Updates: []allocator.Update{{
				Coordinate: coordinate,
				Delete:     true,
				Timestamp:  cells[0].Timestamp + 1,
			}},
		},
	)
	if err != nil || status != allocator.StatusAccepted {
		t.Fatalf("delete immutable outcome = %v, %v", status, err)
	}
	if _, committed, err := runtime.Committed(
		context.Background(),
		result.TXN,
		result.LogicalDigest,
	); committed || !errors.Is(err, transaction.ErrInternal) {
		t.Fatalf("committed proof without outcome = %v, %v", committed, err)
	}
}

func TestGenericPublishSerializesPendingRegistrationWithRecovery(t *testing.T) {
	config := testRuntimeConfig(t, testDirectory(t))
	fired := false
	config.testStageHook = func(stage recoveryStage) error {
		if stage == recoveryStageIntent && !fired {
			fired = true
			return context.Canceled
		}
		return nil
	}
	runtime, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	blocking := &blockingIntentStore{
		EngineStore: runtime.store,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	runtime.intents.store = blocking
	intent := testIntent(
		t,
		config.Domain,
		"generic-recovery-race",
		"token",
		"value",
		guard.ModeAbsentOrIdentical,
		0,
		coordination.Digest{},
	)
	publishDone := make(chan error, 1)
	go func() {
		_, err := runtime.Publish(
			context.Background(),
			Request{Intent: intent},
		)
		publishDone <- err
	}()
	select {
	case <-blocking.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("generic publication did not reach intent persistence")
	}

	recoverDone := make(chan error, 1)
	go func() {
		recoverDone <- runtime.Recover(context.Background())
	}()
	var earlyRecovery error
	recoveredEarly := false
	select {
	case earlyRecovery = <-recoverDone:
		recoveredEarly = true
	case <-time.After(100 * time.Millisecond):
	}
	close(blocking.release)
	if err := <-publishDone; !errors.Is(err, ErrIndeterminatePublication) {
		t.Fatalf("staged generic publication = %v", err)
	}
	if recoveredEarly {
		t.Fatalf("recovery passed index-before-intent window: %v", earlyRecovery)
	}
	if err := <-recoverDone; err != nil {
		t.Fatalf("recover serialized generic publication = %v", err)
	}
	digest, err := LogicalDigest(intent)
	if err != nil {
		t.Fatal(err)
	}
	txn, err := DeriveTXN(config.Domain, intent.Operation, intent.Token)
	if err != nil {
		t.Fatal(err)
	}
	if _, committed, err := runtime.Committed(
		context.Background(),
		txn,
		digest,
	); err != nil || !committed {
		t.Fatalf("serialized recovered publication = %v, %v", committed, err)
	}
}

func TestSuccessfulRecoveryRefreshesPendingCache(t *testing.T) {
	config := testRuntimeConfig(t, testDirectory(t))
	runtime, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	txn, err := DeriveTXN(config.Domain, []byte("stale-cache"), []byte("token"))
	if err != nil {
		t.Fatal(err)
	}
	runtime.intents.markPending(txn)
	if pending, err := runtime.PendingPublications(
		context.Background(),
	); err != nil || !pending {
		t.Fatalf("seed stale pending cache = %v, %v", pending, err)
	}
	if err := runtime.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pending, err := runtime.PendingPublications(
		context.Background(),
	); err != nil || pending {
		t.Fatalf("refreshed pending cache = %v, %v", pending, err)
	}
}

func TestCommittedScanDefaultWorkBoundSupportsMaximumLimit(t *testing.T) {
	config := testRuntimeConfig(t, testDirectory(t))
	runtime, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	page, err := runtime.ScanCommitted(
		context.Background(),
		CommittedScanRequest{
			Table:     "records",
			RowPrefix: []byte("missing/"),
			Family:    []byte("record"),
			Qualifier: []byte("v1"),
			Limit:     MaxCommittedScanLimit,
		},
	)
	if err != nil || len(page.Cells) != 0 {
		t.Fatalf("maximum default committed scan = %#v, %v", page, err)
	}
}

func TestCommittedScanRejectsOversizedCursors(t *testing.T) {
	config := testRuntimeConfig(t, testDirectory(t))
	runtime, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	oversized := bytes.Repeat([]byte{'x'}, coordination.MaxCoordinateBytes+1)
	for name, applyCursor := range map[string]func(*CommittedScanRequest){
		"start row": func(request *CommittedScanRequest) {
			request.StartRow = oversized
		},
		"exclusive start row": func(request *CommittedScanRequest) {
			request.StartAfterRow = oversized
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := CommittedScanRequest{
				Table:     "records",
				RowPrefix: []byte("x"),
				Family:    []byte("record"),
				Qualifier: []byte("v1"),
				Limit:     1,
			}
			applyCursor(&request)
			if _, err := runtime.ScanCommitted(
				context.Background(),
				request,
			); !errors.Is(err, transaction.ErrInvalid) {
				t.Fatalf("oversized committed scan cursor = %v", err)
			}
		})
	}
}

func TestRecoveryRestoresAmbiguousRecordAttemptBinding(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "not applied", true: "applied"}[applied], func(t *testing.T) {
			config := testRuntimeConfig(t, testDirectory(t))
			runtime, err := Open(config)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()
			faults := &ambiguousAttemptStore{
				EngineStore: runtime.store,
				apply:       applied,
			}
			runtime.intents.store = faults
			request := testRecordPublication(
				"document",
				"revision",
				"value",
				nil,
			)
			if _, err := runtime.PublishRecord(
				context.Background(),
				request,
			); !errors.Is(err, ErrIndeterminatePublication) ||
				!errors.Is(err, allocator.ErrConditionalUnknown) {
				t.Fatalf("ambiguous record binding = %v", err)
			}

			runtime.intents.store = runtime.store
			if err := runtime.Recover(context.Background()); err != nil {
				t.Fatalf("recover ambiguous record binding = %v", err)
			}
			attempt, err := runtime.RecordAttempt(context.Background(), request)
			if err != nil || attempt == nil ||
				!bytes.Equal(attempt.Value, request.Value) ||
				attempt.ExpectedEpoch != request.ExpectedEpoch ||
				attempt.ExpectedDigest != request.ExpectedDigest {
				t.Fatalf("recovered record attempt = %#v, %v", attempt, err)
			}
			head, err := runtime.RecordHead(
				context.Background(),
				request.EntityKind,
				request.EntityID,
			)
			if err != nil || head == nil {
				t.Fatalf("recovered record head = %#v, %v", head, err)
			}
			retry, err := runtime.PublishRecord(context.Background(), request)
			if err != nil || !retry.Unchanged || retry.Epoch != head.Epoch {
				t.Fatalf("exact retry after binding recovery = %#v, %v", retry, err)
			}
			after, err := runtime.CurrentHead(context.Background())
			if err != nil || after.Frontier != head.Epoch {
				t.Fatalf("retry duplicated recovered publication = %#v, %v", after, err)
			}
		})
	}
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
	if !s.ambiguous || !bytes.HasPrefix(mutation.Row, intentRowMagic) {
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

type scanFailIntentStore struct {
	*EngineStore
}

func (s *scanFailIntentStore) ScanPrefixFrom(
	context.Context,
	[]byte,
	[]byte,
	[]byte,
	[]byte,
	[]byte,
	int,
) ([]allocator.Cell, error) {
	return nil, errors.New("unexpected historical pending scan")
}

type readFailIntentStore struct {
	*EngineStore
	err error
}

func (s *readFailIntentStore) ReadExact(
	context.Context,
	[]allocator.Coordinate,
) ([]allocator.Cell, error) {
	return nil, s.err
}

type blockingIntentStore struct {
	*EngineStore
	entered chan struct{}
	release chan struct{}
	blocked bool
}

func (s *blockingIntentStore) CompareAndMutate(
	ctx context.Context,
	mutation allocator.Mutation,
) (allocator.Status, error) {
	if !s.blocked && bytes.HasPrefix(mutation.Row, intentRowMagic) {
		s.blocked = true
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return allocator.StatusUnknown, ctx.Err()
		}
	}
	return s.EngineStore.CompareAndMutate(ctx, mutation)
}

type ambiguousAttemptStore struct {
	*EngineStore
	apply        bool
	ambiguous    bool
	failReadback bool
}

func (s *ambiguousAttemptStore) CompareAndMutate(
	ctx context.Context,
	mutation allocator.Mutation,
) (allocator.Status, error) {
	if s.ambiguous || !bytes.HasPrefix(mutation.Row, attemptRowMagic) {
		return s.EngineStore.CompareAndMutate(ctx, mutation)
	}
	s.ambiguous = true
	s.failReadback = true
	if s.apply {
		status, err := s.EngineStore.CompareAndMutate(ctx, mutation)
		if err != nil || status != allocator.StatusAccepted {
			return status, err
		}
	}
	return allocator.StatusUnknown, allocator.ErrConditionalUnknown
}

func (s *ambiguousAttemptStore) ReadExact(
	ctx context.Context,
	coordinates []allocator.Coordinate,
) ([]allocator.Cell, error) {
	if s.failReadback && len(coordinates) == 1 &&
		bytes.HasPrefix(coordinates[0].Row, attemptRowMagic) {
		s.failReadback = false
		return nil, errors.New("injected attempt binding readback failure")
	}
	return s.EngineStore.ReadExact(ctx, coordinates)
}
