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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/guard"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/transaction"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestStoreCASReplayAndRevocation(t *testing.T) {
	directory := t.TempDir()
	runtime := openRuntime(t, directory)
	defer func() {
		if runtime != nil {
			_ = runtime.Close()
		}
	}()
	store, err := NewStore(runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := testDescriptor("agent-\x00-\xff", 1)
	create := fleet.Mutation{RegistrationKey: "create-key", Descriptor: base}
	first, err := store.Apply(context.Background(), create)
	if err != nil {
		t.Fatal(err)
	}
	replayed := base
	replayed.UpdatedAt = replayed.UpdatedAt.Add(time.Second)
	second, err := store.Apply(context.Background(), fleet.Mutation{
		RegistrationKey: "create-key", Descriptor: replayed,
	})
	if err != nil || second.Epoch != first.Epoch {
		t.Fatalf("identical replay = %#v, %v", second, err)
	}
	divergent := replayed
	divergent.ExecutorRef = "executor-b"
	if _, err := store.Apply(context.Background(), fleet.Mutation{
		RegistrationKey: "create-key", Descriptor: divergent,
	}); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("divergent replay = %v", err)
	}

	left := base
	left.Generation = 2
	left.UpdatedAt = left.UpdatedAt.Add(2 * time.Second)
	left.LeaseExpiresAt = left.LeaseExpiresAt.Add(time.Minute)
	right := left
	right.ExecutorRef = "executor-b"
	var wait sync.WaitGroup
	results := make(chan error, 2)
	for _, mutation := range []fleet.Mutation{
		{RegistrationKey: "left", ExpectedGeneration: 1, Descriptor: left},
		{RegistrationKey: "right", ExpectedGeneration: 1, Descriptor: right},
	} {
		mutation := mutation
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, applyErr := store.Apply(context.Background(), mutation)
			results <- applyErr
		}()
	}
	wait.Wait()
	close(results)
	successes, conflicts := 0, 0
	for applyErr := range results {
		if applyErr == nil {
			successes++
		} else if shoal.IsErrorCode(applyErr, shoal.ErrorConflict) {
			conflicts++
		} else {
			t.Fatalf("competing mutation = %v", applyErr)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
	current, err := store.Get(context.Background(), base.ID)
	if err != nil || current.Descriptor.Generation != 2 {
		t.Fatalf("current = %#v, %v", current, err)
	}
	if err := runtime.Recover(context.Background()); err != nil {
		t.Fatalf("recover after conflict = %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	runtime = openRuntime(t, directory)
	store, _ = NewStore(runtime, nil)
	current, err = store.Get(context.Background(), base.ID)
	if err != nil || current.Descriptor.Generation != 2 {
		t.Fatalf("post-conflict restart = %#v, %v", current, err)
	}
	revoked := current.Descriptor
	revoked.Generation = 3
	revoked.UpdatedAt = revoked.UpdatedAt.Add(time.Second)
	revoked.RevokedAt = revoked.UpdatedAt
	if _, err := store.Apply(context.Background(), fleet.Mutation{
		RegistrationKey: "revoke", ExpectedGeneration: 2, Descriptor: revoked,
	}); err != nil {
		t.Fatal(err)
	}
	listed, err := store.ListPage(
		context.Background(), "", fleet.DefaultListPageSize)
	if err != nil || len(listed.Items) != 1 ||
		listed.Items[0].Stored.Descriptor.ID != base.ID ||
		listed.NextCursor != "" {
		t.Fatalf("listed = %#v, %v", listed, err)
	}
}

func TestStoreRestartPersistence(t *testing.T) {
	directory := t.TempDir()
	runtime := openRuntime(t, directory)
	store, _ := NewStore(runtime, nil)
	descriptor := testDescriptor("persistent-agent", 1)
	if _, err := store.Apply(context.Background(), fleet.Mutation{
		RegistrationKey: "persistent-create", Descriptor: descriptor,
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	runtime = openRuntime(t, directory)
	defer runtime.Close()
	store, _ = NewStore(runtime, nil)
	restarted, err := store.Get(context.Background(), descriptor.ID)
	if err != nil || restarted.Descriptor.Generation != 1 ||
		restarted.Descriptor.ID != descriptor.ID {
		t.Fatalf("restarted = %#v, %v", restarted, err)
	}
}

func TestStoreListPageBoundsDescriptorReads(t *testing.T) {
	runtime := openRuntime(t, t.TempDir())
	defer runtime.Close()
	store, err := NewStore(runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[shoal.ID]bool{}
	for index := 0; index < 3; index++ {
		id := shoal.ID(fmt.Sprintf("page-agent-%d", index))
		descriptor := testDescriptor(id, 1)
		if _, err := store.Apply(context.Background(), fleet.Mutation{
			RegistrationKey: shoal.ID(fmt.Sprintf("page-key-%d", index)),
			Descriptor:      descriptor,
		}); err != nil {
			t.Fatal(err)
		}
		want[id] = true
	}
	cursor := ""
	got := map[shoal.ID]bool{}
	for {
		page, err := store.ListPage(context.Background(), cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Items) > 1 {
			t.Fatalf("page returned %d descriptors", len(page.Items))
		}
		for _, item := range page.Items {
			got[item.Stored.Descriptor.ID] = true
			if item.Cursor == "" {
				t.Fatal("page item omitted continuation position")
			}
		}

		if page.NextCursor == "" {
			break
		}
		if page.NextCursor == cursor {
			t.Fatal("fleet list cursor did not advance")
		}
		cursor = page.NextCursor
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paged IDs = %#v, want %#v", got, want)
	}
}

type blockingReconciliationRuntime struct {
	readCount int
}

func (r *blockingReconciliationRuntime) Publish(
	context.Context, explorercoord.Request,
) (explorercoord.Result, error) {
	return explorercoord.Result{}, explorercoord.ErrIndeterminatePublication
}

func (r *blockingReconciliationRuntime) ReadEntity(
	ctx context.Context,
	_ guard.Entity,
) (*guard.Head, *guard.Pending, error) {
	r.readCount++
	if r.readCount <= 3 {
		return nil, nil, guard.ErrNotFound
	}
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func (*blockingReconciliationRuntime) ReadCommittedCell(
	context.Context, string, []byte, []byte, []byte, []byte,
	coordination.Epoch,
) (explorercoord.CommittedCell, bool, error) {
	return explorercoord.CommittedCell{}, false,
		errors.New("unexpected committed cell read")
}

func TestStoreBoundsAmbiguousPublicationReconciliation(t *testing.T) {
	runtime := &blockingReconciliationRuntime{}
	store, err := NewStore(runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.reconciliationTimeout = 10 * time.Millisecond
	started := time.Now()
	_, err = store.Apply(context.Background(), fleet.Mutation{
		RegistrationKey: "bounded-reconciliation",
		Descriptor:      testDescriptor("bounded-agent", 1),
	})
	if err == nil {
		t.Fatal("indeterminate publication unexpectedly succeeded")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("reconciliation exceeded its bound: %v", elapsed)
	}
}

type blockingReadbackRuntime struct {
	blockingReconciliationRuntime
}

func (*blockingReadbackRuntime) Publish(
	context.Context, explorercoord.Request,
) (explorercoord.Result, error) {
	return explorercoord.Result{}, nil
}

func TestStoreMarksUnresolvedPostPublishReadbackIndeterminate(t *testing.T) {
	runtime := &blockingReadbackRuntime{}
	store, err := NewStore(runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	store.reconciliationTimeout = 10 * time.Millisecond
	_, err = store.Apply(context.Background(), fleet.Mutation{
		RegistrationKey: "bounded-readback",
		Descriptor:      testDescriptor("readback-agent", 1),
	})
	if !explorer.IsIndeterminateCommit(err) {
		t.Fatalf("post-publish readback error = %v", err)
	}
}

func openRuntime(t *testing.T, directory string) *explorercoord.Runtime {
	t.Helper()
	runtime, err := explorercoord.Open(explorercoord.Config{
		Directory: directory, Domain: coordination.DomainID("fleet-registry"),
		Owner: coordination.OwnerID("fleet-worker"),
		Authority: transaction.Authority{
			Generation: 1, Fence: 1, Holder: coordination.OwnerID("fleet-process"),
			Mode:                coordination.WriterModeEmbeddedPrimary,
			RetentionGeneration: 1, HistoryFloor: 1,
		},
		PhysicalTables: []string{PhysicalTable()}, Lease: time.Minute,
		RecoveryLimit: 16, RecoveryMaxPages: 64,
		RetryBackoff: time.Nanosecond, RecoveryBackoff: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func testDescriptor(id shoal.ID, generation int64) fleet.Descriptor {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	return fleet.Descriptor{
		ID: id, Generation: generation, Subject: "subject", Actor: "actor",
		AuthorizationDomain: []byte("domain"),
		Scopes:              []fleet.Scope{{SourceID: []byte("source"), PolicyID: []byte("policy")}},
		ExecutorRef:         "executor-a",
		Capabilities: []fleet.Capability{{
			Name: "search", Actions: []fleet.Action{{
				Name: "query", InputSchema: json.RawMessage(`{"type":"object"}`),
				OutputSchema: json.RawMessage(`{"type":"object"}`),
			}},
		}},
		LeaseExpiresAt: now.Add(time.Hour), UpdatedAt: now,
	}
}
