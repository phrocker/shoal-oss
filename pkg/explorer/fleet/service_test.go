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

package fleet

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestGenerationValidationRejectsIncrementOverflow(t *testing.T) {
	if err := validateGeneration(math.MaxInt64); !shoal.IsErrorCode(
		err, shoal.ErrorInvalidArgument,
	) {
		t.Fatalf("register maximum generation error = %v", err)
	}
	if err := validateMutationIdentity(
		"agent", "registration", math.MaxInt64,
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("mutation maximum generation error = %v", err)
	}
}

func TestServiceListPaginatesAfterIncrementalFiltering(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryStore()
	for _, value := range []struct {
		id     shoal.ID
		source string
	}{
		{id: "agent-a", source: "source-b"},
		{id: "agent-b", source: "source-a"},
		{id: "agent-c", source: "source-a"},
	} {
		descriptor := Descriptor{
			ID: value.id, Generation: 1,
			Subject: "owner", Actor: "owner-actor",
			AuthorizationDomain: []byte("domain"),
			Scopes: []Scope{{
				SourceID: []byte(value.source), PolicyID: []byte("policy"),
			}},
			ExecutorRef: "exec",
			Capabilities: []Capability{{
				Name: "search", Actions: []Action{{
					Name:         "query",
					InputSchema:  json.RawMessage(`{"type":"object"}`),
					OutputSchema: json.RawMessage(`{"type":"object"}`),
				}},
			}},
			LeaseExpiresAt: now.Add(time.Hour), UpdatedAt: now,
		}
		store.records[value.id] = Stored{Descriptor: descriptor}
	}
	service, err := NewService(Config{
		Store: store, Resolver: authority.Resolver(),
		Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": struct{}{}},
		Clock:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	list := func(requestID shoal.ID, cursor string) ListPage {
		t.Helper()
		decision := testDecision(
			t, "owner", "owner-actor", string(requestID),
			[][]byte{[]byte("source-a")},
		)
		page, listErr := service.List(
			bindDecision(t, authority, decision),
			ListRequest{
				Context: requestContext(now, string(requestID)),
				Limit:   1, Cursor: cursor,
			},
		)
		if listErr != nil {
			t.Fatal(listErr)
		}
		return page
	}
	first := list("list-1", "")
	if len(first.Descriptors) != 1 ||
		first.Descriptors[0].ID != "agent-b" ||
		first.NextCursor == "" {
		t.Fatalf("first filtered page = %#v", first)
	}
	second := list("list-2", first.NextCursor)
	if len(second.Descriptors) != 1 ||
		second.Descriptors[0].ID != "agent-c" ||
		second.NextCursor != "" {
		t.Fatalf("second filtered page = %#v", second)
	}
}

func TestServiceAuthorizationDelegationLeaseAndRevocation(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryStore()
	recorder := &memoryRecorder{}
	service, err := NewService(Config{
		Store: store, Resolver: authority.Resolver(), Recorder: recorder,
		Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": struct{}{}}, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	owner := testDecision(t, "owner", "owner-actor", "request-1",
		[][]byte{[]byte("source-a"), []byte("source-b")})
	ctx := bindDecision(t, authority, owner)
	parentRequest := registerRequest(now, "request-1", "parent", "", "source-a")
	parent, err := service.Register(ctx, parentRequest)
	if err != nil {
		t.Fatal(err)
	}
	if parent.Subject != "owner" || parent.Actor != "owner-actor" {
		t.Fatalf("trusted actor projection = %#v", parent)
	}

	delegationDecision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "owner", Actor: "delegating-actor", AuthorizationDomain: []byte("domain"),
		AllowedOperations:  []auth.Operation{auth.OperationAgentRegister, auth.OperationDelegate},
		PermittedSourceIDs: [][]byte{[]byte("source-a")},
		PermittedPolicyIDs: [][]byte{[]byte("policy")}, PolicyGeneration: 1,
		AuthenticationExpires: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
		RequestID:             "request-delegate",
	})
	if err != nil {
		t.Fatal(err)
	}
	delegationCtx := bindDecision(t, authority, delegationDecision)
	overlongChild := registerRequest(now, "request-delegate", "overlong", "parent", "source-a")
	overlongChild.Spec.LeaseExpiresAt = parent.LeaseExpiresAt.Add(time.Second)
	if _, err := service.Register(delegationCtx, overlongChild); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("delegated lease widening = %v", err)
	}
	childRequest := registerRequest(now, "request-delegate", "child", "parent", "source-a")
	child, err := service.Register(delegationCtx, childRequest)
	if err != nil {
		t.Fatal(err)
	}
	if child.ParentID != parent.ID {
		t.Fatalf("child parent = %q", child.ParentID)
	}
	childDecision := testDecision(t, "owner", "delegating-actor", "request-2",
		[][]byte{[]byte("source-a"), []byte("source-b")})
	childCtx := bindDecision(t, authority, childDecision)
	updateDecision := testDecision(t, "owner", "owner-actor", "request-update",
		[][]byte{[]byte("source-a"), []byte("source-b")})
	updateCtx := bindDecision(t, authority, updateDecision)
	directWidening := registerRequest(now, "request-update", "parent", "", "source-b")
	directWidening.ExpectedGeneration = 1
	if _, err := service.Register(updateCtx, directWidening); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("direct authorization widening = %v", err)
	}
	widened := registerRequest(now, "request-2", "wide", "parent", "source-b")
	widened.Spec.Capabilities[0].Actions[0].Name = "admin"
	if _, err := service.Register(childCtx, widened); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("widened delegation = %v", err)
	}

	other := testDecision(t, "other", "other-actor", "request-3", [][]byte{[]byte("source-b")})
	otherCtx := bindDecision(t, authority, other)
	if _, err := service.Resolve(otherCtx, ResolveRequest{
		Context: requestContext(now, "request-3"), ID: child.ID,
	}); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("cross-principal resolution = %v", err)
	}
	peer := testDecision(t, "peer", "peer-actor", "request-peer", [][]byte{[]byte("source-a")})
	peerCtx := bindDecision(t, authority, peer)
	resolved, err := service.Resolve(peerCtx, ResolveRequest{
		Context: requestContext(now, "request-peer"), ID: child.ID,
	})
	if err != nil || resolved.Descriptor.Subject != "owner" || resolved.Executor == nil {
		t.Fatalf("authorized cross-principal resolution = %#v, %v", resolved, err)
	}
	if err := service.ValidateDelivery(peerCtx, child.ID, child.Generation); err != nil {
		t.Fatalf("delivery validation = %v", err)
	}
	if err := service.ValidateCurrentDelivery(peerCtx, child.ID); err != nil {
		t.Fatalf("current delivery validation = %v", err)
	}
	if err := service.ValidateDelivery(peerCtx, child.ID, child.Generation+1); !shoal.IsErrorCode(
		err, shoal.ErrorNotFound,
	) {
		t.Fatalf("stale delivery generation = %v", err)
	}

	heartbeatDecision := testDecision(t, "owner", "owner-actor", "request-4",
		[][]byte{[]byte("source-a"), []byte("source-b")})
	heartbeatCtx := bindDecision(t, authority, heartbeatDecision)
	if _, err := service.Heartbeat(heartbeatCtx, HeartbeatRequest{
		Context: requestContext(now, "request-4"), RegistrationKey: "child-heartbeat-too-long",
		ID: child.ID, ExpectedGeneration: child.Generation,
		LeaseExpiresAt: parent.LeaseExpiresAt.Add(time.Second),
	}); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("delegated heartbeat lease widening = %v", err)
	}
	heartbeat, err := service.Heartbeat(heartbeatCtx, HeartbeatRequest{
		Context: requestContext(now, "request-4"), RegistrationKey: "heartbeat-key",
		ID: parent.ID, ExpectedGeneration: parent.Generation,
		LeaseExpiresAt: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if heartbeat.Generation != 2 {
		t.Fatalf("heartbeat generation = %d", heartbeat.Generation)
	}
	now = now.Add(31 * time.Minute)
	if _, err := service.Resolve(childCtx, ResolveRequest{
		Context: requestContext(now, "request-2"), ID: child.ID,
	}); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("expired child resolution = %v", err)
	}
	revokeDecision := testDecision(t, "owner", "owner-actor", "request-5",
		[][]byte{[]byte("source-a"), []byte("source-b")})
	revokeCtx := bindDecision(t, authority, revokeDecision)
	if _, err := service.Revoke(revokeCtx, RevokeRequest{
		Context: requestContext(now, "request-5"), RegistrationKey: "revoke-key",
		ID: parent.ID, ExpectedGeneration: heartbeat.Generation,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Resolve(revokeCtx, ResolveRequest{
		Context: requestContext(now, "request-5"), ID: parent.ID,
	}); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("revoked resolution = %v", err)
	}
	if _, err := service.Resolve(revokeCtx, ResolveRequest{
		Context: requestContext(now, "request-5"), ID: child.ID,
	}); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("parent-revoked delegated resolution = %v", err)
	}
	if err := service.ValidateDelivery(revokeCtx, child.ID, child.Generation); !shoal.IsErrorCode(
		err, shoal.ErrorNotFound,
	) {
		t.Fatalf("parent-revoked delivery validation = %v", err)
	}
	if err := service.ValidateCurrentDelivery(revokeCtx, child.ID); !shoal.IsErrorCode(
		err, shoal.ErrorNotFound,
	) {
		t.Fatalf("parent-revoked current delivery validation = %v", err)
	}
	if recorder.count() != 5 {
		t.Fatalf("lifecycle records = %d, want 5", recorder.count())
	}
}

func TestServiceRecorderAndAmbiguousStoreFailClosed(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	store := newMemoryStore()
	recorder := &memoryRecorder{err: shoal.NewError(shoal.ErrorUnavailable, "recorder unavailable")}
	service, _ := NewService(Config{
		Store: store, Resolver: authority.Resolver(), Recorder: recorder,
		Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": struct{}{}}, Clock: func() time.Time { return now },
	})
	decision := testDecision(t, "owner", "actor", "request-1", [][]byte{[]byte("source-a")})
	ctx := bindDecision(t, authority, decision)
	request := registerRequest(now, "request-1", "agent", "", "source-a")
	if _, err := service.Register(ctx, request); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("recorder failure = %v", err)
	}
	if len(store.records) != 0 {
		t.Fatal("mutation occurred after recorder failure")
	}

	recorder.err = nil
	store.applyErr = shoal.NewError(shoal.ErrorUnavailable, "publication outcome indeterminate")
	if _, err := service.Register(ctx, request); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("ambiguous write = %v", err)
	}
	if len(store.records) != 0 {
		t.Fatal("ambiguous write fabricated committed state")
	}
	if recorder.count() != 2 {
		t.Fatalf("pre-admission records = %d, want 2", recorder.count())
	}
}

type memoryStore struct {
	mu       sync.Mutex
	records  map[shoal.ID]Stored
	keys     map[shoal.ID][32]byte
	applyErr error
}

type fixedSnapshot struct{ now time.Time }

func (s fixedSnapshot) InteractionSnapshot(context.Context) (explorer.Snapshot, error) {
	return explorer.Snapshot{ID: "snapshot", AsOf: s.now, Frontier: 1}, nil
}

func newMemoryStore() *memoryStore {
	return &memoryStore{records: make(map[shoal.ID]Stored), keys: make(map[shoal.ID][32]byte)}
}

func (s *memoryStore) Apply(_ context.Context, mutation Mutation) (Stored, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applyErr != nil {
		return Stored{}, s.applyErr
	}
	current, exists := s.records[mutation.Descriptor.ID]
	if (!exists && mutation.ExpectedGeneration != 0) ||
		(exists && current.Descriptor.Generation != mutation.ExpectedGeneration) {
		return Stored{}, shoal.NewError(shoal.ErrorConflict, "generation conflict")
	}
	stored := Stored{Descriptor: cloneDescriptor(mutation.Descriptor), Epoch: mutation.Descriptor.Generation}
	stored.Digest = descriptorDigest(stored.Descriptor)
	s.records[mutation.Descriptor.ID] = stored
	return stored, nil
}

func (s *memoryStore) Get(_ context.Context, id shoal.ID) (Stored, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.records[id]
	if !ok {
		return Stored{}, shoal.NewError(shoal.ErrorNotFound, "not found")
	}
	stored.Descriptor = cloneDescriptor(stored.Descriptor)
	return stored, nil
}

func (s *memoryStore) ListPage(
	_ context.Context, cursor string, limit uint32,
) (StoredPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]shoal.ID, 0, len(s.records))
	for id := range s.records {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return shoal.CompareID(ids[i], ids[j]) < 0
	})
	start := sort.Search(len(ids), func(index int) bool {
		return shoal.CompareID(ids[index], shoal.ID(cursor)) > 0
	})
	result := StoredPage{
		Items: make([]StoredListItem, 0, limit),
	}
	for _, id := range ids[start:] {
		stored := s.records[id]
		stored.Descriptor = cloneDescriptor(stored.Descriptor)
		result.Items = append(result.Items, StoredListItem{
			Stored: stored, Cursor: string(id),
		})
		if len(result.Items) == int(limit) {
			if start+len(result.Items) < len(ids) {
				result.NextCursor = string(id)
			}
			break
		}
	}
	return result, nil
}

type memoryRecorder struct {
	mu      sync.Mutex
	records []Lifecycle
	err     error
}

func (r *memoryRecorder) RecordLifecycle(_ context.Context, lifecycle Lifecycle) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, lifecycle)
	return r.err
}
func (r *memoryRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

type executorMap map[string]Executor

func (m executorMap) ResolveExecutor(ref string) (Executor, bool) {
	executor, ok := m[ref]
	return executor, ok
}

func testDecision(t *testing.T, subject, actor, requestID string, sources [][]byte) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: shoal.ID(subject), Actor: shoal.ID(actor),
		AuthorizationDomain: []byte("domain"),
		AllowedOperations: []auth.Operation{
			auth.OperationAgentRegister, auth.OperationAgentHeartbeat,
			auth.OperationAgentRevoke, auth.OperationAgentResolve, auth.OperationDelegate,
		},
		PermittedSourceIDs: sources, PermittedPolicyIDs: [][]byte{[]byte("policy")},
		PolicyGeneration:      1,
		AuthenticationExpires: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
		RequestID:             shoal.ID(requestID),
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

func bindDecision(t *testing.T, authority *auth.Authority, decision auth.Decision) context.Context {
	t.Helper()
	ctx, err := authority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func registerRequest(now time.Time, requestID, id, parent, source string) RegisterRequest {
	return RegisterRequest{
		Context: requestContext(now, requestID), RegistrationKey: shoal.ID("key-" + id),
		Spec: Spec{
			ID: shoal.ID(id), ParentID: shoal.ID(parent),
			AuthorizationDomain: []byte("domain"),
			Scopes:              []Scope{{SourceID: []byte(source), PolicyID: []byte("policy")}},
			ExecutorRef:         "exec", LeaseExpiresAt: now.Add(30 * time.Minute),
			Capabilities: []Capability{{
				Name: "search", Actions: []Action{{
					Name: "query", InputSchema: json.RawMessage(`{"type":"object"}`),
					OutputSchema: json.RawMessage(`{"type":"object"}`),
				}},
			}},
		},
	}
}

func requestContext(now time.Time, requestID string) RequestContext {
	return RequestContext{
		RequestID: shoal.ID(requestID), ReasonCode: "operator_request",
		Deadline: now.Add(time.Minute),
	}
}
