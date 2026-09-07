// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/transaction"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestDispatchStoreCASReplayFenceAndRestart(t *testing.T) {
	directory := t.TempDir()
	runtime := openDispatchRuntime(t, directory)
	defer func() {
		if runtime != nil {
			_ = runtime.Close()
		}
	}()
	store, err := NewDispatchStore(runtime, nil)
	if err != nil {
		t.Fatal(err)
	}

	record := testActionRecord()
	created, err := store.ApplyAction(context.Background(), fleet.DispatchMutation{
		Token: []byte("enqueue-key"), Record: record,
	})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := store.ApplyAction(context.Background(), fleet.DispatchMutation{
		Token: []byte("enqueue-key"), Record: record,
	})
	if err != nil || replayed.Version != created.Version {
		t.Fatalf("replay = %#v, %v", replayed, err)
	}
	divergent := record
	divergent.Input = json.RawMessage(`{"value":2}`)
	if _, err := store.ApplyAction(context.Background(), fleet.DispatchMutation{
		Token: []byte("enqueue-key"), Record: divergent,
	}); !errors.Is(err, fleet.ErrActionConflict) {
		t.Fatalf("divergent replay = %v", err)
	}
	secondRecord := record
	secondRecord.ID = []byte{'b', 0, 255}
	secondRecord.IdempotencyKey = []byte("second-key")
	secondRecord.ExecutorKey = []byte("second-executor-key")
	if _, err := store.ApplyAction(context.Background(), fleet.DispatchMutation{
		Token: []byte("second-key"), Record: secondRecord,
	}); err != nil {
		t.Fatal(err)
	}
	firstPage, err := store.ScanActions(context.Background(), nil, 1)
	if err != nil || len(firstPage.Actions) != 1 ||
		string(firstPage.Actions[0].ID) != string(record.ID) ||
		string(firstPage.Next) != string(record.ID) {
		t.Fatalf("first page = %#v, %v", firstPage, err)
	}
	secondPage, err := store.ScanActions(context.Background(), firstPage.Next, 1)
	if err != nil || len(secondPage.Actions) != 1 ||
		string(secondPage.Actions[0].ID) != string(secondRecord.ID) {
		t.Fatalf("second page = %#v, %v", secondPage, err)
	}

	left, right := record, record
	left.Version, right.Version = 2, 2
	left.State, right.State = fleet.DispatchClaimed, fleet.DispatchClaimed
	left.ClaimID, right.ClaimID = []byte("left"), []byte("right")
	left.ClaimFence, right.ClaimFence = 1, 1
	left.ClaimLease, right.ClaimLease = time.Minute, time.Minute
	left.ClaimLeaseUntil = record.UpdatedAt.Add(time.Minute)
	right.ClaimLeaseUntil = record.UpdatedAt.Add(time.Minute)
	left.ExecutionPolicyGeneration, right.ExecutionPolicyGeneration = 1, 1
	left.ExecutionExpiresAt, right.ExecutionExpiresAt = record.Deadline, record.Deadline
	var wait sync.WaitGroup
	results := make(chan error, 2)
	for _, item := range []struct {
		token  []byte
		record fleet.ActionRecord
	}{{[]byte("claim-left"), left}, {[]byte("claim-right"), right}} {
		item := item
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, applyErr := store.ApplyAction(context.Background(), fleet.DispatchMutation{
				Token: item.token, ExpectedVersion: 1, Record: item.record,
			})
			results <- applyErr
		}()
	}
	wait.Wait()
	close(results)
	successes, conflicts := 0, 0
	for result := range results {
		if result == nil {
			successes++
		} else if errors.Is(result, fleet.ErrActionConflict) ||
			errors.Is(result, transaction.ErrConflict) {
			conflicts++
		} else {
			t.Fatalf("claim result = %v", result)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
	current, err := store.GetAction(context.Background(), record.ID)
	if err != nil || current.Version != 2 || current.ClaimFence != 1 {
		t.Fatalf("current = %#v, %v", current, err)
	}
	stale := current
	stale.Version++
	stale.ClaimFence++
	if _, err := store.ApplyAction(context.Background(), fleet.DispatchMutation{
		Token: []byte("stale-fence"), ExpectedVersion: current.Version,
		ExpectedFence: current.ClaimFence + 1, Record: stale,
	}); !errors.Is(err, fleet.ErrActionConflict) {
		t.Fatalf("stale fence = %v", err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	runtime = nil
	runtime = openDispatchRuntime(t, directory)
	store, _ = NewDispatchStore(runtime, nil)
	restarted, err := store.GetAction(context.Background(), record.ID)
	if err != nil || restarted.Version != 2 ||
		string(restarted.ExecutorKey) != string(record.ExecutorKey) ||
		string(restarted.ClaimID) != string(current.ClaimID) {
		t.Fatalf("restart = %#v, %v", restarted, err)
	}
}

func TestDispatchRealRuntimeRegistryAndRestart(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	directory := t.TempDir()
	runtime := openFleetDispatchRuntime(t, directory)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	executor := &integratedExecutor{}
	registry, dispatch := composeIntegratedServices(t, runtime, authority, executor, now)
	decision := integratedDecision(t, now)
	ctx, err := authority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := registry.Register(ctx, fleet.RegisterRequest{
		Context: integratedContext(now), RegistrationKey: "register-agent",
		Spec: fleet.Spec{
			ID: "agent", AuthorizationDomain: []byte("domain"),
			Scopes:      []fleet.Scope{{SourceID: []byte("source"), PolicyID: []byte("policy")}},
			ExecutorRef: "exec", LeaseExpiresAt: now.Add(time.Hour),
			Capabilities: []fleet.Capability{{Name: "search", Actions: []fleet.Action{{
				Name:         "query",
				InputSchema:  json.RawMessage(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
				OutputSchema: json.RawMessage(`{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}`),
			}}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := dispatch.Enqueue(ctx, fleet.EnqueueRequest{
		ID: []byte("durable-action"), IdempotencyKey: []byte("durable-key"),
		AgentID: descriptor.ID, AgentGeneration: descriptor.Generation,
		Capability: "search", Action: "query", SourceID: []byte("source"),
		PolicyID: []byte("policy"), ObjectID: "object",
		Input: json.RawMessage(`{"value":1}`), Context: integratedContext(now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	runtime = openFleetDispatchRuntime(t, directory)
	defer runtime.Close()
	_, dispatch = composeIntegratedServices(t, runtime, authority, executor, now)
	restarted, err := dispatch.Status(ctx, fleet.StatusRequest{
		ID: queued.ID, Context: integratedContext(now),
	})
	if err != nil || restarted.State != fleet.DispatchQueued ||
		string(restarted.ExecutorKey) != string(queued.ExecutorKey) {
		t.Fatalf("restarted action = %#v, %v", restarted, err)
	}
}

func TestDispatchCollidingOpaqueTuplesExecuteAndSurviveRuntimeRestart(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	directory := t.TempDir()
	runtime := openFleetDispatchRuntime(t, directory)
	defer func() {
		if runtime != nil {
			_ = runtime.Close()
		}
	}()
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	executor := &integratedExecutor{}
	registry, dispatch := composeIntegratedServices(
		t, runtime, authority, executor, now,
	)
	decision := integratedDecision(t, now)
	ctx, err := authority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := registerIntegratedAgent(t, registry, ctx, now)
	requests := []struct {
		id, key, claim []byte
	}{
		{
			id: []byte("a\x00b"), key: []byte("c"),
			claim: []byte("claim-one"),
		},
		{
			id: []byte("a"), key: []byte("b\x00c"),
			claim: []byte("claim-two"),
		},
	}
	for _, request := range requests {
		enqueue := integratedEnqueue(
			now, descriptor, request.id, request.key,
		)
		queued, enqueueErr := dispatch.Enqueue(ctx, enqueue)
		if enqueueErr != nil {
			t.Fatalf("enqueue %x = %v", request.id, enqueueErr)
		}
		claimed, claimErr := dispatch.Claim(ctx, fleet.ClaimRequest{
			ID: queued.ID, ExpectedVersion: queued.Version,
			ClaimID: request.claim, Lease: time.Minute,
			Context: enqueue.Context,
		})
		if claimErr != nil {
			current, readErr := dispatch.Status(ctx, fleet.StatusRequest{
				ID: request.id, Context: enqueue.Context,
			})
			t.Fatalf(
				"claim %x = %v; current = %#v, %v",
				request.id, claimErr, current, readErr,
			)
		}
		completed, executeErr := dispatch.ExecuteClaim(ctx, claimed)
		if executeErr != nil {
			t.Fatalf("execute %x = %v", request.id, executeErr)
		}
		if completed.State != fleet.DispatchSucceeded {
			t.Fatalf("invoke %x state = %q", request.id, completed.State)
		}
	}
	calls, effects, keys := executor.snapshot()
	if calls != 2 || effects != 2 || len(keys) != 2 ||
		bytes.Equal(keys[0], keys[1]) {
		t.Fatalf(
			"pre-restart executor calls/effects/keys = %d/%d/%x",
			calls, effects, keys,
		)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	runtime = nil
	runtime = openFleetDispatchRuntime(t, directory)
	_, dispatch = composeIntegratedServices(t, runtime, authority, executor, now)
	for _, request := range requests {
		restarted, statusErr := dispatch.Status(ctx, fleet.StatusRequest{
			ID: request.id, Context: integratedContext(now),
		})
		if statusErr != nil || restarted.State != fleet.DispatchSucceeded {
			t.Fatalf(
				"restarted action %x = %#v, %v",
				request.id, restarted, statusErr,
			)
		}
	}
	calls, effects, _ = executor.snapshot()
	if calls != 2 || effects != 2 {
		t.Fatalf(
			"restart repeated effects: calls=%d effects=%d", calls, effects,
		)
	}
}

func TestDispatchDurableRestartPreservesStoredExecutorKeyAfterAmbiguousEffect(
	t *testing.T,
) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	directory := t.TempDir()
	runtime := openFleetDispatchRuntime(t, directory)
	defer func() {
		if runtime != nil {
			_ = runtime.Close()
		}
	}()
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	executor := &integratedExecutor{}
	failingRecorder := &controlledIntegratedActionRecorder{
		failPhase: "effect_outcome",
	}
	registry, dispatch := composeIntegratedServicesWithRecorder(
		t, runtime, authority, executor, failingRecorder, now,
	)
	decision := integratedDecision(t, now)
	ctx, err := authority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	descriptor := registerIntegratedAgent(t, registry, ctx, now)
	request := integratedEnqueue(
		now, descriptor, []byte("legacy-action"), []byte("legacy-request-key"),
	)
	queued, err := dispatch.Enqueue(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewDispatchStore(runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	legacyKey := []byte("persisted-pre-v2-executor-key")
	withLegacyKey := queued
	withLegacyKey.Version++
	withLegacyKey.ExecutorKey = append([]byte(nil), legacyKey...)
	withLegacyKey, err = store.ApplyAction(
		ctx,
		fleet.DispatchMutation{
			Token:           []byte("install-legacy-executor-key"),
			ExpectedVersion: queued.Version,
			ExpectedFence:   queued.ClaimFence,
			Record:          withLegacyKey,
		},
	)
	if err != nil {
		current, readErr := store.GetAction(ctx, request.ID)
		t.Fatalf("install legacy key = %v; current = %#v, %v", err, current, readErr)
	}
	_, err = dispatch.Invoke(ctx, fleet.InvokeRequest{
		Enqueue: request, ClaimID: []byte("claim"), Lease: time.Minute,
	})
	if !errors.Is(err, fleet.ErrExecutionAmbiguous) ||
		!errors.Is(err, fleet.ErrRecordingUnavailable) {
		t.Fatalf("post-effect receipt failure = %v", err)
	}
	claimed, err := store.GetAction(ctx, request.ID)
	if err != nil || claimed.State != fleet.DispatchClaimed ||
		!bytes.Equal(claimed.ExecutorKey, legacyKey) ||
		claimed.Version != withLegacyKey.Version+1 {
		t.Fatalf("ambiguous durable state = %#v, %v", claimed, err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	runtime = nil
	runtime = openFleetDispatchRuntime(t, directory)
	_, dispatch = composeIntegratedServicesWithRecorder(
		t, runtime, authority, executor, integratedActionRecorder{}, now,
	)
	completed, err := dispatch.Invoke(ctx, fleet.InvokeRequest{
		Enqueue: request, ClaimID: []byte("claim"), Lease: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	calls, effects, keys := executor.snapshot()
	if completed.State != fleet.DispatchSucceeded ||
		completed.Version != claimed.Version+1 ||
		!bytes.Equal(completed.ExecutorKey, legacyKey) ||
		calls != 2 || effects != 1 || len(keys) != 2 ||
		!bytes.Equal(keys[0], legacyKey) ||
		!bytes.Equal(keys[1], legacyKey) {
		t.Fatalf(
			"recovered action=%#v calls=%d effects=%d keys=%x",
			completed, calls, effects, keys,
		)
	}
}

func TestDispatchMutationAfterFixedTimestampRestartRuntimeGate(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	directory := t.TempDir()
	open := func() *explorercoord.Runtime {
		runtime, err := explorercoord.Open(explorercoord.Config{
			Directory: directory, Domain: coordination.DomainID("fleet-dispatch-fixed"),
			Owner: coordination.OwnerID("dispatch-worker"),
			Authority: transaction.Authority{
				Generation: 1, Fence: 1, Holder: coordination.OwnerID("dispatch-process"),
				Mode: coordination.WriterModeEmbeddedPrimary, RetentionGeneration: 1, HistoryFloor: 1,
			},
			PhysicalTables: []string{DispatchPhysicalTable()}, Lease: time.Minute,
			Clock:         func() time.Time { return now },
			RecoveryLimit: 16, RecoveryMaxPages: 64,
			RetryBackoff: time.Nanosecond, RecoveryBackoff: time.Nanosecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		return runtime
	}

	runtime := open()
	store, err := NewDispatchStore(runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	record := testActionRecord()
	if _, err := store.ApplyAction(context.Background(), fleet.DispatchMutation{
		Token: []byte("fixed-enqueue"), Record: record,
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}

	runtime = open()
	defer runtime.Close()
	store, err = NewDispatchStore(runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	current, err := store.GetAction(context.Background(), record.ID)
	if err != nil {
		t.Fatal(err)
	}
	claimed := current
	claimed.Version++
	claimed.State = fleet.DispatchClaimed
	claimed.ClaimID = []byte("fixed-claim")
	claimed.ClaimFence++
	claimed.ClaimLease = time.Minute
	claimed.ClaimLeaseUntil = now.Add(time.Minute)
	claimed.ExecutionPolicyGeneration = 1
	claimed.ExecutionExpiresAt = record.Deadline
	stored, err := store.ApplyAction(context.Background(), fleet.DispatchMutation{
		Token: []byte("fixed-claim-token"), ExpectedVersion: current.Version,
		ExpectedFence: current.ClaimFence, Record: claimed,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stored.Version != claimed.Version || stored.ClaimFence != claimed.ClaimFence ||
		!bytes.Equal(stored.ClaimID, claimed.ClaimID) {
		t.Fatalf("claimed action = %#v", stored)
	}
}

func openDispatchRuntime(t *testing.T, directory string) *explorercoord.Runtime {
	t.Helper()
	runtime, err := explorercoord.Open(explorercoord.Config{
		Directory: directory, Domain: coordination.DomainID("fleet-dispatch"),
		Owner: coordination.OwnerID("dispatch-worker"),
		Authority: transaction.Authority{
			Generation: 1, Fence: 1, Holder: coordination.OwnerID("dispatch-process"),
			Mode: coordination.WriterModeEmbeddedPrimary, RetentionGeneration: 1, HistoryFloor: 1,
		},
		PhysicalTables: []string{DispatchPhysicalTable()}, Lease: time.Minute,
		RecoveryLimit: 16, RecoveryMaxPages: 64,
		RetryBackoff: time.Nanosecond, RecoveryBackoff: time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	return runtime
}

func openFleetDispatchRuntime(t *testing.T, directory string) *explorercoord.Runtime {
	t.Helper()
	config := ConfigureRuntime(explorercoord.Config{
		Directory: directory, Domain: coordination.DomainID("fleet-integrated"),
		Owner: coordination.OwnerID("fleet-worker"),
		Authority: transaction.Authority{
			Generation: 1, Fence: 1, Holder: coordination.OwnerID("fleet-process"),
			Mode: coordination.WriterModeEmbeddedPrimary, RetentionGeneration: 1, HistoryFloor: 1,
		},
		Lease: time.Minute, RecoveryLimit: 16, RecoveryMaxPages: 64,
		RetryBackoff: time.Nanosecond, RecoveryBackoff: time.Nanosecond,
	})
	runtime, err := explorercoord.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func composeIntegratedServices(
	t *testing.T,
	runtime *explorercoord.Runtime,
	authority *auth.Authority,
	executor *integratedExecutor,
	now time.Time,
) (*fleet.Service, *fleet.DispatchService) {
	return composeIntegratedServicesWithRecorder(
		t, runtime, authority, executor, integratedActionRecorder{}, now,
	)
}

func composeIntegratedServicesWithRecorder(
	t *testing.T,
	runtime *explorercoord.Runtime,
	authority *auth.Authority,
	executor *integratedExecutor,
	recorder fleet.ActionRecorder,
	now time.Time,
) (*fleet.Service, *fleet.DispatchService) {
	t.Helper()
	registryStore, err := NewStore(runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := fleet.NewService(fleet.Config{
		Store: registryStore, Resolver: authority.Resolver(),
		Recorder:  integratedLifecycleRecorder{},
		Snapshots: integratedSnapshot{now},
		Executors: integratedExecutors{"exec": executor},
		Clock:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	dispatchStore, err := NewDispatchStore(runtime, nil)
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := fleet.NewDispatchService(fleet.DispatchConfig{
		Store: dispatchStore, Registry: registry, Resolver: authority.Resolver(),
		Recorder: recorder, Events: integratedEvents{},
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry, dispatch
}

type integratedSnapshot struct{ now time.Time }

func (s integratedSnapshot) InteractionSnapshot(context.Context) (explorer.Snapshot, error) {
	return explorer.Snapshot{ID: "snapshot", AsOf: s.now, Frontier: 1}, nil
}

type integratedLifecycleRecorder struct{}

func (integratedLifecycleRecorder) RecordLifecycle(context.Context, fleet.Lifecycle) error {
	return nil
}

type integratedActionRecorder struct{}

func (integratedActionRecorder) RecordAction(context.Context, fleet.ActionAudit) error {
	return nil
}

type controlledIntegratedActionRecorder struct {
	failPhase string
}

func (r *controlledIntegratedActionRecorder) RecordAction(
	_ context.Context,
	audit fleet.ActionAudit,
) error {
	if audit.Phase == r.failPhase {
		return errors.New("injected action receipt failure")
	}
	return nil
}

type integratedEvents struct{}

func (integratedEvents) PublishActionEvent(context.Context, string, fleet.ActionRecord) error {
	return nil
}

type integratedExecutors map[string]fleet.Executor

func (e integratedExecutors) ResolveExecutor(ref string) (fleet.Executor, bool) {
	value, ok := e[ref]
	return value, ok
}

type integratedExecutor struct {
	mu      sync.Mutex
	calls   int
	keys    [][]byte
	effects map[string]struct{}
}

func (e *integratedExecutor) Execute(
	_ context.Context,
	invocation fleet.Invocation,
) (fleet.ExecutionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
	key := append([]byte(nil), invocation.IdempotencyKey...)
	e.keys = append(e.keys, key)
	if e.effects == nil {
		e.effects = make(map[string]struct{})
	}
	e.effects[string(key)] = struct{}{}
	return fleet.ExecutionResult{Output: json.RawMessage(`{"ok":true}`)}, nil
}

func (e *integratedExecutor) snapshot() (int, int, [][]byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	keys := make([][]byte, len(e.keys))
	for index := range e.keys {
		keys[index] = append([]byte(nil), e.keys[index]...)
	}
	return e.calls, len(e.effects), keys
}

func registerIntegratedAgent(
	t *testing.T,
	registry *fleet.Service,
	ctx context.Context,
	now time.Time,
) fleet.Descriptor {
	t.Helper()
	descriptor, err := registry.Register(ctx, fleet.RegisterRequest{
		Context: integratedContext(now), RegistrationKey: "register-agent",
		Spec: fleet.Spec{
			ID: "agent", AuthorizationDomain: []byte("domain"),
			Scopes: []fleet.Scope{{
				SourceID: []byte("source"), PolicyID: []byte("policy"),
			}},
			ExecutorRef: "exec", LeaseExpiresAt: now.Add(time.Hour),
			Capabilities: []fleet.Capability{{
				Name: "search", Actions: []fleet.Action{{
					Name: "query",
					InputSchema: json.RawMessage(
						`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`,
					),
					OutputSchema: json.RawMessage(
						`{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}`,
					),
				}},
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func integratedEnqueue(
	now time.Time,
	descriptor fleet.Descriptor,
	id, key []byte,
) fleet.EnqueueRequest {
	return fleet.EnqueueRequest{
		ID: id, IdempotencyKey: key,
		AgentID: descriptor.ID, AgentGeneration: descriptor.Generation,
		Capability: "search", Action: "query", SourceID: []byte("source"),
		PolicyID: []byte("policy"), ObjectID: "object",
		Input: json.RawMessage(`{"value":1}`), Context: integratedContext(now),
	}
}

func integratedDecision(t *testing.T, now time.Time) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "owner", Actor: "actor", AuthorizationDomain: []byte("domain"),
		AllowedOperations: []auth.Operation{
			auth.OperationAgentRegister, auth.OperationDispatch, auth.OperationInvoke,
		},
		PermittedSourceIDs: [][]byte{[]byte("source")},
		PermittedPolicyIDs: [][]byte{[]byte("policy")}, PolicyGeneration: 1,
		AuthenticationExpires: now.Add(2 * time.Hour), RequestID: "request",
		CorrelationID: "correlation",
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

func integratedContext(now time.Time) fleet.RequestContext {
	return fleet.RequestContext{
		RequestID: "request", CorrelationID: "correlation",
		ReasonCode: "operator_request", Deadline: now.Add(time.Hour),
	}
}

func testActionRecord() fleet.ActionRecord {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	return fleet.ActionRecord{
		ID: []byte{'a', 0, 255}, IdempotencyKey: []byte{'k', 0, 255},
		Version: 1, State: fleet.DispatchQueued, AgentID: "agent",
		AgentGeneration: 1, Capability: "search", Action: "query",
		SourceID: []byte("source"), PolicyID: []byte("policy"), ObjectID: "object",
		Input: json.RawMessage(`{"value":1}`), Subject: "subject", Actor: "actor",
		ClientID: "client", OnBehalfOf: []shoal.ID{"principal"},
		PolicyGeneration: 1, AuthorizedOperations: []auth.Operation{auth.OperationDispatch},
		AuthorizationExpiresAt: now.Add(time.Hour),
		RequestID:              "request", CorrelationID: "correlation",
		Reason: interaction.Reason{Code: "test"}, Deadline: now.Add(time.Hour),
		CreatedAt: now, UpdatedAt: now, ExecutorKey: []byte{'e', 0, 255},
	}
}
