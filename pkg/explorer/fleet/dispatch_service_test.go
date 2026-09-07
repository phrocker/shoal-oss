// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestValidateExecutionEvidenceRequiresCompletePinnedAnchors(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	result := ExecutionResult{
		EvidenceSnapshotID: "snapshot", EvidenceSnapshotAsOf: now.Add(-time.Minute),
		Evidence: []EvidenceRef{{
			AnchorID: "anchor", Kind: interaction.EvidenceGraph,
			NodeIDs: []shoal.ID{"left", "right"}, EdgeIDs: []shoal.ID{"edge"},
			Assertions: []interaction.AssertionReference{{
				AssertionID: "assertion", EdgeID: "edge",
				Origin: ontology.AssertionExplicit,
			}},
			Visibility: []string{"restricted"},
		}},
	}
	if err := validateExecutionEvidence(result, now, now.Add(time.Hour)); err != nil {
		t.Fatalf("complete pinned evidence = %v", err)
	}
	missingPin := result
	missingPin.EvidenceSnapshotID = ""
	if err := validateExecutionEvidence(
		missingPin, now, now.Add(time.Hour),
	); err == nil {
		t.Fatal("accepted evidence without a snapshot ID")
	}
	future := result
	future.EvidenceSnapshotAsOf = now.Add(time.Second)
	if err := validateExecutionEvidence(
		future, now, now.Add(time.Hour),
	); err == nil {
		t.Fatal("accepted a future evidence snapshot")
	}
}

func TestDispatchInvokeFreshAuthorizationAndAmbiguousRecording(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	registryStore := newMemoryStore()
	executor := &dispatchExecutor{result: ExecutionResult{Output: json.RawMessage(`{"ok":true}`)}}
	registry, err := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(), Recorder: &memoryRecorder{},
		Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": executor}, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	registryStore.records["agent"] = Stored{Descriptor: dispatchDescriptor(now)}
	store := newMemoryDispatchStore()
	recorder := &dispatchRecorder{failPhase: "effect_outcome"}
	service, err := NewDispatchService(DispatchConfig{
		Store: store, Registry: registry, Resolver: authority.Resolver(),
		Recorder: recorder, Events: dispatchEvents{}, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	decision := dispatchDecision(t, "owner", "actor", "request", auth.OperationDispatch, auth.OperationInvoke)
	ctx := bindDecision(t, authority, decision)
	_, err = service.Invoke(ctx, InvokeRequest{
		Enqueue: dispatchEnqueue(now, "request"), ClaimID: []byte("claim"), Lease: time.Minute,
	})
	if !errors.Is(err, ErrExecutionAmbiguous) || !errors.Is(err, ErrRecordingUnavailable) {
		t.Fatalf("invoke error = %v", err)
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d", executor.calls)
	}
	if len(executor.invocations) != 1 ||
		!bytes.Equal(executor.invocations[0].SourceID, []byte("source")) ||
		!bytes.Equal(executor.invocations[0].PolicyID, []byte("policy")) ||
		executor.invocations[0].ObjectID != "object" {
		t.Fatalf("executor target = %#v", executor.invocations)
	}
	stored, err := store.GetAction(ctx, []byte("action"))
	if err != nil || stored.State != DispatchClaimed || stored.EffectPossible {
		t.Fatalf("stored = %#v, %v", stored, err)
	}
	recorder.failPhase = ""
	retryDecision := dispatchDecision(t, "owner", "actor", "retry", auth.OperationDispatch, auth.OperationInvoke)
	retryCtx := bindDecision(t, authority, retryDecision)
	completed, err := service.Invoke(retryCtx, InvokeRequest{
		Enqueue: dispatchEnqueue(now, "retry"), ClaimID: []byte("claim"), Lease: time.Minute,
	})
	if err != nil || completed.State != DispatchSucceeded {
		t.Fatalf("retry = %#v, %v", completed, err)
	}
	if len(executor.keys) != 2 ||
		string(executor.keys[0]) != string(executor.keys[1]) ||
		string(executor.keys[0]) != string(stored.ExecutorKey) {
		t.Fatalf("executor key was not stable: %#v", executor.keys)
	}
}

func TestDispatchInvokeOnlyAuthorizationEnqueuesAndExecutes(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	registryStore := newMemoryStore()
	registry, _ := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(),
		Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": &dispatchExecutor{
			result: ExecutionResult{Output: json.RawMessage(`{"ok":true}`)},
		}},
		Clock: func() time.Time { return now },
	})
	registryStore.records["agent"] = Stored{Descriptor: dispatchDescriptor(now)}
	service, _ := NewDispatchService(DispatchConfig{
		Store: newMemoryDispatchStore(), Registry: registry,
		Resolver: authority.Resolver(), Recorder: &dispatchRecorder{},
		Events: dispatchEvents{}, Clock: func() time.Time { return now },
	})
	decision := dispatchDecision(t, "owner", "actor", "request", auth.OperationInvoke)
	ctx := bindDecision(t, authority, decision)
	result, err := service.Invoke(ctx, InvokeRequest{
		Enqueue: dispatchEnqueue(now, "request"), ClaimID: []byte("claim"),
		Lease: time.Minute,
	})
	if err != nil || result.State != DispatchSucceeded {
		t.Fatalf("invoke-only dispatch = %#v, %v", result, err)
	}
}

func TestDispatchFailsClosedBeforeEffectAndOnRevokedLease(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	registryStore := newMemoryStore()
	executor := &dispatchExecutor{result: ExecutionResult{Output: json.RawMessage(`{"ok":true}`)}}
	registry, _ := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(), Recorder: &memoryRecorder{},
		Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": executor}, Clock: func() time.Time { return now },
	})
	registryStore.records["agent"] = Stored{Descriptor: dispatchDescriptor(now)}
	store := newMemoryDispatchStore()
	recorder := &dispatchRecorder{}
	service, _ := NewDispatchService(DispatchConfig{
		Store: store, Registry: registry, Resolver: authority.Resolver(),
		Recorder: recorder, Events: dispatchEvents{}, Clock: func() time.Time { return now },
	})
	decision := dispatchDecision(t, "owner", "actor", "request", auth.OperationDispatch, auth.OperationInvoke)
	ctx := bindDecision(t, authority, decision)
	queued, err := service.Enqueue(ctx, dispatchEnqueue(now, "request"))
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := service.Claim(ctx, ClaimRequest{
		ID: queued.ID, ExpectedVersion: queued.Version, ClaimID: []byte("claim"),
		Lease: time.Minute, Context: dispatchContext(now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := registryStore.records["agent"]
	descriptor.Descriptor.RevokedAt = now
	registryStore.records["agent"] = descriptor
	if _, err := service.ExecuteClaim(ctx, claimed); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("revoked execution = %v", err)
	}
	if executor.calls != 0 {
		t.Fatalf("revoked executor calls = %d", executor.calls)
	}
	descriptor.Descriptor.RevokedAt = time.Time{}
	registryStore.records["agent"] = descriptor
	recorder.failPhase = "effect_admission"
	if _, err := service.ExecuteClaim(ctx, claimed); !errors.Is(err, ErrRecordingUnavailable) {
		t.Fatalf("admission recording = %v", err)
	}
	if executor.calls != 0 {
		t.Fatalf("recording failure executor calls = %d", executor.calls)
	}
}

func TestDispatchFencesExecutorWhenClaimExpiresDuringEffect(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	clock := now
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return clock })
	registryStore := newMemoryStore()
	executor := &dispatchExecutor{
		result:  ExecutionResult{Output: json.RawMessage(`{"ok":true}`)},
		advance: func() { clock = clock.Add(2 * time.Minute) },
	}
	registry, _ := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(), Recorder: &memoryRecorder{},
		Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": executor}, Clock: func() time.Time { return clock },
	})
	registryStore.records["agent"] = Stored{Descriptor: dispatchDescriptor(now)}
	store := newMemoryDispatchStore()
	service, _ := NewDispatchService(DispatchConfig{
		Store: store, Registry: registry, Resolver: authority.Resolver(),
		Recorder: &dispatchRecorder{}, Events: dispatchEvents{}, Clock: func() time.Time { return clock },
	})
	decision := dispatchDecision(t, "owner", "actor", "request", auth.OperationDispatch, auth.OperationInvoke)
	ctx := bindDecision(t, authority, decision)
	queued, err := service.Enqueue(ctx, dispatchEnqueue(now, "request"))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := service.Claim(ctx, ClaimRequest{
		ID: queued.ID, ExpectedVersion: queued.Version, ClaimID: []byte("claim"),
		Lease: time.Minute, Context: dispatchContext(now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ExecuteClaim(ctx, claimed); !errors.Is(err, ErrExecutionAmbiguous) ||
		!errors.Is(err, ErrClaimLost) {
		t.Fatalf("expired execution = %v", err)
	}
	current, err := store.GetAction(ctx, queued.ID)
	if err != nil || current.State != DispatchClaimed || current.EffectPossible {
		t.Fatalf("expired state = %#v, %v", current, err)
	}
}

func TestDispatchAuthorizationReplayConflictAndCancellationFence(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	registryStore := newMemoryStore()
	registry, _ := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(), Recorder: &memoryRecorder{},
		Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": &dispatchExecutor{}}, Clock: func() time.Time { return now },
	})
	registryStore.records["agent"] = Stored{Descriptor: dispatchDescriptor(now)}
	store := newMemoryDispatchStore()
	service, _ := NewDispatchService(DispatchConfig{
		Store: store, Registry: registry, Resolver: authority.Resolver(),
		Recorder: &dispatchRecorder{}, Events: dispatchEvents{}, Clock: func() time.Time { return now },
	})
	denied := dispatchDecision(t, "owner", "actor", "request", auth.OperationInvoke)
	deniedCtx := bindDecision(t, authority, denied)
	if _, err := service.Enqueue(deniedCtx, dispatchEnqueue(now, "request")); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("denied enqueue = %v", err)
	}

	delegatedWithoutGrant, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "owner", Actor: "actor", OnBehalfOf: []shoal.ID{"principal"},
		AuthorizationDomain: []byte("domain"),
		AllowedOperations:   []auth.Operation{auth.OperationDispatch},
		PermittedSourceIDs:  [][]byte{[]byte("source")},
		PermittedPolicyIDs:  [][]byte{[]byte("policy")}, PolicyGeneration: 1,
		AuthenticationExpires: now.Add(time.Hour), RequestID: "request",
		CorrelationID: "correlation",
	})
	if err != nil {
		t.Fatal(err)
	}
	delegatedCtx := bindDecision(t, authority, delegatedWithoutGrant)
	if _, err := service.Enqueue(delegatedCtx, dispatchEnqueue(now, "request")); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("delegation without grant = %v", err)
	}
	allowed := dispatchDecision(t, "owner", "actor", "request", auth.OperationDispatch, auth.OperationInvoke)
	ctx := bindDecision(t, authority, allowed)
	first, err := service.Enqueue(ctx, dispatchEnqueue(now, "request"))
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := service.Enqueue(ctx, dispatchEnqueue(now, "request"))
	if err != nil || replayed.Version != first.Version {
		t.Fatalf("replay = %#v, %v", replayed, err)
	}
	divergent := dispatchEnqueue(now, "request")
	divergent.Input = json.RawMessage(`{"value":2}`)
	if _, err := service.Enqueue(ctx, divergent); !errors.Is(err, ErrActionConflict) {
		t.Fatalf("divergent replay = %v", err)
	}
	claimed, err := service.Claim(ctx, ClaimRequest{
		ID: first.ID, ExpectedVersion: first.Version, ClaimID: []byte("claim"),
		Lease: time.Minute, Context: dispatchContext(now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	replayedClaim, err := service.Claim(ctx, ClaimRequest{
		ID: first.ID, ExpectedVersion: first.Version, ClaimID: []byte("claim"),
		Lease: time.Minute, Context: dispatchContext(now, "request"),
	})
	if err != nil || replayedClaim.Version != claimed.Version {
		t.Fatalf("claim replay = %#v, %v", replayedClaim, err)
	}
	if _, err := service.Claim(ctx, ClaimRequest{
		ID: first.ID, ExpectedVersion: first.Version, ClaimID: []byte("claim"),
		Lease: 2 * time.Minute, Context: dispatchContext(now, "request"),
	}); !errors.Is(err, ErrActionConflict) {
		t.Fatalf("divergent claim replay = %v", err)
	}
	if _, err := service.Cancel(ctx, CancelRequest{
		ID: first.ID, ExpectedVersion: claimed.Version,
		MutationKey: []byte("cancel-key"), Context: dispatchContext(now, "request"),
	}); !errors.Is(err, ErrActionConflict) {
		t.Fatalf("cancel active claim = %v", err)
	}
	now = now.Add(2 * time.Minute)
	canceled, err := service.Cancel(ctx, CancelRequest{
		ID: first.ID, ExpectedVersion: claimed.Version,
		MutationKey: []byte("cancel-key"), Context: dispatchContext(now, "request"),
	})
	if err != nil || canceled.State != DispatchCanceled {
		t.Fatalf("cancel = %#v, %v", canceled, err)
	}
	replayedCancel, err := service.Cancel(ctx, CancelRequest{
		ID: first.ID, ExpectedVersion: claimed.Version,
		MutationKey: []byte("cancel-key"), Context: dispatchContext(now, "request"),
	})
	if err != nil || replayedCancel.Version != canceled.Version {
		t.Fatalf("cancel replay = %#v, %v", replayedCancel, err)
	}
	if _, err := service.Cancel(ctx, CancelRequest{
		ID: first.ID, ExpectedVersion: claimed.Version,
		MutationKey: []byte("different-key"), Context: dispatchContext(now, "request"),
	}); !errors.Is(err, ErrActionConflict) {
		t.Fatalf("divergent cancel replay = %v", err)
	}
	other := dispatchDecision(t, "other", "other-actor", "request", auth.OperationDispatch)
	otherCtx := bindDecision(t, authority, other)
	if _, err := service.Status(otherCtx, StatusRequest{
		ID: first.ID, Context: dispatchContext(now, "request"),
	}); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("cross-principal status = %v", err)
	}
}

func TestDispatchCommittedEventAmbiguityReconcilesOnRetry(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	registryStore := newMemoryStore()
	registry, _ := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(), Recorder: &memoryRecorder{},
		Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": &dispatchExecutor{}}, Clock: func() time.Time { return now },
	})
	registryStore.records["agent"] = Stored{Descriptor: dispatchDescriptor(now)}
	store := newMemoryDispatchStore()
	events := &controlledDispatchEvents{err: errors.New("event unavailable")}
	service, _ := NewDispatchService(DispatchConfig{
		Store: store, Registry: registry, Resolver: authority.Resolver(),
		Recorder: &dispatchRecorder{}, Events: events, Clock: func() time.Time { return now },
	})
	decision := dispatchDecision(t, "owner", "actor", "request", auth.OperationDispatch)
	ctx := bindDecision(t, authority, decision)
	request := dispatchEnqueue(now, "request")
	if _, err := service.Enqueue(ctx, request); !errors.Is(err, ErrActionCommitted) {
		t.Fatalf("event failure = %v", err)
	}
	if current, err := store.GetAction(ctx, request.ID); err != nil || current.State != DispatchQueued {
		t.Fatalf("committed enqueue = %#v, %v", current, err)
	}
	events.err = nil
	reconciled, err := service.Enqueue(ctx, request)
	if err != nil || reconciled.State != DispatchQueued || events.calls != 2 {
		t.Fatalf("reconciled = %#v calls=%d err=%v", reconciled, events.calls, err)
	}
}

func TestDispatchTerminalEventReconcilesOnInvokeRetry(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	registryStore := newMemoryStore()
	registry, _ := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(),
		Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": &dispatchExecutor{
			result: ExecutionResult{Output: json.RawMessage(`{"ok":true}`)},
		}},
		Clock: func() time.Time { return now },
	})
	registryStore.records["agent"] = Stored{Descriptor: dispatchDescriptor(now)}
	events := &terminalFailEvents{err: errors.New("event unavailable")}
	service, _ := NewDispatchService(DispatchConfig{
		Store: newMemoryDispatchStore(), Registry: registry,
		Resolver: authority.Resolver(), Recorder: &dispatchRecorder{},
		Events: events, Clock: func() time.Time { return now },
	})
	decision := dispatchDecision(t, "owner", "actor", "request", auth.OperationDispatch, auth.OperationInvoke)
	ctx := bindDecision(t, authority, decision)
	request := dispatchEnqueue(now, "request")
	if _, err := service.Invoke(ctx, InvokeRequest{
		Enqueue: request, ClaimID: []byte("claim"), Lease: time.Minute,
	}); !errors.Is(err, ErrActionCommitted) {
		t.Fatalf("terminal event failure = %v", err)
	}
	events.err = nil
	completed, err := service.Invoke(ctx, InvokeRequest{
		Enqueue: request, ClaimID: []byte("claim"), Lease: time.Minute,
	})
	if err != nil || completed.State != DispatchSucceeded || events.calls != 3 {
		t.Fatalf("terminal event retry = %#v, calls=%d, err=%v", completed, events.calls, err)
	}
}

func TestDispatchDoesNotExecuteExpiredClaimAfterAdmission(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	clock := now
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return clock })
	registryStore := newMemoryStore()
	executor := &dispatchExecutor{
		result: ExecutionResult{Output: json.RawMessage(`{"ok":true}`)},
	}
	registry, _ := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(),
		Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": executor},
		Clock:     func() time.Time { return clock },
	})
	registryStore.records["agent"] = Stored{Descriptor: dispatchDescriptor(now)}
	recorder := &dispatchRecorder{advance: func() {
		clock = clock.Add(2 * time.Minute)
	}}
	service, _ := NewDispatchService(DispatchConfig{
		Store: newMemoryDispatchStore(), Registry: registry,
		Resolver: authority.Resolver(), Recorder: recorder,
		Events: dispatchEvents{}, Clock: func() time.Time { return clock },
	})
	decision := dispatchDecision(
		t, "owner", "actor", "request",
		auth.OperationDispatch, auth.OperationInvoke,
	)
	ctx := bindDecision(t, authority, decision)
	queued, err := service.Enqueue(ctx, dispatchEnqueue(now, "request"))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := service.Claim(ctx, ClaimRequest{
		ID: queued.ID, ExpectedVersion: queued.Version, ClaimID: []byte("claim"),
		Lease: time.Minute, Context: dispatchContext(now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ExecuteClaim(ctx, claimed); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("expired admission execution = %v", err)
	}
	if executor.calls != 0 {
		t.Fatalf("expired admission invoked executor %d times", executor.calls)
	}
}

func TestDispatchInvalidEvidencePersistsFailedOutcome(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	registryStore := newMemoryStore()
	executor := &dispatchExecutor{result: ExecutionResult{
		Output: json.RawMessage(`{"ok":true}`),
		Evidence: []EvidenceRef{{
			NodeIDs: []shoal.ID{"node"},
		}},
	}}
	registry, _ := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(),
		Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": executor},
		Clock:     func() time.Time { return now },
	})
	registryStore.records["agent"] = Stored{Descriptor: dispatchDescriptor(now)}
	store := newMemoryDispatchStore()
	service, _ := NewDispatchService(DispatchConfig{
		Store: store, Registry: registry, Resolver: authority.Resolver(),
		Recorder: &dispatchRecorder{}, Events: dispatchEvents{},
		Clock: func() time.Time { return now },
	})
	decision := dispatchDecision(
		t, "owner", "actor", "request",
		auth.OperationDispatch, auth.OperationInvoke,
	)
	ctx := bindDecision(t, authority, decision)
	result, err := service.Invoke(ctx, InvokeRequest{
		Enqueue: dispatchEnqueue(now, "request"), ClaimID: []byte("claim"),
		Lease: time.Minute,
	})
	if err == nil || result.State != DispatchFailed || len(result.Evidence) != 0 {
		t.Fatalf("invalid evidence result = %#v, %v", result, err)
	}
	stored, err := store.GetAction(ctx, result.ID)
	if err != nil || stored.State != DispatchFailed || len(stored.Evidence) != 0 {
		t.Fatalf("invalid evidence stored = %#v, %v", stored, err)
	}
}

func TestDispatchSchemaRejectsWrongKeywordTypes(t *testing.T) {
	for name, schema := range map[string]json.RawMessage{
		"type":                 json.RawMessage(`{"type":1}`),
		"properties":           json.RawMessage(`{"properties":[]}`),
		"required":             json.RawMessage(`{"required":"value"}`),
		"items":                json.RawMessage(`{"items":[]}`),
		"additionalProperties": json.RawMessage(`{"additionalProperties":"false"}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := validateAgainstSchema(
				schema, json.RawMessage(`{}`), "action input", MaxActionPayloadBytes,
			); err == nil {
				t.Fatal("wrong schema keyword type was accepted")
			}
		})
	}
}

func TestDispatchSchemaInfersObjectFromObjectKeywords(t *testing.T) {
	schema := json.RawMessage(`{"properties":{"value":{"type":"integer"}},"required":["value"]}`)
	if _, err := validateAgainstSchema(
		schema, json.RawMessage(`{"value":"wrong"}`), "action input", MaxActionPayloadBytes,
	); err == nil {
		t.Fatal("implicit object schema accepted an invalid property")
	}
}

type memoryDispatchStore struct {
	mu      sync.Mutex
	records map[string]ActionRecord
}

type uniqueTokenDispatchStore struct {
	*memoryDispatchStore
	tokens map[string]struct{}
}

func (s *uniqueTokenDispatchStore) ApplyAction(
	ctx context.Context,
	mutation DispatchMutation,
) (ActionRecord, error) {
	token := string(mutation.Token)
	if _, exists := s.tokens[token]; exists {
		return ActionRecord{}, ErrActionConflict
	}
	s.tokens[token] = struct{}{}
	return s.memoryDispatchStore.ApplyAction(ctx, mutation)
}

func newMemoryDispatchStore() *memoryDispatchStore {
	return &memoryDispatchStore{records: make(map[string]ActionRecord)}
}

func (s *memoryDispatchStore) GetAction(_ context.Context, id []byte) (ActionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[string(id)]
	if !ok {
		return ActionRecord{}, ErrActionNotFound
	}
	return cloneActionRecord(record), nil
}

func (s *memoryDispatchStore) ApplyAction(_ context.Context, mutation DispatchMutation) (ActionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.records[string(mutation.Record.ID)]
	if mutation.ExpectedVersion == 0 {
		if exists {
			if equivalentEnqueue(current, mutation.Record) {
				return cloneActionRecord(current), nil
			}
			return ActionRecord{}, ErrActionConflict
		}
	} else if !exists || current.Version != mutation.ExpectedVersion ||
		current.ClaimFence != mutation.ExpectedFence {
		return ActionRecord{}, ErrActionConflict
	}
	s.records[string(mutation.Record.ID)] = cloneActionRecord(mutation.Record)
	return cloneActionRecord(mutation.Record), nil
}

func (s *memoryDispatchStore) ScanActions(_ context.Context, after []byte, limit int) (ActionPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := ActionPage{}
	for key, record := range s.records {
		if key <= string(after) {
			continue
		}
		result.Actions = append(result.Actions, cloneActionRecord(record))
		if len(result.Actions) == limit {
			result.Next = append([]byte(nil), record.ID...)
			break
		}
	}
	return result, nil
}

func TestActionEventKindsAreStable(t *testing.T) {
	for state, want := range map[DispatchState]string{
		DispatchQueued:    "action.enqueued",
		DispatchClaimed:   "action.claimed",
		DispatchSucceeded: "action.completed",
		DispatchFailed:    "action.failed",
		DispatchCanceled:  "action.canceled",
	} {
		if got := actionEventKind(ActionRecord{State: state}); got != want {
			t.Fatalf("state %q event kind = %q, want %q", state, got, want)
		}
	}
	if got := actionEventKind(ActionRecord{}); got != "" {
		t.Fatalf("invalid state event kind = %q", got)
	}
}

func TestDispatchTupleKeysAreUnambiguousForOpaqueBytes(t *testing.T) {
	tests := []struct {
		name                string
		firstID, firstKey   []byte
		secondID, secondKey []byte
	}{
		{
			name:    "embedded NUL",
			firstID: []byte("a\x00b"), firstKey: []byte("c"),
			secondID: []byte("a"), secondKey: []byte("b\x00c"),
		},
		{
			name:    "invalid UTF-8 with embedded NUL",
			firstID: []byte{0xff, 0, 0xfe}, firstKey: []byte{0xfd},
			secondID: []byte{0xff}, secondKey: []byte{0xfe, 0, 0xfd},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !bytes.Equal(
				legacyDispatchTuple(test.firstID, test.firstKey),
				legacyDispatchTuple(test.secondID, test.secondKey),
			) {
				t.Fatal("regression tuple does not reproduce the legacy collision")
			}
			if bytes.Equal(
				executorKey(test.firstID, test.firstKey),
				executorKey(test.secondID, test.secondKey),
			) {
				t.Fatal("distinct executor-key tuples collided")
			}
			if bytes.Equal(
				transitionToken("claim", test.firstID, test.firstKey, 2),
				transitionToken("claim", test.secondID, test.secondKey, 2),
			) {
				t.Fatal("distinct transition-token tuples collided")
			}
		})
	}
}

func legacyDispatchTuple(actionID, discriminator []byte) []byte {
	result := append([]byte(nil), actionID...)
	result = append(result, 0)
	return append(result, discriminator...)
}

func TestDispatchExecutesPreviouslyAmbiguousActionsIndependently(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	registryStore := newMemoryStore()
	executor := &dispatchExecutor{
		result: ExecutionResult{Output: json.RawMessage(`{"ok":true}`)},
	}
	registry, err := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(),
		Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": executor},
		Clock:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	registryStore.records["agent"] = Stored{
		Descriptor: dispatchDescriptor(now),
	}
	store := newMemoryDispatchStore()
	service, err := NewDispatchService(DispatchConfig{
		Store: store, Registry: registry, Resolver: authority.Resolver(),
		Recorder: &dispatchRecorder{}, Events: dispatchEvents{},
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	decision := dispatchDecision(
		t, "owner", "actor", "request",
		auth.OperationDispatch, auth.OperationInvoke,
	)
	ctx := bindDecision(t, authority, decision)
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
		enqueue := dispatchEnqueue(now, "request")
		enqueue.ID = request.id
		enqueue.IdempotencyKey = request.key
		completed, invokeErr := service.Invoke(ctx, InvokeRequest{
			Enqueue: enqueue, ClaimID: request.claim, Lease: time.Minute,
		})
		if invokeErr != nil {
			t.Fatalf("invoke %x = %v", request.id, invokeErr)
		}
		if completed.State != DispatchSucceeded {
			t.Fatalf("invoke %x state = %q", request.id, completed.State)
		}
		stored, readErr := store.GetAction(ctx, request.id)
		if readErr != nil || stored.State != DispatchSucceeded {
			t.Fatalf("stored %x = %#v, %v", request.id, stored, readErr)
		}
	}
	if executor.calls != 2 || len(executor.keys) != 2 {
		t.Fatalf("executor calls/keys = %d/%d", executor.calls, len(executor.keys))
	}
	if bytes.Equal(executor.keys[0], executor.keys[1]) {
		t.Fatalf("distinct actions shared executor key %x", executor.keys[0])
	}
}

func TestDispatchEnqueueTokenCannotAliasClaimTransition(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	registryStore := newMemoryStore()
	registry, err := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(),
		Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": &dispatchExecutor{}},
		Clock:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	registryStore.records["agent"] = Stored{
		Descriptor: dispatchDescriptor(now),
	}
	actionID := []byte("action")
	claimID := []byte("claim")
	hostileIdempotency := transitionToken("claim", actionID, claimID, 2)
	store := &uniqueTokenDispatchStore{
		memoryDispatchStore: newMemoryDispatchStore(),
		tokens:              make(map[string]struct{}),
	}
	service, err := NewDispatchService(DispatchConfig{
		Store: store, Registry: registry, Resolver: authority.Resolver(),
		Recorder: &dispatchRecorder{}, Events: dispatchEvents{},
		Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	decision := dispatchDecision(
		t, "owner", "actor", "request",
		auth.OperationDispatch, auth.OperationInvoke,
	)
	ctx := bindDecision(t, authority, decision)
	enqueue := dispatchEnqueue(now, "request")
	enqueue.ID = actionID
	enqueue.IdempotencyKey = hostileIdempotency
	queued, err := service.Enqueue(ctx, enqueue)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Claim(ctx, ClaimRequest{
		ID: queued.ID, ExpectedVersion: queued.Version,
		ClaimID: claimID, Lease: time.Minute,
		Context: dispatchContext(now, "request"),
	}); err != nil {
		t.Fatalf("claim collided with caller enqueue key: %v", err)
	}
}

func TestDispatchRestartPreservesStoredExecutorKeyAcrossAmbiguousRetry(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	registryStore := newMemoryStore()
	executor := &dispatchExecutor{
		result: ExecutionResult{Output: json.RawMessage(`{"ok":true}`)},
	}
	registry, err := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(),
		Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
		Executors: executorMap{"exec": executor},
		Clock:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	registryStore.records["agent"] = Stored{
		Descriptor: dispatchDescriptor(now),
	}
	store := newMemoryDispatchStore()
	newService := func(recorder ActionRecorder) *DispatchService {
		t.Helper()
		service, serviceErr := NewDispatchService(DispatchConfig{
			Store: store, Registry: registry, Resolver: authority.Resolver(),
			Recorder: recorder, Events: dispatchEvents{},
			Clock: func() time.Time { return now },
		})
		if serviceErr != nil {
			t.Fatal(serviceErr)
		}
		return service
	}
	decision := dispatchDecision(
		t, "owner", "actor", "request",
		auth.OperationDispatch, auth.OperationInvoke,
	)
	ctx := bindDecision(t, authority, decision)
	request := dispatchEnqueue(now, "request")
	if _, err := newService(&dispatchRecorder{}).Enqueue(ctx, request); err != nil {
		t.Fatal(err)
	}
	legacyKey := []byte("persisted-v1-executor-key")
	store.mu.Lock()
	stored := store.records[string(request.ID)]
	stored.ExecutorKey = append([]byte(nil), legacyKey...)
	store.records[string(request.ID)] = stored
	store.mu.Unlock()

	failingRecorder := &dispatchRecorder{failPhase: "effect_outcome"}
	_, err = newService(failingRecorder).Invoke(ctx, InvokeRequest{
		Enqueue: request, ClaimID: []byte("claim"), Lease: time.Minute,
	})
	if !errors.Is(err, ErrExecutionAmbiguous) {
		t.Fatalf("first post-restart execution = %v", err)
	}
	completed, err := newService(&dispatchRecorder{}).Invoke(
		ctx,
		InvokeRequest{
			Enqueue: request, ClaimID: []byte("claim"), Lease: time.Minute,
		},
	)
	if err != nil || completed.State != DispatchSucceeded {
		t.Fatalf("ambiguous retry = %#v, %v", completed, err)
	}
	if len(executor.keys) != 2 ||
		!bytes.Equal(executor.keys[0], legacyKey) ||
		!bytes.Equal(executor.keys[1], legacyKey) ||
		!bytes.Equal(completed.ExecutorKey, legacyKey) {
		t.Fatalf(
			"stored executor key rerolled across restart/retry: %#v",
			executor.keys,
		)
	}
}

type dispatchRecorder struct {
	failPhase string
	phases    []string
	advance   func()
}

func (r *dispatchRecorder) RecordAction(_ context.Context, audit ActionAudit) error {
	r.phases = append(r.phases, audit.Phase)
	if r.advance != nil && audit.Phase == "effect_admission" {
		r.advance()
	}
	if audit.Phase == r.failPhase {
		return errors.New("recorder unavailable")
	}
	return nil
}

type dispatchEvents struct{}

func (dispatchEvents) PublishActionEvent(context.Context, string, ActionRecord) error { return nil }

type controlledDispatchEvents struct {
	calls int
	err   error
}

type terminalFailEvents struct {
	calls int
	err   error
}

func (e *terminalFailEvents) PublishActionEvent(_ context.Context, kind string, _ ActionRecord) error {
	if kind == "action.completed" || kind == "action.failed" {
		e.calls++
		return e.err
	}
	return nil
}

func (e *controlledDispatchEvents) PublishActionEvent(context.Context, string, ActionRecord) error {
	e.calls++
	return e.err
}

type dispatchExecutor struct {
	calls       int
	keys        [][]byte
	invocations []Invocation
	result      ExecutionResult
	err         error
	advance     func()
}

func (e *dispatchExecutor) Execute(_ context.Context, invocation Invocation) (ExecutionResult, error) {
	e.calls++
	e.keys = append(e.keys, append([]byte(nil), invocation.IdempotencyKey...))
	e.invocations = append(e.invocations, invocation)
	if e.advance != nil {
		e.advance()
	}
	return e.result, e.err
}

func dispatchDescriptor(now time.Time) Descriptor {
	return Descriptor{
		ID: "agent", Generation: 1, Subject: "owner", Actor: "actor",
		AuthorizationDomain: []byte("domain"),
		Scopes:              []Scope{{SourceID: []byte("source"), PolicyID: []byte("policy")}},
		ExecutorRef:         "exec", LeaseExpiresAt: now.Add(time.Hour), UpdatedAt: now,
		Capabilities: []Capability{{Name: "search", Actions: []Action{{
			Name:         "query",
			InputSchema:  json.RawMessage(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
			OutputSchema: json.RawMessage(`{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}`),
		}}}},
	}
}

func dispatchDecision(t *testing.T, subject, actor, request string, operations ...auth.Operation) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: shoal.ID(subject), Actor: shoal.ID(actor), AuthorizationDomain: []byte("domain"),
		AllowedOperations: operations, PermittedSourceIDs: [][]byte{[]byte("source")},
		PermittedPolicyIDs: [][]byte{[]byte("policy")}, PolicyGeneration: 1,
		AuthenticationExpires: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
		RequestID:             shoal.ID(request), CorrelationID: "correlation",
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

func dispatchContext(now time.Time, request string) RequestContext {
	return RequestContext{
		RequestID: shoal.ID(request), CorrelationID: "correlation",
		ReasonCode: "operator_request", Deadline: now.Add(time.Hour),
	}
}

func dispatchEnqueue(now time.Time, request string) EnqueueRequest {
	return EnqueueRequest{
		ID: []byte("action"), IdempotencyKey: []byte("idempotency"), AgentID: "agent",
		AgentGeneration: 1, Capability: "search", Action: "query",
		SourceID: []byte("source"), PolicyID: []byte("policy"), ObjectID: "object",
		Input: json.RawMessage(`{"value":1}`), Context: dispatchContext(now, request),
	}
}
