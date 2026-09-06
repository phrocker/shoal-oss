/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package explorerfleetevents

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/transaction"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleetevents"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestActionEventPublisherReplayAndDivergence(t *testing.T) {
	now := time.Date(2026, 9, 6, 18, 0, 0, 0, time.UTC)
	config := runtimeConfig(t.TempDir())
	runtime, err := explorercoord.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	backend, err := New(runtime, config.Domain)
	if err != nil {
		t.Fatal(err)
	}
	publisher := dispatchEventPublisher(
		t, backend, now, restartGenerationReader{}, restartAuditor{},
		auth.OperationInvoke)
	record := authorizedActionRecord(fleet.ActionRecord{
		ID: []byte{0, 'a', '|', 'b'}, Version: 7,
		State: fleet.DispatchSucceeded, AgentID: "agent-\x00one", AgentGeneration: 3,
		CorrelationID: "correlation", SourceID: []byte("source"),
		PolicyID: []byte("policy"), ObjectID: "object",
		ExecutorKey: []byte{'e', 0, 'x'},
		Reason:      interaction.Reason{Code: "completed"}, UpdatedAt: now.Add(-time.Minute),
	}, now, auth.OperationInvoke)
	if err := publisher.PublishActionEvent(context.Background(), "action.completed", record); err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishActionEvent(context.Background(), "action.completed", record); err != nil {
		t.Fatalf("identical replay failed: %v", err)
	}
	events, _, err := backend.Scan(context.Background(), 1, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("replay committed %d events, want 1", len(events))
	}
	event := events[0]
	if event.Kind != "action.completed" ||
		!bytes.Equal(event.ProducerID, []byte(record.AgentID)) ||
		event.ProducerGeneration != record.AgentGeneration ||
		!bytes.Equal(event.ActionID, record.ID) ||
		len(event.TransitionID) != sha256.Size ||
		bytes.Equal(event.TransitionID, record.ExecutorKey) ||
		!bytes.Equal(event.CorrelationID, []byte(record.CorrelationID)) ||
		event.Reason != record.Reason || !event.OccurredAt.Equal(record.UpdatedAt) {
		t.Fatalf("mapped event = %#v", event)
	}
	if len(event.Evidence) != 1 ||
		!bytes.Equal(event.Evidence[0].SourceID, record.SourceID) ||
		!bytes.Equal(event.Evidence[0].PolicyID, record.PolicyID) ||
		event.Evidence[0].ObjectID != record.ObjectID {
		t.Fatalf("mapped evidence = %#v", event.Evidence)
	}

	divergent := record
	divergent.Reason = interaction.Reason{Code: "changed"}
	if err := publisher.PublishActionEvent(
		context.Background(), "action.completed", divergent,
	); !errors.Is(err, transaction.ErrConflict) {
		t.Fatalf("divergent replay error = %v, want conflict", err)
	}
	divergent = record
	divergent.ExecutorKey = []byte("changed-transition")
	if err := publisher.PublishActionEvent(
		context.Background(), "action.completed", divergent,
	); !errors.Is(err, transaction.ErrConflict) {
		t.Fatalf("transition replay error = %v, want conflict", err)
	}
	divergent = record
	divergent.AgentGeneration++
	if err := publisher.PublishActionEvent(
		context.Background(), "action.completed", divergent,
	); !errors.Is(err, transaction.ErrConflict) {
		t.Fatalf("generation replay error = %v, want conflict", err)
	}
	record.Version++
	if err := publisher.PublishActionEvent(context.Background(), "action.completed", record); err != nil {
		t.Fatalf("new action version failed: %v", err)
	}
	events, _, err = backend.Scan(context.Background(), 1, 0, 10)
	if err != nil || len(events) != 2 {
		t.Fatalf("versioned events = %d, %v", len(events), err)
	}
}

func TestActionEventPublisherPreservesPublicationAmbiguity(t *testing.T) {
	now := time.Date(2026, 9, 6, 18, 0, 0, 0, time.UTC)
	config := runtimeConfig(t.TempDir())
	runtime, err := explorercoord.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	backend, err := New(runtime, config.Domain)
	if err != nil {
		t.Fatal(err)
	}
	recordingFailure := errors.New("recording failed")
	publisher := dispatchEventPublisher(
		t, backend, now, restartGenerationReader{},
		failingDispatchAuditor{err: recordingFailure}, auth.OperationInvoke)
	err = publisher.PublishActionEvent(context.Background(), "action.failed", authorizedActionRecord(fleet.ActionRecord{
		ID: []byte("action"), Version: 1, State: fleet.DispatchFailed,
		AgentID: "agent", AgentGeneration: 1, ExecutorKey: []byte("executor"),
		CorrelationID: "correlation", SourceID: []byte("source"),
		PolicyID: []byte("policy"), ObjectID: "object",
		Reason: interaction.Reason{Code: "failed"}, UpdatedAt: now,
	}, now, auth.OperationInvoke))
	if !errors.Is(err, fleetevents.ErrAuditOutcomeUnknown) ||
		!errors.Is(err, recordingFailure) {
		t.Fatalf("publication error = %v", err)
	}
	events, _, scanErr := backend.Scan(context.Background(), 1, 0, 10)
	if scanErr != nil || len(events) != 1 {
		t.Fatalf("committed ambiguous event = %d, %v", len(events), scanErr)
	}
}

func TestActionEventPublisherRetriesIndeterminateReceiptWithoutDuplicate(t *testing.T) {
	now := time.Date(2026, 9, 6, 18, 0, 0, 0, time.UTC)
	config := runtimeConfig(t.TempDir())
	runtime, err := explorercoord.Open(config)
	if err != nil {
		t.Fatal(err)
	}

	defer runtime.Close()
	backend, err := New(runtime, config.Domain)
	if err != nil {
		t.Fatal(err)
	}
	auditor := &failingDispatchAuditor{err: errors.New("recording failed")}
	publisher := dispatchEventPublisher(
		t, backend, now, restartGenerationReader{}, auditor, auth.OperationInvoke)
	record := authorizedActionRecord(fleet.ActionRecord{
		ID: []byte("action"), Version: 1, State: fleet.DispatchSucceeded,
		AgentID: "agent", AgentGeneration: 1, ExecutorKey: []byte("executor"),
		CorrelationID: "correlation", SourceID: []byte("source"),
		PolicyID: []byte("policy"), ObjectID: "object",
		Reason: interaction.Reason{Code: "completed"}, UpdatedAt: now,
	}, now, auth.OperationInvoke)
	if err := publisher.PublishActionEvent(
		context.Background(), "action.completed", record,
	); !errors.Is(err, fleetevents.ErrAuditOutcomeUnknown) {
		t.Fatalf("first publication error = %v", err)
	}
	auditor.err = nil
	if err := publisher.PublishActionEvent(
		context.Background(), "action.completed", record,
	); err != nil {
		t.Fatalf("receipt retry = %v", err)
	}
	events, _, err := backend.Scan(context.Background(), 1, 0, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("events after receipt retry = %d, %v", len(events), err)
	}
}

func TestActionEventPublisherRetrySurvivesPolicyGenerationChange(t *testing.T) {
	now := time.Date(2026, 9, 6, 18, 0, 0, 0, time.UTC)
	config := runtimeConfig(t.TempDir())
	runtime, err := explorercoord.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	backend, err := New(runtime, config.Domain)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &mutableDispatchResolver{}
	resolver.set(t, now, 1, auth.OperationInvoke)
	generations := &mutableDispatchGeneration{generation: 1}
	var receipts []fleetevents.AuditRecord
	auditor := &failingDispatchAuditor{
		err: errors.New("recording failed"), records: &receipts,
	}
	service, err := fleetevents.New(fleetevents.Config{
		Backend: backend, Resolver: resolver, GenerationReader: generations,
		LeaseValidator: dispatchLeaseValidator{}, Auditor: auditor,
		CursorKey: bytes.Repeat([]byte{7}, 32),
		Clock:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewActionEventPublisher(
		service, resolver, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	record := authorizedActionRecord(fleet.ActionRecord{
		ID: []byte("action"), Version: 9, State: fleet.DispatchSucceeded,
		AgentID: "agent", AgentGeneration: 4, ExecutorKey: []byte("executor"),
		SourceID: []byte("source"), PolicyID: []byte("policy"), ObjectID: "object",
		Reason: interaction.Reason{Code: "completed"}, UpdatedAt: now,
	}, now, auth.OperationInvoke)
	if err := publisher.PublishActionEvent(
		context.Background(), "action.completed", record,
	); !errors.Is(err, fleetevents.ErrAuditOutcomeUnknown) {
		t.Fatalf("first publication = %v", err)
	}
	resolver.set(t, now, 2, auth.OperationInvoke)
	generations.mu.Lock()
	generations.generation = 2
	generations.mu.Unlock()
	auditor.err = nil
	if err := publisher.PublishActionEvent(
		context.Background(), "action.completed", record,
	); err != nil {
		t.Fatalf("policy-changed retry = %v", err)
	}
	events, _, err := backend.Scan(context.Background(), 1, 0, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("policy retry events = %d, %v", len(events), err)
	}
	if len(receipts) != 2 ||
		receipts[0].AuthorizationFingerprint != record.ExecutionFingerprint ||
		receipts[1].AuthorizationFingerprint != record.ExecutionFingerprint ||
		!receipts[0].AuthorizationExpiresAt.Equal(record.ExecutionExpiresAt) ||
		!receipts[1].AuthorizationExpiresAt.Equal(record.ExecutionExpiresAt) {
		t.Fatalf("lifecycle receipt pins = %#v", receipts)
	}
}

func TestActionEventPublisherPreservesExecutorEvidence(t *testing.T) {
	now := time.Date(2026, 9, 6, 18, 0, 0, 0, time.UTC)
	backend := &recordingBackend{}
	publisher := dispatchEventPublisher(
		t, backend, now, restartGenerationReader{}, restartAuditor{},
		auth.OperationInvoke)
	record := authorizedActionRecord(fleet.ActionRecord{
		ID: []byte("action"), Version: 2, State: fleet.DispatchSucceeded,
		AgentID: "agent", AgentGeneration: 1, ExecutorKey: []byte("executor"),
		SourceID: []byte("source"), PolicyID: []byte("policy"), ObjectID: "object",
		Reason: interaction.Reason{Code: "completed"}, UpdatedAt: now,
		Evidence: []fleet.EvidenceRef{{
			NodeIDs: []shoal.ID{"node"}, EdgeIDs: []shoal.ID{"edge"},
			AnchorID:   "anchor",
			Visibility: []string{"A", "B"},
		}},
	}, now, auth.OperationInvoke)
	if err := publisher.PublishActionEvent(
		context.Background(), "action.completed", record,
	); err != nil {
		t.Fatal(err)
	}
	if backend.appends != 1 || len(backend.request.Event.Evidence) != 2 {
		t.Fatalf("preserved evidence = %#v", backend.request.Event.Evidence)
	}
	evidence := backend.request.Event.Evidence[1]
	if evidence.NodeID != "node" || evidence.EdgeID != "edge" ||
		evidence.AnchorID != "anchor" ||
		!reflect.DeepEqual(evidence.Visibility, []string{"A", "B"}) {
		t.Fatalf("executor evidence = %#v", evidence)
	}
	record.Evidence[0].Visibility[0] = "changed"
	if evidence.Visibility[0] != "A" {
		t.Fatal("event evidence visibility aliases the action record")
	}
}

func TestActionEventPublisherPreservesCommittedAuthorizationFailure(t *testing.T) {
	now := time.Date(2026, 9, 6, 18, 0, 0, 0, time.UTC)
	config := runtimeConfig(t.TempDir())
	runtime, err := explorercoord.Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	backend, err := New(runtime, config.Domain)
	if err != nil {
		t.Fatal(err)
	}
	publisher := dispatchEventPublisher(
		t, backend, now, new(changingDispatchGeneration), restartAuditor{},
		auth.OperationDispatch)
	err = publisher.PublishActionEvent(context.Background(), "action.canceled", authorizedActionRecord(fleet.ActionRecord{
		ID: []byte("action"), Version: 1, State: fleet.DispatchCanceled,
		AgentID: "agent", AgentGeneration: 1, CancelKey: []byte("cancel"),
		CorrelationID: "correlation", SourceID: []byte("source"),
		PolicyID: []byte("policy"), ObjectID: "object",
		Reason: interaction.Reason{Code: "canceled"}, UpdatedAt: now,
	}, now, auth.OperationDispatch))
	if !errors.Is(err, fleetevents.ErrActionCommitted) {
		t.Fatalf("authorization race error = %v", err)
	}
	events, _, scanErr := backend.Scan(context.Background(), 1, 0, 10)
	if scanErr != nil || len(events) != 1 {
		t.Fatalf("committed event after authorization race = %d, %v", len(events), scanErr)
	}
}

func TestActionEventTokenIsLengthFramed(t *testing.T) {
	baseRecord := fleet.ActionRecord{
		ID: []byte("action"), Version: 4, State: fleet.DispatchSucceeded,
		AgentID: "agent", AgentGeneration: 7, ExecutorKey: []byte("transition"),
	}
	base := mustActionEventToken(t, "action.completed", baseRecord)
	changedKind := baseRecord
	changedKind.State = fleet.DispatchFailed
	changedAction := baseRecord
	changedAction.ID = []byte("different")
	changedAgent := baseRecord
	changedAgent.AgentID = "other"
	changedGeneration := baseRecord
	changedGeneration.AgentGeneration++
	changedTransition := baseRecord
	changedTransition.ExecutorKey = []byte("different")
	changedVersion := baseRecord
	changedVersion.Version++
	for name, token := range map[string][]byte{
		"kind":    mustActionEventToken(t, "action.failed", changedKind),
		"action":  mustActionEventToken(t, "action.completed", changedAction),
		"version": mustActionEventToken(t, "action.completed", changedVersion),
	} {
		if bytes.Equal(base, token) {
			t.Fatalf("different %s produced the same token", name)
		}
	}
	for name, token := range map[string][]byte{
		"agent":      mustActionEventToken(t, "action.completed", changedAgent),
		"generation": mustActionEventToken(t, "action.completed", changedGeneration),
		"transition": mustActionEventToken(t, "action.completed", changedTransition),
	} {
		if !bytes.Equal(base, token) {
			t.Fatalf("different %s changed the stable publication token", name)
		}
	}
	left := baseRecord
	left.ID = []byte{'a', 0, 'b'}
	left.ExecutorKey = []byte("c")
	right := baseRecord
	right.ID = []byte("a")
	right.ExecutorKey = []byte{'b', 0, 'c'}
	if bytes.Equal(
		mustActionEventToken(t, "action.completed", left),
		mustActionEventToken(t, "action.completed", right),
	) {
		t.Fatal("ambiguous fields produced the same token")
	}
}

func TestActionEventTokenUsesCanonicalTransitionIdentity(t *testing.T) {
	tests := []struct {
		kind      string
		state     fleet.DispatchState
		configure func(*fleet.ActionRecord)
	}{
		{"action.enqueued", fleet.DispatchQueued, func(record *fleet.ActionRecord) {
			record.IdempotencyKey = []byte("enqueue")
		}},
		{"action.claimed", fleet.DispatchClaimed, func(record *fleet.ActionRecord) {
			record.ClaimID = []byte("claim")
		}},
		{"action.completed", fleet.DispatchSucceeded, func(record *fleet.ActionRecord) {
			record.ExecutorKey = []byte("execute")
		}},
		{"action.failed", fleet.DispatchFailed, func(record *fleet.ActionRecord) {
			record.ExecutorKey = []byte("execute")
		}},
		{"action.canceled", fleet.DispatchCanceled, func(record *fleet.ActionRecord) {
			record.CancelKey = []byte("cancel")
		}},
	}
	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			record := fleet.ActionRecord{
				ID: []byte("action"), Version: 2, State: test.state,
				AgentID: "agent", AgentGeneration: 3,
			}
			test.configure(&record)
			originalToken, originalTransition, err := actionEventIdentities(test.kind, record)
			if err != nil {
				t.Fatal(err)
			}
			changed := record
			switch test.kind {
			case "action.enqueued":
				changed.IdempotencyKey = []byte("different")
			case "action.claimed":
				changed.ClaimID = []byte("different")
			case "action.completed", "action.failed":
				changed.ExecutorKey = []byte("different")
			case "action.canceled":
				changed.CancelKey = []byte("different")
			}
			changedToken, changedTransition, err := actionEventIdentities(test.kind, changed)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(originalToken, changedToken) ||
				bytes.Equal(originalTransition, changedTransition) ||
				len(originalTransition) != sha256.Size {
				t.Fatal("transition discriminator did not alter only the transition ID")
			}
			record.AgentGeneration++
			generationToken, generationTransition, err := actionEventIdentities(test.kind, record)
			if err != nil {
				t.Fatal(err)
			}
			baseToken, baseTransition, err := actionEventIdentities(test.kind, fleet.ActionRecord{
				ID: []byte("action"), Version: 2, State: test.state,
				AgentID: "agent", AgentGeneration: 3,
				IdempotencyKey: record.IdempotencyKey,
				ClaimID:        record.ClaimID, CancelKey: record.CancelKey,
				ExecutorKey: record.ExecutorKey,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(generationToken, baseToken) ||
				!bytes.Equal(generationTransition, baseTransition) {
				t.Fatal("agent generation altered token or transition ID")
			}
			record.AgentGeneration = 3
			record.AgentID = "different"
			agentToken, agentTransition, err := actionEventIdentities(test.kind, record)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(agentToken, baseToken) ||
				!bytes.Equal(agentTransition, baseTransition) {
				t.Fatal("agent identity altered token or transition ID")
			}
		})
	}
}

func TestActionTransitionIDUsesOpaqueLengthFraming(t *testing.T) {
	left := fleet.ActionRecord{
		ID: []byte{'a', 0, 'b'}, Version: 1, State: fleet.DispatchSucceeded,
		AgentID:         shoal.ID(string([]byte{'x', 0xff, 0, 'y'})),
		AgentGeneration: 7, ExecutorKey: []byte("c"),
	}
	right := fleet.ActionRecord{
		ID: []byte("a"), Version: 1, State: fleet.DispatchSucceeded,
		AgentID:         shoal.ID(string([]byte{'x', 0xff})),
		AgentGeneration: 7, ExecutorKey: []byte{'b', 0, 'c'},
	}
	leftToken, leftTransition, err := actionEventIdentities("action.completed", left)
	if err != nil {
		t.Fatal(err)
	}
	rightToken, rightTransition, err := actionEventIdentities("action.completed", right)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(leftToken, rightToken) {
		t.Fatal("length-framed action identities collided")
	}
	if bytes.Equal(leftTransition, rightTransition) {
		t.Fatal("length-framed opaque transition identities collided")
	}
	if bytes.Equal(leftTransition, left.ExecutorKey) ||
		bytes.Equal(rightTransition, right.ExecutorKey) {
		t.Fatal("transition ID exposed a raw dispatch discriminator")
	}
}

func TestActionEventTokenRejectsMismatchedOrMissingTransition(t *testing.T) {
	record := fleet.ActionRecord{
		ID: []byte("action"), Version: 1, State: fleet.DispatchSucceeded,
		AgentID: "agent", AgentGeneration: 1, ExecutorKey: []byte("executor"),
	}
	if _, err := actionEventToken("action.failed", record); err == nil {
		t.Fatal("mismatched event kind succeeded")
	}
	record.ExecutorKey = nil
	if _, err := actionEventToken("action.completed", record); err == nil {
		t.Fatal("missing transition identity succeeded")
	}
	if _, err := actionEventToken("action.unknown", record); err == nil {
		t.Fatal("unknown event kind succeeded")
	}
}

func TestNewActionEventPublisherRejectsMissingDependencies(t *testing.T) {
	if _, err := NewActionEventPublisher(
		nil, nil, nil,
	); err == nil {
		t.Fatal("missing dependencies succeeded")
	}
}

func mustActionEventToken(
	t *testing.T, kind string, record fleet.ActionRecord,
) []byte {
	t.Helper()
	token, err := actionEventToken(kind, record)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func dispatchEventPublisher(
	t *testing.T, backend fleetevents.Backend, now time.Time,
	generations auth.GenerationReader, auditor fleetevents.Auditor,
	operation auth.Operation,
) *ActionEventPublisher {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "dispatcher", Actor: "dispatcher", RequestID: "request",
		CorrelationID:       "correlation",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations:   []auth.Operation{operation},
		PermittedSourceIDs:  [][]byte{[]byte("source")},
		PermittedPolicyIDs:  [][]byte{[]byte("policy")},
		PolicyGeneration:    1, AuthenticationExpires: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := auth.NewStaticResolverWithClock(decision, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	service, err := fleetevents.New(fleetevents.Config{
		Backend: backend, Resolver: resolver, GenerationReader: generations,
		LeaseValidator: dispatchLeaseValidator{}, Auditor: auditor,
		CursorKey: bytes.Repeat([]byte{7}, 32),
		Clock:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := NewActionEventPublisher(
		service, resolver, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return publisher
}

type dispatchLeaseValidator struct{}

func (dispatchLeaseValidator) ValidateDelivery(
	context.Context, shoal.ID, int64,
) error {
	return nil
}

func authorizedActionRecord(
	record fleet.ActionRecord, now time.Time, operation auth.Operation,
) fleet.ActionRecord {
	record.Subject = "dispatcher"
	record.Actor = "dispatcher"
	record.RequestID = "request"
	record.CorrelationID = "correlation"
	record.AuthorizedOperations = []auth.Operation{operation}
	if operation == auth.OperationInvoke {
		record.ExecutionFingerprint = auth.Fingerprint{2}
		record.ExecutionPolicyGeneration = 1
		record.ExecutionExpiresAt = now.Add(time.Hour)
	} else {
		record.AuthorizationFingerprint = auth.Fingerprint{1}
		record.PolicyGeneration = 1
		record.AuthorizationExpiresAt = now.Add(time.Hour)
	}
	return record
}

type failingDispatchAuditor struct {
	err     error
	records *[]fleetevents.AuditRecord
}

func (a failingDispatchAuditor) RecordFleetAction(
	_ context.Context, record fleetevents.AuditRecord,
) error {
	if a.records != nil {
		*a.records = append(*a.records, record)
	}
	return a.err
}

type changingDispatchGeneration struct {
	calls atomic.Int64
}

type mutableDispatchGeneration struct {
	mu         sync.Mutex
	generation int64
}

func (g *mutableDispatchGeneration) CurrentPolicyGeneration(
	context.Context, []byte,
) (int64, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.generation, nil
}

type mutableDispatchResolver struct {
	mu       sync.Mutex
	decision auth.Decision
}

func (r *mutableDispatchResolver) Resolve(context.Context) (auth.Decision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.decision, nil
}

func (r *mutableDispatchResolver) set(
	t *testing.T, now time.Time, generation int64, operation auth.Operation,
) {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "dispatcher", Actor: "dispatcher", RequestID: "request",
		CorrelationID: "correlation", AuthorizationDomain: []byte("domain"),
		AllowedOperations:  []auth.Operation{operation},
		PermittedSourceIDs: [][]byte{[]byte("source")},
		PermittedPolicyIDs: [][]byte{[]byte("policy")},
		PolicyGeneration:   generation, AuthenticationExpires: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.decision = decision
	r.mu.Unlock()
}

type recordingBackend struct {
	appends int
	request fleetevents.PublishRequest
}

func (*recordingBackend) Create(
	context.Context, fleetevents.CreateRequest, auth.Fingerprint, int64, time.Time,
) (fleetevents.Subscription, bool, error) {
	return fleetevents.Subscription{}, false, errors.New("unexpected create")
}
func (*recordingBackend) Subscription(
	context.Context, []byte,
) (fleetevents.Subscription, error) {
	return fleetevents.Subscription{}, errors.New("unexpected subscription")
}
func (*recordingBackend) Delete(
	context.Context, []byte, shoal.ID, uint64, time.Time, time.Time,
) (fleetevents.Subscription, error) {
	return fleetevents.Subscription{}, errors.New("unexpected delete")
}
func (b *recordingBackend) Append(
	_ context.Context, request fleetevents.PublishRequest, _ time.Time,
) (fleetevents.PublishResult, error) {
	b.appends++
	b.request = request
	return fleetevents.PublishResult{}, nil
}
func (*recordingBackend) Scan(
	context.Context, uint64, uint64, int,
) ([]fleetevents.Event, uint64, error) {
	return nil, 0, errors.New("unexpected scan")
}

func (g *changingDispatchGeneration) CurrentPolicyGeneration(
	context.Context, []byte,
) (int64, error) {
	if g.calls.Add(1) == 1 {
		return 1, nil
	}
	return 2, nil
}
