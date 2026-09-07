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

package fleetevents

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestCursorTamperCrossSubscriberAndRevocation(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	backend := &memoryBackend{}
	generation := &generationReader{generation: 7}
	lease := &leaseValidator{}
	audit := &auditor{}
	service := testService(t, "alice", now, backend, generation, lease, audit)
	subscription, err := service.Create(context.Background(), CreateRequest{
		Token: []byte("create"), RetryUntil: now.Add(time.Hour), AgentID: shoal.ID("agent"), AgentGeneration: 1, TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	lease.mu.Lock()
	if lease.agentID != "agent" || lease.generation != 1 {
		t.Fatalf("lease binding = %q/%d", lease.agentID, lease.generation)
	}
	lease.mu.Unlock()
	backend.events = []Event{eventAt(1)}
	page, err := service.Pull(context.Background(), PullRequest{
		SubscriptionID: subscription.ID, Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	tamperedBytes, err := base64.RawURLEncoding.DecodeString(page.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	tamperedBytes[len(tamperedBytes)-1] ^= 0x01
	tampered := base64.RawURLEncoding.EncodeToString(tamperedBytes)
	if _, err := service.Pull(context.Background(), PullRequest{
		SubscriptionID: subscription.ID, Cursor: tampered, Limit: 1,
	}); !errors.Is(err, ErrCursorInvalid) ||
		!shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("tampered cursor error = %v", err)
	}
	bob := testService(t, "bob", now, backend, generation, lease, audit)
	if _, err := bob.Pull(context.Background(), PullRequest{
		SubscriptionID: subscription.ID, Cursor: page.NextCursor, Limit: 1,
	}); err == nil {
		t.Fatal("cross-subscriber cursor replay succeeded")
	}
	if err := bob.Delete(context.Background(), DeleteRequest{
		SubscriptionID: subscription.ID, ExpectedGeneration: 1,
		RetryUntil: now.Add(time.Hour),
	}); err == nil {
		t.Fatal("cross-subscriber deletion succeeded")
	}
	if backend.subscription.Generation != 1 ||
		!backend.subscription.RevokedAt.IsZero() {
		t.Fatalf("cross-subscriber deletion mutated subscription: %#v", backend.subscription)
	}
	backend.subscription.RevokedAt = now.Add(time.Second)
	if _, err := service.Pull(context.Background(), PullRequest{
		SubscriptionID: subscription.ID, Cursor: page.NextCursor, Limit: 1,
	}); err == nil {
		t.Fatal("revoked subscription delivered")
	}
}

func TestServiceRejectsTransportRacingLongPollBound(t *testing.T) {
	now := time.Date(2026, 9, 6, 4, 0, 0, 0, time.UTC)
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "alice", Actor: "alice", RequestID: "request",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations:   []auth.Operation{auth.OperationRead},
		PolicyGeneration:    7, AuthenticationExpires: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := auth.NewStaticResolverWithClock(
		decision, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(Config{
		Backend: &memoryBackend{}, Resolver: resolver,
		GenerationReader: &generationReader{generation: 7},
		LeaseValidator:   &leaseValidator{}, Auditor: &auditor{},
		CursorKey: make([]byte, 32), Clock: func() time.Time { return now },
		PollInterval: time.Millisecond, MaxWait: 30 * time.Second,
	})
	if err == nil {
		t.Fatal("accepted long poll equal to the production write timeout")
	}
}

func TestPullResyncAndMidPageGenerationChange(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	backend := &memoryBackend{floor: 4, events: []Event{eventAt(4), eventAt(5)}}
	generation := &generationReader{generation: 7}
	lease := &leaseValidator{}
	service := testService(t, "alice", now, backend, generation, lease, &auditor{})
	subscription, err := service.Create(context.Background(), CreateRequest{
		Token: []byte("create"), RetryUntil: now.Add(time.Hour), AgentID: shoal.ID("agent"), AgentGeneration: 1, TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Pull(context.Background(), PullRequest{
		SubscriptionID: subscription.ID, Limit: 2,
	}); !errors.Is(err, ErrResyncRequired) ||
		!shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("overrun error = %v", err)
	}
	backend.floor = 1
	lease.onCall = func(count int) {
		if count == 4 {
			generation.generation = 8
		}
	}
	if _, err := service.Pull(context.Background(), PullRequest{
		SubscriptionID: subscription.ID, Limit: 2,
	}); err == nil {
		t.Fatal("mid-page policy change delivered")
	}
}

func TestPullDropsEventDeniedOnlyByFinalAuthorizationCheck(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	allowed, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "alice", Actor: "alice", RequestID: "request",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations:   []auth.Operation{auth.OperationSubscriptionDeliver},
		PermittedSourceIDs:  [][]byte{[]byte("source")},
		PermittedPolicyIDs:  [][]byte{[]byte("policy")},
		PolicyGeneration:    7, AuthenticationExpires: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	denied, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "alice", Actor: "alice", RequestID: "request",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations:   []auth.Operation{auth.OperationRead},
		PolicyGeneration:    7, AuthenticationExpires: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := auth.AuthorizationFingerprint(allowed)
	if err != nil {
		t.Fatal(err)
	}
	backend := &memoryBackend{
		subscription: Subscription{
			ID: []byte("subscription"), SubscriberID: "alice",
			AgentID: "agent", AgentGeneration: 1,
			AuthorizationFingerprint: fingerprint, PolicyGeneration: 7,
			Generation: 1, CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		},
		events: []Event{eventAt(1)},
	}
	resolver := &sequenceResolver{allowed: allowed, denied: denied, denyAt: 4}
	service, err := New(Config{
		Backend: backend, Resolver: resolver,
		GenerationReader: &generationReader{generation: 7},
		LeaseValidator:   &leaseValidator{}, Auditor: &auditor{},
		CursorKey: make([]byte, 32), Clock: func() time.Time { return now },
		PollInterval: time.Millisecond, MaxWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := service.Pull(context.Background(), PullRequest{
		SubscriptionID: []byte("subscription"), Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 0 {
		t.Fatalf("final authorization denial leaked events: %#v", page.Events)
	}
	resolver.denyAt = 0
	resumed, err := service.Pull(context.Background(), PullRequest{
		SubscriptionID: []byte("subscription"), Cursor: page.NextCursor, Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resumed.Events) != 0 {
		t.Fatalf("filtered event was redelivered after cursor advance: %#v", resumed.Events)
	}
}

func TestRecorderFailureReportsAmbiguousCommittedAction(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	backend := &memoryBackend{}
	audit := &auditor{err: errors.New("sink unavailable")}
	service := testService(t, "alice", now, backend, &generationReader{generation: 7},
		&leaseValidator{}, audit)
	_, err := service.Publish(context.Background(), PublishRequest{
		Token: []byte("publish"), RetryUntil: now.Add(time.Hour), Event: eventAt(0),
	})
	if !errors.Is(err, ErrAuditOutcomeUnknown) || len(backend.events) != 1 {
		t.Fatalf("publish error/events = %v/%d", err, len(backend.events))
	}
	if len(audit.records) != 1 ||
		len(audit.records[0].Evidence) != len(backend.events[0].Evidence) ||
		audit.records[0].Evidence[0].ObjectID != backend.events[0].Evidence[0].ObjectID {
		t.Fatalf("audit evidence = %#v", audit.records)
	}
}

func TestRetryDeadlineBoundsFailBeforeMutation(t *testing.T) {
	now := time.Date(2026, 9, 6, 20, 0, 0, 0, time.UTC)
	backend := &memoryBackend{}
	service := testService(
		t, "alice", now, backend, &generationReader{generation: 7},
		&leaseValidator{}, &auditor{},
	)
	for name, deadline := range map[string]time.Time{
		"missing":  {},
		"expired":  now,
		"too-long": now.Add(MaxMutationRetryWindow + time.Nanosecond),
	} {
		t.Run(name, func(t *testing.T) {
			_, createErr := service.Create(context.Background(), CreateRequest{
				Token: []byte("create"), AgentID: "agent", AgentGeneration: 1,
				RetryUntil: deadline,
			})
			if createErr == nil {
				t.Fatal("create accepted invalid retry deadline")
			}
			_, publishErr := service.Publish(context.Background(), PublishRequest{
				Token: []byte("publish"), RetryUntil: deadline, Event: eventAt(0),
			})
			if publishErr == nil {
				t.Fatal("publish accepted invalid retry deadline")
			}
		})
	}
	if len(backend.events) != 0 || len(backend.subscription.ID) != 0 {
		t.Fatalf("invalid retry deadline mutated backend: %#v", backend)
	}
}

func TestPostCommitAuthorizationChangeIsRecordedAndReported(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	generation := &generationReader{generation: 7}
	audit := &auditor{}
	backend := &memoryBackend{onAppend: func() {
		generation.mu.Lock()
		generation.generation = 8
		generation.mu.Unlock()
	}}
	service := testService(t, "alice", now, backend, generation,
		&leaseValidator{}, audit)
	_, err := service.Publish(context.Background(), PublishRequest{
		Token: []byte("publish"), RetryUntil: now.Add(time.Hour), Event: eventAt(0),
	})
	if !errors.Is(err, ErrActionCommitted) || len(backend.events) != 1 ||
		audit.calls != 1 {
		t.Fatalf("publish error/events/audits = %v/%d/%d",
			err, len(backend.events), audit.calls)
	}
}

func TestPullRechecksLeaseWhileWaitingAndHonorsCancellation(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	backend := &memoryBackend{}
	generation := &generationReader{generation: 7}
	revoked := errors.New("lease revoked")
	lease := &leaseValidator{}
	lease.onCall = func(count int) {
		if count == 2 {
			lease.err = revoked
		}
	}
	service := testService(t, "alice", now, backend, generation, lease, &auditor{})
	subscription, err := service.Create(context.Background(), CreateRequest{
		Token: []byte("create"), RetryUntil: now.Add(time.Hour), AgentID: shoal.ID("agent"), AgentGeneration: 1, TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := service.Pull(ctx, PullRequest{
		SubscriptionID: subscription.ID, Limit: 1, Wait: time.Second,
	}); !errors.Is(err, revoked) {
		t.Fatalf("mid-wait lease error = %v", err)
	}

	cancelLease := &leaseValidator{}
	cancelService := testService(t, "alice", now, backend, generation, cancelLease, &auditor{})
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if _, err := cancelService.Pull(ctx, PullRequest{
		SubscriptionID: subscription.ID, Limit: 1, Wait: time.Second,
	}); err == nil {
		t.Fatal("cancelled pull succeeded")
	}
}

func TestPublishScopesIdempotencyTokenToCaller(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	backend := &memoryBackend{}
	generation := &generationReader{generation: 7}
	lease := &leaseValidator{}
	audit := &auditor{}
	alice := testService(t, "alice", now, backend, generation, lease, audit)
	bob := testService(t, "bob", now, backend, generation, lease, audit)
	request := PublishRequest{Token: []byte("same-client-token"), RetryUntil: now.Add(time.Hour), Event: eventAt(0)}
	aliceResult, err := alice.Publish(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	bobResult, err := bob.Publish(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if aliceResult.Sequence != 0 || bobResult.Sequence != 0 {
		t.Fatalf("publication exposed stream positions: %d, %d",
			aliceResult.Sequence, bobResult.Sequence)
	}
	if len(backend.tokens) != 2 || bytes.Equal(backend.tokens[0], backend.tokens[1]) {
		t.Fatalf("scoped publication tokens = %x, %x", backend.tokens[0], backend.tokens[1])
	}
}

func TestEventRequiresProducerGenerationAndTransitionIdentity(t *testing.T) {
	event := eventAt(1)
	event.ProducerGeneration = 0
	if _, err := normalizeEvent(event, true); err == nil {
		t.Fatal("zero producer generation succeeded")
	}
	event = eventAt(1)
	event.TransitionID = nil
	if _, err := normalizeEvent(event, true); err == nil {
		t.Fatal("empty transition identity succeeded")
	}
}

func TestPublishLifecycleKeepsStableTokenAndRejectsBroadOperation(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "dispatcher", Actor: "dispatcher", RequestID: "request",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations:   []auth.Operation{auth.OperationDispatch},
		PermittedSourceIDs:  [][]byte{[]byte("source")},
		PermittedPolicyIDs:  [][]byte{[]byte("policy")},
		PolicyGeneration:    7, AuthenticationExpires: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := auth.NewStaticResolverWithClock(
		decision, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	backend := &memoryBackend{}
	service, err := New(Config{
		Backend: backend, Resolver: resolver,
		GenerationReader: &generationReader{generation: 7},
		LeaseValidator:   &leaseValidator{}, Auditor: &auditor{},
		CursorKey: make([]byte, 32), Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	request := PublishRequest{
		Token: []byte("stable-transition-token"), RetryUntil: now.Add(time.Hour), Event: eventAt(0),
	}
	request.Event.Kind = "action.enqueued"
	request.Event.CorrelationID = []byte("correlation")
	receipt := LifecycleReceipt{
		RequestID: "request", CorrelationID: []byte("correlation"),
		AuthorizationFingerprint: func() auth.Fingerprint {
			value, fingerprintErr := auth.AuthorizationFingerprint(decision)
			if fingerprintErr != nil {
				t.Fatal(fingerprintErr)
			}
			return value
		}(),
		AuthorizationExpiresAt: decision.AuthenticationExpires(),
	}
	if _, err := service.PublishLifecycle(
		context.Background(), auth.OperationDispatch, request, receipt,
	); err != nil {
		t.Fatal(err)
	}
	if len(backend.tokens) != 1 ||
		!bytes.Equal(backend.tokens[0], request.Token) {
		t.Fatalf("lifecycle token = %x", backend.tokens)
	}
	if _, err := service.PublishLifecycle(
		context.Background(), auth.OperationEventPublish, request, receipt,
	); err == nil {
		t.Fatal("event_publish was accepted by the trusted lifecycle path")
	}
	if _, err := service.PublishLifecycle(
		context.Background(), auth.OperationInvoke, request, receipt,
	); err == nil {
		t.Fatal("invoke was accepted for a dispatch lifecycle event")
	}
	request.Event.Kind = "agent.completed"
	if _, err := service.PublishLifecycle(
		context.Background(), auth.OperationDispatch, request, receipt,
	); err == nil {
		t.Fatal("non-lifecycle event was accepted by the trusted lifecycle path")
	}
}

func TestPublicPublishRejectsReservedLifecycleKinds(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	backend := &memoryBackend{}
	service := testService(
		t, "publisher", now, backend, &generationReader{generation: 7},
		&leaseValidator{}, &auditor{},
	)
	for _, kind := range []string{
		"action.enqueued", "action.claimed", "action.completed",
		"action.canceled", "action.failed",
	} {
		request := PublishRequest{
			Token: []byte("public-token"), RetryUntil: now.Add(time.Hour), Event: eventAt(0),
		}
		request.Event.Kind = kind
		if _, err := service.Publish(context.Background(), request); err == nil {
			t.Fatalf("public publish accepted reserved lifecycle kind %q", kind)
		}
	}
	if len(backend.events) != 0 {
		t.Fatalf("reserved public lifecycle events appended: %d", len(backend.events))
	}
}

func TestCreateScopesIdempotencyTokenToCaller(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	aliceBackend, bobBackend := &memoryBackend{}, &memoryBackend{}
	generation := &generationReader{generation: 7}
	request := CreateRequest{Token: []byte("same-client-token"), RetryUntil: now.Add(time.Hour), AgentID: shoal.ID("agent"), AgentGeneration: 1}
	if _, err := testService(t, "alice", now, aliceBackend, generation,
		&leaseValidator{}, &auditor{}).Create(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := testService(t, "bob", now, bobBackend, generation,
		&leaseValidator{}, &auditor{}).Create(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(aliceBackend.createToken, bobBackend.createToken) {
		t.Fatalf("create tokens were not scoped: %x", aliceBackend.createToken)
	}
}

func TestPullCursorAdvancesFromPinnedFrontier(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	backend := &memoryBackend{events: []Event{eventAt(1), eventAt(2)}}
	service := testService(t, "alice", now, backend, &generationReader{generation: 7},
		&leaseValidator{}, &auditor{})
	subscription, err := service.Create(context.Background(), CreateRequest{
		Token: []byte("create"), RetryUntil: now.Add(time.Hour), AgentID: shoal.ID("agent"), AgentGeneration: 1, TTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := service.Pull(context.Background(), PullRequest{
		SubscriptionID: subscription.ID, Limit: 1,
	})
	if err != nil || len(first.Events) != 1 {
		t.Fatalf("first page = %#v, %v", first, err)
	}
	backend.mu.Lock()
	backend.events = append(backend.events, eventAt(3))
	backend.mu.Unlock()
	second, err := service.Pull(context.Background(), PullRequest{
		SubscriptionID: subscription.ID, Cursor: first.NextCursor, Limit: 2,
	})
	if err != nil || len(second.Events) != 1 {
		t.Fatalf("pinned page = %#v, %v", second, err)
	}
	third, err := service.Pull(context.Background(), PullRequest{
		SubscriptionID: subscription.ID, Cursor: second.NextCursor, Limit: 2,
	})
	if err != nil || len(third.Events) != 1 {
		t.Fatalf("advanced page = %#v, %v", third, err)
	}
}

func TestSubscriptionRoleOnlyCanDeliverWithoutGenericRead(t *testing.T) {
	now := time.Date(2026, 9, 6, 5, 0, 0, 0, time.UTC)
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "subscriber", Actor: "subscriber", RequestID: "request",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations: []auth.Operation{
			auth.OperationSubscriptionCreate,
			auth.OperationSubscriptionDeliver,
		},
		PermittedSourceIDs: [][]byte{[]byte("source")},
		PermittedPolicyIDs: [][]byte{[]byte("policy")},
		PolicyGeneration:   7, AuthenticationExpires: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := decision.Authorize(auth.OperationRead, auth.ResourceRequest{
		AuthorizationDomain: decision.AuthorizationDomain(),
	}, now); err == nil {
		t.Fatal("subscription-only decision unexpectedly allows generic read")
	}
	resolver, err := auth.NewStaticResolverWithClock(
		decision, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	backend := &memoryBackend{}
	service, err := New(Config{
		Backend: backend, Resolver: resolver,
		GenerationReader: &generationReader{generation: 7},
		LeaseValidator:   &leaseValidator{}, Auditor: &auditor{},
		CursorKey:    bytes.Repeat([]byte{0x44}, 32),
		Clock:        func() time.Time { return now },
		PollInterval: time.Millisecond, MaxWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := service.Create(context.Background(), CreateRequest{
		Token: []byte("create"), RetryUntil: now.Add(time.Hour), AgentID: "agent", AgentGeneration: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	backend.events = []Event{eventAt(1)}
	page, err := service.Pull(context.Background(), PullRequest{
		SubscriptionID: subscription.ID, Limit: 1,
	})
	if err != nil || len(page.Events) != 1 {
		t.Fatalf("subscription-only delivery = %#v, %v", page, err)
	}
}

func TestCursorExpiresAndAEADIsRequired(t *testing.T) {
	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	codec, err := newCursorCodec(bytesOf(7, 32), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	value, err := codec.seal(cursorState{
		SubscriptionID: []byte("subscription"), SubscriberID: "alice",
		Fingerprint: auth.Fingerprint{1}, Generation: 1, NextSequence: 2,
		ExpiresAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.open(value, now.Add(time.Minute)); !errors.Is(err, ErrCursorInvalid) {
		t.Fatalf("expired cursor error = %v", err)
	}
	mutated := value[:len(value)-1] + "A"
	if _, err := codec.open(mutated, now); !errors.Is(err, ErrCursorInvalid) {
		t.Fatalf("unauthenticated mutation error = %v", err)
	}
	legacy := base64.RawURLEncoding.EncodeToString(
		append([]byte{1}, bytes.Repeat([]byte{0x41}, 96)...))
	if _, err := codec.open(legacy, now); !errors.Is(err, ErrCursorInvalid) {
		t.Fatalf("legacy cursor error = %v", err)
	}
}

func testService(
	t *testing.T, subject string, now time.Time, backend Backend,
	generation auth.GenerationReader, lease LeaseValidator, audit Auditor,
) *Service {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: shoal.ID(subject), Actor: shoal.ID(subject), RequestID: "request",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations: []auth.Operation{
			auth.OperationSubscriptionCreate, auth.OperationSubscriptionDelete,
			auth.OperationEventPublish, auth.OperationSubscriptionDeliver,
		},
		PermittedSourceIDs: [][]byte{[]byte("source")},
		PermittedPolicyIDs: [][]byte{[]byte("policy")},
		PolicyGeneration:   7, AuthenticationExpires: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := auth.NewStaticResolverWithClock(decision, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{
		Backend: backend, Resolver: resolver, GenerationReader: generation,
		LeaseValidator: lease, Auditor: audit, CursorKey: make([]byte, 32),
		Clock: func() time.Time { return now }, PollInterval: time.Millisecond, MaxWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type generationReader struct {
	mu         sync.Mutex
	generation int64
}

type sequenceResolver struct {
	mu              sync.Mutex
	allowed, denied auth.Decision
	calls, denyAt   int
}

func (r *sequenceResolver) Resolve(context.Context) (auth.Decision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.denyAt > 0 && r.calls == r.denyAt {
		return r.denied, nil
	}
	return r.allowed, nil
}

func (r *generationReader) CurrentPolicyGeneration(context.Context, []byte) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.generation, nil
}

type leaseValidator struct {
	mu         sync.Mutex
	calls      int
	agentID    shoal.ID
	generation int64
	err        error
	onCall     func(int)
}

func (v *leaseValidator) ValidateDelivery(
	_ context.Context, agentID shoal.ID, generation int64,
) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls++
	v.agentID = agentID
	v.generation = generation
	if v.onCall != nil {
		v.onCall(v.calls)
	}
	return v.err
}

type auditor struct {
	err     error
	calls   int
	records []AuditRecord
}

func (a *auditor) RecordFleetAction(_ context.Context, record AuditRecord) error {
	a.calls++
	record.ActionID = cloneBytes(record.ActionID)
	record.ObjectID = cloneBytes(record.ObjectID)
	record.CorrelationID = cloneBytes(record.CorrelationID)
	record.Evidence = cloneEvidence(record.Evidence)
	a.records = append(a.records, record)
	return a.err
}

type memoryBackend struct {
	mu           sync.Mutex
	subscription Subscription
	events       []Event
	tokens       [][]byte
	createToken  []byte
	floor        uint64
	onAppend     func()
}

func (b *memoryBackend) Create(
	_ context.Context, request CreateRequest, fingerprint auth.Fingerprint,
	generation int64, now time.Time,
) (Subscription, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.subscription.ID) != 0 {
		return b.subscription, true, nil
	}
	b.createToken = cloneBytes(request.Token)
	b.subscription = Subscription{
		ID: deriveID("test-sub", request.Token), SubscriberID: request.SubscriberID,
		AgentID: request.AgentID, AgentGeneration: request.AgentGeneration,
		AuthorizationFingerprint: fingerprint,
		PolicyGeneration:         generation, Filter: request.Filter, Generation: 1,
		CreatedAt: now, ExpiresAt: now.Add(request.TTL),
	}
	return b.subscription, false, nil
}

func (b *memoryBackend) Subscription(context.Context, []byte) (Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.subscription.ID) == 0 {
		return Subscription{}, ErrSubscriptionNotFound
	}
	return cloneSubscription(b.subscription), nil
}

func (b *memoryBackend) Delete(
	_ context.Context, _ []byte, subscriberID shoal.ID, expected uint64,
	_ time.Time, now time.Time,
) (Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subscription.SubscriberID != subscriberID {
		return Subscription{}, ErrSubscriptionNotFound
	}
	if b.subscription.Generation != expected {
		return Subscription{}, ErrGenerationConflict
	}
	b.subscription.Generation++
	b.subscription.RevokedAt = now
	return cloneSubscription(b.subscription), nil
}

func (b *memoryBackend) Append(_ context.Context, request PublishRequest, _ time.Time) (PublishResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	event := request.Event
	event.Sequence = uint64(len(b.events) + 1)
	event.EventID = deriveID("test-event", request.Token)
	b.events = append(b.events, event)
	b.tokens = append(b.tokens, cloneBytes(request.Token))
	if b.onAppend != nil {
		b.onAppend()
	}
	return PublishResult{EventID: event.EventID, Sequence: event.Sequence}, nil
}

func (b *memoryBackend) Scan(
	_ context.Context, next, frontier uint64, limit int,
) ([]Event, uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if next < b.floor {
		return nil, 0, ErrResyncRequired
	}
	if frontier == 0 {
		frontier = uint64(len(b.events))
	}
	result := make([]Event, 0, limit)
	for _, event := range b.events {
		if event.Sequence >= next && event.Sequence <= frontier && len(result) < limit {
			result = append(result, cloneEvent(event))
		}
	}
	if len(result) == 0 && frontier < uint64(len(b.events)) {
		frontier = uint64(len(b.events))
		for _, event := range b.events {
			if event.Sequence >= next && event.Sequence <= frontier && len(result) < limit {
				result = append(result, cloneEvent(event))
			}
		}
	}
	return result, frontier, nil
}

func eventAt(sequence uint64) Event {
	return Event{
		Sequence: sequence, EventID: []byte("event"), Kind: "agent.completed",
		ProducerID: []byte("producer"), ProducerGeneration: 1,
		ActionID: []byte("action"), TransitionID: []byte("transition"),
		Reason:     interaction.Reason{Code: "completed"},
		Evidence:   []Evidence{{SourceID: []byte("source"), PolicyID: []byte("policy"), ObjectID: "object"}},
		OccurredAt: time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC),
	}
}

func bytesOf(value byte, count int) []byte {
	result := make([]byte, count)
	for i := range result {
		result[i] = value
	}
	return result
}
