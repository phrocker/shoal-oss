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

// Package coordtest provides the coordinator contract shared by backend
// implementations.
package coordtest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/coordination"
)

// Factory must return a fresh coordinator connected to the same durable state
// on every call.
type Factory func(owner string) (coordination.Coordinator, error)

func Run(t *testing.T, factory Factory) {
	t.Helper()
	const resource = "table/graph"

	firstCoordinator, err := factory("owner-a")
	if err != nil {
		t.Fatal(err)
	}
	watchContext, cancelWatch := context.WithCancel(context.Background())
	watch, err := firstCoordinator.Watch(watchContext, resource)
	if err != nil {
		t.Fatal(err)
	}
	first, err := firstCoordinator.Acquire(context.Background(), resource)
	if err != nil {
		t.Fatal(err)
	}
	firstToken := first.Token()
	if err := firstToken.Validate(); err != nil {
		t.Fatalf("first token: %v", err)
	}
	assertEvent(t, watch, coordination.EventAcquired, firstToken)
	if err := first.Renew(context.Background()); err != nil {
		t.Fatalf("renew: %v", err)
	}
	members, err := firstCoordinator.Members(context.Background(), resource)
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 1 || members[0].Token != firstToken {
		t.Fatalf("members = %+v, want first tenure", members)
	}
	if _, err := firstCoordinator.Acquire(context.Background(), resource); !errors.Is(err, coordination.ErrLeaseHeld) {
		t.Fatalf("duplicate acquire = %v, want ErrLeaseHeld", err)
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatalf("duplicate release: %v", err)
	}
	select {
	case <-first.Lost():
	default:
		t.Fatal("loss signal remained open after release")
	}
	assertEvent(t, watch, coordination.EventReleased, firstToken)
	cancelWatch()

	secondCoordinator, err := factory("owner-b")
	if err != nil {
		t.Fatal(err)
	}
	second, err := secondCoordinator.Acquire(context.Background(), resource)
	if err != nil {
		t.Fatalf("restart acquire: %v", err)
	}
	secondToken := second.Token()
	if secondToken.Epoch != firstToken.Epoch {
		t.Fatalf("restart epoch = %d, want same-domain epoch %d", secondToken.Epoch, firstToken.Epoch)
	}
	if secondToken.Generation <= firstToken.Generation {
		t.Fatalf("restart generation = %d, want > %d", secondToken.Generation, firstToken.Generation)
	}
	validator, ok := secondCoordinator.(coordination.Validator)
	if !ok {
		t.Fatal("coordinator does not implement token validation")
	}
	if err := validator.Validate(context.Background(), firstToken); !errors.Is(err, coordination.ErrStaleToken) {
		t.Fatalf("stale token validation = %v, want ErrStaleToken", err)
	}
	if err := validator.Validate(context.Background(), secondToken); err != nil {
		t.Fatalf("current token validation: %v", err)
	}
	epochLease, ok := second.(coordination.EpochLease)
	if !ok {
		t.Fatal("lease does not implement durable epoch advancement")
	}
	advanced, err := epochLease.AdvanceEpoch(context.Background(), secondToken.Epoch+1)
	if err != nil {
		t.Fatalf("advance epoch: %v", err)
	}
	if advanced.Epoch <= secondToken.Epoch || advanced.Generation <= secondToken.Generation {
		t.Fatalf("advanced token = %+v, previous %+v", advanced, secondToken)
	}
	if _, err := epochLease.AdvanceEpoch(context.Background(), advanced.Epoch); err == nil {
		t.Fatal("duplicate epoch advancement succeeded")
	}
	if err := validator.Validate(context.Background(), secondToken); !errors.Is(err, coordination.ErrStaleToken) {
		t.Fatalf("pre-advance token validation = %v, want ErrStaleToken", err)
	}
	if err := validator.Validate(context.Background(), advanced); err != nil {
		t.Fatalf("advanced token validation: %v", err)
	}
	if err := second.Release(context.Background()); err != nil {
		t.Fatalf("release restarted lease: %v", err)
	}
}

func assertEvent(
	t *testing.T,
	events <-chan coordination.Event,
	kind coordination.EventKind,
	token coordination.AuthorityToken,
) {
	t.Helper()
	select {
	case event := <-events:
		if event.Kind != kind || event.Member.Token != token {
			t.Fatalf("event = %+v, want %s for %+v", event, kind, token)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s event", kind)
	}
}
