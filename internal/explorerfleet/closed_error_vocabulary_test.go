// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// TestWithClosedErrorVocabularyReachesTheDispatchService is the join between
// the operator's flag and the rule it is meant to turn on.
//
// WithClosedErrorVocabulary sets one field on one config struct, and every
// test of the rule itself lives in pkg/explorer/fleet where that field is
// set directly. So a composer that accepted the option and dropped it would
// leave all of them green, and the deployed service would stay in phase 1
// with an operator believing they had closed the channel. This goes through
// ComposeDispatchWithAttestations and asserts on a recorded outcome.
func TestWithClosedErrorVocabularyReachesTheDispatchService(t *testing.T) {
	for _, probe := range []struct {
		name    string
		closed  bool
		want    string
		wantOwn fleet.ErrorCodeOrigin
	}{
		{
			// Without the option the code is a well-formed token and
			// survives, which is phase 1: narrowed, not closed.
			name:    "without the option an undeclared token is recorded",
			want:    "patient_4471_hiv_positive",
			wantOwn: fleet.ErrorCodeOriginExecutor,
		},
		{
			name: "with the option it is adjudicated", closed: true,
			want:    "invalid_executor_error",
			wantOwn: fleet.ErrorCodeOriginService,
		},
	} {
		t.Run(probe.name, func(t *testing.T) {
			now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
			runtime := openFleetDispatchRuntime(t, t.TempDir())
			defer runtime.Close()
			authority, err := auth.NewAuthorityWithClock(
				func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			executor := &integratedExecutor{}
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
			var options []DispatchOption
			if probe.closed {
				options = append(options, WithClosedErrorVocabulary())
			}
			dispatch, err := ComposeDispatchWithAttestations(
				runtime, registry, authority.Resolver(),
				integratedActionRecorder{}, integratedEvents{}, nil,
				func() time.Time { return now }, nil, DispatchLabels{},
				options...,
			)
			if err != nil {
				t.Fatal(err)
			}

			ctx, err := authority.Binder().Bind(
				context.Background(), integratedDecision(t, now))
			if err != nil {
				t.Fatal(err)
			}
			descriptor, err := registry.Register(ctx, fleet.RegisterRequest{
				Context:         integratedContext(now),
				RegistrationKey: "register-agent",
				Spec: fleet.Spec{
					ID: "agent", AuthorizationDomain: []byte("domain"),
					Scopes: []fleet.Scope{{
						SourceID: []byte("source"),
						PolicyID: []byte("policy"),
					}},
					ExecutorRef:    "exec",
					LeaseExpiresAt: now.Add(time.Hour),
					Capabilities: []fleet.Capability{{
						Name: "search", Actions: []fleet.Action{{
							Name:         "query",
							InputSchema:  json.RawMessage(`{"type":"object"}`),
							OutputSchema: json.RawMessage(`{"type":"object"}`),
							// Declares one code and not the reported one,
							// so the descriptor is what decides.
							ErrorCodes: []string{"upstream_refused"},
						}},
					}},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			queued, err := dispatch.Enqueue(ctx, fleet.EnqueueRequest{
				ID: []byte("action"), IdempotencyKey: []byte("key"),
				AgentID:         descriptor.ID,
				AgentGeneration: descriptor.Generation,
				Capability:      "search", Action: "query",
				SourceID: []byte("source"), PolicyID: []byte("policy"),
				ObjectID: "object", Input: json.RawMessage(`{}`),
				Context: integratedContext(now),
			})
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := dispatch.Claim(ctx, fleet.ClaimRequest{
				ID: queued.ID, ExpectedVersion: queued.Version,
				ClaimID: []byte("claim"), Lease: time.Minute,
				Context: integratedContext(now),
			})
			if err != nil {
				t.Fatal(err)
			}
			completed, err := dispatch.CompleteClaim(
				ctx, fleet.CompletionRequest{
					ID: queued.ID, ExpectedVersion: claimed.Version,
					ClaimFence: claimed.ClaimFence,
					ClaimID:    []byte("claim"), Failed: true,
					Result: fleet.ExecutionResult{
						ErrorCode: "patient_4471_hiv_positive",
					},
					Context: integratedContext(now),
				})
			if err != nil {
				t.Fatalf("the completion was refused rather than "+
					"recorded: %v", err)
			}
			if completed.ErrorCode != probe.want {
				t.Fatalf("code = %q, want %q", completed.ErrorCode,
					probe.want)
			}
			if completed.ErrorCodeOrigin != probe.wantOwn {
				t.Fatalf("origin = %q, want %q", completed.ErrorCodeOrigin,
					probe.wantOwn)
			}
			// The probe above cannot tell "the descriptor declares
			// something else" from "the descriptor declares nothing", and
			// an action declaring nothing adjudicates everything — so on
			// its own it would pass against a build that drops the
			// vocabulary on the way to storage. It did, twice: Action has
			// a hand-written MarshalJSON and the registry has a versioned
			// binary codec, and the field had to be added to both.
			//
			// A second completion reporting a *declared* code is what
			// distinguishes them. Under the closed rule it must be
			// recorded as the executor's own word, which is only possible
			// if the stored descriptor still carries the declaration.
			second, err := dispatch.Enqueue(ctx, fleet.EnqueueRequest{
				ID: []byte("declared"), IdempotencyKey: []byte("key-2"),
				AgentID:         descriptor.ID,
				AgentGeneration: descriptor.Generation,
				Capability:      "search", Action: "query",
				SourceID: []byte("source"), PolicyID: []byte("policy"),
				ObjectID: "object", Input: json.RawMessage(`{}`),
				Context: integratedContext(now),
			})
			if err != nil {
				t.Fatal(err)
			}
			secondClaim, err := dispatch.Claim(ctx, fleet.ClaimRequest{
				ID: second.ID, ExpectedVersion: second.Version,
				ClaimID: []byte("claim-2"), Lease: time.Minute,
				Context: integratedContext(now),
			})
			if err != nil {
				t.Fatal(err)
			}
			declared, err := dispatch.CompleteClaim(
				ctx, fleet.CompletionRequest{
					ID: second.ID, ExpectedVersion: secondClaim.Version,
					ClaimFence: secondClaim.ClaimFence,
					ClaimID:    []byte("claim-2"), Failed: true,
					Result: fleet.ExecutionResult{
						ErrorCode: "upstream_refused",
					},
					Context: integratedContext(now),
				})
			if err != nil {
				t.Fatal(err)
			}
			if declared.ErrorCode != "upstream_refused" ||
				declared.ErrorCodeOrigin != fleet.ErrorCodeOriginExecutor {
				t.Fatalf("a declared code was recorded as %q/%q, want it "+
					"kept as the executor's own; the stored descriptor has "+
					"lost its vocabulary, so the probe above adjudicated "+
					"because the action declares nothing rather than "+
					"because the rule works", declared.ErrorCode,
					declared.ErrorCodeOrigin)
			}
		})
	}
}
