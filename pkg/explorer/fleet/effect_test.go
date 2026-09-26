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

package fleet

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

var anyObject = json.RawMessage(`{"type":"object"}`)

type ceilingExecutor struct{ ceiling Effect }

func (e ceilingExecutor) MaxEffect() Effect { return e.ceiling }

type unboundedExecutor struct{}

func externalAction() []Capability {
	return []Capability{{Name: "deploy", Actions: []Action{{
		Name: "ship", Effect: EffectExternal,
		InputSchema: anyObject, OutputSchema: anyObject,
	}}}}
}

func evidenceAction() []Capability {
	return []Capability{{Name: "explorer.reason", Actions: []Action{{
		Name: "ask", InputSchema: anyObject, OutputSchema: anyObject,
	}}}}
}

// TestExternalEffectNeedsAnExternalCeiling is the boundary the README states:
// Shoal runs evidence-only work and dispatches the rest. An action declaring an
// external effect cannot resolve to an executor the host bound for evidence.
func TestExternalEffectNeedsAnExternalCeiling(t *testing.T) {
	if err := validateDeclaredEffects(
		externalAction(), EffectEvidence); err == nil {
		t.Fatal("an external action must not register against an evidence executor")
	}
	if err := validateDeclaredEffects(
		externalAction(), EffectExternal); err != nil {
		t.Fatalf("an external ceiling must admit an external action: %v", err)
	}
	if err := validateDeclaredEffects(
		evidenceAction(), EffectEvidence); err != nil {
		t.Fatalf("evidence work must run under an evidence ceiling: %v", err)
	}
}

// TestUndeclaredExecutorIsEvidenceOnly pins the conservative default. A host
// that wants an executor to perform external work has to say so; forgetting to
// declare must not widen what an executor may run.
func TestUndeclaredExecutorIsEvidenceOnly(t *testing.T) {
	if got := executorCeiling(unboundedExecutor{}); got != EffectEvidence {
		t.Fatalf("undeclared executor ceiling = %q", got)
	}
	if got := executorCeiling(ceilingExecutor{ceiling: EffectExternal}); got != EffectExternal {
		t.Fatalf("declared ceiling = %q", got)
	}
	if err := validateDeclaredEffects(
		externalAction(), executorCeiling(unboundedExecutor{})); err == nil {
		t.Fatal("an undeclared executor must not admit external work")
	}
}

// TestAbsentEffectKeepsItsPreviousMeaning proves the zero value is
// evidence-only, so descriptors registered before this field existed continue
// to resolve exactly as they did.
func TestAbsentEffectKeepsItsPreviousMeaning(t *testing.T) {
	if EffectEvidence != "" {
		t.Fatal("evidence must be the zero value or existing descriptors change meaning")
	}
	canonical, err := canonicalCapabilities(evidenceAction())
	if err != nil {
		t.Fatal(err)
	}
	if canonical[0].Actions[0].Effect != EffectEvidence {
		t.Fatalf("absent effect canonicalized to %q", canonical[0].Actions[0].Effect)
	}
}

// TestUnknownEffectFailsClosed proves a class nobody recognizes is refused
// rather than silently treated as the permissive or the restrictive one.
func TestUnknownEffectFailsClosed(t *testing.T) {
	_, err := canonicalCapabilities([]Capability{{
		Name: "c", Actions: []Action{{
			Name: "a", Effect: Effect("whatever"),
			InputSchema: anyObject, OutputSchema: anyObject,
		}},
	}})
	if err == nil {
		t.Fatal("an unknown effect class must be refused")
	}
}

// TestDelegationCannotWidenEffect proves effect is part of what delegation may
// narrow. Without it a child, or a later generation, could turn an
// evidence-only action into an external one and still pass the subset check
// that exists to stop widening, whenever its executor had an external ceiling.
func TestDelegationCannotWidenEffect(t *testing.T) {
	evidence := evidenceAction()
	external := []Capability{{Name: "explorer.reason", Actions: []Action{{
		Name: "ask", Effect: EffectExternal,
		InputSchema: anyObject, OutputSchema: anyObject,
	}}}}

	if capabilitiesSubset(external, evidence) {
		t.Fatal("a child must not widen an evidence-only action to external")
	}
	if !capabilitiesSubset(evidence, external) {
		t.Fatal("a child may narrow an external action to evidence-only")
	}
	if !capabilitiesSubset(evidence, evidence) ||
		!capabilitiesSubset(external, external) {
		t.Fatal("an unchanged effect must remain a subset of itself")
	}
}

// TestEffectSurvivesCloning proves the field is carried by cloneDescriptor.
// It was not, so a registered external action was returned to callers, and
// re-resolved, as evidence-only: the boundary was enforced once at
// registration and then silently dropped.
func TestEffectSurvivesCloning(t *testing.T) {
	descriptor := Descriptor{Capabilities: []Capability{{
		Name: "deploy", Actions: []Action{{
			Name: "ship", Effect: EffectExternal,
			InputSchema: anyObject, OutputSchema: anyObject,
		}},
	}}}
	clone := cloneDescriptor(descriptor)
	if got := clone.Capabilities[0].Actions[0].Effect; got != EffectExternal {
		t.Fatalf("cloned effect = %q, want %q", got, EffectExternal)
	}
}

// TestMutationDigestSeparatesEffects proves an otherwise identical
// registration with a different effect is a different mutation. The digest
// omitted the field, so an exact replay could swap an evidence-only action for
// an external one under the same mutation identity and be treated as the same
// request.
func TestMutationDigestSeparatesEffects(t *testing.T) {
	mutation := func(effect Effect) Mutation {
		return Mutation{Descriptor: Descriptor{
			ID: "agent", Generation: 1,
			Capabilities: []Capability{{Name: "deploy", Actions: []Action{{
				Name: "ship", Effect: effect,
				InputSchema: anyObject, OutputSchema: anyObject,
			}}}},
		}}
	}
	evidence := registryMutationDigest(mutation(EffectEvidence))
	external := registryMutationDigest(mutation(EffectExternal))
	if evidence == external {
		t.Fatal("effect must change the mutation digest")
	}
	if registryMutationDigest(mutation(EffectExternal)) != external {
		t.Fatal("the digest must stay stable for an unchanged effect")
	}
}

// TestUnknownEffectsFailClosedAtResolution is the regression test for a
// high-severity hole. Registration validates a declared effect, but the durable
// decoder reads whatever string is stored, so a malformed or tampered
// descriptor can reach resolution having never passed validation. Comparing
// only against the exact external string made every unrecognized value
// permitted under an evidence-only ceiling, which inverts the purpose of the
// class.
func TestUnknownEffectsFailClosedAtResolution(t *testing.T) {
	unknown := Effect("something-nobody-defined")

	// An unrecognized declaration is an unproven claim: beyond every ceiling.
	for _, ceiling := range []Effect{EffectEvidence, EffectExternal, unknown} {
		if !unknown.exceeds(ceiling) {
			t.Fatalf("unknown declaration was permitted under ceiling %q", ceiling)
		}
	}

	// An unrecognized ceiling is a host that failed to declare itself: it
	// permits evidence and nothing more.
	if EffectEvidence.exceeds(unknown) {
		t.Fatal("evidence-only work must remain permitted under an unknown ceiling")
	}
	if !EffectExternal.exceeds(unknown) {
		t.Fatal("an unknown ceiling must not authorize external work")
	}

	// The known cases are unchanged.
	if EffectExternal.exceeds(EffectEvidence) != true ||
		EffectExternal.exceeds(EffectExternal) != false ||
		EffectEvidence.exceeds(EffectEvidence) != false ||
		EffectEvidence.exceeds(EffectExternal) != false {
		t.Fatal("the recognized comparisons changed")
	}
}

// TestUnknownEffectFromStorageIsRefusedAtRegistration pins the same rule
// through the narrowing comparison, which is the other place a stored value is
// compared rather than validated.
func TestUnknownEffectFromStorageIsRefusedAtRegistration(t *testing.T) {
	unknown := []Capability{{Name: "explorer.reason", Actions: []Action{{
		Name: "ask", Effect: Effect("decoded-from-a-tampered-record"),
		InputSchema: anyObject, OutputSchema: anyObject,
	}}}}
	if err := validateDeclaredEffects(unknown, EffectEvidence); err == nil {
		t.Fatal("an unknown declared effect must not register")
	}
	if err := validateDeclaredEffects(unknown, EffectExternal); err == nil {
		t.Fatal("an unknown declared effect must not register even at the widest ceiling")
	}
	if capabilitiesSubset(unknown, evidenceAction()) {
		t.Fatal("an unknown effect must not pass the narrowing check")
	}
}

// externalRegisterRequest is registerRequest with the action declaring an
// external effect, so the request reaches Service.Register and exercises the
// real enforcement call rather than the validator underneath it.
func externalRegisterRequest(now time.Time, requestID, id, source string) RegisterRequest {
	request := registerRequest(now, requestID, id, "", source)
	request.Spec.Capabilities[0].Actions[0].Effect = EffectExternal
	return request
}

// TestServiceRegisterEnforcesTheEffectCeiling reaches the enforcement through
// Service.Register. The other effect tests call validateDeclaredEffects and
// exceeds directly, so removing the call from Register would leave them all
// green while the boundary stopped existing.
func TestServiceRegisterEnforcesTheEffectCeiling(t *testing.T) {
	now := time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	newService := func(executor Executor) *Service {
		service, err := NewService(Config{
			Store: newMemoryStore(), Resolver: authority.Resolver(),
			Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
			Executors: executorMap{"exec": executor},
			Clock:     func() time.Time { return now },
		})
		if err != nil {
			t.Fatal(err)
		}
		return service
	}
	decision := testDecision(
		t, "owner", "owner-actor", "effect-request", [][]byte{[]byte("source-a")})
	ctx := bindDecision(t, authority, decision)

	// An executor the host bound for evidence-only work must refuse an
	// external declaration at registration.
	evidenceOnly := newService(ceilingExecutor{ceiling: EffectEvidence})
	if _, err := evidenceOnly.Register(ctx,
		externalRegisterRequest(now, "effect-request", "agent", "source-a"),
	); err == nil {
		t.Fatal("Register admitted an external action under an evidence ceiling")
	}

	// An undeclared executor is read as evidence-only, so it must refuse too.
	undeclared := newService(struct{}{})
	if _, err := undeclared.Register(ctx,
		externalRegisterRequest(now, "effect-request", "agent", "source-a"),
	); err == nil {
		t.Fatal("Register admitted an external action under an undeclared executor")
	}

	// A host that declared the wider ceiling admits it.
	external := newService(ceilingExecutor{ceiling: EffectExternal})
	descriptor, err := external.Register(ctx,
		externalRegisterRequest(now, "effect-request", "agent", "source-a"))
	if err != nil {
		t.Fatalf("Register refused an external action under an external ceiling: %v", err)
	}
	if got := descriptor.Capabilities[0].Actions[0].Effect; got != EffectExternal {
		t.Fatalf("registered effect = %q, want %q", got, EffectExternal)
	}
}

// TestResolveActionRefusesAfterARebindToANarrowerCeiling is the case that
// motivated checking at resolution as well as registration. A descriptor
// registered while the host permitted external work must stop resolving once
// the reference is rebound to an evidence-only executor, without the executor
// being invoked.
func TestResolveActionRefusesAfterARebindToANarrowerCeiling(t *testing.T) {
	now := time.Date(2026, 9, 6, 8, 0, 0, 0, time.UTC)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryStore()
	// The registry is mutable, standing in for a host that rebinds a reference
	// between process starts while durable descriptors survive.
	executors := executorMap{"exec": ceilingExecutor{ceiling: EffectExternal}}
	service, err := NewService(Config{
		Store: store, Resolver: authority.Resolver(),
		Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{now},
		Executors: executors, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	decision := effectDecision(t, "effect-request")
	ctx := bindDecision(t, authority, decision)
	descriptor, err := service.Register(ctx,
		externalRegisterRequest(now, "effect-request", "agent", "source-a"))
	if err != nil {
		t.Fatalf("register under the wider ceiling: %v", err)
	}

	// Rebind to an executor that both implements execution and is bound for
	// evidence-only work, so the refusal cannot be attributed to a missing
	// ActionExecutor.
	tracked := &countingActionExecutor{ceiling: EffectEvidence}
	executors["exec"] = tracked

	_, _, _, err = service.resolveAction(
		ctx, decision, descriptor.ID, descriptor.Generation,
		"search", "query", []byte("source-a"), []byte("policy"), "object",
		auth.OperationInvoke, now)
	if err == nil {
		t.Fatal("resolution succeeded against a narrower ceiling")
	}
	if tracked.calls != 0 {
		t.Fatalf("executor was invoked %d times despite the refusal", tracked.calls)
	}
}

// countingActionExecutor implements execution and records whether it ran, so a
// refusal can be distinguished from a silent success.
type countingActionExecutor struct {
	ceiling Effect
	calls   int
}

func (e *countingActionExecutor) MaxEffect() Effect { return e.ceiling }

func (e *countingActionExecutor) Execute(
	context.Context, Invocation,
) (ExecutionResult, error) {
	e.calls++
	return ExecutionResult{}, nil
}

// effectDecision is testDecision plus OperationInvoke, which resolveAction
// authorizes and the shared helper does not grant.
func effectDecision(t *testing.T, requestID string) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "owner", Actor: "owner-actor",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations: []auth.Operation{
			auth.OperationAgentRegister, auth.OperationAgentResolve,
			auth.OperationInvoke, auth.OperationDispatch,
		},
		PermittedSourceIDs:    [][]byte{[]byte("source-a")},
		PermittedPolicyIDs:    [][]byte{[]byte("policy")},
		PolicyGeneration:      1,
		AuthenticationExpires: time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC),
		RequestID:             shoal.ID(requestID),
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}
