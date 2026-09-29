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
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

var anyObject = json.RawMessage(`{"type":"object"}`)

type ceilingExecutor struct{ ceiling Effects }

func (e ceilingExecutor) MaxEffects() Effects { return e.ceiling }

type unboundedExecutor struct{}

// everyEffect is the widest ceiling a host can declare.
func everyEffect() Effects {
	return Effects{
		EffectEgressesContent, EffectMutatesExternal, EffectReadsCorpus,
	}
}

func externalAction() []Capability {
	return []Capability{{Name: "deploy", Actions: []Action{{
		Name: "ship", Effects: Effects{EffectMutatesExternal},
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
		externalAction(), nil); err == nil {
		t.Fatal("an external action must not register against an evidence executor")
	}
	if err := validateDeclaredEffects(
		externalAction(), everyEffect()); err != nil {
		t.Fatalf("an external ceiling must admit an external action: %v", err)
	}
	if err := validateDeclaredEffects(
		evidenceAction(), nil); err != nil {
		t.Fatalf("evidence work must run under an evidence ceiling: %v", err)
	}
}

// TestUndeclaredExecutorIsEvidenceOnly pins the conservative default. A host
// that wants an executor to perform external work has to say so; forgetting to
// declare must not widen what an executor may run.
func TestUndeclaredExecutorIsEvidenceOnly(t *testing.T) {
	if got := executorCeiling(unboundedExecutor{}); len(got) != 0 {
		t.Fatalf("undeclared executor ceiling = %q", got)
	}
	if got := executorCeiling(ceilingExecutor{ceiling: everyEffect()}); got.exceeds(everyEffect()) {
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
	if len(Effects(nil)) != 0 {
		t.Fatal("evidence must be the zero value or existing descriptors change meaning")
	}
	canonical, err := canonicalCapabilities(evidenceAction())
	if err != nil {
		t.Fatal(err)
	}
	if len(canonical[0].Actions[0].Effects) != 0 {
		t.Fatalf("absent effect canonicalized to %q", canonical[0].Actions[0].Effects)
	}
}

// TestUnknownEffectFailsClosed proves a class nobody recognizes is refused
// rather than silently treated as the permissive or the restrictive one.
func TestUnknownEffectFailsClosed(t *testing.T) {
	_, err := canonicalCapabilities([]Capability{{
		Name: "c", Actions: []Action{{
			Name: "a", Effects: Effects{Effect("whatever")},
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
		Name: "ask", Effects: Effects{EffectMutatesExternal},
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
			Name: "ship", Effects: Effects{EffectMutatesExternal},
			InputSchema: anyObject, OutputSchema: anyObject,
		}},
	}}}
	clone := cloneDescriptor(descriptor)
	if got := clone.Capabilities[0].Actions[0].Effects; !got.equalForTest(Effects{EffectMutatesExternal}) {
		t.Fatalf("cloned effects = %v, want %v", got, Effects{EffectMutatesExternal})
	}
}

// TestMutationDigestSeparatesEffects proves an otherwise identical
// registration with a different effect is a different mutation. The digest
// omitted the field, so an exact replay could swap an evidence-only action for
// an external one under the same mutation identity and be treated as the same
// request.
func TestMutationDigestSeparatesEffects(t *testing.T) {
	mutation := func(effects Effects) Mutation {
		return Mutation{Descriptor: Descriptor{
			ID: "agent", Generation: 1,
			Capabilities: []Capability{{Name: "deploy", Actions: []Action{{
				Name: "ship", Effects: effects,
				InputSchema: anyObject, OutputSchema: anyObject,
			}}}},
		}}
	}
	evidence := registryMutationDigest(mutation(nil))
	external := registryMutationDigest(mutation(Effects{EffectMutatesExternal}))
	if evidence == external {
		t.Fatal("effect must change the mutation digest")
	}
	if registryMutationDigest(mutation(Effects{EffectMutatesExternal})) != external {
		t.Fatal("the digest must stay stable for an unchanged effect")
	}
	// Distinct classes are distinct mutations, and a wider set is distinct
	// from either — otherwise a replay could widen a declaration under the
	// same mutation identity.
	egress := registryMutationDigest(mutation(Effects{EffectEgressesContent}))
	both := registryMutationDigest(mutation(Effects{
		EffectEgressesContent, EffectMutatesExternal,
	}))
	if egress == external || both == external || both == egress || egress == evidence {
		t.Fatal("distinct effect sets collided in the mutation digest")
	}
	// Declaration order is not part of the declaration.
	reordered := registryMutationDigest(mutation(Effects{
		EffectMutatesExternal, EffectEgressesContent,
	}))
	if reordered == both {
		return
	}
	t.Fatal("declaration order changed the mutation digest; canonicalization " +
		"must happen before hashing")
}

// TestExternalMutationDigestIsUnchangedAcrossTheSetUpgrade is the cross-upgrade
// half of the golden test below.
//
// Before this change an external action hashed the bare string "external".
// That value is embedded in the lifecycle QueryDigest, where a changed digest
// reads as a divergent mutation, so a heartbeat or revoke retry spanning the
// upgrade would be rejected. EffectMutatesExternal therefore keeps that exact
// wire string, and a set holding only it contributes exactly those bytes.
func TestExternalMutationDigestIsUnchangedAcrossTheSetUpgrade(t *testing.T) {
	declared := Effects{EffectMutatesExternal}
	if got := string(declared.digestBytes()); got != "external" {
		t.Fatalf("external-only digest bytes = %q, want %q: a heartbeat or "+
			"revoke retry spanning the upgrade would be rejected as a "+
			"divergent mutation", got, "external")
	}
	if Effects(nil).digestBytes() != nil {
		t.Fatal("an empty declaration must contribute no bytes at all: hashing " +
			"an empty value still writes an eight-byte length prefix and would " +
			"change every pre-existing digest")
	}
	// A set that could not have existed before is free to hash as itself.
	wider := Effects{EffectEgressesContent, EffectMutatesExternal}
	if got := string(wider.digestBytes()); got != "egresses-content,external" {
		t.Fatalf("wider set digest bytes = %q", got)
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
	unknown := Effects{Effect("something-nobody-defined")}
	external := Effects{EffectMutatesExternal}

	// An unrecognized declaration is an unproven claim: beyond every ceiling.
	for _, ceiling := range []Effects{nil, everyEffect(), unknown} {
		if !unknown.exceeds(ceiling) {
			t.Fatalf("unknown declaration was permitted under ceiling %v", ceiling)
		}
	}

	// The case neither subset containment nor either guard alone catches: a
	// declaration and a ceiling naming the same unrecognized value. Containment
	// alone would report it permitted, because the ceiling does contain it.
	if !unknown.exceeds(unknown) {
		t.Fatal("a declaration was permitted by a ceiling naming the same " +
			"unrecognized class: containment cannot distinguish an agreed-upon " +
			"unknown from a validated one")
	}

	// An unrecognized ceiling is a host that failed to declare itself: it
	// permits the empty set and nothing more.
	if Effects(nil).exceeds(unknown) {
		t.Fatal("a declaration of nothing must remain permitted under an unknown ceiling")
	}
	if !external.exceeds(unknown) {
		t.Fatal("an unknown ceiling must not authorize external work")
	}
	readsCorpus := Effects{EffectReadsCorpus}
	if !readsCorpus.exceeds(unknown) {
		t.Fatal("an unknown ceiling must not authorize corpus reads either")
	}

	// The recognized comparisons are plain subset semantics.
	if external.exceeds(nil) != true ||
		external.exceeds(external) != false ||
		Effects(nil).exceeds(nil) != false ||
		Effects(nil).exceeds(external) != false ||
		external.exceeds(everyEffect()) != false {
		t.Fatal("the recognized comparisons changed")
	}
}

// TestEffectClassesDoNotOrder is why this is a set rather than a ladder.
//
// Writing a local file mutates without transmitting; streaming a compartmented
// corpus to a hosted model transmits without mutating. Neither is a subset of
// the other, so there is no answer to "does egress outrank mutation" — and any
// ladder has to invent one.
func TestEffectClassesDoNotOrder(t *testing.T) {
	egress := Effects{EffectEgressesContent}
	mutation := Effects{EffectMutatesExternal}

	if !egress.exceeds(mutation) {
		t.Fatal("egress was permitted by a ceiling that only allows mutation")
	}
	if !mutation.exceeds(egress) {
		t.Fatal("mutation was permitted by a ceiling that only allows egress")
	}
	// Both are permitted by a ceiling that names both.
	both := Effects{EffectEgressesContent, EffectMutatesExternal}
	if egress.exceeds(both) || mutation.exceeds(both) || both.exceeds(both) {
		t.Fatal("a ceiling naming both classes refused one of them")
	}
	// And a ceiling naming both does not thereby permit a third class.
	readsCorpus := Effects{EffectReadsCorpus}
	if !readsCorpus.exceeds(both) {
		t.Fatal("a ceiling naming egress and mutation also permitted corpus reads")
	}
}

// TestCanonicalEffectsSortsAndDeduplicates pins the property the digest
// depends on: the same declared classes in a different order are the same
// declaration, and must not hash differently.
func TestCanonicalEffectsSortsAndDeduplicates(t *testing.T) {
	canonical, err := canonicalEffects(Effects{
		EffectMutatesExternal, EffectReadsCorpus, EffectMutatesExternal,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := Effects{EffectMutatesExternal, EffectReadsCorpus}
	if !canonical.equalForTest(want) {
		t.Fatalf("canonical = %v, want %v", canonical, want)
	}
	reordered, err := canonicalEffects(Effects{EffectReadsCorpus, EffectMutatesExternal})
	if err != nil {
		t.Fatal(err)
	}
	if !reordered.equalForTest(canonical) {
		t.Fatalf("declaration order changed the canonical form: %v vs %v",
			reordered, canonical)
	}
	if _, err := canonicalEffects(Effects{EffectReadsCorpus, "invented"}); err == nil {
		t.Fatal("an unknown class in a set was accepted")
	}
}

// TestUnknownEffectFromStorageIsRefusedAtRegistration pins the same rule
// through the narrowing comparison, which is the other place a stored value is
// compared rather than validated.
func TestUnknownEffectFromStorageIsRefusedAtRegistration(t *testing.T) {
	unknownCapabilities := []Capability{{Name: "explorer.reason", Actions: []Action{{
		Name: "ask", Effects: Effects{Effect("decoded-from-a-tampered-record")},
		InputSchema: anyObject, OutputSchema: anyObject,
	}}}}
	if err := validateDeclaredEffects(unknownCapabilities, nil); err == nil {
		t.Fatal("an unknown declared effect must not register")
	}
	if err := validateDeclaredEffects(unknownCapabilities, everyEffect()); err == nil {
		t.Fatal("an unknown declared effect must not register even at the widest ceiling")
	}
	if capabilitiesSubset(unknownCapabilities, evidenceAction()) {
		t.Fatal("an unknown effect must not pass the narrowing check")
	}
}

// externalRegisterRequest is registerRequest with the action declaring an
// external effect, so the request reaches Service.Register and exercises the
// real enforcement call rather than the validator underneath it.
func externalRegisterRequest(now time.Time, requestID, id, source string) RegisterRequest {
	request := registerRequest(now, requestID, id, "", source)
	request.Spec.Capabilities[0].Actions[0].Effects = Effects{EffectMutatesExternal}
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
	evidenceOnly := newService(ceilingExecutor{ceiling: nil})
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
	external := newService(ceilingExecutor{ceiling: everyEffect()})
	descriptor, err := external.Register(ctx,
		externalRegisterRequest(now, "effect-request", "agent", "source-a"))
	if err != nil {
		t.Fatalf("Register refused an external action under an external ceiling: %v", err)
	}
	if got := descriptor.Capabilities[0].Actions[0].Effects; !got.equalForTest(Effects{EffectMutatesExternal}) {
		t.Fatalf("registered effects = %v, want %v", got, Effects{EffectMutatesExternal})
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
	executors := executorMap{"exec": ceilingExecutor{ceiling: everyEffect()}}
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
	tracked := &countingActionExecutor{ceiling: nil}
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
	ceiling Effects
	calls   int
}

func (e *countingActionExecutor) MaxEffects() Effects { return e.ceiling }

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

// TestEvidenceMutationDigestIsUnchangedAcrossTheUpgrade pins the byte-level
// stability of an evidence-only mutation digest. The value below was computed
// from main before the effect field existed.
//
// It matters because the digest namespace is still v1 and the value is
// embedded in the lifecycle QueryDigest, where a changed digest reads as a
// divergent mutation. Hashing the zero value would have changed every existing
// mutation, since an empty field still contributes its eight-byte length
// prefix, and a heartbeat or revoke retry that spanned an upgrade would then
// have been rejected.
func TestEvidenceMutationDigestIsUnchangedAcrossTheUpgrade(t *testing.T) {
	const beforeTheEffectField = "990a0fe870e3c27d873e281441bcb7cb" +
		"cf299cf6dd5c6a5060d3dda992d5512f"
	digest := registryMutationDigest(Mutation{Descriptor: Descriptor{
		ID: "agent", Generation: 1,
		Capabilities: []Capability{{Name: "deploy", Actions: []Action{{
			Name: "ship", InputSchema: anyObject, OutputSchema: anyObject,
		}}}},
	}})
	if got := hex.EncodeToString(digest[:]); got != beforeTheEffectField {
		t.Fatalf("evidence-only mutation digest changed\n got  %s\n want %s",
			got, beforeTheEffectField)
	}
}

// equalForTest compares two declarations elementwise. Production code never
// needs this — sets are compared by exceeds — so it lives here.
func (e Effects) equalForTest(other Effects) bool {
	if len(e) != len(other) {
		return false
	}
	for i := range e {
		if e[i] != other[i] {
			return false
		}
	}
	return true
}
