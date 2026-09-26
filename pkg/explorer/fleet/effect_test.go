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
	"encoding/json"
	"testing"
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
