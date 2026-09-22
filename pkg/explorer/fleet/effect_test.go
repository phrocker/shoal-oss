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
