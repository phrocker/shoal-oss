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

package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// gatewayAgentSpec is a descriptor for work whose consequences land outside
// Shoal: it declares the effects given and names the executor reference given,
// so the same spec can be aimed at a bound gateway reference, at the
// reasoning executor, and at a reference that is merely allowlisted.
func gatewayAgentSpec(
	now time.Time, id, reference string, effects fleet.Effects,
) fleet.Spec {
	return fleet.Spec{
		ID: shoal.ID(id), AuthorizationDomain: workspaceAuthorizationDomain,
		Scopes: []fleet.Scope{{
			SourceID: workspaceSourceID, PolicyID: workspaceGrantPolicyID,
		}},
		ExecutorRef: reference,
		Capabilities: []fleet.Capability{{
			Name: "gateway.operate",
			Actions: []fleet.Action{{
				Name:         "ship",
				InputSchema:  json.RawMessage(`{"type":"object"}`),
				OutputSchema: json.RawMessage(`{"type":"object"}`),
				Effects:      effects,
			}},
		}},
		LeaseExpiresAt: now.Add(10 * time.Minute),
	}
}

// TestExternalEffectBindingsAreAnOptInPerReference covers the composition
// seam: what -fleet-external-executor-refs and
// -fleet-external-egress-executor-refs put in the registry, and what every
// other reference keeps meaning.
//
// The last case is the one that holds the whole feature in place. An
// allowlisted reference nobody named must still resolve to a placeholder
// declaring no ceiling — if that stopped holding, every reference in every
// existing deployment would start admitting external declarations on upgrade.
func TestExternalEffectBindingsAreAnOptInPerReference(t *testing.T) {
	executors, err := newConfiguredFleetExecutors(
		[]string{"gateway", "notify", "named"})
	if err != nil {
		t.Fatal(err)
	}
	if err := bindExternalFleetEffects(executors, externalFleetEffectBindings{
		mutating:     []string{"gateway"},
		transmitting: []string{"notify"},
	}); err != nil {
		t.Fatalf("bind the declared references: %v", err)
	}
	for _, probe := range []struct {
		reference string
		want      fleet.Effects
	}{
		{"gateway", fleet.Effects{fleet.EffectMutatesExternal}},
		{"notify", fleet.Effects{
			fleet.EffectEgressesContent, fleet.EffectMutatesExternal,
		}},
	} {
		resolved, ok := executors.ResolveExecutor(probe.reference)
		if !ok {
			t.Fatalf("%s did not resolve", probe.reference)
		}
		bounded, ok := resolved.(fleet.EffectBounded)
		if !ok {
			t.Fatalf("%s resolved to something declaring no ceiling",
				probe.reference)
		}
		got := bounded.MaxEffects()
		if len(got) != len(probe.want) {
			t.Fatalf("%s ceiling = %v, want %v",
				probe.reference, got, probe.want)
		}
		for i := range got {
			if got[i] != probe.want[i] {
				t.Fatalf("%s ceiling = %v, want %v",
					probe.reference, got, probe.want)
			}
		}
		// Nothing bound this way runs in process. The reference is reachable
		// over the dispatch queue and nowhere else.
		if _, performs := resolved.(fleet.ActionExecutor); performs {
			t.Fatalf("%s resolved to an in-process executor", probe.reference)
		}
	}
	// Named in -fleet-executor-refs and nowhere else: unchanged, and permits
	// nothing.
	resolved, ok := executors.ResolveExecutor("named")
	if !ok {
		t.Fatal("an allowlisted reference must still resolve")
	}
	if _, bounded := resolved.(fleet.EffectBounded); bounded {
		t.Fatal("a reference nobody named for external work declared a " +
			"ceiling; the opt-in is per reference")
	}
}

// TestNoExternalCeilingWithoutTheFlags is the acceptance criterion that no
// configuration produces an external-mutation ceiling by omission.
//
// It is asserted as the absence of EffectBounded across every allowlisted
// reference rather than as "the ceiling is narrow", because the failure it
// guards against is a default that widens when a list is left unset — and such
// a default would make every positive test in this file pass too.
func TestNoExternalCeilingWithoutTheFlags(t *testing.T) {
	for _, absent := range []struct {
		name   string
		config externalFleetEffectBindings
	}{
		{"nothing set at all", externalFleetEffectBindings{}},
		{
			"an ask reference and no external lists",
			externalFleetEffectBindings{askReference: "ask"},
		},
		{
			// splitCommaList drops blank entries, so a flag an operator set to
			// whitespace is the same as an unset one here. The chart refuses
			// that at render time precisely because the binary cannot: a flag
			// value this process never sees is not a value it can refuse.
			"lists holding only blanks",
			externalFleetEffectBindings{
				mutating:     splitCommaList("  "),
				transmitting: splitCommaList(" , "),
			},
		},
	} {
		executors, err := newConfiguredFleetExecutors(
			[]string{"ask", "gateway", "notify"})
		if err != nil {
			t.Fatal(err)
		}
		if err := bindExternalFleetEffects(
			executors, absent.config); err != nil {
			t.Fatalf("%s: %v", absent.name, err)
		}
		for _, reference := range []string{"ask", "gateway", "notify"} {
			resolved, ok := executors.ResolveExecutor(reference)
			if !ok {
				t.Fatalf("%s: %s did not resolve", absent.name, reference)
			}
			if _, bounded := resolved.(fleet.EffectBounded); bounded {
				t.Fatalf("%s: %s acquired an effect ceiling with no "+
					"reference naming it", absent.name, reference)
			}
		}
	}
}

// TestExternalEffectBindingsRefuseAContradictoryReference covers the three
// values files that say two things about one reference. Each would otherwise be
// settled silently by the order these bindings happen to be applied in, and
// two of the three settle it by widening.
func TestExternalEffectBindingsRefuseAContradictoryReference(t *testing.T) {
	for _, refused := range []struct {
		name   string
		cites  string
		config externalFleetEffectBindings
	}{
		{
			// The wider ceiling winning would mean egress authority is
			// acquired by naming a reference twice.
			name:  "one reference in both effect lists",
			cites: "-fleet-external-egress-executor-refs",
			config: externalFleetEffectBindings{
				mutating:     []string{"gateway"},
				transmitting: []string{"gateway"},
			},
		},
		{
			// Not narrower and wider but incompatible: AskExecutor's floor
			// equals its ceiling and excludes external mutation, so whichever
			// bound last would break the other.
			name:  "the ask reference named for external mutation",
			cites: "-fleet-ask-executor-ref",
			config: externalFleetEffectBindings{
				mutating:     []string{"gateway"},
				askReference: "gateway",
			},
		},
		{
			name:  "the ask reference named for transmitting external work",
			cites: "-fleet-ask-executor-ref",
			config: externalFleetEffectBindings{
				transmitting: []string{"gateway"},
				askReference: "gateway",
			},
		},
	} {
		executors, err := newConfiguredFleetExecutors([]string{"gateway"})
		if err != nil {
			t.Fatal(err)
		}
		err = bindExternalFleetEffects(executors, refused.config)
		if err == nil {
			t.Fatalf("%s: a contradictory reference was bound", refused.name)
		}
		if !strings.Contains(err.Error(), refused.cites) {
			t.Fatalf("%s: refused, but not by the guard under test: %v",
				refused.name, err)
		}
	}
	// And a reference the allow-list does not carry is refused by bind, so a
	// binding the registry would never accept cannot be created.
	executors, err := newConfiguredFleetExecutors([]string{"gateway"})
	if err != nil {
		t.Fatal(err)
	}
	if err := bindExternalFleetEffects(executors, externalFleetEffectBindings{
		mutating: []string{"smuggled"},
	}); err == nil {
		t.Fatal("an unallowlisted reference was bound for external work")
	}
	if _, resolved := executors.ResolveExecutor("smuggled"); resolved {
		t.Fatal("a refused binding must not enter the registry")
	}
}

// TestExternalDescriptorRegistersOnlyAgainstABoundReference is #436's
// acceptance criteria through the real composition: the registry the explorer
// actually serves, the ask executor the explorer actually binds, and the
// placeholder an allowlisted reference actually resolves to.
//
// The three outcomes are the point. Against a reference named in
// -fleet-external-executor-refs the descriptor registers. Against
// -fleet-ask-executor-ref it is refused, which is the result the comment on
// AskExecutor.MaxEffects asks for and the one it would be tempting to get by
// widening that ceiling instead. Against a reference that is merely
// allowlisted it is refused, which is what makes the new flags an opt-in.
func TestExternalDescriptorRegistersOnlyAgainstABoundReference(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(time.Minute)
	authority := auth.NewAuthority()
	executors, err := newConfiguredFleetExecutors(
		[]string{"gateway", "ask", "named"})
	if err != nil {
		t.Fatal(err)
	}
	if err := bindExternalFleetEffects(executors, externalFleetEffectBindings{
		mutating:     []string{"gateway"},
		askReference: "ask",
	}); err != nil {
		t.Fatal(err)
	}
	// The real executor the explorer binds for -fleet-ask-executor-ref. Its
	// provider is a stub only because nothing here invokes it; the bound it
	// declares is derived from that provider exactly as in production.
	askExecutor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
		Provider: bindStubProvider{}, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := executors.bind("ask", askExecutor); err != nil {
		t.Fatal(err)
	}
	opened, err := openService(context.Background(), serviceConfig{
		backend: "embedded", data: filepath.Join(root, "corpus"),
		policyDir: filepath.Join(root, "policy"),
		resolver:  authority.Resolver(), clock: func() time.Time { return now },
		executors: executors,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.close()

	register := func(
		t *testing.T, id, reference string, effects fleet.Effects,
	) error {
		t.Helper()
		ctx, err := authority.Binder().Bind(context.Background(), askDecision(
			t, now, "gateway-"+id, auth.OperationAgentRegister))
		if err != nil {
			t.Fatal(err)
		}
		_, err = opened.fleetRegistry.Register(ctx, fleet.RegisterRequest{
			Context: fleet.RequestContext{
				RequestID: "register", ReasonCode: "test",
				Deadline: now.Add(time.Minute),
			},
			RegistrationKey: shoal.ID("key-" + id),
			Spec:            gatewayAgentSpec(now, id, reference, effects),
		})
		return err
	}
	external := fleet.Effects{fleet.EffectMutatesExternal}
	// The ceiling refusal, as validateDeclaredEffects words it. The refusals
	// below are matched against it rather than against "any error", because
	// the floor refusal is also available against the reasoning executor and
	// is not the same statement: its floor equals its ceiling, so a descriptor
	// declaring only {external} omits what every invocation causes as well as
	// exceeding what it permits. A bare "was refused" would therefore keep
	// passing with the ask ceiling widened to admit external mutation — which
	// is precisely the change this test exists to refuse.
	const exceedsTheCeiling = "not bound to perform"
	refusedByTheCeiling := func(t *testing.T, name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: external mutation registered", name)
		}
		if !strings.Contains(err.Error(), exceedsTheCeiling) {
			t.Fatalf("%s: refused, but not by the ceiling: %v", name, err)
		}
	}

	if err := register(t, "bound", "gateway", external); err != nil {
		t.Fatalf("a descriptor declaring external mutation must register "+
			"against a reference bound for it: %v", err)
	}
	refusedByTheCeiling(t, "the grounded-reasoning executor",
		register(t, "ask", "ask", external))
	refusedByTheCeiling(t, "an allowlisted but unbound reference",
		register(t, "named", "named", external))
	// The second witness, independent of that wording. This declaration states
	// everything the reasoning executor always causes *and* external mutation,
	// so it omits nothing the floor requires and the ceiling is the only thing
	// that can reject it. A bare refusal is therefore enough here, which is
	// what makes it independent: the case above depends on the error sentence,
	// this one depends on the declaration, and a widened ask ceiling has to
	// get past both.
	widest := append(
		append(fleet.Effects(nil),
			webapi.AskActionEffects(bindStubProvider{})...),
		fleet.EffectMutatesExternal)
	if err := register(t, "ask-superset", "ask", widest); err == nil {
		t.Fatal("the reasoning executor admitted external mutation in a " +
			"declaration its floor has no objection to; its ceiling must " +
			"exclude external mutation whatever else an action declares")
	}
	// The reasoning executor still serves its own work. The refusal above is
	// the ceiling refusing external mutation, not the ask binding having been
	// displaced by anything added here.
	askCtx, err := authority.Binder().Bind(context.Background(), askDecision(
		t, now, "gateway-ask-own", auth.OperationAgentRegister))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opened.fleetRegistry.Register(askCtx, fleet.RegisterRequest{
		Context: fleet.RequestContext{
			RequestID: "register", ReasonCode: "test",
			Deadline: now.Add(time.Minute),
		},
		RegistrationKey: "key-ask-own",
		Spec:            askAgentSpec(now, bindStubProvider{}),
	}); err != nil {
		t.Fatalf("the reasoning descriptor must still register: %v", err)
	}
}
