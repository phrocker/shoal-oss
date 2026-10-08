// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package atpl

import (
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// TestPlanRefusesALiveApprovalRequirement is the plan side of the export
// contract. A policy managing an agent whose live actions require approval
// cannot say whether it keeps the requirement, so the entry is refused by name
// rather than planned as unchanged (claiming the file describes the agent) or
// as a narrowing (which the registry would refuse as a widening on apply).
func TestPlanRefusesALiveApprovalRequirement(t *testing.T) {
	document := base(t)
	registry := newRegistry(t, document)
	policy, live := compileLive(t, document, registry)
	applyDirect(t, registry, Diff(policy, live, ""))

	var planner fleet.Spec
	for _, spec := range compileOne(t, base(t)).Agents() {
		if spec.ID == "planner" {
			planner = spec
		}
	}
	planner.Capabilities[0].Actions[0].RequiresApproval = true
	if _, err := registry.Register(t, planner, 1, "key-approval"); err != nil {
		t.Fatalf("register approval on the live agent: %v", err)
	}
	registry.Clock.Set(testNow.Add(time.Minute))

	policy, live = compileLive(t, document, registry)
	plan := Diff(policy, live, "")
	var entry Entry
	for _, candidate := range plan.Entries {
		if candidate.ID == "planner" {
			entry = candidate
		}
	}
	if entry.Kind != KindRefusedApproval || !entry.Kind.Refused() ||
		!strings.Contains(entry.Reason, "#452") {
		t.Fatalf("planner entry = %+v", entry)
	}
	wantPath := "capabilities[name=" + planner.Capabilities[0].Name +
		"].actions[name=" + planner.Capabilities[0].Actions[0].Name + "].approval"
	if len(entry.Changes) != 1 || !strings.Contains(entry.Changes[0].Path, wantPath) {
		t.Fatalf("planner changes = %+v, want %s", entry.Changes, wantPath)
	}
	if len(plan.Writes()) != 0 || len(plan.Refusals()) == 0 {
		t.Fatalf("a refused plan writes: %+v", plan.Writes())
	}
}
