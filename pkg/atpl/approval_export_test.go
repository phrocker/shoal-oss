// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package atpl

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestExportRefusesAnApprovalRequiredAction pins the slice-1 contract with
// #451: an action that requires approval cannot be written to a policy file by
// this version, so export refuses it by path rather than writing the action
// without the requirement. A file missing the flag is a policy that, applied,
// would describe the agent without its control.
func TestExportRefusesAnApprovalRequiredAction(t *testing.T) {
	descriptor := fleet.Descriptor{
		ID: "agent", Generation: 1, AuthorizationDomain: []byte("domain"),
		Scopes:      []fleet.Scope{{SourceID: []byte("source"), PolicyID: []byte("policy")}},
		ExecutorRef: "exec",
		Capabilities: []fleet.Capability{{Name: "gateway", Actions: []fleet.Action{
			{
				Name:        "read",
				InputSchema: json.RawMessage(`{}`), OutputSchema: json.RawMessage(`{}`),
			},
			{
				Name:        "ship",
				InputSchema: json.RawMessage(`{}`), OutputSchema: json.RawMessage(`{}`),
				RequiresApproval: true,
			},
		}}},
		LeaseExpiresAt: testNow.Add(time.Hour), UpdatedAt: testNow,
	}
	_, err := Export(map[shoal.ID]fleet.Descriptor{"agent": descriptor}, nil, testNow)
	if err == nil {
		t.Fatal("export wrote an approval-required action")
	}
	for _, want := range []string{
		"agents[id=agent].capabilities[name=gateway].actions[name=ship].approval",
		"requires approval",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("export refusal = %q, want it to contain %q", err, want)
		}
	}
	// The refusal is the flag and nothing else: the same agent without it
	// exports, so this is not an agent export refuses for another reason.
	descriptor.Capabilities[0].Actions[1].RequiresApproval = false
	if _, err := Export(
		map[shoal.ID]fleet.Descriptor{"agent": descriptor}, nil, testNow,
	); err != nil {
		t.Fatalf("export without approval = %v", err)
	}
}
