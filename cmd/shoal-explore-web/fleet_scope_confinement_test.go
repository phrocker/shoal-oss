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

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/model"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// A second source in the workspace domain, selected by URI prefix. Only this
// test ingests into it.
var (
	otherSourceID      = []byte("shoal-explore-web/other")
	otherGrantPolicyID = []byte("shoal-explore-web/other-grant")
)

const otherSourceURIPrefix = "shoal://other/"

// twoSourceSelector selects the other source for a URI under its prefix and
// the workspace source for everything else, application edges included.
type twoSourceSelector struct{}

func (twoSourceSelector) SelectPolicy(
	_ context.Context, decision auth.Decision, source explorer.Source,
) (auth.Policy, error) {
	if strings.HasPrefix(source.URI, otherSourceURIPrefix) {
		return selectorPolicy(decision, otherSourceID, otherGrantPolicyID)
	}
	return selectorPolicy(decision, workspaceSourceID, workspaceGrantPolicyID)
}

func (twoSourceSelector) SelectEdgePolicy(
	_ context.Context, decision auth.Decision, _ graph.Edge,
) (auth.Policy, error) {
	return selectorPolicy(decision, workspaceSourceID, workspaceGrantPolicyID)
}

func selectorPolicy(decision auth.Decision, source, grant []byte) (auth.Policy, error) {
	return auth.NewPolicy(auth.PolicyConfig{
		AuthorizationDomain: decision.AuthorizationDomain(),
		SourceID:            source, GrantPolicyID: grant,
		Epoch: decision.PolicyGeneration(),
	})
}

var _ authorized.PolicySelector = twoSourceSelector{}

// bothSourcesDecision may read the workspace source and the other one: a
// principal whose reach is wider than the agent's scope.
func bothSourcesDecision(
	t *testing.T, now time.Time, requestID string, operations ...auth.Operation,
) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "owner", Actor: "operator",
		AuthorizationDomain:   workspaceAuthorizationDomain,
		AllowedOperations:     operations,
		PermittedSourceIDs:    [][]byte{workspaceSourceID, otherSourceID},
		PermittedPolicyIDs:    [][]byte{workspaceGrantPolicyID, otherGrantPolicyID},
		PolicyGeneration:      workspacePolicyGeneration,
		AuthenticationExpires: now.Add(time.Hour),
		RequestID:             shoal.ID(requestID),
		CorrelationID:         shoal.ID(requestID + "-correlation"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

// unconfiningProvider is the real reasoning service behind an interface that
// hides HonoursRetrievalBound: a provider that could drop the bound.
// It forwards the egress posture, so it registers under the same effects.
type unconfiningProvider struct{ webapi.AskProvider }

func (p unconfiningProvider) EgressesOffHost() bool {
	return model.EgressesOffHost(p.AskProvider)
}

// TestAScopedAgentDrawsNoEvidenceFromAnotherSource is #560's acceptance test,
// and the third acceptance bullet of #370 that change could not satisfy: an
// agent scoped to the workspace source, invoked by a principal that can read
// both the workspace source and another, records evidence drawn from the
// workspace source only — through the real fleet invocation path, the real
// reasoning service and a real two-source corpus.
//
// It also pins the fail-closed half: the same invocation against an executor
// whose provider does not honour the bound is still refused, as #370 refused
// every one before the bound existed.
func TestAScopedAgentDrawsNoEvidenceFromAnotherSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	now := time.Now().UTC().Add(time.Minute)
	authority := auth.NewAuthority()
	executors := configuredFleetExecutors{
		"ask":   configuredFleetExecutor{reference: "ask"},
		"plain": configuredFleetExecutor{reference: "plain"},
	}
	opened, err := openService(context.Background(), serviceConfig{
		backend: "embedded", data: filepath.Join(root, "corpus"),
		policyDir: filepath.Join(root, "policy"),
		resolver:  authority.Resolver(), clock: func() time.Time { return now },
		executors: executors, policySelector: twoSourceSelector{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.close()
	if opened.client == nil {
		t.Skip("embedded service exposes no authorized client")
	}
	provenance, err := inference.NewModelProvenance("fake", "fake", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	chat, err := webapi.NewChatService(context.Background(), webapi.ChatConfig{
		Client: opened.client, Resolver: authority.Resolver(),
		Generator: model.FakeGenerator{Model: "fake"}, Model: provenance,
		RetrievalModes: []retrieval.Mode{retrieval.ModeLexical},
		Clock:          func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	for reference, provider := range map[string]webapi.AskProvider{
		"ask": chat, "plain": unconfiningProvider{chat},
	} {
		executor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
			Provider: provider, Clock: func() time.Time { return now },
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := executors.bind(reference, executor); err != nil {
			t.Fatal(err)
		}
	}

	reads := []auth.Operation{
		auth.OperationRetrieve, auth.OperationRead, auth.OperationList,
		auth.OperationNeighborhood, auth.OperationWorkspaceSettingsRead,
		auth.OperationValidate,
	}
	ingestCtx, err := authority.Binder().Bind(context.Background(),
		bothSourcesDecision(t, now, "scope-ingest",
			append([]auth.Operation{auth.OperationIngest}, reads...)...))
	if err != nil {
		t.Fatal(err)
	}
	inScope, err := opened.client.Ingest(ingestCtx, explorer.Source{
		URI: "shoal://test/promotion.md", Title: "Promotion",
		MediaType: "text/markdown",
		Content:   "# Promotion\n\nLocal tables promote under a fenced handoff.\n",
	})
	if err != nil {
		t.Fatalf("ingest the in-scope source: %v", err)
	}
	outOfScope, err := opened.client.Ingest(ingestCtx, explorer.Source{
		URI: otherSourceURIPrefix + "promotion.md", Title: "Other promotion",
		MediaType: "text/markdown",
		Content:   "# Other\n\nRemote tables promote under a fenced handoff too.\n",
	})
	if err != nil {
		t.Fatalf("ingest the out-of-scope source: %v", err)
	}
	outOfScopeView, err := opened.client.Document(ingestCtx, outOfScope.Document.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	outOfScopeNodes := map[shoal.ID]struct{}{outOfScope.Document.ID: {}}
	var walk func(explorer.SectionView)
	walk = func(section explorer.SectionView) {
		outOfScopeNodes[section.Section.ID] = struct{}{}
		for _, span := range section.Spans {
			outOfScopeNodes[span.ID] = struct{}{}
		}
		for _, child := range section.Children {
			walk(child)
		}
	}
	walk(outOfScopeView.Root)

	const question = "fenced handoff promote"
	// Control: the principal can read both sources and the question grounds
	// in both, so a scoped answer citing one is the bound at work, not the
	// corpus or the principal.
	control, err := opened.client.Retrieve(ingestCtx, retrieval.Request{
		Text: question, TopK: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	reached := map[shoal.ID]bool{}
	for _, result := range control.Results {
		for _, evidence := range result.Evidence {
			reached[evidence.Citation.DocumentID] = true
		}
	}
	if !reached[inScope.Document.ID] || !reached[outOfScope.Document.ID] {
		t.Fatalf("control retrieval reached %v; it must reach both sources", reached)
	}

	registerCtx, err := authority.Binder().Bind(context.Background(),
		askDecision(t, now, "scope-register", auth.OperationAgentRegister))
	if err != nil {
		t.Fatal(err)
	}
	invokeCtx, err := authority.Binder().Bind(context.Background(),
		bothSourcesDecision(t, now, "scope-invoke",
			append([]auth.Operation{auth.OperationInvoke}, reads...)...))
	if err != nil {
		t.Fatal(err)
	}
	invoke := func(t *testing.T, reference string) (fleet.ActionRecord, error) {
		t.Helper()
		spec := askAgentSpec(now, chat)
		spec.ID = shoal.ID("scope-agent-" + reference)
		spec.ExecutorRef = reference
		descriptor, err := opened.fleetRegistry.Register(
			registerCtx, fleet.RegisterRequest{
				Context: fleet.RequestContext{
					RequestID: shoal.ID("register-" + reference), ReasonCode: "test",
					Deadline: now.Add(time.Minute),
				},
				RegistrationKey: shoal.ID("scope-registration-" + reference),
				Spec:            spec,
			})
		if err != nil {
			t.Fatalf("register %s agent: %v", reference, err)
		}
		return opened.fleetDispatch.Invoke(invokeCtx, fleet.InvokeRequest{
			Enqueue: fleet.EnqueueRequest{
				ID:             []byte("scope-" + reference),
				IdempotencyKey: []byte("scope-key-" + reference),
				AgentID:        descriptor.ID, AgentGeneration: descriptor.Generation,
				Capability: webapi.AskCapability, Action: webapi.AskAction,
				SourceID: workspaceSourceID, PolicyID: workspaceGrantPolicyID,
				ObjectID: "corpus",
				Input:    json.RawMessage(`{"question":"` + question + `"}`),
				Context: fleet.RequestContext{
					RequestID: shoal.ID("invoke-" + reference), CorrelationID: shoal.ID("scope-" + reference),
					ReasonCode: "test", Deadline: now.Add(time.Minute),
				},
			},
			ClaimID: []byte("scope-claim-" + reference), Lease: time.Minute,
		})
	}

	t.Run("a bound-honouring executor runs confined", func(t *testing.T) {
		record, err := invoke(t, "ask")
		if err != nil {
			t.Fatalf("invoke the confining executor: %v", err)
		}
		if record.State != fleet.DispatchSucceeded {
			t.Fatalf("action state = %q, error code = %q", record.State, record.ErrorCode)
		}
		if len(record.Evidence) == 0 {
			t.Fatal("a grounded answer must record evidence")
		}
		citedInScope := false
		for _, evidence := range record.Evidence {
			if evidence.Citation.DocumentID == outOfScope.Document.ID {
				t.Fatalf("recorded evidence cites the out-of-scope source: %#v", evidence)
			}
			if evidence.Citation.DocumentID == inScope.Document.ID {
				citedInScope = true
			}
			for _, nodeID := range evidence.NodeIDs {
				if _, ok := outOfScopeNodes[nodeID]; ok {
					t.Fatalf("recorded evidence names out-of-scope node %s", nodeID)
				}
			}
		}
		if !citedInScope {
			t.Fatalf("recorded evidence = %#v; it must cite the in-scope source", record.Evidence)
		}
	})

	t.Run("an executor that cannot honour the bound is still refused", func(t *testing.T) {
		_, err := invoke(t, "plain")
		if !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) ||
			!strings.Contains(err.Error(), "does not confine retrieval") {
			t.Fatalf("unconfined invoke = %v, want the #370 refusal", err)
		}
	})
}
