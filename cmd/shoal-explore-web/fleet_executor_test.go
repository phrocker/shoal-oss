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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/reasoning"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// stubAskProvider stands in for the authorized reasoning service. It returns a
// fixed verified envelope so the test exercises the executor, dispatch
// admission, schema validation, evidence validation, and the terminal
// transition rather than a model.
type stubAskProvider struct {
	envelope webapi.CitationEnvelope
	err      error
	calls    int
	last     webapi.AskRequest
}

func (p *stubAskProvider) Ask(
	_ context.Context, request webapi.AskRequest,
) (webapi.CitationEnvelope, error) {
	p.calls++
	p.last = request
	if p.err != nil {
		return webapi.CitationEnvelope{}, p.err
	}
	return p.envelope, nil
}

func verifiedAskEnvelope(asOf time.Time) webapi.CitationEnvelope {
	citation := document.Citation{
		DocumentID: "document", RevisionID: "revision", SectionID: "section",
	}
	return webapi.CitationEnvelope{
		Finalized:       true,
		DurablyRecorded: true,
		Verification:    reasoning.VerificationVerified,
		SessionID:       "session",
		SnapshotID:      "snapshot",
		SnapshotAsOf:    asOf,
		Evidence: []webapi.CitationEvidence{{
			AnchorID:   "anchor",
			Citation:   &citation,
			Visibility: []string{"public"},
		}},
	}
}

func askAgentSpec(now time.Time) fleet.Spec {
	return fleet.Spec{
		ID: "ask-agent", AuthorizationDomain: workspaceAuthorizationDomain,
		Scopes: []fleet.Scope{{
			SourceID: workspaceSourceID, PolicyID: workspaceGrantPolicyID,
		}},
		ExecutorRef: "ask",
		Capabilities: []fleet.Capability{{
			Name: webapi.AskCapability,
			Actions: []fleet.Action{{
				Name:         webapi.AskAction,
				InputSchema:  webapi.AskActionInputSchema(),
				OutputSchema: webapi.AskActionOutputSchema(),
			}},
		}},
		LeaseExpiresAt: now.Add(10 * time.Minute),
	}
}

func askDecision(
	t *testing.T, now time.Time, requestID string, operations ...auth.Operation,
) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "owner", Actor: "operator",
		AuthorizationDomain:   workspaceAuthorizationDomain,
		AllowedOperations:     operations,
		PermittedSourceIDs:    [][]byte{workspaceSourceID},
		PermittedPolicyIDs:    [][]byte{workspaceGrantPolicyID},
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

// TestAskExecutorCompletesDispatchedAction is the end-to-end proof for #362:
// a registered agent's action runs through the real production executor and
// commits a succeeded record carrying authorized evidence. No executor test
// double participates.
func TestAskExecutorCompletesDispatchedAction(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(time.Minute)
	noEvidence := verifiedAskEnvelope(now.Add(-time.Hour))
	noEvidence.Evidence = nil
	provider := &stubAskProvider{envelope: noEvidence}
	executor, err := webapi.NewAskExecutor(
		webapi.AskExecutorConfig{Provider: provider})
	if err != nil {
		t.Fatal(err)
	}
	authority := auth.NewAuthority()
	opened, err := openService(context.Background(), serviceConfig{
		backend: "embedded", data: filepath.Join(root, "corpus"),
		policyDir: filepath.Join(root, "policy"),
		resolver:  authority.Resolver(), clock: func() time.Time { return now },
		executors: configuredFleetExecutors{"ask": executor},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.close()
	if opened.fleetRegistry == nil || opened.fleetDispatch == nil {
		t.Fatal("embedded service did not compose the fleet")
	}

	registerCtx, err := authority.Binder().Bind(
		context.Background(),
		askDecision(t, now, "ask-register", auth.OperationAgentRegister))
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := opened.fleetRegistry.Register(
		registerCtx, fleet.RegisterRequest{
			Context: fleet.RequestContext{
				RequestID: "register", ReasonCode: "test",
				Deadline: now.Add(time.Minute),
			},
			RegistrationKey: "ask-registration",
			Spec:            askAgentSpec(now),
		})
	if err != nil {
		t.Fatalf("register ask agent: %v", err)
	}

	invokeCtx, err := authority.Binder().Bind(
		context.Background(),
		askDecision(t, now, "ask-invoke", auth.OperationInvoke))
	if err != nil {
		t.Fatal(err)
	}
	record, err := opened.fleetDispatch.Invoke(invokeCtx, fleet.InvokeRequest{
		Enqueue: fleet.EnqueueRequest{
			ID: []byte("ask-action"), IdempotencyKey: []byte("ask-idempotency"),
			AgentID: descriptor.ID, AgentGeneration: descriptor.Generation,
			Capability: webapi.AskCapability, Action: webapi.AskAction,
			SourceID: workspaceSourceID, PolicyID: workspaceGrantPolicyID,
			ObjectID: "corpus",
			Input:    json.RawMessage(`{"question":"what gates promotion?"}`),
			Context: fleet.RequestContext{
				RequestID: "invoke", CorrelationID: "ask-correlation",
				ReasonCode: "test", Deadline: now.Add(time.Minute),
			},
		},
		ClaimID: []byte("ask-claim"), Lease: time.Minute,
	})
	if err != nil {
		t.Fatalf("invoke ask action: %v", err)
	}
	if record.State != fleet.DispatchSucceeded {
		t.Fatalf("action state = %q, error code = %q", record.State, record.ErrorCode)
	}
	if provider.calls != 1 {
		t.Fatalf("reasoning provider calls = %d", provider.calls)
	}
	if provider.last.Question != "what gates promotion?" {
		t.Fatalf("question = %q", provider.last.Question)
	}
	if !record.EffectPossible {
		t.Fatal("a completed execution must record that an effect was possible")
	}
	// This envelope carries no evidence, so the record must carry no snapshot
	// pin either: the dispatch service rejects a pin without evidence.
	if len(record.Evidence) != 0 || record.EvidenceSnapshotID != "" ||
		!record.EvidenceSnapshotAsOf.IsZero() {
		t.Fatalf("unpinned execution recorded a snapshot: %#v", record.Evidence)
	}
	var output webapi.AskExecutionOutput
	if err := json.Unmarshal(record.Output, &output); err != nil {
		t.Fatalf("decode action output: %v", err)
	}
	if output.Verification != string(reasoning.VerificationVerified) ||
		output.EvidenceCount != 0 {
		t.Fatalf("action output = %#v", output)
	}
}

// TestAskExecutorEvidenceIsCanonical proves the executor translates verified
// citation evidence into dispatch evidence that satisfies the interaction
// evidence variants. It canonicalizes each produced reference through the same
// public validation the dispatch service applies before recording an effect,
// so a mapping regression fails here rather than at commit time.
func TestAskExecutorEvidenceIsCanonical(t *testing.T) {
	now := time.Now().UTC()
	executor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
		Provider: &stubAskProvider{envelope: verifiedAskEnvelope(now.Add(-time.Hour))},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: webapi.AskCapability, Action: webapi.AskAction,
		Input: json.RawMessage(`{"question":"what gates promotion?"}`),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(result.Evidence) != 1 {
		t.Fatalf("evidence = %#v", result.Evidence)
	}
	evidence := result.Evidence[0]
	if evidence.AnchorID != "anchor" ||
		evidence.Kind != interaction.EvidenceDocument ||
		evidence.Citation.DocumentID != "document" {
		t.Fatalf("evidence = %#v", evidence)
	}
	// A document citation names its document and section, so exactly those two
	// source roles must appear as nodes and no graph members may.
	if len(evidence.NodeIDs) != 2 ||
		len(evidence.EdgeIDs) != 0 || len(evidence.Assertions) != 0 {
		t.Fatalf("evidence members = %#v", evidence)
	}
	reference := interaction.EvidenceReference{
		AnchorID: evidence.AnchorID, Kind: evidence.Kind,
		Citation: evidence.Citation, NodeIDs: evidence.NodeIDs,
		EdgeIDs: evidence.EdgeIDs, Assertions: evidence.Assertions,
	}
	if _, err := reference.Canonical(); err != nil {
		t.Fatalf("executor evidence is not canonical: %v", err)
	}
	if result.EvidenceSnapshotID != "snapshot" ||
		!result.EvidenceSnapshotAsOf.Equal(now.Add(-time.Hour)) {
		t.Fatalf("evidence snapshot = %q at %v",
			result.EvidenceSnapshotID, result.EvidenceSnapshotAsOf)
	}
}

// TestAskExecutorFailsClosedOnUnverifiedResponse proves the executor refuses to
// record a successful effect over a response the reasoning path did not verify
// and durably record.
func TestAskExecutorFailsClosedOnUnverifiedResponse(t *testing.T) {
	envelope := verifiedAskEnvelope(time.Now().UTC().Add(-time.Hour))
	envelope.Verification = reasoning.VerificationUnverified
	executor, err := webapi.NewAskExecutor(
		webapi.AskExecutorConfig{Provider: &stubAskProvider{envelope: envelope}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: webapi.AskCapability, Action: webapi.AskAction,
		Input: json.RawMessage(`{"question":"anything"}`),
	})
	if err == nil {
		t.Fatal("an unverified response must fail the action")
	}
	if result.ErrorCode != webapi.AskErrorUnverified {
		t.Fatalf("error code = %q", result.ErrorCode)
	}
	if len(result.Evidence) != 0 || result.Output != nil {
		t.Fatalf("a failed execution must not carry output or evidence: %#v", result)
	}
}

// TestAskExecutorRejectsForeignAction proves an executor bound to one
// capability refuses to run another agent's action rather than silently
// answering it.
func TestAskExecutorRejectsForeignAction(t *testing.T) {
	executor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
		Provider: &stubAskProvider{
			envelope: verifiedAskEnvelope(time.Now().UTC().Add(-time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: "documents", Action: "delete",
		Input: json.RawMessage(`{"question":"anything"}`),
	})
	if err == nil || result.ErrorCode != webapi.AskErrorUnsupportedAction {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
}

// TestAskExecutorSurfacesReasoningFailure proves a provider failure fails the
// action with a durable, distinguishable code rather than an empty success.
func TestAskExecutorSurfacesReasoningFailure(t *testing.T) {
	failure := errors.New("reasoning unavailable")
	executor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
		Provider: &stubAskProvider{err: failure},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: webapi.AskCapability, Action: webapi.AskAction,
		Input: json.RawMessage(`{"question":"anything"}`),
	})
	if !errors.Is(err, failure) {
		t.Fatalf("error = %v", err)
	}
	if result.ErrorCode != webapi.AskErrorReasoningFailed {
		t.Fatalf("error code = %q", result.ErrorCode)
	}
}

// TestBindRejectsUnallowlistedExecutorReference proves late binding cannot
// widen the host-owned executor allowlist.
func TestBindRejectsUnallowlistedExecutorReference(t *testing.T) {
	executor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
		Provider: &stubAskProvider{
			envelope: verifiedAskEnvelope(time.Now().UTC().Add(-time.Hour)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := configuredFleetExecutors{"allowed": configuredFleetExecutor{
		reference: "allowed",
	}}
	if err := registry.bind("smuggled", executor); err == nil {
		t.Fatal("binding an unallowlisted reference must fail closed")
	}
	if _, resolved := registry.ResolveExecutor("smuggled"); resolved {
		t.Fatal("a refused binding must not enter the registry")
	}
	if err := registry.bind("allowed", executor); err != nil {
		t.Fatalf("binding an allowlisted reference: %v", err)
	}
	bound, resolved := registry.ResolveExecutor("allowed")
	if !resolved {
		t.Fatal("an allowlisted reference must resolve after binding")
	}
	if _, ok := bound.(fleet.ActionExecutor); !ok {
		t.Fatal("a bound reference must resolve to a real action executor")
	}
}

// TestAskExecutorForwardsAbsentTopKForWorkspaceClamping proves the executor
// does not substitute its own retrieval width. Sending a constant here would
// exceed a workspace whose retrieval limit is lower than that constant and
// fail an action the same principal can ask over chat.
func TestAskExecutorForwardsAbsentTopKForWorkspaceClamping(t *testing.T) {
	provider := &stubAskProvider{
		envelope: verifiedAskEnvelope(time.Now().UTC().Add(-time.Hour)),
	}
	executor, err := webapi.NewAskExecutor(
		webapi.AskExecutorConfig{Provider: provider})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: webapi.AskCapability, Action: webapi.AskAction,
		Input: json.RawMessage(`{"question":"anything"}`),
	}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if provider.last.TopK != 0 {
		t.Fatalf("absent top_k was rewritten to %d", provider.last.TopK)
	}
	if _, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: webapi.AskCapability, Action: webapi.AskAction,
		Input: json.RawMessage(`{"question":"anything","top_k":3}`),
	}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if provider.last.TopK != 3 {
		t.Fatalf("explicit top_k = %d", provider.last.TopK)
	}
}

// TestAskExecutorFailsWhenNoEvidenceSurvives proves a verified answer whose
// anchors are all unusable fails rather than committing a record that asserts
// claims the record does not ground.
func TestAskExecutorFailsWhenNoEvidenceSurvives(t *testing.T) {
	envelope := verifiedAskEnvelope(time.Now().UTC().Add(-time.Hour))
	envelope.Evidence[0].Visibility = nil
	executor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
		Provider: &stubAskProvider{envelope: envelope},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: webapi.AskCapability, Action: webapi.AskAction,
		Input: json.RawMessage(`{"question":"anything"}`),
	})
	if err == nil {
		t.Fatal("an answer with no usable evidence must fail the action")
	}
	if result.ErrorCode != webapi.AskErrorUngroundedClaims {
		t.Fatalf("error code = %q", result.ErrorCode)
	}
}

// TestAskExecutorSkipsMalformedEvidenceWithoutFailingBatch proves one bad
// anchor is dropped rather than discarding a verified answer. A citation
// without a revision cannot validate, and a graph anchor carrying an assertion
// for an edge it does not reference must shed that assertion rather than
// producing a reference the dispatch service rejects wholesale.
func TestAskExecutorSkipsMalformedEvidenceWithoutFailingBatch(t *testing.T) {
	now := time.Now().UTC()
	envelope := verifiedAskEnvelope(now.Add(-time.Hour))
	unrevisioned := document.Citation{DocumentID: "other", SectionID: "section"}
	envelope.Evidence = append(envelope.Evidence,
		webapi.CitationEvidence{
			AnchorID: "malformed", Citation: &unrevisioned,
			Visibility: []string{"public"},
		},
		webapi.CitationEvidence{
			AnchorID: "graph",
			Path: &graph.Path{
				Nodes: []graph.Node{{ID: "node"}},
			},
			Assertions: []webapi.CitationAssertion{{
				AssertionID: "assertion", EdgeID: "unreferenced",
				Origin: ontology.AssertionExplicit,
			}},
			Visibility: []string{"public"},
		},
	)
	executor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
		Provider: &stubAskProvider{envelope: envelope},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: webapi.AskCapability, Action: webapi.AskAction,
		Input: json.RawMessage(`{"question":"anything"}`),
	})
	if err != nil {
		t.Fatalf("one malformed anchor discarded a verified answer: %v", err)
	}
	if len(result.Evidence) != 2 {
		t.Fatalf("evidence = %#v", result.Evidence)
	}
	for _, evidence := range result.Evidence {
		if evidence.AnchorID == "malformed" {
			t.Fatal("an unvalidatable citation must be skipped")
		}
		if evidence.AnchorID == "graph" && len(evidence.Assertions) != 0 {
			t.Fatalf("an unreferenced assertion survived: %#v", evidence)
		}
		reference := interaction.EvidenceReference{
			AnchorID: evidence.AnchorID, Kind: evidence.Kind,
			Citation: evidence.Citation, NodeIDs: evidence.NodeIDs,
			EdgeIDs: evidence.EdgeIDs, Assertions: evidence.Assertions,
		}
		if _, err := reference.Canonical(); err != nil {
			t.Fatalf("surviving evidence is not canonical: %v", err)
		}
	}
}

// TestAskExecutorRejectsFutureSnapshot proves a snapshot pinned ahead of the
// execution clock fails with a code that names the cause, rather than being
// rewritten to fit or rejected generically by the dispatch service.
func TestAskExecutorRejectsFutureSnapshot(t *testing.T) {
	now := time.Now().UTC()
	executor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
		Provider: &stubAskProvider{envelope: verifiedAskEnvelope(now.Add(time.Hour))},
		Clock:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: webapi.AskCapability, Action: webapi.AskAction,
		Input: json.RawMessage(`{"question":"anything"}`),
	})
	if err == nil || result.ErrorCode != webapi.AskErrorEvidenceSkew {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
}

// TestAskExecutorOutputCarriesNoDocumentContent proves the action receipt
// holds no document-derived content. TeamActions serializes Output to any
// reader authorized on the action's scope without checking visibility labels,
// so claim values, issue reasons, and ontology identifiers must not appear.
func TestAskExecutorOutputCarriesNoDocumentContent(t *testing.T) {
	envelope := verifiedAskEnvelope(time.Now().UTC().Add(-time.Hour))
	// The effective output label expression is itself derived from the
	// retrieved sources, so it must not reach the receipt either.
	envelope.OutputVisibility = "secret&project-x"
	envelope.Issues = []webapi.CitationIssue{{
		Kind: "unsupported", Reason: "restricted phrasing from a source document",
	}}
	executor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
		Provider: &stubAskProvider{envelope: envelope},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: webapi.AskCapability, Action: webapi.AskAction,
		Input: json.RawMessage(`{"question":"anything"}`),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if bytes.Contains(result.Output, []byte("restricted phrasing")) {
		t.Fatalf("issue text leaked into the action receipt: %s", result.Output)
	}
	if bytes.Contains(result.Output, []byte("project-x")) {
		t.Fatalf("source label expression leaked into the receipt: %s", result.Output)
	}
	var receipt map[string]any
	if err := json.Unmarshal(result.Output, &receipt); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"verification": true, "snapshot_id": true, "session_id": true,
		"evidence_count": true, "evidence_considered": true,
		"evidence_truncated": true, "claim_count": true, "issue_count": true,
	}
	for key := range receipt {
		if !allowed[key] {
			t.Fatalf("unexpected receipt field %q", key)
		}
	}
	var output webapi.AskExecutionOutput
	if err := json.Unmarshal(result.Output, &output); err != nil {
		t.Fatal(err)
	}
	if output.IssueCount != 1 {
		t.Fatalf("receipt = %#v", output)
	}
}

// TestAskExecutorReportsEvidenceTruncation proves a receipt distinguishes a
// complete grounding from a clipped one. The dispatch bound counts evidence
// members rather than anchors, so an evidence-rich verified answer truncates
// well before the reasoning harness anchor limit, and a reader who cannot see
// that would read the claim count as fully grounded.
func TestAskExecutorReportsEvidenceTruncation(t *testing.T) {
	now := time.Now().UTC()
	envelope := verifiedAskEnvelope(now.Add(-time.Hour))
	envelope.Evidence = nil
	// Each fully-cited document anchor costs three members against a bound of
	// 256, so 120 anchors cannot all be recorded.
	for index := 0; index < 120; index++ {
		suffix := shoal.ID(strconv.Itoa(index))
		citation := document.Citation{
			DocumentID: "document" + suffix, RevisionID: "revision" + suffix,
			SectionID: "section" + suffix, SpanID: "span" + suffix,
		}
		envelope.Evidence = append(envelope.Evidence, webapi.CitationEvidence{
			AnchorID: "anchor" + suffix, Citation: &citation,
			Visibility: []string{"public"},
		})
	}
	executor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
		Provider: &stubAskProvider{envelope: envelope},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: webapi.AskCapability, Action: webapi.AskAction,
		Input: json.RawMessage(`{"question":"anything"}`),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	var output webapi.AskExecutionOutput
	if err := json.Unmarshal(result.Output, &output); err != nil {
		t.Fatal(err)
	}
	if output.EvidenceConsidered != 120 {
		t.Fatalf("considered = %d", output.EvidenceConsidered)
	}
	if !output.EvidenceTruncated {
		t.Fatal("a clipped grounding must be reported as truncated")
	}
	if output.EvidenceCount != len(result.Evidence) ||
		output.EvidenceCount >= output.EvidenceConsidered {
		t.Fatalf("receipt = %#v, recorded = %d", output, len(result.Evidence))
	}
}

// TestAskExecutorUsesResolvedCitationSourceRoles proves the recorded evidence
// carries the same document, section, and span roles the verifier resolved,
// rather than a narrower set rebuilt from a document-granularity citation.
func TestAskExecutorUsesResolvedCitationSourceRoles(t *testing.T) {
	now := time.Now().UTC()
	envelope := verifiedAskEnvelope(now.Add(-time.Hour))
	envelope.Evidence[0].Citation = &document.Citation{
		DocumentID: "document", RevisionID: "revision", SectionID: "section",
	}
	envelope.Evidence[0].SourceIDs = []shoal.ID{"document", "section", "span"}
	executor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
		Provider: &stubAskProvider{envelope: envelope},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: webapi.AskCapability, Action: webapi.AskAction,
		Input: json.RawMessage(`{"question":"anything"}`),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(result.Evidence) != 1 || len(result.Evidence[0].NodeIDs) != 3 {
		t.Fatalf("evidence = %#v", result.Evidence)
	}
	for _, want := range []shoal.ID{"document", "section", "span"} {
		found := false
		for _, got := range result.Evidence[0].NodeIDs {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("resolved role %q missing from %#v",
				want, result.Evidence[0].NodeIDs)
		}
	}
}

// TestAskExecutorNamesMissingRetrieveGrant proves an agent principal granted
// only invoke and dispatch gets a code naming the missing grant, not a generic
// reasoning failure that looks like a model outage.
func TestAskExecutorNamesMissingRetrieveGrant(t *testing.T) {
	executor, err := webapi.NewAskExecutor(webapi.AskExecutorConfig{
		Provider: &stubAskProvider{
			err: shoal.NewError(shoal.ErrorUnauthorized, "retrieve is not permitted"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), fleet.Invocation{
		Capability: webapi.AskCapability, Action: webapi.AskAction,
		Input: json.RawMessage(`{"question":"anything"}`),
	})
	if err == nil || result.ErrorCode != webapi.AskErrorRetrieveUnauthorized {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
}

// TestBindRejectsTypedNilExecutor proves a typed nil cannot enter the registry.
// fleet.Executor is an empty interface, so a plain nil check would admit one,
// and it would panic mid-dispatch after the effect-admission record is written.
func TestBindRejectsTypedNilExecutor(t *testing.T) {
	registry := configuredFleetExecutors{"allowed": configuredFleetExecutor{
		reference: "allowed",
	}}
	var typedNil *webapi.AskExecutor
	if err := registry.bind("allowed", typedNil); err == nil {
		t.Fatal("a typed-nil executor must be refused")
	}
	bound, _ := registry.ResolveExecutor("allowed")
	if _, ok := bound.(fleet.ActionExecutor); ok {
		t.Fatal("a refused binding must not replace the placeholder")
	}
}
