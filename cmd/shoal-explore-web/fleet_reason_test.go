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
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/explorerfleet"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestHostedATPLRegistrationRecordsAssertedPolicyDigestDurably drives the
// registry HTTP route against the lifecycle recorder exactly as openService
// composes it (authorized client over the embedded corpus, not a stub sink),
// then restarts and reads the receipt back from disk.
func TestHostedATPLRegistrationRecordsAssertedPolicyDigestDurably(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC().Add(time.Minute)
	authority := auth.NewAuthority()
	config := serviceConfig{
		backend: "embedded", data: filepath.Join(root, "corpus"),
		policyDir: filepath.Join(root, "policy"),
		resolver:  authority.Resolver(), clock: func() time.Time { return now },
		executors: configuredFleetExecutors{
			"local": configuredFleetExecutor{reference: "local"},
		},
	}
	const (
		agentID         = shoal.ID("policy-agent")
		auditPurpose    = "apply reviewed fleet policy"
		goodRequestID   = shoal.ID("atpl-apply-request")
		malformedReqID  = shoal.ID("atpl-malformed-request")
		malformedDetail = "atpl:policy:v1:NOT-A-DIGEST"
	)
	policyDigest := "atpl:policy:v1:" + strings.Repeat("3c", 32)

	opened, err := openService(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := webapi.NewFleetHandler(
		opened.fleetRegistry, opened.fleetDispatch)
	if err != nil {
		opened.close()
		t.Fatal(err)
	}
	register := func(requestID shoal.ID, detail string) *httptest.ResponseRecorder {
		t.Helper()
		decision, err := auth.NewDecision(auth.DecisionConfig{
			Subject: "owner", Actor: "operator", ClientID: "shoalctl",
			AuthorizationDomain:   workspaceAuthorizationDomain,
			AllowedOperations:     []auth.Operation{auth.OperationAgentRegister},
			PermittedSourceIDs:    [][]byte{workspaceSourceID},
			PermittedPolicyIDs:    [][]byte{workspaceGrantPolicyID},
			PolicyGeneration:      workspacePolicyGeneration,
			AuthenticationExpires: now.Add(time.Hour),
			AuditPurpose:          auditPurpose,
			RequestID:             requestID,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, err := authority.Binder().Bind(context.Background(), decision)
		if err != nil {
			t.Fatal(err)
		}
		body, err := json.Marshal(map[string]any{
			"context": map[string]any{
				"request_id":    encodeTestID(requestID),
				"reason_code":   fleet.ReasonCodeATPLApply,
				"reason_detail": detail,
				"deadline":      now.Add(time.Minute),
			},
			"registration_key":    encodeTestID("atpl-key"),
			"expected_generation": 0,
			"descriptor": map[string]any{
				"id":                   encodeTestID(agentID),
				"authorization_domain": workspaceAuthorizationDomain,
				"scopes": []fleet.Scope{{
					SourceID: workspaceSourceID, PolicyID: workspaceGrantPolicyID,
				}},
				"executor_ref": "local",
				"capabilities": []fleet.Capability{{
					Name: "documents",
					Actions: []fleet.Action{{
						Name:         "list",
						InputSchema:  json.RawMessage(`{"type":"object"}`),
						OutputSchema: json.RawMessage(`{"type":"object"}`),
					}},
				}},
				"lease_expires_at": now.Add(10 * time.Minute),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(
			http.MethodPost, "/api/v1/fleet/agents", bytes.NewReader(body),
		).WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}

	refused := register(malformedReqID, malformedDetail)
	if refused.Code != http.StatusBadRequest {
		opened.close()
		t.Fatalf("malformed digest status = %d: %s",
			refused.Code, refused.Body.String())
	}
	accepted := register(goodRequestID, policyDigest)
	if accepted.Code != http.StatusCreated {
		opened.close()
		t.Fatalf("policy registration status = %d: %s",
			accepted.Code, accepted.Body.String())
	}
	opened.close()

	// Restart the hosted service on the same state; the registration and its
	// receipt must both survive.
	reopened, err := openService(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	resolveDecision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "owner", Actor: "operator",
		AuthorizationDomain:   workspaceAuthorizationDomain,
		AllowedOperations:     []auth.Operation{auth.OperationAgentResolve},
		PermittedSourceIDs:    [][]byte{workspaceSourceID},
		PermittedPolicyIDs:    [][]byte{workspaceGrantPolicyID},
		PolicyGeneration:      workspacePolicyGeneration,
		AuthenticationExpires: now.Add(time.Hour),
		RequestID:             "atpl-resolve-request",
	})
	if err != nil {
		reopened.close()
		t.Fatal(err)
	}
	resolveContext, err := authority.Binder().Bind(
		context.Background(), resolveDecision)
	if err != nil {
		reopened.close()
		t.Fatal(err)
	}
	resolved, err := reopened.fleetRegistry.Resolve(resolveContext,
		fleet.ResolveRequest{
			Context: fleet.RequestContext{
				RequestID: "atpl-resolve-request", ReasonCode: "atpl-read",
				Deadline: now.Add(time.Minute),
			},
			ID: agentID,
		})
	reopened.close()
	if err != nil || resolved.Descriptor.Generation != 1 {
		t.Fatalf("restarted resolve = %#v, %v", resolved.Descriptor, err)
	}

	corpus, err := explorer.Open(filepath.Join(root, "corpus"))
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	record, err := corpus.InteractionRecord(context.Background(),
		explorerfleet.LifecycleReceiptID(
			auth.OperationAgentRegister, goodRequestID, agentID))
	if err != nil {
		t.Fatalf("durable receipt = %v", err)
	}
	receipt := record.Session
	if receipt.CallerAssertedReason != (interaction.CallerAssertedReason{
		Code: fleet.ReasonCodeATPLApply, Source: policyDigest,
	}) {
		t.Fatalf("caller-asserted reason = %#v", receipt.CallerAssertedReason)
	}
	// Attributed to the authenticated caller, beside an unchanged trusted
	// audit purpose.
	wantPurpose, err := interaction.NewReason("audit_purpose", auditPurpose)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Actor.SubjectID != "owner" || receipt.Actor.ActorID != "operator" ||
		receipt.Actor.ClientID != "shoalctl" || receipt.Reason != wantPurpose ||
		receipt.ResultID != agentID ||
		receipt.AuthorizationOperation != string(auth.OperationAgentRegister) {
		t.Fatalf("durable receipt = %#v", receipt)
	}

	// The refused request wrote nothing: no receipt under its request, and
	// only the accepted registration and the restart's resolve are recorded.
	if _, err := corpus.InteractionRecord(context.Background(),
		explorerfleet.LifecycleReceiptID(
			auth.OperationAgentRegister, malformedReqID, agentID),
	); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("malformed request receipt lookup = %v", err)
	}
	summaries, err := corpus.Interactions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 2 {
		t.Fatalf("lifecycle receipts = %#v", summaries)
	}
	for _, summary := range summaries {
		session, err := corpus.Interaction(
			context.Background(), summary.SessionID)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(session.CallerAssertedReason.Source, "NOT-A-DIGEST") {
			t.Fatalf("malformed digest persisted: %#v", session)
		}
	}
}

func encodeTestID(id shoal.ID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}
