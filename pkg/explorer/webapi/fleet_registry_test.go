// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package webapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestFleetRegistryMountRequiresAuthenticationAndUsesBoundDecision(t *testing.T) {
	base := &stubWorkspaceService{}
	anonymous, err := NewHandler(base, "example.test")
	if err != nil {
		t.Fatal(err)
	}
	if err := anonymous.MountFleetRegistry(&stubFleetProvider{}); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("anonymous mount = %v", err)
	}

	now := time.Date(2026, 9, 5, 20, 0, 0, 0, time.UTC)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "subject", Actor: "actor", AuthorizationDomain: []byte("domain"),
		AllowedOperations:  []auth.Operation{auth.OperationAgentResolve},
		PermittedSourceIDs: [][]byte{[]byte("source")},
		PermittedPolicyIDs: [][]byte{[]byte("policy")},
		PolicyGeneration:   1, AuthenticationExpires: now.Add(time.Hour),
		RequestID: "request",
	})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewAuthenticatedHandler(base,
		AuthenticatorFunc(func(*http.Request) (auth.Decision, error) { return decision, nil }),
		authority.Binder(), "example.test")
	if err != nil {
		t.Fatal(err)
	}
	provider := &stubFleetProvider{resolver: authority.Resolver()}
	if err := handler.MountFleetRegistry(provider); err != nil {
		t.Fatal(err)
	}
	if err := handler.MountFleetRegistry(provider); !shoal.IsErrorCode(
		err, shoal.ErrorInvalidArgument,
	) {
		t.Fatalf("duplicate fleet mount = %v", err)
	}
	body, _ := json.Marshal(fleetRequestContextWire{
		RequestID: encodeFleetID("request"), ReasonCode: "resolve",
		Deadline: now.Add(time.Minute),
	})
	request := httptest.NewRequest(http.MethodPost,
		"http://example.test/api/v1/fleet/agents/"+base64.RawURLEncoding.EncodeToString([]byte("agent"))+"/resolve",
		bytes.NewReader(body))
	request.Host = "example.test"
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if !provider.resolved || strings.Contains(response.Body.String(), "executor-secret") {
		t.Fatalf("provider=%v body=%s", provider.resolved, response.Body.String())
	}

	listBody, _ := json.Marshal(fleetListWire{
		Context: fleetRequestContextWire{
			RequestID: encodeFleetID("request"), ReasonCode: "resolve",
			Deadline: now.Add(time.Minute),
		},
		Limit: 7, Cursor: "opaque-cursor",
	})
	listRequest := httptest.NewRequest(
		http.MethodPost, "http://example.test/api/v1/fleet/agents/resolve",
		bytes.NewReader(listBody),
	)
	listRequest.Host = "example.test"
	listRequest.Header.Set("Content-Type", "application/json")
	listResponse := httptest.NewRecorder()
	handler.ServeHTTP(listResponse, listRequest)
	if listResponse.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s",
			listResponse.Code, listResponse.Body.String())
	}
	if provider.listRequest.Limit != 7 ||
		provider.listRequest.Cursor != "opaque-cursor" ||
		!strings.Contains(listResponse.Body.String(), `"next_cursor":"next"`) {
		t.Fatalf("list request=%+v body=%s",
			provider.listRequest, listResponse.Body.String())
	}
}

type stubFleetProvider struct {
	resolver    auth.Resolver
	resolved    bool
	listRequest fleet.ListRequest
}

type stubWorkspaceService struct{}

func (*stubWorkspaceService) Documents(context.Context, DocumentsRequest) (DocumentsResponse, error) {
	return DocumentsResponse{}, nil
}
func (*stubWorkspaceService) Document(context.Context, DocumentRequest) (DocumentResponse, error) {
	return DocumentResponse{}, nil
}
func (*stubWorkspaceService) Retrieve(context.Context, RetrievalRequest) (RetrievalResponse, error) {
	return RetrievalResponse{}, nil
}
func (*stubWorkspaceService) Neighborhood(context.Context, NeighborhoodRequest) (NeighborhoodResponse, error) {
	return NeighborhoodResponse{}, nil
}
func (*stubWorkspaceService) Path(context.Context, PathRequest) (PathResponse, error) {
	return PathResponse{}, nil
}

func (*stubFleetProvider) Register(context.Context, fleet.RegisterRequest) (fleet.Descriptor, error) {
	return fleet.Descriptor{}, nil
}
func (*stubFleetProvider) Heartbeat(context.Context, fleet.HeartbeatRequest) (fleet.Descriptor, error) {
	return fleet.Descriptor{}, nil
}
func (*stubFleetProvider) Revoke(context.Context, fleet.RevokeRequest) (fleet.Descriptor, error) {
	return fleet.Descriptor{}, nil
}
func (p *stubFleetProvider) Resolve(ctx context.Context, request fleet.ResolveRequest) (fleet.Resolved, error) {
	if _, err := p.resolver.Resolve(ctx); err != nil {
		return fleet.Resolved{}, err
	}
	p.resolved = true
	return fleet.Resolved{
		Descriptor: fleet.Descriptor{
			ID: request.ID, Generation: 1, Subject: "subject", Actor: "actor",
			AuthorizationDomain: []byte("domain"),
			Scopes:              []fleet.Scope{{SourceID: []byte("source"), PolicyID: []byte("policy")}},
			ExecutorRef:         "opaque-ref", LeaseExpiresAt: time.Now().Add(time.Hour), UpdatedAt: time.Now(),
		},
		Executor: "executor-secret",
	}, nil
}
func (p *stubFleetProvider) List(
	_ context.Context, request fleet.ListRequest,
) (fleet.ListPage, error) {
	p.listRequest = request
	return fleet.ListPage{NextCursor: "next"}, nil
}
