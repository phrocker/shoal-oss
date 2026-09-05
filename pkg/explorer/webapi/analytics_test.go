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

package webapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/analytics"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestAnalyticsRouteUsesHostAndAuthenticationGuards(t *testing.T) {
	now := time.Date(2026, time.September, 5, 18, 0, 0, 0, time.UTC)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "reader", Actor: "reader-actor",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations:   []auth.Operation{auth.OperationAnalyticsRead},
		PermittedSourceIDs:  [][]byte{[]byte("source")},
		PermittedPolicyIDs:  [][]byte{[]byte("policy")},
		PolicyGeneration:    1, AuthenticationExpires: now.Add(time.Hour),
		RequestID: "request",
	})
	if err != nil {
		t.Fatal(err)
	}
	service := &analyticsRouteService{resolver: authority.Resolver()}
	handler, err := webapi.NewAuthenticatedHandler(
		service,
		webapi.AuthenticatorFunc(func(*http.Request) (auth.Decision, error) {
			return decision, nil
		}),
		authority.Binder(),
		"workspace.test",
	)
	if err != nil {
		t.Fatal(err)
	}
	request := webapi.AnalyticsRequest{
		Scope: analytics.Scope{
			NodeIDs: []shoal.ID{"node"},
			Depth:   1, Direction: explorer.GraphDirectionBoth,
			Fanout: 4, MaxNodes: 8, MaxEdges: 16,
			MaxScannedEdgesPerNode: 32,
		},
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	httpRequest := httptest.NewRequest(
		http.MethodPost, "http://workspace.test/api/v1/analytics",
		bytes.NewReader(body),
	)
	httpRequest.Host = "workspace.test"
	httpRequest.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httpRequest)
	if response.Code != http.StatusOK || service.calls != 1 {
		t.Fatalf("analytics response = %d %s, calls=%d",
			response.Code, response.Body.String(), service.calls)
	}
	if !strings.Contains(response.Body.String(), `"recorded":false`) ||
		strings.Contains(response.Body.String(), `"Recorded"`) {
		t.Fatalf("analytics response schema = %s", response.Body.String())
	}

	badHost := httptest.NewRequest(
		http.MethodPost, "http://evil.test/api/v1/analytics",
		bytes.NewReader(body),
	)
	badHost.Host = "evil.test"
	badHost.Header.Set("Content-Type", "application/json")
	badHostResponse := httptest.NewRecorder()
	handler.ServeHTTP(badHostResponse, badHost)
	if badHostResponse.Code != http.StatusMisdirectedRequest || service.calls != 1 {
		t.Fatalf("host guard response = %d, calls=%d",
			badHostResponse.Code, service.calls)
	}

	metaRequest := httptest.NewRequest(
		http.MethodGet, "http://workspace.test/api/v1/meta", nil)
	metaRequest.Host = "workspace.test"
	metaResponse := httptest.NewRecorder()
	handler.ServeHTTP(metaResponse, metaRequest)
	if metaResponse.Code != http.StatusOK ||
		!strings.Contains(metaResponse.Body.String(), `"analytics":true`) ||
		!strings.Contains(metaResponse.Body.String(), `"analytics_limits"`) {
		t.Fatalf("metadata = %d %s", metaResponse.Code, metaResponse.Body.String())
	}
}

func TestAnalyticsWireRoundTripsOpaqueIDs(t *testing.T) {
	request := webapi.AnalyticsRequest{
		Snapshot: webapi.AnalyticsSnapshot{ID: "analytics-sha256:pin"},
		Scope: analytics.Scope{
			NodeIDs: []shoal.ID{shoal.ID([]byte{0xff, 0x00})},
			Depth:   1, Direction: explorer.GraphDirectionIncoming,
			Fanout: 2, MaxNodes: 3, MaxEdges: 4,
			MaxScannedEdgesPerNode: 5,
			EdgeTypes:              []string{"related"},
		},
		PageRank: analytics.PageRankOptions{
			DampingFactor: 0.8, ConvergenceTolerance: 1e-9,
			MaxIterations: 50,
		},
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte{0xff}) {
		t.Fatalf("opaque ID was emitted without encoding: %q", encoded)
	}
	var decoded webapi.AnalyticsRequest
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, request) {
		t.Fatalf("round trip:\n got %#v\nwant %#v", decoded, request)
	}
}

type analyticsRouteService struct {
	gateStubService
	resolver auth.Resolver
	calls    int
}

func (s *analyticsRouteService) AnalyticsLimits() (analytics.Limits, bool) {
	return analytics.DefaultLimits(), true
}

func (s *analyticsRouteService) Analytics(
	ctx context.Context,
	request webapi.AnalyticsRequest,
) (webapi.AnalyticsResponse, error) {
	decision, err := s.resolver.Resolve(ctx)
	if err != nil {
		return webapi.AnalyticsResponse{}, err
	}
	if err := decision.Authorize(
		auth.OperationAnalyticsRead,
		auth.ResourceRequest{AuthorizationDomain: decision.AuthorizationDomain()},
		time.Date(2026, time.September, 5, 18, 0, 0, 0, time.UTC),
	); err != nil {
		return webapi.AnalyticsResponse{}, err
	}
	s.calls++
	return webapi.AnalyticsResponse{
		Snapshot: webapi.AnalyticsSnapshot{ID: "analytics-sha256:test"},
		Analytics: analytics.Result{
			Scope: analytics.ScopeMetadata{
				SnapshotID:               "analytics-sha256:test",
				AuthorizationFingerprint: "auth-sha256:test",
				PolicyGeneration:         1,
				SeedNodeIDs:              []shoal.ID{"node"},
				Depth:                    request.Scope.Depth, Direction: request.Scope.Direction,
				Fanout: request.Scope.Fanout, MaxNodes: request.Scope.MaxNodes,
				MaxEdges:               request.Scope.MaxEdges,
				MaxScannedEdgesPerNode: request.Scope.MaxScannedEdgesPerNode,
				Complete:               true,
			},
			PageRank: analytics.PageRankSummary{
				DampingFactor:        analytics.DefaultDampingFactor,
				ConvergenceTolerance: analytics.DefaultConvergenceTolerance,
				MaxIterations:        analytics.DefaultMaxIterations, Converged: true,
			},
		},
	}, nil
}
