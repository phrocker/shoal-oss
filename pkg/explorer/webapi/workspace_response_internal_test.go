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
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	exploreranalytics "github.com/phrocker/shoal-oss/pkg/explorer/analytics"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/workspace"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestWorkspaceSettingsClampAnalyticsAndMarkResponseLoss(t *testing.T) {
	request := AnalyticsRequest{
		Scope: exploreranalytics.Scope{
			Depth: 9, Fanout: 10, MaxNodes: 100,
		},
	}
	applyAnalyticsWorkspaceLimits(&request, workspace.Limits{
		GraphDepth: 3, GraphFanout: 4, GraphNodes: 25,
	})
	if request.Scope.Depth != 3 ||
		request.Scope.Fanout != 4 ||
		request.Scope.MaxNodes != 25 {
		t.Fatalf("analytics workspace limits = %#v", request.Scope)
	}
	for _, test := range []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/api/v1/analytics"},
		{http.MethodPost, "/api/v1/fleet/agents"},
		{http.MethodPost, "/api/v1/fleet/agents/agent/heartbeat"},
		{http.MethodPost, "/api/v1/fleet/agents/agent/revoke"},
		{http.MethodPost, "/api/v1/fleet/actions"},
		{http.MethodPost, "/api/v1/fleet/actions/invoke"},
		{http.MethodPost, "/api/v1/fleet/actions/action/claim"},
		{http.MethodPost, "/api/v1/fleet/actions/action/cancel"},
		{http.MethodPost, "/api/v1/fleet/events/subscriptions"},
		{http.MethodDelete, "/api/v1/fleet/events/subscriptions/subscription"},
		{http.MethodPost, "/api/v1/fleet/events/publish"},
	} {
		if !requestMayCommit(test.method, test.path) {
			t.Fatalf("%s %s response loss must be indeterminate",
				test.method, test.path)
		}
	}
	for _, test := range []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/api/v1/fleet/actions/pull"},
		{http.MethodPost, "/api/v1/fleet/actions/action/status"},
		{http.MethodPost, "/api/v1/fleet/agents/resolve"},
		{http.MethodPost, "/api/v1/fleet/agents/agent/resolve"},
	} {
		if requestMayCommit(test.method, test.path) {
			t.Fatalf("%s %s is read-only", test.method, test.path)
		}
	}
}

func TestWorkspaceOperationForRequestUsesRouteOperation(t *testing.T) {
	for _, test := range []struct {
		method    string
		path      string
		operation auth.Operation
		apply     bool
	}{
		{http.MethodPost, "/api/v1/retrieve", auth.OperationRetrieve, true},
		{http.MethodPost, "/api/v1/analytics", auth.OperationAnalyticsRead, true},
		{http.MethodPost, "/api/v1/neighborhood", auth.OperationNeighborhood, true},
		{http.MethodPost, "/api/v1/ingest", auth.OperationIngest, true},
		{http.MethodPost, "/api/v1/documents", auth.OperationList, true},
		{http.MethodPost, "/api/v1/document", auth.OperationRead, true},
		{http.MethodPost, "/api/v1/fleet/actions/invoke", auth.OperationInvoke, true},
		{http.MethodPost, "/api/v1/fleet/actions/action/cancel", auth.OperationDispatch, true},
		{http.MethodPost, "/api/v1/fleet/agents/agent/heartbeat", auth.OperationAgentHeartbeat, true},
		{http.MethodPost, "/api/v1/fleet/events/publish", auth.OperationEventPublish, true},
		{http.MethodGet, "/api/v1/identity", auth.OperationRead, true},
		{http.MethodPost, "/mcp", "", false},
	} {
		operation, apply := workspaceOperationForRequest(test.method, test.path)
		if operation != test.operation || apply != test.apply {
			t.Fatalf("%s %s = %q, %v; want %q, %v",
				test.method, test.path, operation, apply,
				test.operation, test.apply)
		}
	}
}

func TestWorkspaceIDFromHeaderRequiresCanonicalSingleton(t *testing.T) {
	for _, test := range []struct {
		name    string
		values  []string
		want    shoal.ID
		present bool
		wantErr bool
	}{
		{name: "absent"},
		{name: "canonical", values: []string{"YQ"}, want: "a", present: true},
		{name: "noncanonical trailing bits", values: []string{"YR"}, wantErr: true},
		{name: "padded", values: []string{"YQ=="}, wantErr: true},
		{name: "whitespace", values: []string{" YQ"}, wantErr: true},
		{name: "duplicate", values: []string{"YQ", "YQ"}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			header := make(http.Header)
			for _, value := range test.values {
				header.Add(WorkspaceIDHeader, value)
			}
			got, present, err := workspaceIDFromHeader(header)
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, test.wantErr)
			}
			if got != test.want || present != test.present {
				t.Fatalf(
					"workspace = %q, present = %v; want %q, %v",
					got, present, test.want, test.present,
				)
			}
		})
	}
}

func TestPublicIngestErrorPreservesIndeterminateCommit(t *testing.T) {
	original := explorer.MarkIndeterminateCommit(
		shoal.NewError(shoal.ErrorUnavailable, "private storage detail"))
	public := publicIngestError(original)
	if !explorer.IsIndeterminateCommit(public) ||
		!shoal.IsErrorCode(public, shoal.ErrorUnavailable) ||
		strings.Contains(public.Error(), "private storage detail") {
		t.Fatalf("public ingest error = %v", public)
	}
}

func TestDecodeRemoteErrorPreservesIndeterminateCommit(t *testing.T) {
	for _, test := range []struct {
		name   string
		header bool
		body   bool
	}{
		{name: "header", header: true},
		{name: "body", body: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload, err := json.Marshal(struct {
				Code          shoal.ErrorCode `json:"code"`
				Message       string          `json:"message"`
				Indeterminate bool            `json:"indeterminate,omitempty"`
			}{
				Code:          shoal.ErrorUnavailable,
				Message:       "unavailable: workspace outcome unknown",
				Indeterminate: test.body,
			})
			if err != nil {
				t.Fatal(err)
			}
			response := &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(payload)),
			}
			if test.header {
				response.Header.Set(
					CommitOutcomeHeader, CommitOutcomeIndeterminate)
			}
			decoded := decodeRemoteError(response)
			if !explorer.IsIndeterminateCommit(decoded) ||
				!shoal.IsErrorCode(decoded, shoal.ErrorUnavailable) {
				t.Fatalf("decoded remote error = %v", decoded)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	return f(request)
}

func TestRemoteIngestMarksUnknownPostDispatchOutcomes(t *testing.T) {
	for _, test := range []struct {
		name   string
		ingest func() (*http.Response, error)
	}{
		{
			name: "transport",
			ingest: func() (*http.Response, error) {
				return nil, errors.New("response unavailable")
			},
		},
		{
			name: "malformed success",
			ingest: func() (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("{")),
				}, nil
			},
		},
		{
			name: "malformed failure",
			ingest: func() (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusBadGateway,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader("{")),
				}, nil
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata, err := json.Marshal(MetadataResponse{
				MaxPageSize:         MaxPageSize,
				MaxTopK:             MaxTopK,
				MaxDepth:            MaxDepth,
				MaxFanout:           MaxFanout,
				MaxNodes:            MaxNodes,
				MaxEdgeTypes:        MaxEdgeTypes,
				MaxResponseBytes:    MaxResponseBytes,
				MaxUploadFiles:      MaxUploadFiles,
				MaxUploadFileBytes:  MaxUploadFileBytes,
				MaxUploadTotalBytes: MaxUploadTotalBytes,
				Capabilities:        AllCapabilities(),
			})
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			client := &http.Client{Transport: roundTripFunc(func(
				request *http.Request,
			) (*http.Response, error) {
				calls++
				if calls == 1 {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       io.NopCloser(bytes.NewReader(metadata)),
					}, nil
				}
				return test.ingest()
			})}
			remote, err := NewRemoteService("http://remote.example", client)
			if err != nil {
				t.Fatal(err)
			}
			_, err = remote.Ingest(context.Background(), IngestRequest{
				Files: []UploadFile{{
					Name: "note.txt", Content: []byte("hello"),
				}},
			})
			if !explorer.IsIndeterminateCommit(err) {
				t.Fatalf("remote ingest error = %v", err)
			}
		})
	}
}

func TestRemoteIngestKeepsVerifiedConflictDeterminate(t *testing.T) {
	metadata, err := json.Marshal(MetadataResponse{
		MaxPageSize:         MaxPageSize,
		MaxTopK:             MaxTopK,
		MaxDepth:            MaxDepth,
		MaxFanout:           MaxFanout,
		MaxNodes:            MaxNodes,
		MaxEdgeTypes:        MaxEdgeTypes,
		MaxResponseBytes:    MaxResponseBytes,
		MaxUploadFiles:      MaxUploadFiles,
		MaxUploadFileBytes:  MaxUploadFileBytes,
		MaxUploadTotalBytes: MaxUploadTotalBytes,
		Capabilities:        AllCapabilities(),
	})
	if err != nil {
		t.Fatal(err)
	}
	conflict, err := json.Marshal(struct {
		Code    shoal.ErrorCode `json:"code"`
		Message string          `json:"message"`
	}{
		Code:    shoal.ErrorConflict,
		Message: "conflict: duplicate upload",
	})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(
		request *http.Request,
	) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(metadata)),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusConflict,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(conflict)),
		}, nil
	})}
	remote, err := NewRemoteService("http://remote.example", client)
	if err != nil {
		t.Fatal(err)
	}
	_, err = remote.Ingest(context.Background(), IngestRequest{
		Files: []UploadFile{{
			Name: "note.txt", Content: []byte("hello"),
		}},
	})
	if explorer.IsIndeterminateCommit(err) ||
		!shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("verified conflict error = %v", err)
	}
}

func TestMutationResponseOverflowIsExplicitlyIndeterminate(t *testing.T) {
	response := httptest.NewRecorder()
	writer := workspaceResponseWriter{
		ResponseWriter:          response,
		maxResponseBytes:        32,
		indeterminateOnOverflow: true,
	}
	writeResponse(writer, http.StatusOK, strings.Repeat("x", 1024))
	if response.Code != http.StatusServiceUnavailable ||
		response.Header().Get(CommitOutcomeHeader) !=
			CommitOutcomeIndeterminate ||
		response.Body.Len() > 32 {
		t.Fatalf("overflow status = %d, header = %q, bytes = %d",
			response.Code, response.Header().Get(CommitOutcomeHeader),
			response.Body.Len())
	}
}

func TestErrorResponseOverflowPreservesDeterministicStatus(t *testing.T) {
	response := httptest.NewRecorder()
	writer := workspaceResponseWriter{
		ResponseWriter:          response,
		maxResponseBytes:        8,
		indeterminateOnOverflow: true,
	}
	writeResponse(writer, http.StatusBadRequest, strings.Repeat("x", 1024))
	if response.Code != http.StatusBadRequest ||
		response.Header().Get(CommitOutcomeHeader) != "" ||
		response.Body.Len() != 0 {
		t.Fatalf("overflow status = %d, header = %q, bytes = %d",
			response.Code, response.Header().Get(CommitOutcomeHeader),
			response.Body.Len())
	}
}
