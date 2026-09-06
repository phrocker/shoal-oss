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
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

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
