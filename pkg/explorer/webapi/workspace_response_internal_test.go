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
