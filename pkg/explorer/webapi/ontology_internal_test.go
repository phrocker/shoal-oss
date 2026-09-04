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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOntologyEndpointIsNotPubliclyReachable(t *testing.T) {
	handler, err := NewHandler(allowlistStubService{}, "127.0.0.1:8080")
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/ontology", nil)
	if handler.publiclyReachable(request) {
		t.Fatal("GET /api/v1/ontology is public, want authenticated API route")
	}
}

type allowlistStubService struct{}

func (allowlistStubService) Documents(context.Context, DocumentsRequest) (DocumentsResponse, error) {
	return DocumentsResponse{}, nil
}

func (allowlistStubService) Document(context.Context, DocumentRequest) (DocumentResponse, error) {
	return DocumentResponse{}, nil
}

func (allowlistStubService) Retrieve(context.Context, RetrievalRequest) (RetrievalResponse, error) {
	return RetrievalResponse{}, nil
}

func (allowlistStubService) Neighborhood(
	context.Context,
	NeighborhoodRequest,
) (NeighborhoodResponse, error) {
	return NeighborhoodResponse{}, nil
}

func (allowlistStubService) Path(context.Context, PathRequest) (PathResponse, error) {
	return PathResponse{}, nil
}

var _ Service = allowlistStubService{}
