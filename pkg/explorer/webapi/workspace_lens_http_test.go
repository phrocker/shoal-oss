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
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/explorer/workspace"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type httpCallerOntologyChoices struct {
	bySubject map[shoal.ID][]workspace.OntologyChoice
}

type lensObservingService struct {
	settingsStubService
	effective    workspace.EffectiveDecision
	retrieval    webapi.RetrievalRequest
	neighborhood webapi.NeighborhoodRequest
	path         webapi.PathRequest
	found        bool
}

func (s *lensObservingService) Documents(
	ctx context.Context,
	_ webapi.DocumentsRequest,
) (webapi.DocumentsResponse, error) {
	s.effective, s.found = webapi.EffectiveWorkspaceSettings(ctx)
	return webapi.DocumentsResponse{}, nil
}

func (s *lensObservingService) Retrieve(
	_ context.Context,
	request webapi.RetrievalRequest,
) (webapi.RetrievalResponse, error) {
	s.retrieval = request
	return webapi.RetrievalResponse{}, nil
}

func (s *lensObservingService) Neighborhood(
	_ context.Context,
	request webapi.NeighborhoodRequest,
) (webapi.NeighborhoodResponse, error) {
	s.neighborhood = request
	return webapi.NeighborhoodResponse{}, nil
}

func (s *lensObservingService) Path(
	_ context.Context,
	request webapi.PathRequest,
) (webapi.PathResponse, error) {
	s.path = request
	return webapi.PathResponse{}, nil
}

func (c httpCallerOntologyChoices) ListOntologyChoices(
	ctx context.Context,
	decision auth.Decision,
) ([]workspace.OntologyChoice, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return append(
		[]workspace.OntologyChoice(nil),
		c.bySubject[decision.Subject()]...,
	), nil
}

func (c httpCallerOntologyChoices) AuthorizeOntology(
	ctx context.Context,
	decision auth.Decision,
	identity ontology.OntologyIdentity,
) error {
	choices, err := c.ListOntologyChoices(ctx, decision)
	if err != nil {
		return err
	}
	for _, choice := range choices {
		if choice.Identity == identity {
			return nil
		}
	}
	return shoal.NewError(shoal.ErrorUnauthorized, "authorization denied")
}

func TestHTTPSelectableLensIsPerCallerAndPreservesSettings(t *testing.T) {
	now := time.Date(2026, 9, 5, 18, 0, 0, 0, time.UTC)
	first, second := settingsHTTPOntologies(t, now)
	authority, _ := auth.NewAuthorityWithClock(func() time.Time { return now })
	store, err := workspace.OpenDurableStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	options := settingsProviderOptions(authority.Resolver(), now)
	options.OntologyChoices = httpCallerOntologyChoices{
		bySubject: map[shoal.ID][]workspace.OntologyChoice{
			"owner": {
				{Identity: first, Active: true},
				{Identity: second},
			},
			"other": {{Identity: second, Active: true}},
		},
	}
	provider, err := workspace.NewProvider(store, options)
	if err != nil {
		t.Fatal(err)
	}
	service := &lensObservingService{}
	handler, err := webapi.NewAuthenticatedHandler(
		service,
		webapi.AuthenticatorFunc(func(request *http.Request) (auth.Decision, error) {
			return settingsHTTPDecision(
				t, now, shoal.ID(request.Header.Get("X-Test-Subject"))), nil
		}),
		authority.Binder(), "example.test",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.SetWorkspaceSettingsProvider(provider); err != nil {
		t.Fatal(err)
	}
	workspacePath := base64.RawURLEncoding.EncodeToString([]byte("lens-http"))
	settingsPath := "/api/v1/workspaces/" + workspacePath + "/settings"
	topK, depth, fanout, graphNodes := uint32(6), uint32(2), uint32(3), uint32(4)
	sourceID := mustComponent(t, []byte("source-a"))
	policyID := mustComponent(t, []byte("policy-a"))
	response := settingsRequest(
		t, handler, http.MethodPut, settingsPath,
		map[string]any{
			"expected_revision": 0,
			"mutation_id": base64.RawURLEncoding.EncodeToString(
				[]byte("lens-settings-create")),
			"settings": map[string]any{
				"allowed_operations":   []string{"read"},
				"permitted_source_ids": []string{sourceID},
				"budgets": map[string]any{
					"retrieval_top_k": topK,
					"graph_depth":     depth,
					"graph_fanout":    fanout,
					"graph_nodes":     graphNodes,
				},
				"output_policies": []map[string]any{{
					"source_id":       sourceID,
					"grant_policy_id": policyID,
					"epoch":           int64(1),
				}},
			},
		},
		"owner", "http://example.test")
	if response.Code != http.StatusCreated {
		t.Fatalf("settings create status = %d, body = %s",
			response.Code, response.Body.String())
	}

	lensPath := settingsPath + "/lens"
	response = settingsRequest(
		t, handler, http.MethodGet, lensPath, nil, "owner", "")
	if response.Code != http.StatusOK {
		t.Fatalf("lens choices status = %d, body = %s",
			response.Code, response.Body.String())
	}
	var choices webapi.WorkspaceOntologyChoicesResponse
	if err := json.Unmarshal(response.Body.Bytes(), &choices); err != nil {
		t.Fatal(err)
	}
	if len(choices.Choices) != 2 || !choices.Choices[0].Active ||
		choices.SettingsRevision != 1 {
		t.Fatalf("owner lens choices = %#v", choices)
	}

	response = settingsRequest(
		t, handler, http.MethodGet, lensPath, nil, "other", "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("cross-caller lens status = %d, body = %s",
			response.Code, response.Body.String())
	}
	otherPath := "/api/v1/workspaces/" +
		base64.RawURLEncoding.EncodeToString([]byte("other-lens")) +
		"/settings/lens"
	response = settingsRequest(
		t, handler, http.MethodGet, otherPath, nil, "other", "")
	if response.Code != http.StatusOK {
		t.Fatalf("other choices status = %d, body = %s",
			response.Code, response.Body.String())
	}
	var otherChoices webapi.WorkspaceOntologyChoicesResponse
	if err := json.Unmarshal(response.Body.Bytes(), &otherChoices); err != nil {
		t.Fatal(err)
	}
	if len(otherChoices.Choices) != 1 ||
		otherChoices.Choices[0].SchemaID != encodeTestID(second.SchemaID()) ||
		otherChoices.Choices[0].VersionID != encodeTestID(second.VersionID()) {
		t.Fatalf("other caller choices leaked owner eligibility: %#v", otherChoices)
	}

	response = settingsRequest(
		t, handler, http.MethodPut, lensPath,
		map[string]any{
			"expected_revision": 1,
			"mutation_id": base64.RawURLEncoding.EncodeToString(
				[]byte("lens-select")),
			"selected_ontology": map[string]any{
				"schema_id":  encodeTestID(second.SchemaID()),
				"version_id": encodeTestID(second.VersionID()),
			},
		},
		"owner", "http://example.test")
	if response.Code != http.StatusOK {
		t.Fatalf("lens select status = %d, body = %s",
			response.Code, response.Body.String())
	}
	stale := settingsRequest(
		t, handler, http.MethodPut, lensPath,
		map[string]any{
			"expected_revision": 1,
			"mutation_id": base64.RawURLEncoding.EncodeToString(
				[]byte("lens-select-stale")),
			"selected_ontology": map[string]any{
				"schema_id":  encodeTestID(first.SchemaID()),
				"version_id": encodeTestID(first.VersionID()),
			},
		},
		"owner", "http://example.test")
	if stale.Code != http.StatusConflict {
		t.Fatalf("stale lens select status = %d, body = %s",
			stale.Code, stale.Body.String())
	}
	response = settingsRequest(
		t, handler, http.MethodGet, settingsPath, nil, "owner", "")
	if response.Code != http.StatusOK {
		t.Fatalf("settings after selection status = %d, body = %s",
			response.Code, response.Body.String())
	}
	var selected webapi.WorkspaceSettingsResponse
	if err := json.Unmarshal(response.Body.Bytes(), &selected); err != nil {
		t.Fatal(err)
	}
	if selected.Revision != 2 ||
		selected.Settings.AllowedOperations == nil ||
		len(*selected.Settings.AllowedOperations) != 1 ||
		(*selected.Settings.AllowedOperations)[0] != "read" ||
		selected.Settings.PermittedSourceIDs == nil ||
		len(*selected.Settings.PermittedSourceIDs) != 1 ||
		selected.Settings.Budgets.RetrievalTopK == nil ||
		*selected.Settings.Budgets.RetrievalTopK != topK ||
		selected.Settings.SelectedOntology == nil ||
		selected.Settings.SelectedOntology.VersionID !=
			encodeTestID(second.VersionID()) {
		t.Fatalf("selected lens did not preserve settings: %#v", selected)
	}

	identityResponse := settingsWorkspaceRequest(
		t, handler, http.MethodGet, "/api/v1/identity",
		nil, "owner", workspacePath)
	if identityResponse.Code != http.StatusOK {
		t.Fatalf("effective identity status = %d, body = %s",
			identityResponse.Code, identityResponse.Body.String())
	}
	var identity webapi.IdentityResponse
	if err := json.Unmarshal(identityResponse.Body.Bytes(), &identity); err != nil {
		t.Fatal(err)
	}
	if len(identity.Operations) != 1 || identity.Operations[0] != "read" ||
		identity.SelectedOntology == nil ||
		identity.SelectedOntology.VersionID != encodeTestID(second.VersionID()) ||
		identity.Subject != "owner" || identity.Actor != "actor" ||
		identity.RequestID != "request" {
		t.Fatalf("effective issuer decision = %#v", identity)
	}
	baseIdentityResponse := settingsRequest(
		t, handler, http.MethodGet, "/api/v1/identity", nil, "owner", "")
	if baseIdentityResponse.Code != http.StatusOK {
		t.Fatalf("base identity status = %d, body = %s",
			baseIdentityResponse.Code, baseIdentityResponse.Body.String())
	}
	var baseIdentity webapi.IdentityResponse
	if err := json.Unmarshal(baseIdentityResponse.Body.Bytes(), &baseIdentity); err != nil {
		t.Fatal(err)
	}
	if baseIdentity.SelectedOntology != nil ||
		len(baseIdentity.Operations) <= len(identity.Operations) {
		t.Fatalf("base identity was unexpectedly replaced: %#v", baseIdentity)
	}
	crossCaller := settingsWorkspaceRequest(
		t, handler, http.MethodGet, "/api/v1/identity",
		nil, "other", workspacePath)
	if crossCaller.Code != http.StatusNotFound ||
		crossCaller.Header().Get("WWW-Authenticate") != "" {
		t.Fatalf("cross-caller effective status = %d, challenge = %q, body = %s",
			crossCaller.Code, crossCaller.Header().Get("WWW-Authenticate"),
			crossCaller.Body.String())
	}
	documents := settingsWorkspaceRequest(
		t, handler, http.MethodPost, "/api/v1/documents",
		map[string]any{}, "owner", workspacePath)
	if documents.Code != http.StatusOK {
		t.Fatalf("effective documents status = %d, body = %s",
			documents.Code, documents.Body.String())
	}
	if !service.found ||
		service.effective.Revision() != selected.Revision ||
		service.effective.Limits().RetrievalTopK != topK ||
		service.effective.Limits().GraphDepth != depth ||
		service.effective.Limits().GraphFanout != fanout ||
		service.effective.Limits().GraphNodes != graphNodes ||
		len(service.effective.OutputPolicies()) != 1 ||
		service.effective.SettingsID() == "" {
		t.Fatalf("mounted transport effective settings = found %v, %#v",
			service.found, service.effective)
	}

	retrieve := settingsWorkspaceRequest(
		t, handler, http.MethodPost, "/api/v1/retrieve",
		webapi.RetrievalRequest{
			Query: retrieval.Request{Text: "bounded", TopK: 50},
		},
		"owner", workspacePath)
	if retrieve.Code != http.StatusOK || service.retrieval.Query.TopK != topK {
		t.Fatalf("retrieval budget = %d, status = %d, body = %s",
			service.retrieval.Query.TopK, retrieve.Code, retrieve.Body.String())
	}
	neighborhood := settingsWorkspaceRequest(
		t, handler, http.MethodPost, "/api/v1/neighborhood",
		webapi.NeighborhoodRequest{
			NodeIDs: []shoal.ID{"node"}, Depth: 4,
			Fanout: 50, MaxNodes: 250,
		},
		"owner", workspacePath)
	if neighborhood.Code != http.StatusOK ||
		service.neighborhood.Depth != depth ||
		service.neighborhood.Fanout != fanout ||
		service.neighborhood.MaxNodes != graphNodes {
		t.Fatalf("neighborhood budget = %#v, status = %d, body = %s",
			service.neighborhood, neighborhood.Code, neighborhood.Body.String())
	}
	pathResponse := settingsWorkspaceRequest(
		t, handler, http.MethodPost, "/api/v1/path",
		webapi.PathRequest{
			From: "from", To: "to", MaxDepth: 4, Fanout: 50,
			MaxNodes: 250,
		},
		"owner", workspacePath)
	if pathResponse.Code != http.StatusOK ||
		service.path.MaxDepth != depth ||
		service.path.Fanout != fanout ||
		service.path.MaxNodes != graphNodes {
		t.Fatalf("path budget = %#v, status = %d, body = %s",
			service.path, pathResponse.Code, pathResponse.Body.String())
	}
	metadataResponse := settingsWorkspaceRequest(
		t, handler, http.MethodGet, "/api/v1/meta",
		nil, "owner", workspacePath)
	if metadataResponse.Code != http.StatusOK {
		t.Fatalf("metadata status = %d, body = %s",
			metadataResponse.Code, metadataResponse.Body.String())
	}
	var metadata webapi.MetadataResponse
	if err := json.Unmarshal(metadataResponse.Body.Bytes(), &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.MaxTopK != topK || metadata.MaxDepth != depth ||
		metadata.MaxFanout != fanout || metadata.MaxNodes != graphNodes {
		t.Fatalf("effective metadata limits = %#v", metadata)
	}

	limitedWorkspace := base64.RawURLEncoding.EncodeToString(
		[]byte("limited-output"))
	limitedSettingsPath := "/api/v1/workspaces/" +
		limitedWorkspace + "/settings"
	outputBytes := uint64(64)
	response = settingsRequest(
		t, handler, http.MethodPut, limitedSettingsPath,
		map[string]any{
			"expected_revision": 0,
			"mutation_id": base64.RawURLEncoding.EncodeToString(
				[]byte("limited-output-create")),
			"settings": map[string]any{
				"budgets": map[string]any{"output_bytes": outputBytes},
			},
		},
		"owner", "http://example.test")
	if response.Code != http.StatusCreated {
		t.Fatalf("limited output create status = %d, body = %s",
			response.Code, response.Body.String())
	}
	limited := settingsWorkspaceRequest(
		t, handler, http.MethodGet, "/api/v1/identity",
		nil, "owner", limitedWorkspace)
	if limited.Code != http.StatusInternalServerError {
		t.Fatalf("limited output status = %d, body = %s",
			limited.Code, limited.Body.String())
	}
	if uint64(limited.Body.Len()) > outputBytes {
		t.Fatalf("limited output body = %d bytes, want <= %d",
			limited.Body.Len(), outputBytes)
	}
}

func settingsHTTPOntologies(
	t *testing.T,
	now time.Time,
) (ontology.OntologyIdentity, ontology.OntologyIdentity) {
	t.Helper()
	schema, err := ontology.NewOntologySchema("workspace", "Workspace", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := ontology.NewOntologyVersion(
		schema, "1", now, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ontology.NewOntologyVersion(
		schema, "2", now.Add(time.Second), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	firstIdentity, err := ontology.NewOntologyIdentity(first)
	if err != nil {
		t.Fatal(err)
	}
	secondIdentity, err := ontology.NewOntologyIdentity(second)
	if err != nil {
		t.Fatal(err)
	}
	return firstIdentity, secondIdentity
}

func mustComponent(t *testing.T, value []byte) string {
	t.Helper()
	encoded, err := auth.EncodeComponent(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func encodeTestID(value shoal.ID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func settingsWorkspaceRequest(
	t *testing.T,
	handler http.Handler,
	method, path string,
	body any,
	subject, workspaceID string,
) *httptest.ResponseRecorder {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	request := httptest.NewRequest(
		method, "http://example.test"+path, bytes.NewReader(encoded))
	request.Host = "example.test"
	request.Header.Set("X-Test-Subject", subject)
	request.Header.Set(webapi.WorkspaceIDHeader, workspaceID)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
