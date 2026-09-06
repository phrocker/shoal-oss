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

package webapi

import (
	"context"
	"net/http"
	"strings"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/workspace"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// WorkspaceIDHeader selects the owned durable workspace settings applied to a
// non-settings API request. Its value is one canonical opaque wire ID.
const WorkspaceIDHeader = "Shoal-Workspace-ID"

// WorkspaceSettingsProvider is the transport-neutral settings extension used
// by the HTTP endpoint and future chat/MCP adapters.
type WorkspaceSettingsProvider interface {
	Get(context.Context, shoal.ID) (workspace.Settings, error)
	Update(
		context.Context,
		shoal.ID,
		workspace.UpdateRequest,
	) (workspace.Settings, error)
	ListOntologyChoices(
		context.Context,
		shoal.ID,
	) (workspace.OntologyChoiceSet, error)
	SelectOntology(
		context.Context,
		shoal.ID,
		uint64,
		shoal.ID,
		ontology.OntologyIdentity,
	) (workspace.Settings, error)
	ApplyDecision(context.Context, shoal.ID) (auth.Decision, error)
	Apply(
		context.Context,
		shoal.ID,
		workspace.Limits,
		[]auth.Policy,
	) (workspace.EffectiveDecision, error)
}

type effectiveWorkspaceSettingsContextKey struct{}

// EffectiveWorkspaceSettings returns the authenticated workspace settings
// effect already applied by Handler.ServeHTTP. Additive mounted transports can
// consume its budgets, output policies, and cache dimensions without
// re-resolving settings or replacing the issuer decision.
func EffectiveWorkspaceSettings(
	ctx context.Context,
) (workspace.EffectiveDecision, bool) {
	effective, ok := ctx.Value(
		effectiveWorkspaceSettingsContextKey{}).(workspace.EffectiveDecision)
	return effective, ok
}

type workspaceResponseWriter struct {
	http.ResponseWriter
	maxResponseBytes        uint64
	indeterminateOnOverflow bool
}

func (w workspaceResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// SetWorkspaceSettingsProvider enables the settings routes on a constructed
// handler. Authentication and host-authority checks remain centralized in
// ServeHTTP.
func (h *Handler) SetWorkspaceSettingsProvider(
	provider WorkspaceSettingsProvider,
) error {
	if h == nil {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "workspace handler is required")
	}
	if isAbsentInterface(provider) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "workspace settings provider is required")
	}
	if isAbsentInterface(h.authenticator) || isAbsentInterface(h.binder) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"workspace settings require an authenticated handler",
		)
	}
	h.workspaceSettings = provider
	return nil
}

func (h *Handler) applyWorkspaceSettings(
	request *http.Request,
) (context.Context, error) {
	if isAbsentInterface(h.workspaceSettings) ||
		isWorkspaceSettingsManagementPath(request.URL.Path) {
		return request.Context(), nil
	}
	encoded := request.Header.Get(WorkspaceIDHeader)
	if encoded == "" {
		return request.Context(), nil
	}
	workspaceID, err := decodeID(encoded)
	if err != nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "workspace settings header "+err.Error())
	}
	effective, err := h.workspaceSettings.Apply(
		request.Context(), workspaceID, workspace.MaximumLimits(), nil)
	if err != nil {
		return nil, err
	}
	decision := effective.Decision()
	ctx, err := h.binder.Bind(request.Context(), decision)
	if err != nil || ctx == nil {
		return nil, authenticationDenied()
	}
	ctx = withEffectiveWorkspaceSettings(ctx, effective)
	return withIdentity(ctx, decision), nil
}

func withEffectiveWorkspaceSettings(
	ctx context.Context,
	effective workspace.EffectiveDecision,
) context.Context {
	return context.WithValue(
		ctx, effectiveWorkspaceSettingsContextKey{}, effective)
}

func applyWorkspaceRequestLimits(ctx context.Context, request any) {
	effective, ok := EffectiveWorkspaceSettings(ctx)
	if !ok {
		return
	}
	limits := effective.Limits()
	switch value := request.(type) {
	case *RetrievalRequest:
		value.Query.TopK = lowerRequestLimit(
			value.Query.TopK, retrieval.DefaultTopK, limits.RetrievalTopK)
	case *NeighborhoodRequest:
		value.Depth = lowerRequestLimit(
			value.Depth, DefaultDepth, limits.GraphDepth)
		value.Fanout = lowerRequestLimit(
			value.Fanout, DefaultFanout, limits.GraphFanout)
		value.MaxNodes = lowerRequestLimit(
			value.MaxNodes, DefaultMaxNodes, limits.GraphNodes)
	case *PathRequest:
		value.MaxDepth = lowerRequestLimit(
			value.MaxDepth, DefaultDepth, limits.GraphDepth)
		value.Fanout = lowerRequestLimit(
			value.Fanout, DefaultFanout, limits.GraphFanout)
	}
}

func lowerRequestLimit(value, defaultValue, maximum uint32) uint32 {
	if value == 0 {
		value = defaultValue
	}
	if value > maximum {
		return maximum
	}
	return value
}

func effectiveGraphNodeLimit(ctx context.Context, fallback uint32) uint32 {
	effective, ok := EffectiveWorkspaceSettings(ctx)
	if !ok || effective.Limits().GraphNodes >= fallback {
		return fallback
	}
	return effective.Limits().GraphNodes
}

func applyWorkspaceMetadataLimits(
	ctx context.Context,
	metadata MetadataResponse,
) MetadataResponse {
	effective, ok := EffectiveWorkspaceSettings(ctx)
	if !ok {
		return metadata
	}
	limits := effective.Limits()
	metadata.MaxTopK = min(metadata.MaxTopK, limits.RetrievalTopK)
	metadata.MaxDepth = min(metadata.MaxDepth, limits.GraphDepth)
	metadata.MaxFanout = min(metadata.MaxFanout, limits.GraphFanout)
	metadata.MaxNodes = min(metadata.MaxNodes, limits.GraphNodes)
	metadata.MaxResponseBytes = min(
		metadata.MaxResponseBytes, limits.OutputBytes)
	return metadata
}

func responseLimitFor(writer http.ResponseWriter) uint64 {
	if limited, ok := writer.(workspaceResponseWriter); ok {
		return limited.maxResponseBytes
	}
	return MaxResponseBytes
}

func responseOverflowIsIndeterminate(writer http.ResponseWriter) bool {
	limited, ok := writer.(workspaceResponseWriter)
	return ok && limited.indeterminateOnOverflow
}

func responseLimitForContext(ctx context.Context) uint64 {
	effective, ok := EffectiveWorkspaceSettings(ctx)
	if !ok {
		return MaxResponseBytes
	}
	return effective.Limits().OutputBytes
}

func isWorkspaceSettingsManagementPath(path string) bool {
	return strings.HasPrefix(path, "/api/v1/workspaces/") &&
		(strings.HasSuffix(path, "/settings") ||
			strings.HasSuffix(path, "/settings/lens"))
}

func requestMayCommit(method, path string) bool {
	if method != http.MethodPost {
		return false
	}
	switch path {
	case "/api/v1/ingest",
		"/api/v1/extract",
		"/api/v1/derivation/recompute",
		"/api/v1/ontology/proposals":
		return true
	default:
		return strings.HasPrefix(path, "/api/v1/ontology/proposals/") &&
			strings.HasSuffix(path, "/transition")
	}
}
