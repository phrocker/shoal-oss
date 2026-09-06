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

// WorkspaceSettingsHTTPConfig configures the independently mountable settings
// management handler.
type WorkspaceSettingsHTTPConfig struct {
	Provider WorkspaceSettingsProvider
}

// NewWorkspaceSettingsHTTPHandler constructs the settings management subtree.
// The returned handler performs authorization through Provider but must be
// mounted behind the host's existing authentication and host/origin gates.
func NewWorkspaceSettingsHTTPHandler(
	config WorkspaceSettingsHTTPConfig,
) (http.Handler, error) {
	if isAbsentInterface(config.Provider) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "workspace settings provider is required")
	}
	routes := &Handler{
		mux:               http.NewServeMux(),
		workspaceSettings: config.Provider,
	}
	routes.registerWorkspaceSettingsRoutes()
	return routes.mux, nil
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

// ConfigureWorkspaceSettings enables effective-decision application without
// registering management routes. It is intended for hosts that mount the
// handler returned by NewWorkspaceSettingsHTTPHandler through their existing
// authenticated routing seam.
func (h *Handler) ConfigureWorkspaceSettings(
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
	if h.workspaceSettingsMounted {
		return shoal.NewError(
			shoal.ErrorConflict, "workspace settings routes are already mounted")
	}
	h.workspaceSettings = provider
	return nil
}

// MountWorkspaceSettings configures effective-decision application and mounts
// the settings management subtree behind this Handler's existing gates.
func (h *Handler) MountWorkspaceSettings(
	config WorkspaceSettingsHTTPConfig,
) error {
	if err := h.ConfigureWorkspaceSettings(config.Provider); err != nil {
		return err
	}
	settingsHandler, err := NewWorkspaceSettingsHTTPHandler(config)
	if err != nil {
		return err
	}
	h.mux.Handle("GET /api/v1/workspaces/", settingsHandler)
	h.mux.Handle("PUT /api/v1/workspaces/", settingsHandler)
	h.workspaceSettingsMounted = true
	return nil
}

// SetWorkspaceSettingsProvider preserves the original one-call integration
// surface and delegates to MountWorkspaceSettings.
func (h *Handler) SetWorkspaceSettingsProvider(
	provider WorkspaceSettingsProvider,
) error {
	return h.MountWorkspaceSettings(
		WorkspaceSettingsHTTPConfig{Provider: provider})
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
		narrowed := ClampWorkspaceRequestLimits(
			workspace.Limits{RetrievalTopK: value.Query.TopK},
			workspace.Limits{RetrievalTopK: retrieval.DefaultTopK},
			limits,
		)
		value.Query.TopK = narrowed.RetrievalTopK
	case *NeighborhoodRequest:
		narrowed := ClampWorkspaceRequestLimits(
			workspace.Limits{
				GraphDepth:  value.Depth,
				GraphFanout: value.Fanout,
				GraphNodes:  value.MaxNodes,
			},
			workspace.Limits{
				GraphDepth:  DefaultDepth,
				GraphFanout: DefaultFanout,
				GraphNodes:  DefaultMaxNodes,
			},
			limits,
		)
		value.Depth = narrowed.GraphDepth
		value.Fanout = narrowed.GraphFanout
		value.MaxNodes = narrowed.GraphNodes
	case *PathRequest:
		narrowed := ClampWorkspaceRequestLimits(
			workspace.Limits{
				GraphDepth:  value.MaxDepth,
				GraphFanout: value.Fanout,
			},
			workspace.Limits{
				GraphDepth:  DefaultDepth,
				GraphFanout: DefaultFanout,
			},
			limits,
		)
		value.MaxDepth = narrowed.GraphDepth
		value.Fanout = narrowed.GraphFanout
	}
}

// ClampWorkspaceRequestLimits replaces zero request values with their defaults
// and lowers every request dimension to its effective workspace maximum.
func ClampWorkspaceRequestLimits(
	requested workspace.Limits,
	defaults workspace.Limits,
	maximum workspace.Limits,
) workspace.Limits {
	return workspace.Limits{
		RetrievalTopK: lowerRequestLimit(
			requested.RetrievalTopK,
			defaults.RetrievalTopK,
			maximum.RetrievalTopK,
		),
		GraphDepth: lowerRequestLimit(
			requested.GraphDepth,
			defaults.GraphDepth,
			maximum.GraphDepth,
		),
		GraphFanout: lowerRequestLimit(
			requested.GraphFanout,
			defaults.GraphFanout,
			maximum.GraphFanout,
		),
		GraphNodes: lowerRequestLimit(
			requested.GraphNodes,
			defaults.GraphNodes,
			maximum.GraphNodes,
		),
		OutputBytes: lowerRequestByteLimit(
			requested.OutputBytes,
			defaults.OutputBytes,
			maximum.OutputBytes,
		),
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

func lowerRequestByteLimit(value, defaultValue, maximum uint64) uint64 {
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
