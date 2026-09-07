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
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// WorkspaceIDHeader selects the owned durable workspace settings applied to a
// non-settings API request. Its value is one canonical opaque wire ID.
const WorkspaceIDHeader = "Shoal-Workspace-ID"

// WorkspaceSettingsApplier narrows one authenticated request under the exact
// operation that the consuming route will execute.
type WorkspaceSettingsApplier interface {
	ApplyForOperation(
		context.Context,
		shoal.ID,
		auth.Operation,
		workspace.Limits,
		[]auth.Policy,
	) (workspace.EffectiveDecision, error)
}

// WorkspaceSettingsProvider is the transport-neutral settings extension used
// by the HTTP endpoint and future chat/MCP adapters.
type WorkspaceSettingsProvider interface {
	WorkspaceSettingsApplier
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
}

type effectiveWorkspaceSettingsContextKey struct{}
type effectiveWorkspaceIDContextKey struct{}

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

// EffectiveWorkspaceID returns the authenticated workspace selector already
// resolved by Handler.ServeHTTP.
func EffectiveWorkspaceID(ctx context.Context) (shoal.ID, bool) {
	workspaceID, ok := ctx.Value(
		effectiveWorkspaceIDContextKey{}).(shoal.ID)
	return workspaceID, ok
}

type workspaceResponseWriter struct {
	http.ResponseWriter
	maxResponseBytes        uint64
	indeterminateOnOverflow bool
}

func (w workspaceResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func responseSupportsFlush(writer http.ResponseWriter) bool {
	for depth := 0; writer != nil && depth < 100; depth++ {
		if _, ok := writer.(http.Flusher); ok {
			return true
		}
		unwrapper, ok := writer.(interface {
			Unwrap() http.ResponseWriter
		})
		if !ok {
			return false
		}
		writer = unwrapper.Unwrap()
	}
	return false
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
	if request.URL.Path == "/mcp" {
		return withEffectiveWorkspaceID(request.Context(), workspaceID), nil
	}
	operation, ok := workspaceOperationForRequest(
		request.Method, request.URL.Path)
	if !ok {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"workspace settings are not registered for this route",
		)
	}
	return ApplyWorkspaceSettingsForOperation(
		request.Context(), h.workspaceSettings, h.binder,
		workspaceID, operation, workspace.MaximumLimits(), nil,
	)
}

// ApplyWorkspaceSettingsForOperation loads and binds one owned workspace under
// the exact operation that the consuming transport is about to execute.
func ApplyWorkspaceSettingsForOperation(
	ctx context.Context,
	provider WorkspaceSettingsApplier,
	binder auth.Binder,
	workspaceID shoal.ID,
	operation auth.Operation,
	baseLimits workspace.Limits,
	baseOutputPolicies []auth.Policy,
) (context.Context, error) {
	if ctx == nil || isAbsentInterface(provider) || isAbsentInterface(binder) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"workspace settings application dependencies are required",
		)
	}
	effective, err := provider.ApplyForOperation(
		ctx, workspaceID, operation, baseLimits, baseOutputPolicies)
	if err != nil {
		return nil, err
	}
	decision := effective.Decision()
	ctx, err = binder.Bind(ctx, decision)
	if err != nil || ctx == nil {
		return nil, authenticationDenied()
	}
	visibility, err := effective.OutputVisibility()
	if err != nil {
		return nil, err
	}
	labels, err := interaction.ParseVisibility(string(visibility))
	if err != nil {
		return nil, err
	}
	ctx, err = interaction.WithRequiredVisibility(ctx, labels)
	if err != nil {
		return nil, err
	}
	ctx = withEffectiveWorkspaceSettings(ctx, workspaceID, effective)
	return withIdentity(ctx, decision), nil
}

func withEffectiveWorkspaceID(
	ctx context.Context,
	workspaceID shoal.ID,
) context.Context {
	return context.WithValue(
		ctx, effectiveWorkspaceIDContextKey{}, workspaceID)
}

func withEffectiveWorkspaceSettings(
	ctx context.Context,
	workspaceID shoal.ID,
	effective workspace.EffectiveDecision,
) context.Context {
	ctx = context.WithValue(
		ctx, effectiveWorkspaceSettingsContextKey{}, effective)
	return withEffectiveWorkspaceID(ctx, workspaceID)
}

func workspaceOperationForRequest(
	method, path string,
) (auth.Operation, bool) {
	if method == http.MethodHead {
		method = http.MethodGet
	}
	switch {
	case method == http.MethodGet &&
		(path == "/api/v1/meta" ||
			path == "/api/v1/identity" ||
			path == "/api/v1/ontology" ||
			path == "/api/v1/ontology/proposals" ||
			path == "/api/v1/provenance" ||
			strings.HasPrefix(path, "/api/v1/provenance/")):
		return auth.OperationRead, true
	case method == http.MethodGet &&
		strings.HasPrefix(path, "/api/v1/ontology/proposals/") &&
		strings.HasSuffix(path, "/blast-radius"):
		return auth.OperationRead, true
	case method == http.MethodPost &&
		(path == "/api/v1/ingest" ||
			path == "/api/v1/extract" ||
			path == "/api/v1/derivation/recompute" ||
			path == "/api/v1/ontology/proposals"):
		return auth.OperationIngest, true
	case method == http.MethodPost &&
		strings.HasPrefix(path, "/api/v1/ontology/proposals/") &&
		strings.HasSuffix(path, "/transition"):
		return auth.OperationIngest, true
	case method == http.MethodPost &&
		(path == "/api/v1/changes" || path == "/api/v1/documents"):
		return auth.OperationList, true
	case method == http.MethodPost && path == "/api/v1/document":
		return auth.OperationRead, true
	case method == http.MethodPost &&
		(path == "/api/v1/retrieve" ||
			path == "/api/v1/ask" ||
			path == "/api/v1/chat/stream"):
		return auth.OperationRetrieve, true
	case method == http.MethodPost &&
		(path == "/api/v1/neighborhood" || path == "/api/v1/path"):
		return auth.OperationNeighborhood, true
	case method == http.MethodPost && path == "/api/v1/analytics":
		return auth.OperationAnalyticsRead, true
	case method == http.MethodPost &&
		(path == "/api/v1/provenance/fold" ||
			path == "/api/v1/provenance/unfold"):
		return auth.OperationRead, true
	case method == http.MethodPost && path == "/api/v1/fleet/agents":
		return auth.OperationAgentRegister, true
	case method == http.MethodPost &&
		strings.HasPrefix(path, "/api/v1/fleet/agents/") &&
		strings.HasSuffix(path, "/heartbeat"):
		return auth.OperationAgentHeartbeat, true
	case method == http.MethodPost &&
		strings.HasPrefix(path, "/api/v1/fleet/agents/") &&
		strings.HasSuffix(path, "/revoke"):
		return auth.OperationAgentRevoke, true
	case method == http.MethodPost &&
		(path == "/api/v1/fleet/agents/resolve" ||
			(strings.HasPrefix(path, "/api/v1/fleet/agents/") &&
				strings.HasSuffix(path, "/resolve"))):
		return auth.OperationAgentResolve, true
	case method == http.MethodPost && path == "/api/v1/fleet/actions":
		return auth.OperationDispatch, true
	case method == http.MethodPost &&
		(path == "/api/v1/fleet/actions/invoke" ||
			path == "/api/v1/fleet/actions/pull" ||
			(strings.HasPrefix(path, "/api/v1/fleet/actions/") &&
				strings.HasSuffix(path, "/claim"))):
		return auth.OperationInvoke, true
	case method == http.MethodPost &&
		strings.HasPrefix(path, "/api/v1/fleet/actions/") &&
		(strings.HasSuffix(path, "/cancel") ||
			strings.HasSuffix(path, "/status")):
		return auth.OperationDispatch, true
	case method == http.MethodPost &&
		path == "/api/v1/fleet/events/subscriptions":
		return auth.OperationSubscriptionCreate, true
	case method == http.MethodDelete &&
		strings.HasPrefix(path, "/api/v1/fleet/events/subscriptions/"):
		return auth.OperationSubscriptionDelete, true
	case method == http.MethodPost &&
		strings.HasPrefix(path, "/api/v1/fleet/events/subscriptions/") &&
		strings.HasSuffix(path, "/pull"):
		return auth.OperationSubscriptionDeliver, true
	case method == http.MethodPost &&
		path == "/api/v1/fleet/events/publish":
		return auth.OperationEventPublish, true
	default:
		return "", false
	}
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
	if path == "/mcp" {
		// The outer handler cannot inspect the JSON-RPC method without
		// consuming the body. Treat MCP POST overflow conservatively because
		// tools may durably mutate before their response is written.
		return true
	}
	switch path {
	case "/api/v1/ingest",
		"/api/v1/extract",
		"/api/v1/derivation/recompute",
		"/api/v1/ontology/proposals",
		"/api/v1/ask",
		"/api/v1/chat/stream",
		"/api/v1/provenance/fold":
		return true
	default:
		return strings.HasPrefix(path, "/api/v1/fleet/") ||
			(strings.HasPrefix(path, "/api/v1/ontology/proposals/") &&
				strings.HasSuffix(path, "/transition"))
	}
}
