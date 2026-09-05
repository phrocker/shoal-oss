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
	ctx = context.WithValue(
		ctx, effectiveWorkspaceSettingsContextKey{}, effective)
	return withIdentity(ctx, decision), nil
}

func isWorkspaceSettingsManagementPath(path string) bool {
	return strings.HasPrefix(path, "/api/v1/workspaces/") &&
		(strings.HasSuffix(path, "/settings") ||
			strings.HasSuffix(path, "/settings/lens"))
}
