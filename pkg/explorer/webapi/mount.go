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
	"path"
	"strings"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type preAuthenticationValidator interface {
	ValidatePreAuthentication(*http.Request) int
}

// MountAuthenticated mounts an additive endpoint behind the Handler's existing
// host-authority and authentication gates. It cannot make an endpoint public;
// callers must choose a path outside publiclyReachable's fixed allowlist.
func (h *Handler) MountAuthenticated(
	pattern string, handler http.Handler,
) (err error) {
	if h == nil || h.mux == nil {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "workspace handler is required")
	}
	if isAbsentInterface(handler) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "mounted handler is required")
	}
	if isAbsentInterface(h.authenticator) || isAbsentInterface(h.binder) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"authenticated mount requires authentication dependencies")
	}
	cleaned := path.Clean(pattern)
	canonical := cleaned
	if strings.HasSuffix(pattern, "/") && canonical != "/" {
		canonical += "/"
	}
	if !strings.HasPrefix(pattern, "/") || pattern == "/" || pattern != canonical ||
		cleaned == "/api/v1/auth-config" ||
		cleaned == "/assets" || strings.HasPrefix(cleaned, "/assets/") ||
		conflictsWithWorkspaceRoute(cleaned) {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "authenticated mount pattern is invalid")
	}
	root := strings.TrimSuffix(pattern, "/")
	for mounted := range h.authenticatedMounts {
		if strings.TrimSuffix(mounted, "/") == root {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"authenticated mount pattern conflicts with an existing route")
		}
	}
	h.authenticatedMounts[pattern] = handler
	if validator, ok := handler.(preAuthenticationValidator); ok {
		h.preAuth[pattern] = validator
	}
	return nil
}

func conflictsWithWorkspaceRoute(mount string) bool {
	protected := []string{
		"/api/v1/meta", "/api/v1/identity", "/api/v1/ontology",
		"/api/v1/auth-config", "/api/v1/ingest", "/api/v1/extract",
		"/api/v1/derivation/recompute", "/api/v1/changes",
		"/api/v1/documents", "/api/v1/document", "/api/v1/retrieve",
		"/api/v1/neighborhood", "/api/v1/path", "/api/v1/analytics",
	}
	for _, route := range protected {
		if mount == route || strings.HasPrefix(route, mount+"/") ||
			strings.HasPrefix(mount, route+"/") {
			return true
		}
	}
	return false
}
