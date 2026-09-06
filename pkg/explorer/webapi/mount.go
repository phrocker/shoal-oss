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
	if !strings.HasPrefix(pattern, "/") || pattern == "/" ||
		pattern == "/api/v1/auth-config" ||
		pattern == "/assets" || strings.HasPrefix(pattern, "/assets/") {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "authenticated mount pattern is invalid")
	}
	defer func() {
		if recover() != nil {
			err = shoal.NewError(
				shoal.ErrorInvalidArgument,
				"authenticated mount pattern conflicts with an existing route",
			)
		}
	}()
	if validator, ok := handler.(preAuthenticationValidator); ok {
		h.preAuth[pattern] = validator
	}
	h.mux.Handle("POST "+pattern, handler)
	h.mux.Handle("GET "+pattern, handler)
	h.mux.Handle("DELETE "+pattern, handler)
	return nil
}
