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
	"net/url"
	"path"
	"strings"
)

// BrowserAuthConfig carries the non-secret parameters a browser needs to begin
// an OIDC Authorization Code + PKCE login against a configured identity
// provider. Every field appears in the address bar of an ordinary OIDC redirect
// and none is a secret: there is deliberately no client secret here. The
// browser performs a public-client PKCE exchange and the server only validates
// the bearer token that results; it never issues one.
type BrowserAuthConfig struct {
	// TenantID is the directory identifier, surfaced for display only.
	TenantID string
	// ClientID is the public application (client) ID the browser authenticates
	// as and that the resulting token is audienced to.
	ClientID string
	// Scope is the space-delimited scope string the browser requests.
	Scope string
	// Authority is the OIDC authority base, for example
	// https://login.microsoftonline.com/<tenant>. The browser derives the
	// authorize and token endpoints from it.
	Authority string
}

// AuthConfigResponse is the body of GET /api/v1/auth-config. It is unauthenticated
// and non-secret by construction: it discloses only what an interactive login
// needs and nothing about the corpus, the policy catalog, or any principal.
// When no interactive login is configured it reports Configured false and the
// browser renders no login UI at all, which is the correct state for -dev-auth.
type AuthConfigResponse struct {
	Configured bool   `json:"configured"`
	TenantID   string `json:"tenant_id,omitempty"`
	ClientID   string `json:"client_id,omitempty"`
	Scope      string `json:"scope,omitempty"`
	Authority  string `json:"authority,omitempty"`
}

// SetBrowserAuthConfig supplies the non-secret parameters a browser needs to
// begin an interactive login. It is optional: when unset (or set to nil) the
// auth-config endpoint reports login as unconfigured, the connect-src policy
// stays at its strict local default, and the UI renders no login control. That
// is the seam that keeps local development a single command.
func (h *Handler) SetBrowserAuthConfig(config *BrowserAuthConfig) {
	h.browserAuth = config
}

// authConfigEndpoint answers GET /api/v1/auth-config. It is on the
// unauthenticated allowlist (see publiclyReachable) because the browser must
// read it before it can hold any token.
func (h *Handler) authConfigEndpoint(writer http.ResponseWriter, _ *http.Request) {
	if h.browserAuth == nil {
		writeResponse(writer, http.StatusOK, AuthConfigResponse{Configured: false})
		return
	}
	writeResponse(writer, http.StatusOK, AuthConfigResponse{
		Configured: true,
		TenantID:   h.browserAuth.TenantID,
		ClientID:   h.browserAuth.ClientID,
		Scope:      h.browserAuth.Scope,
		Authority:  h.browserAuth.Authority,
	})
}

// connectSources is the connect-src value for the Content-Security-Policy. With
// no interactive login configured it is exactly 'self', byte-identical to the
// local development posture. When a login is configured the browser must POST
// the PKCE token exchange to the identity provider, so exactly that provider's
// origin — scheme and host only, derived from the configured authority — is
// added and nothing wider. A malformed or non-https authority contributes
// nothing, failing closed to 'self'.
func (h *Handler) connectSources() string {
	if h.browserAuth == nil {
		return "'self'"
	}
	origin := authorityOrigin(h.browserAuth.Authority)
	if origin == "" {
		return "'self'"
	}
	return "'self' " + origin
}

func authorityOrigin(authority string) string {
	parsed, err := url.Parse(strings.TrimSpace(authority))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return ""
	}
	return parsed.Scheme + "://" + parsed.Host
}

// publiclyReachable is the fail-closed allowlist of request lines the transport
// serves without a trusted decision. It is expressed as an allowlist consulted
// before authenticate, so a route that is not named here is authenticated by
// default: a contributor who adds a new /api/v1/* handler gets authentication
// for free and cannot make it public by omission.
//
// The match is exact on method+path for the two named routes and a single
// prefix for the immutable asset tree. The prefix is "/assets/" only, which can
// never overlap an "/api/v1/*" path, and the whole predicate is gated to GET,
// so no state-changing route is ever reachable unauthenticated. The request
// path is cleaned before matching, so a traversal such as
// /assets/../api/v1/documents is judged as the API route it resolves to, not as
// an asset, and remains authenticated.
//
// This does not bypass the host-authority gate, which runs first and
// unconditionally in ServeHTTP; it only decides whether authentication is
// required for a request the host gate has already admitted.
func (h *Handler) publiclyReachable(request *http.Request) bool {
	if request.Method != http.MethodGet {
		return false
	}
	cleaned := request.URL.Path
	if cleaned == "" {
		cleaned = "/"
	}
	cleaned = path.Clean(cleaned)
	switch cleaned {
	case "/", "/api/v1/auth-config":
		return true
	}
	return cleaned == "/assets" || strings.HasPrefix(cleaned, "/assets/")
}
