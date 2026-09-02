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

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Authenticator turns an inbound request into a trusted decision. Any returned
// error denies the request; the transport never distinguishes denial reasons to
// the caller. Implementations must derive identity only from material they can
// verify, never from unauthenticated request metadata.
type Authenticator interface {
	Authenticate(*http.Request) (auth.Decision, error)
}

// AuthenticatorFunc adapts a function to the Authenticator contract.
type AuthenticatorFunc func(*http.Request) (auth.Decision, error)

// Authenticate calls the trusted authenticator function.
func (f AuthenticatorFunc) Authenticate(
	request *http.Request,
) (auth.Decision, error) {
	return f(request)
}

// NewAuthenticatedHandler constructs the standard HTTP transport with
// mandatory per-request identity. Every request, including the workspace shell
// and its assets, must carry a decision that the binder accepts before any
// route observes it. Requests that fail authentication are answered with the
// repository's unauthorized shape and never reach the service.
//
// The binder must belong to the same auth.Authority whose resolver the
// service's Explorer client was constructed with; identity is carried per
// request through the context capability rather than through process state, so
// the transport scales unchanged from one to many instances.
func NewAuthenticatedHandler(
	service Service,
	allowedAuthority string,
	authenticator Authenticator,
	binder auth.Binder,
) (*Handler, error) {
	if authenticator == nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "workspace authenticator is required")
	}
	if binder == nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "workspace decision binder is required")
	}
	handler, err := NewHandler(service, allowedAuthority)
	if err != nil {
		return nil, err
	}
	handler.authenticator = authenticator
	handler.binder = binder
	return handler, nil
}

// authenticate fails closed. Every authenticator or binder failure denies the
// request with shoal.ErrorUnauthorized, which writeError maps to 401.
func (h *Handler) authenticate(
	request *http.Request,
) (context.Context, error) {
	if h.authenticator == nil || h.binder == nil {
		return nil, authenticationDenied()
	}
	decision, err := h.authenticator.Authenticate(request)
	if err != nil {
		return nil, authenticationDenied()
	}
	ctx, err := h.binder.Bind(request.Context(), decision)
	if err != nil || ctx == nil {
		return nil, authenticationDenied()
	}
	return ctx, nil
}

func authenticationDenied() error {
	return shoal.NewError(shoal.ErrorUnauthorized, "authentication required")
}
