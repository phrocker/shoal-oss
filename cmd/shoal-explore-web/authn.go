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

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// The workspace serves one authorization domain. Every decision this command
// accepts, whether minted by the development authenticator or by a future
// integration, is scoped to it, and the policy selector registers ingested
// content under the same trusted source and grant identities.
var (
	workspaceAuthorizationDomain = []byte("shoal-explore-web")
	workspaceSourceID            = []byte("shoal-explore-web/workspace")
	workspaceGrantPolicyID       = []byte("shoal-explore-web/workspace-grant")
)

// workspacePolicyGeneration is the immutable configured generation published
// by this build. It is configuration rather than mutable process state, so
// every instance of the command reports the same value.
const workspacePolicyGeneration int64 = 1

// The development principal is deliberately named so it is unmistakable in
// audit output. It is minted only for loopback listeners under -dev-auth.
const (
	developmentSubject shoal.ID = "development-principal@localhost"
	developmentActor   shoal.ID = "shoal-explore-web-dev-auth"
	// developmentAuditPurpose records why the decision exists.
	developmentAuditPurpose = "localhost development workspace"
	// developmentSessionLifetime bounds every minted development credential.
	developmentSessionLifetime = 15 * time.Minute
)

// workspaceOperations is the operation set the workspace UI needs. It is the
// ceiling of the development principal, not of the transport: a real
// authenticator may mint narrower decisions and the authorized Explorer client
// enforces whatever it minted.
var workspaceOperations = []auth.Operation{
	auth.OperationIngest,
	auth.OperationList,
	auth.OperationRead,
	auth.OperationConnect,
	auth.OperationNeighborhood,
	auth.OperationRetrieve,
}

// developmentAuthenticator mints one short-lived decision per request for a
// fixed, clearly-named development principal. It authenticates nothing: it is
// valid only because the listener is proven to be loopback-only, so the only
// callers are processes on this host.
type developmentAuthenticator struct {
	clock    func() time.Time
	lifetime time.Duration
}

func newDevelopmentAuthenticator(
	clock func() time.Time,
) (*developmentAuthenticator, error) {
	if clock == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "clock is required")
	}
	return &developmentAuthenticator{
		clock:    clock,
		lifetime: developmentSessionLifetime,
	}, nil
}

// Authenticate returns a fresh decision with a unique request identity so that
// two concurrent requests are never correlated as one.
func (a *developmentAuthenticator) Authenticate(
	request *http.Request,
) (auth.Decision, error) {
	if request == nil {
		return auth.Decision{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "request is required")
	}
	return a.mint()
}

// mint issues one short-lived development decision.
func (a *developmentAuthenticator) mint() (auth.Decision, error) {
	requestID, err := newRequestID()
	if err != nil {
		return auth.Decision{}, err
	}
	return auth.NewDecision(auth.DecisionConfig{
		Subject:               developmentSubject,
		Actor:                 developmentActor,
		AuthorizationDomain:   workspaceAuthorizationDomain,
		AllowedOperations:     workspaceOperations,
		PermittedSourceIDs:    [][]byte{workspaceSourceID},
		PermittedPolicyIDs:    [][]byte{workspaceGrantPolicyID},
		PolicyGeneration:      workspacePolicyGeneration,
		AuthenticationExpires: a.clock().Add(a.lifetime),
		RequestID:             requestID,
		AuditPurpose:          developmentAuditPurpose,
	})
}

func newRequestID() (shoal.ID, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", shoal.WrapError(
			shoal.ErrorUnavailable, "request identity unavailable", err)
	}
	return shoal.ID("dev-request-" + hex.EncodeToString(raw)), nil
}

// fixedGenerationReader publishes the immutable configured authorization
// generation for the workspace domain. It holds no mutable process state, so
// every instance of the command answers identically. An unknown domain reports
// zero, which the generation guard treats as unavailable and fails closed.
type fixedGenerationReader struct {
	domain     []byte
	generation int64
}

func (r fixedGenerationReader) CurrentPolicyGeneration(
	ctx context.Context,
	domain []byte,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !bytes.Equal(domain, r.domain) {
		return 0, nil
	}
	return r.generation, nil
}

// listenAddressIsLoopback reports whether a resolved listener address is bound
// exclusively to loopback interfaces.
//
// An unspecified address binds every interface and is never loopback, so
// ":8080", "0.0.0.0:8080", and "[::]:8080" are all rejected. Literal addresses
// are classified by net.IP.IsLoopback. A hostname is loopback only when every
// address it resolves to is loopback; an unresolvable name is not loopback.
func listenAddressIsLoopback(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || port == "" {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsUnspecified() {
			return false
		}
		return ip.IsLoopback()
	}
	resolved, err := net.LookupIP(host)
	if err != nil || len(resolved) == 0 {
		return false
	}
	for _, ip := range resolved {
		if ip.IsUnspecified() || !ip.IsLoopback() {
			return false
		}
	}
	return true
}

// selectAuthenticator fails closed. The workspace refuses to serve any request
// without a trusted decision, and refuses the development principal on any
// listener that is reachable from another host.
func selectAuthenticator(
	developmentAuth bool,
	address string,
	clock func() time.Time,
) (webapi.Authenticator, error) {
	if !developmentAuth {
		return nil, fmt.Errorf(
			"refusing to serve %s without authentication: no authenticator is "+
				"configured; pass -dev-auth to mint the %s development "+
				"principal on a loopback listener, or supply a real "+
				"authenticator before exposing the Explorer API",
			address, developmentSubject,
		)
	}
	if !listenAddressIsLoopback(address) {
		return nil, fmt.Errorf(
			"refusing to serve %s with -dev-auth: the %s development principal "+
				"is granted the whole workspace corpus and is only safe on a "+
				"loopback listener; bind 127.0.0.1 or [::1], or supply a real "+
				"authenticator",
			address, developmentSubject,
		)
	}
	return newDevelopmentAuthenticator(clock)
}
