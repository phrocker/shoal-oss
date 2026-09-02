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
	"context"
	"fmt"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
)

// policyBackfiller registers corpus content that predates the policy catalog.
type policyBackfiller interface {
	BackfillExistingDocuments(context.Context) (int, error)
}

// developmentBackfill grants the documents already on disk to the development
// principal at startup.
//
// TEMPORARY (issue #284): the only PolicyStore implementation keeps its
// registrations in process memory, so restarting the workspace hides a corpus
// that was previously ingested through it. Without this, every restart would
// present an empty demo whose only recovery is re-ingesting everything. Delete
// this together with the backfill it calls once #284 lands a durable catalog.
//
// It is safe only under the conditions newDevelopmentBackfill enforces: the
// development principal is already granted the whole workspace corpus on a
// loopback listener, so backfilling grants it nothing that re-ingesting the
// same files would not.
type developmentBackfill struct {
	authenticator *developmentAuthenticator
	binder        auth.Binder
}

// newDevelopmentBackfill is the gate. It returns a backfill only when the
// selected authenticator is the development authenticator, which
// selectAuthenticator returns only for -dev-auth, and only when the resolved
// listener address is loopback. Any real authenticator, and any listener that
// another host can reach, gets nil and no backfill.
func newDevelopmentBackfill(
	authenticator webapi.Authenticator,
	address string,
	binder auth.Binder,
) *developmentBackfill {
	development, ok := authenticator.(*developmentAuthenticator)
	if !ok {
		return nil
	}
	if !listenAddressIsLoopback(address) {
		return nil
	}
	if development == nil || binder == nil {
		return nil
	}
	return &developmentBackfill{authenticator: development, binder: binder}
}

// run mints a development decision, binds it exactly as the HTTP transport
// binds a request decision, and registers the pre-existing corpus under it. It
// fails closed: a partial or failed backfill is returned as an error so the
// caller refuses to serve rather than serving a corpus it did not finish
// authorizing.
func (b *developmentBackfill) run(
	ctx context.Context,
	client policyBackfiller,
) (int, error) {
	if b == nil {
		return 0, nil
	}
	if b.authenticator == nil || b.binder == nil || client == nil {
		return 0, fmt.Errorf(
			"refusing to serve: the development corpus backfill is misconfigured")
	}
	decision, err := b.authenticator.mint()
	if err != nil {
		return 0, fmt.Errorf(
			"refusing to serve: minting the %s development decision failed: %w",
			developmentSubject, err)
	}
	bound, err := b.binder.Bind(ctx, decision)
	if err != nil {
		return 0, fmt.Errorf(
			"refusing to serve: binding the %s development decision failed: %w",
			developmentSubject, err)
	}
	registered, err := client.BackfillExistingDocuments(bound)
	if err != nil {
		return 0, fmt.Errorf(
			"refusing to serve: granting the existing corpus to %s failed, so "+
				"some documents would be served unregistered or not at all: %w",
			developmentSubject, err)
	}
	return registered, nil
}
