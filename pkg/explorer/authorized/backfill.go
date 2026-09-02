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

package authorized

import (
	"context"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
)

// BackfillExistingDocuments registers the current revision of every base
// document that has no policy-catalog registration yet, and returns how many
// were registered.
//
// TEMPORARY (issue #284): this exists only because PolicyStore has no durable
// implementation. MemoryPolicyStore loses every registration when the process
// exits, so a corpus that outlives the process becomes permanently invisible
// even to the principal that ingested it. Delete this method when #284 lands a
// durable catalog.
//
// It grants nothing that ingest would not grant. The decision is read from ctx
// through the same trusted resolver every other operation uses, it must
// authorize OperationIngest and satisfy the generation guard, and each rule is
// derived by the trusted policy selector exactly as Ingest derives it. Callers
// therefore need the binding capability to reach this at all. Documents that
// are already registered are left untouched, and any failure aborts with the
// error rather than reporting partial success.
func (c *Client) BackfillExistingDocuments(ctx context.Context) (int, error) {
	decision, guard, now, err := c.begin(ctx, auth.OperationIngest)
	if err != nil {
		return 0, err
	}
	summaries, err := c.base.Documents(ctx)
	if err != nil {
		return 0, err
	}

	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	lease, err := c.policyStore.AcquireMutation(ctx)
	if err != nil {
		return 0, policyCatalogWriteError(ctx, err)
	}
	defer lease.Release()
	if err := guard.Check(ctx); err != nil {
		return 0, err
	}

	registered := 0
	for _, summary := range summaries {
		if err := validateSummary(summary); err != nil {
			return 0, inconsistentBase()
		}
		_, exists, err := c.policyStore.CurrentRevision(ctx, summary.Document.ID)
		if err != nil {
			return 0, policyCatalogReadError(ctx, err)
		}
		if exists {
			continue
		}
		rule, err := c.selectIngestRule(ctx, decision, explorer.Source{
			URI:       summary.SourceURI,
			Title:     summary.Document.Title,
			MediaType: summary.SourceMediaType,
		}, now)
		if err != nil {
			return 0, err
		}
		registration, err := c.existingRevisionRegistration(ctx, summary, rule)
		if err != nil {
			return 0, err
		}
		if err := c.policyStore.PutRevision(ctx, registration); err != nil {
			return 0, policyCatalogWriteError(ctx, err)
		}
		registered++
	}
	if err := guard.Check(ctx); err != nil {
		return 0, err
	}
	c.invalidateAuthorizedVectorAvailability()
	return registered, nil
}

// existingRevisionRegistration derives the catalog record for a base document
// using the same view verification, node identity, digest, and intrinsic edge
// derivation that Ingest performs, so a backfilled registration is
// indistinguishable from one written at ingest time.
func (c *Client) existingRevisionRegistration(
	ctx context.Context,
	summary explorer.DocumentSummary,
	rule AccessRule,
) (RevisionRegistration, error) {
	view, err := c.base.Document(
		ctx, summary.Document.ID, summary.Revision.ID)
	if err != nil {
		return RevisionRegistration{}, err
	}
	if view.Document.ID != summary.Document.ID ||
		view.Document.RevisionID != summary.Revision.ID ||
		view.Revision.ID != summary.Revision.ID ||
		view.Revision.DocumentID != summary.Document.ID {
		return RevisionRegistration{}, inconsistentBase()
	}
	nodeIDs, err := documentViewNodeIDs(view)
	if err != nil {
		return RevisionRegistration{}, inconsistentBase()
	}
	digest, err := documentViewDigest(view)
	if err != nil {
		return RevisionRegistration{}, inconsistentBase()
	}
	intrinsicEdges, err := c.intrinsicEdges(ctx, nodeIDs)
	if err != nil {
		return RevisionRegistration{}, err
	}
	return RevisionRegistration{
		DocumentID:     summary.Document.ID,
		RevisionID:     summary.Revision.ID,
		NodeIDs:        nodeIDs,
		IntrinsicEdges: intrinsicEdges,
		ContentDigest:  digest,
		Rule:           rule,
		Current:        true,
	}, nil
}
