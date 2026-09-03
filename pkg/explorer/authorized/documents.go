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
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Documents lists only exact current revisions whose catalog rules authorize
// the resolved collection request. Base order is retained.
func (c *Client) Documents(
	ctx context.Context,
) ([]explorer.DocumentSummary, error) {
	var suppressed uint32
	return c.documents(ctx, &suppressed)
}

// DocumentsWithSuppressed lists the same authorized documents as Documents and
// additionally reports how many current documents this identity was denied and
// therefore never listed. The count comes from the exact same authorization
// gate Documents enforces; it is reporting only and never changes which
// summaries are returned. See the counting site in documents and the webapi
// emission point for the amplification risk this disclosure carries.
func (c *Client) DocumentsWithSuppressed(
	ctx context.Context,
) ([]explorer.DocumentSummary, uint32, error) {
	var suppressed uint32
	summaries, err := c.documents(ctx, &suppressed)
	if err != nil {
		return nil, 0, err
	}
	return summaries, suppressed, nil
}

func (c *Client) documents(
	ctx context.Context,
	suppressed *uint32,
) ([]explorer.DocumentSummary, error) {
	decision, guard, now, err := c.begin(ctx, auth.OperationList)
	if err != nil {
		return nil, err
	}
	summaries, err := c.base.Documents(ctx)
	if err != nil {
		return nil, err
	}
	visible := make([]explorer.DocumentSummary, 0, len(summaries))
	for _, summary := range summaries {
		if err := validateSummary(summary); err != nil {
			return nil, inconsistentBase()
		}
		registration, ok, err := c.policyStore.CurrentRevision(
			ctx, summary.Document.ID)
		if err != nil {
			return nil, policyCatalogReadError(ctx, err)
		}
		if !ok {
			// A document present in the corpus but covered by no policy grant
			// is withheld from every caller: that is an authorization outcome,
			// so it is counted. This is deliberately asymmetric with the
			// stale-revision drop just below, which is an availability lag, not
			// a policy decision about this caller, and is not counted. Counting
			// !ok is also the signal that reveals a lost or empty policy
			// catalog, where the corpus is intact but every document falls
			// through here; without it a fully withheld corpus would read as
			// "nothing withheld". The record is still dropped exactly as before.
			*suppressed++
			continue
		}
		if registration.RevisionID != summary.Revision.ID {
			continue
		}
		allowed, err := ruleAllows(
			registration.Rule, decision, auth.OperationList, now)
		if err != nil {
			return nil, err
		}
		if !allowed {
			// Accounting beside the enforcement branch, not within it: the
			// record is still dropped exactly as before. Counting a document
			// the identity's rule denies is unambiguous authorization
			// suppression, alongside the missing-grant case counted above.
			*suppressed++
			continue
		}
		view, err := c.base.Document(
			ctx, registration.DocumentID, registration.RevisionID)
		if err != nil {
			return nil, directBaseError(err)
		}
		legacyDigest, err := verifyDocumentViewRegistrationMode(view, registration)
		if err != nil {
			return nil, err
		}
		sourceMediaType := view.SourceMediaType
		if legacyDigest {
			sourceMediaType = ""
		}
		visible = append(visible, explorer.DocumentSummary{
			Document:        cloneDocument(view.Document),
			Revision:        cloneRevision(view.Revision),
			SourceURI:       view.SourceURI,
			SourceMediaType: sourceMediaType,
		})
	}
	if err := guard.Check(ctx); err != nil {
		return nil, err
	}
	return visible, nil
}

// Document returns an authorized exact revision. Hidden and absent direct
// objects have the same non-disclosing error shape.
func (c *Client) Document(
	ctx context.Context,
	documentID, revisionID shoal.ID,
) (explorer.DocumentView, error) {
	if err := shoal.ValidateRequiredID("document ID", documentID); err != nil {
		return explorer.DocumentView{}, err
	}
	if err := shoal.ValidateOptionalID("revision ID", revisionID); err != nil {
		return explorer.DocumentView{}, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return explorer.DocumentView{}, err
	}

	var registration RevisionRegistration
	var ok bool
	if revisionID == "" {
		registration, ok, err = c.policyStore.CurrentRevision(ctx, documentID)
		if err != nil {
			return explorer.DocumentView{}, policyCatalogReadError(ctx, err)
		}
		if !ok {
			return explorer.DocumentView{}, auth.ObjectNotFound()
		}
	} else {
		registration, ok, err = c.policyStore.Revision(
			ctx, documentID, revisionID)
		if err != nil {
			return explorer.DocumentView{}, policyCatalogReadError(ctx, err)
		}
		if !ok {
			return explorer.DocumentView{}, auth.ObjectNotFound()
		}
	}
	allowed, err := ruleAllows(
		registration.Rule, decision, auth.OperationRead, now)
	if err != nil {
		return explorer.DocumentView{}, err
	}
	if !allowed {
		return explorer.DocumentView{}, auth.ObjectNotFound()
	}
	view, err := c.base.Document(ctx, documentID, registration.RevisionID)
	if err != nil {
		return explorer.DocumentView{}, directBaseError(err)
	}
	if view.Document.ID != documentID ||
		view.Document.RevisionID != registration.RevisionID ||
		view.Revision.ID != registration.RevisionID ||
		view.Revision.DocumentID != documentID {
		return explorer.DocumentView{}, inconsistentBase()
	}
	legacyDigest, err := verifyDocumentViewRegistrationMode(view, registration)
	if err != nil {
		return explorer.DocumentView{}, err
	}
	cloned := cloneDocumentView(view)
	if legacyDigest {
		cloned.SourceMediaType = ""
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.DocumentView{}, err
	}
	return cloned, nil
}

func validateSummary(summary explorer.DocumentSummary) error {
	if err := summary.Document.Validate(); err != nil {
		return err
	}
	if err := summary.Revision.Validate(); err != nil {
		return err
	}
	if summary.Document.RevisionID != summary.Revision.ID ||
		summary.Revision.DocumentID != summary.Document.ID {
		return inconsistentBase()
	}
	return nil
}

func verifyDocumentViewRegistration(
	view explorer.DocumentView,
	registration RevisionRegistration,
) error {
	_, err := verifyDocumentViewRegistrationMode(view, registration)
	return err
}

func verifyDocumentViewRegistrationMode(
	view explorer.DocumentView,
	registration RevisionRegistration,
) (bool, error) {
	legacyDigest := false
	digest, err := documentViewDigest(view)
	if err != nil {
		return false, inconsistentBase()
	}
	if digest != registration.ContentDigest {
		legacy, legacyErr := legacyDocumentViewDigestV1(view)
		if legacyErr != nil || legacy != registration.ContentDigest {
			return false, inconsistentBase()
		}
		legacyDigest = true
	}
	nodeIDs, err := documentViewNodeIDs(view)
	if err != nil || len(nodeIDs) != len(registration.NodeIDs) {
		return false, inconsistentBase()
	}
	for index := range nodeIDs {
		if nodeIDs[index] != registration.NodeIDs[index] {
			return false, inconsistentBase()
		}
	}
	return legacyDigest, nil
}

func ruleAllows(
	rule AccessRule,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (bool, error) {
	err := rule.Authorize(decision, operation, now)
	if err == nil {
		return true, nil
	}
	if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		return false, nil
	}
	return false, inconsistentBase()
}

func directBaseError(err error) error {
	if shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		return auth.ObjectNotFound()
	}
	return err
}
