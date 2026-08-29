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

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Retrieve always supplies an authorized current-document projection to the
// base scorer, then validates every returned citation and path.
func (c *Client) Retrieve(
	ctx context.Context,
	request retrieval.Request,
) (retrieval.Response, error) {
	normalized, err := request.Normalize()
	if err != nil {
		return retrieval.Response{}, err
	}
	if err := normalized.ValidateSeedPlan(true); err != nil {
		return retrieval.Response{}, err
	}
	if normalized.HasMode(retrieval.ModeVector) && !c.authorizedVectorScoringAvailable() {
		return retrieval.Response{}, shoal.NewError(
			shoal.ErrorUnavailable,
			"authorized vector retrieval requires trusted vector validation",
		)
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRetrieve)
	if err != nil {
		return retrieval.Response{}, err
	}
	summaries, err := c.base.Documents(ctx)
	if err != nil {
		return retrieval.Response{}, err
	}
	visibleOrder := make([]shoal.ID, 0, len(summaries))
	visible := make(map[shoal.ID]RevisionRegistration, len(summaries))
	for _, summary := range summaries {
		if err := validateSummary(summary); err != nil {
			return retrieval.Response{}, inconsistentBase()
		}
		registration, ok, err := c.policyStore.CurrentRevision(
			ctx, summary.Document.ID)
		if err != nil {
			return retrieval.Response{}, policyCatalogReadError(ctx, err)
		}
		if !ok || registration.RevisionID != summary.Revision.ID {
			continue
		}
		allowed, err := ruleAllows(
			registration.Rule, decision, auth.OperationRetrieve, now)
		if err != nil {
			return retrieval.Response{}, err
		}
		if !allowed {
			continue
		}
		if _, duplicate := visible[summary.Document.ID]; duplicate {
			return retrieval.Response{}, inconsistentBase()
		}
		visible[summary.Document.ID] = registration
		visibleOrder = append(visibleOrder, summary.Document.ID)
	}

	documentIDs := visibleOrder
	if len(normalized.Scope.DocumentIDs) > 0 {
		documentIDs = make([]shoal.ID, 0, len(normalized.Scope.DocumentIDs))
		for _, documentID := range normalized.Scope.DocumentIDs {
			if _, ok := visible[documentID]; ok {
				documentIDs = append(documentIDs, documentID)
			}
		}
		if len(documentIDs) == 0 {
			if err := c.probeAuthorizedVector(ctx, normalized); err != nil {
				return retrieval.Response{}, err
			}
			return c.emptyRetrieval(ctx, guard, normalized)
		}
	}
	if len(documentIDs) == 0 {
		if err := c.probeAuthorizedVector(ctx, normalized); err != nil {
			return retrieval.Response{}, err
		}
		return c.emptyRetrieval(ctx, guard, normalized)
	}
	selected := make(map[shoal.ID]RevisionRegistration, len(documentIDs))
	selectedNodes := make(map[shoal.ID]NodeRegistration)
	for _, documentID := range documentIDs {
		registration := visible[documentID]
		selected[documentID] = registration
		for _, nodeID := range registration.NodeIDs {
			selectedNodes[nodeID] = NodeRegistration{
				DocumentID: documentID,
				RevisionID: registration.RevisionID,
				Rule:       registration.Rule,
			}
		}
	}

	var nodeIDs []shoal.ID
	if len(normalized.Scope.NodeIDs) > 0 {
		nodeIDs = make([]shoal.ID, 0, len(normalized.Scope.NodeIDs))
		for _, nodeID := range normalized.Scope.NodeIDs {
			if _, ok := selectedNodes[nodeID]; ok {
				nodeIDs = append(nodeIDs, nodeID)
			}
		}
		if len(nodeIDs) == 0 {
			if err := c.probeAuthorizedVector(ctx, normalized); err != nil {
				return retrieval.Response{}, err
			}
			return c.emptyRetrieval(ctx, guard, normalized)
		}
	}
	if len(documentIDs)+len(nodeIDs) > retrieval.MaxScopeIDs {
		return retrieval.Response{}, shoal.NewError(
			shoal.ErrorUnavailable,
			"authorized retrieval projection exceeds the public bound",
		)
	}
	projected := normalized
	projected.Scope = retrieval.Scope{
		DocumentIDs: append([]shoal.ID(nil), documentIDs...),
		NodeIDs:     append([]shoal.ID(nil), nodeIDs...),
	}
	corpus, err := c.hydrateRetrievalCorpus(
		ctx, documentIDs, selected, decision, now)
	if err != nil {
		return retrieval.Response{}, err
	}
	response, err := c.base.Retrieve(ctx, projected)
	if err != nil {
		return retrieval.Response{}, err
	}
	if err := response.ValidateFor(projected); err != nil {
		return retrieval.Response{}, inconsistentRetrieval()
	}
	if err := c.validateRetrievedResponse(
		ctx, response, projected, corpus, decision, now,
	); err != nil {
		return retrieval.Response{}, err
	}
	cloned := cloneRetrievalResponse(response)
	cloned.RequestID = ""
	if err := guard.Check(ctx); err != nil {
		return retrieval.Response{}, err
	}
	return cloned, nil
}

func (c *Client) emptyRetrieval(
	ctx context.Context,
	guard auth.GenerationGuard,
	request retrieval.Request,
) (retrieval.Response, error) {
	response := retrieval.Response{}
	if err := response.ValidateFor(request); err != nil {
		return retrieval.Response{}, inconsistentRetrieval()
	}
	if err := guard.Check(ctx); err != nil {
		return retrieval.Response{}, err
	}
	return response, nil
}

func inconsistentRetrieval() error {
	return shoal.NewError(
		shoal.ErrorInternal,
		"underlying Explorer returned unauthorized retrieval data",
	)
}
