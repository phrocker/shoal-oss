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
			return c.emptyRetrieval(ctx, guard, normalized)
		}
	}
	if len(documentIDs) == 0 {
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
	response, err := c.base.Retrieve(ctx, projected)
	if err != nil {
		return retrieval.Response{}, err
	}
	if err := response.ValidateFor(projected); err != nil {
		return retrieval.Response{}, inconsistentRetrieval()
	}
	if err := c.validateRetrievedResponse(
		ctx, response, selected, selectedNodes, decision, now,
	); err != nil {
		return retrieval.Response{}, err
	}
	cloned := cloneRetrievalResponse(response)
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

func (c *Client) validateRetrievedResponse(
	ctx context.Context,
	response retrieval.Response,
	documents map[shoal.ID]RevisionRegistration,
	nodes map[shoal.ID]NodeRegistration,
	decision auth.Decision,
	now time.Time,
) error {
	for _, result := range response.Results {
		if len(result.Evidence) == 0 {
			return inconsistentRetrieval()
		}
		if err := c.validateRetrievedNode(
			ctx, result.ID, nodes, decision, now); err != nil {
			return err
		}
		for _, evidence := range result.Evidence {
			citation := evidence.Citation
			registration, ok := documents[citation.DocumentID]
			if !ok || registration.RevisionID != citation.RevisionID {
				return inconsistentRetrieval()
			}
			if citation.SectionID != "" {
				if err := c.validateRetrievedNode(
					ctx, citation.SectionID, nodes, decision, now); err != nil {
					return err
				}
				node := nodes[citation.SectionID]
				if node.DocumentID != citation.DocumentID ||
					node.RevisionID != citation.RevisionID {
					return inconsistentRetrieval()
				}
			}
			if citation.SpanID != "" {
				if err := c.validateRetrievedNode(
					ctx, citation.SpanID, nodes, decision, now); err != nil {
					return err
				}
				node := nodes[citation.SpanID]
				if node.DocumentID != citation.DocumentID ||
					node.RevisionID != citation.RevisionID {
					return inconsistentRetrieval()
				}
			}
			for _, node := range evidence.Path.Nodes {
				if err := c.validateRetrievedNode(
					ctx, node.ID, nodes, decision, now); err != nil {
					return err
				}
			}
			for _, edge := range evidence.Path.Edges {
				registration, ok, err := c.policyStore.Edge(ctx, edge.ID)
				if err != nil {
					return policyCatalogReadError(ctx, err)
				}
				if !ok || !graphEdgesEqual(registration.Edge, edge) {
					return inconsistentRetrieval()
				}
				allowed, err := ruleAllows(
					registration.Rule, decision, auth.OperationRetrieve, now)
				if err != nil {
					return err
				}
				if !allowed {
					return inconsistentRetrieval()
				}
			}
		}
	}
	return nil
}

func (c *Client) validateRetrievedNode(
	ctx context.Context,
	nodeID shoal.ID,
	expected map[shoal.ID]NodeRegistration,
	decision auth.Decision,
	now time.Time,
) error {
	expectedRegistration, ok := expected[nodeID]
	if !ok {
		return inconsistentRetrieval()
	}
	current, ok, err := c.policyStore.Node(ctx, nodeID)
	if err != nil {
		return policyCatalogReadError(ctx, err)
	}
	if !ok || current.DocumentID != expectedRegistration.DocumentID ||
		current.RevisionID != expectedRegistration.RevisionID {
		return inconsistentRetrieval()
	}
	allowed, err := ruleAllows(
		current.Rule, decision, auth.OperationRetrieve, now)
	if err != nil {
		return err
	}
	if !allowed {
		return inconsistentRetrieval()
	}
	return nil
}

func inconsistentRetrieval() error {
	return shoal.NewError(
		shoal.ErrorInternal,
		"underlying Explorer returned unauthorized retrieval data",
	)
}
