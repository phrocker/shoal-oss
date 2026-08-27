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
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Connect authorizes both current endpoints and stores the conjunction of the
// trusted edge policy and both endpoint rules.
func (c *Client) Connect(ctx context.Context, edge graph.Edge) error {
	if err := edge.Validate(); err != nil {
		return err
	}
	ownedEdge := cloneGraphEdge(edge)
	decision, guard, now, err := c.begin(ctx, auth.OperationConnect)
	if err != nil {
		return err
	}
	from, err := c.authorizedNode(
		ctx, ownedEdge.From, decision, auth.OperationConnect, now)
	if err != nil {
		return err
	}
	to, err := c.authorizedNode(
		ctx, ownedEdge.To, decision, auth.OperationConnect, now)
	if err != nil {
		return err
	}
	if !rulesShareDomain(from.Rule, to.Rule) {
		return auth.ObjectNotFound()
	}
	edgeRule, err := c.selectEdgeRule(
		ctx, decision, cloneGraphEdge(ownedEdge), now)
	if err != nil {
		return err
	}
	components := edgeRule.components()
	components = append(components, from.Rule.components()...)
	components = append(components, to.Rule.components()...)
	effective, err := NewAccessRule(components...)
	if err != nil {
		return authorizationDenied()
	}
	if err := effective.Authorize(
		decision, auth.OperationConnect, now); err != nil {
		return authorizationDenied()
	}

	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if err := c.base.Connect(ctx, cloneGraphEdge(ownedEdge)); err != nil {
		return directBaseError(err)
	}
	if err := c.policyStore.PutEdge(ctx, EdgeRegistration{
		Edge: ownedEdge,
		Rule: effective,
	}); err != nil {
		return policyCatalogWriteError(ctx, err)
	}
	return guard.Check(ctx)
}

// Neighborhood filters by current node and edge rules, then recomputes
// reachability so hidden intermediates cannot disclose or bridge.
func (c *Client) Neighborhood(
	ctx context.Context,
	request explorer.NeighborhoodRequest,
) (explorer.Neighborhood, error) {
	normalized, err := request.Normalize()
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationNeighborhood)
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	for _, nodeID := range normalized.NodeIDs {
		if _, err := c.authorizedNode(
			ctx, nodeID, decision, auth.OperationNeighborhood, now); err != nil {
			return explorer.Neighborhood{}, err
		}
	}
	raw, err := c.base.Neighborhood(ctx, normalized)
	if err != nil {
		return explorer.Neighborhood{}, directBaseError(err)
	}

	visibleNodes := make(map[shoal.ID]graph.Node, len(raw.Nodes))
	for _, node := range raw.Nodes {
		if err := node.Validate(); err != nil {
			return explorer.Neighborhood{}, inconsistentBase()
		}
		registration, ok, err := c.policyStore.Node(ctx, node.ID)
		if err != nil {
			return explorer.Neighborhood{}, policyCatalogReadError(ctx, err)
		}
		if !ok {
			continue
		}
		allowed, err := ruleAllows(
			registration.Rule, decision, auth.OperationNeighborhood, now)
		if err != nil {
			return explorer.Neighborhood{}, err
		}
		if !allowed {
			continue
		}
		if _, duplicate := visibleNodes[node.ID]; duplicate {
			return explorer.Neighborhood{}, inconsistentBase()
		}
		visibleNodes[node.ID] = cloneGraphNode(node)
	}
	for _, seed := range normalized.NodeIDs {
		if _, ok := visibleNodes[seed]; !ok {
			return explorer.Neighborhood{}, inconsistentBase()
		}
	}

	typeFilter := make(map[string]struct{}, len(normalized.EdgeTypes))
	for _, edgeType := range normalized.EdgeTypes {
		typeFilter[edgeType] = struct{}{}
	}
	admittedEdges := make(map[shoal.ID]graph.Edge, len(raw.Edges))
	for _, edge := range raw.Edges {
		if err := edge.Validate(); err != nil {
			return explorer.Neighborhood{}, inconsistentBase()
		}
		if len(typeFilter) > 0 {
			if _, ok := typeFilter[edge.Type]; !ok {
				continue
			}
		}
		if _, ok := visibleNodes[edge.From]; !ok {
			continue
		}
		if _, ok := visibleNodes[edge.To]; !ok {
			continue
		}
		registration, ok, err := c.policyStore.Edge(ctx, edge.ID)
		if err != nil {
			return explorer.Neighborhood{}, policyCatalogReadError(ctx, err)
		}
		if !ok || !graphEdgesEqual(registration.Edge, edge) {
			continue
		}
		allowed, err := ruleAllows(
			registration.Rule, decision, auth.OperationNeighborhood, now)
		if err != nil {
			return explorer.Neighborhood{}, err
		}
		if !allowed {
			continue
		}
		if _, duplicate := admittedEdges[edge.ID]; duplicate {
			return explorer.Neighborhood{}, inconsistentBase()
		}
		admittedEdges[edge.ID] = cloneGraphEdge(edge)
	}

	reachable := make(map[shoal.ID]struct{}, len(normalized.NodeIDs))
	frontier := make(map[shoal.ID]struct{}, len(normalized.NodeIDs))
	for _, seed := range normalized.NodeIDs {
		reachable[seed] = struct{}{}
		frontier[seed] = struct{}{}
	}
	selectedEdges := make(map[shoal.ID]graph.Edge)
	for depth := uint32(0); depth < normalized.Depth && len(frontier) > 0; depth++ {
		next := make(map[shoal.ID]struct{})
		for edgeID, edge := range admittedEdges {
			_, from := frontier[edge.From]
			_, to := frontier[edge.To]
			if !from && !to {
				continue
			}
			selectedEdges[edgeID] = edge
			for _, nodeID := range []shoal.ID{edge.From, edge.To} {
				if _, seen := reachable[nodeID]; seen {
					continue
				}
				reachable[nodeID] = struct{}{}
				next[nodeID] = struct{}{}
			}
		}
		frontier = next
	}

	result := explorer.Neighborhood{
		Nodes: make([]graph.Node, 0, len(reachable)),
		Edges: make([]graph.Edge, 0, len(selectedEdges)),
	}
	for nodeID := range reachable {
		result.Nodes = append(result.Nodes, cloneGraphNode(visibleNodes[nodeID]))
	}
	for _, edge := range selectedEdges {
		result.Edges = append(result.Edges, cloneGraphEdge(edge))
	}
	sort.Slice(result.Nodes, func(left, right int) bool {
		return shoal.CompareID(result.Nodes[left].ID, result.Nodes[right].ID) < 0
	})
	sort.Slice(result.Edges, func(left, right int) bool {
		return shoal.CompareID(result.Edges[left].ID, result.Edges[right].ID) < 0
	})
	if err := guard.Check(ctx); err != nil {
		return explorer.Neighborhood{}, err
	}
	return result, nil
}

func (c *Client) authorizedNode(
	ctx context.Context,
	nodeID shoal.ID,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (NodeRegistration, error) {
	registration, ok, err := c.policyStore.Node(ctx, nodeID)
	if err != nil {
		return NodeRegistration{}, policyCatalogReadError(ctx, err)
	}
	if !ok {
		return NodeRegistration{}, auth.ObjectNotFound()
	}
	allowed, err := ruleAllows(registration.Rule, decision, operation, now)
	if err != nil {
		return NodeRegistration{}, err
	}
	if !allowed {
		return NodeRegistration{}, auth.ObjectNotFound()
	}
	return registration, nil
}
