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

// Connect authorizes both current endpoints and stores only the trusted
// edge-local policy. Endpoint rules are re-evaluated dynamically on every use.
func (c *Client) Connect(ctx context.Context, edge graph.Edge) error {
	if err := edge.Validate(); err != nil {
		return err
	}
	ownedEdge := cloneGraphEdge(edge)
	decision, guard, now, err := c.begin(ctx, auth.OperationConnect)
	if err != nil {
		return err
	}

	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	lease, err := c.policyStore.AcquireMutation(ctx)
	if err != nil {
		return policyCatalogWriteError(ctx, err)
	}
	defer lease.Release()
	if err := guard.Check(ctx); err != nil {
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
	if !rulesShareDomain(edgeRule, from.Rule) ||
		!rulesShareDomain(edgeRule, to.Rule) {
		return authorizationDenied()
	}
	if err := edgeRule.Authorize(
		decision, auth.OperationConnect, now); err != nil {
		return authorizationDenied()
	}
	if err := guard.Check(ctx); err != nil {
		return err
	}
	registration := EdgeRegistration{Edge: ownedEdge, Rule: edgeRule}
	if err := c.policyStore.ReserveEdge(ctx, registration); err != nil {
		return policyCatalogWriteError(ctx, err)
	}
	if err := c.base.Connect(ctx, cloneGraphEdge(ownedEdge)); err != nil {
		if !explorer.IsIndeterminateCommit(err) {
			if rollbackErr := c.policyStore.RollbackEdgeReservation(
				context.WithoutCancel(ctx), registration,
			); rollbackErr != nil {
				return policyCatalogWriteError(ctx, rollbackErr)
			}
		}
		return directBaseError(err)
	}
	if err := c.policyStore.PutEdge(ctx, registration); err != nil {
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

	candidates := make(map[shoal.ID]graph.Node, len(raw.Nodes))
	registrations := make(map[shoal.ID]NodeRegistration, len(raw.Nodes))
	rawNodeIDs := make([]shoal.ID, 0, len(raw.Nodes))
	for _, node := range raw.Nodes {
		if err := node.Validate(); err != nil {
			return explorer.Neighborhood{}, inconsistentBase()
		}
		rawNodeIDs = append(rawNodeIDs, node.ID)
	}
	resolved, err := c.resolveNodes(ctx, rawNodeIDs)
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	for _, node := range raw.Nodes {
		registration, ok := resolved[node.ID]
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
		if _, duplicate := candidates[node.ID]; duplicate {
			return explorer.Neighborhood{}, inconsistentBase()
		}
		candidates[node.ID] = cloneGraphNode(node)
		registrations[node.ID] = registration
	}
	canonicalNodes, err := c.canonicalRegisteredNodes(ctx, registrations)
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	visibleNodes := make(map[shoal.ID]graph.Node, len(candidates))
	for nodeID, node := range candidates {
		canonical, ok := canonicalNodes[nodeID]
		if !ok || !graphNodesEqual(canonical, node) {
			return explorer.Neighborhood{}, inconsistentBase()
		}
		visibleNodes[nodeID] = node
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
	candidateEdges := make([]graph.Edge, 0, len(raw.Edges))
	candidateEdgeIDs := make([]shoal.ID, 0, len(raw.Edges))
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
		candidateEdges = append(candidateEdges, edge)
		candidateEdgeIDs = append(candidateEdgeIDs, edge.ID)
	}
	resolvedEdges, err := c.resolveEdges(ctx, candidateEdgeIDs)
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	for _, edge := range candidateEdges {
		registration, ok := resolvedEdges[edge.ID]
		if !ok || !graphEdgesEqual(registration.Edge, edge) {
			continue
		}
		allowed, err := edgeAllowsResolved(
			resolved, registration, decision, auth.OperationNeighborhood, now)
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

func (c *Client) edgeAllows(
	ctx context.Context,
	registration EdgeRegistration,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (bool, error) {
	allowed, err := ruleAllows(registration.Rule, decision, operation, now)
	if err != nil || !allowed {
		return allowed, err
	}
	resolved, err := c.resolveNodes(
		ctx, []shoal.ID{registration.Edge.From, registration.Edge.To})
	if err != nil {
		return false, err
	}
	return edgeEndpointsAllow(resolved, registration, decision, operation, now)
}

// registeredNodes holds the node registrations resolved for one page in a
// single batch lookup. Membership is the only presence signal, exactly as the
// boolean returned by PolicyStore.Node is: an identifier that is absent was not
// registered and therefore denies. A partial or empty batch result consequently
// authorizes nothing.
type registeredNodes map[shoal.ID]NodeRegistration

// resolveNodes collapses every identifier a page needs into one policy-store
// round trip, deduplicating first so the round trip carries distinct nodes
// rather than one entry per edge endpoint. Identifiers the store does not know
// are omitted from the result, preserving the fail-closed path a point lookup
// takes when it reports !ok. Registrations for identifiers that were not
// requested are discarded so a misbehaving store cannot widen a page.
func (c *Client) resolveNodes(
	ctx context.Context,
	nodeIDs []shoal.ID,
) (registeredNodes, error) {
	distinct := make([]shoal.ID, 0, len(nodeIDs))
	requested := make(map[shoal.ID]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if _, duplicate := requested[nodeID]; duplicate {
			continue
		}
		requested[nodeID] = struct{}{}
		distinct = append(distinct, nodeID)
	}
	if len(distinct) == 0 {
		return registeredNodes{}, nil
	}
	batch, err := c.policyStore.Nodes(ctx, distinct)
	if err != nil {
		return nil, policyCatalogReadError(ctx, err)
	}
	resolved := make(registeredNodes, len(distinct))
	for _, nodeID := range distinct {
		registration, ok := batch[nodeID]
		if !ok {
			continue
		}
		resolved[nodeID] = registration
	}
	return resolved, nil
}

// registeredEdges holds the edge registrations resolved for one page in a
// single batch lookup, under the same rule as registeredNodes: membership is
// the only presence signal, so an identifier that is absent was not registered
// and denies.
type registeredEdges map[shoal.ID]EdgeRegistration

// resolveEdges collapses a page's candidate edges into one policy-store round
// trip under the same discipline as resolveNodes: deduplicate first, skip the
// round trip entirely when there is nothing to resolve, treat omission from the
// result as the fail-closed !ok path, and discard registrations for identifiers
// that were not requested.
func (c *Client) resolveEdges(
	ctx context.Context,
	edgeIDs []shoal.ID,
) (registeredEdges, error) {
	distinct := make([]shoal.ID, 0, len(edgeIDs))
	requested := make(map[shoal.ID]struct{}, len(edgeIDs))
	for _, edgeID := range edgeIDs {
		if _, duplicate := requested[edgeID]; duplicate {
			continue
		}
		requested[edgeID] = struct{}{}
		distinct = append(distinct, edgeID)
	}
	if len(distinct) == 0 {
		return registeredEdges{}, nil
	}
	batch, err := c.policyStore.Edges(ctx, distinct)
	if err != nil {
		return nil, policyCatalogReadError(ctx, err)
	}
	resolved := make(registeredEdges, len(distinct))
	for _, edgeID := range distinct {
		registration, ok := batch[edgeID]
		if !ok {
			continue
		}
		resolved[edgeID] = registration
	}
	return resolved, nil
}

// edgeAllowsResolved authorizes one edge against endpoint registrations that
// were already batched for the page, issuing no further policy-store round
// trips.
func edgeAllowsResolved(
	resolved registeredNodes,
	registration EdgeRegistration,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (bool, error) {
	allowed, err := ruleAllows(registration.Rule, decision, operation, now)
	if err != nil || !allowed {
		return allowed, err
	}
	return edgeEndpointsAllow(resolved, registration, decision, operation, now)
}

// edgeEndpointsAllow requires both endpoints to be present in the resolved page
// and allowed. An endpoint missing from resolved denies, exactly as a point
// lookup reporting !ok denies.
func edgeEndpointsAllow(
	resolved registeredNodes,
	registration EdgeRegistration,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (bool, error) {
	from, ok := resolved[registration.Edge.From]
	if !ok {
		return false, nil
	}
	to, ok := resolved[registration.Edge.To]
	if !ok {
		return false, nil
	}
	fromAllowed, err := ruleAllows(from.Rule, decision, operation, now)
	if err != nil || !fromAllowed {
		return fromAllowed, err
	}
	toAllowed, err := ruleAllows(to.Rule, decision, operation, now)
	if err != nil || !toAllowed {
		return toAllowed, err
	}
	return true, nil
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
	if _, err := c.canonicalRegisteredNodes(
		ctx, map[shoal.ID]NodeRegistration{nodeID: registration},
	); err != nil {
		return NodeRegistration{}, err
	}
	return registration, nil
}

func (c *Client) canonicalRegisteredNodes(
	ctx context.Context,
	registrations map[shoal.ID]NodeRegistration,
) (map[shoal.ID]graph.Node, error) {
	summaries, err := c.base.Documents(ctx)
	if err != nil {
		return nil, err
	}
	currentBase := make(map[shoal.ID]shoal.ID, len(summaries))
	for _, summary := range summaries {
		if err := validateSummary(summary); err != nil {
			return nil, inconsistentBase()
		}
		if _, duplicate := currentBase[summary.Document.ID]; duplicate {
			return nil, inconsistentBase()
		}
		currentBase[summary.Document.ID] = summary.Revision.ID
	}
	required := make(map[shoal.ID]shoal.ID)
	for _, registration := range registrations {
		if revisionID, ok := required[registration.DocumentID]; ok &&
			revisionID != registration.RevisionID {
			return nil, inconsistentBase()
		}
		required[registration.DocumentID] = registration.RevisionID
	}
	canonicalDocuments := make(
		map[shoal.ID]*canonicalRetrievalDocument, len(required))
	for documentID, revisionID := range required {
		if currentBase[documentID] != revisionID {
			return nil, auth.ObjectNotFound()
		}
		current, ok, err := c.policyStore.CurrentRevision(ctx, documentID)
		if err != nil {
			return nil, policyCatalogReadError(ctx, err)
		}
		if !ok || current.RevisionID != revisionID {
			return nil, auth.ObjectNotFound()
		}
		view, err := c.base.Document(ctx, documentID, revisionID)
		if err != nil {
			return nil, directBaseError(err)
		}
		if err := verifyDocumentViewRegistration(view, current); err != nil {
			return nil, err
		}
		canonical, err := buildCanonicalRetrievalDocument(view, current)
		if err != nil {
			return nil, inconsistentBase()
		}
		canonicalDocuments[documentID] = canonical
	}
	nodes := make(map[shoal.ID]graph.Node, len(registrations))
	for nodeID, registration := range registrations {
		canonical := canonicalDocuments[registration.DocumentID]
		node, ok := canonical.nodes[nodeID]
		if !ok {
			return nil, inconsistentBase()
		}
		nodes[nodeID] = cloneGraphNode(node)
	}
	return nodes, nil
}
