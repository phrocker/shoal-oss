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
	"strconv"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const vectorCapabilityProbeText = "shoal vector capability probe"
const graphAssertionEdgeIDMetadata = "shoal.graph.edge_id"

func (c *Client) Snapshot(ctx context.Context) (explorer.Snapshot, error) {
	bounded, err := c.boundedBase()
	if err != nil {
		return explorer.Snapshot{}, err
	}
	_, guard, _, err := c.begin(ctx, auth.OperationRetrieve)
	if err != nil {
		return explorer.Snapshot{}, err
	}
	snapshot, err := bounded.Snapshot(ctx)
	if err != nil {
		return explorer.Snapshot{}, directBaseError(err)
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.Snapshot{}, err
	}
	return snapshot, nil
}

func (c *Client) BoundedAvailable() bool {
	_, ok := c.base.(explorer.BoundedClient)
	return ok
}

func (c *Client) VectorAvailable(ctx context.Context) (bool, error) {
	if !c.authorizedVectorScoringAvailable() {
		return false, nil
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRetrieve)
	if err != nil {
		return false, err
	}
	summaries, err := c.base.Documents(ctx)
	if err != nil {
		return false, directBaseError(err)
	}
	visibleOrder := make([]shoal.ID, 0, len(summaries))
	visible := make(map[shoal.ID]RevisionRegistration, len(summaries))
	for _, summary := range summaries {
		if err := validateSummary(summary); err != nil {
			return false, inconsistentBase()
		}
		registration, ok, err := c.policyStore.CurrentRevision(
			ctx, summary.Document.ID)
		if err != nil {
			return false, policyCatalogReadError(ctx, err)
		}
		if !ok || registration.RevisionID != summary.Revision.ID {
			continue
		}
		allowed, err := ruleAllows(
			registration.Rule, decision, auth.OperationRetrieve, now)
		if err != nil {
			return false, err
		}
		if !allowed {
			continue
		}
		if _, duplicate := visible[summary.Document.ID]; duplicate {
			return false, inconsistentBase()
		}
		visible[summary.Document.ID] = registration
		visibleOrder = append(visibleOrder, summary.Document.ID)
	}
	cacheKey := authorizedVectorAvailabilityKey(decision, visibleOrder, visible)
	if available, ok := c.cachedAuthorizedVectorAvailability(cacheKey, now); ok {
		if err := guard.Check(ctx); err != nil {
			return false, err
		}
		return available, nil
	}
	if len(visibleOrder) > retrieval.MaxScopeIDs {
		if err := guard.Check(ctx); err != nil {
			return false, err
		}
		c.cacheAuthorizedVectorAvailability(cacheKey, false, now)
		return false, nil
	}
	corpus, err := c.hydrateRetrievalCorpus(ctx, visibleOrder, visible, decision, now)
	if err != nil {
		return false, err
	}
	available := true
	if len(visibleOrder) == 0 {
		available = false
	} else {
		probe := retrieval.Request{
			Text:  vectorCapabilityProbeText,
			Modes: []retrieval.Mode{retrieval.ModeVector},
			Scope: retrieval.Scope{
				DocumentIDs: append([]shoal.ID(nil), visibleOrder...),
			},
		}
		probe, err = probe.Normalize()
		if err != nil {
			return false, err
		}
		response, err := c.base.Retrieve(ctx, probe)
		if err != nil {
			err = directBaseError(err)
			switch {
			case shoal.IsErrorCode(err, shoal.ErrorCanceled),
				shoal.IsErrorCode(err, shoal.ErrorDeadline):
				return false, err
			case shoal.IsErrorCode(err, shoal.ErrorUnavailable),
				shoal.IsErrorCode(err, shoal.ErrorConflict),
				shoal.IsErrorCode(err, shoal.ErrorInvalidArgument):
				available = false
			default:
				return false, err
			}
		} else if err := response.ValidateFor(probe); err != nil {
			available = false
		} else if err := c.validateRetrievedResponse(
			ctx, response, probe, corpus, decision, now,
		); err != nil {
			if shoal.IsErrorCode(err, shoal.ErrorCanceled) ||
				shoal.IsErrorCode(err, shoal.ErrorDeadline) {
				return false, err
			}
			available = false
		}
	}
	if err := guard.Check(ctx); err != nil {
		return false, err
	}
	c.cacheAuthorizedVectorAvailability(cacheKey, available, now)
	return available, nil
}

func (c *Client) cachedAuthorizedVectorAvailability(
	key string,
	now time.Time,
) (bool, bool) {
	c.vectorMu.Lock()
	defer c.vectorMu.Unlock()
	cached := c.vectorAvailability
	age := now.Sub(cached.checkedAt)
	if cached.key == key && !cached.checkedAt.IsZero() &&
		age >= 0 && age < time.Minute {
		return cached.available, true
	}
	return false, false
}

func (c *Client) cacheAuthorizedVectorAvailability(
	key string,
	available bool,
	checkedAt time.Time,
) {
	c.vectorMu.Lock()
	defer c.vectorMu.Unlock()
	c.vectorAvailability = authorizedVectorAvailabilityCache{
		key: key, checkedAt: checkedAt, available: available,
	}
}

func (c *Client) invalidateAuthorizedVectorAvailability() {
	c.vectorMu.Lock()
	defer c.vectorMu.Unlock()
	c.vectorAvailability = authorizedVectorAvailabilityCache{}
}

func authorizedVectorAvailabilityKey(
	decision auth.Decision,
	visibleOrder []shoal.ID,
	visible map[shoal.ID]RevisionRegistration,
) string {
	ids := append([]shoal.ID(nil), visibleOrder...)
	sort.Slice(ids, func(left, right int) bool {
		return shoal.CompareID(ids[left], ids[right]) < 0
	})
	var builder strings.Builder
	builder.WriteString(strconv.FormatInt(decision.PolicyGeneration(), 10))
	for _, documentID := range ids {
		registration := visible[documentID]
		builder.WriteByte('|')
		writeLengthPrefixedID(&builder, documentID)
		writeLengthPrefixedID(&builder, registration.RevisionID)
	}
	return builder.String()
}

func writeLengthPrefixedID(builder *strings.Builder, id shoal.ID) {
	value := string(id)
	builder.WriteString(strconv.Itoa(len(value)))
	builder.WriteByte(':')
	builder.WriteString(value)
	builder.WriteByte(';')
}

func (c *Client) ValidateAuthorization(ctx context.Context, pin inference.AuthPin) error {
	decision, guard, _, err := c.begin(ctx, auth.OperationValidate)
	if err != nil {
		return err
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return err
	}
	if shoal.ID(fingerprint.String()) != pin.Fingerprint() ||
		decision.AuthenticationExpires().Before(pin.ExpiresAt()) {
		return shoal.NewError(shoal.ErrorUnauthorized, "authorization pin does not match current authorization")
	}
	return guard.Check(ctx)
}

func (c *Client) BoundedNeighborhood(
	ctx context.Context,
	request explorer.BoundedNeighborhoodRequest,
) (explorer.BoundedNeighborhood, error) {
	bounded, err := c.boundedBase()
	if err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	if request.Depth == 0 || request.Fanout == 0 || request.MaxNodes == 0 {
		return explorer.BoundedNeighborhood{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "bounded graph limits must be nonzero")
	}
	normalized, err := (explorer.NeighborhoodRequest{
		NodeIDs: request.NodeIDs, Depth: request.Depth, EdgeTypes: request.EdgeTypes,
	}).Normalize()
	if err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationNeighborhood)
	if err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	for _, nodeID := range normalized.NodeIDs {
		if possibleProvenanceNodeID(nodeID) {
			continue
		}
		if _, err := c.authorizedNode(
			ctx, nodeID, decision, auth.OperationNeighborhood, now); err != nil {
			return explorer.BoundedNeighborhood{}, err
		}
	}
	direction := request.Direction
	if direction == "" {
		direction = explorer.GraphDirectionBoth
	}
	if authorizedCursorEligible(normalized) {
		return c.boundedAuthorizedNeighborhoodPage(
			ctx, bounded, request, normalized, decision, guard, now, direction)
	}
	raw, err := bounded.BoundedNeighborhood(ctx, request)
	if err != nil {
		return explorer.BoundedNeighborhood{}, directBaseError(err)
	}
	filtered, err := c.filterNeighborhood(
		ctx, raw.Neighborhood, normalized, direction, decision, now, false)
	if err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	raw.Neighborhood = filtered
	raw.Neighborhood, err = c.applyOntologyLens(ctx, raw.Neighborhood, decision)
	if err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	raw.NextAfterEdgeID = ""
	raw.Continuation = false
	return raw, nil
}

const maxAuthorizedBoundedScanPages = 1024

const (
	derivedAssertionPropertyAssertionID  = "ontology.assertion.id"
	derivedAssertionPropertyDerivationID = "ontology.assertion.derivation.id"
)

func authorizedCursorEligible(normalized explorer.NeighborhoodRequest) bool {
	return len(normalized.NodeIDs) == 1 && normalized.Depth == 1
}

func (c *Client) boundedAuthorizedNeighborhoodPage(
	ctx context.Context,
	bounded explorer.BoundedClient,
	request explorer.BoundedNeighborhoodRequest,
	normalized explorer.NeighborhoodRequest,
	decision auth.Decision,
	guard auth.GenerationGuard,
	now time.Time,
	direction explorer.GraphDirection,
) (explorer.BoundedNeighborhood, error) {
	scan := request
	scan.Depth = 1
	scan.NodeIDs = normalized.NodeIDs
	scan.EdgeTypes = nil
	if scan.Fanout == 0 {
		scan.Fanout = request.Fanout
	}
	nodes := make(map[shoal.ID]graph.Node)
	edges := make(map[shoal.ID]graph.Edge)
	assertions := make(map[shoal.ID]ontology.Assertion)
	after := request.AfterEdgeID
	exhaustedScanLimit := true
	scannedEdges := uint32(0)
	scanLimit := authorizedScanEdgeLimit(request)
	for page := 0; page < maxAuthorizedBoundedScanPages && scannedEdges < scanLimit; page++ {
		if err := ctx.Err(); err != nil {
			return explorer.BoundedNeighborhood{}, err
		}
		if err := guard.Check(ctx); err != nil {
			return explorer.BoundedNeighborhood{}, err
		}
		remainingScan := scanLimit - scannedEdges
		scan.Fanout = request.Fanout
		if scan.Fanout == 0 || scan.Fanout > remainingScan {
			scan.Fanout = remainingScan
		}
		scan.MaxScannedEdges = remainingScan
		scan.AfterEdgeID = after
		raw, err := bounded.BoundedNeighborhood(ctx, scan)
		if err != nil {
			return explorer.BoundedNeighborhood{}, directBaseError(err)
		}
		scannedEdges += uint32(len(raw.Neighborhood.Edges))
		filtered, err := c.filterNeighborhood(
			ctx, raw.Neighborhood, normalized, direction, decision, now, true)
		if err != nil {
			return explorer.BoundedNeighborhood{}, err
		}
		for _, node := range filtered.Nodes {
			if _, ok := nodes[node.ID]; !ok {
				nodes[node.ID] = cloneGraphNode(node)
			}
		}
		for _, edge := range filtered.Edges {
			if _, ok := edges[edge.ID]; !ok {
				edges[edge.ID] = cloneGraphEdge(edge)
			}
		}
		for _, assertion := range filtered.Assertions {
			if _, ok := assertions[assertion.ID()]; !ok {
				assertions[assertion.ID()] = assertion
			}
		}
		if len(edges) > int(request.Fanout) {
			exhaustedScanLimit = false
			break
		}
		if !raw.Continuation {
			exhaustedScanLimit = false
			break
		}
		if raw.NextAfterEdgeID == "" || raw.NextAfterEdgeID == after {
			return explorer.BoundedNeighborhood{}, inconsistentBase()
		}
		after = raw.NextAfterEdgeID
	}
	if exhaustedScanLimit {
		return explorer.BoundedNeighborhood{}, shoal.NewError(
			shoal.ErrorUnavailable, "authorized bounded graph scan limit exhausted")
	}
	for _, nodeID := range normalized.NodeIDs {
		if _, ok := nodes[nodeID]; ok {
			continue
		}
		if possibleProvenanceNodeID(nodeID) {
			return explorer.BoundedNeighborhood{}, auth.ObjectNotFound()
		}
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	result := authorizedBoundedPage(
		nodes, edges, assertions, request, len(normalized.NodeIDs))
	interpreted, interpretErr := c.applyOntologyLens(ctx, result.Neighborhood, decision)
	if interpretErr != nil {
		return explorer.BoundedNeighborhood{}, interpretErr
	}
	result.Neighborhood = interpreted
	if err := guard.Check(ctx); err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	return result, nil
}

func authorizedScanEdgeLimit(request explorer.BoundedNeighborhoodRequest) uint32 {
	if request.MaxScannedEdges > 0 {
		return request.MaxScannedEdges
	}
	if request.Fanout > 0 {
		return request.Fanout
	}
	return 1
}

func authorizedBoundedPage(
	nodes map[shoal.ID]graph.Node,
	edges map[shoal.ID]graph.Edge,
	assertions map[shoal.ID]ontology.Assertion,
	request explorer.BoundedNeighborhoodRequest,
	seedCount int,
) explorer.BoundedNeighborhood {
	edgeIDs := make([]shoal.ID, 0, len(edges))
	for edgeID := range edges {
		edgeIDs = append(edgeIDs, edgeID)
	}
	sort.Slice(edgeIDs, func(left, right int) bool {
		return shoal.CompareID(edgeIDs[left], edgeIDs[right]) < 0
	})
	nodeIDs := make(map[shoal.ID]struct{}, len(request.NodeIDs)+len(edgeIDs)+1)
	for _, nodeID := range request.NodeIDs {
		nodeIDs[nodeID] = struct{}{}
	}
	selectedEdges := make([]graph.Edge, 0, len(edgeIDs))
	truncated := false
	continuation := false
	nextAfter := shoal.ID("")
	for _, edgeID := range edgeIDs {
		edge := edges[edgeID]
		adds := 0
		if _, ok := nodeIDs[edge.From]; !ok {
			adds++
		}
		if _, ok := nodeIDs[edge.To]; !ok && edge.To != edge.From {
			adds++
		}
		if uint32(len(nodeIDs)+adds) > request.MaxNodes {
			truncated = true
			continuation = len(selectedEdges) > 0
			break
		}
		if len(selectedEdges) == int(request.Fanout) {
			continuation = true
			truncated = true
			break
		}
		selectedEdges = append(selectedEdges, cloneGraphEdge(edge))
		nodeIDs[edge.From] = struct{}{}
		nodeIDs[edge.To] = struct{}{}
		nextAfter = edge.ID
	}
	if len(selectedEdges) == 0 {
		continuation = false
		nextAfter = ""
	}
	resultNodes := make([]graph.Node, 0, len(nodeIDs))
	for nodeID := range nodeIDs {
		if node, ok := nodes[nodeID]; ok {
			resultNodes = append(resultNodes, cloneGraphNode(node))
		}
	}
	sort.Slice(resultNodes, func(left, right int) bool {
		return shoal.CompareID(resultNodes[left].ID, resultNodes[right].ID) < 0
	})
	if len(resultNodes) < seedCount {
		truncated = true
	}
	if !continuation {
		nextAfter = ""
	}
	selectedAssertions := make([]ontology.Assertion, 0, len(selectedEdges))
	seenAssertions := make(map[shoal.ID]struct{}, len(selectedEdges))
	for _, edge := range selectedEdges {
		if assertion, ok := assertions[edge.ID]; ok {
			seenAssertions[assertion.ID()] = struct{}{}
			selectedAssertions = append(selectedAssertions, assertion)
			continue
		}
		if edge.Type != graph.EdgeTypeProduced {
			continue
		}
		assertionID, ok := edge.Properties[derivedAssertionPropertyAssertionID]
		if !ok {
			continue
		}
		assertion, ok := assertions[shoal.ID(assertionID)]
		if !ok {
			continue
		}
		if _, duplicate := seenAssertions[assertion.ID()]; duplicate {
			continue
		}
		seenAssertions[assertion.ID()] = struct{}{}
		selectedAssertions = append(selectedAssertions, assertion)
	}
	sort.Slice(selectedAssertions, func(left, right int) bool {
		return shoal.CompareID(selectedAssertions[left].ID(), selectedAssertions[right].ID()) < 0
	})
	return explorer.BoundedNeighborhood{
		Neighborhood: explorer.Neighborhood{
			Nodes: resultNodes, Edges: selectedEdges, Assertions: selectedAssertions,
		},
		Truncated: truncated, NextAfterEdgeID: nextAfter, Continuation: continuation,
	}
}

func (c *Client) boundedBase() (explorer.BoundedClient, error) {
	bounded, ok := c.base.(explorer.BoundedClient)
	if !ok {
		return nil, shoal.NewError(shoal.ErrorUnavailable, "bounded Explorer base unavailable")
	}
	return bounded, nil
}

func (c *Client) filterNeighborhood(
	ctx context.Context,
	raw explorer.Neighborhood,
	normalized explorer.NeighborhoodRequest,
	direction explorer.GraphDirection,
	decision auth.Decision,
	now time.Time,
	allowMissingProvenanceSeeds bool,
) (explorer.Neighborhood, error) {
	candidates := make(map[shoal.ID]graph.Node, len(raw.Nodes))
	registrations := make(map[shoal.ID]NodeRegistration, len(raw.Nodes))
	rawNodes := make(map[shoal.ID]graph.Node, len(raw.Nodes))
	rawNodeIDs := make([]shoal.ID, 0, len(raw.Nodes))
	for _, node := range raw.Nodes {
		if err := node.Validate(); err != nil {
			return explorer.Neighborhood{}, inconsistentBase()
		}
		if _, duplicate := rawNodes[node.ID]; duplicate {
			return explorer.Neighborhood{}, inconsistentBase()
		}
		rawNodes[node.ID] = cloneGraphNode(node)
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

	typeFilter := make(map[string]struct{}, len(normalized.EdgeTypes))
	for _, edgeType := range normalized.EdgeTypes {
		typeFilter[edgeType] = struct{}{}
	}
	derivedAssertions, err := derivedAssertionsByEdge(raw.Assertions)
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	admittedEdges := make(map[shoal.ID]graph.Edge, len(raw.Edges))
	admittedAssertions := make(map[shoal.ID]ontology.Assertion, len(raw.Assertions))
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
		assertion, hasAssertion := derivedAssertions[edge.ID]
		if hasAssertion && assertion.Origin() == ontology.AssertionDerived {
			allowed, err := c.derivedAssertionEndpointsAllow(
				ctx, rawNodes, visibleNodes, resolved, assertion, decision,
				auth.OperationNeighborhood, now)
			if err != nil {
				return explorer.Neighborhood{}, err
			}
			if !allowed || !derivedAssertionMatchesEdge(assertion, edge) {
				continue
			}
			admittedEdges[edge.ID] = cloneGraphEdge(edge)
			admittedAssertions[edge.ID] = assertion
			continue
		}
		if edge.Type == graph.EdgeTypeProduced {
			assertion, ok := derivedAssertions[edge.To]
			if !ok {
				return explorer.Neighborhood{}, inconsistentBase()
			}
			allowed, err := c.derivedAssertionAllows(
				ctx, assertion, decision, auth.OperationNeighborhood, now)
			if err != nil {
				return explorer.Neighborhood{}, err
			}
			if !allowed || !producerDerivationEdgeMatches(edge, rawNodes, assertion) {
				continue
			}
			// Load-bearing: TestAuthorizedProducerProvenanceDoesNotAggregateHiddenAssertions
			// pins that producer and assertion nodes are admitted only through
			// an already authorized produced assertion.
			visibleNodes[edge.From] = cloneGraphNode(rawNodes[edge.From])
			visibleNodes[edge.To] = cloneGraphNode(rawNodes[edge.To])
			admittedEdges[edge.ID] = cloneGraphEdge(edge)
			admittedAssertions[edge.ID] = assertion
			continue
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
		assertion, hasAssertion := derivedAssertions[edge.ID]
		if hasAssertion {
			admittedAssertions[edge.ID] = assertion
		}
	}
	missingProvenanceSeed := false
	for _, seed := range normalized.NodeIDs {
		if _, ok := visibleNodes[seed]; ok {
			continue
		}
		if node, ok := rawNodes[seed]; ok && graph.IsProvenanceKind(node.Kind) {
			if allowMissingProvenanceSeeds {
				missingProvenanceSeed = true
				continue
			}
			// Load-bearing:
			// TestAuthorizedUnboundedNeighborhoodProducerSeedRequiresAuthorization
			// pins that this unbounded Neighborhood-only seed guard returns
			// not-found rather than empty. Empty success is an existence oracle,
			// and Neighborhood has no post-filter provenance seed check.
			return explorer.Neighborhood{}, auth.ObjectNotFound()
		}
		return explorer.Neighborhood{}, inconsistentBase()
	}
	if missingProvenanceSeed {
		return explorer.Neighborhood{}, nil
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
			reached := reachableThrough(edge, frontier, direction)
			if len(reached) == 0 {
				continue
			}
			selectedEdges[edgeID] = edge
			for _, nodeID := range reached {
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
		Nodes:      make([]graph.Node, 0, len(reachable)),
		Edges:      make([]graph.Edge, 0, len(selectedEdges)),
		Assertions: make([]ontology.Assertion, 0, len(selectedEdges)),
	}
	for nodeID := range reachable {
		result.Nodes = append(result.Nodes, cloneGraphNode(visibleNodes[nodeID]))
	}
	for _, edge := range selectedEdges {
		result.Edges = append(result.Edges, cloneGraphEdge(edge))
	}
	for edgeID := range selectedEdges {
		if assertion, ok := admittedAssertions[edgeID]; ok {
			result.Assertions = append(result.Assertions, assertion)
		}
	}
	sort.Slice(result.Nodes, func(left, right int) bool {
		return shoal.CompareID(result.Nodes[left].ID, result.Nodes[right].ID) < 0
	})
	sort.Slice(result.Edges, func(left, right int) bool {
		return shoal.CompareID(result.Edges[left].ID, result.Edges[right].ID) < 0
	})
	sort.Slice(result.Assertions, func(left, right int) bool {
		return shoal.CompareID(result.Assertions[left].ID(), result.Assertions[right].ID()) < 0
	})
	return result, nil
}

func reachableThrough(
	edge graph.Edge,
	frontier map[shoal.ID]struct{},
	direction explorer.GraphDirection,
) []shoal.ID {
	switch direction {
	case explorer.GraphDirectionOutgoing:
		if _, ok := frontier[edge.From]; ok {
			return []shoal.ID{edge.To}
		}
	case explorer.GraphDirectionIncoming:
		if _, ok := frontier[edge.To]; ok {
			return []shoal.ID{edge.From}
		}
	default:
		_, from := frontier[edge.From]
		_, to := frontier[edge.To]
		switch {
		case from && to:
			return []shoal.ID{edge.From, edge.To}
		case from:
			return []shoal.ID{edge.To}
		case to:
			return []shoal.ID{edge.From}
		}
	}
	return nil
}

func derivedAssertionsByEdge(
	assertions []ontology.Assertion,
) (map[shoal.ID]ontology.Assertion, error) {
	byEdge := make(map[shoal.ID]ontology.Assertion, len(assertions))
	for _, assertion := range assertions {
		if err := assertion.Validate(); err != nil {
			return nil, inconsistentBase()
		}
		if assertion.Origin() != ontology.AssertionDerived {
			edgeID := shoal.ID(assertion.Metadata()[graphAssertionEdgeIDMetadata])
			if edgeID == "" {
				// This skip is load-bearing; TestExtractDocumentAuthorizationControlsDerivedGraph pins that cited inferred extraction assertions do not make authorized graph reads fail closed.
				continue
			}
			if err := shoal.ValidateRequiredID("assertion graph edge ID", edgeID); err != nil {
				return nil, inconsistentBase()
			}
			if _, duplicate := byEdge[edgeID]; duplicate {
				return nil, inconsistentBase()
			}
			byEdge[edgeID] = assertion
			continue
		}
		if _, ok := assertion.Object().ReferenceValue(); !ok {
			return nil, inconsistentBase()
		}
		if _, duplicate := byEdge[assertion.ID()]; duplicate {
			return nil, inconsistentBase()
		}
		byEdge[assertion.ID()] = assertion
	}
	return byEdge, nil
}

func derivedAssertionMatchesEdge(assertion ontology.Assertion, edge graph.Edge) bool {
	target, ok := assertion.Object().ReferenceValue()
	if !ok {
		return false
	}
	return edge.ID == assertion.ID() &&
		edge.From == assertion.Subject() &&
		edge.To == target &&
		edge.Type == string(assertion.Predicate()) &&
		scoresEqual(edge.Weight, assertion.Confidence())
}

func producerDerivationEdgeMatches(
	edge graph.Edge,
	rawNodes map[shoal.ID]graph.Node,
	assertion ontology.Assertion,
) bool {
	producer, ok := rawNodes[edge.From]
	if !ok || producer.Kind != graph.NodeKindProducer {
		return false
	}
	assertionNode, ok := rawNodes[edge.To]
	if !ok || assertionNode.Kind != graph.NodeKindDerivedAssertion {
		return false
	}
	if assertionNode.ID != assertion.ID() {
		return false
	}
	assertionID, ok := edge.Properties[derivedAssertionPropertyAssertionID]
	if !ok || shoal.ID(assertionID) != assertion.ID() {
		return false
	}
	derivation, ok := assertion.Evidence()[0].Derivation()
	if !ok {
		return false
	}
	return edge.Properties[derivedAssertionPropertyDerivationID] ==
		string(derivation.ID())
}

func (c *Client) derivedAssertionEndpointsAllow(
	ctx context.Context,
	rawNodes map[shoal.ID]graph.Node,
	visibleNodes map[shoal.ID]graph.Node,
	resolved registeredNodes,
	assertion ontology.Assertion,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (bool, error) {
	target, ok := assertion.Object().ReferenceValue()
	if !ok {
		return false, nil
	}
	allowed, err := edgeEndpointsAllow(
		resolved,
		EdgeRegistration{Edge: graph.Edge{
			ID: assertion.ID(), From: assertion.Subject(), To: target,
			Type: string(assertion.Predicate()), Weight: assertion.Confidence(),
		}},
		decision,
		operation,
		now,
	)
	if err != nil || !allowed {
		return allowed, err
	}
	registrations := map[shoal.ID]NodeRegistration{
		assertion.Subject(): resolved[assertion.Subject()],
		target:              resolved[target],
	}
	canonical, err := c.canonicalRegisteredNodes(ctx, registrations)
	if err != nil {
		return false, err
	}
	for _, nodeID := range []shoal.ID{assertion.Subject(), target} {
		node, ok := rawNodes[nodeID]
		if !ok || !graphNodesEqual(canonical[nodeID], node) {
			return false, inconsistentBase()
		}
		visibleNodes[nodeID] = cloneGraphNode(node)
	}
	return true, nil
}

func (c *Client) derivedAssertionAllows(
	ctx context.Context,
	assertion ontology.Assertion,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (bool, error) {
	target, ok := assertion.Object().ReferenceValue()
	if !ok {
		return false, nil
	}
	resolved, err := c.resolveNodes(ctx, []shoal.ID{assertion.Subject(), target})
	if err != nil {
		return false, err
	}
	allowed, err := edgeEndpointsAllow(
		resolved,
		EdgeRegistration{Edge: graph.Edge{
			ID: assertion.ID(), From: assertion.Subject(), To: target,
			Type: string(assertion.Predicate()), Weight: assertion.Confidence(),
		}},
		decision,
		operation,
		now,
	)
	if err != nil || !allowed {
		return allowed, err
	}
	registrations := map[shoal.ID]NodeRegistration{
		assertion.Subject(): resolved[assertion.Subject()],
		target:              resolved[target],
	}
	_, err = c.canonicalRegisteredNodes(ctx, registrations)
	if err != nil {
		return false, err
	}
	return true, nil
}
