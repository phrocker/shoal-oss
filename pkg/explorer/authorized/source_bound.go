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
	"bytes"
	"context"
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// A retrieval source bound (#560) confines a read to content governed by named
// (SourceID, PolicyID) scopes, which is what a fleet descriptor's scope means
// for an executor that retrieves. It is applied here and only here, as a
// second filter after the decision has authorized the content, never instead
// of it and never by changing the decision. Re-minting a decision with
// narrowed sources would forge authority and change the authorization
// fingerprint every interaction record pins; a filter changes neither.
//
// The two filters are ordered so a bound reveals nothing the decision hides:
// what the decision withholds is withheld and counted exactly as it is
// unbounded, and only then is readable content outside the bound dropped, as
// if it were absent and uncounted. Every refusal a bound causes has the shape
// a hidden or absent object already has.

// ruleWithinBound reports whether content governed by rule lies within bound.
// A nil bound contains everything.
//
// Every term of the conjunction must be a scope the bound names, or a label
// policy (#570) on a source one of the rule's own admitted terms names. A
// label only narrows its source (every label rule is LabelRule(sourcePolicy,
// labels...)), so a labelled document on a bound source is within the bound,
// while a term on any other source puts the content outside it. At least one
// term must be an exact admitted scope: a rule of labels alone is anchored to
// no scope and is outside every bound.
//
// This is the per-object counterpart of the check fleet's
// refuseUnconfinedRetrieval applies to a whole decision, and treats own-source
// label policies the same way.
func ruleWithinBound(rule AccessRule, bound *retrieval.SourceBound) bool {
	if bound == nil {
		return true
	}
	if len(rule.policies) == 0 {
		return false
	}
	var anchored [][]byte
	for _, policy := range rule.policies {
		if bound.Admits(policy.SourceID(), policy.GrantPolicyID()) {
			anchored = append(anchored, policy.SourceID())
		}
	}
	// A label term is admitted only on a source an admitted term anchors, so
	// a rule with no admitted term — labels alone — is outside every bound.
	for _, policy := range rule.policies {
		source := policy.SourceID()
		if bound.Admits(source, policy.GrantPolicyID()) {
			continue
		}
		labelSource, _, err := auth.ParseLabelPolicyID(policy.GrantPolicyID())
		if err != nil || !bytes.Equal(labelSource, source) ||
			!containsBytes(anchored, source) {
			return false
		}
	}
	return true
}

func containsBytes(values [][]byte, wanted []byte) bool {
	for _, value := range values {
		if bytes.Equal(value, wanted) {
			return true
		}
	}
	return false
}

// revisionWithinBound applies ruleWithinBound to one revision registration
// the way revisionAllows applies the decision: an older revision is governed
// by its own rule and its document's current rule, so both must be within the
// bound. A nil bound contains everything.
func (c *Client) revisionWithinBound(
	ctx context.Context,
	registration RevisionRegistration,
	bound *retrieval.SourceBound,
) (bool, error) {
	if bound == nil {
		return true, nil
	}
	if !ruleWithinBound(registration.Rule, bound) {
		return false, nil
	}
	if registration.Current {
		return true, nil
	}
	current, ok, err := c.policyStore.CurrentRevision(
		ctx, registration.DocumentID)
	if err != nil {
		return false, policyCatalogReadError(ctx, err)
	}
	if !ok || current.DocumentID != registration.DocumentID {
		return false, nil
	}
	return ruleWithinBound(current.Rule, bound), nil
}

// DocumentWithin is Document confined to bound: a revision the caller may
// read but the bound does not cover is refused with the same not-found shape
// as a hidden or absent one. A nil bound is Document.
func (c *Client) DocumentWithin(
	ctx context.Context,
	bound *retrieval.SourceBound,
	documentID, revisionID shoal.ID,
) (explorer.DocumentView, error) {
	normalized, err := bound.Normalize()
	if err != nil {
		return explorer.DocumentView{}, err
	}
	return c.document(ctx, normalized, documentID, revisionID)
}

// BoundedNeighborhoodWithin is BoundedNeighborhood confined to bound. A nil
// bound is BoundedNeighborhood.
//
// The authorized neighborhood is computed first, exactly as it is unbounded,
// and then confined: a seed outside the bound is refused with the not-found
// shape a hidden seed has, and the page keeps only what lies within the
// bound and is still reachable from the seeds through what it keeps.
//
// The confinement is deliberately conservative. A node or edge is kept only
// when its own registration is within the bound; provenance nodes and every
// edge without a registration of its own (derived and produced edges) are
// dropped, since nothing places them on a source; and an assertion is kept
// only when every piece of its evidence is within the bound, which drops any
// assertion resting on a derivation. Withholding something the bound might
// have covered costs an answer some grounding. Keeping something it does not
// cover is the failure #560 exists to remove.
//
// Truncated, Continuation and NextAfterEdgeID describe the authorized page and
// are passed through. They can say that more adjacency exists beyond what the
// bound keeps, but that adjacency is content this caller may already read:
// nothing the decision hides is counted, here or anywhere a bound applies.
func (c *Client) BoundedNeighborhoodWithin(
	ctx context.Context,
	bound *retrieval.SourceBound,
	request explorer.BoundedNeighborhoodRequest,
) (explorer.BoundedNeighborhood, error) {
	normalizedBound, err := bound.Normalize()
	if err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	page, err := c.BoundedNeighborhood(ctx, request)
	if err != nil || normalizedBound == nil {
		return page, err
	}
	normalized, err := (explorer.NeighborhoodRequest{
		NodeIDs: request.NodeIDs, Depth: request.Depth, EdgeTypes: request.EdgeTypes,
	}).Normalize()
	if err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	direction := request.Direction
	if direction == "" {
		direction = explorer.GraphDirectionBoth
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationNeighborhood)
	if err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	confined, err := c.confineNeighborhood(
		ctx, page.Neighborhood, normalized, direction, normalizedBound,
		decision, auth.OperationNeighborhood, now)
	if err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.BoundedNeighborhood{}, err
	}
	page.Neighborhood = confined
	return page, nil
}

// confineNeighborhood keeps the part of an already authorized neighborhood
// that lies within bound. See BoundedNeighborhoodWithin.
func (c *Client) confineNeighborhood(
	ctx context.Context,
	raw explorer.Neighborhood,
	normalized explorer.NeighborhoodRequest,
	direction explorer.GraphDirection,
	bound *retrieval.SourceBound,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (explorer.Neighborhood, error) {
	reader := &registrationReader{
		decision: decision, operation: operation, now: now, bound: bound,
	}
	within := func(rule AccessRule) (bool, error) {
		allowed, err := ruleAllows(rule, decision, operation, now)
		if err != nil || !allowed {
			return false, err
		}
		return ruleWithinBound(rule, bound), nil
	}

	// Nodes, seeds included, by their own projected registration.
	nodeIDs := make([]shoal.ID, 0, len(raw.Nodes)+len(normalized.NodeIDs))
	nodeIDs = append(nodeIDs, normalized.NodeIDs...)
	for _, node := range raw.Nodes {
		nodeIDs = append(nodeIDs, node.ID)
	}
	resolvedNodes, err := c.resolveNodes(ctx, nodeIDs, reader)
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	nodeWithin := func(nodeID shoal.ID, kind string) (bool, error) {
		if graph.IsProvenanceKind(kind) || possibleProvenanceNodeID(nodeID) {
			return false, nil
		}
		registration, ok := resolvedNodes[nodeID]
		if !ok {
			return false, nil
		}
		return within(registration.Rule)
	}
	kept := make(map[shoal.ID]graph.Node, len(raw.Nodes))
	for _, node := range raw.Nodes {
		ok, err := nodeWithin(node.ID, node.Kind)
		if err != nil {
			return explorer.Neighborhood{}, err
		}
		if ok {
			kept[node.ID] = node
		}
	}
	for _, seed := range normalized.NodeIDs {
		// The decision already admitted every seed; one outside the bound is
		// refused as a hidden one is, not answered with an empty page.
		if _, ok := kept[seed]; !ok {
			return explorer.Neighborhood{}, auth.ObjectNotFound()
		}
	}

	// Edges by their own projected registration, between kept endpoints.
	candidateIDs := make([]shoal.ID, 0, len(raw.Edges))
	for _, edge := range raw.Edges {
		_, fromKept := kept[edge.From]
		_, toKept := kept[edge.To]
		if fromKept && toKept {
			candidateIDs = append(candidateIDs, edge.ID)
		}
	}
	resolvedEdges, err := c.resolveEdges(ctx, candidateIDs, reader)
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	keptEdges := make(map[shoal.ID]graph.Edge, len(candidateIDs))
	for _, edge := range raw.Edges {
		_, fromKept := kept[edge.From]
		_, toKept := kept[edge.To]
		if !fromKept || !toKept {
			continue
		}
		registration, ok := resolvedEdges[edge.ID]
		if !ok || !graphEdgesEqual(registration.Edge, edge) {
			continue
		}
		ok, err := within(registration.Rule)
		if err != nil {
			return explorer.Neighborhood{}, err
		}
		if ok {
			keptEdges[edge.ID] = edge
		}
	}

	// Assertions by their edge and every piece of their evidence. An edge
	// whose assertion is dropped is dropped with it, as an unreadable shared
	// relation is (readerSafeExtractionAssertions).
	byEdge, err := derivedAssertionsByEdge(raw.Assertions)
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	var keptAssertions map[shoal.ID]ontology.Assertion
	for {
		// Each round decides every assertion against the same kept edges,
		// so no verdict depends on map order, and repeats until dropping an
		// edge no longer withdraws an assertion whose evidence path used it.
		keptAssertions = make(map[shoal.ID]ontology.Assertion, len(byEdge))
		var droppedEdges []shoal.ID
		for edgeID, assertion := range byEdge {
			if _, ok := keptEdges[edgeID]; !ok {
				continue
			}
			ok, err := c.assertionWithinBound(
				ctx, assertion, keptEdges, reader, within)
			if err != nil {
				return explorer.Neighborhood{}, err
			}
			if !ok {
				droppedEdges = append(droppedEdges, edgeID)
				continue
			}
			keptAssertions[assertion.ID()] = assertion
		}
		if len(droppedEdges) == 0 {
			break
		}
		for _, edgeID := range droppedEdges {
			delete(keptEdges, edgeID)
		}
	}

	// Reachability over what is kept: a node reached only through content the
	// bound excludes is not part of the bounded neighborhood.
	reachable := make(map[shoal.ID]struct{}, len(normalized.NodeIDs))
	frontier := make(map[shoal.ID]struct{}, len(normalized.NodeIDs))
	for _, seed := range normalized.NodeIDs {
		reachable[seed] = struct{}{}
		frontier[seed] = struct{}{}
	}
	selectedEdges := make(map[shoal.ID]graph.Edge)
	for depth := uint32(0); depth < normalized.Depth && len(frontier) > 0; depth++ {
		next := make(map[shoal.ID]struct{})
		for edgeID, edge := range keptEdges {
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
		Nodes: make([]graph.Node, 0, len(reachable)),
		Edges: make([]graph.Edge, 0, len(selectedEdges)),
	}
	for _, node := range raw.Nodes {
		if _, ok := reachable[node.ID]; ok {
			result.Nodes = append(result.Nodes, cloneGraphNode(node))
		}
	}
	for _, edge := range raw.Edges {
		if _, ok := selectedEdges[edge.ID]; ok {
			result.Edges = append(result.Edges, cloneGraphEdge(edge))
		}
	}
	selectedAssertions := make(map[shoal.ID]struct{}, len(keptAssertions))
	for _, assertion := range raw.Assertions {
		if _, ok := keptAssertions[assertion.ID()]; !ok {
			continue
		}
		edgeID := shoal.ID(assertion.Metadata()[graphAssertionEdgeIDMetadata])
		if _, ok := selectedEdges[edgeID]; !ok {
			continue
		}
		result.Assertions = append(result.Assertions, assertion)
		selectedAssertions[assertion.ID()] = struct{}{}
	}
	for _, interpretation := range raw.Interpretations {
		if _, ok := selectedAssertions[interpretation.Original().ID()]; ok {
			result.Interpretations = append(result.Interpretations, interpretation)
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

// assertionWithinBound reports whether an extraction assertion may be shown
// under a bound: it is not derived, and every piece of its evidence — the
// cited revision, and each node and edge of a cited path — is authorized and
// within the bound. Evidence resting on a derivation is never within one.
//
// The citation and path-node checks are defence in depth, and no test makes
// either the deciding one. An extraction assertion's evidence cites, and its
// path runs through, the document revision that asserted it, and its edge is
// kept only when that edge's own registration — owned by an asserting
// revision, and conjoined with that document's current rule — is within the
// bound. They can differ only for a relation shared across rule namespaces,
// and the entity namespace is a digest of the whole access rule, so such a
// relation arises only after a relabel or TightenRule; in every scenario
// built so far an earlier layer (the edge's projection or an endpoint)
// already drops it. They stay because the bound must hold for what the
// assertion carries, not only for the edge it is attached to.
func (c *Client) assertionWithinBound(
	ctx context.Context,
	assertion ontology.Assertion,
	keptEdges map[shoal.ID]graph.Edge,
	reader *registrationReader,
	within func(AccessRule) (bool, error),
) (bool, error) {
	if assertion.Origin() == ontology.AssertionDerived {
		return false, nil
	}
	for _, item := range assertion.Evidence() {
		if _, derived := item.Derivation(); derived {
			return false, nil
		}
		citation := item.Citation()
		if citation.DocumentID != "" || citation.RevisionID != "" {
			if citation.DocumentID == "" || citation.RevisionID == "" {
				return false, nil
			}
			registration, ok, err := c.policyStore.Revision(
				ctx, citation.DocumentID, citation.RevisionID)
			if err != nil {
				return false, policyCatalogReadError(ctx, err)
			}
			if !ok {
				return false, nil
			}
			allowed, err := c.revisionAllows(
				ctx, registration, reader.decision, reader.operation, reader.now)
			if err != nil || !allowed {
				return false, err
			}
			ok, err = c.revisionWithinBound(ctx, registration, reader.bound)
			if err != nil || !ok {
				return false, err
			}
		}
		path, ok := item.Path()
		if !ok {
			continue
		}
		pathNodes := make([]shoal.ID, 0, len(path.Nodes))
		for _, node := range path.Nodes {
			if graph.IsProvenanceKind(node.Kind) || possibleProvenanceNodeID(node.ID) {
				return false, nil
			}
			pathNodes = append(pathNodes, node.ID)
		}
		resolved, err := c.resolveNodes(ctx, pathNodes, reader)
		if err != nil {
			return false, err
		}
		for _, nodeID := range pathNodes {
			registration, ok := resolved[nodeID]
			if !ok {
				return false, nil
			}
			ok, err := within(registration.Rule)
			if err != nil || !ok {
				return false, err
			}
		}
		for _, edge := range path.Edges {
			if _, ok := keptEdges[edge.ID]; !ok {
				return false, nil
			}
		}
	}
	return true, nil
}
