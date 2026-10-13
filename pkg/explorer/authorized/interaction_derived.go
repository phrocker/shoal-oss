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

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// A derived (latent) edge is built at graph build from a derived assertion
// and has no policy-store edge registration (#719). Reads admit it through
// its trusted derived assertion when the reader may read both endpoints
// (filterNeighborhood): the edge carries no rule of its own, and its
// derivation is computed over nothing but those two endpoints. An
// interaction touching one is authorized the same way, so a reader shown a
// derived edge may record over it and read the record back, and a reader
// who may not read an endpoint is refused exactly as for an edge that never
// existed.
//
// The session's revision binding (#695) needs nothing further. A touched
// edge is always on a graph evidence path; the trusted validator requires
// the path's nodes to be its edges' endpoints, Session.Validate requires
// them among the session's touched nodes, and interactionSourceRevisions
// binds every one of them. TestDerivedEdgeSessionIsHeldToItsEndpointRevisions
// pins it. A derived assertion records no revision of its own: the latent
// link it projects names only the two endpoint nodes.

// interactionEdges is one chunk of touched edges resolved for authorization:
// each by its registration or, when the store holds none for it, as the
// derived edge its trusted derived assertion builds.
type interactionEdges struct {
	registered registeredEdges
	derived    map[shoal.ID]graph.Edge
}

// endpoints is every endpoint of the resolved edges.
func (e interactionEdges) endpoints() []shoal.ID {
	endpoints := make([]shoal.ID, 0, 2*(len(e.registered)+len(e.derived)))
	for _, registration := range e.registered {
		endpoints = append(endpoints, registration.Edge.From, registration.Edge.To)
	}
	for _, edge := range e.derived {
		endpoints = append(endpoints, edge.From, edge.To)
	}
	return endpoints
}

// edge is the resolved edge edgeID names: its registration's, or the derived
// edge its trusted assertion builds.
func (e interactionEdges) edge(edgeID shoal.ID) (graph.Edge, bool) {
	if registration, ok := e.registered[edgeID]; ok {
		return registration.Edge, registration.Edge.ID == edgeID
	}
	edge, ok := e.derived[edgeID]
	return edge, ok
}

// resolveGateEdges is resolveInteractionEdges over any number of edges, in
// lookups bounded by maxInteractionAuthorizationIDs.
func (c *Client) resolveGateEdges(
	ctx context.Context, edgeIDs []shoal.ID, reader *registrationReader,
) (interactionEdges, error) {
	edges := interactionEdges{registered: registeredEdges{}}
	for start := 0; start < len(edgeIDs); {
		end := chunkEnd(start, len(edgeIDs), maxInteractionAuthorizationIDs)
		resolved, err := c.resolveInteractionEdges(ctx, edgeIDs[start:end], reader)
		if err != nil {
			return interactionEdges{}, err
		}
		start = end
		for id, registration := range resolved.registered {
			edges.registered[id] = registration
		}
		for id, edge := range resolved.derived {
			if edges.derived == nil {
				edges.derived = make(map[shoal.ID]graph.Edge)
			}
			edges.derived[id] = edge
		}
	}
	return edges, nil
}

// resolveInteractionEdges resolves touched edges as resolveEdges does, and
// every one that resolves no registration through the trusted
// DerivedAssertionReader. One the reader does not hold, or holds in a form
// derivedInteractionEdge refuses, is left out, which every caller treats as
// unregistered and denied. Without a trusted reader no derived edge
// resolves.
//
// The trusted reader is asked about every edge, registered or not, so that
// whether it is consulted, and whether it fails, never depends on which
// edges are registered: otherwise a failing reader would tell a recorder
// that a hidden edge it named exists (refused) rather than never existed
// (unavailable); TestUnavailableDerivedReaderDoesNotRevealHiddenEdges pins
// it. A registered edge it is asked about stays decided by its
// registration, and one withheld by effectiveEdgeRegistrations (an
// extracted relation whose document closed) stays denied: the reader
// returns derived assertions only, under their own content-derived ID,
// which is the derived edge's ID and no registered edge's.
func (c *Client) resolveInteractionEdges(
	ctx context.Context,
	edgeIDs []shoal.ID,
	reader *registrationReader,
) (interactionEdges, error) {
	registered, err := c.resolveEdges(ctx, edgeIDs, reader)
	if err != nil {
		return interactionEdges{}, err
	}
	derived, err := c.trustedDerivedEdges(ctx, edgeIDs)
	if err != nil {
		return interactionEdges{}, err
	}
	for edgeID := range registered {
		delete(derived, edgeID)
	}
	if len(derived) == 0 {
		derived = nil
	}
	return interactionEdges{registered: registered, derived: derived}, nil
}

// trustedDerivedEdges asks the trusted DerivedAssertionReader about every
// distinct edge in edgeIDs, registered or not (see resolveInteractionEdges
// for why every one), and returns the derived edge each held assertion
// builds, keyed by edge ID. An assertion derivedInteractionEdge refuses, or
// one the reader does not hold, is left out. The caller drops every edge
// that resolves a registration: a registered edge is decided by its
// registration alone. Without a trusted reader nothing resolves.
func (c *Client) trustedDerivedEdges(
	ctx context.Context, edgeIDs []shoal.ID,
) (map[shoal.ID]graph.Edge, error) {
	if len(edgeIDs) == 0 || isNilDependency(c.derivedAssertions) {
		return nil, nil
	}
	distinct := make([]shoal.ID, 0, len(edgeIDs))
	seen := make(map[shoal.ID]struct{}, len(edgeIDs))
	for _, edgeID := range edgeIDs {
		if _, duplicate := seen[edgeID]; duplicate {
			continue
		}
		seen[edgeID] = struct{}{}
		distinct = append(distinct, edgeID)
	}
	trusted, err := c.derivedAssertions.DerivedAssertions(ctx, distinct)
	if err != nil {
		return nil, directBaseError(err)
	}
	var derived map[shoal.ID]graph.Edge
	for _, edgeID := range distinct {
		assertion, ok := trusted[edgeID]
		if !ok {
			continue
		}
		edge, ok := derivedInteractionEdge(edgeID, assertion)
		if !ok {
			continue
		}
		if derived == nil {
			derived = make(map[shoal.ID]graph.Edge)
		}
		derived[edgeID] = edge
	}
	return derived, nil
}

// derivedInteractionEdge is the edge a trusted derived assertion builds
// (explorer.latentAssertionGraphEdge), with what authorization reads. It is
// false unless the assertion is a valid derived assertion under edgeID whose
// single derivation was computed over exactly its subject and object, so
// that the derivation is attributable to the two endpoints the caller
// authorizes and to nothing else. A malformed assertion is withheld rather
// than reported, so the outcome never depends on an edge whose endpoints the
// recorder may not read.
func derivedInteractionEdge(
	edgeID shoal.ID, assertion ontology.Assertion,
) (graph.Edge, bool) {
	if assertion.Validate() != nil || assertion.ID() != edgeID ||
		assertion.Origin() != ontology.AssertionDerived {
		return graph.Edge{}, false
	}
	target, ok := assertion.Object().ReferenceValue()
	if !ok {
		return graph.Edge{}, false
	}
	evidence := assertion.Evidence()
	if len(evidence) != 1 {
		return graph.Edge{}, false
	}
	derivation, ok := evidence[0].Derivation()
	// Load-bearing, with the ID check above:
	// TestDerivedEdgeDerivationMustCoverOnlyItsEndpoints pins that a
	// derivation computed to or from another node, or an assertion held
	// under another edge's ID, is not recorded on the strength of the
	// edge's own endpoints.
	if !ok || derivation.SourceEndpoint() != assertion.Subject() ||
		derivation.TargetEndpoint() != target {
		return graph.Edge{}, false
	}
	return graph.Edge{
		ID: edgeID, From: assertion.Subject(), To: target,
		Type: string(assertion.Predicate()), Weight: assertion.Confidence(),
	}, true
}
