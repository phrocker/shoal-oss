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
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// EnsureInteractionSink verifies both the caller's current authorization pin
// and the base corpus's durable write path. This makes *Client directly usable
// with harness.NewGraphRecorder without bypassing the authorization wrapper.
func (c *Client) EnsureInteractionSink(ctx context.Context) error {
	writer, err := c.interactionWriter()
	if err != nil {
		return err
	}
	_, guard, _, err := c.begin(ctx, auth.OperationRetrieve)
	if err != nil {
		return err
	}
	if err := writer.EnsureInteractionSink(ctx); err != nil {
		return directBaseError(err)
	}
	return guard.Check(ctx)
}

type operationInteractionSink struct {
	client    *Client
	operation auth.Operation
}

// AnalyticsInteractionSink returns the shared durable interaction sink bound
// to the exact analytics_read operation. Nil means the underlying Explorer has
// no durable interaction writer, so analytics must not be advertised.
func (c *Client) AnalyticsInteractionSink() interaction.ResultSink {
	if c == nil {
		return nil
	}
	if _, err := c.interactionWriter(); err != nil {
		return nil
	}
	return operationInteractionSink{
		client: c, operation: auth.OperationAnalyticsRead,
	}
}

func (s operationInteractionSink) EnsureInteractionSink(
	ctx context.Context,
) error {
	if s.client == nil {
		return shoal.NewError(
			shoal.ErrorUnavailable, "authorized interaction sink is unavailable")
	}
	writer, err := s.client.interactionWriter()
	if err != nil {
		return err
	}
	return directBaseError(writer.EnsureInteractionSink(ctx))
}

func (s operationInteractionSink) RecordInteraction(
	ctx context.Context,
	session interaction.Session,
) error {
	_, err := s.RecordInteractionResult(ctx, session)
	return err
}

func (s operationInteractionSink) RecordInteractionResult(
	ctx context.Context,
	session interaction.Session,
) (interaction.Session, error) {
	if s.client == nil {
		return interaction.Session{}, shoal.NewError(
			shoal.ErrorUnavailable, "authorized interaction sink is unavailable")
	}
	return s.client.recordInteractionForOperation(ctx, session, s.operation)
}

// RecordInteraction appends one redacted interaction after verifying that its
// pinned authorization is the exact current decision and that every source
// node it retrieved or cited is still authorized. A revoked or missing source
// fails the inference instead of creating an under-authorized record.
func (c *Client) RecordInteraction(
	ctx context.Context, session interaction.Session,
) error {
	_, err := c.recordInteractionForOperation(
		ctx, session, auth.OperationRetrieve)
	return err
}

// RecordInteractionResult records one interaction and returns the exact
// trusted session accepted for persistence, including actor, delegation, and
// derived reason metadata supplied by the bound authorization Decision.
func (c *Client) RecordInteractionResult(
	ctx context.Context, session interaction.Session,
) (interaction.Session, error) {
	return c.recordInteractionForOperation(
		ctx, session, auth.OperationRetrieve)
}

func (c *Client) recordInteractionForOperation(
	ctx context.Context,
	session interaction.Session,
	operation auth.Operation,
) (interaction.Session, error) {
	writer, err := c.interactionWriter()
	if err != nil {
		return interaction.Session{}, err
	}
	canonical, err := session.Canonical()
	if err != nil {
		return interaction.Session{}, err
	}
	decision, guard, now, err := c.begin(ctx, operation)
	if err != nil {
		return interaction.Session{}, err
	}
	if !interactionPinMatchesDecision(canonical, decision) {
		return interaction.Session{}, authorizationDenied()
	}
	canonical.Actor = interaction.ActorContext{
		SubjectID:  decision.Subject(),
		ActorID:    decision.Actor(),
		ClientID:   decision.ClientID(),
		OnBehalfOf: decision.OnBehalfOf(),
	}
	canonical.RequestID = decision.RequestID()
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return interaction.Session{}, authorizationDenied()
	}
	canonical.AuthorizationFingerprint = shoal.ID(fingerprint.String())
	canonical.AuthorizationExpiresAt = decision.AuthenticationExpires()
	canonical.OntologySchemaID = ""
	canonical.OntologyVersionID = ""
	if selected, ok := decision.SelectedOntology(); ok {
		canonical.OntologySchemaID = selected.SchemaID()
		canonical.OntologyVersionID = selected.VersionID()
	}
	canonical.Reason = interaction.Reason{}
	if decision.AuditPurpose() != "" {
		canonical.Reason, err = interaction.NewReason(
			"audit_purpose", decision.AuditPurpose())
		if err != nil {
			return interaction.Session{}, authorizationDenied()
		}
	}
	canonical.RequiredVisibility = nil
	visibility, err := c.authorizeInteractionEvidence(
		ctx, canonical, decision, operation, now)
	if err != nil {
		return interaction.Session{}, err
	}
	canonical.RequiredVisibility = visibility
	canonical, err = canonical.Canonical()
	if err != nil {
		return interaction.Session{}, err
	}
	if err := guard.Check(ctx); err != nil {
		return interaction.Session{}, err
	}
	persisted := canonical
	if resultWriter, ok := writer.(interaction.ResultSink); ok {
		persisted, err = resultWriter.RecordInteractionResult(ctx, canonical)
	} else {
		err = writer.RecordInteraction(ctx, canonical)
	}
	if err != nil {
		return interaction.Session{}, directBaseError(err)
	}
	if err := guard.Check(ctx); err != nil {
		if operation == auth.OperationAnalyticsRead {
			return interaction.Session{}, explorer.MarkIndeterminateCommit(
				shoal.WrapError(
					shoal.ErrorUnavailable,
					"interaction was recorded but authorization generation revalidation failed",
					err,
				),
			)
		}
		return interaction.Session{}, explorer.MarkCommittedInteraction(err)
	}
	return persisted, nil
}

// Interactions lists only derived records whose complete current source set
// the caller may read. Tombstones have intentionally discarded their source
// edges, so they are visible only to the exact authorization projection that
// created the original record.
func (c *Client) Interactions(
	ctx context.Context,
) ([]explorer.InteractionSummary, error) {
	records, err := c.InteractionRecords(ctx)
	if err != nil {
		return nil, err
	}
	summaries := make([]explorer.InteractionSummary, len(records))
	for index, record := range records {
		summaries[index] = record.Summary
	}
	return summaries, nil
}

// InteractionRecords returns authorized interaction summaries and provenance
// in one base read and one batched policy lookup.
func (c *Client) InteractionRecords(
	ctx context.Context,
) ([]explorer.InteractionRecord, error) {
	reader, err := c.interactionReader()
	if err != nil {
		return nil, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return nil, err
	}
	records, err := reader.InteractionRecords(ctx)
	if err != nil {
		return nil, directBaseError(err)
	}
	allNodeIDs := make([]shoal.ID, 0)
	for _, record := range records {
		if !record.Summary.Deleted {
			allNodeIDs = append(allNodeIDs, record.TouchedNodeIDs...)
		}
	}
	registrations, err := c.resolveNodes(ctx, allNodeIDs)
	if err != nil {
		return nil, err
	}
	visible := make([]explorer.InteractionRecord, 0, len(records))
	for _, record := range records {
		if record.Summary.Deleted {
			if summaryFingerprintMatchesDecision(record.Summary, decision) {
				visible = append(visible, record)
			}
			continue
		}
		if len(record.TouchedNodeIDs) == 0 {
			if summaryFingerprintMatchesDecision(record.Summary, decision) {
				visible = append(visible, record)
			}
			continue
		}
		_, analytics, evidenceErr := analyticsInteractionEvidence(record.Session)
		hasExactEvidence := len(interactionSourceEdges(record.Session)) != 0
		for _, turn := range record.Session.Turns {
			if turn.ToolCall != nil &&
				(len(turn.ToolCall.RetrievedNodes) != 0 ||
					len(turn.ToolCall.RetrievedAssertions) != 0) {
				hasExactEvidence = true
				break
			}
		}
		if evidenceErr == nil && (analytics || hasExactEvidence) {
			_, evidenceErr = c.authorizeInteractionEvidence(
				ctx, record.Session, decision, auth.OperationRead, now)
		} else if evidenceErr == nil {
			var allowed bool
			allowed, evidenceErr = interactionSourcesAllow(
				registrations, record.TouchedNodeIDs,
				decision, auth.OperationRead, now)
			if evidenceErr == nil && !allowed {
				evidenceErr = auth.ObjectNotFound()
			}
		}
		if evidenceErr != nil {
			if shoal.IsErrorCode(evidenceErr, shoal.ErrorNotFound) ||
				shoal.IsErrorCode(evidenceErr, shoal.ErrorUnauthorized) {
				continue
			}
			return nil, evidenceErr
		}
		visible = append(visible, record)
	}
	if err := guard.Check(ctx); err != nil {
		return nil, err
	}
	return visible, nil
}

// InteractionRecord returns one authorized point record without scanning the
// complete interaction history.
func (c *Client) InteractionRecord(
	ctx context.Context, sessionID shoal.ID,
) (explorer.InteractionRecord, error) {
	reader, err := c.interactionReader()
	if err != nil {
		return explorer.InteractionRecord{}, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return explorer.InteractionRecord{}, err
	}
	record, err := reader.InteractionRecord(ctx, sessionID)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnavailable) ||
			shoal.IsErrorCode(err, shoal.ErrorConflict) {
			return explorer.InteractionRecord{}, auth.ObjectNotFound()
		}
		return explorer.InteractionRecord{}, directBaseError(err)
	}
	if record.Summary.Deleted {
		if !summaryFingerprintMatchesDecision(record.Summary, decision) {
			return explorer.InteractionRecord{}, auth.ObjectNotFound()
		}
	} else if len(record.TouchedNodeIDs) == 0 {
		if !summaryFingerprintMatchesDecision(record.Summary, decision) {
			return explorer.InteractionRecord{}, auth.ObjectNotFound()
		}
	} else if _, err := c.authorizeInteractionEvidence(
		ctx, record.Session, decision, auth.OperationRead, now,
	); err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) ||
			shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			return explorer.InteractionRecord{}, auth.ObjectNotFound()
		}
		return explorer.InteractionRecord{}, err
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.InteractionRecord{}, err
	}
	return record, nil
}

// Interaction returns one authorized typed interaction. It is an explicit
// derived view and therefore cannot affect the source-only retrieval surface.
func (c *Client) Interaction(
	ctx context.Context, sessionID shoal.ID,
) (interaction.Session, error) {
	record, err := c.InteractionRecord(ctx, sessionID)
	if err != nil {
		return interaction.Session{}, err
	}
	if record.Summary.Deleted || record.Session.ID == "" {
		return interaction.Session{}, auth.ObjectNotFound()
	}
	return record.Session, nil
}

// InteractionSubgraph returns an authorized explicit graph view. Every
// touched source is re-authorized before any derived node or edge is returned.
func (c *Client) InteractionSubgraph(
	ctx context.Context, sessionID shoal.ID,
) (explorer.Neighborhood, error) {
	reader, err := c.interactionReader()
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return explorer.Neighborhood{}, err
	}
	record, err := reader.InteractionRecord(ctx, sessionID)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnavailable) ||
			shoal.IsErrorCode(err, shoal.ErrorConflict) {
			return explorer.Neighborhood{}, auth.ObjectNotFound()
		}
		return explorer.Neighborhood{}, directBaseError(err)
	}
	if record.Summary.Deleted {
		if !summaryFingerprintMatchesDecision(record.Summary, decision) {
			return explorer.Neighborhood{}, auth.ObjectNotFound()
		}
		subgraph, readErr := reader.InteractionSubgraph(ctx, sessionID)
		if readErr != nil {
			return explorer.Neighborhood{}, directBaseError(readErr)
		}
		if err := guard.Check(ctx); err != nil {
			return explorer.Neighborhood{}, err
		}
		return subgraph, nil
	}
	if len(record.TouchedNodeIDs) == 0 &&
		!summaryFingerprintMatchesDecision(record.Summary, decision) {
		return explorer.Neighborhood{}, auth.ObjectNotFound()
	}
	if _, err := c.authorizeInteractionEvidence(
		ctx, record.Session, decision, auth.OperationRead, now,
	); err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
			return explorer.Neighborhood{}, auth.ObjectNotFound()
		}
		return explorer.Neighborhood{}, err
	}
	subgraph, err := reader.InteractionSubgraph(ctx, sessionID)
	if err != nil {
		return explorer.Neighborhood{}, directBaseError(err)
	}
	if interactionSubgraphIsTombstone(subgraph) &&
		!summaryFingerprintMatchesDecision(record.Summary, decision) {
		return explorer.Neighborhood{}, auth.ObjectNotFound()
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.Neighborhood{}, err
	}
	return subgraph, nil
}

func interactionSubgraphIsTombstone(subgraph explorer.Neighborhood) bool {
	return len(subgraph.Nodes) == 1 &&
		subgraph.Nodes[0].Kind == interaction.KindTombstone
}

func (c *Client) authorizeInteractionEvidence(
	ctx context.Context,
	session interaction.Session,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) ([]string, error) {
	evidence, analytics, err := analyticsInteractionEvidence(session)
	if err != nil {
		return nil, err
	}
	if analytics {
		if operation != auth.OperationAnalyticsRead &&
			operation != auth.OperationRead {
			return nil, authorizationDenied()
		}
		return c.authorizeAnalyticsInteractionEvidence(
			ctx, evidence, decision, operation, now)
	}
	if operation == auth.OperationAnalyticsRead {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"analytics interaction requires complete graph evidence",
		)
	}
	for _, turn := range session.Turns {
		if turn.ToolCall != nil &&
			(len(turn.ToolCall.RetrievedNodes) != 0 ||
				len(turn.ToolCall.RetrievedAssertions) != 0) {
			return nil, shoal.NewError(
				shoal.ErrorInvalidArgument,
				"exact node and assertion evidence requires an analytics interaction",
			)
		}
	}
	if err := validateInteractionSourceEdges(session); err != nil {
		return nil, err
	}
	nodeIDs := session.TouchedNodeIDs()
	registrations, err := c.resolveNodes(ctx, nodeIDs)
	if err != nil {
		return nil, err
	}
	visibilitySets := make(
		[][]string, 0, len(nodeIDs)+len(session.TouchedEdgeIDs()))
	for _, nodeID := range nodeIDs {
		registration, ok := registrations[nodeID]
		if !ok {
			return nil, auth.ObjectNotFound()
		}
		allowed, err := ruleAllows(
			registration.Rule, decision, operation, now)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, auth.ObjectNotFound()
		}
		labels, err := accessRuleVisibility(registration.Rule)
		if err != nil {
			return nil, err
		}
		visibilitySets = append(visibilitySets, labels)
	}
	edges := interactionSourceEdges(session)
	edgeIDs := make([]shoal.ID, len(edges))
	for index, edge := range edges {
		edgeIDs[index] = edge.ID
	}
	resolvedEdges, err := c.resolveEdges(ctx, edgeIDs)
	if err != nil {
		return nil, err
	}
	touchedNodes := make(map[shoal.ID]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		touchedNodes[nodeID] = struct{}{}
	}
	for _, edge := range edges {
		if _, ok := touchedNodes[edge.From]; !ok {
			return nil, auth.ObjectNotFound()
		}
		if _, ok := touchedNodes[edge.To]; !ok {
			return nil, auth.ObjectNotFound()
		}
		registration, ok := resolvedEdges[edge.ID]
		if !ok || !graphEdgesEqual(registration.Edge, edge) {
			return nil, auth.ObjectNotFound()
		}
		allowed, err := edgeAllowsResolved(
			registrations, registration, decision, operation, now)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, auth.ObjectNotFound()
		}
		labels, err := accessRuleVisibility(registration.Rule)
		if err != nil {
			return nil, err
		}
		visibilitySets = append(visibilitySets, labels)
	}
	return interaction.Conjoin(visibilitySets...)
}

type analyticsInteractionGraph struct {
	Nodes      []graph.Node
	Edges      []graph.Edge
	Assertions []interaction.AssertionEvidence
}

func analyticsInteractionEvidence(
	session interaction.Session,
) (analyticsInteractionGraph, bool, error) {
	var evidence analyticsInteractionGraph
	analyticsCalls := 0
	for _, turn := range session.Turns {
		if turn.ToolCall != nil && turn.ToolCall.Kind == "analytics" {
			analyticsCalls++
		}
	}
	if analyticsCalls == 0 {
		return evidence, false, nil
	}
	if analyticsCalls != 1 {
		return analyticsInteractionGraph{}, false, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"analytics interaction has multiple analytics tool calls",
		)
	}
	if len(session.CitedNodeIDs) != 0 || len(session.CitedEdges) != 0 {
		return analyticsInteractionGraph{}, false, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"analytics interaction has evidence outside its analytics tool call",
		)
	}
	for _, turn := range session.Turns {
		if turn.ToolCall == nil {
			continue
		}
		if turn.ToolCall.Kind != "analytics" {
			if len(turn.ToolCall.RetrievedNodeIDs) != 0 ||
				len(turn.ToolCall.RetrievedNodes) != 0 ||
				len(turn.ToolCall.RetrievedEdges) != 0 ||
				len(turn.ToolCall.RetrievedAssertions) != 0 {
				return analyticsInteractionGraph{}, false, shoal.NewError(
					shoal.ErrorInvalidArgument,
					"analytics interaction has evidence outside its analytics tool call",
				)
			}
			continue
		}
		evidence.Nodes = append(
			[]graph.Node(nil), turn.ToolCall.RetrievedNodes...)
		evidence.Edges = append(
			[]graph.Edge(nil), turn.ToolCall.RetrievedEdges...)
		evidence.Assertions = append(
			[]interaction.AssertionEvidence(nil),
			turn.ToolCall.RetrievedAssertions...,
		)
		nodeIDs := make([]shoal.ID, len(evidence.Nodes))
		for index, node := range evidence.Nodes {
			nodeIDs[index] = node.ID
		}
		sort.Slice(nodeIDs, func(i, j int) bool {
			return shoal.CompareID(nodeIDs[i], nodeIDs[j]) < 0
		})
		expected := append(
			[]shoal.ID(nil), turn.ToolCall.RetrievedNodeIDs...)
		sort.Slice(expected, func(i, j int) bool {
			return shoal.CompareID(expected[i], expected[j]) < 0
		})
		if !equalInteractionIDs(nodeIDs, expected) {
			return analyticsInteractionGraph{}, false, shoal.NewError(
				shoal.ErrorInvalidArgument,
				"analytics interaction node evidence is inconsistent",
			)
		}
		if len(session.SeedNodeIDs) == 0 {
			return analyticsInteractionGraph{}, false, shoal.NewError(
				shoal.ErrorInvalidArgument,
				"analytics interaction requires at least one seed node",
			)
		}
		retrieved := make(map[shoal.ID]struct{}, len(nodeIDs))
		for _, nodeID := range nodeIDs {
			retrieved[nodeID] = struct{}{}
		}
		for _, seedNodeID := range session.SeedNodeIDs {
			if _, ok := retrieved[seedNodeID]; !ok {
				return analyticsInteractionGraph{}, false, shoal.NewError(
					shoal.ErrorInvalidArgument,
					"analytics interaction seed is absent from exact node evidence",
				)
			}
		}
	}
	return evidence, true, nil
}

func equalInteractionIDs(left, right []shoal.ID) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (c *Client) authorizeAnalyticsInteractionEvidence(
	ctx context.Context,
	raw analyticsInteractionGraph,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) ([]string, error) {
	rawNodes := make(map[shoal.ID]graph.Node, len(raw.Nodes))
	nodeIDs := make([]shoal.ID, 0, len(raw.Nodes))
	for _, node := range raw.Nodes {
		if err := node.Validate(); err != nil {
			return nil, inconsistentBase()
		}
		if _, duplicate := rawNodes[node.ID]; duplicate {
			return nil, inconsistentBase()
		}
		rawNodes[node.ID] = cloneGraphNode(node)
		nodeIDs = append(nodeIDs, node.ID)
	}
	resolved, err := c.resolveNodes(ctx, nodeIDs)
	if err != nil {
		return nil, err
	}
	visibleNodes := make(map[shoal.ID]graph.Node, len(raw.Nodes))
	registrations := make(map[shoal.ID]NodeRegistration, len(resolved))
	visibilitySets := make([][]string, 0, len(raw.Nodes)+len(raw.Edges))
	for _, node := range raw.Nodes {
		registration, ok := resolved[node.ID]
		if !ok {
			if graph.IsProvenanceKind(node.Kind) {
				continue
			}
			return nil, auth.ObjectNotFound()
		}
		allowed, err := ruleAllows(
			registration.Rule, decision, operation, now)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, auth.ObjectNotFound()
		}
		visibleNodes[node.ID] = cloneGraphNode(node)
		registrations[node.ID] = registration
		labels, err := accessRuleVisibility(registration.Rule)
		if err != nil {
			return nil, err
		}
		visibilitySets = append(visibilitySets, labels)
	}
	canonical, err := c.canonicalRegisteredNodes(ctx, registrations)
	if err != nil {
		return nil, err
	}
	for nodeID, node := range visibleNodes {
		if !graphNodesEqual(canonical[nodeID], node) {
			return nil, inconsistentBase()
		}
	}
	assertionsByEdge, err := interactionAssertionsByEdge(raw.Assertions)
	if err != nil {
		return nil, err
	}
	verifier, ok := c.base.(explorer.InteractionEvidenceVerifier)
	if !ok || isNilDependency(verifier) {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"underlying Explorer cannot verify exact interaction evidence")
	}
	if err := verifier.VerifyInteractionEvidence(
		ctx, raw.Nodes, raw.Edges, raw.Assertions,
	); err != nil {
		return nil, directBaseError(err)
	}
	candidateEdgeIDs := make([]shoal.ID, 0, len(raw.Edges))
	for _, edge := range raw.Edges {
		if assertion, derived := assertionsByEdge[edge.ID]; (derived &&
			assertion.Origin == string(ontology.AssertionDerived)) ||
			edge.Type == graph.EdgeTypeProduced {
			continue
		}
		candidateEdgeIDs = append(candidateEdgeIDs, edge.ID)
	}
	resolvedEdges, err := c.resolveEdges(ctx, candidateEdgeIDs)
	if err != nil {
		return nil, err
	}
	admittedEdges := make(map[shoal.ID]struct{}, len(raw.Edges))
	admittedAssertions := make(map[shoal.ID]struct{}, len(raw.Assertions))
	assertionEndpoints := make(registeredNodes)
	for _, edge := range raw.Edges {
		if err := edge.Validate(); err != nil {
			return nil, inconsistentBase()
		}
		if assertion, ok := assertionsByEdge[edge.ID]; ok &&
			assertion.Origin == string(ontology.AssertionDerived) {
			endpoints, allowed, err := c.interactionAssertionAllows(
				ctx, assertion, decision, operation, now)
			if err != nil {
				return nil, err
			}
			if !allowed || !interactionAssertionMatchesEdge(assertion, edge) {
				return nil, auth.ObjectNotFound()
			}
			for nodeID, registration := range endpoints {
				node, ok := rawNodes[nodeID]
				if !ok || !graphNodesEqual(registration.Node, node) {
					return nil, inconsistentBase()
				}
				visibleNodes[nodeID] = cloneGraphNode(node)
				assertionEndpoints[nodeID] = registration
			}
			admittedEdges[edge.ID] = struct{}{}
			admittedAssertions[assertion.ID] = struct{}{}
			continue
		}
		if edge.Type == graph.EdgeTypeProduced {
			assertion, ok := assertionsByEdge[edge.To]
			if !ok {
				return nil, inconsistentBase()
			}
			endpoints, allowed, err := c.interactionAssertionAllows(
				ctx, assertion, decision, operation, now)
			if err != nil {
				return nil, err
			}
			if !allowed ||
				!interactionProducerEdgeMatches(edge, rawNodes, assertion) {
				return nil, auth.ObjectNotFound()
			}
			for _, registration := range endpoints {
				labels, err := accessRuleVisibility(registration.Rule)
				if err != nil {
					return nil, err
				}
				visibilitySets = append(visibilitySets, labels)
			}
			for nodeID, registration := range endpoints {
				assertionEndpoints[nodeID] = registration
			}
			visibleNodes[edge.From] = cloneGraphNode(rawNodes[edge.From])
			visibleNodes[edge.To] = cloneGraphNode(rawNodes[edge.To])
			admittedEdges[edge.ID] = struct{}{}
			admittedAssertions[assertion.ID] = struct{}{}
			continue
		}
		registration, ok := resolvedEdges[edge.ID]
		if !ok || !graphEdgesEqual(registration.Edge, edge) {
			return nil, auth.ObjectNotFound()
		}
		allowed, err := edgeAllowsResolved(
			resolved, registration, decision, operation, now)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, auth.ObjectNotFound()
		}
		labels, err := accessRuleVisibility(registration.Rule)
		if err != nil {
			return nil, err
		}
		visibilitySets = append(visibilitySets, labels)
		admittedEdges[edge.ID] = struct{}{}
		if assertion, ok := assertionsByEdge[edge.ID]; ok {
			admittedAssertions[assertion.ID] = struct{}{}
		}
	}
	if len(admittedEdges) != len(raw.Edges) ||
		len(visibleNodes) != len(raw.Nodes) {
		return nil, auth.ObjectNotFound()
	}
	for _, assertion := range raw.Assertions {
		if _, ok := admittedAssertions[assertion.ID]; !ok {
			return nil, auth.ObjectNotFound()
		}
		if assertion.ObjectReference == "" {
			continue
		}
		for _, nodeID := range []shoal.ID{
			assertion.Subject, assertion.ObjectReference,
		} {
			registration, ok := resolved[nodeID]
			if !ok {
				registration, ok = assertionEndpoints[nodeID]
				if !ok {
					return nil, auth.ObjectNotFound()
				}
			}
			labels, err := accessRuleVisibility(registration.Rule)
			if err != nil {
				return nil, err
			}
			visibilitySets = append(visibilitySets, labels)
		}
	}
	return interaction.Conjoin(visibilitySets...)
}

func interactionAssertionsByEdge(
	assertions []interaction.AssertionEvidence,
) (map[shoal.ID]interaction.AssertionEvidence, error) {
	result := make(
		map[shoal.ID]interaction.AssertionEvidence, len(assertions))
	for _, assertion := range assertions {
		if err := assertion.Validate(); err != nil {
			return nil, inconsistentBase()
		}

		edgeID := assertion.GraphEdgeID
		if assertion.Origin == string(ontology.AssertionDerived) {
			edgeID = assertion.ID
		}
		if edgeID == "" {
			continue
		}
		if _, duplicate := result[edgeID]; duplicate {
			return nil, inconsistentBase()
		}
		result[edgeID] = assertion
	}
	return result, nil
}

func (c *Client) interactionAssertionAllows(
	ctx context.Context,
	assertion interaction.AssertionEvidence,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (registeredNodes, bool, error) {
	if assertion.ObjectReference == "" {
		return nil, false, nil
	}
	resolved, err := c.resolveNodes(ctx, []shoal.ID{
		assertion.Subject, assertion.ObjectReference,
	})
	if err != nil {
		return nil, false, err
	}
	allowed, err := edgeEndpointsAllow(
		resolved,
		EdgeRegistration{Edge: graph.Edge{
			ID: assertion.ID, From: assertion.Subject,
			To: assertion.ObjectReference, Type: string(assertion.Predicate),
			Weight: assertion.Confidence,
		}},
		decision,
		operation,
		now,
	)
	if err != nil || !allowed {
		return resolved, allowed, err
	}
	if _, err := c.canonicalRegisteredNodes(ctx, resolved); err != nil {
		return nil, false, err
	}
	return resolved, true, nil
}

func interactionAssertionMatchesEdge(
	assertion interaction.AssertionEvidence,
	edge graph.Edge,
) bool {
	expectedProperties := shoal.Metadata{
		"ontology.assertion.origin":          assertion.Origin,
		derivedAssertionPropertyAssertionID:  string(assertion.ID),
		derivedAssertionPropertyDerivationID: string(assertion.DerivationID),
		derivedAssertionPropertyDerivationScore: strconv.FormatFloat(
			float64(assertion.DerivationScore), 'g', -1, 64),
	}
	return edge.ID == assertion.ID &&
		edge.From == assertion.Subject &&
		edge.To == assertion.ObjectReference &&
		edge.Type == string(assertion.Predicate) &&
		scoresEqual(edge.Weight, assertion.Confidence) &&
		metadataEqual(edge.Properties, expectedProperties)
}

func interactionProducerEdgeMatches(
	edge graph.Edge,
	rawNodes map[shoal.ID]graph.Node,
	assertion interaction.AssertionEvidence,
) bool {
	producer, ok := rawNodes[edge.From]
	if !ok || producer.Kind != graph.NodeKindProducer {
		return false
	}
	assertionNode, ok := rawNodes[edge.To]
	if !ok || assertionNode.Kind != graph.NodeKindDerivedAssertion ||
		assertionNode.ID != assertion.ID {
		return false
	}
	return edge.Weight == 1 && len(edge.Properties) == 2 &&
		edge.Properties[derivedAssertionPropertyAssertionID] ==
			string(assertion.ID) &&
		edge.Properties[derivedAssertionPropertyDerivationID] ==
			string(assertion.DerivationID)
}

func metadataEqual(left, right shoal.Metadata) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func interactionSourceEdges(session interaction.Session) []graph.Edge {
	edges := append([]graph.Edge(nil), session.CitedEdges...)
	for _, turn := range session.Turns {
		if turn.ToolCall != nil {
			edges = append(edges, turn.ToolCall.RetrievedEdges...)
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		return shoal.CompareID(edges[i].ID, edges[j].ID) < 0
	})
	result := edges[:0]
	for _, edge := range edges {
		if len(result) > 0 && result[len(result)-1].ID == edge.ID {
			continue
		}
		result = append(result, edge)
	}
	return result
}

func validateInteractionSourceEdges(session interaction.Session) error {
	edges := append([]graph.Edge(nil), session.CitedEdges...)
	for _, turn := range session.Turns {
		if turn.ToolCall != nil {
			edges = append(edges, turn.ToolCall.RetrievedEdges...)
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		return shoal.CompareID(edges[i].ID, edges[j].ID) < 0
	})
	for index := 1; index < len(edges); index++ {
		if edges[index-1].ID == edges[index].ID &&
			!graphEdgesEqual(edges[index-1], edges[index]) {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"interaction source edge ID has conflicting values",
			)
		}
	}
	return nil
}

func accessRuleVisibility(rule AccessRule) ([]string, error) {
	expression, err := auth.ConjoinPolicies(rule.components()...)
	if err != nil {
		return nil, inconsistentBase()
	}
	labels, err := interaction.ParseVisibility(string(expression))
	if err != nil {
		return nil, inconsistentBase()
	}
	return labels, nil
}

func interactionSourcesAllow(
	registrations registeredNodes,
	nodeIDs []shoal.ID,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (bool, error) {
	for _, nodeID := range nodeIDs {
		registration, ok := registrations[nodeID]
		if !ok {
			return false, nil
		}
		allowed, err := ruleAllows(
			registration.Rule, decision, operation, now)
		if err != nil {
			return false, err
		}
		if !allowed {
			return false, nil
		}
	}
	return true, nil
}

func interactionPinMatchesDecision(
	session interaction.Session, decision auth.Decision,
) bool {
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return false
	}
	return session.AuthorizationFingerprint == shoal.ID(fingerprint.String()) &&
		!decision.AuthenticationExpires().Before(session.AuthorizationExpiresAt)
}

func summaryFingerprintMatchesDecision(
	summary explorer.InteractionSummary, decision auth.Decision,
) bool {
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return false
	}
	return summary.AuthorizationFingerprint == shoal.ID(fingerprint.String())
}

func (c *Client) interactionWriter() (explorer.InteractionWriter, error) {
	writer, ok := c.base.(explorer.InteractionWriter)
	if !ok || isNilDependency(writer) {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"underlying Explorer has no durable interaction writer",
		)
	}
	return writer, nil
}

func (c *Client) interactionReader() (explorer.InteractionReader, error) {
	reader, ok := c.base.(explorer.InteractionReader)
	if !ok || isNilDependency(reader) {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"underlying Explorer has no interaction reader",
		)
	}
	return reader, nil
}

var (
	_ explorer.InteractionWriter       = (*Client)(nil)
	_ explorer.InteractionResultWriter = (*Client)(nil)
	_ explorer.InteractionReader       = (*Client)(nil)
	_ interaction.ResultSink           = operationInteractionSink{}
)
