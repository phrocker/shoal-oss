// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package authorized

import (
	"context"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func (c *Client) OntologyProposals(
	ctx context.Context,
) ([]ontology.GovernedProposal, error) {
	store, err := c.ontologyProposalStore()
	if err != nil {
		return nil, err
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return nil, err
	}
	proposals, err := store.OntologyProposals(ctx)
	if err != nil {
		return nil, directBaseError(err)
	}
	visible := make([]ontology.GovernedProposal, 0, len(proposals))
	for _, proposal := range proposals {
		allowed, err := c.ontologyProposalEvidenceAllows(
			ctx, proposal, decision, auth.OperationRead, now)
		if err != nil {
			return nil, err
		}
		if allowed {
			visible = append(visible, proposal)
		}
	}
	if err := guard.Check(ctx); err != nil {
		return nil, err
	}
	return visible, nil
}

func (c *Client) CreateOntologyProposal(
	ctx context.Context,
	proposal ontology.GovernedProposal,
	baseVersion ontology.OntologyVersion,
) error {
	store, err := c.ontologyProposalStore()
	if err != nil {
		return err
	}

	// This operation check is load-bearing; TestOntologyProposalEndpointDistinguishesAuthorizationDenial
	// pins that proposal mutations require write authority and surface a
	// governance 401 without a bearer challenge when the caller lacks it.
	decision, guard, now, err := c.begin(ctx, auth.OperationIngest)
	if err != nil {
		return err
	}
	if err := proposal.Validate(); err != nil {
		return err
	}
	allowed, err := c.ontologyProposalEvidenceAllows(
		ctx, proposal, decision, auth.OperationIngest, now)
	if err != nil {
		return err
	}
	if !allowed {
		return auth.ObjectNotFound()
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if err := guard.Check(ctx); err != nil {
		return err
	}
	if err := store.CreateOntologyProposal(ctx, proposal, baseVersion); err != nil {
		return directBaseError(err)
	}
	return guard.Check(ctx)
}

func (c *Client) OntologyProposalMutationState(
	ctx context.Context,
	configured ontology.OntologyVersion,
	proposalID shoal.ID,
) (explorer.OntologyProposalMutationState, error) {
	store, ok := c.ontologyProposals.(explorer.OntologyProposalMutationStateProvider)
	if !ok {
		return explorer.OntologyProposalMutationState{}, shoal.NewError(
			shoal.ErrorUnavailable,
			"workspace capability \"ontology proposal mutation state\" is unavailable",
		)
	}
	decision, guard, now, err := c.begin(ctx, auth.OperationIngest)
	if err != nil {
		return explorer.OntologyProposalMutationState{}, err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if err := guard.Check(ctx); err != nil {
		return explorer.OntologyProposalMutationState{}, err
	}
	if proposalID != "" {
		evidenceStore, ok := c.ontologyProposals.(explorer.OntologyProposalEvidenceProvider)
		if !ok {
			return explorer.OntologyProposalMutationState{}, shoal.NewError(
				shoal.ErrorUnavailable,
				"workspace capability \"ontology proposal evidence\" is unavailable",
			)
		}
		evidence, found, err := evidenceStore.OntologyProposalEvidence(ctx, proposalID)
		if err != nil {
			return explorer.OntologyProposalMutationState{}, directBaseError(err)
		}
		if !found {
			return explorer.OntologyProposalMutationState{}, nil
		}
		for _, item := range evidence {
			allowed, evidenceErr := c.ontologyEvidenceAllows(
				ctx, item, decision, auth.OperationIngest, now)
			if evidenceErr != nil {
				return explorer.OntologyProposalMutationState{}, evidenceErr
			}
			if !allowed {
				return explorer.OntologyProposalMutationState{}, nil
			}
		}
		if err := guard.Check(ctx); err != nil {
			return explorer.OntologyProposalMutationState{}, err
		}
	}
	state, err := store.OntologyProposalMutationState(ctx, configured, proposalID)
	if err != nil {
		return explorer.OntologyProposalMutationState{}, directBaseError(err)
	}
	if err := guard.Check(ctx); err != nil {
		return explorer.OntologyProposalMutationState{}, err
	}
	return state, nil
}

func (c *Client) OntologyActiveState(
	ctx context.Context,
	configured ontology.OntologyVersion,
) (ontology.OntologyVersion, error) {
	provider, ok := c.ontologyProposals.(explorer.OntologyActiveStateProvider)
	if !ok {
		return ontology.OntologyVersion{}, shoal.NewError(
			shoal.ErrorUnavailable,
			"workspace capability \"active ontology state\" is unavailable",
		)
	}
	_, guard, _, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return ontology.OntologyVersion{}, err
	}
	active, err := provider.OntologyActiveState(ctx, configured)
	if err != nil {
		return ontology.OntologyVersion{}, directBaseError(err)
	}
	if err := guard.Check(ctx); err != nil {
		return ontology.OntologyVersion{}, err
	}
	return active, nil
}

func (c *Client) TransitionOntologyProposal(
	ctx context.Context,
	proposalID shoal.ID,
	next ontology.ProposalState,
	actor, note string,
	at time.Time,
) (ontology.GovernedProposal, error) {
	return c.transitionOntologyProposal(ctx, proposalID, next, actor, note, at, nil)
}

// TransitionOntologyProposalWithLimits keeps bounded transitions under the
// same mutation and evidence authority as the ordinary domain operation.
func (c *Client) TransitionOntologyProposalWithLimits(
	ctx context.Context,
	proposalID shoal.ID,
	next ontology.ProposalState,
	actor, note string,
	at time.Time,
	limits explorer.OntologyProjectionLimits,
) (ontology.GovernedProposal, error) {
	return c.transitionOntologyProposal(ctx, proposalID, next, actor, note, at, &limits)
}

func (c *Client) transitionOntologyProposal(
	ctx context.Context,
	proposalID shoal.ID,
	next ontology.ProposalState,
	actor, note string,
	at time.Time,
	limits *explorer.OntologyProjectionLimits,
) (ontology.GovernedProposal, error) {
	store, err := c.ontologyProposalStore()
	if err != nil {
		return ontology.GovernedProposal{}, err
	}
	// This operation check is load-bearing; TestOntologyProposalEndpointDistinguishesAuthorizationDenial
	// pins that proposal mutations require write authority and surface a
	// governance 401 without a bearer challenge when the caller lacks it.
	decision, guard, now, err := c.begin(ctx, auth.OperationIngest)
	if err != nil {
		return ontology.GovernedProposal{}, err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if err := guard.Check(ctx); err != nil {
		return ontology.GovernedProposal{}, err
	}
	evidenceStore, ok := c.ontologyProposals.(explorer.OntologyProposalEvidenceProvider)
	if !ok {
		return ontology.GovernedProposal{}, shoal.NewError(
			shoal.ErrorUnavailable,
			"workspace capability \"ontology proposal evidence\" is unavailable",
		)
	}
	evidence, found, err := evidenceStore.OntologyProposalEvidence(ctx, proposalID)
	if err != nil {
		return ontology.GovernedProposal{}, directBaseError(err)
	}
	if !found {
		return ontology.GovernedProposal{}, auth.ObjectNotFound()
	}
	for _, item := range evidence {
		allowed, evidenceErr := c.ontologyEvidenceAllows(
			ctx, item, decision, auth.OperationIngest, now)
		if evidenceErr != nil {
			return ontology.GovernedProposal{}, evidenceErr
		}
		if !allowed {
			return ontology.GovernedProposal{}, auth.ObjectNotFound()
		}
	}
	if err := guard.Check(ctx); err != nil {
		return ontology.GovernedProposal{}, err
	}
	var proposal ontology.GovernedProposal
	if limits == nil {
		proposal, err = store.TransitionOntologyProposal(
			ctx, proposalID, next, actor, note, at)
	} else {
		bounded, ok := store.(explorer.OntologyProposalBoundedTransitionStore)
		if !ok {
			return ontology.GovernedProposal{}, shoal.NewError(
				shoal.ErrorUnavailable,
				"workspace capability \"bounded ontology proposal transitions\" is unavailable",
			)
		}
		proposal, err = bounded.TransitionOntologyProposalWithLimits(
			ctx, proposalID, next, actor, note, at, *limits)
	}
	if err != nil {
		return ontology.GovernedProposal{}, directBaseError(err)
	}
	if err := guard.Check(ctx); err != nil {
		return ontology.GovernedProposal{}, explorer.MarkIndeterminateCommit(err)
	}
	return proposal, nil
}

func (c *Client) ontologyProposalStore() (explorer.OntologyProposalStore, error) {
	if isNilDependency(c.ontologyProposals) {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable, "workspace capability \"ontology proposals\" is unavailable")
	}
	return c.ontologyProposals, nil
}

func (c *Client) ontologyProposalEvidenceAllows(
	ctx context.Context,
	proposal ontology.GovernedProposal,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (bool, error) {
	if err := proposal.Validate(); err != nil {
		return false, inconsistentBase()
	}
	for _, morphism := range proposal.Morphisms() {
		for _, evidence := range morphism.Evidence() {
			allowed, err := c.ontologyEvidenceAllows(
				ctx, evidence, decision, operation, now)
			if err != nil || !allowed {
				return allowed, err
			}
		}
	}
	return true, nil
}

func (c *Client) ontologyEvidenceAllows(
	ctx context.Context,
	evidence ontology.EvidenceRef,
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) (bool, error) {
	if err := evidence.Validate(); err != nil {
		return false, inconsistentBase()
	}
	citation := evidence.Citation()
	if citation.SectionID == "" && citation.SpanID == "" {
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
	allowed, err := ruleAllows(registration.Rule, decision, operation, now)
	if err != nil || !allowed {
		return allowed, err
	}
	view, err := c.base.Document(ctx, citation.DocumentID, citation.RevisionID)
	if err != nil {
		return false, directBaseError(err)
	}
	if err := verifyDocumentViewRegistration(view, registration); err != nil {
		return false, err
	}
	canonical, err := buildCanonicalRetrievalDocument(view, registration)
	if err != nil {
		return false, inconsistentBase()
	}
	if citation.SectionID != "" {
		section, ok := canonical.sections[citation.SectionID]
		if !ok || section.Range.Start.Offset > citation.Range.Start.Offset ||
			citation.Range.End.Offset > section.Range.End.Offset {
			return false, nil
		}
	}
	if citation.SpanID != "" {
		span, ok := canonical.spans[citation.SpanID]
		if !ok || citation.SectionID != "" && span.SectionID != citation.SectionID ||
			span.Range.Start.Offset > citation.Range.Start.Offset ||
			citation.Range.End.Offset > span.Range.End.Offset {
			return false, nil
		}
	}
	resolver, ok := c.ontologyProposals.(explorer.OntologyEvidenceCitationResolver)
	if !ok {
		return false, shoal.NewError(
			shoal.ErrorUnavailable,
			"workspace capability \"ontology evidence citation\" is unavailable",
		)
	}
	quote, err := resolver.ResolveOntologyEvidenceCitation(ctx, citation)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) ||
			shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			return false, nil
		}
		return false, directBaseError(err)
	}
	if evidence.Quote() != "" && quote != evidence.Quote() {
		return false, nil
	}
	path, hasPath := evidence.Path()
	if !hasPath {
		return true, nil
	}
	nodeIDs := make([]shoal.ID, len(path.Nodes))
	for index, node := range path.Nodes {
		nodeIDs[index] = node.ID
	}
	nodes, err := c.resolveNodes(ctx, nodeIDs)
	if err != nil {
		return false, err
	}
	for _, node := range path.Nodes {
		nodeRegistration, ok := nodes[node.ID]
		if !ok {
			return false, nil
		}
		allowed, err := ruleAllows(
			nodeRegistration.Rule, decision, operation, now)
		if err != nil || !allowed {
			return allowed, err
		}
	}
	canonicalNodes, err := c.canonicalRegisteredNodes(ctx, nodes)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			return false, nil
		}
		return false, err
	}
	for _, node := range path.Nodes {
		if !graphNodesEqual(canonicalNodes[node.ID], node) {
			return false, nil
		}
	}
	for _, edge := range path.Edges {
		edgeRegistration, ok, err := c.policyStore.Edge(ctx, edge.ID)
		if err != nil {
			return false, policyCatalogReadError(ctx, err)
		}
		if !ok || !graphEdgesEqual(edgeRegistration.Edge, edge) {
			return false, nil
		}
		allowed, err := c.edgeAllows(
			ctx, edgeRegistration, decision, operation, now)
		if err != nil || !allowed {
			return allowed, err
		}
	}
	return true, nil
}
