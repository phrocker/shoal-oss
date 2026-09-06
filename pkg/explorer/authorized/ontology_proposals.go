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
	_, guard, _, err := c.begin(ctx, auth.OperationRead)
	if err != nil {
		return nil, err
	}
	proposals, err := store.OntologyProposals(ctx)
	if err != nil {
		return nil, directBaseError(err)
	}
	if err := guard.Check(ctx); err != nil {
		return nil, err
	}
	return proposals, nil
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
	_, guard, _, err := c.begin(ctx, auth.OperationIngest)
	if err != nil {
		return err
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

func (c *Client) TransitionOntologyProposal(
	ctx context.Context,
	proposalID shoal.ID,
	next ontology.ProposalState,
	actor, note string,
	at time.Time,
) (ontology.GovernedProposal, error) {
	store, err := c.ontologyProposalStore()
	if err != nil {
		return ontology.GovernedProposal{}, err
	}
	// This operation check is load-bearing; TestOntologyProposalEndpointDistinguishesAuthorizationDenial
	// pins that proposal mutations require write authority and surface a
	// governance 401 without a bearer challenge when the caller lacks it.
	_, guard, _, err := c.begin(ctx, auth.OperationIngest)
	if err != nil {
		return ontology.GovernedProposal{}, err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	if err := guard.Check(ctx); err != nil {
		return ontology.GovernedProposal{}, err
	}
	proposal, err := store.TransitionOntologyProposal(
		ctx, proposalID, next, actor, note, at)
	if err != nil {
		return ontology.GovernedProposal{}, directBaseError(err)
	}
	if err := guard.Check(ctx); err != nil {
		return ontology.GovernedProposal{}, err
	}
	return proposal, nil
}

func (c *Client) ontologyProposalStore() (explorer.OntologyProposalStore, error) {
	store, ok := c.base.(explorer.OntologyProposalStore)
	if !ok {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable, "workspace capability \"ontology proposals\" is unavailable")
	}
	return store, nil
}
