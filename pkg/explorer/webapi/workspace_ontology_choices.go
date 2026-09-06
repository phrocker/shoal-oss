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

package webapi

import (
	"context"
	"reflect"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/workspace"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// GovernedOntologySource exposes the corrected RDO active pointer and durable
// proposal history without giving settings any publication authority.
type GovernedOntologySource interface {
	ActiveOntology(context.Context) (ontology.OntologyVersion, bool, error)
	OntologyProposals(context.Context) ([]ontology.GovernedProposal, error)
}

// GovernedOntologyChoices adapts the live RDO publication state to the
// workspace settings eligibility contract.
type GovernedOntologyChoices struct {
	source GovernedOntologySource
}

// NewGovernedOntologyChoices constructs a live, read-only choice adapter.
func NewGovernedOntologyChoices(
	source GovernedOntologySource,
) (*GovernedOntologyChoices, error) {
	if absentGovernedOntologySource(source) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"governed ontology source is required",
		)
	}
	return &GovernedOntologyChoices{source: source}, nil
}

// ListOntologyChoices returns only the active ontology and its retained
// published ancestry. Disconnected or unpublished proposal versions are never
// selectable.
func (c *GovernedOntologyChoices) ListOntologyChoices(
	ctx context.Context,
	_ auth.Decision,
) ([]workspace.OntologyChoice, error) {
	active, configured, err := c.source.ActiveOntology(ctx)
	if err != nil {
		return nil, err
	}
	if !configured {
		return []workspace.OntologyChoice{}, nil
	}
	activeIdentity, err := ontology.NewOntologyIdentity(active)
	if err != nil {
		return nil, err
	}
	proposals, err := c.source.OntologyProposals(ctx)
	if err != nil {
		return nil, err
	}
	confirmed, stillConfigured, err := c.source.ActiveOntology(ctx)
	if err != nil {
		return nil, err
	}
	if !stillConfigured {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"active ontology changed while listing workspace choices",
		)
	}
	confirmedIdentity, err := ontology.NewOntologyIdentity(confirmed)
	if err != nil {
		return nil, err
	}
	if confirmedIdentity != activeIdentity {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable,
			"active ontology changed while listing workspace choices",
		)
	}

	byTarget := make(map[shoal.ID]ontology.GovernedProposal)
	for _, proposal := range proposals {
		if proposal.State() != ontology.ProposalPublished ||
			proposal.Schema().ID() != activeIdentity.SchemaID() {
			continue
		}
		target := proposal.ProposedVersion().ID()
		if _, duplicate := byTarget[target]; duplicate {
			return nil, shoal.NewError(
				shoal.ErrorConflict,
				"published ontology history has duplicate targets",
			)
		}
		byTarget[target] = proposal
	}
	choices := []workspace.OntologyChoice{{
		Identity: activeIdentity,
		Active:   true,
	}}
	current := activeIdentity.VersionID()
	visited := map[shoal.ID]struct{}{current: {}}
	for range MaxOntologyProposals {
		proposal, ok := byTarget[current]
		if !ok {
			return choices, nil
		}
		baseID, ok := proposal.BaseVersionID()
		if !ok {
			return choices, nil
		}
		if _, cycle := visited[baseID]; cycle {
			return nil, shoal.NewError(
				shoal.ErrorConflict,
				"published ontology history contains a cycle",
			)
		}
		visited[baseID] = struct{}{}
		base, err := ontology.NewOntologyIdentityFromIDs(
			activeIdentity.SchemaID(), baseID)
		if err != nil {
			return nil, err
		}
		choices = append(choices, workspace.OntologyChoice{Identity: base})
		current = baseID
	}
	return nil, shoal.NewError(
		shoal.ErrorUnavailable,
		"published ontology history exceeds the service bound",
	)
}

// AuthorizeOntology permits only identities returned by the current live
// eligibility snapshot.
func (c *GovernedOntologyChoices) AuthorizeOntology(
	ctx context.Context,
	decision auth.Decision,
	identity ontology.OntologyIdentity,
) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	choices, err := c.ListOntologyChoices(ctx, decision)
	if err != nil {
		return err
	}
	for _, choice := range choices {
		if choice.Identity == identity {
			return nil
		}
	}
	return shoal.NewError(shoal.ErrorUnauthorized, "authorization denied")
}

func absentGovernedOntologySource(value GovernedOntologySource) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
