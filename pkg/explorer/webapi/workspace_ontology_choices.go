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

// GovernedOntologyProposalSource exposes durable proposal history without
// routing through the generic graph-read authorization path.
type GovernedOntologyProposalSource interface {
	OntologyProposals(context.Context) ([]ontology.GovernedProposal, error)
}

// GovernedOntologyChoices adapts the live RDO publication state to the
// workspace settings eligibility contract.
type GovernedOntologyChoices struct {
	configured *ontology.OntologyVersion
	source     GovernedOntologyProposalSource
}

// NewGovernedOntologyChoices constructs a live, read-only choice adapter.
func NewGovernedOntologyChoices(
	configured *ontology.OntologyVersion,
	source GovernedOntologyProposalSource,
) (*GovernedOntologyChoices, error) {
	if absentGovernedOntologyProposalSource(source) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"governed ontology source is required",
		)
	}
	var cloned *ontology.OntologyVersion
	if configured != nil {
		if err := configured.Validate(); err != nil {
			return nil, err
		}
		value := *configured
		cloned = &value
	}
	return &GovernedOntologyChoices{
		configured: cloned,
		source:     source,
	}, nil
}

// ListOntologyChoices returns only the active ontology and its retained
// published ancestry. Disconnected or unpublished proposal versions are never
// selectable.
func (c *GovernedOntologyChoices) ListOntologyChoices(
	ctx context.Context,
	decision auth.Decision,
) ([]workspace.OntologyChoice, error) {
	catalog, configured, err := c.catalog(ctx)
	if err != nil {
		return nil, err
	}
	if !configured {
		return []workspace.OntologyChoice{}, nil
	}
	active := catalog.ActiveIdentity()
	versions := catalog.Versions()
	choices := make([]workspace.OntologyChoice, 0, len(versions))
	selected, selectedSet := decision.SelectedOntology()
	for index := len(versions) - 1; index >= 0; index-- {
		version := versions[index]
		identity, err := ontology.NewOntologyIdentity(version)
		if err != nil {
			return nil, err
		}
		if selectedSet && identity != selected {
			continue
		}
		choices = append(choices, workspace.OntologyChoice{
			Identity: identity,
			Version:  version.Version(),
			Active:   identity == active,
		})
	}
	return choices, nil
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
	if selected, ok := decision.SelectedOntology(); ok && selected != identity {
		return shoal.NewError(shoal.ErrorUnauthorized, "authorization denied")
	}
	catalog, configured, err := c.catalog(ctx)
	if err != nil {
		return err
	}
	if configured && catalog.Contains(identity) {
		return nil
	}
	return shoal.NewError(shoal.ErrorUnauthorized, "authorization denied")
}

func (c *GovernedOntologyChoices) catalog(
	ctx context.Context,
) (ontology.PublishedCatalog, bool, error) {
	if c.configured == nil {
		return ontology.PublishedCatalog{}, false, nil
	}
	proposals, err := c.source.OntologyProposals(ctx)
	if err != nil {
		return ontology.PublishedCatalog{}, false, err
	}
	catalog, err := boundedOntologyCatalog(*c.configured, proposals)
	if err != nil {
		return ontology.PublishedCatalog{}, false, err
	}
	return catalog, true, nil
}

func absentGovernedOntologyProposalSource(
	value GovernedOntologyProposalSource,
) bool {
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
