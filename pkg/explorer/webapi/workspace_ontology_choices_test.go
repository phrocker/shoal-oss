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

package webapi

import (
	"context"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type mutableGovernedOntologySource struct {
	active        ontology.OntologyVersion
	configured    bool
	proposals     []ontology.GovernedProposal
	activeCalls   int
	changeOnCheck *ontology.OntologyVersion
}

func (s *mutableGovernedOntologySource) ActiveOntology(
	context.Context,
) (ontology.OntologyVersion, bool, error) {
	s.activeCalls++
	if s.activeCalls > 1 && s.changeOnCheck != nil {
		return *s.changeOnCheck, true, nil
	}
	return s.active, s.configured, nil
}

func (s *mutableGovernedOntologySource) OntologyProposals(
	context.Context,
) ([]ontology.GovernedProposal, error) {
	return append([]ontology.GovernedProposal(nil), s.proposals...), nil
}

func TestGovernedOntologyChoicesUsesLivePublishedAncestry(t *testing.T) {
	first, second, third, published := governedOntologyFixture(t)
	source := &mutableGovernedOntologySource{
		active: second, configured: true,
		proposals: []ontology.GovernedProposal{published},
	}
	choices, err := NewGovernedOntologyChoices(source)
	if err != nil {
		t.Fatal(err)
	}
	listed, err := choices.ListOntologyChoices(
		context.Background(), auth.Decision{})
	if err != nil {
		t.Fatal(err)
	}
	firstIdentity, _ := ontology.NewOntologyIdentity(first)
	secondIdentity, _ := ontology.NewOntologyIdentity(second)
	thirdIdentity, _ := ontology.NewOntologyIdentity(third)
	if len(listed) != 2 ||
		listed[0].Identity != secondIdentity || !listed[0].Active ||
		listed[1].Identity != firstIdentity || listed[1].Active {
		t.Fatalf("published choices = %#v", listed)
	}
	if err := choices.AuthorizeOntology(
		context.Background(), auth.Decision{}, firstIdentity); err != nil {
		t.Fatalf("retained published ancestor: %v", err)
	}
	if err := choices.AuthorizeOntology(
		context.Background(), auth.Decision{}, thirdIdentity,
	); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("unpublished choice error = %v", err)
	}

	source.active = first
	source.proposals = nil
	source.activeCalls = 0
	listed, err = choices.ListOntologyChoices(
		context.Background(), auth.Decision{})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].Identity != firstIdentity ||
		!listed[0].Active {
		t.Fatalf("live active choice = %#v", listed)
	}
}

func TestGovernedOntologyChoicesRejectsMovingActiveSnapshot(t *testing.T) {
	first, second, _, _ := governedOntologyFixture(t)
	source := &mutableGovernedOntologySource{
		active: first, configured: true,
		changeOnCheck: &second,
	}
	choices, err := NewGovernedOntologyChoices(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := choices.ListOntologyChoices(
		context.Background(), auth.Decision{},
	); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("moving active snapshot error = %v", err)
	}
}

func governedOntologyFixture(
	t *testing.T,
) (
	ontology.OntologyVersion,
	ontology.OntologyVersion,
	ontology.OntologyVersion,
	ontology.GovernedProposal,
) {
	t.Helper()
	now := time.Date(2026, 9, 6, 1, 0, 0, 0, time.UTC)
	schema, err := ontology.NewOntologySchema(
		"workspace", "Workspace", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := ontology.NewOntologyVersion(
		schema, "1", now, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ontology.NewOntologyVersion(
		schema, "2", now.Add(time.Minute), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	third, err := ontology.NewOntologyVersion(
		schema, "3", now.Add(2*time.Minute), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := ontology.NewGovernedProposal(
		schema, first, second, "author", "refine", now.Add(3*time.Minute), nil)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err = proposal.Transition(
		ontology.ProposalSubmitted, "author", "submit", now.Add(4*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	proposal, err = proposal.Transition(
		ontology.ProposalApproved, "reviewer", "approve", now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	proposal, err = proposal.Transition(
		ontology.ProposalPublished, "publisher", "publish", now.Add(6*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return first, second, third, proposal
}
