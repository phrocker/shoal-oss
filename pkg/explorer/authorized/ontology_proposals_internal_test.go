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
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestPublishedOntologyRemainsDurableWhenFinalGenerationGuardFails(t *testing.T) {
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	schema, _ := ontology.NewOntologySchema("guarded", "Guarded", "", nil)
	at := time.Date(2026, time.September, 6, 0, 0, 0, 0, time.UTC)
	base, _ := ontology.NewOntologyVersion(schema, "1", at, nil, nil, nil, nil)
	target, _ := ontology.NewOntologyVersion(
		schema, "2", at.Add(time.Second), nil, nil, nil, nil)
	proposal, err := ontology.NewGovernedProposal(
		schema, base, target, "author", "publish", at.Add(2*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := corpus.CreateOntologyProposal(ctx, proposal, base); err != nil {
		t.Fatal(err)
	}
	for index, state := range []ontology.ProposalState{
		ontology.ProposalSubmitted, ontology.ProposalApproved,
	} {
		proposal, err = corpus.TransitionOntologyProposal(
			ctx, proposal.ID(), state, "governor", "approved",
			at.Add(time.Duration(index+3)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}

	generation := int64(1)
	baseClient := &generationChangingProposalBase{
		Explorer: corpus,
		after: func(next ontology.ProposalState) {
			if next == ontology.ProposalPublished {
				generation = 2
			}
		},
	}
	selector, err := NewStaticPolicySelector([]byte("source"), []byte("policy"))
	if err != nil {
		t.Fatal(err)
	}
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "subject", Actor: "actor",
		AuthorizationDomain: []byte("domain"),
		AllowedOperations:   []auth.Operation{auth.OperationIngest},
		PermittedSourceIDs:  [][]byte{[]byte("source")},
		PermittedPolicyIDs:  [][]byte{[]byte("policy")},
		PolicyGeneration:    1, AuthenticationExpires: at.Add(time.Hour),
		RequestID: "request",
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(Config{
		Base: baseClient,
		Resolver: resolverFunc(func(context.Context) (auth.Decision, error) {
			return decision, nil
		}),
		PolicySelector: selector, PolicyStore: NewMemoryPolicyStore(),
		GenerationReader: generationReaderFunc(
			func(context.Context, []byte) (int64, error) {
				return generation, nil
			}),
		Clock: func() time.Time { return at.Add(5 * time.Second) },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.TransitionOntologyProposal(
		ctx, proposal.ID(), ontology.ProposalPublished,
		"governor", "published", at.Add(5*time.Second))
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("post-commit generation change = %v, want unavailable", err)
	}
	if !explorer.IsIndeterminateCommit(err) {
		t.Fatalf("post-commit generation change lost indeterminate marker: %v", err)
	}
	stored, err := corpus.OntologyProposals(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored) != 1 || stored[0].State() != ontology.ProposalPublished {
		t.Fatalf("durable proposal after guard failure = %#v", stored)
	}
}

type generationChangingProposalBase struct {
	*explorer.Explorer
	after func(ontology.ProposalState)
}

func (b *generationChangingProposalBase) TransitionOntologyProposal(
	ctx context.Context,
	proposalID shoal.ID,
	next ontology.ProposalState,
	actor, note string,
	at time.Time,
) (ontology.GovernedProposal, error) {
	proposal, err := b.Explorer.TransitionOntologyProposal(
		ctx, proposalID, next, actor, note, at)
	if err == nil && b.after != nil {
		b.after(next)
	}
	return proposal, err
}
