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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestReplayPublishedOntologyExactBound(t *testing.T) {
	base := ontologyBoundVersion(t, 0)
	active := base
	var proposals []ontology.GovernedProposal
	for index := 1; index <= int(MaxOntologyProposals)+1; index++ {
		next := ontologyBoundVersion(t, index)
		proposal := ontologyBoundProposal(t, active, next)
		for offset, state := range []ontology.ProposalState{
			ontology.ProposalSubmitted, ontology.ProposalApproved, ontology.ProposalPublished,
		} {
			var err error
			proposal, err = proposal.Transition(
				state, "reviewer", "advance",
				proposal.CreatedAt().Add(time.Duration(offset+1)*time.Nanosecond))
			if err != nil {
				t.Fatal(err)
			}
		}
		proposals = append(proposals, proposal)
		active = next
		if index < int(MaxOntologyProposals)-1 {
			continue
		}
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			got, err := replayPublishedOntology(base, proposals)
			if index > int(MaxOntologyProposals) {
				if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
					t.Fatalf("overflow replay = %v", err)
				}
			} else if err != nil || got.ID() != active.ID() {
				t.Fatalf("replay at %d = %q, %v; want %q", index, got.ID(), err, active.ID())
			}
		})
	}
}

func TestActiveOntologyExactBoundSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	corpus, err := explorer.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = corpus.Close() })
	base := ontologyBoundVersion(t, 0)
	service := ontologyBoundService(t, corpus, base)
	for index := 1; index <= int(MaxOntologyProposals); index++ {
		proposal, err := service.CreateOntologyProposal(ctx, CreateOntologyProposalRequest{
			Rationale:       "bounded chain",
			ProposedVersion: OntologyProposalVersionDraft{Version: fmt.Sprint(index)},
		})
		if err != nil {
			t.Fatalf("create %d: %v", index, err)
		}
		for _, state := range []ontology.ProposalState{
			ontology.ProposalSubmitted, ontology.ProposalApproved, ontology.ProposalPublished,
		} {
			if _, err := service.TransitionOntologyProposal(ctx, proposal.ID(),
				TransitionOntologyProposalRequest{State: string(state), Note: "advance"}); err != nil {
				t.Fatalf("transition %d to %s: %v", index, state, err)
			}
		}
		if index < int(MaxOntologyProposals)-1 {
			continue
		}
		for _, reopen := range []bool{false, true} {
			if reopen {
				if err := corpus.Close(); err != nil {
					t.Fatal(err)
				}
				corpus, err = explorer.Open(directory)
				if err != nil {
					t.Fatal(err)
				}
				service = ontologyBoundService(t, corpus, base)
			}
			active, configured, err := service.ActiveOntology(ctx)
			if err != nil || !configured || active.ID() != proposal.ProposedVersion().ID() {
				t.Fatalf("active at %d, reopen %v = %q, %v, %v",
					index, reopen, active.ID(), configured, err)
			}
		}
	}
	_, err = service.CreateOntologyProposal(ctx, CreateOntologyProposalRequest{
		Rationale:       "overflow must not persist",
		ProposedVersion: OntologyProposalVersionDraft{Version: "overflow"},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("overflow creation = %v", err)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}
	corpus, err = explorer.Open(directory)
	if err != nil {
		t.Fatal(err)
	}
	service = ontologyBoundService(t, corpus, base)
	proposals, err := service.OntologyProposals(ctx)
	if err != nil || len(proposals) != int(MaxOntologyProposals) {
		t.Fatalf("persisted proposals after rejected overflow = %d, %v", len(proposals), err)
	}
	active, configured, err := service.ActiveOntology(ctx)
	if err != nil || !configured || active.Version() != fmt.Sprint(MaxOntologyProposals) {
		t.Fatalf("active after rejected overflow/reopen = %q, %v, %v",
			active.Version(), configured, err)
	}
}

func TestOntologyProposalBoundIsAtomicAcrossServices(t *testing.T) {
	ctx := context.Background()
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	base := ontologyBoundVersion(t, 0)
	first := ontologyBoundProposal(t, base, ontologyBoundVersion(t, 1))
	for index := 1; index < int(MaxOntologyProposals); index++ {
		proposal := ontologyBoundProposal(t, base, ontologyBoundVersion(t, index))
		if err := corpus.CreateOntologyProposal(ctx, proposal, base); err != nil {
			t.Fatal(err)
		}
	}
	services := []*EmbeddedService{
		ontologyBoundService(t, corpus, base),
		ontologyBoundService(t, corpus, base),
	}
	start := make(chan struct{})
	results := make(chan error, len(services))
	var workers sync.WaitGroup
	for index, service := range services {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, err := service.CreateOntologyProposal(ctx, CreateOntologyProposalRequest{
				Rationale:       "contend for the final slot",
				ProposedVersion: OntologyProposalVersionDraft{Version: fmt.Sprintf("candidate-%d", index)},
			})
			results <- err
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
			t.Fatalf("admission error = %v", err)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted %d competing proposals, want one", accepted)
	}
	if err := corpus.CreateOntologyProposal(ctx, first, base); err != nil {
		t.Fatalf("identical retry at capacity = %v", err)
	}
	extra := ontologyBoundProposal(t, base, ontologyBoundVersion(t, 999))
	if err := corpus.CreateOntologyProposal(ctx, extra, base); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("direct-store overflow = %v", err)
	}
	proposals, err := corpus.OntologyProposals(ctx)
	if err != nil || len(proposals) != int(MaxOntologyProposals) {
		t.Fatalf("final proposal count = %d, %v", len(proposals), err)
	}
}

func ontologyBoundVersion(t *testing.T, index int) ontology.OntologyVersion {
	t.Helper()
	schema, err := ontology.NewOntologySchema("bounded", "Bounded", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	version, err := ontology.NewOntologyVersion(
		schema, fmt.Sprint(index), time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC),
		nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return version
}

func ontologyBoundProposal(
	t *testing.T, base, next ontology.OntologyVersion,
) ontology.GovernedProposal {
	t.Helper()
	proposal, err := ontology.NewGovernedProposal(
		base.Schema(), base, next, "proposer", "bounded proposal", next.CreatedAt(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return proposal
}

func ontologyBoundService(
	t *testing.T, corpus *explorer.Explorer, base ontology.OntologyVersion,
) *EmbeddedService {
	t.Helper()
	service, err := NewEmbeddedService(corpus)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SetOntologyVersion(base); err != nil {
		t.Fatal(err)
	}
	return service
}
