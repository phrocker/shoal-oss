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
	"testing"

	"github.com/phrocker/shoal-oss/internal/disclosureconformance"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/workspace"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/model"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func corpusBound(source, policy []byte) *retrieval.SourceBound {
	return &retrieval.SourceBound{Scopes: []retrieval.BoundScope{{
		SourceID: source, PolicyID: policy,
	}}}
}

func boundChatService(t *testing.T, corpus *disclosureconformance.Corpus) *ChatService {
	t.Helper()
	provenance, err := inference.NewModelProvenance("fake", "fake", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewChatService(context.Background(), ChatConfig{
		Client: corpus.Client, Resolver: corpus.Resolver(),
		Generator: model.FakeGenerator{Model: "fake"}, Model: provenance,
		RetrievalModes: []retrieval.Mode{retrieval.ModeLexical},
		Clock:          corpus.Clock(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func boundedAsk(
	t *testing.T, service *ChatService, ctx context.Context, term string,
	bound *retrieval.SourceBound,
) askOutcome {
	t.Helper()
	envelope, err := service.Ask(ctx, AskRequest{Question: term, TopK: 8, Bound: bound})
	if err != nil {
		return askOutcome{ErrorCode: primaryErrorCode(err), ErrorText: err.Error()}
	}
	return askOutcome{Envelope: &envelope}
}

// TestBoundedAskGroundsOnlyInItsScope runs the real reasoning path for a
// principal that can read both compartments, confined to one (#560): the
// answer cites only the bound compartment, and asking for something only the
// other compartment holds looks exactly like asking for something that does
// not exist.
func TestBoundedAskGroundsOnlyInItsScope(t *testing.T) {
	corpus := disclosureconformance.NewCorpus(t)
	service := boundChatService(t, corpus)
	openBound := corpusBound(corpus.OpenScope())
	cleared := corpus.Cleared(t)

	both := boundedAsk(t, service, cleared,
		disclosureconformance.OpenTerm+" "+disclosureconformance.RestrictedTerm, nil)
	if both.Envelope == nil {
		t.Fatalf("control: the cleared principal must be answerable: %s", both.ErrorText)
	}
	reachesRestricted := false
	for _, evidence := range both.Envelope.Evidence {
		if evidence.Citation != nil &&
			evidence.Citation.DocumentID == corpus.RestrictedNodeID() {
			reachesRestricted = true
		}
	}
	if !reachesRestricted {
		t.Fatal("control: the unbounded answer must draw on both compartments")
	}
	confined := boundedAsk(t, service, cleared,
		disclosureconformance.OpenTerm+" "+disclosureconformance.RestrictedTerm, openBound)
	if confined.Envelope == nil {
		t.Fatalf("a bound on a readable compartment must still answer: %s", confined.ErrorText)
	}
	if len(confined.Envelope.Evidence) == 0 {
		t.Fatal("the confined answer must carry evidence")
	}
	for _, evidence := range confined.Envelope.Evidence {
		if evidence.Citation != nil &&
			evidence.Citation.DocumentID == corpus.RestrictedNodeID() {
			t.Fatalf("a bounded answer cites the other compartment: %#v", evidence)
		}
		for _, source := range evidence.SourceIDs {
			if source == corpus.RestrictedNodeID() {
				t.Fatalf("a bounded answer names the other compartment's node: %#v", evidence)
			}
		}
		if evidence.Path != nil {
			for _, node := range evidence.Path.Nodes {
				if node.ID == corpus.RestrictedNodeID() {
					t.Fatalf("a bounded answer's path enters the other compartment: %#v", evidence)
				}
			}
		}
	}

	// Bound-excluded but readable is indistinguishable from absent.
	disclosureconformance.Run(t, disclosureconformance.Probe{
		Name: "ask/bound-excluded-vs-absent",
		Withheld: boundedAsk(t, service, cleared,
			disclosureconformance.RestrictedTerm, openBound),
		Control: boundedAsk(t, service, cleared,
			disclosureconformance.AbsentTerm, openBound),
	})
	// And for the uncleared principal, a bound changes nothing about what is
	// hidden from it.
	uncleared := corpus.Uncleared(t)
	disclosureconformance.Run(t, disclosureconformance.Probe{
		Name: "ask/bounded-withheld-vs-absent",
		Withheld: boundedAsk(t, service, uncleared,
			disclosureconformance.RestrictedTerm, openBound),
		Control: boundedAsk(t, service, uncleared,
			disclosureconformance.AbsentTerm, openBound),
	})
}

// TestBoundedAskAuthorizesItsScope pins that a bounded question is admitted
// on its own scope rather than domain-wide (#560), so a bound can never be a
// way to retrieve on a scope the caller holds no grant on, and that a bound
// naming nothing is refused before any read.
func TestBoundedAskAuthorizesItsScope(t *testing.T) {
	corpus := disclosureconformance.NewCorpus(t)
	service := boundChatService(t, corpus)
	uncleared := corpus.Uncleared(t)

	outcome := boundedAsk(t, service, uncleared, disclosureconformance.OpenTerm,
		corpusBound(corpus.RestrictedScope()))
	if outcome.ErrorCode != shoal.ErrorUnauthorized {
		t.Fatalf("bounded ask on an ungranted scope = %q (%s), want unauthorized",
			outcome.ErrorCode, outcome.ErrorText)
	}
	outcome = boundedAsk(t, service, uncleared, disclosureconformance.OpenTerm,
		&retrieval.SourceBound{})
	if outcome.ErrorCode != shoal.ErrorInvalidArgument {
		t.Fatalf("bounded ask naming no scope = %q (%s), want invalid argument",
			outcome.ErrorCode, outcome.ErrorText)
	}
	outcome = boundedAsk(t, service, uncleared, disclosureconformance.OpenTerm,
		corpusBound(corpus.OpenScope()))
	if outcome.Envelope == nil {
		t.Fatalf("bounded ask on the caller's own scope = %q (%s)",
			outcome.ErrorCode, outcome.ErrorText)
	}
}

// TestChatReadSeamsCarryTheBound pins each read the reasoning path makes
// besides its first retrieval — tool retrieval, tool document reads, tool
// and context-builder graph expansion, and corpus listing — against the
// question's bound, for a principal that can read both compartments.
func TestChatReadSeamsCarryTheBound(t *testing.T) {
	corpus := disclosureconformance.NewCorpus(t)
	ctx := corpus.Cleared(t)
	limits := workspace.Limits{
		RetrievalTopK: 32, GraphDepth: 4, GraphFanout: 32,
		GraphNodes: MaxNodes, OutputBytes: 1 << 20,
	}
	bound := corpusBound(corpus.OpenScope())
	open, restricted := corpus.OpenNodeID(), corpus.RestrictedNodeID()
	unbounded := chatToolClient{Client: corpus.Client, limits: limits}
	bounded := chatToolClient{Client: corpus.Client, limits: limits, bound: bound}
	reader := chatContextReader{client: corpus.Client, limits: limits, bound: bound}

	query := retrieval.Request{
		Text: disclosureconformance.OpenTerm + " " + disclosureconformance.RestrictedTerm,
		TopK: 8,
	}
	cites := func(response retrieval.Response, id shoal.ID) bool {
		for _, result := range response.Results {
			for _, evidence := range result.Evidence {
				if evidence.Citation.DocumentID == id {
					return true
				}
			}
		}
		return false
	}
	control, err := unbounded.Retrieve(ctx, query)
	if err != nil || !cites(control, restricted) {
		t.Fatalf("control tool retrieval = %v, %v; it must reach both", control, err)
	}
	confined, err := bounded.Retrieve(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if cites(confined, restricted) || !cites(confined, open) {
		t.Fatalf("bounded tool retrieval = %#v", confined)
	}

	if _, err := unbounded.Document(ctx, restricted, ""); err != nil {
		t.Fatalf("control document read: %v", err)
	}
	for name, read := range map[string]func() error{
		"tool document": func() error {
			_, err := bounded.Document(ctx, restricted, "")
			return err
		},
		"context document": func() error {
			_, err := reader.Document(ctx, restricted, "")
			return err
		},
	} {
		if err := read(); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			t.Fatalf("%s outside the bound = %v, want not-found", name, err)
		}
	}

	shows := func(neighborhood explorer.Neighborhood, id shoal.ID) bool {
		for _, node := range neighborhood.Nodes {
			if node.ID == id {
				return true
			}
		}
		return false
	}
	request := explorer.NeighborhoodRequest{NodeIDs: []shoal.ID{open}, Depth: 1}
	around, err := unbounded.Neighborhood(ctx, request)
	if err != nil || !shows(around, restricted) {
		t.Fatalf("control expansion = %v, %v; it must reach the other compartment", around, err)
	}
	for name, expand := range map[string]func() (explorer.Neighborhood, error){
		"tool neighborhood": func() (explorer.Neighborhood, error) {
			return bounded.Neighborhood(ctx, request)
		},
		"context neighborhood": func() (explorer.Neighborhood, error) {
			return reader.Neighborhood(ctx, request)
		},
		"tool bounded neighborhood": func() (explorer.Neighborhood, error) {
			page, err := bounded.BoundedNeighborhood(ctx, explorer.BoundedNeighborhoodRequest{
				NodeIDs: []shoal.ID{open}, Depth: 1, Fanout: 32, MaxNodes: 64,
				MaxScannedEdges: 64, Direction: explorer.GraphDirectionBoth,
			})
			return page.Neighborhood, err
		},
	} {
		neighborhood, err := expand()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if shows(neighborhood, restricted) || !shows(neighborhood, open) {
			t.Fatalf("%s under the bound = %#v", name, neighborhood)
		}
	}

	if _, err := bounded.Documents(ctx); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("listing under a bound = %v, want unavailable", err)
	}
	if _, err := unbounded.Documents(ctx); err != nil {
		t.Fatalf("unbounded listing: %v", err)
	}
}
