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

package analytics

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestAnalyzeEmptyGraph(t *testing.T) {
	got, err := Analyze(context.Background(), explorer.Neighborhood{}, PageRankOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 0 || len(got.WeaklyConnectedComponents) != 0 ||
		!got.PageRank.Converged || got.PageRank.Iterations != 0 {
		t.Fatalf("empty analysis = %#v", got)
	}
}

func TestAnalyzeDanglingGraphConvergesAndPreservesMass(t *testing.T) {
	got, err := Analyze(context.Background(), explorer.Neighborhood{
		Nodes: []graph.Node{{ID: "A"}, {ID: "B"}},
		Edges: []graph.Edge{{
			ID: "A-B", From: "A", To: "B", Type: "link", Weight: 1,
		}},
	}, PageRankOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Nodes) != 2 || got.Nodes[0].NodeID != "B" ||
		got.Nodes[1].NodeID != "A" {
		t.Fatalf("rank order = %#v", got.Nodes)
	}
	if math.Abs(got.Nodes[0].PageRank-0.6491228070) > 1e-8 ||
		math.Abs(got.Nodes[1].PageRank-0.3508771930) > 1e-8 {
		t.Fatalf("dangling ranks = %#v", got.Nodes)
	}
	if got.Nodes[0].InDegree != 1 || got.Nodes[0].OutDegree != 0 ||
		got.Nodes[1].InDegree != 0 || got.Nodes[1].OutDegree != 1 {
		t.Fatalf("degrees = %#v", got.Nodes)
	}
	if len(got.WeaklyConnectedComponents) != 1 ||
		got.WeaklyConnectedComponents[0].EdgeCount != 1 {
		t.Fatalf("components = %#v", got.WeaklyConnectedComponents)
	}
}

func TestAnalyzeDisconnectedCycleIsDeterministic(t *testing.T) {
	first := explorer.Neighborhood{
		Nodes: []graph.Node{{ID: "D"}, {ID: "B"}, {ID: "A"}, {ID: "C"}},
		Edges: []graph.Edge{
			{ID: "C-A", From: "C", To: "A", Type: "link", Weight: 1},
			{ID: "B-C", From: "B", To: "C", Type: "link", Weight: 1},
			{ID: "A-B", From: "A", To: "B", Type: "link", Weight: 1},
		},
	}
	second := explorer.Neighborhood{
		Nodes: []graph.Node{{ID: "C"}, {ID: "A"}, {ID: "D"}, {ID: "B"}},
		Edges: []graph.Edge{
			first.Edges[2], first.Edges[0], first.Edges[1],
		},
	}
	left, err := Analyze(context.Background(), first, PageRankOptions{})
	if err != nil {
		t.Fatal(err)
	}
	right, err := Analyze(context.Background(), second, PageRankOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("input order changed result:\nleft=%#v\nright=%#v", left, right)
	}
	if len(left.WeaklyConnectedComponents) != 2 ||
		!reflect.DeepEqual(
			left.WeaklyConnectedComponents[0].NodeIDs,
			[]shoal.ID{"A", "B", "C"},
		) ||
		!reflect.DeepEqual(
			left.WeaklyConnectedComponents[1].NodeIDs,
			[]shoal.ID{"D"},
		) {
		t.Fatalf("components = %#v", left.WeaklyConnectedComponents)
	}
	for index, want := range []shoal.ID{"A", "B", "C", "D"} {
		if left.Nodes[index].NodeID != want {
			t.Fatalf("tie order[%d] = %q, want %q", index, left.Nodes[index].NodeID, want)
		}
	}
}

func TestAnalyzeRejectsDanglingEdgeEndpoint(t *testing.T) {
	_, err := Analyze(context.Background(), explorer.Neighborhood{
		Nodes: []graph.Node{{ID: "A"}},
		Edges: []graph.Edge{{
			ID: "A-missing", From: "A", To: "missing", Type: "link", Weight: 1,
		}},
	}, PageRankOptions{})
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("error = %v", err)
	}
}

func TestAnalyzeFailsOnNonconvergence(t *testing.T) {
	_, err := Analyze(context.Background(), explorer.Neighborhood{
		Nodes: []graph.Node{{ID: "A"}, {ID: "B"}},
		Edges: []graph.Edge{{
			ID: "A-B", From: "A", To: "B", Type: "link", Weight: 1,
		}},
	}, PageRankOptions{
		DampingFactor: 0.85, ConvergenceTolerance: MinPageRankTolerance,
		MaxIterations: 1,
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("error = %v", err)
	}
}

func TestAnalyzeHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Analyze(ctx, explorer.Neighborhood{}, PageRankOptions{})
	if !shoal.IsErrorCode(err, shoal.ErrorCanceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestValidateRequestBoundsExtremes(t *testing.T) {
	limits := DefaultLimits()
	nodeIDs := make([]shoal.ID, limits.MaxSeeds)
	for index := range nodeIDs {
		nodeIDs[index] = shoal.ID(string(rune('a' + index)))
	}
	maximum := Request{
		Scope: Scope{
			NodeIDs: nodeIDs, Depth: limits.MaxDepth,
			Direction: explorer.GraphDirectionBoth,
			Fanout:    limits.MaxFanout, MaxNodes: limits.MaxNodes,
			MaxEdges:               limits.MaxEdges,
			MaxScannedEdgesPerNode: limits.MaxScannedEdgesPerNode,
			EdgeTypes:              make([]string, limits.MaxEdgeTypes),
		},
		PageRank: PageRankOptions{
			DampingFactor:        0.5,
			ConvergenceTolerance: limits.MinPageRankTolerance,
			MaxIterations:        limits.MaxPageRankIterations,
		},
	}
	for index := range maximum.Scope.EdgeTypes {
		maximum.Scope.EdgeTypes[index] = fmt.Sprintf("edge-%d", index)
	}
	if err := ValidateRequest(maximum, limits); err != nil {
		t.Fatalf("maximum request: %v", err)
	}
	tests := []Request{
		func() Request {
			value := maximum
			value.Scope.Depth++
			return value
		}(),
		func() Request {
			value := maximum
			value.Scope.MaxEdges++
			return value
		}(),
		func() Request {
			value := maximum
			value.PageRank.MaxIterations++
			return value
		}(),
		func() Request {
			value := maximum
			value.Scope.MaxScannedEdgesPerNode = value.Scope.Fanout - 1
			return value
		}(),
		func() Request {
			value := maximum
			value.Scope.MaxNodes = 0
			return value
		}(),
	}
	for index, request := range tests {
		if err := ValidateRequest(request, limits); !shoal.IsErrorCode(
			err, shoal.ErrorInvalidArgument,
		) {
			t.Fatalf("invalid request %d error = %v", index, err)
		}
	}
}

func TestServiceRejectsProviderMarkedIncomplete(t *testing.T) {
	source := &staticMaterializer{materialization: Materialization{}}
	service, err := NewService(Config{Source: source, Limits: DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Run(context.Background(), boundedRequest("node"))
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("incomplete materialization error = %v", err)
	}
	if source.revalidations != 0 {
		t.Fatalf("incomplete materialization was revalidated %d times", source.revalidations)
	}
}

func TestServiceReportsUnresolvedOntologySemantics(t *testing.T) {
	schema, err := ontology.NewOntologySchema("analytics", "Analytics", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	version, err := ontology.NewOntologyVersion(
		schema, "1", time.Date(2026, time.September, 5, 18, 0, 0, 0, time.UTC),
		nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	lens, _ := ontology.NewOntologyIdentity(version)
	property, err := ontology.NewPropertyDefinition(
		"name", "Name", "", ontology.ValueString, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	value, err := ontology.NewStringValue("alpha")
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := ontology.NewEvidenceRef(document.Citation{
		DocumentID: "document", RevisionID: "revision",
		SectionID: "section", SpanID: "span",
		Range: document.SourceRange{
			Start: document.SourcePosition{Offset: 0, Page: 1},
			End:   document.SourcePosition{Offset: 5, Page: 1},
		},
	}, "alpha", nil)
	if err != nil {
		t.Fatal(err)
	}
	provenance, err := ontology.NewExtractionProvenance(
		"provider", "model", "1", "prompt", "1", "extractor", "1", nil)
	if err != nil {
		t.Fatal(err)
	}
	assertion, err := ontology.NewAssertion(
		"entity", property.ID(), value, ontology.AssertionExplicit, 1,
		[]ontology.EvidenceRef{evidence}, provenance, nil)
	if err != nil {
		t.Fatal(err)
	}
	var fingerprint auth.Fingerprint
	fingerprint[0] = 1
	source := &staticMaterializer{materialization: Materialization{
		Snapshot: explorer.Snapshot{ID: "internal"},
		Neighborhood: explorer.Neighborhood{
			Nodes:      []graph.Node{{ID: "node"}},
			Assertions: []ontology.Assertion{assertion},
			Interpretations: []ontology.AssertionInterpretation{
				ontology.UnresolvedInterpretation(
					assertion, lens, "no unique published morphism path"),
			},
		},
		AuthorizationFingerprint: fingerprint,
		PolicyGeneration:         1,
		SelectedOntology:         lens,
		Complete:                 true,
	}}
	service, err := NewService(Config{Source: source, Limits: DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Run(context.Background(), boundedRequest("node"))
	if err != nil {
		t.Fatal(err)
	}
	if result.Scope.Ontology == nil ||
		result.Scope.Ontology.VersionID != lens.VersionID() ||
		result.Scope.ResolvedAssertionCount != 0 ||
		len(result.Scope.UnresolvedAssertions) != 1 ||
		result.Scope.UnresolvedAssertions[0].AssertionID != assertion.ID() ||
		result.Scope.UnresolvedAssertions[0].Reason !=
			"no unique published morphism path" {
		t.Fatalf("ontology scope = %#v", result.Scope)
	}
	if source.revalidations != 1 || result.Recording.Recorded {
		t.Fatalf("revalidations=%d recording=%#v",
			source.revalidations, result.Recording)
	}
}

func TestServicePreservesIndeterminateRecordingFailure(t *testing.T) {
	var fingerprint auth.Fingerprint
	fingerprint[0] = 1
	source := &staticMaterializer{materialization: Materialization{
		Neighborhood: explorer.Neighborhood{
			Nodes: []graph.Node{{ID: "node"}},
		},
		AuthorizationFingerprint: fingerprint,
		PolicyGeneration:         1,
		Complete:                 true,
	}}
	failure := errors.New("unknown durable outcome")
	service, err := NewService(Config{
		Source: source, Limits: DefaultLimits(),
		Recorder: recorderStub(func(
			context.Context,
			Record,
		) (RecordingReceipt, error) {
			return RecordingReceipt{},
				explorer.MarkIndeterminateCommit(failure)
		}),
		RequireRecording: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Run(context.Background(), boundedRequest("node"))
	if !explorer.IsIndeterminateCommit(err) ||
		!shoal.IsErrorCode(err, shoal.ErrorUnavailable) ||
		!errors.Is(err, failure) {
		t.Fatalf("recording failure = %v", err)
	}
}

func boundedRequest(nodeID shoal.ID) Request {
	return Request{Scope: Scope{
		NodeIDs: []shoal.ID{nodeID}, Depth: 1,
		Direction: explorer.GraphDirectionBoth,
		Fanout:    4, MaxNodes: 8, MaxEdges: 16,
		MaxScannedEdgesPerNode: 32,
	}}
}

type staticMaterializer struct {
	materialization Materialization
	revalidations   int
}

type recorderStub func(
	context.Context,
	Record,
) (RecordingReceipt, error)

func (f recorderStub) RecordAnalytics(
	ctx context.Context,
	record Record,
) (RecordingReceipt, error) {
	return f(ctx, record)
}

func (s *staticMaterializer) MaterializeAnalytics(
	context.Context,
	explorer.BoundedNeighborhoodRequest,
	uint32,
	uint64,
) (Materialization, error) {
	return s.materialization, nil
}

func (s *staticMaterializer) RevalidateAnalytics(
	context.Context,
	Materialization,
) error {
	s.revalidations++
	return nil
}
