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

package harness

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/inference"
	lowmodel "github.com/phrocker/shoal-oss/pkg/model"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestDeterministicFixtureEvaluationReport(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	root := filepath.Join("..", "..", "..", "testdata", "explorer-eval")
	cases, err := LoadFixtureEvaluationCases(root, now)
	if err != nil {
		t.Fatal(err)
	}
	first, err := runFixtureEvaluation(t, cases, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := runFixtureEvaluation(t, cases, now)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, err := first.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	secondJSON, err := second.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("evaluation output is nondeterministic\nfirst:\n%s\nsecond:\n%s", firstJSON, secondJSON)
	}
	if first.Summary.CaseCount != 3 ||
		first.Summary.ClaimCount != 3 ||
		first.Summary.SupportedClaimCount != 3 ||
		first.Summary.GroundingSupportRate != 1 ||
		first.Summary.InvalidCitationRefs != 0 ||
		first.Summary.InvalidGraphPathRefs != 0 ||
		first.Summary.CitationReferenceCount != 2 ||
		first.Summary.GraphPathReferenceCount != 1 ||
		first.Summary.StopReasons[StopReasonStop] != 3 {
		t.Fatalf("unexpected metrics: %#v", first.Summary)
	}
	citationRefs, graphRefs := 0, 0
	for _, report := range first.Cases {
		if report.Iterations != 2 || report.Budget.ModelCalls != 2 ||
			report.ValidEvidenceReferences != 1 ||
			report.TraceDigest == "" {
			t.Fatalf("case metrics not defensible: %#v", report)
		}
		citationRefs += report.CitationReferences
		graphRefs += report.GraphPathReferences
	}
	if citationRefs != 2 || graphRefs != 1 {
		t.Fatalf("unexpected citation/path coverage: citations=%d graph=%d", citationRefs, graphRefs)
	}
}

func TestEvaluationDetectsNegativeCases(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	root := filepath.Join("..", "..", "..", "testdata", "explorer-eval")
	cases, err := LoadFixtureEvaluationCases(root, now)
	if err != nil {
		t.Fatal(err)
	}
	base := cases[:1]

	t.Run("unsupported claim", func(t *testing.T) {
		g := scriptedEvaluationGenerator(t, now, func(_ context.Context, tr Transcript) (Action, error) {
			value, _ := ontology.NewStringValue("not-grounded")
			claim, err := inference.NewClaim(
				"subject:unexpected", "predicate:summary", value, 1,
				[]shoal.ID{tr.Context().Evidence()[0].ID()}, inference.ClaimInferred,
				evalModelProvenance(t), evalPromptProvenance(t), nil,
			)
			if err != nil {
				return Action{}, err
			}
			result, err := inference.NewInferenceResult(tr.Context(), []inference.Claim{claim}, nil, now, nil)
			if err != nil {
				return Action{}, err
			}
			return NewStopAction("stop", result, Usage{})
		})
		report, err := Evaluate(context.Background(), g, base, now)
		if err != nil {
			t.Fatal(err)
		}
		if report.Summary.SupportedClaimCount != 0 || report.Summary.GroundingSupportRate != 0 {
			t.Fatalf("unsupported claim counted as supported: %#v", report.Summary)
		}
	})

	t.Run("stale citation", func(t *testing.T) {
		stale := append([]EvaluationCase(nil), base...)
		stale[0].Sources = map[DocumentRevision]string{
			DocumentRevision{DocumentID: "aster-relay-protocol", RevisionID: "r2"}: "fixture contents changed",
		}
		report, err := runFixtureEvaluation(t, stale, now)
		if err != nil {
			t.Fatal(err)
		}
		if report.Summary.InvalidCitationRefs != 1 {
			t.Fatalf("stale citation was not detected: %#v", report.Summary)
		}
	})

	t.Run("invalid graph path", func(t *testing.T) {
		graphCase := []EvaluationCase{cases[2]}
		graphCase[0].GraphPaths = map[shoal.ID]graph.Path{}
		report, err := runFixtureEvaluation(t, graphCase, now)
		if err != nil {
			t.Fatal(err)
		}
		if report.Summary.InvalidGraphPathRefs != 1 {
			t.Fatalf("invalid graph path was not detected: %#v", report.Summary)
		}
	})

	t.Run("invalid graph path metadata", func(t *testing.T) {
		graphCase := []EvaluationCase{cases[2]}
		anchorID := graphCase[0].Pack.Evidence()[0].ID()
		want := graphCase[0].GraphPaths[anchorID]
		want.Nodes = append([]graph.Node(nil), want.Nodes...)
		want.Edges = append([]graph.Edge(nil), want.Edges...)
		want.Nodes[0].Labels = []string{"unexpected-label"}
		want.Nodes[0].Properties = shoal.Metadata{"unexpected": "node"}
		want.Edges[0].Weight = 0.5
		want.Edges[0].Properties = shoal.Metadata{"unexpected": "edge"}
		graphCase[0].GraphPaths = map[shoal.ID]graph.Path{anchorID: want}
		report, err := runFixtureEvaluation(t, graphCase, now)
		if err != nil {
			t.Fatal(err)
		}
		if report.Summary.InvalidGraphPathRefs != 1 {
			t.Fatalf("invalid graph path metadata was not detected: %#v", report.Summary)
		}
	})

	t.Run("budget violation", func(t *testing.T) {
		g := scriptedEvaluationGenerator(t, now, func(context.Context, Transcript) (Action, error) {
			return mustRetrieve(t, "retrieve", "more", 1), nil
		})
		g.budgets.MaxSteps = 1
		report, err := Evaluate(context.Background(), g, base, now)
		if err != nil {
			t.Fatal(err)
		}
		if report.Cases[0].StopReason != StopReasonBudgetExhausted || report.Cases[0].Error == "" {
			t.Fatalf("budget violation not reported: %#v", report.Cases[0])
		}
	})
}

func TestEvaluationTraceDigestDetectsDifferentToolTraces(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	root := filepath.Join("..", "..", "..", "testdata", "explorer-eval")
	cases, err := LoadFixtureEvaluationCases(root, now)
	if err != nil {
		t.Fatal(err)
	}
	makeRunner := func(query string) *Generator {
		return scriptedEvaluationGenerator(t, now,
			func(context.Context, Transcript) (Action, error) {
				return mustRetrieve(t, "retrieve", query, 1), nil
			},
			func(_ context.Context, tr Transcript) (Action, error) {
				result := evalResultFor(t, tr.Context(), tr.Context().Evidence()[0], now)
				return NewStopAction("stop", result, Usage{})
			},
		)
	}
	first, err := Evaluate(context.Background(), makeRunner("alpha"), cases[:1], now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Evaluate(context.Background(), makeRunner("beta"), cases[:1], now)
	if err != nil {
		t.Fatal(err)
	}
	if first.Cases[0].Iterations != second.Cases[0].Iterations ||
		first.Cases[0].Budget != second.Cases[0].Budget {
		t.Fatal("test setup should differ only by trace identity")
	}
	if first.Cases[0].TraceDigest == second.Cases[0].TraceDigest {
		t.Fatal("different tool traces produced identical report digest")
	}
}

func runFixtureEvaluation(t *testing.T, cases []EvaluationCase, now time.Time) (EvaluationReport, error) {
	t.Helper()
	modelProvenance := evalModelProvenance(t)
	prompt := evalPromptProvenance(t)
	provenance, err := NewProvenance("fixture-harness", modelProvenance, prompt, "fixture-tools-v1")
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewModelRunner(lowmodel.FakeGenerator{Model: "deterministic"}, ModelRunnerConfig{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	g := evalGenerator(t, runner, now, provenance)
	return Evaluate(context.Background(), g, cases, now)
}

func scriptedEvaluationGenerator(t *testing.T, now time.Time, steps ...ScriptStep) *Generator {
	t.Helper()
	provenance, err := NewProvenance("fixture-harness", evalModelProvenance(t), evalPromptProvenance(t), "fixture-tools-v1")
	if err != nil {
		t.Fatal(err)
	}
	return evalGenerator(t, NewFakeRunner(steps...), now, provenance)
}

func evalGenerator(t *testing.T, runner Runner, now time.Time, provenance Provenance) *Generator {
	t.Helper()
	b := Budgets{
		MaxSteps: 4, MaxElapsed: time.Second, MaxInputTokens: 1_000_000, MaxOutputTokens: 10_000,
		MaxEvidence: 4, MaxGraphHops: 1, MaxGraphNodes: 4, MaxFanout: 1, MaxRepeatedAction: 1,
	}
	g, err := NewGenerator(runner, emptyRetrieveTools{}, b, provenance, nil)
	if err != nil {
		t.Fatal(err)
	}
	g.now = func() time.Time { return now }
	return g
}

func evalModelProvenance(t *testing.T) inference.ModelProvenance {
	t.Helper()
	seed := int64(1)
	modelProvenance, err := inference.NewModelProvenance("fake", "deterministic", "v1", nil, &seed)
	if err != nil {
		t.Fatal(err)
	}
	return modelProvenance
}

func evalPromptProvenance(t *testing.T) inference.PromptProvenance {
	t.Helper()
	prompt, err := inference.NewPromptProvenance("agent", "v1", "sha256:"+strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	return prompt
}

func evalResultFor(t *testing.T, pack inference.ContextPack, evidence inference.EvidenceAnchor, generatedAt time.Time) inference.InferenceResult {
	t.Helper()
	value, _ := ontology.NewStringValue("grounded")
	claim, err := inference.NewClaim(
		"entity:fake", "predicate:summary", value, 1, []shoal.ID{evidence.ID()},
		inference.ClaimInferred, evalModelProvenance(t), evalPromptProvenance(t), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := inference.NewInferenceResult(pack, []inference.Claim{claim}, nil, generatedAt, nil)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

type emptyRetrieveTools struct{}

func (emptyRetrieveTools) Retrieve(_ context.Context, call ToolContext, _ RetrieveRequest) (ToolResult, error) {
	return NewToolResult(call.CorrelationID(), ActionRetrieve, nil, call.Snapshot(), call.Authorization())
}
func (emptyRetrieveTools) OpenSection(_ context.Context, call ToolContext, _ OpenSectionRequest) (ToolResult, error) {
	return NewToolResult(call.CorrelationID(), ActionOpenSection, nil, call.Snapshot(), call.Authorization())
}
func (emptyRetrieveTools) Neighbors(_ context.Context, call ToolContext, _ NeighborsRequest) (ToolResult, error) {
	return NewToolResult(call.CorrelationID(), ActionNeighbors, nil, call.Snapshot(), call.Authorization())
}
