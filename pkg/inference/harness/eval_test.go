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

	"github.com/phrocker/shoal-oss/pkg/inference"
	lowmodel "github.com/phrocker/shoal-oss/pkg/model"
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
	if first.Summary.CaseCount != 2 ||
		first.Summary.ClaimCount != 2 ||
		first.Summary.SupportedClaimCount != 2 ||
		first.Summary.GroundingSupportRate != 1 ||
		first.Summary.InvalidCitationRefs != 0 ||
		first.Summary.StopReasons[StopReasonStop] != 2 {
		t.Fatalf("unexpected metrics: %#v", first.Summary)
	}
	for _, report := range first.Cases {
		if report.Iterations != 2 || report.Budget.ModelCalls != 2 ||
			report.ValidEvidenceReferences != 1 || report.CitationReferences != 1 {
			t.Fatalf("case metrics not defensible: %#v", report)
		}
	}
}

func runFixtureEvaluation(t *testing.T, cases []EvaluationCase, now time.Time) (EvaluationReport, error) {
	t.Helper()
	seed := int64(1)
	modelProvenance, err := inference.NewModelProvenance("fake", "deterministic", "v1", nil, &seed)
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := inference.NewPromptProvenance("agent", "v1", "sha256:"+strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	provenance, err := NewProvenance("fixture-harness", modelProvenance, prompt, "fixture-tools-v1")
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewModelRunner(lowmodel.FakeGenerator{Model: "deterministic"}, ModelRunnerConfig{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	b := Budgets{
		MaxSteps: 4, MaxElapsed: time.Second, MaxInputTokens: 1_000_000, MaxOutputTokens: 10_000,
		MaxEvidence: 4, MaxGraphHops: 1, MaxGraphNodes: 4, MaxFanout: 1, MaxRepeatedAction: 1,
	}
	g, err := NewGenerator(runner, emptyRetrieveTools{}, b, provenance, nil)
	if err != nil {
		t.Fatal(err)
	}
	g.now = func() time.Time { return now }
	return Evaluate(context.Background(), g, cases, now)
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
