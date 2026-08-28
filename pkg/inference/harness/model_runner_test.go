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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/inference"
	lowmodel "github.com/phrocker/shoal-oss/pkg/model"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestModelRunnerDrivesBoundedLoopAndTrace(t *testing.T) {
	pack, _, additions := fixture(t)
	stopJSON := `{"action":"stop","correlation_id":"stop","claims":[{"subject":"entity:fake","predicate":"predicate:summary","object":{"type":"string","value":"grounded"},"confidence":1,"evidence_ids":["` + string(additions[0].ID()) + `"]}]}`
	runner, err := NewModelRunner(&scriptedTextGenerator{responses: []string{
		`{"action":"retrieve","correlation_id":"retrieve","query":"entity","limit":1}`,
		stopJSON,
	}}, ModelRunnerConfig{Now: func() time.Time { return fixedTime }})
	if err != nil {
		t.Fatal(err)
	}
	host := &fakeTools{pack: pack, results: map[shoal.ID][]inference.EvidenceAnchor{
		"retrieve": {additions[0]},
	}}
	record, err := newGenerator(t, runner, host).Run(context.Background(), pack)
	if err != nil {
		t.Fatal(err)
	}
	if record.Trace.StopReason != StopReasonStop || len(record.Trace.Iterations) != 2 {
		t.Fatalf("unexpected trace: %#v", record.Trace)
	}
	if record.Trace.Usage.ModelCalls != 2 || record.Trace.Usage.Evidence != 1 {
		t.Fatalf("budget usage not recorded: %#v", record.Trace.Usage)
	}
	if len(record.Result.Claims()) != 1 {
		t.Fatal("grounded claim was not returned")
	}
}

func TestModelRunnerRejectsUngroundedStopWithTrace(t *testing.T) {
	pack, _, _ := fixture(t)
	runner, err := NewModelRunner(&scriptedTextGenerator{responses: []string{
		`{"action":"stop","correlation_id":"stop","claims":[{"subject":"entity:fake","predicate":"predicate:summary","object":{"type":"string","value":"grounded"},"confidence":1,"evidence_ids":["missing"]}]}`,
	}}, ModelRunnerConfig{Now: func() time.Time { return fixedTime }})
	if err != nil {
		t.Fatal(err)
	}
	record, err := newGenerator(t, runner, &fakeTools{pack: pack}).Run(context.Background(), pack)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("error = %v", err)
	}
	if record.Trace.StopReason != StopReasonInvalid || len(record.Trace.Failures) != 1 {
		t.Fatalf("failure trace missing: %#v", record.Trace)
	}
}

func TestModelRunnerWorksWithDeterministicFakeProvider(t *testing.T) {
	pack, initial, _ := fixture(t)
	runner, err := NewModelRunner(lowmodel.FakeGenerator{}, ModelRunnerConfig{Now: func() time.Time { return fixedTime }})
	if err != nil {
		t.Fatal(err)
	}
	host := &fakeTools{pack: pack, results: map[shoal.ID][]inference.EvidenceAnchor{
		"fake-retrieve-1": nil,
	}}
	custom := budgets()
	custom.MaxInputTokens = 10000
	custom.MaxOutputTokens = 10000
	record, err := newGeneratorWithBudgets(t, runner, host, custom).Run(context.Background(), pack)
	if err != nil {
		t.Fatal(err)
	}
	if record.Trace.StopReason != StopReasonStop || len(record.Result.Claims()) != 1 {
		t.Fatalf("fake provider did not produce deterministic grounded stop: %#v", record.Trace)
	}
	if got := record.Result.Claims()[0].EvidenceIDs()[0]; got != initial.ID() {
		t.Fatalf("claim evidence = %q, want %q", got, initial.ID())
	}
}

type scriptedTextGenerator struct {
	responses []string
	next      int
}

func (g *scriptedTextGenerator) Generate(ctx context.Context, req lowmodel.GenerateRequest) (lowmodel.GenerateResult, error) {
	if err := ctx.Err(); err != nil {
		return lowmodel.GenerateResult{}, err
	}
	if !strings.Contains(req.Prompt, ModelPromptMarker) {
		return lowmodel.GenerateResult{}, errors.New("prompt omitted protocol marker")
	}
	if g.next >= len(g.responses) {
		return lowmodel.GenerateResult{}, errors.New("script exhausted")
	}
	text := g.responses[g.next]
	g.next++
	return lowmodel.GenerateResult{
		Text:       text,
		Provenance: lowmodel.Provenance{Provider: "fake", Model: "scripted"},
		Usage:      lowmodel.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2},
	}, nil
}
