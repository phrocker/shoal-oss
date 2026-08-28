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
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const FixtureEvaluationVersion = "shoal-grounded-inference-eval-v1"

type EvaluationCase struct {
	ID   string
	Pack inference.ContextPack
}

type EvaluationReport struct {
	Version   string                 `json:"version"`
	Cases     []EvaluationCaseReport `json:"cases"`
	Summary   EvaluationSummary      `json:"summary"`
	Generated string                 `json:"generated"`
}

type EvaluationCaseReport struct {
	ID                      string      `json:"id"`
	ContextPackID           string      `json:"context_pack_id"`
	ResultID                string      `json:"result_id,omitempty"`
	StopReason              StopReason  `json:"stop_reason"`
	Iterations              int         `json:"iterations"`
	Budget                  BudgetUsage `json:"budget"`
	Claims                  int         `json:"claims"`
	SupportedClaims         int         `json:"supported_claims"`
	UnsupportedIssues       int         `json:"unsupported_issues"`
	CitationReferences      int         `json:"citation_references"`
	InvalidCitationRefs     int         `json:"invalid_citation_references"`
	ValidEvidenceReferences int         `json:"valid_evidence_references"`
	InvalidEvidenceRefs     int         `json:"invalid_evidence_references"`
	Error                   string      `json:"error,omitempty"`
}

type EvaluationSummary struct {
	CaseCount               int                `json:"case_count"`
	ClaimCount              int                `json:"claim_count"`
	SupportedClaimCount     int                `json:"supported_claim_count"`
	UnsupportedIssueCount   int                `json:"unsupported_issue_count"`
	GroundingSupportRate    float64            `json:"grounding_support_rate"`
	UnsupportedOutcomeRate  float64            `json:"unsupported_outcome_rate"`
	CitationReferenceCount  int                `json:"citation_reference_count"`
	InvalidCitationRefs     int                `json:"invalid_citation_references"`
	ValidEvidenceReferences int                `json:"valid_evidence_references"`
	InvalidEvidenceRefs     int                `json:"invalid_evidence_references"`
	TotalBudget             BudgetUsage        `json:"total_budget"`
	IterationCounts         []int              `json:"iteration_counts"`
	StopReasons             map[StopReason]int `json:"stop_reasons"`
}

func Evaluate(ctx context.Context, generator *Generator, cases []EvaluationCase, generatedAt time.Time) (EvaluationReport, error) {
	if generator == nil {
		return EvaluationReport{}, invalid("evaluation generator is required")
	}
	ordered := append([]EvaluationCase(nil), cases...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ID < ordered[j].ID })
	report := EvaluationReport{
		Version:   FixtureEvaluationVersion,
		Generated: generatedAt.UTC().Format(time.RFC3339Nano),
		Summary:   EvaluationSummary{StopReasons: make(map[StopReason]int)},
	}
	for _, tc := range ordered {
		if strings.TrimSpace(tc.ID) == "" {
			return EvaluationReport{}, invalid("evaluation case ID is required")
		}
		record, err := generator.Run(ctx, tc.Pack)
		caseReport := evaluateRecord(tc.ID, tc.Pack, record, err)
		report.Cases = append(report.Cases, caseReport)
		report.Summary.CaseCount++
		report.Summary.ClaimCount += caseReport.Claims
		report.Summary.SupportedClaimCount += caseReport.SupportedClaims
		report.Summary.UnsupportedIssueCount += caseReport.UnsupportedIssues
		report.Summary.CitationReferenceCount += caseReport.CitationReferences
		report.Summary.InvalidCitationRefs += caseReport.InvalidCitationRefs
		report.Summary.ValidEvidenceReferences += caseReport.ValidEvidenceReferences
		report.Summary.InvalidEvidenceRefs += caseReport.InvalidEvidenceRefs
		report.Summary.TotalBudget = addBudgetUsage(report.Summary.TotalBudget, caseReport.Budget)
		report.Summary.IterationCounts = append(report.Summary.IterationCounts, caseReport.Iterations)
		report.Summary.StopReasons[caseReport.StopReason]++
		if err != nil {
			continue
		}
	}
	report.Summary.GroundingSupportRate = ratio(report.Summary.SupportedClaimCount, report.Summary.ClaimCount)
	report.Summary.UnsupportedOutcomeRate = ratio(report.Summary.UnsupportedIssueCount, report.Summary.ClaimCount+report.Summary.UnsupportedIssueCount)
	return report, nil
}

func (r EvaluationReport) CanonicalJSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

func evaluateRecord(id string, pack inference.ContextPack, record Record, runErr error) EvaluationCaseReport {
	resultID := ""
	if runErr == nil {
		resultID = string(record.Result.ID())
	}
	out := EvaluationCaseReport{
		ID:            id,
		ContextPackID: string(pack.ID()),
		ResultID:      resultID,
		StopReason:    record.Trace.StopReason,
		Iterations:    len(record.Trace.Iterations),
		Budget:        record.Trace.Usage,
	}
	if runErr != nil {
		out.Error = runErr.Error()
		return out
	}
	available := evidenceIndex(pack.Evidence(), record.Result.EvidenceAdditions())
	for _, claim := range record.Result.Claims() {
		out.Claims++
		ok := true
		for _, evidenceID := range claim.EvidenceIDs() {
			anchor, found := available[evidenceID]
			if !found {
				out.InvalidEvidenceRefs++
				ok = false
				continue
			}
			out.ValidEvidenceReferences++
			if citation, _, cited := anchor.Document(); cited {
				out.CitationReferences++
				if err := citation.Validate(); err != nil {
					out.InvalidCitationRefs++
					ok = false
				}
			}
		}
		if ok {
			out.SupportedClaims++
		}
	}
	out.UnsupportedIssues = len(record.Result.Unsupported())
	return out
}

func evidenceIndex(anchors ...[]inference.EvidenceAnchor) map[shoal.ID]inference.EvidenceAnchor {
	index := make(map[shoal.ID]inference.EvidenceAnchor)
	for _, set := range anchors {
		for _, anchor := range set {
			index[anchor.ID()] = anchor
		}
	}
	return index
}

func addBudgetUsage(left, right BudgetUsage) BudgetUsage {
	return BudgetUsage{
		ModelCalls:   left.ModelCalls + right.ModelCalls,
		InputTokens:  left.InputTokens + right.InputTokens,
		OutputTokens: left.OutputTokens + right.OutputTokens,
		Evidence:     left.Evidence + right.Evidence,
		GraphHops:    left.GraphHops + right.GraphHops,
		GraphNodes:   left.GraphNodes + right.GraphNodes,
	}
}

func ratio(numerator, denominator int) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

// LoadFixtureEvaluationCases builds a small, deterministic grounded inference
// suite from the public fixture corpus. It reads local fixture files only.
func LoadFixtureEvaluationCases(root string, generatedAt time.Time) ([]EvaluationCase, error) {
	if strings.TrimSpace(root) == "" {
		return nil, invalid("fixture root is required")
	}
	specs := []struct {
		id         string
		path       string
		documentID shoal.ID
		revisionID shoal.ID
		sectionID  shoal.ID
		spanID     shoal.ID
		query      string
		quote      string
	}{
		{
			id: "q-grounded-aster-relay", path: "aster-relay-protocol-r2.md",
			documentID: "aster-relay-protocol", revisionID: "r2", sectionID: "purpose", spanID: "purpose-relay",
			query: "What carries sealed telemetry batches?", quote: "The Aster Relay is a component of the Aster Mesh.",
		},
		{
			id: "q-grounded-quartz-ring", path: "adr-004-quartz-ring.md",
			documentID: "adr-004-quartz-ring", revisionID: "r1", sectionID: "decision", spanID: "decision-quartz",
			query: "What was selected for relay assignment?", quote: "The Aster Relay will place each sealed telemetry batch on the Quartz Ring.",
		},
	}
	snapshot, err := inference.NewSnapshotPin("fixture-snapshot", generatedAt.Add(-time.Hour))
	if err != nil {
		return nil, err
	}
	auth, err := inference.NewAuthPin("fixture-public", generatedAt.Add(time.Hour))
	if err != nil {
		return nil, err
	}
	cases := make([]EvaluationCase, 0, len(specs))
	for _, spec := range specs {
		path := filepath.Join(root, "corpus", spec.path)
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if bytes.Contains(content, []byte("\r")) {
			return nil, invalid("fixture corpus must use LF line endings")
		}
		offset := bytes.Index(content, []byte(spec.quote))
		if offset < 0 {
			return nil, invalid("fixture quote is absent from corpus")
		}
		anchor, err := inference.NewDocumentAnchor(document.Citation{
			DocumentID: spec.documentID,
			RevisionID: spec.revisionID,
			SectionID:  spec.sectionID,
			SpanID:     spec.spanID,
			Range: document.SourceRange{
				Start: document.SourcePosition{Offset: int64(offset)},
				End:   document.SourcePosition{Offset: int64(offset + len(spec.quote))},
			},
		}, spec.quote)
		if err != nil {
			return nil, err
		}
		pack, err := inference.NewContextPack(
			spec.query, []inference.EvidenceAnchor{anchor}, nil,
			snapshot, auth, shoal.Metadata{"fixture": "explorer-eval"},
		)
		if err != nil {
			return nil, err
		}
		cases = append(cases, EvaluationCase{ID: spec.id, Pack: pack})
	}
	return cases, nil
}
