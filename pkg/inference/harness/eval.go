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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const FixtureEvaluationVersion = "shoal-grounded-inference-eval-v1"

type EvaluationCase struct {
	ID             string
	Pack           inference.ContextPack
	ExpectedClaims []ExpectedClaim
	Sources        map[DocumentRevision]string
}

type ExpectedClaim struct {
	Subject     shoal.ID
	Predicate   shoal.ID
	Object      ontology.Value
	EvidenceIDs []shoal.ID
}

type DocumentRevision struct {
	DocumentID shoal.ID
	RevisionID shoal.ID
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
	TraceDigest             string      `json:"trace_digest"`
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
		caseReport := evaluateRecord(tc, record, err)
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

func evaluateRecord(tc EvaluationCase, record Record, runErr error) EvaluationCaseReport {
	resultID := ""
	if runErr == nil {
		resultID = string(record.Result.ID())
	}
	out := EvaluationCaseReport{
		ID:            tc.ID,
		ContextPackID: string(tc.Pack.ID()),
		ResultID:      resultID,
		StopReason:    record.Trace.StopReason,
		Iterations:    len(record.Trace.Iterations),
		TraceDigest:   traceDigest(record.Trace),
		Budget:        record.Trace.Usage,
	}
	if runErr != nil {
		out.Error = runErr.Error()
		return out
	}
	available := evidenceIndex(tc.Pack.Evidence(), record.Result.EvidenceAdditions())
	for _, claim := range record.Result.Claims() {
		out.Claims++
		ok := claimMatchesExpected(claim, tc.ExpectedClaims)
		for _, evidenceID := range claim.EvidenceIDs() {
			anchor, found := available[evidenceID]
			if !found {
				out.InvalidEvidenceRefs++
				ok = false
				continue
			}
			out.ValidEvidenceReferences++
			if citation, quote, cited := anchor.Document(); cited {
				out.CitationReferences++
				if err := validateFixtureCitation(citation, quote, tc.Sources); err != nil {
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

func claimMatchesExpected(claim inference.Claim, expected []ExpectedClaim) bool {
	for _, item := range expected {
		if claim.Subject() != item.Subject || claim.Predicate() != item.Predicate ||
			!ontologyValuesEqual(claim.Object(), item.Object) {
			continue
		}
		claimEvidence := claim.EvidenceIDs()
		expectedEvidence := append([]shoal.ID(nil), item.EvidenceIDs...)
		sort.Slice(expectedEvidence, func(i, j int) bool {
			return shoal.CompareID(expectedEvidence[i], expectedEvidence[j]) < 0
		})
		if len(claimEvidence) != len(expectedEvidence) {
			continue
		}
		matches := true
		for i := range claimEvidence {
			if claimEvidence[i] != expectedEvidence[i] {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func ontologyValuesEqual(left, right ontology.Value) bool {
	if left.Type() != right.Type() {
		return false
	}
	switch left.Type() {
	case ontology.ValueString:
		l, _ := left.StringValue()
		r, _ := right.StringValue()
		return l == r
	case ontology.ValueInteger:
		l, _ := left.IntegerValue()
		r, _ := right.IntegerValue()
		return l == r
	case ontology.ValueNumber:
		l, _ := left.NumberValue()
		r, _ := right.NumberValue()
		return l == r
	case ontology.ValueBoolean:
		l, _ := left.BooleanValue()
		r, _ := right.BooleanValue()
		return l == r
	case ontology.ValueTimestamp:
		l, _ := left.TimestampValue()
		r, _ := right.TimestampValue()
		return l.Equal(r)
	case ontology.ValueReference:
		l, _ := left.ReferenceValue()
		r, _ := right.ReferenceValue()
		return l == r
	default:
		return false
	}
}

func validateFixtureCitation(citation document.Citation, quote string, sources map[DocumentRevision]string) error {
	if err := citation.Validate(); err != nil {
		return err
	}
	source, ok := sources[DocumentRevision{DocumentID: citation.DocumentID, RevisionID: citation.RevisionID}]
	if !ok {
		return invalid("citation source is absent from fixture case")
	}
	start, end := citation.Range.Start.Offset, citation.Range.End.Offset
	if start < 0 || end <= start || end > int64(len(source)) {
		return invalid("citation range is outside the fixture source")
	}
	if source[int(start):int(end)] != quote {
		return invalid("citation quote does not match fixture source")
	}
	return nil
}

func traceDigest(trace RunTrace) string {
	digest := sha256.New()
	for _, iteration := range trace.Iterations {
		writeDigestPart(digest, strconv.Itoa(iteration.Index))
		writeDigestPart(digest, string(iteration.Decision))
		writeDigestPart(digest, iteration.ActionKey)
		writeDigestPart(digest, string(iteration.CorrelationID))
		writeDigestPart(digest, strconv.Itoa(iteration.Usage.InputTokens))
		writeDigestPart(digest, strconv.Itoa(iteration.Usage.OutputTokens))
		writeDigestPart(digest, strconv.Itoa(iteration.Budget.ModelCalls))
		writeDigestPart(digest, strconv.Itoa(iteration.Budget.InputTokens))
		writeDigestPart(digest, strconv.Itoa(iteration.Budget.OutputTokens))
		writeDigestPart(digest, strconv.Itoa(iteration.Budget.Evidence))
		writeDigestPart(digest, strconv.Itoa(iteration.Budget.GraphHops))
		writeDigestPart(digest, strconv.Itoa(iteration.Budget.GraphNodes))
		writeDigestPart(digest, iteration.Failure)
		for _, id := range iteration.EvidenceIDs {
			writeDigestPart(digest, string(id))
		}
	}
	writeDigestPart(digest, string(trace.StopReason))
	for _, failure := range trace.Failures {
		writeDigestPart(digest, strconv.Itoa(failure.Iteration))
		writeDigestPart(digest, failure.Operation)
		writeDigestPart(digest, failure.Error)
	}
	return "sha256:" + hex.EncodeToString(digest.Sum(nil))
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeDigestPart(digest digestWriter, part string) {
	_, _ = digest.Write([]byte(strconv.Itoa(len(part))))
	_, _ = digest.Write([]byte{':'})
	_, _ = digest.Write([]byte(part))
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
		value, err := ontology.NewStringValue("grounded")
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
		cases = append(cases, EvaluationCase{
			ID:   spec.id,
			Pack: pack,
			ExpectedClaims: []ExpectedClaim{{
				Subject:     "entity:fake",
				Predicate:   "predicate:summary",
				Object:      value,
				EvidenceIDs: []shoal.ID{anchor.ID()},
			}},
			Sources: map[DocumentRevision]string{
				DocumentRevision{DocumentID: spec.documentID, RevisionID: spec.revisionID}: string(content),
			},
		})
	}
	return cases, nil
}
