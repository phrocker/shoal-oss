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
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/inference"
	lowmodel "github.com/phrocker/shoal-oss/pkg/model"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const ModelPromptMarker = "shoal-harness-action-json/v1"

type ModelRunnerConfig struct {
	MaxOutputTokens int
	Now             func() time.Time
}

type ModelRunner struct {
	generator lowmodel.TextGenerator
	cfg       ModelRunnerConfig
}

func NewModelRunner(generator lowmodel.TextGenerator, cfg ModelRunnerConfig) (*ModelRunner, error) {
	if generator == nil {
		return nil, invalid("model text generator is required")
	}
	if cfg.MaxOutputTokens < 0 {
		return nil, invalid("model max output tokens cannot be negative")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &ModelRunner{generator: generator, cfg: cfg}, nil
}

func (r *ModelRunner) Start(ctx context.Context, request SessionRequest) (Session, error) {
	if r == nil || r.generator == nil {
		return nil, ErrRunnerUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := request.context.Validate(); err != nil {
		return nil, err
	}
	return &modelSession{runner: r, request: request}, nil
}

type modelSession struct {
	runner  *ModelRunner
	request SessionRequest
}

func (s *modelSession) Next(ctx context.Context, transcript Transcript) (Action, error) {
	if err := ctx.Err(); err != nil {
		return Action{}, err
	}
	prompt, err := modelPrompt(s.request, transcript)
	if err != nil {
		return Action{}, err
	}
	remainingOutput := s.request.budgets.MaxOutputTokens - transcriptOutputTokens(transcript)
	if remainingOutput < 0 {
		return Action{}, budget("output token")
	}
	maxOutput := remainingOutput
	if s.runner.cfg.MaxOutputTokens > 0 && s.runner.cfg.MaxOutputTokens < maxOutput {
		maxOutput = s.runner.cfg.MaxOutputTokens
	}
	generated, err := s.runner.generator.Generate(ctx, lowmodel.GenerateRequest{
		Prompt:          prompt,
		MaxOutputTokens: maxOutput,
	})
	if err != nil {
		return Action{}, err
	}
	usage := Usage{
		InputTokens:  generated.Usage.InputTokens,
		OutputTokens: generated.Usage.OutputTokens,
	}
	return parseModelAction(generated.Text, transcript.context, s.request.provenance, usage, s.runner.cfg.Now())
}

func transcriptOutputTokens(transcript Transcript) int {
	total := 0
	for _, exchange := range transcript.exchanges {
		total += exchange.action.usage.OutputTokens
	}
	if transcript.final != nil {
		total += transcript.final.usage.OutputTokens
	}
	return total
}

type promptEnvelope struct {
	Protocol    string           `json:"protocol"`
	Tools       []string         `json:"tools"`
	Budgets     Budgets          `json:"budgets"`
	Snapshot    promptSnapshot   `json:"snapshot"`
	Auth        promptAuth       `json:"authorization"`
	Query       string           `json:"query"`
	ContextID   shoal.ID         `json:"context_id"`
	Evidence    []promptEvidence `json:"evidence"`
	Transcript  []promptExchange `json:"transcript"`
	Instruction string           `json:"instruction"`
}

type promptSnapshot struct {
	ID   shoal.ID `json:"id"`
	AsOf string   `json:"as_of"`
}

type promptAuth struct {
	Fingerprint shoal.ID `json:"fingerprint"`
	ExpiresAt   string   `json:"expires_at"`
}

type promptEvidence struct {
	ID       shoal.ID             `json:"id"`
	Kind     inference.AnchorKind `json:"kind"`
	Citation *document.Citation   `json:"citation,omitempty"`
	Quote    string               `json:"quote,omitempty"`
	Path     *graph.Path          `json:"path,omitempty"`
}

type promptExchange struct {
	Action      ActionKind `json:"action"`
	Correlation shoal.ID   `json:"correlation_id"`
	EvidenceIDs []shoal.ID `json:"evidence_ids,omitempty"`
}

func modelPrompt(request SessionRequest, transcript Transcript) (string, error) {
	contextEvidence := transcript.context.Evidence()
	evidence := make([]promptEvidence, 0, len(contextEvidence))
	for _, anchor := range contextEvidence {
		item := promptEvidence{ID: anchor.ID(), Kind: anchor.Kind()}
		if citation, quote, ok := anchor.Document(); ok {
			item.Citation = &citation
			item.Quote = quote
		}
		if path, ok := anchor.Path(); ok {
			item.Path = &path
		}
		evidence = append(evidence, item)
	}
	exchanges := make([]promptExchange, 0, len(transcript.exchanges))
	for _, exchange := range transcript.exchanges {
		exchanges = append(exchanges, promptExchange{
			Action:      exchange.action.kind,
			Correlation: exchange.action.correlation,
			EvidenceIDs: anchorIDs(exchange.result.anchors),
		})
	}
	envelope := promptEnvelope{
		Protocol: ModelPromptMarker,
		Tools: []string{
			string(ActionRetrieve), string(ActionOpenSection),
			string(ActionNeighbors), string(ActionStop),
		},
		Budgets:    request.budgets,
		Snapshot:   promptSnapshot{ID: request.context.Snapshot().ID(), AsOf: request.context.Snapshot().AsOf().Format(time.RFC3339Nano)},
		Auth:       promptAuth{Fingerprint: request.context.Authorization().Fingerprint(), ExpiresAt: request.context.Authorization().ExpiresAt().Format(time.RFC3339Nano)},
		Query:      request.context.Query(),
		ContextID:  transcript.context.ID(),
		Evidence:   evidence,
		Transcript: exchanges,
		Instruction: "Return only one JSON object. Use action retrieve, open_section, neighbors, or stop. " +
			"Final stop claims must cite evidence_ids from this context; put unsupported or ungrounded output in unsupported.",
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

type modelActionPayload struct {
	Action        ActionKind          `json:"action"`
	CorrelationID shoal.ID            `json:"correlation_id"`
	Query         string              `json:"query"`
	Limit         int                 `json:"limit"`
	DocumentID    shoal.ID            `json:"document_id"`
	RevisionID    shoal.ID            `json:"revision_id"`
	SectionID     shoal.ID            `json:"section_id"`
	NodeID        shoal.ID            `json:"node_id"`
	Hops          int                 `json:"hops"`
	Fanout        int                 `json:"fanout"`
	Claims        []modelClaimPayload `json:"claims"`
	Unresolved    []modelIssuePayload `json:"unresolved"`
	Unsupported   []modelIssuePayload `json:"unsupported"`
}

type modelClaimPayload struct {
	Subject     shoal.ID        `json:"subject"`
	Predicate   shoal.ID        `json:"predicate"`
	Object      json.RawMessage `json:"object"`
	Confidence  shoal.Score     `json:"confidence"`
	EvidenceIDs []shoal.ID      `json:"evidence_ids"`
}

type modelIssuePayload struct {
	Input       string     `json:"input"`
	Reason      string     `json:"reason"`
	EvidenceIDs []shoal.ID `json:"evidence_ids"`
}

func parseModelAction(
	text string,
	pack inference.ContextPack,
	provenance Provenance,
	usage Usage,
	generatedAt time.Time,
) (Action, error) {
	var payload modelActionPayload
	decoder := json.NewDecoder(bytes.NewReader([]byte(strings.TrimSpace(text))))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return Action{}, invalid("model output is not a valid action JSON object")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Action{}, invalid("model output contains multiple JSON values")
	}
	switch payload.Action {
	case ActionRetrieve:
		request, err := NewRetrieveRequest(payload.Query, payload.Limit)
		if err != nil {
			return Action{}, err
		}
		return NewRetrieveAction(payload.CorrelationID, request, usage)
	case ActionOpenSection:
		request, err := NewOpenSectionRequest(payload.DocumentID, payload.RevisionID, payload.SectionID)
		if err != nil {
			return Action{}, err
		}
		return NewOpenSectionAction(payload.CorrelationID, request, usage)
	case ActionNeighbors:
		request, err := NewNeighborsRequest(payload.NodeID, payload.Hops, payload.Fanout)
		if err != nil {
			return Action{}, err
		}
		return NewNeighborsAction(payload.CorrelationID, request, usage)
	case ActionStop:
		result, err := resultFromPayload(pack, provenance, payload, generatedAt)
		if err != nil {
			return Action{}, fmt.Errorf("%w: model stop result: %v", ErrInvalid, err)
		}
		return NewStopAction(payload.CorrelationID, result, usage)
	default:
		return Action{}, invalid("model selected an unknown or forbidden action")
	}
}

func resultFromPayload(
	pack inference.ContextPack,
	provenance Provenance,
	payload modelActionPayload,
	generatedAt time.Time,
) (inference.InferenceResult, error) {
	claims := make([]inference.Claim, 0, len(payload.Claims))
	for _, item := range payload.Claims {
		value, err := parseOntologyValue(item.Object)
		if err != nil {
			return inference.InferenceResult{}, err
		}
		claim, err := inference.NewClaim(
			item.Subject, item.Predicate, value, item.Confidence, item.EvidenceIDs,
			inference.ClaimInferred, provenance.model, provenance.prompt, nil)
		if err != nil {
			return inference.InferenceResult{}, err
		}
		claims = append(claims, claim)
	}
	issues := make([]inference.Issue, 0, len(payload.Unresolved)+len(payload.Unsupported))
	for _, item := range payload.Unresolved {
		issue, err := inference.NewIssue(
			inference.IssueUnresolved, item.Input, item.Reason, item.EvidenceIDs)
		if err != nil {
			return inference.InferenceResult{}, err
		}
		issues = append(issues, issue)
	}
	for _, item := range payload.Unsupported {
		issue, err := inference.NewIssue(
			inference.IssueUnsupported, item.Input, item.Reason, item.EvidenceIDs)
		if err != nil {
			return inference.InferenceResult{}, err
		}
		issues = append(issues, issue)
	}
	return inference.NewInferenceResult(pack, claims, issues, generatedAt, nil)
}

type ontologyValuePayload struct {
	Type  ontology.ValueType `json:"type"`
	Value json.RawMessage    `json:"value"`
}

func parseOntologyValue(raw json.RawMessage) (ontology.Value, error) {
	var payload ontologyValuePayload
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return ontology.Value{}, invalid("claim object is not a valid ontology value")
	}
	switch payload.Type {
	case ontology.ValueString:
		var value string
		if err := json.Unmarshal(payload.Value, &value); err != nil {
			return ontology.Value{}, invalid("string claim object has invalid value")
		}
		return ontology.NewStringValue(value)
	case ontology.ValueInteger:
		var number json.Number
		if err := json.Unmarshal(payload.Value, &number); err != nil {
			return ontology.Value{}, invalid("integer claim object has invalid value")
		}
		value, err := strconv.ParseInt(number.String(), 10, 64)
		if err != nil {
			return ontology.Value{}, invalid("integer claim object is out of range")
		}
		return ontology.NewIntegerValue(value), nil
	case ontology.ValueNumber:
		var number json.Number
		if err := json.Unmarshal(payload.Value, &number); err != nil {
			return ontology.Value{}, invalid("number claim object has invalid value")
		}
		value, err := strconv.ParseFloat(number.String(), 64)
		if err != nil {
			return ontology.Value{}, invalid("number claim object is invalid")
		}
		return ontology.NewNumberValue(value)
	case ontology.ValueBoolean:
		var value bool
		if err := json.Unmarshal(payload.Value, &value); err != nil {
			return ontology.Value{}, invalid("boolean claim object has invalid value")
		}
		return ontology.NewBooleanValue(value), nil
	case ontology.ValueTimestamp:
		var value string
		if err := json.Unmarshal(payload.Value, &value); err != nil {
			return ontology.Value{}, invalid("timestamp claim object has invalid value")
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return ontology.Value{}, invalid("timestamp claim object is invalid")
		}
		return ontology.NewTimestampValue(parsed)
	case ontology.ValueReference:
		var value shoal.ID
		if err := json.Unmarshal(payload.Value, &value); err != nil {
			return ontology.Value{}, invalid("reference claim object has invalid value")
		}
		return ontology.NewReferenceValue(value)
	default:
		return ontology.Value{}, invalid(fmt.Sprintf("unsupported claim object type %q", payload.Type))
	}
}
