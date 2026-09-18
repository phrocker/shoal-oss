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
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/reasoning"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// AskExecutor is the production fleet.ActionExecutor for grounded reasoning.
// It runs one registered agent action through the authorized reasoning path
// that already backs recorded chat, so tool use, retrieval, visibility, and
// interaction recording remain bound to the request's trusted decision. The
// executor adds no model, tool, or storage authority of its own: it decodes a
// bounded question, delegates, and translates the verified citation envelope
// into dispatch evidence.
//
// It deliberately refuses any response that is not finalized, durably
// recorded, and verified, so a dispatch action can never commit a successful
// effect over ungrounded output.
const (
	// AskCapability and AskAction name the single capability this executor
	// serves. A descriptor must declare both for resolution to select it.
	AskCapability = "explorer.reason"
	AskAction     = "ask"

	// MaxAskQuestionBytes bounds one decoded question independently of the
	// action's declared input schema.
	MaxAskQuestionBytes = 4096

	defaultAskTopK uint32 = 8
)

// Executor error codes. They are recorded verbatim on a failed ActionRecord
// and are stable wire values.
const (
	AskErrorUnsupportedAction = "unsupported_action"
	AskErrorInvalidInput      = "invalid_input"
	AskErrorReasoningFailed   = "reasoning_failed"
	AskErrorUnverified        = "unverified_response"
	AskErrorInvalidEvidence   = "invalid_evidence"
	AskErrorOutputEncoding    = "output_encoding_failed"
)

// AskExecutorConfig configures one executor. Provider is required and is
// always an authorization-enforcing reasoning service.
type AskExecutorConfig struct {
	Provider    AskProvider
	Capability  string
	Action      string
	DefaultTopK uint32
}

// AskExecutor implements fleet.ActionExecutor.
type AskExecutor struct {
	provider    AskProvider
	capability  string
	action      string
	defaultTopK uint32
}

var _ fleet.ActionExecutor = (*AskExecutor)(nil)

// NewAskExecutor validates the configuration and returns a ready executor.
func NewAskExecutor(config AskExecutorConfig) (*AskExecutor, error) {
	if isAbsentInterface(config.Provider) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "ask executor requires a reasoning provider")
	}
	capability := config.Capability
	if capability == "" {
		capability = AskCapability
	}
	action := config.Action
	if action == "" {
		action = AskAction
	}
	if strings.TrimSpace(capability) != capability ||
		strings.TrimSpace(action) != action {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "ask executor capability and action must be canonical")
	}
	topK := config.DefaultTopK
	if topK == 0 {
		topK = defaultAskTopK
	}
	if topK > MaxTopK {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "ask executor default top_k exceeds the transport limit")
	}
	return &AskExecutor{
		provider: config.Provider, capability: capability,
		action: action, defaultTopK: topK,
	}, nil
}

// Capability and Action report what this executor serves, so a composition
// owner can register a descriptor that matches without restating literals.
func (e *AskExecutor) Capability() string { return e.capability }
func (e *AskExecutor) Action() string     { return e.action }

// AskActionInputSchema is the declarative schema a descriptor must register
// for this action. Registering it from here keeps admission and execution from
// drifting apart.
func AskActionInputSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{` +
		`"question":{"type":"string"},` +
		`"top_k":{"type":"integer"}},` +
		`"required":["question"],"additionalProperties":false}`)
}

// AskActionOutputSchema is the matching output schema.
func AskActionOutputSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{` +
		`"verification":{"type":"string"},` +
		`"snapshot_id":{"type":"string"},` +
		`"session_id":{"type":"string"},` +
		`"evidence_count":{"type":"integer"},` +
		`"claims":{"type":"array","items":{"type":"object","properties":{` +
		`"subject":{"type":"string"},"predicate":{"type":"string"},` +
		`"object":{"type":"string"},"object_type":{"type":"string"},` +
		`"status":{"type":"string"},"confidence":{"type":"number"}},` +
		`"additionalProperties":false}},` +
		`"issues":{"type":"array","items":{"type":"object","properties":{` +
		`"kind":{"type":"string"},"reason":{"type":"string"}},` +
		`"additionalProperties":false}}},` +
		`"required":["verification","evidence_count","claims","issues"],` +
		`"additionalProperties":false}`)
}

type askExecutorInput struct {
	Question string `json:"question"`
	TopK     uint32 `json:"top_k,omitempty"`
}

// AskOutputClaim is one verified claim in executor output. Opaque IDs are
// unpadded base64url so they round-trip losslessly.
type AskOutputClaim struct {
	Subject    string  `json:"subject"`
	Predicate  string  `json:"predicate"`
	Object     string  `json:"object"`
	ObjectType string  `json:"object_type"`
	Status     string  `json:"status"`
	Confidence float64 `json:"confidence"`
}

// AskOutputIssue is one unresolved issue reported instead of a claim.
type AskOutputIssue struct {
	Kind   string `json:"kind"`
	Reason string `json:"reason"`
}

// AskExecutionOutput is the executor's action output document.
type AskExecutionOutput struct {
	Verification  string           `json:"verification"`
	SnapshotID    string           `json:"snapshot_id,omitempty"`
	SessionID     string           `json:"session_id,omitempty"`
	EvidenceCount int              `json:"evidence_count"`
	Claims        []AskOutputClaim `json:"claims"`
	Issues        []AskOutputIssue `json:"issues"`
}

// Execute runs one claimed action. The dispatch service owns the claim fence,
// the execution deadline, output schema validation, evidence validation, and
// the terminal state transition; this method owns only the translation between
// a fleet invocation and the authorized reasoning path.
func (e *AskExecutor) Execute(
	ctx context.Context, invocation fleet.Invocation,
) (fleet.ExecutionResult, error) {
	if invocation.Capability != e.capability || invocation.Action != e.action {
		return fleet.ExecutionResult{ErrorCode: AskErrorUnsupportedAction},
			shoal.NewError(shoal.ErrorInvalidArgument,
				"ask executor does not serve the invoked action")
	}
	input, err := decodeAskExecutorInput(invocation.Input)
	if err != nil {
		return fleet.ExecutionResult{ErrorCode: AskErrorInvalidInput}, err
	}
	topK := input.TopK
	if topK == 0 {
		topK = e.defaultTopK
	}
	envelope, err := e.provider.Ask(
		ctx, AskRequest{Question: input.Question, TopK: topK})
	if err != nil {
		return fleet.ExecutionResult{ErrorCode: AskErrorReasoningFailed}, err
	}
	if !envelope.Finalized || !envelope.DurablyRecorded ||
		envelope.Verification != reasoning.VerificationVerified {
		return fleet.ExecutionResult{ErrorCode: AskErrorUnverified},
			shoal.NewError(shoal.ErrorInternal,
				"ask response was not verified and durably recorded")
	}
	evidence, err := askExecutorEvidence(envelope)
	if err != nil {
		return fleet.ExecutionResult{ErrorCode: AskErrorInvalidEvidence}, err
	}
	output, err := json.Marshal(askExecutorOutput(envelope, len(evidence)))
	if err != nil {
		return fleet.ExecutionResult{ErrorCode: AskErrorOutputEncoding}, err
	}
	result := fleet.ExecutionResult{Output: output, Evidence: evidence}
	if len(evidence) > 0 {
		// A snapshot pin is only meaningful alongside evidence, and the
		// dispatch service rejects one without it.
		result.EvidenceSnapshotID = envelope.SnapshotID
		result.EvidenceSnapshotAsOf = envelope.SnapshotAsOf.UTC()
	}
	return result, nil
}

func decodeAskExecutorInput(raw json.RawMessage) (askExecutorInput, error) {
	if len(raw) == 0 {
		return askExecutorInput{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "ask executor input is required")
	}
	var input askExecutorInput
	if err := json.Unmarshal(raw, &input); err != nil {
		return askExecutorInput{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "ask executor input is invalid JSON")
	}
	if strings.TrimSpace(input.Question) == "" {
		return askExecutorInput{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "ask executor question is required")
	}
	if len(input.Question) > MaxAskQuestionBytes {
		return askExecutorInput{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "ask executor question exceeds its byte bound")
	}
	if input.TopK > MaxTopK {
		return askExecutorInput{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "ask executor top_k exceeds the transport limit")
	}
	return input, nil
}

func askExecutorOutput(
	envelope CitationEnvelope, evidenceCount int,
) AskExecutionOutput {
	output := AskExecutionOutput{
		Verification:  string(envelope.Verification),
		EvidenceCount: evidenceCount,
		Claims:        make([]AskOutputClaim, 0, len(envelope.Claims)),
		Issues:        make([]AskOutputIssue, 0, len(envelope.Issues)),
	}
	if evidenceCount > 0 {
		output.SnapshotID = encodeID(envelope.SnapshotID)
	}
	if envelope.SessionID != "" {
		output.SessionID = encodeID(envelope.SessionID)
	}
	for _, claim := range envelope.Claims {
		object, objectType := askExecutorValue(claim.Object)
		output.Claims = append(output.Claims, AskOutputClaim{
			Subject:    encodeID(claim.Subject),
			Predicate:  encodeID(claim.Predicate),
			Object:     object,
			ObjectType: objectType,
			Status:     string(claim.Status),
			Confidence: float64(claim.Confidence),
		})
	}
	for _, issue := range envelope.Issues {
		output.Issues = append(output.Issues, AskOutputIssue{
			Kind: string(issue.Kind), Reason: issue.Reason,
		})
	}
	return output
}

// askExecutorValue renders one ontology value as a JSON string plus its
// declared type, so output stays inside the declarative schema subset while
// preserving the value's kind.
func askExecutorValue(value ontology.Value) (string, string) {
	switch value.Type() {
	case ontology.ValueString:
		text, _ := value.StringValue()
		return text, string(value.Type())
	case ontology.ValueInteger:
		number, _ := value.IntegerValue()
		return strconv.FormatInt(number, 10), string(value.Type())
	case ontology.ValueNumber:
		number, _ := value.NumberValue()
		return strconv.FormatFloat(number, 'g', -1, 64), string(value.Type())
	case ontology.ValueBoolean:
		boolean, _ := value.BooleanValue()
		return strconv.FormatBool(boolean), string(value.Type())
	case ontology.ValueTimestamp:
		stamp, _ := value.TimestampValue()
		return stamp.UTC().Format(time.RFC3339Nano), string(value.Type())
	case ontology.ValueReference:
		reference, _ := value.ReferenceValue()
		return encodeID(reference), string(value.Type())
	default:
		return "", string(value.Type())
	}
}

// askExecutorEvidence translates verified citation evidence into dispatch
// evidence references. It emits only entries that satisfy the interaction
// evidence variants — an exact document citation, or a connected graph path —
// and stops at the dispatch member bound rather than producing a reference the
// service would reject wholesale.
func askExecutorEvidence(envelope CitationEnvelope) ([]fleet.EvidenceRef, error) {
	refs := make([]fleet.EvidenceRef, 0, len(envelope.Evidence))
	seen := make(map[shoal.ID]struct{}, len(envelope.Evidence))
	members := 0
	for _, evidence := range envelope.Evidence {
		if evidence.AnchorID == "" {
			continue
		}
		if _, duplicate := seen[evidence.AnchorID]; duplicate {
			continue
		}
		ref, ok, err := askExecutorEvidenceRef(evidence)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		cost := len(ref.NodeIDs) + len(ref.EdgeIDs) + len(ref.Assertions)
		if members+cost > fleet.MaxActionEvidence ||
			len(refs) >= fleet.MaxActionEvidence {
			break
		}
		members += cost
		seen[evidence.AnchorID] = struct{}{}
		refs = append(refs, ref)
	}
	return refs, nil
}

func askExecutorEvidenceRef(
	evidence CitationEvidence,
) (fleet.EvidenceRef, bool, error) {
	visibility, err := interaction.Conjoin(evidence.Visibility)
	if err != nil {
		return fleet.EvidenceRef{}, false, err
	}
	if len(visibility) == 0 {
		// Dispatch evidence requires visibility; an unlabeled anchor cannot be
		// attributed and is dropped rather than recorded without a label.
		return fleet.EvidenceRef{}, false, nil
	}
	switch {
	case evidence.Citation != nil:
		citation := *evidence.Citation
		nodes := []shoal.ID{citation.DocumentID}
		if citation.SectionID != "" {
			nodes = append(nodes, citation.SectionID)
		}
		if citation.SpanID != "" {
			nodes = append(nodes, citation.SpanID)
		}
		return fleet.EvidenceRef{
			AnchorID:   evidence.AnchorID,
			Kind:       interaction.EvidenceDocument,
			Citation:   citation,
			NodeIDs:    dedupeAskEvidenceIDs(nodes),
			Visibility: visibility,
		}, true, nil
	case evidence.Path != nil && len(evidence.Path.Nodes) > 0:
		path := *evidence.Path
		if len(path.Edges) != len(path.Nodes)-1 {
			return fleet.EvidenceRef{}, false, nil
		}
		nodes := make([]shoal.ID, 0, len(path.Nodes))
		for _, node := range path.Nodes {
			nodes = append(nodes, node.ID)
		}
		edges := make([]shoal.ID, 0, len(path.Edges))
		for _, edge := range path.Edges {
			edges = append(edges, edge.ID)
		}
		assertions := make([]interaction.AssertionReference, 0, len(evidence.Assertions))
		for _, assertion := range evidence.Assertions {
			assertions = append(assertions, interaction.AssertionReference{
				AssertionID: assertion.AssertionID,
				EdgeID:      assertion.EdgeID,
				Origin:      assertion.Origin,
			})
		}
		return fleet.EvidenceRef{
			AnchorID:   evidence.AnchorID,
			Kind:       interaction.EvidenceGraph,
			NodeIDs:    nodes,
			EdgeIDs:    edges,
			Assertions: assertions,
			Visibility: visibility,
		}, true, nil
	default:
		return fleet.EvidenceRef{}, false, nil
	}
}

func dedupeAskEvidenceIDs(values []shoal.ID) []shoal.ID {
	result := make([]shoal.ID, 0, len(values))
	for _, value := range values {
		duplicate := false
		for _, existing := range result {
			if existing == value {
				duplicate = true
				break
			}
		}
		if !duplicate {
			result = append(result, value)
		}
	}
	return result
}
