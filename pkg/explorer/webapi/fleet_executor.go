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
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
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
//
// The invoking decision must carry auth.OperationRetrieve in addition to
// auth.OperationInvoke: the reasoning service authorizes retrieval on its own
// terms. A principal granted only invoke and dispatch fails every action with
// AskErrorRetrieveUnauthorized rather than a generic reasoning failure.
//
// Action output is a receipt, not the answer. DispatchService.Status, Pull and
// TeamActions authorize a reader on (domain, source, policy, object) and apply
// no visibility-label check before serializing an ActionRecord, so anything
// this executor writes there is readable by a principal who does not hold the
// source labels. Claim values, issue reasons, ontology identifiers, and the
// effective output label expression are all derived from retrieved documents,
// so none of them belong in Output. The receipt reports only what the
// execution did; the verified claims stay reachable through the
// visibility-enforcing interaction path using the recorded session ID.
//
// ActionRecord.Evidence is the larger surface and is not addressed here: it
// carries citation identifiers, offsets, and label expressions past the same
// unenforced read path. Each reference records its own Visibility, but no
// reader applies it. That gap belongs to the dispatch read path rather than to
// this executor and is tracked by issue #369.
const (
	// AskCapability and AskAction name the single capability this executor
	// serves. A descriptor must declare both for resolution to select it.
	AskCapability = "explorer.reason"
	AskAction     = "ask"

	// MaxAskQuestionBytes bounds one decoded question independently of the
	// action's declared input schema.
	MaxAskQuestionBytes = 4096
)

// Executor error codes. They are recorded verbatim on a failed ActionRecord
// and are stable wire values.
const (
	AskErrorUnsupportedAction = "unsupported_action"
	AskErrorInvalidInput      = "invalid_input"
	AskErrorReasoningFailed   = "reasoning_failed"
	AskErrorUnverified        = "unverified_response"
	AskErrorUngroundedClaims  = "ungrounded_claims"
	// AskErrorReasoningUnauthorized covers every authorization refusal from
	// the reasoning path. The usual cause is an agent principal granted
	// invoke and dispatch but not retrieve, but a workspace limit set to zero
	// and a per-object or mosaic-budget denial land here too, so the code does
	// not claim to name which.
	AskErrorReasoningUnauthorized = "reasoning_unauthorized"
	AskErrorEvidenceSkew          = "evidence_snapshot_skew"
	AskErrorEvidenceUnpinned      = "evidence_unpinned"
	AskErrorEvidenceUnrecordable  = "evidence_unrecordable"
	AskErrorOutputEncoding        = "output_encoding_failed"
)

// AskExecutorConfig configures one executor. Provider is required and is
// always an authorization-enforcing reasoning service.
type AskExecutorConfig struct {
	Provider   AskProvider
	Capability string
	Action     string
	// Clock defaults to time.Now and exists so snapshot-skew detection is
	// testable. It is never used to stamp provenance.
	Clock func() time.Time
}

// AskExecutor implements fleet.ActionExecutor.
type AskExecutor struct {
	provider   AskProvider
	capability string
	action     string
	clock      func() time.Time
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
	clock := config.Clock
	if clock == nil {
		clock = time.Now
	}
	return &AskExecutor{
		provider: config.Provider, capability: capability,
		action: action, clock: clock,
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
		`"evidence_considered":{"type":"integer"},` +
		`"evidence_unusable":{"type":"integer"},` +
		`"evidence_truncated":{"type":"boolean"},` +
		`"claim_count":{"type":"integer"},` +
		`"issue_count":{"type":"integer"}},` +
		`"required":["verification","evidence_count","evidence_considered",` +
		`"evidence_unusable","evidence_truncated","claim_count",` +
		`"issue_count"],"additionalProperties":false}`)
}

type askExecutorInput struct {
	Question string `json:"question"`
	TopK     uint32 `json:"top_k,omitempty"`
}

// AskExecutionOutput is the executor's action output receipt. It deliberately
// carries no document-derived content: see the AskExecutor doc comment.
type AskExecutionOutput struct {
	Verification  string `json:"verification"`
	SnapshotID    string `json:"snapshot_id,omitempty"`
	SessionID     string `json:"session_id,omitempty"`
	EvidenceCount int    `json:"evidence_count"`
	// EvidenceConsidered and EvidenceTruncated report when the dispatch member
	// bound dropped usable anchors. The bound counts members, not anchors, so
	// a fully-cited document anchor costs three and an evidence-rich answer
	// truncates well before the reasoning harness anchor limit. Without this a
	// reader cannot tell a complete grounding from a clipped one.
	EvidenceConsidered int `json:"evidence_considered"`
	// EvidenceUnusable counts anchors the envelope offered that could not be
	// expressed as a dispatch evidence reference at all. It is separate from
	// truncation because the two mean different things: an unusable anchor is
	// a correctness problem upstream, a truncated one is a bounds problem.
	EvidenceUnusable  int  `json:"evidence_unusable"`
	EvidenceTruncated bool `json:"evidence_truncated"`
	ClaimCount        int  `json:"claim_count"`
	IssueCount        int  `json:"issue_count"`
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
	// An absent top_k is forwarded as zero so the reasoning service applies
	// its own workspace-aware default. Substituting a constant here would
	// exceed a workspace whose retrieval limit is lower and fail the action
	// for a question the same principal can ask over chat.
	envelope, err := e.provider.Ask(
		ctx, AskRequest{Question: input.Question, TopK: input.TopK})
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
			// Distinguish an authorization refusal from a model or transport
			// failure. The error itself carries which refusal it was.
			return fleet.ExecutionResult{
				ErrorCode: AskErrorReasoningUnauthorized}, err
		}
		return fleet.ExecutionResult{ErrorCode: AskErrorReasoningFailed}, err
	}
	// Both consumers take the same AskProvider, so the executor applies the
	// chat transport's own finalization contract rather than a weaker subset
	// of it: output visibility matching the verified evidence, a complete
	// workspace settings identity, and a resolved ontology interpretation are
	// all part of what "verified" means here.
	if err := validateFinalizedChatResponse(envelope); err != nil {
		return fleet.ExecutionResult{ErrorCode: AskErrorUnverified}, err
	}
	evidence, tally := askExecutorEvidence(envelope)
	// The guard is on what the envelope offered, not on what survived
	// translation: an answer whose anchors were all unusable is exactly the
	// case that must not commit.
	if tally.considered > 0 && len(evidence) == 0 {
		// Nothing could be recorded. Succeeding would commit a record
		// asserting claims that the record itself does not ground. Name which
		// of the two causes it was, because they need different responses: an
		// unusable anchor is a correctness problem in the reasoning path,
		// while an anchor too large to pin is a bounds problem.
		code := AskErrorUngroundedClaims
		detail := "no verified evidence survived translation"
		if tally.usable > 0 {
			code = AskErrorEvidenceUnrecordable
			detail = "verified evidence exceeds the recordable member bound"
		}
		return fleet.ExecutionResult{ErrorCode: code},
			shoal.NewError(shoal.ErrorInternal, detail)
	}
	result := fleet.ExecutionResult{
		Output: nil, Evidence: evidence,
	}
	if len(evidence) > 0 {
		// A snapshot pin is only meaningful alongside evidence, and the
		// dispatch service rejects one without it.
		asOf := envelope.SnapshotAsOf.UTC()
		if envelope.SnapshotID == "" || asOf.IsZero() {
			// The dispatch service requires a pin alongside evidence and would
			// otherwise reject the whole batch under a code naming nothing.
			return fleet.ExecutionResult{ErrorCode: AskErrorEvidenceUnpinned},
				shoal.NewError(shoal.ErrorInternal,
					"verified evidence has no snapshot pin")
		}
		if asOf.After(e.clock().UTC()) {
			// The service would reject this as outside execution bounds. Fail
			// with a code that names the cause instead of a generic evidence
			// rejection; the pin is provenance and is never rewritten.
			return fleet.ExecutionResult{ErrorCode: AskErrorEvidenceSkew},
				shoal.NewError(shoal.ErrorInternal,
					"evidence snapshot is ahead of the execution clock")
		}
		result.EvidenceSnapshotID = envelope.SnapshotID
		result.EvidenceSnapshotAsOf = asOf
	}
	output, err := json.Marshal(
		askExecutorOutput(envelope, len(evidence), tally))
	if err != nil {
		return fleet.ExecutionResult{ErrorCode: AskErrorOutputEncoding}, err
	}
	result.Output = output
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
	envelope CitationEnvelope, evidenceCount int, tally askEvidenceTally,
) AskExecutionOutput {
	output := AskExecutionOutput{
		Verification:       string(envelope.Verification),
		EvidenceCount:      evidenceCount,
		EvidenceConsidered: tally.considered,
		EvidenceUnusable:   tally.considered - tally.usable,
		EvidenceTruncated:  tally.usable > evidenceCount,
		ClaimCount:         len(envelope.Claims),
		IssueCount:         len(envelope.Issues),
	}
	if evidenceCount > 0 {
		output.SnapshotID = encodeID(envelope.SnapshotID)
	}
	if envelope.SessionID != "" {
		output.SessionID = encodeID(envelope.SessionID)
	}
	return output
}

// askExecutorEvidence translates verified citation evidence into dispatch
// evidence references. Every candidate is canonicalized through the same
// interaction validation the dispatch service applies, and one that does not
// satisfy an evidence variant is skipped rather than failing the batch: a
// single malformed anchor must not discard a verified answer. Execute refuses
// to succeed if nothing survives.
// askEvidenceTally separates the two ways an offered anchor fails to reach the
// record. Collapsing them would let a partially grounded answer report as a
// complete one, which is the distinction the receipt exists to preserve.
type askEvidenceTally struct {
	// considered counts every distinct anchor the envelope offered.
	considered int
	// usable counts those that translated into a valid reference, whether or
	// not the member bound then left room to record them.
	usable int
}

func askExecutorEvidence(
	envelope CitationEnvelope,
) ([]fleet.EvidenceRef, askEvidenceTally) {
	refs := make([]fleet.EvidenceRef, 0, len(envelope.Evidence))
	seen := make(map[shoal.ID]struct{}, len(envelope.Evidence))
	var tally askEvidenceTally
	members := 0
	for _, evidence := range envelope.Evidence {
		if evidence.AnchorID == "" {
			continue
		}
		if _, duplicate := seen[evidence.AnchorID]; duplicate {
			continue
		}
		seen[evidence.AnchorID] = struct{}{}
		tally.considered++
		ref, ok := askExecutorEvidenceRef(evidence)
		if !ok {
			continue
		}
		tally.usable++
		cost := len(ref.NodeIDs) + len(ref.EdgeIDs) + len(ref.Assertions)
		if len(refs) >= fleet.MaxActionEvidence {
			continue
		}
		if members+cost > fleet.MaxActionEvidence {
			// One oversized path must not discard every smaller anchor after
			// it, so keep packing what still fits.
			continue
		}
		members += cost
		refs = append(refs, ref)
	}
	return refs, tally
}

func askExecutorEvidenceRef(
	evidence CitationEvidence,
) (fleet.EvidenceRef, bool) {
	visibility, err := interaction.Conjoin(evidence.Visibility)
	if err != nil || len(visibility) == 0 {
		// Dispatch evidence requires visibility; an unlabeled anchor cannot be
		// attributed and is dropped rather than recorded without a label.
		return fleet.EvidenceRef{}, false
	}
	var ref fleet.EvidenceRef
	switch {
	case evidence.Citation != nil:
		citation := *evidence.Citation
		// The verifier resolves a citation's document, section, and span
		// identities into SourceIDs even when the model cited at document
		// granularity, and the interaction record for this anchor uses those
		// three roles. Rebuilding the roles from the raw citation would commit
		// a narrower grounding than the interaction record asserts for the
		// same anchor.
		nodes := append([]shoal.ID(nil), evidence.SourceIDs...)
		if len(nodes) != 3 {
			nodes = []shoal.ID{citation.DocumentID}
			if citation.SectionID != "" {
				nodes = append(nodes, citation.SectionID)
			}
			if citation.SpanID != "" {
				nodes = append(nodes, citation.SpanID)
			}
		}
		ref = fleet.EvidenceRef{
			AnchorID:   evidence.AnchorID,
			Kind:       interaction.EvidenceDocument,
			Citation:   citation,
			NodeIDs:    dedupeAskEvidenceIDs(nodes),
			Visibility: visibility,
		}
	case evidence.Path != nil && len(evidence.Path.Nodes) > 0:
		path := *evidence.Path
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
			// An assertion must name an edge this path actually references.
			if !containsAskEvidenceID(edges, assertion.EdgeID) {
				continue
			}
			assertions = append(assertions, interaction.AssertionReference{
				AssertionID: assertion.AssertionID,
				EdgeID:      assertion.EdgeID,
				Origin:      assertion.Origin,
			})
		}
		ref = fleet.EvidenceRef{
			AnchorID:   evidence.AnchorID,
			Kind:       interaction.EvidenceGraph,
			NodeIDs:    nodes,
			EdgeIDs:    edges,
			Assertions: assertions,
			Visibility: visibility,
		}
	default:
		return fleet.EvidenceRef{}, false
	}
	canonical, err := interaction.EvidenceReference{
		AnchorID: ref.AnchorID, Kind: ref.Kind, Citation: ref.Citation,
		NodeIDs: ref.NodeIDs, EdgeIDs: ref.EdgeIDs, Assertions: ref.Assertions,
	}.Canonical()
	if err != nil {
		return fleet.EvidenceRef{}, false
	}
	ref.NodeIDs = canonical.NodeIDs
	ref.EdgeIDs = canonical.EdgeIDs
	ref.Assertions = canonical.Assertions
	return ref, true
}

func containsAskEvidenceID(values []shoal.ID, value shoal.ID) bool {
	for _, existing := range values {
		if existing == value {
			return true
		}
	}
	return false
}

func dedupeAskEvidenceIDs(values []shoal.ID) []shoal.ID {
	result := make([]shoal.ID, 0, len(values))
	for _, value := range values {
		if !containsAskEvidenceID(result, value) {
			result = append(result, value)
		}
	}
	return result
}
