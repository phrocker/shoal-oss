// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package narrate

import (
	"strconv"
	"strings"

	"github.com/phrocker/shoal-oss/pkg/decision"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// The closed vocabularies this package narrates. Where the owner exports
// typed constants they are referenced directly; where the owner is internal/
// or writes string literals, the set is mirrored here and a parity test reads
// the owner's source, so a value added there without a template here fails.

// Effects gateway error codes: the closed set in
// internal/effectsgateway/classify.go. Mirrored rather than imported so this
// public package does not depend on an internal one.
const (
	GatewayRequestNotSent       = "request_not_sent"
	GatewayOutcomeUnknown       = "outcome_unknown"
	GatewayRetryExhausted       = "retry_exhausted"
	GatewayInputInvalid         = "input_invalid"
	GatewayTargetRejectedPrefix = "target_rejected_"
)

// GatewayErrorCodes lists the gateway's fixed codes; target_rejected_NNN is
// the parameterized one.
var GatewayErrorCodes = []string{
	GatewayRequestNotSent, GatewayOutcomeUnknown,
	GatewayRetryExhausted, GatewayInputInvalid,
}

// TargetRejectedStatus parses target_rejected_NNN exactly as the gateway's
// ValidErrorCode accepts it: three digits, 400 to 599.
func TargetRejectedStatus(code string) (int, bool) {
	digits, ok := strings.CutPrefix(code, GatewayTargetRejectedPrefix)
	if !ok || len(digits) != 3 || digits[0] == '0' {
		return 0, false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false
		}
	}
	status, err := strconv.Atoi(digits)
	if err != nil || status < 400 || status > 599 {
		return 0, false
	}
	return status, true
}

// FleetErrorCodes are the codes the dispatch service itself writes on a
// failed record (pkg/explorer/fleet/dispatch_service.go).
var FleetErrorCodes = []string{
	"invalid_executor_output",
	"invalid_executor_evidence",
	"invalid_executor_error",
	"executor_error",
}

// errorCodeKey maps a recorded error code to its catalog stem. A known code
// is still only what was reported: the record does not say who assigned it
// (#508), so its templates state the meaning conditionally. ok is false
// for a code outside both closed sets: an executor may record any bounded
// string, and such a code is shown only as a quotation.
func errorCodeKey(code string) (stem string, status int, ok bool) {
	if s, isRejected := TargetRejectedStatus(code); isRejected {
		return "dispatch.error.target_rejected", s, true
	}
	for _, known := range GatewayErrorCodes {
		if code == known {
			return "dispatch.error." + code, 0, true
		}
	}
	for _, known := range FleetErrorCodes {
		if code == known {
			return "dispatch.error." + code, 0, true
		}
	}
	return "dispatch.error.unrecognized", 0, false
}

// DispatchStates is every fleet.DispatchState.
var DispatchStates = []fleet.DispatchState{
	fleet.DispatchQueued, fleet.DispatchClaimed, fleet.DispatchSucceeded,
	fleet.DispatchFailed, fleet.DispatchCanceled,
}

// ActionTransitionKinds is every kind fleet.NewActionTransition accepts.
var ActionTransitionKinds = []string{
	"action.enqueued", "action.claimed", "action.completed",
	"action.failed", "action.canceled",
}

// Edge is one transition of a narrated state machine. From is empty for a
// record's creation. Name keys its templates.
type Edge struct {
	Name string
	From string
	To   string
	// Kind is the durable transition kind, where the owner has one.
	Kind string
	// Applies limits the edge to admission records ("admission") or to
	// dispatched ones ("dispatch"); empty applies to both.
	Applies string
}

// DispatchEdges is the dispatch state machine (docs/admission-seam.md,
// pkg/explorer/fleet/dispatch_service.go). An admission is born claimed, or
// born canceled when refused, and cannot be re-claimed or canceled.
var DispatchEdges = []Edge{
	{Name: "enqueue", To: "queued", Kind: "action.enqueued", Applies: "dispatch"},
	{Name: "admit", To: "claimed", Kind: "action.claimed", Applies: "admission"},
	{Name: "deny", To: "canceled", Kind: "action.canceled", Applies: "admission"},
	{Name: "claim", From: "queued", To: "claimed", Kind: "action.claimed", Applies: "dispatch"},
	{Name: "cancel", From: "queued", To: "canceled", Kind: "action.canceled", Applies: "dispatch"},
	{Name: "complete", From: "claimed", To: "succeeded", Kind: "action.completed"},
	{Name: "fail", From: "claimed", To: "failed", Kind: "action.failed"},
	{Name: "reclaim", From: "claimed", To: "claimed", Kind: "action.claimed", Applies: "dispatch"},
	{Name: "cancel_lapsed", From: "claimed", To: "canceled", Kind: "action.canceled", Applies: "dispatch"},
}

// ApprovalStates is every stored fleet.ApprovalState plus the effective-only
// ApprovalUnresolvable.
var ApprovalStates = []fleet.ApprovalState{
	fleet.ApprovalPending, fleet.ApprovalApproved, fleet.ApprovalRefused,
	fleet.ApprovalExpired, fleet.ApprovalEnqueued, fleet.ApprovalUnresolvable,
}

// ApprovalConditions is every fleet.ApprovalCondition, including none.
var ApprovalConditions = []fleet.ApprovalCondition{
	fleet.ApprovalConditionNone, fleet.ApprovalConditionWindowClosed,
	fleet.ApprovalConditionTargetMoved, fleet.ApprovalConditionPolicyMoved,
	fleet.ApprovalConditionDeadlinePassed, fleet.ApprovalConditionIdentityTaken,
	fleet.ApprovalConditionAwaitingAction,
}

// EffectiveApproval is one row of the status table in docs/approval.md: an
// effective state, its condition, and the stored states it can arise from.
type EffectiveApproval struct {
	State     fleet.ApprovalState
	Condition fleet.ApprovalCondition
	Stored    []fleet.ApprovalState
}

// EffectiveApprovals is the status table of docs/approval.md
// ("Status reports the effective state"), as ApprovalService.effectiveState
// computes it.
var EffectiveApprovals = []EffectiveApproval{
	{fleet.ApprovalPending, fleet.ApprovalConditionNone, []fleet.ApprovalState{fleet.ApprovalPending}},
	{fleet.ApprovalApproved, fleet.ApprovalConditionNone, []fleet.ApprovalState{fleet.ApprovalApproved}},
	{fleet.ApprovalExpired, fleet.ApprovalConditionWindowClosed, []fleet.ApprovalState{fleet.ApprovalPending, fleet.ApprovalApproved}},
	{fleet.ApprovalEnqueued, fleet.ApprovalConditionNone, []fleet.ApprovalState{fleet.ApprovalEnqueued}},
	{fleet.ApprovalEnqueued, fleet.ApprovalConditionAwaitingAction, []fleet.ApprovalState{fleet.ApprovalEnqueued}},
	{fleet.ApprovalUnresolvable, fleet.ApprovalConditionIdentityTaken, []fleet.ApprovalState{fleet.ApprovalEnqueued}},
	{fleet.ApprovalUnresolvable, fleet.ApprovalConditionTargetMoved, []fleet.ApprovalState{fleet.ApprovalPending, fleet.ApprovalApproved, fleet.ApprovalEnqueued}},
	{fleet.ApprovalUnresolvable, fleet.ApprovalConditionPolicyMoved, []fleet.ApprovalState{fleet.ApprovalPending, fleet.ApprovalApproved, fleet.ApprovalEnqueued}},
	{fleet.ApprovalUnresolvable, fleet.ApprovalConditionDeadlinePassed, []fleet.ApprovalState{fleet.ApprovalPending, fleet.ApprovalApproved, fleet.ApprovalEnqueued}},
	{fleet.ApprovalRefused, fleet.ApprovalConditionNone, []fleet.ApprovalState{fleet.ApprovalRefused}},
	{fleet.ApprovalExpired, fleet.ApprovalConditionNone, []fleet.ApprovalState{fleet.ApprovalExpired}},
}

// ApprovalEdges is the approval state machine (docs/approval.md, "The model").
var ApprovalEdges = []Edge{
	{Name: "requested", To: "pending"},
	{Name: "approved", From: "pending", To: "approved"},
	{Name: "refused", From: "pending", To: "refused"},
	{Name: "expired_undecided", From: "pending", To: "expired"},
	{Name: "materialized", From: "approved", To: "enqueued"},
	{Name: "expired_unused", From: "approved", To: "expired"},
}

// Decision vocabularies (pkg/decision).
var (
	ResultStatuses = []decision.ResultStatus{decision.Completed, decision.Abstained, decision.Failed}
	AnswerStatuses = []decision.AnswerStatus{decision.Answered, decision.AnswerAbstained}
	AnswerKinds    = []decision.AnswerKind{decision.Choice, decision.Ordinal, decision.Probability}
	Dispositions   = []decision.Disposition{
		decision.Supported, decision.Unsupported, decision.Missing, decision.OutOfScope,
	}
	InspectionReasons = []decision.InspectionReason{
		decision.EvidenceTruncated, decision.SourceStale, decision.SourceRoleRejected,
		decision.SourceControlRejected, decision.SourceAuthorityRejected,
		decision.SourceAttestationMissing, decision.CoverageUnsatisfied,
		decision.SubjectUnsupported, decision.SubjectNotEvaluated,
		decision.PredictionFailed, decision.PredictionAbstained,
	}
)

// A result does not say whether the service or the predictor set its reason
// (#509), so these are narrated conditionally and attributed to both.
//
// DecisionServiceReasons are the whole-request reasons the decision service
// writes (internal/decisionservice/service.go, terminal(...)), by status.
// Mirrored for the same reason as the gateway codes. A predictor may return
// its own whole-request abstention with any reason; such a reason is quoted.
var DecisionServiceReasons = map[decision.ResultStatus][]string{
	decision.Abstained: {"evidence_ineligible"},
	decision.Failed: {
		"predictor_unavailable", "predictor_identity_mismatch",
		"deadline_exceeded", "predictor_failed", "invalid_predictor_response",
	},
}

// AnswerAbstentionReasons are per-answer abstention reasons with a meaning
// documented in docs/decision-contracts.md. Any other reason is the
// predictor's own text and is quoted.
var AnswerAbstentionReasons = []string{"unavailable"}

func knownDecisionReason(status decision.ResultStatus, reason string) bool {
	for _, r := range DecisionServiceReasons[status] {
		if r == reason {
			return true
		}
	}
	return false
}

func knownAnswerReason(reason string) bool {
	for _, r := range AnswerAbstentionReasons {
		if r == reason {
			return true
		}
	}
	return false
}
