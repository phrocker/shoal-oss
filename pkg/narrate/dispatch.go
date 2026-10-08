// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package narrate

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

func actionRecordID(id []byte) string { return "action:" + opaqueID(id) }

func isAdmission(record fleet.ActionRecord) bool { return len(record.AdmittedEffects) > 0 }

func appliesTo(edge Edge, admission bool) bool {
	switch edge.Applies {
	case "admission":
		return admission
	case "dispatch":
		return !admission
	}
	return true
}

// principal names who acted: the actor, or the subject when no distinct
// actor was recorded.
func (b *builder) principal(actor, subject string) Fragment {
	if actor != "" {
		return b.ident(actor)
	}
	if subject != "" {
		return b.ident(subject)
	}
	return b.frag("principal.unrecorded", Args{})
}

func (b *builder) actionArgs(record fleet.ActionRecord) Args {
	effects := make([]Fragment, 0, len(record.AdmittedEffects))
	for _, effect := range record.AdmittedEffects {
		effects = append(effects, b.ident(string(effect)))
	}
	return Args{
		"action":     b.ident(record.Action),
		"capability": b.ident(record.Capability),
		"agent":      b.ident(string(record.AgentID)),
		"generation": record.AgentGeneration,
		"admission":  yesNo(isAdmission(record)),
		"effects":    b.bounded(effects),
		"at":         record.UpdatedAt,
		"created":    record.CreatedAt,
		"deadline":   record.Deadline,
		"until":      record.ClaimLeaseUntil,
		"lease":      record.ClaimLease,
		"requester":  b.principal(string(record.Actor), string(record.Subject)),
		"claimant":   b.principal(string(record.ClaimantActor), string(record.ClaimantSubject)),
		"reporter":   b.reporter(record),
		"claimed":    yesNo(record.ClaimFence > 0),
	}
}

// reporter names who reports a claimed record's outcome: the admitted caller
// for an admission, which is reported by the identity that requested it, and
// the claimant otherwise.
func (b *builder) reporter(record fleet.ActionRecord) Fragment {
	if isAdmission(record) {
		return b.principal(string(record.Actor), string(record.Subject))
	}
	return b.principal(string(record.ClaimantActor), string(record.ClaimantSubject))
}

func reporterName(record fleet.ActionRecord) string {
	if isAdmission(record) {
		return firstNonEmpty(string(record.Actor), string(record.Subject))
	}
	return firstNonEmpty(string(record.ClaimantActor), string(record.ClaimantSubject))
}

func withArgs(base Args, extra Args) Args {
	out := Args{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// errorCode presents a recorded error code: a code from a closed set as
// itself, anything else as the executor's quoted text.
func (b *builder) errorCode(code string) Fragment {
	if _, _, ok := errorCodeKey(code); ok {
		return Fragment{text: code}
	}
	return b.quote(AttributedToExecutor, "", code)
}

func principalRef(actor, subject string) []Ref {
	if id := firstNonEmpty(actor, subject); id != "" {
		return []Ref{{Kind: "principal", ID: id}}
	}
	return nil
}

func requesterRefs(record fleet.ActionRecord) []Ref {
	refs := principalRef(string(record.Actor), string(record.Subject))
	if record.RequestID != "" {
		refs = append(refs, Ref{Kind: "request", ID: string(record.RequestID)})
	}
	return refs
}

func claimantRefs(record fleet.ActionRecord) []Ref {
	refs := principalRef(string(record.ClaimantActor), string(record.ClaimantSubject))
	if len(record.ClaimID) > 0 {
		refs = append(refs, Ref{Kind: "claim", ID: string(record.ClaimID)})
	}
	return refs
}

// reporterRefs names who reported a terminal outcome: the admitted caller
// for an admission, the claimant otherwise.
func reporterRefs(record fleet.ActionRecord) []Ref {
	if isAdmission(record) {
		return requesterRefs(record)
	}
	return claimantRefs(record)
}

func actionRefs(record fleet.ActionRecord) []Ref {
	refs := []Ref{{Kind: "agent", ID: string(record.AgentID)}}
	if record.RequestID != "" {
		refs = append(refs, Ref{Kind: "request", ID: string(record.RequestID)})
	}
	if record.CorrelationID != "" {
		refs = append(refs, Ref{Kind: "correlation", ID: string(record.CorrelationID)})
	}
	return refs
}

// Action narrates one dispatch action record: its state, why it is there,
// what the record cannot establish, how it got there, what blocks the next
// step, and what can happen next.
func (r *Renderer) Action(record fleet.ActionRecord, opts Options) ([]Sentence, error) {
	b := r.begin(actionRecordID(record.ID), opts)
	args := b.actionArgs(record)
	admission := isAdmission(record)
	state := record.State

	known := false
	for _, s := range DispatchStates {
		known = known || s == state
	}
	if !known {
		b.add(RoleOutcome, "dispatch.outcome.unrecognized", withArgs(args, Args{
			"state": b.quote(AttributedToRecord, "", string(state)),
		}), actionRefs(record)...)
		return b.finish()
	}
	outcomeRefs := actionRefs(record)
	if state == fleet.DispatchSucceeded || state == fleet.DispatchFailed {
		outcomeRefs = append(outcomeRefs, reporterRefs(record)...)
	}
	b.add(RoleOutcome, "dispatch.outcome."+string(state), args, outcomeRefs...)

	// Why.
	if state == fleet.DispatchFailed {
		stem, status, _ := errorCodeKey(record.ErrorCode)
		b.add(RoleReason, stem, withArgs(args, Args{
			"status": status, "code": b.errorCode(record.ErrorCode),
		}), reporterRefs(record)...)
	}
	if state == fleet.DispatchCanceled && !admission && record.ClaimFence > 0 {
		b.add(RoleReason, "dispatch.reason.canceled_after_lapse", args)
	}
	if len(record.Evidence) > 0 {
		refs := make([]Ref, 0, len(record.Evidence)+1)
		if record.EvidenceSnapshotID != "" {
			refs = append(refs, Ref{Kind: "snapshot", ID: string(record.EvidenceSnapshotID)})
		}
		for _, evidence := range record.Evidence {
			refs = append(refs, Ref{Kind: "evidence", ID: string(evidence.AnchorID)})
		}
		b.add(RoleReason, "dispatch.evidence", withArgs(args, Args{
			"n":        len(record.Evidence),
			"snapshot": b.ident(string(record.EvidenceSnapshotID)),
			"asof":     record.EvidenceSnapshotAsOf,
		}), refs...)
	}

	// What the record cannot establish.
	//
	// EffectPossible is read only while a claim is open, where it is set from
	// the action's declared effects when the claim is taken. On a completed
	// record it says nothing: completion sets it unconditionally and Validate
	// refuses a terminal record without it (#508; #510 makes it answerable).
	if record.EffectPossible && state == fleet.DispatchClaimed {
		b.add(RoleGap, "dispatch.gap.effect_possible", args)
	}
	if state == fleet.DispatchSucceeded && len(record.Evidence) == 0 {
		b.add(RoleGap, "dispatch.gap.no_evidence", args)
	}

	// How it got here.
	b.add(RoleHistory, "dispatch.history.requested", args, requesterRefs(record)...)
	if len(record.ApprovalRequestDigest) > 0 {
		b.add(RoleHistory, "dispatch.history.approved", withArgs(args, Args{
			"approver": b.principal(string(record.ApproverActor), string(record.ApproverSubject)),
			"approved": record.ApprovedAt,
			"policy":   record.ApprovalPolicyGeneration,
		}), append(principalRef(string(record.ApproverActor), string(record.ApproverSubject)),
			Ref{Kind: "approval_digest", ID: hex.EncodeToString(record.ApprovalRequestDigest)})...)
	}
	if record.ClaimFence > 0 && !admission {
		lapsed := int64(record.ClaimFence) - 1
		if lapsed < 0 {
			lapsed = 0
		}
		b.add(RoleHistory, "dispatch.history.claims", withArgs(args, Args{
			"claims": record.ClaimFence, "lapsed": lapsed,
		}))
	}
	// An admission's claim is the admitted caller's own, already named as the
	// requester; only a dispatched action has a claimant worth naming.
	if record.ClaimFence > 0 && !admission &&
		(record.ClaimantActor != "" || record.ClaimantSubject != "") {
		live := Selector("unknown")
		if state != fleet.DispatchClaimed {
			live = "ended"
		} else if !opts.Now.IsZero() {
			live = yesNo(opts.Now.Before(record.ClaimLeaseUntil))
		}
		b.add(RoleHistory, "dispatch.history.claimant", withArgs(args, Args{"live": live}),
			claimantRefs(record)...)
	}

	// What blocks the next step, and what can happen next.
	b.dispatchNext(record, args)

	// Quoted detail.
	if opts.QuoteInput && len(record.Input) > 0 {
		b.add(RoleDetail, "dispatch.detail.input", withArgs(args, Args{
			"input": b.quote(AttributedToRequester,
				firstNonEmpty(string(record.Actor), string(record.Subject)), string(record.Input)),
		}), requesterRefs(record)...)
	}
	if opts.QuoteOutput && len(record.Output) > 0 {
		b.add(RoleDetail, "dispatch.detail.output", withArgs(args, Args{
			"output": b.quote(AttributedToExecutor, reporterName(record), string(record.Output)),
		}), reporterRefs(record)...)
	}
	return b.finish()
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// dispatchNext derives "what next" from the outgoing edges of the record's
// state and "why not yet" from the guards that block them. Without
// Options.Now no guard is evaluated and each option states its condition.
func (b *builder) dispatchNext(record fleet.ActionRecord, args Args) {
	admission := isAdmission(record)
	switch record.State {
	case fleet.DispatchSucceeded:
		b.add(RoleNext, "dispatch.next.succeeded", args)
		return
	case fleet.DispatchFailed:
		stem, status, _ := errorCodeKey(record.ErrorCode)
		b.add(RoleNext, stem+".next", withArgs(args, Args{"status": status}))
		return
	case fleet.DispatchCanceled:
		b.add(RoleNext, "dispatch.next.canceled", args)
		return
	}
	now := b.opts.Now
	timed := !now.IsZero()
	leaseLive := timed && now.Before(record.ClaimLeaseUntil) && now.Before(record.Deadline)
	deadlinePassed := timed && !now.Before(record.Deadline)
	blocked := map[string]bool{}
	var options []Fragment
	for _, edge := range DispatchEdges {
		if edge.From != string(record.State) || !appliesTo(edge, admission) {
			continue
		}
		if timed {
			reason := ""
			switch edge.Name {
			case "claim":
				if deadlinePassed {
					reason = "deadline_passed"
				}
			case "complete", "fail":
				if !leaseLive {
					reason = "lease_lapsed"
				}
			case "reclaim":
				if leaseLive {
					reason = "lease_live"
				} else if deadlinePassed {
					reason = "deadline_passed"
				}
			case "cancel_lapsed":
				if leaseLive {
					reason = "lease_live"
				}
			}
			if reason != "" {
				blocked[reason] = true
				continue
			}
		}
		options = append(options, b.frag("dispatch.next.edge."+edge.Name, args))
	}
	left := Args{}
	if timed {
		left["left"] = record.ClaimLeaseUntil.Sub(now)
	}
	for _, reason := range []string{"lease_live", "lease_lapsed", "deadline_passed"} {
		if blocked[reason] {
			b.add(RoleBlocked, "dispatch.blocked."+reason, withArgs(args, left), claimantRefs(record)...)
		}
	}
	if record.State == fleet.DispatchQueued && !deadlinePassed {
		b.add(RoleBlocked, "dispatch.blocked.unclaimed", args)
	}
	if len(options) == 0 {
		b.add(RoleNext, "dispatch.next.none", args)
		return
	}
	b.add(RoleNext, "dispatch.next.options", Args{"options": options})
}

// ActionHistory narrates a sequence of committed transitions of one action,
// oldest first, one sentence per transition. A run of consecutive claims is
// aggregated into one sentence, since each claim after the first means the
// one before it lapsed without a report.
func (r *Renderer) ActionHistory(transitions []fleet.ActionTransition, opts Options) ([]Sentence, error) {
	if len(transitions) == 0 {
		return nil, nil
	}
	id := transitions[0].Record.ID
	b := r.begin(actionRecordID(id), opts)
	prev := ""
	var lastVersion uint64
	for i := 0; i < len(transitions); i++ {
		t := transitions[i]
		if !bytes.Equal(t.Record.ID, id) {
			return nil, fmt.Errorf("narrate: transitions name more than one action")
		}
		if i > 0 && t.Record.Version <= lastVersion {
			return nil, fmt.Errorf("narrate: transitions are not in version order")
		}
		lastVersion = t.Record.Version
		admission := isAdmission(t.Record)
		edge, ok := findEdge(prev, string(t.Record.State), t.Kind, admission)
		args := b.actionArgs(t.Record)
		refs := []Ref{{Kind: "transition", ID: string(t.ID)}}
		if !ok {
			b.add(RoleHistory, "dispatch.transition.unrecognized", withArgs(args, Args{
				"kind": b.quote(AttributedToRecord, "", t.Kind),
			}), refs...)
			prev = string(t.Record.State)
			continue
		}
		// A history that starts part way through cannot see the claims
		// before it; the fence says whether this one was a re-claim.
		if edge.Name == "claim" && prev == "" && t.Record.ClaimFence > 1 {
			edge = Edge{Name: "reclaim"}
		}
		if edge.Name == "claim" || edge.Name == "reclaim" {
			j := i + 1
			for j < len(transitions) && transitions[j].Kind == "action.claimed" &&
				bytes.Equal(transitions[j].Record.ID, id) &&
				transitions[j].Record.Version > transitions[j-1].Record.Version &&
				transitions[j].Record.State == fleet.DispatchClaimed {
				refs = append(refs, Ref{Kind: "transition", ID: string(transitions[j].ID)})
				j++
			}
			if run := j - i; run > 1 {
				last := transitions[j-1].Record
				var claimants []Fragment
				seen := map[string]bool{}
				for _, c := range transitions[i:j] {
					refs = append(refs, claimantRefs(c.Record)...)
					name := firstNonEmpty(string(c.Record.ClaimantActor), string(c.Record.ClaimantSubject))
					if name == "" || seen[name] {
						continue
					}
					seen[name] = true
					claimants = append(claimants, b.ident(name))
				}
				b.add(RoleHistory, "dispatch.transition.claim_run", withArgs(args, Args{
					"count":     run,
					"first":     t.Record.UpdatedAt,
					"last":      last.UpdatedAt,
					"claimants": b.bounded(claimants),
					"named":     len(claimants),
				}), refs...)
				lastVersion = last.Version
				prev = string(last.State)
				i = j - 1
				continue
			}
		}
		if edge.Name == "cancel" && t.Record.ClaimFence > 0 {
			edge = Edge{Name: "cancel_lapsed"}
		}
		switch edge.Name {
		case "enqueue", "admit", "deny":
			refs = append(refs, requesterRefs(t.Record)...)
		case "claim", "reclaim":
			refs = append(refs, claimantRefs(t.Record)...)
		case "complete", "fail":
			refs = append(refs, reporterRefs(t.Record)...)
		}
		extra := Args{}
		if edge.Name == "fail" {
			extra["code"] = b.errorCode(t.Record.ErrorCode)
		}
		b.add(RoleHistory, "dispatch.transition."+edge.Name, withArgs(args, extra), refs...)
		prev = string(t.Record.State)
	}
	return b.finish()
}

// findEdge identifies the edge a transition took. A history may start part
// way through, so when nothing leaves prev the edge is matched on its
// destination and kind alone.
func findEdge(prev, to, kind string, admission bool) (Edge, bool) {
	for _, edge := range DispatchEdges {
		if edge.From == prev && edge.To == to && edge.Kind == kind && appliesTo(edge, admission) {
			return edge, true
		}
	}
	if prev != "" {
		return Edge{}, false
	}
	for _, edge := range DispatchEdges {
		if edge.To == to && edge.Kind == kind && appliesTo(edge, admission) {
			return edge, true
		}
	}
	return Edge{}, false
}
