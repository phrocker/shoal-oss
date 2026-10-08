// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package narrate

import (
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/decision"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// A failed record's ErrorCode is whatever the executor reported: fleet checks
// only its length and whitespace, so an executor can report a gateway code or
// one fleet itself writes. Every error sentence is therefore a report, never a
// finding, and retry advice depends only on the record's EffectPossible.
func TestErrorCodesAreReportsNotFindings(t *testing.T) {
	r := New(nil)
	codes := append(sourceErrorCodes(t), "made up by an executor")
	for _, code := range codes {
		for _, effect := range []bool{true, false} {
			record := action(fleet.DispatchFailed)
			record.ErrorCode = code
			record.EffectPossible = effect
			sentences, err := r.Action(record, Options{})
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range sentences {
				if !strings.HasPrefix(s.Key, "dispatch.error.") {
					continue
				}
				text := strings.ToLower(s.Text)
				switch s.Role {
				case RoleReason:
					if !strings.Contains(text, "reported") {
						t.Errorf("%s: reason is not phrased as a report: %s", code, s.Text)
					}
					if _, _, known := errorCodeKey(code); known && !strings.Contains(text, "if accurate") {
						t.Errorf("%s: meaning is stated as fact: %s", code, s.Text)
					}
					if len(s.Refs) == 0 {
						t.Errorf("%s: reason does not name the reporter", code)
					}
				case RoleNext:
					possible := strings.Contains(text, "marks an effect as possible")
					if possible != effect {
						t.Errorf("%s effect=%v: next step ignores EffectPossible: %s", code, effect, s.Text)
					}
					if effect {
						if !strings.Contains(text, "reconcile") && !strings.Contains(text, "confirm") {
							t.Errorf("%s: next step does not require reconciliation: %s", code, s.Text)
						}
						for _, unsafe := range []string{"without repeating", "safe to", "can be requested again"} {
							if strings.Contains(text, unsafe) {
								t.Errorf("%s: next step advises a retry while an effect is possible: %s", code, s.Text)
							}
						}
					}
				}
			}
		}
	}
}

// A whole-request decision reason may be the service's or a predictor's own;
// the record does not say which, so it is attributed to both and its meaning
// is conditional.
func TestDecisionReasonsAreReportsNotFindings(t *testing.T) {
	r := New(nil)
	req := request(t, taskConfig(), []shoal.ID{"subject1"}, nil)
	for status, reasons := range DecisionServiceReasons {
		for _, reason := range append(append([]string{}, reasons...), "predictor words") {
			sentences, err := r.Prediction(prediction(t, req, status, reason, nil), Options{})
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range sentences {
				text := strings.ToLower(s.Text)
				switch {
				case strings.HasPrefix(s.Key, "decision.reason."):
					if !strings.Contains(text, "reported, by the predictor or the service") {
						t.Errorf("%s/%s: reason not attributed to both: %s", status, reason, s.Text)
					}
					if knownDecisionReason(status, reason) && !strings.Contains(text, "if accurate") {
						t.Errorf("%s/%s: meaning stated as fact: %s", status, reason, s.Text)
					}
					for _, causal := range []string{"no predictor was run", "so no"} {
						if strings.Contains(text, causal) {
							t.Errorf("%s/%s: causal claim: %s", status, reason, s.Text)
						}
					}
					for _, q := range s.Quotes {
						if q.Attribution != AttributedToPredictorOrService {
							t.Errorf("%s/%s: quote attributed to %s", status, reason, q.Attribution)
						}
					}
				case strings.HasPrefix(s.Key, "decision.next.") && knownDecisionReason(status, reason):
					if !strings.HasPrefix(text, "if ") {
						t.Errorf("%s/%s: next step is not conditional: %s", status, reason, s.Text)
					}
				}
			}
		}
	}
}

func TestSuccessIsAReport(t *testing.T) {
	r := New(nil)
	for _, record := range []fleet.ActionRecord{action(fleet.DispatchSucceeded), admission(fleet.DispatchSucceeded)} {
		sentences, err := r.Action(record, Options{})
		if err != nil {
			t.Fatal(err)
		}
		outcome := sentences[0]
		want := "worker-1"
		if isAdmission(record) {
			want = "alice"
		}
		if !strings.Contains(outcome.Text, "was reported as succeeded by "+want) ||
			!hasRef(outcome, "principal", want) {
			t.Errorf("success not attributed to %s: %s %v", want, outcome.Text, outcome.Refs)
		}
	}
}

func hasRef(s Sentence, kind, id string) bool {
	for _, ref := range s.Refs {
		if ref.Kind == kind && ref.ID == id {
			return true
		}
	}
	return false
}

func refByKey(t *testing.T, sentences []Sentence, key, kind, id string) {
	t.Helper()
	for _, s := range sentences {
		if s.Key == key {
			if !hasRef(s, kind, id) {
				t.Errorf("%s does not ref %s=%s: %v", key, kind, id, s.Refs)
			}
			return
		}
	}
	t.Errorf("no %s sentence", key)
}

// Sentences that name a principal or a claim carry it as a reference.
func TestPrincipalsAreGrounded(t *testing.T) {
	r := New(nil)
	claimed, err := r.Action(action(fleet.DispatchClaimed), Options{Now: t0.Add(2 * 60e9), QuoteInput: true})
	if err != nil {
		t.Fatal(err)
	}
	refByKey(t, claimed, "dispatch.history.requested", "principal", "alice")
	refByKey(t, claimed, "dispatch.history.requested", "request", "req-1")
	refByKey(t, claimed, "dispatch.history.claimant", "principal", "worker-1")
	refByKey(t, claimed, "dispatch.history.claimant", "claim", "claim-1")
	refByKey(t, claimed, "dispatch.blocked.lease_live", "claim", "claim-1")
	refByKey(t, claimed, "dispatch.detail.input", "principal", "alice")

	approved := action(fleet.DispatchSucceeded)
	approved.ApprovalRequestDigest = make([]byte, 32)
	approved.ApproverActor, approved.ApproverSubject = "bob", "bob"
	sentences, err := r.Action(approved, Options{})
	if err != nil {
		t.Fatal(err)
	}
	refByKey(t, sentences, "dispatch.history.approved", "principal", "bob")

	history, err := r.ActionHistory([]fleet.ActionTransition{
		{ID: []byte("t1"), Kind: "action.enqueued", Record: action(fleet.DispatchQueued)},
		{ID: []byte("t2"), Kind: "action.claimed", Record: action(fleet.DispatchClaimed)},
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	refByKey(t, history, "dispatch.transition.enqueue", "principal", "alice")
	refByKey(t, history, "dispatch.transition.claim", "principal", "worker-1")
	refByKey(t, history, "dispatch.transition.claim", "claim", "claim-1")

	for _, verdict := range []fleet.ApprovalVerdict{fleet.ApprovalVerdictApprove, fleet.ApprovalVerdictRefuse} {
		state := fleet.ApprovalApproved
		if verdict == fleet.ApprovalVerdictRefuse {
			state = fleet.ApprovalRefused
		}
		sentences, err := r.Approval(fleet.ApprovalStatus{
			Approval: approvalRecord(state, verdict), State: state,
		}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		refByKey(t, sentences, "approval.transition.requested", "principal", "alice")
		refByKey(t, sentences, "approval.transition."+string(state), "principal", "bob")
		refByKey(t, sentences, "approval.transition."+string(state), "request", "req-2")
	}

	req := request(t, taskConfig(), []shoal.ID{"subject1"}, nil)
	sentences, err = r.Prediction(prediction(t, req, decision.Failed, "deadline_exceeded", nil), Options{})
	if err != nil {
		t.Fatal(err)
	}
	refByKey(t, sentences, "decision.history.requested", "principal", "carol")
}
