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

// A failed record's ErrorCode was written by the service or by the executor,
// and since #529 ErrorCodeOrigin says which; a record written before it does
// not. Who a reason sentence says assigned the code follows the origin alone,
// never the code (#508):
//
//   - service: Shoal's own determination, stated as such ("Shoal recorded the
//     failure as …, which means …");
//   - executor: the executor's account, a report whose meaning is conditional
//     ("reported as …, which, if accurate, means …");
//   - omitted, or a value this build does not know: attributed to neither, and
//     conditional.
//
// EffectPossible carries no information on a completed record — completion
// sets it unconditionally and Validate requires it on a failure (#510) — so
// retry advice never depends on it: a failure's next step is always to
// reconcile with the target, whoever assigned the code, and nothing a
// terminal record says cites EffectPossible.
func TestErrorCodesAreReportsNotFindings(t *testing.T) {
	r := New(nil)
	codes := append(sourceErrorCodes(t), "made up by an executor")
	for _, origin := range errorCodeOriginCases(t) {
		for _, code := range codes {
			what := code + " origin=" + string(origin)
			var nexts []string
			for _, effect := range []bool{true, false} {
				record := failedWith(code, origin)
				record.EffectPossible = effect
				sentences, err := r.Action(record, Options{})
				if err != nil {
					t.Fatal(err)
				}
				for _, s := range sentences {
					text := strings.ToLower(s.Text)
					if strings.Contains(text, "effect as possible") || strings.Contains(text, "may already have happened") {
						t.Errorf("%s: a terminal record cites EffectPossible: %s", what, s.Text)
					}
					if !strings.HasPrefix(s.Key, "dispatch.error.") {
						continue
					}
					switch s.Role {
					case RoleReason:
						reasonIsFaithful(t, what, code, wantOrigin[origin], s)
					case RoleNext:
						nexts = append(nexts, s.Text)
						if !strings.HasPrefix(text, "reconcile with the target") {
							t.Errorf("%s: next step does not start with reconciliation: %s", what, s.Text)
						}
						for _, unsafe := range []string{"without repeating", "safe to", "can be requested again"} {
							if strings.Contains(text, unsafe) {
								t.Errorf("%s: next step advises a retry: %s", what, s.Text)
							}
						}
						nextIsFaithful(t, what, wantOrigin[origin], text)
					}
				}
			}
			if len(nexts) != 2 || nexts[0] != nexts[1] {
				t.Errorf("%s: next step depends on EffectPossible: %q", what, nexts)
			}
		}
	}
	// A succeeded record says nothing about EffectPossible either.
	for _, effect := range []bool{true, false} {
		record := action(fleet.DispatchSucceeded)
		record.EffectPossible = effect
		sentences, err := r.Action(record, Options{})
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range sentences {
			if strings.Contains(s.Text, "effect as possible") || strings.Contains(s.Text, "may already have happened") {
				t.Errorf("succeeded record cites EffectPossible: %s", s.Text)
			}
		}
	}
}

// reasonIsFaithful checks a failure's reason sentence says who assigned the
// code exactly as the origin establishes, and no more.
func reasonIsFaithful(t *testing.T, what, code string, want Selector, s Sentence) {
	t.Helper()
	text := strings.ToLower(s.Text)
	_, _, known := errorCodeKey(code)
	if len(s.Refs) == 0 {
		t.Errorf("%s: reason does not name the reporter", what)
	}
	attribution := AttributedToExecutorOrService
	switch want {
	case OriginService:
		attribution = AttributedToService
		if !strings.HasPrefix(text, "shoal recorded the failure") {
			t.Errorf("%s: Shoal's own code is not stated as Shoal's: %s", what, s.Text)
		}
		if strings.Contains(text, "if accurate") || strings.Contains(text, "reported") {
			t.Errorf("%s: Shoal's own code is hedged as a report: %s", what, s.Text)
		}
	case OriginExecutor:
		attribution = AttributedToExecutor
		if !strings.Contains(text, "reported") || strings.Contains(text, "shoal recorded") {
			t.Errorf("%s: the executor's code is not phrased as its report: %s", what, s.Text)
		}
		if known && !strings.Contains(text, "if accurate") {
			t.Errorf("%s: the executor's meaning is stated as fact: %s", what, s.Text)
		}
	default:
		if !strings.Contains(text, "either the executor reported or shoal assigned") ||
			!strings.Contains(text, "does not say which") {
			t.Errorf("%s: a code of unknown origin is not attributed to neither: %s", what, s.Text)
		}
		if strings.Contains(text, "shoal recorded") || strings.Contains(text, "was reported as") {
			t.Errorf("%s: a code of unknown origin is attributed to one side: %s", what, s.Text)
		}
		if known && !strings.Contains(text, "if accurate") {
			t.Errorf("%s: a code of unknown origin is stated as fact: %s", what, s.Text)
		}
	}
	for _, q := range s.Quotes {
		if q.Attribution != attribution {
			t.Errorf("%s: code quoted as %s's, want %s's", what, q.Attribution, attribution)
		}
	}
	if known == (len(s.Quotes) > 0) {
		t.Errorf("%s: a known code is quoted, or an unknown one is not: %s", what, s.Text)
	}
}

// nextIsFaithful checks a failure's next step does not contradict who the
// origin says assigned the code: only the executor's code is "the report",
// a code of unknown origin is only "the code", and Shoal's own code is
// neither second-guessed nor said to be beyond Shoal.
func nextIsFaithful(t *testing.T, what string, want Selector, text string) {
	t.Helper()
	var wrong []string
	switch want {
	case OriginService:
		wrong = []string{"the report says", "the code says", "shoal cannot interpret"}
	case OriginExecutor:
		wrong = []string{"the code says", "this renderer"}
	default:
		wrong = []string{"the report says", "shoal cannot interpret"}
	}
	for _, phrase := range wrong {
		if strings.Contains(text, phrase) {
			t.Errorf("%s: next step says %q under a %s origin: %s", what, phrase, want, text)
		}
	}
}

// failureSentences renders a failed record and the transition that failed it.
func failureSentences(t *testing.T, r *Renderer, record fleet.ActionRecord) []Sentence {
	t.Helper()
	sentences, err := r.Action(record, Options{})
	if err != nil {
		t.Fatal(err)
	}
	history, err := r.ActionHistory([]fleet.ActionTransition{
		{ID: []byte("t1"), Kind: "action.failed", Record: record},
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return append(sentences, history...)
}

// TestAnUnknownOriginIsNeverTheExecutors: a record written before #529 has no
// origin, and its code may have been the service's (invalid_executor_output,
// say) or the executor's. Reading it as the executor's would put Shoal's
// adjudication in the worker's mouth, or a worker's claim in nobody's, so it
// is attributed to neither — and so is any origin this build does not know,
// which is how a record from a newer build arrives.
func TestAnUnknownOriginIsNeverTheExecutors(t *testing.T) {
	r := New(nil)
	unknown := []fleet.ErrorCodeOrigin{
		fleet.ErrorCodeOriginUnknown, futureOrigin,
		"Executor", " executor", "executor\x00", "service ",
	}
	for _, origin := range unknown {
		if got := errorOrigin(origin); got != OriginEither {
			t.Errorf("origin %q is narrated as %q, not as either", origin, got)
		}
	}
	for _, code := range append(sourceErrorCodes(t), "made up by an executor") {
		omitted := failureSentences(t, r, failedWith(code, fleet.ErrorCodeOriginUnknown))
		executor := failureSentences(t, r, failedWith(code, fleet.ErrorCodeOriginExecutor))
		if len(omitted) != len(executor) {
			t.Fatalf("%s: %d sentences, %d for the executor's", code, len(omitted), len(executor))
		}
		for i, s := range omitted {
			text := strings.ToLower(s.Text)
			for _, phrase := range []string{"reported as failed by", "reported failure with", "was reported as", "shoal recorded"} {
				if strings.Contains(text, phrase) {
					t.Errorf("%s: an omitted origin is attributed (%q): %s", code, phrase, s.Text)
				}
			}
			for _, q := range s.Quotes {
				if q.Attribution == AttributedToExecutor || q.Attribution == AttributedToService {
					t.Errorf("%s: an omitted origin's code is quoted as %s's", code, q.Attribution)
				}
			}
			// Where the executor's account reads differently, the omitted
			// one must not read like it: the outcome, the reason and the
			// failing transition.
			switch s.Role {
			case RoleOutcome, RoleReason:
				if s.Text == executor[i].Text {
					t.Errorf("%s: an omitted origin reads as the executor's: %s", code, s.Text)
				}
			case RoleHistory:
				if s.Key == "dispatch.transition.fail" && s.Text == executor[i].Text {
					t.Errorf("%s: an omitted origin reads as the executor's: %s", code, s.Text)
				}
			}
		}
		for _, origin := range unknown[1:] {
			other := failureSentences(t, r, failedWith(code, origin))
			if dump(other) != dump(omitted) {
				t.Errorf("%s: origin %q renders differently from an omitted one:\n%s\n---\n%s",
					code, origin, dump(other), dump(omitted))
			}
		}
	}
}

// TestShoalIsCreditedOnlyWithTheCode: an origin of service says Shoal
// assigned the error code, and nothing more. In particular it does not say
// the executor reported a failure — it may have reported success, with
// output Shoal then refused (invalid_executor_output) — and it does not say
// Shoal decided the work failed: executor_error is Shoal's code for a
// failure the executor reported without one.
func TestShoalIsCreditedOnlyWithTheCode(t *testing.T) {
	r := New(nil)
	for _, code := range append(sourceErrorCodes(t), "made up by an executor") {
		for _, s := range failureSentences(t, r, failedWith(code, fleet.ErrorCodeOriginService)) {
			text := strings.ToLower(s.Text)
			for _, phrase := range []string{"reported as failed", "reported failure", "reported the failure"} {
				if strings.Contains(text, phrase) {
					t.Errorf("%s: the reporter is said to have reported a failure: %s", code, s.Text)
				}
			}
			for _, phrase := range []string{"shoal failed", "shoal decided", "failed by shoal", "shoal determined"} {
				if strings.Contains(text, phrase) {
					t.Errorf("%s: Shoal is credited with the failure itself: %s", code, s.Text)
				}
			}
			if s.Role == RoleOutcome && strings.Contains(text, "shoal") {
				t.Errorf("%s: the outcome credits Shoal: %s", code, s.Text)
			}
			if strings.Contains(text, "shoal") && s.Role != RoleReason && s.Key != "dispatch.transition.fail" {
				t.Errorf("%s: %s names Shoal: %s", code, s.Key, s.Text)
			}
		}
	}
}

// Admissions are reported by the identity that requested them; histories and
// outcomes name it, never an unrecorded claimant.
func TestAdmissionReporterIsTheRequester(t *testing.T) {
	r := New(nil)
	granted := admission(fleet.DispatchClaimed)
	for _, c := range []struct {
		end     fleet.DispatchState
		origin  fleet.ErrorCodeOrigin
		outcome string
	}{
		{fleet.DispatchSucceeded, "", "reported as succeeded by alice"},
		{fleet.DispatchFailed, fleet.ErrorCodeOriginExecutor, "reported as failed by alice"},
		{fleet.DispatchFailed, fleet.ErrorCodeOriginService, "after alice reported its outcome"},
		{fleet.DispatchFailed, fleet.ErrorCodeOriginUnknown, "after alice reported its outcome"},
	} {
		end := c.end
		finished := admission(end)
		finished.ErrorCodeOrigin = c.origin
		finished.Version = granted.Version + 1
		kind := "action.completed"
		if end == fleet.DispatchFailed {
			kind = "action.failed"
		}
		history, err := r.ActionHistory([]fleet.ActionTransition{
			{ID: []byte("t1"), Kind: "action.claimed", Record: granted},
			{ID: []byte("t2"), Kind: kind, Record: finished},
		}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		last := history[len(history)-1]
		if !strings.Contains(last.Text, "alice reported") || !hasRef(last, "principal", "alice") ||
			strings.Contains(last.Text, "unrecorded") {
			t.Errorf("%s: admission report not attributed to the requester: %s %v", end, last.Text, last.Refs)
		}
		sentences, err := r.Action(finished, Options{QuoteOutput: true})
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range sentences {
			if strings.Contains(s.Text, "unrecorded principal") {
				t.Errorf("%s: %s", end, s.Text)
			}
			for _, q := range s.Quotes {
				if s.Key == "dispatch.detail.output" && q.By != "alice" {
					t.Errorf("%s: output attributed to %q", end, q.By)
				}
			}
		}
		if !strings.Contains(sentences[0].Text, c.outcome) {
			t.Errorf("%s: outcome %q", end, sentences[0].Text)
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
