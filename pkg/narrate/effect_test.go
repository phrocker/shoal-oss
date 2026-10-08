// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package narrate

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// EffectPossible on a record, as #538 defines it and the renderer reads it:
//
//   - claimed, set: an effect may already have happened (unchanged);
//   - succeeded or failed, clear: the action declared no external or egress
//     effect, so it could not have had an external effect and its whole
//     outcome is in the record. No record written before #538 has this shape:
//     ActionRecord.Validate required the flag on both states;
//   - any terminal state, set: an effect may have occurred. Never that it
//     did: the flag is monotonic, so even a request that never left reads
//     set;
//   - canceled, clear: nothing is said. The record may never have been
//     claimed, and a clear flag there says only that no claim set it.
//
// Only the flag decides. A declaration on the record (an admission's admitted
// effects) is never read for this, in either direction.

const (
	effectNoneKey = "dispatch.effect.none"
	effectMayKey  = "dispatch.gap.effect_may"
	effectOpenKey = "dispatch.gap.effect_possible"
)

// effectKeys returns which effect sentences a rendering carries.
func effectKeys(sentences []Sentence) []string {
	var keys []string
	for _, s := range sentences {
		switch s.Key {
		case effectNoneKey, effectMayKey, effectOpenKey:
			keys = append(keys, s.Key)
		}
	}
	sort.Strings(keys)
	return keys
}

func TestEffectPossibleOnEveryState(t *testing.T) {
	r := New(nil)
	for _, state := range sourceDispatchStates(t) {
		for _, kind := range []string{"action", "admission"} {
			build := action
			if kind == "admission" {
				if state == string(fleet.DispatchQueued) {
					continue // an admission is never queued
				}
				build = admission
			}
			for _, flag := range []bool{true, false} {
				what := fmt.Sprintf("%s %s effect_possible=%v", kind, state, flag)
				record := build(fleet.DispatchState(state))
				record.EffectPossible = flag
				sentences, err := r.Action(record, Options{})
				if err != nil {
					t.Fatal(err)
				}
				var want []string
				switch fleet.DispatchState(state) {
				case fleet.DispatchClaimed:
					if flag {
						want = []string{effectOpenKey}
					}
				case fleet.DispatchSucceeded, fleet.DispatchFailed:
					want = []string{effectNoneKey}
					if flag {
						want = []string{effectMayKey}
					}
				case fleet.DispatchCanceled:
					if flag {
						want = []string{effectMayKey}
					}
				}
				if got := effectKeys(sentences); strings.Join(got, ",") != strings.Join(want, ",") {
					t.Errorf("%s: effect sentences %v, want %v", what, got, want)
				}
				for _, s := range sentences {
					text := strings.ToLower(s.Text)
					if flag && strings.Contains(text, "could not have had") {
						t.Errorf("%s: a set flag is rendered as no possible effect: %s", what, s.Text)
					}
					if s.Key == effectMayKey && !strings.Contains(text, "may have occurred") {
						t.Errorf("%s: a possible effect is not hedged: %s", what, s.Text)
					}
					if s.Key == effectNoneKey && !strings.Contains(text, "could not have had an external effect") {
						t.Errorf("%s: no possible effect is not said: %s", what, s.Text)
					}
				}
			}
		}
	}
}

// TestEffectIsNeverInferredFromDeclarations: only the flag can say no effect
// was possible. A record whose declared effects include neither external
// mutation nor egress, with the flag set, still reads "may" — a pre-#538
// record of a non-declaring action looks exactly like this, and so does any
// record whose declaration the renderer cannot see (a dispatched action's
// record does not carry its action's effects at all).
func TestEffectIsNeverInferredFromDeclarations(t *testing.T) {
	r := New(nil)
	declarations := map[string]fleet.Effects{
		"none recorded":     nil,
		"reads-corpus only": {fleet.EffectReadsCorpus},
	}
	for name, effects := range declarations {
		for _, state := range []fleet.DispatchState{fleet.DispatchSucceeded, fleet.DispatchFailed} {
			record := action(state)
			record.AdmittedEffects = effects
			record.EffectPossible = true
			what := fmt.Sprintf("%s declared=%s", state, name)
			sentences, err := r.Action(record, Options{})
			if err != nil {
				t.Fatal(err)
			}
			if got := effectKeys(sentences); strings.Join(got, ",") != effectMayKey {
				t.Errorf("%s: effect sentences %v, want only %s", what, got, effectMayKey)
			}
			for _, s := range sentences {
				text := strings.ToLower(s.Text)
				if strings.Contains(text, "could not have had") || strings.Contains(text, "no reconciliation") {
					t.Errorf("%s: no effect inferred from the declaration: %s", what, s.Text)
				}
			}
		}
	}
}

// effectVerb finds a clause that says an effect, a request or content did
// something outside Shoal. hedge finds what makes such a clause not an
// assertion that it happened.
var (
	effectVerb = regexp.MustCompile(`\b(occurred|happened|took place|took effect|was (sent|written|transmitted|applied|performed|delivered|egressed)|were (sent|written|transmitted|applied|performed)|been (sent|written|transmitted|applied|performed)|reached|left the host|egressed)\b`)
	hedge      = regexp.MustCompile(`\b(may|might|whether|if|no|nothing|not|never|could not|cannot)\b`)
	clauseEnd  = regexp.MustCompile(`[.;:,—]`)
)

// affirmsEffect returns the clause of text that says an effect happened, if
// any.
func affirmsEffect(text string) string {
	for _, clause := range clauseEnd.Split(strings.ToLower(text), -1) {
		if effectVerb.MatchString(clause) && !hedge.MatchString(clause) {
			return strings.TrimSpace(clause)
		}
	}
	return ""
}

func TestEffectScannerCatchesAffirmations(t *testing.T) {
	for _, text := range []string{
		"An external effect occurred; reconcile with the target.",
		"The effect happened at the target.",
		"The request was sent, so reconcile with the target.",
		"Content left the host.",
	} {
		if affirmsEffect(text) == "" {
			t.Errorf("not caught: %q", text)
		}
	}
	for _, text := range []string{
		"An external effect may have occurred; reconcile with the target.",
		"This action could not have had an external effect; its whole outcome is in this record.",
		"which, if accurate, means no byte of the request reached a connection.",
		"nothing establishes whether the effect happened.",
	} {
		if clause := affirmsEffect(text); clause != "" {
			t.Errorf("hedged text flagged: %q (%q)", text, clause)
		}
	}
}

// TestNoSentenceSaysAnEffectHappened scans every sentence the value goldens,
// the scenario goldens and every (state × EffectPossible) combination render.
// Nothing Shoal records establishes that an external effect happened — a set
// flag says only that one may have — so no sentence may say it did.
func TestNoSentenceSaysAnEffectHappened(t *testing.T) {
	r := New(nil)
	scanned := 0
	scan := func(what string, sentences []Sentence, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		for _, s := range sentences {
			scanned++
			if clause := affirmsEffect(s.Text); clause != "" {
				t.Errorf("%s: %s says an effect happened (%q): %s", what, s.Key, clause, s.Text)
			}
		}
	}
	for _, family := range valueFamilies(t) {
		for _, c := range family.cases {
			sentences, err := c.run(r)
			scan(family.name+": "+c.name, sentences, err)
		}
	}
	for name, run := range goldenScenarios() {
		sentences, err := run(t, r)
		scan(name, sentences, err)
	}
	for _, state := range DispatchStates {
		for _, build := range []func(fleet.DispatchState) fleet.ActionRecord{action, admission} {
			for _, flag := range []bool{true, false} {
				for _, now := range times() {
					record := build(state)
					record.EffectPossible = flag
					sentences, err := r.Action(record, Options{Now: now, QuoteInput: true, QuoteOutput: true})
					scan(fmt.Sprintf("%s admission=%v effect_possible=%v", state, isAdmission(record), flag), sentences, err)
				}
			}
		}
	}
	if scanned < 1000 {
		t.Fatalf("only %d sentences scanned", scanned)
	}
}
