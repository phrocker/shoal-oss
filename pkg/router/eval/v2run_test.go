// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package eval

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/internal/routerwire"
	"github.com/phrocker/shoal-oss/pkg/router"
)

// The v2 run harness (docs/router-evaluation-v2.md, "Systems compared" and
// "Success criteria"). It is committed with the seal, so the run's pull
// request only flips V2TestRunOpen, writes the report with -update and
// appends the results. Until then TestEvaluationV2Test skips.

const (
	// v2ModelPath is where v2's model goes. The v2 system loads its world,
	// grammars and model only from the v2 fixture tree.
	v2ModelPath  = v2Dir + "/model/router-pair-v2.json"
	v2ReportPath = v2Dir + "/report-test.md"

	// v2G2Min is G2's threshold: correct abstention on the 112 unanswerable
	// cases of the sealed split.
	v2G2Min          = 105
	v2Unanswerable   = 112
	v2CoverageTarget = 96 // S2: half of the 191 answerable cases, rounded up
	v2Alpha          = 0.05
)

// v2System is one system of the run: a world (with its grammars) and a
// model. Its runner yields the router's proposals and, over the same
// analysis, the lexical baseline's.
type v2System struct {
	name  string
	world *World
	model []byte
}

func (s v2System) run(t testing.TB, cases []Case) []Outcome {
	t.Helper()
	provider, err := NewProvider(s.model)
	if err != nil {
		t.Fatalf("%s: %v", s.name, err)
	}
	outcomes, err := NewRunner(s.world, routerwire.Predictor(provider, ReleaseID), routerwire.Validator()).Run(context.Background(), cases)
	if err != nil {
		t.Fatalf("%s: %v", s.name, err)
	}
	for _, o := range outcomes {
		if err := o.Router.Validate(); err != nil {
			t.Fatalf("%s: case %s: invalid proposal: %v", s.name, o.Case.ID, err)
		}
		if err := o.Baseline.Validate(); err != nil {
			t.Fatalf("%s: case %s: invalid baseline proposal: %v", s.name, o.Case.ID, err)
		}
	}
	return outcomes
}

// pair compares two systems' end-to-end correctness case by case.
func pair(w *World, a, b []Outcome, pickA, pickB func(Outcome) router.Proposal) Comparison {
	paired := make([]Outcome, len(a))
	for i := range a {
		paired[i] = Outcome{Case: a[i].Case, Router: pickA(a[i]), Baseline: pickB(b[i])}
	}
	return w.Compare(paired)
}

// pairCoverage compares two systems' coverage on the answerable cases:
// first-only is a case only the first covers.
func pairCoverage(a, b []Outcome) Comparison {
	var c Comparison
	for i := range a {
		if !a[i].Case.Answerable() {
			continue
		}
		x, y := a[i].Router.Kind != router.KindAbstain, b[i].Router.Kind != router.KindAbstain
		switch {
		case x && y:
			c.BothRight++
		case x:
			c.RouterOnly++
		case y:
			c.BaselineOnly++
		default:
			c.BothWrong++
		}
	}
	c.ExactP = exactMcNemar(c.RouterOnly, c.BaselineOnly)
	return c
}

// v2Verdict is the pre-registered decision.
type v2Verdict struct {
	G1, G2     bool
	Primary    string // positive, negative or inconclusive
	H2         string // supported, contradicted or inconclusive
	S1, S2, S3 bool
}

// judgeV2 applies the success criteria. r2 is router v2's report on the
// sealed split; primary is router v2 against the lexical baseline,
// versusV1 router v2 against frozen v1 (end-to-end), coverage the same pair
// on coverage.
func judgeV2(r2 Report, primary, versusV1, coverage Comparison) v2Verdict {
	var v v2Verdict
	// G1: wrong proposals at most 5% of proposals, rounded down, so below
	// 20 proposals no wrong one is allowed. With no proposal G1 fails: there
	// is no precision to guard.
	proposals := r2.TargetPrecision.D
	wrong := proposals - r2.TargetPrecision.N
	v.G1 = proposals > 0 && wrong <= int(math.Floor(0.05*float64(proposals)))
	v.G2 = r2.CorrectAbstention.D == v2Unanswerable && r2.CorrectAbstention.N >= v2G2Min
	wins := func(c Comparison) bool { return c.RouterOnly > c.BaselineOnly && c.ExactP < v2Alpha }
	loses := func(c Comparison) bool { return c.BaselineOnly > c.RouterOnly && c.ExactP < v2Alpha }
	switch {
	case loses(primary) || !v.G1 || !v.G2:
		v.Primary = "negative"
	case wins(primary):
		v.Primary = "positive"
	default:
		v.Primary = "inconclusive"
	}
	switch {
	case loses(coverage) || !v.G1:
		v.H2 = "contradicted"
	case wins(coverage):
		v.H2 = "supported"
	default:
		v.H2 = "inconclusive"
	}
	v.S1 = wins(versusV1)
	v.S2 = r2.Coverage.N >= v2CoverageTarget
	v.S3 = r2.SlotExact.D > 0 && r2.SlotExact.N >= int(math.Ceil(0.9*float64(r2.SlotExact.D)))
	return v
}

func comparisonLine(name, first, second string, c Comparison) string {
	return fmt.Sprintf("%s: %s only %d, %s only %d; both %d, neither %d; exact McNemar p = %.3g.\n",
		name, first, c.RouterOnly, second, c.BaselineOnly, c.BothRight, c.BothWrong, c.ExactP)
}

// v2Evaluate runs both systems over cases and renders the report: the four
// systems' metrics, the paired comparisons and the verdict. It prints no
// case text.
func v2Evaluate(t testing.TB, split string, cases []Case, v2, v1 v2System) (string, v2Verdict) {
	t.Helper()
	o2, o1 := v2.run(t, cases), v1.run(t, cases)
	for i := range o2 {
		if o2[i].Case.ID != o1[i].Case.ID {
			t.Fatalf("outcomes are not paired at %d", i)
		}
	}
	routerOf := func(o Outcome) router.Proposal { return o.Router }
	baselineOf := func(o Outcome) router.Proposal { return o.Baseline }
	r2 := v2.world.Measure(split, "router v2", o2, routerOf)
	reports := []Report{
		r2,
		v2.world.Measure(split, "baseline (v2 grammars)", o2, baselineOf),
		v1.world.Measure(split, "frozen v1", o1, routerOf),
		v1.world.Measure(split, "baseline (v1 grammars)", o1, baselineOf),
	}
	primary := v2.world.Compare(o2)
	versusV1 := pair(v2.world, o2, o1, routerOf, routerOf)
	coverage := pairCoverage(o2, o1)
	v := judgeV2(r2, primary, versusV1, coverage)
	var b strings.Builder
	b.WriteString(Markdown(reports, primary))
	b.WriteString("\n")
	b.WriteString(comparisonLine("Router v2 against frozen v1 (end-to-end)", "router v2", "frozen v1", versusV1))
	b.WriteString(comparisonLine("Router v2 against frozen v1 (coverage, answerable cases)", "router v2", "frozen v1", coverage))
	b.WriteString(comparisonLine("Lexical baseline, v2 grammars against v1 grammars (end-to-end)", "v2 grammars", "v1 grammars",
		pair(v2.world, o2, o1, baselineOf, baselineOf)))
	fmt.Fprintf(&b, "\nG1 (wrong proposals at most 5%% of %d, rounded down): %t. G2 (correct abstention at least %d of %d): %t.\n",
		r2.TargetPrecision.D, v.G1, v2G2Min, v2Unanswerable, v.G2)
	fmt.Fprintf(&b, "Primary outcome: %s. H2: %s. S1 (beats frozen v1): %t. S2 (coverage at least %d): %t. S3 (slot exact-match at least 90%%): %t.\n",
		v.Primary, v.H2, v.S1, v2CoverageTarget, v.S2, v.S3)
	return b.String(), v
}

// TestEvaluationV2Test is the pre-registered v2 run. It skips while the
// split is sealed; the run's pull request flips V2TestRunOpen and writes the
// report with -update.
func TestEvaluationV2Test(t *testing.T) {
	cases, err := sealedV2(t).PreRegisteredV2()
	if errors.Is(err, ErrV2Sealed) {
		t.Skip("the v2 test split is sealed until its pre-registered run")
	}
	if err != nil {
		t.Fatal(err)
	}
	v2World, err := LoadWorld(os.DirFS(v2Dir))
	if err != nil {
		t.Fatal(err)
	}
	v2Model, err := os.ReadFile(v2ModelPath)
	if err != nil {
		t.Fatal(err)
	}
	v1World, _ := load(t)
	v1Model, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatal(err)
	}
	text, _ := v2Evaluate(t, "test", cases, v2System{"router v2", v2World, v2Model}, v2System{"frozen v1", v1World, v1Model})
	t.Log("\n" + text)
	if *update {
		if err := os.WriteFile(v2ReportPath, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := os.ReadFile(v2ReportPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored) != text {
		t.Fatalf("%s differs from this run; the run is reported once", v2ReportPath)
	}
}

// TestV2Readiness loads every v2 artefact the run will load, the way the run
// loads it (the world and grammars/*.json through LoadWorld on the v2
// fixture tree, and model/router-pair-v2.json), and runs the harness on v2's
// dev split. The run is then never the first time v2's artefacts load.
//
// PLACEHOLDERS: until v2's model and train/dev cases are committed, v1's
// model and v1's dev split stand in for them, and the test logs that it used
// them. v2's grammars are whatever is under the v2 fixture tree (none at the
// seal). Loaders for lexicon or recipe files, which no code reads yet, land
// in amendments before the run, and this test then loads them too.
func TestV2Readiness(t *testing.T) {
	v2World, err := LoadWorld(os.DirFS(v2Dir))
	if err != nil {
		t.Fatalf("v2 world and grammars: %v", err)
	}
	v1World, v1Cases := load(t)
	v1Model, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatal(err)
	}
	var placeholders []string
	v2Model, err := os.ReadFile(v2ModelPath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		v2Model = v1Model
		placeholders = append(placeholders, "v1's model for "+v2ModelPath)
	case err != nil:
		t.Fatal(err)
	}
	var dev []Case
	v2Cases, err := LoadCases(os.DirFS(v2Dir))
	switch {
	case errors.Is(err, os.ErrNotExist):
		if dev, err = v1Cases.Split("dev"); err != nil {
			t.Fatal(err)
		}
		placeholders = append(placeholders, "v1's dev split for "+v2Dir+"/cases.jsonl")
	case err != nil:
		t.Fatalf("v2 cases: %v", err)
	default:
		for _, c := range v2Cases.all {
			if c.Split == "test" {
				t.Fatalf("v2 cases.jsonl holds case %s of the test split; the test split is sealed apart", c.ID)
			}
		}
		if dev, err = v2Cases.Split("dev"); err != nil {
			t.Fatal(err)
		}
	}
	if len(dev) == 0 {
		t.Fatal("no dev cases to run")
	}
	text, _ := v2Evaluate(t, "dev", dev, v2System{"router v2", v2World, v2Model}, v2System{"frozen v1", v1World, v1Model})
	if !strings.Contains(text, "Primary outcome: ") {
		t.Fatal("the harness rendered no verdict")
	}
	for _, p := range placeholders {
		t.Logf("PLACEHOLDER in use: %s", p)
	}
}

// TestV2RunHarness exercises the harness end to end on v1's dev split (not
// the sealed split), with v1's world and model standing in for both
// systems: the two are identical, so every pairing is concordant.
func TestV2RunHarness(t *testing.T) {
	w, cs := load(t)
	dev, err := cs.Split("dev")
	if err != nil {
		t.Fatal(err)
	}
	model, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatal(err)
	}
	text, _ := v2Evaluate(t, "dev", dev, v2System{"router v2", w, model}, v2System{"frozen v1", w, model})
	for _, want := range []string{
		"router v2 (dev)", "baseline (v2 grammars) (dev)", "frozen v1 (dev)", "baseline (v1 grammars) (dev)",
		"Router v2 against frozen v1 (end-to-end): router v2 only 0, frozen v1 only 0;",
		"Router v2 against frozen v1 (coverage, answerable cases): router v2 only 0, frozen v1 only 0;",
		"Primary outcome: ",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("harness report lacks %q", want)
		}
	}
	for _, c := range dev {
		if c.Text != "" && strings.Contains(text, c.Text) {
			t.Fatalf("the harness report prints case %s's text", c.ID)
		}
	}
}

// TestJudgeV2 pins the decision rules, including G1 with no proposal.
func TestJudgeV2(t *testing.T) {
	report := func(precisionN, precisionD, abstainN, coverage, slotN, slotD int) Report {
		return Report{
			TargetPrecision:   Counts{N: precisionN, D: precisionD},
			CorrectAbstention: Counts{N: abstainN, D: v2Unanswerable},
			Coverage:          Counts{N: coverage, D: 191},
			SlotExact:         Counts{N: slotN, D: slotD},
		}
	}
	win := Comparison{RouterOnly: 20, BaselineOnly: 5, ExactP: exactMcNemar(20, 5)}
	lose := Comparison{RouterOnly: 5, BaselineOnly: 20, ExactP: exactMcNemar(5, 20)}
	tie := Comparison{RouterOnly: 8, BaselineOnly: 6, ExactP: exactMcNemar(8, 6)}
	for _, tc := range []struct {
		name                       string
		r                          Report
		primary, versus, coverage  Comparison
		wantPrimary, wantH2        string
		wantG1, wantG2, s1, s2, s3 bool
	}{
		{"clean win", report(120, 122, 110, 120, 110, 120), win, win, win, "positive", "supported", true, true, true, true, true},
		{"win but G1 fails", report(100, 120, 110, 120, 100, 100), win, win, win, "negative", "contradicted", false, true, true, true, true},
		{"win but G2 fails", report(120, 120, 104, 120, 120, 120), win, win, win, "negative", "supported", true, false, true, true, true},
		{"no proposals", report(0, 0, 112, 0, 0, 0), tie, tie, lose, "negative", "contradicted", false, true, false, false, false},
		{"loss", report(120, 120, 112, 120, 120, 120), lose, tie, tie, "negative", "inconclusive", true, true, false, true, true},
		{"tie", report(60, 61, 112, 61, 50, 60), tie, tie, tie, "inconclusive", "inconclusive", true, true, false, false, false},
		{"G1 at the bound", report(95, 100, 105, 100, 90, 100), tie, tie, tie, "inconclusive", "inconclusive", true, true, false, true, true},
		{"one wrong of one", report(0, 1, 112, 1, 0, 0), win, tie, win, "negative", "contradicted", false, true, false, false, false},
		{"G1 just over the bound", report(94, 100, 112, 100, 90, 100), win, tie, tie, "negative", "contradicted", false, true, false, true, true},
	} {
		v := judgeV2(tc.r, tc.primary, tc.versus, tc.coverage)
		if v.Primary != tc.wantPrimary || v.H2 != tc.wantH2 || v.G1 != tc.wantG1 || v.G2 != tc.wantG2 ||
			v.S1 != tc.s1 || v.S2 != tc.s2 || v.S3 != tc.s3 {
			t.Errorf("%s: %+v", tc.name, v)
		}
	}
}
