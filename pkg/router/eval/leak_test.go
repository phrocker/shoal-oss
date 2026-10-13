// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package eval

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
)

var printGrandfather = flag.Bool("print-leak-grandfather", false,
	"log the Go literal of goLiteralsAtRegistration for the current Go findings (files, checks and hashes only)")

// moduleRoot is the module root, relative to this package.
const moduleRootRel = "../../.."

// auditRoots are the module trees the leak guard covers. Every non-test and
// test Go file under them (testdata included) is a cue source through its
// string literals: the router, its harness and training code (v1's
// trainModel is in eval_test.go), the lexicon, the shadow service and the
// wiring. Every non-Go file under them must be either under the v2 fixture
// tree, which is audited, or a frozen file pinned below.
var auditRoots = []string{"pkg/router", "pkg/lexicon", "internal/routershadow", "internal/routerwire"}

// v2FixturePrefix is the v2 fixture tree. The v2 run loads its world,
// grammars, lexicon, cases and model only from here.
const v2FixturePrefix = "pkg/router/testdata/eval/v2/"

// frozenNonGo are the other non-Go files under the audit roots, pinned at
// the pre-registration commit. A new or changed non-Go file outside the v2
// fixture tree fails TestNoUnauditedCueFiles. Markdown (documentation and
// generated reports, which nothing loads) is exempt.
var frozenNonGo = map[string]string{
	"pkg/lexicon/testdata/fuzz/FuzzTokenize/6d111b7ebac2773b":          "6d111b7ebac2773ba145dcd0bf8ac3e99cd674720954fdc199167c8c081888fc",
	"pkg/lexicon/testdata/fuzz/FuzzTokenize/be31f720118a5bed":          "be31f720118a5bed457b469f7cf4f78dfc4654385c9527ff3c0b8d69ca07211b",
	"pkg/lexicon/testdata/fuzz/FuzzTokenize/d28e6b09cd587733":          "d28e6b09cd5877335642079068c58fcce437323faa52e3da484b7e90585d170a",
	"pkg/lexicon/testdata/golden_bundle_id.txt":                        "4828548dc8d96136af94b134526c6e2c88c2ac3fe66e4de56d630fafb1121343",
	"pkg/router/testdata/eval/v1/cases.jsonl":                          "00ecebfada339788f703aab6bb364532452c7bfc61a3f0cab6e5c62256524bd6",
	"pkg/router/testdata/eval/v1/generate.py":                          "e31512f394aad4728afc346eae8ea0290b294af438895830eeb575454bcbc739",
	"pkg/router/testdata/eval/v1/grammars/breach_exposure.json":        "1a42e36feacf3c6187f74ee782aec179ed3a79e51edb77f2236a247a844e0b29",
	"pkg/router/testdata/eval/v1/grammars/depends_on_in.json":          "6f8bc7b604d65c9d3c4ae30f29818165bb67e10446858ae7bb591fd5bdfb2581",
	"pkg/router/testdata/eval/v1/grammars/depends_on_out.json":         "cb34c57534e045dabf3f9f17d44007e2c66e51e99f3f1b6a2679d9ae73f7b8ee",
	"pkg/router/testdata/eval/v1/grammars/deploy_readiness.json":       "80ab6ef7d3e1869ad4454451c2f30a9733e26ea81271674188e1eaf2ece366aa",
	"pkg/router/testdata/eval/v1/grammars/drain_environment.json":      "6969d67438a4f1ac289be8d121e82395304c286e015bc0634fa6406d6291d2dd",
	"pkg/router/testdata/eval/v1/grammars/flush_cache.json":            "967c51ea7a64933e3351bb4ebb433595e679daa0aef4fea6142d297d3a747a94",
	"pkg/router/testdata/eval/v1/grammars/open_incident.json":          "4c7250730dfb46b5d5fae53cbfbf4ebef6a8151ccade0370b62ced99e99fa004",
	"pkg/router/testdata/eval/v1/grammars/owned_by_in.json":            "051dfb67ca611a7e0e2f50f8bf2b25fb007b93ef9da74327da3f95d6bf529a05",
	"pkg/router/testdata/eval/v1/grammars/owned_by_out.json":           "25082419eb44f3117fa4b01cf446d6fceabca470a176e1ac71041a75b92a42b2",
	"pkg/router/testdata/eval/v1/grammars/page_team.json":              "8d3f817ccf3d34f5f819f6af3ed9520332f9601bf8ef7d2a88ad228829c8594b",
	"pkg/router/testdata/eval/v1/grammars/restart_service.json":        "14eeb7cd48ac0a856db329ade96e64e5ef705e4bf23ac11e6b157a7e62e778ee",
	"pkg/router/testdata/eval/v1/grammars/rollback.json":               "225b150fa54e278794d9957045f4d52897ced1c58a55d665693f66bd08907689",
	"pkg/router/testdata/eval/v1/grammars/rotate_credentials.json":     "fda2f9a12f5b526e5275860b1ace9dafe24fb9f454563f9ee72b7cd85a23d55d",
	"pkg/router/testdata/eval/v1/grammars/runs_in_in.json":             "51266a951ce4614ec23ae5328501ee473ed9fc5cf721a7146e0ced9dec4cc316",
	"pkg/router/testdata/eval/v1/grammars/runs_in_out.json":            "418791819f2332d980ed209e3b16f9d25299b6d5d2befe6aa05f458b60ef5950",
	"pkg/router/testdata/eval/v1/grammars/scale_service.json":          "025527a5bab53c35889780bd52e4ad40ab5867fb0d002e6d4e1c15895392d3b8",
	"pkg/router/testdata/eval/v1/grammars/service_operation_risk.json": "c90c1d76cb6ddcdb48ddf10cdddff9c2ebac9dde6f5c28243b3517ab0cb19a0e",
	"pkg/router/testdata/eval/v1/grammars/tail_logs.json":              "a16331db930707a71ce12dacf09cc77379f4791fe1ba8e6568e5cca3ba8d14a8",
	"pkg/router/testdata/eval/v1/world.json":                           "7684d173e8fe64f2513bc52e7880fccfad356eff5bbf999dfdc34b4957140748",
	"pkg/router/testdata/model/router-pair-v1.json":                    "f11fa3aca2f97d33cbd72d674d26b01fe4c4dc074bf6068de312bc63c1d82a08",
}

// goUnit is one grandfathered Go literal unit: a (file, check, unit hash)
// that the Go literals already held at the pre-registration commit and that
// the sealed split also uses outside the baseline corpus (function words in
// the router's stop-word and question-word lists, words of error and test
// messages). They predate the split, so they are not leaks. The hash is
// UnitHash, so the list names no test vocabulary in plain sight (a dictionary
// attack recovers single words: this is against casual reading, not a
// determined reader).
type goUnit struct{ File, Check, Hash string }

// grandfatherDigest pins goLiteralsAtRegistration: the SHA-256 over
// "<file>\t<check>\t<hash>\n" for each entry, sorted. The doc records it and
// the entry count, so the set can only change with the doc.
func grandfatherDigest(units []goUnit) string {
	lines := make([]string, 0, len(units))
	for _, u := range units {
		lines = append(lines, u.File+"\t"+u.Check+"\t"+u.Hash+"\n")
	}
	sort.Strings(lines)
	h := sha256.New()
	for _, l := range lines {
		h.Write([]byte(l))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// v2Auditor builds the leak auditor over the sealed split and the frozen
// baseline corpus.
func v2Auditor(t testing.TB) *LeakAuditor {
	t.Helper()
	world, v1 := load(t)
	var texts []string
	for _, c := range v1.all { // every v1 split: v1's test split is spent
		texts = append(texts, c.Text)
	}
	var sources []string
	for _, f := range baselineFiles(t) {
		if strings.HasSuffix(f, "cases.jsonl") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		strs, err := JSONStrings(b, "comment")
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		sources = append(sources, strs...)
	}
	return NewLeakAuditor(world.SurfaceForms(), texts, sources, sealedV2(t).all)
}

func numbered(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = prefix + " " + strconv.Itoa(i+1)
	}
	return out
}

// v2CueSources collects every cue source under a v2 fixture tree. Every
// file is a cue source except the sealed split, the frozen world (part of
// the baseline) and Markdown (documentation and reports, which nothing
// loads). Case files (cases.jsonl) are audited on their texts, with the
// phrase and template checks only: an independent train or dev author
// writing natural paraphrases would trip the word and bigram checks by
// chance. Files under external/ are general-purpose resources not written
// for this evaluation and get the phrase and template checks only too.
// Everything else (grammars, lexicon files, the model, recipes) gets every
// check. A file of any other kind is refused, so a new cue format cannot slip
// past the audit.
func v2CueSources(fsys fs.FS) ([]LeakSource, error) {
	var out []LeakSource
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		switch p {
		case "test.jsonl.gz", "world.json":
			return nil
		}
		b, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		src := LeakSource{Name: v2FixturePrefix + p, PhraseOnly: strings.HasPrefix(p, "external/")}
		ext := path.Ext(p)
		switch {
		case ext == ".md":
			return nil
		case path.Base(p) == "cases.jsonl":
			cs, err := LoadCases(fstest.MapFS{"cases.jsonl": {Data: b}})
			if err != nil {
				return fmt.Errorf("%s: %w", p, err)
			}
			for _, c := range cs.all {
				src.Strings = append(src.Strings, c.Text)
				src.Locations = append(src.Locations, "case "+c.ID)
			}
			src.PhraseOnly = true
		case ext == ".json" || ext == ".jsonl":
			src.Strings, err = JSONStrings(b)
			if err != nil {
				return fmt.Errorf("%s: %w", p, err)
			}
			src.Locations = numbered("string", len(src.Strings))
		case ext == ".txt" || ext == ".csv" || ext == ".tsv" || ext == ".py":
			src.Strings = strings.Split(string(b), "\n")
			src.Locations = numbered("line", len(src.Strings))
		default:
			return fmt.Errorf("%s: the leak audit cannot read this kind of file; teach v2CueSources to", p)
		}
		out = append(out, src)
		return nil
	})
	return out, err
}

// goLiteralSources are the string literals of every Go file under the audit
// roots, test files and testdata included, one source per file, named by
// its module-relative path.
func goLiteralSources(t testing.TB) []LeakSource {
	t.Helper()
	var out []LeakSource
	for _, root := range auditRoots {
		err := filepath.WalkDir(filepath.Join(moduleRootRel, root), func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") {
				return err
			}
			if filepath.Base(p) == "leak_grandfather_test.go" {
				return nil // the guard's own list: file paths and hashes
			}
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(moduleRootRel, p)
			if err != nil {
				return err
			}
			src := LeakSource{Name: filepath.ToSlash(rel)}
			ast.Inspect(f, func(n ast.Node) bool {
				if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if s, err := strconv.Unquote(lit.Value); err == nil {
						src.Strings = append(src.Strings, s)
						src.Locations = append(src.Locations, "line "+strconv.Itoa(fset.Position(lit.Pos()).Line))
					}
				}
				return true
			})
			out = append(out, src)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// TestNoTestPhrasingInCueSources is the leak guard: no cue source under the
// v2 fixtures, and no string literal of the Go code under the audit roots,
// carries a phrase, template, word or bigram that only the sealed test split
// holds. Grandfathered Go units pass; a grandfathered unit that no longer
// occurs is stale and fails, so the set only shrinks.
func TestNoTestPhrasingInCueSources(t *testing.T) {
	a := v2Auditor(t)
	sources, err := v2CueSources(os.DirFS(v2Dir))
	if err != nil {
		t.Fatal(err)
	}
	goSources := goLiteralSources(t)
	goSource := map[string]bool{}
	for _, src := range goSources {
		goSource[src.Name] = true
	}
	sources = append(sources, goSources...)
	grandfathered := map[goUnit]bool{}
	for _, u := range goLiteralsAtRegistration {
		grandfathered[u] = true
	}
	matched := map[goUnit]bool{}
	var current []goUnit
	for _, f := range a.Audit(sources) {
		if goSource[f.Source] {
			u := goUnit{f.Source, f.Check, f.Hash}
			current = append(current, u)
			if grandfathered[u] {
				matched[u] = true
				continue
			}
		}
		t.Errorf("leak: %s", f)
	}
	for _, u := range goLiteralsAtRegistration {
		if !matched[u] {
			t.Errorf("stale grandfathered unit: %s %s sha256:%s; remove it and update the doc's digest", u.File, u.Check, u.Hash)
		}
	}
	if *printGrandfather {
		var b strings.Builder
		for _, u := range current {
			fmt.Fprintf(&b, "\t{%q, %q, %q},\n", u.File, u.Check, u.Hash)
		}
		t.Logf("%d Go units, digest %s:\n%s", len(current), grandfatherDigest(current), b.String())
	}
	fresh, total := a.FreshTemplates()
	t.Logf("audited %d sources; %d test-only word types, %d test-only bigrams; %d of %d test texts have a template the baseline lacks",
		len(sources), a.TestOnlyWords(), a.TestOnlyBigrams(), fresh, total)
}

// TestNoUnauditedCueFiles fails closed on files the leak guard cannot see:
// every non-Go, non-Markdown file under the audit roots is under the v2
// fixture tree (audited by v2CueSources, which refuses unknown kinds) or a
// frozen file at its pinned digest. A pinned file that is gone is stale.
func TestNoUnauditedCueFiles(t *testing.T) {
	seen := map[string]bool{}
	for _, root := range auditRoots {
		err := filepath.WalkDir(filepath.Join(moduleRootRel, root), func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || strings.HasSuffix(p, ".go") || strings.HasSuffix(p, ".md") {
				return err
			}
			rel, err := filepath.Rel(moduleRootRel, p)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			if strings.HasPrefix(rel, v2FixturePrefix) {
				return nil
			}
			seen[rel] = true
			want, ok := frozenNonGo[rel]
			if !ok {
				t.Errorf("%s: a non-Go file the leak guard does not audit; put cue data under %s", rel, v2FixturePrefix)
				return nil
			}
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			if got := sha(b); got != want {
				t.Errorf("%s: frozen file changed (SHA-256 %s)", rel, got)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for rel := range frozenNonGo {
		if !seen[rel] {
			t.Errorf("%s: pinned frozen file is gone; remove the entry", rel)
		}
	}
}

// TestV2BaselineIsClean: the frozen baseline itself (v1's grammars and the
// world) leaks nothing by construction, so the audit has no false positive
// on it.
func TestV2BaselineIsClean(t *testing.T) {
	a := v2Auditor(t)
	var sources []LeakSource
	for _, f := range baselineFiles(t) {
		if strings.HasSuffix(f, "cases.jsonl") {
			continue // its texts are the baseline texts themselves
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		strs, err := JSONStrings(b, "comment")
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, LeakSource{Name: f, Strings: strs})
	}
	if findings := a.Audit(sources); len(findings) != 0 {
		t.Fatalf("the baseline corpus is reported as leaking: %v", findings)
	}
}

// TestV2TestIsFresh: every sealed text with a token is new against all 292
// v1 cases, verbatim and with its mentions abstracted.
func TestV2TestIsFresh(t *testing.T) {
	a := v2Auditor(t)
	_, v1 := load(t)
	old := map[string]bool{}
	for _, c := range v1.all {
		old[strings.Join(words(c.Text), " ")] = true
	}
	for _, c := range a.test {
		if len(c.raw) == 0 {
			continue // the empty-text cases: no phrasing to repeat
		}
		if old[strings.Join(c.raw, " ")] {
			t.Errorf("case %s repeats a v1 text", c.id)
		}
		if a.baselineTemplate[strings.Join(c.abstract, " ")] {
			t.Errorf("case %s repeats a v1 phrasing with other entities", c.id)
		}
	}
}

// TestLeakGuardCatchesLeaks is the guard's mutation check. A grammar or
// lexicon file that carries a sealed phrasing, verbatim, as a pattern, as a
// test-only cue word or as a test-only word pair, fails the audit through the
// same collector the guard uses; so does a train case that copies a test
// text; a file the audit cannot read fails it too; no finding prints what was
// planted; and a train case with a test-only word, or a file that copies a v1
// grammar, passes.
func TestLeakGuardCatchesLeaks(t *testing.T) {
	a := v2Auditor(t)
	// Pick sealed units without printing them.
	var phrase, template auditText
	for _, c := range a.test {
		if phrase.id == "" && len(c.raw) >= 3 && !c.rawKnown {
			phrase = c
		}
		if template.id == "" && len(c.abstract) >= 3 && countLiteral(c.abstract) >= 2 && countLiteral(c.abstract) < len(c.abstract) &&
			!c.abstractKnown {
			template = c
		}
	}
	var testWords []string
	for w := range a.testOnlyWords {
		testWords = append(testWords, w)
	}
	sort.Strings(testWords)
	var testPairs [][2]string
	for b := range a.testOnlyBigrams {
		if a.baselineWords[b[0]] && a.baselineWords[b[1]] {
			testPairs = append(testPairs, b) // a pair of baseline words: only the bigram check sees it
		}
	}
	sort.Slice(testPairs, func(i, j int) bool { return testPairs[i][0]+" "+testPairs[i][1] < testPairs[j][0]+" "+testPairs[j][1] })
	if phrase.id == "" || template.id == "" || len(testWords) == 0 || len(testPairs) == 0 {
		t.Fatal("the sealed split offers no unit to plant")
	}
	phraseText := strings.Join(phrase.raw, " ")
	pattern := strings.ReplaceAll(strings.Join(template.abstract, " "), slotToken, "{service}")
	pair := testPairs[0]
	v1Grammar, err := os.ReadFile(filepath.Join(fixtureDir, "grammars", "restart_service.json"))
	if err != nil {
		t.Fatal(err)
	}
	grammar := func(cues []string, patterns []string) []byte {
		q := func(ss []string) string {
			var out []string
			for _, s := range ss {
				out = append(out, strconv.Quote(s))
			}
			return "[" + strings.Join(out, ",") + "]"
		}
		return []byte(`{"schema":"shoal.router.grammar/v1","target":{"kind":"action","capability":"ops","action":"restart_service"},` +
			`"cues":` + q(cues) + `,"patterns":` + q(patterns) + `}`)
	}
	trainCase := func(text string) []byte {
		return []byte(`{"id":"x","caller":"alice","split":"train","tags":[],"expected":{"kind":"abstain","reason":"no_target"},"text":` +
			strconv.Quote(text) + `}` + "\n")
	}
	planted := []string{phraseText, pattern, testWords[0], testWords[len(testWords)-1], pair[0] + " " + pair[1]}
	for _, tc := range []struct {
		name  string
		files fstest.MapFS
		check string
	}{
		{"verbatim text in a lexicon file", fstest.MapFS{"lexicon/synonyms.json": {Data: []byte(`{"phrases":[` + strconv.Quote(phraseText) + `]}`)}}, "phrase"},
		{"verbatim text in an external resource", fstest.MapFS{"external/thesaurus.txt": {Data: []byte("x\n" + phraseText + "\n")}}, "phrase"},
		{"verbatim text in a train case", fstest.MapFS{"cases.jsonl": {Data: trainCase(phraseText)}}, "phrase"},
		{"test template with other entities in a train case", fstest.MapFS{"cases.jsonl": {Data: trainCase(
			strings.ReplaceAll(strings.Join(template.abstract, " "), slotToken, "billing"))}}, "template"},
		{"test template as a pattern", fstest.MapFS{"grammars/g.json": {Data: grammar(nil, []string{"[please] " + pattern + " [now]"})}}, "template"},
		{"test-only cue word", fstest.MapFS{"grammars/g.json": {Data: grammar([]string{testWords[0]}, nil)}}, "word"},
		{"test-only cue word as a synonym key", fstest.MapFS{"lexicon/syn.json": {Data: []byte(`{` + strconv.Quote(testWords[len(testWords)-1]) + `:"restart"}`)}}, "word"},
		{"test-only word in the model", fstest.MapFS{"model/router-pair-v2.json": {Data: []byte(`{"features":[` + strconv.Quote(testWords[0]) + `]}`)}}, "word"},
		{"test-only word pair", fstest.MapFS{"grammars/g.json": {Data: grammar(nil, []string{pair[0] + " " + pair[1]})}}, "bigram"},
	} {
		sources, err := v2CueSources(tc.files)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		found := false
		for _, f := range a.Audit(sources) {
			found = found || f.Check == tc.check
			for _, unit := range planted {
				if strings.Contains(f.String(), unit) {
					t.Errorf("%s: a finding prints a planted unit", tc.name)
				}
			}
		}
		if !found {
			t.Errorf("%s: the audit found no %s leak", tc.name, tc.check)
		}
	}
	// Train and dev case texts and external resources are exempt from the
	// word and bigram checks.
	for name, files := range map[string]fstest.MapFS{
		"train case":         {"cases.jsonl": {Data: trainCase(testWords[0] + " " + pair[0] + " " + pair[1])}},
		"external resources": {"external/words.txt": {Data: []byte(strings.Join(testWords, "\n"))}},
	} {
		sources, err := v2CueSources(files)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range a.Audit(sources) {
			if f.Check == "word" || f.Check == "bigram" {
				t.Errorf("%s got a %s finding", name, f.Check)
			}
		}
	}
	// Unreadable kinds fail closed.
	if _, err := v2CueSources(fstest.MapFS{"lexicon/vectors.bin": {Data: []byte{0}}}); err == nil {
		t.Error("an unreadable cue file was accepted")
	}
	// A copy of a v1 grammar, the sealed split, the world and Markdown are clean.
	sources, err := v2CueSources(fstest.MapFS{
		"grammars/restart_service.json": {Data: v1Grammar},
		"test.jsonl.gz":                 {Data: []byte{0x1f, 0x8b}},
		"world.json":                    {Data: []byte(`{}`)},
		"README.md":                     {Data: []byte(phraseText)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if findings := a.Audit(sources); len(findings) != 0 {
		t.Errorf("a v1 grammar copy is reported as leaking: %v", findings)
	}
	// A Go literal that plants a test-only word is a finding the
	// grandfathered set does not cover.
	probe := []LeakSource{{Name: "pkg/router/features.go", Strings: []string{testWords[0]}}}
	grandfathered := map[goUnit]bool{}
	for _, u := range goLiteralsAtRegistration {
		grandfathered[u] = true
	}
	for _, f := range a.Audit(probe) {
		if grandfathered[goUnit{f.Source, f.Check, f.Hash}] {
			continue
		}
		return
	}
	t.Error("a Go literal with a test-only word is not a finding")
}

// TestExpandOptional pins the pattern expansion the audit relies on.
func TestExpandOptional(t *testing.T) {
	got := expandOptional("[please] bounce {service} [on {environment}]")
	var norm []string
	for _, s := range got {
		norm = append(norm, strings.Join(strings.Fields(s), " "))
	}
	sort.Strings(norm)
	want := []string{"bounce {service}", "bounce {service} on {environment}", "please bounce {service}", "please bounce {service} on {environment}"}
	if strings.Join(norm, "|") != strings.Join(want, "|") {
		t.Fatalf("expandOptional = %q", norm)
	}
	if got := expandOptional("a [b [c] d] e"); len(got) != 3 {
		t.Fatalf("nested expansion = %q", got)
	}
	if got := expandOptional("a ]b[ c"); len(got) != 1 || strings.ContainsAny(got[0], "[]") {
		t.Fatalf("unbalanced expansion = %q", got)
	}
}
