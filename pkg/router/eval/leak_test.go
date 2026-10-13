// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package eval

import (
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

// goCueRoots are the Go trees whose string literals are cue sources: the
// router (its grammars' code, features, baseline and harness, which could
// synthesize cues) and the lexicon. Test files and testdata are not code the
// router runs.
var goCueRoots = []string{"..", "../../lexicon"}

// goLiteralsAtRegistration are the word and bigram units that the router
// and lexicon Go literals already held at the pre-registration commit and
// that the sealed split also uses, outside the baseline corpus: function
// words in the router's stop-word and question-word lists, and words of
// error messages. They predate the split, so they are not leaks. Each entry
// is the SHA-256 of "<check>:<token>", so this file does not list test
// vocabulary in plain sight (a dictionary attack recovers them: this is
// against casual reading, not a determined reader). The set only shrinks: a
// new Go literal unit that the split uses is a finding.
var goLiteralsAtRegistration = map[string]bool{
	"3c1ce7d4e521ea5b79db9e9aef712422364d3a35df5753d9304500418820e353": true,
	"d73e0cc213665dcf07fab04c799e99b25a7a7800f4ae4af1192e85aaf8a56c40": true,
	"51a34b1eba01e634891a84d2882fceb837b77084fe77ae822eed619cbe196125": true,
	"24b979fe6a36a4ad43ad709271868ac5147c2d0add917387aa77c20dd6ee97c4": true,
	"5c63c5ad5b14aad6027b580fdb2c5313c9e202260e117bed82a89e67cb68d523": true,
	"eb978946fb4196060b1f99844a059a45ec8f35f09e70aec6956cb9d746df66f8": true,
	"9c3fdb26116d0737cd18f4dbf2a91a25a1940dfb17616010e35e89da360cf17c": true,
	"95b5b1b44d78d94cf6624ad94c2807dd7b52fc6ef283968e1adc7ce5fc3ae49e": true,
	"2cf8976efa6528eed57fc30e832c5409b0bf03f449a0e625faa950dadd164d1d": true,
	"6c33baa2202ebedd04c98d4f733413921328bedffbf13a05f5b967a247177ea2": true,
	"53a996abdfc2ff88979a7f92d6020ab6306629dd7d752270748f1c9e33a45700": true,
	"7a8b5c2fbedbcefc1d8f56072e631d7470a29a0d3a3d7c28c7c63fcf87ef7dd0": true,
	"9ac6cc88e6cfda317e5be55abcbd56f917895070fe36309f670396e06b824324": true,
	"0c831829985b1ab4883473b5c5451413c134c8a68e1bf238c8030cc47538549a": true,
	"14bfa0e2aac8fb2e0c27cb7ff7de60db2af2e1c0a7fb542368da95f17c5e992f": true,
	"c5537aaf7bc648c2b7ce725bdd53e8fb07586b87dc0dfad8e16359d3d16c6f94": true,
	"086ad1e4dccfff9522b0a08cb4534bb375f3ac835c837d204298c9abfbb3988f": true,
	"8bb8b637c962f36b78279faaac00d7bb9d575145aa63b2b9fdb0d5ee382d163b": true,
	"41960303e8861e468f9824a2012e829f4805e0e390eea1492600ca7339e6a91a": true,
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

// v2CueSources collects every cue source under a v2 fixture tree. Every
// file is a cue source except the sealed split, the frozen world (part of
// the baseline) and Markdown (documentation and reports, which nothing
// loads). Files under external/ are general-purpose resources not written
// for this evaluation and get the phrase checks only. A file of any other
// kind is refused, so a new cue format cannot slip past the audit.
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
		src := LeakSource{Name: path.Join("testdata/eval/v2", p), PhraseOnly: strings.HasPrefix(p, "external/")}
		ext := path.Ext(p)
		switch {
		case ext == ".md":
			return nil
		case path.Base(p) == "cases.jsonl":
			// A case file: its texts are cue sources (train and dev
			// phrasings feed grammars and training); its labels are closed
			// vocabularies (kinds, reasons, world keys) that LoadCases and
			// the world check.
			cs, err := LoadCases(fstest.MapFS{"cases.jsonl": {Data: b}})
			if err != nil {
				return fmt.Errorf("%s: %w", p, err)
			}
			for _, c := range cs.all {
				src.Strings = append(src.Strings, c.Text)
			}
		case ext == ".json" || ext == ".jsonl":
			src.Strings, err = JSONStrings(b)
			if err != nil {
				return fmt.Errorf("%s: %w", p, err)
			}
		case ext == ".txt" || ext == ".csv" || ext == ".tsv" || ext == ".py":
			src.Strings = strings.Split(string(b), "\n")
		default:
			return fmt.Errorf("%s: the leak audit cannot read this kind of file; teach v2CueSources to", p)
		}
		out = append(out, src)
		return nil
	})
	return out, err
}

// goLiteralSources are the string literals of the non-test Go files under
// root, one source per file.
func goLiteralSources(t testing.TB, root string) []LeakSource {
	t.Helper()
	var out []LeakSource
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		src := LeakSource{Name: filepath.ToSlash(p)}
		ast.Inspect(f, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if s, err := strconv.Unquote(lit.Value); err == nil {
					src.Strings = append(src.Strings, s)
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
	return out
}

// TestNoTestPhrasingInCueSources is the leak guard: no cue source under the
// v2 fixtures, and no string literal of the router or lexicon code, carries a
// phrase, template, word or bigram that only the sealed test split holds.
func TestNoTestPhrasingInCueSources(t *testing.T) {
	a := v2Auditor(t)
	sources, err := v2CueSources(os.DirFS(v2Dir))
	if err != nil {
		t.Fatal(err)
	}
	goSource := map[string]bool{}
	for _, root := range goCueRoots {
		for _, src := range goLiteralSources(t, root) {
			goSource[src.Name] = true
			sources = append(sources, src)
		}
	}
	for _, f := range a.Audit(sources) {
		if goSource[f.Source] && (f.Check == "word" || f.Check == "bigram") && goLiteralsAtRegistration[sha([]byte(f.Check+":"+f.Token))] {
			continue
		}
		t.Errorf("leak: %s", f)
	}
	fresh, total := a.FreshTemplates()
	t.Logf("audited %d sources; %d test-only word types; %d of %d test texts have a template the baseline lacks",
		len(sources), a.TestOnlyWords(), fresh, total)
}

// TestV2BaselineIsClean: the frozen baseline itself (v1's grammars) leaks
// nothing by construction, so the audit has no false positive on it.
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
			continue // the empty-text case: no phrasing to repeat
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
// same collector the guard uses; a file the audit cannot read fails it too;
// and a file that copies a v1 grammar passes.
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
	pattern := strings.ReplaceAll(strings.Join(template.abstract, " "), slotToken, "{service}")
	pair := testPairs[0]
	if pair[0] == slotToken {
		pair[0] = "{service}"
	}
	if pair[1] == slotToken {
		pair[1] = "{service}"
	}
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
	for _, tc := range []struct {
		name  string
		files fstest.MapFS
		check string
	}{
		{"verbatim text in a lexicon file", fstest.MapFS{"lexicon/synonyms.json": {Data: []byte(`{"phrases":[` + strconv.Quote(strings.Join(phrase.raw, " ")) + `]}`)}}, "phrase"},
		{"verbatim text in an external resource", fstest.MapFS{"external/thesaurus.txt": {Data: []byte("x\n" + strings.Join(phrase.raw, " ") + "\n")}}, "phrase"},
		{"test template as a pattern", fstest.MapFS{"grammars/g.json": {Data: grammar(nil, []string{"[please] " + pattern + " [now]"})}}, "template"},
		{"test-only cue word", fstest.MapFS{"grammars/g.json": {Data: grammar([]string{testWords[0]}, nil)}}, "word"},
		{"test-only cue word as a synonym key", fstest.MapFS{"lexicon/syn.json": {Data: []byte(`{"` + testWords[len(testWords)-1] + `":"restart"}`)}}, "word"},
		{"test-only word pair", fstest.MapFS{"grammars/g.json": {Data: grammar(nil, []string{pair[0] + " " + pair[1]})}}, "bigram"},
		{"test-only word in a train case", fstest.MapFS{"cases.jsonl": {Data: []byte(`{"id":"x","caller":"alice","split":"train","tags":[],"expected":{"kind":"abstain","reason":"no_target"},"text":"` +
			testWords[0] + ` it"}` + "\n")}}, "word"},
	} {
		sources, err := v2CueSources(tc.files)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		found := false
		for _, f := range a.Audit(sources) {
			found = found || f.Check == tc.check
		}
		if !found {
			t.Errorf("%s: the audit found no %s leak", tc.name, tc.check)
		}
	}
	// An external resource is exempt from the word check only.
	sources, err := v2CueSources(fstest.MapFS{"external/words.txt": {Data: []byte(strings.Join(testWords, "\n"))}})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range a.Audit(sources) {
		if f.Check == "word" || f.Check == "bigram" {
			t.Errorf("external resource got a %s finding", f.Check)
		}
	}
	// Unreadable kinds fail closed.
	if _, err := v2CueSources(fstest.MapFS{"lexicon/vectors.bin": {Data: []byte{0}}}); err == nil {
		t.Error("an unreadable cue file was accepted")
	}
	// A copy of a v1 grammar, the sealed split and the world are clean.
	sources, err = v2CueSources(fstest.MapFS{
		"grammars/restart_service.json": {Data: v1Grammar},
		"test.jsonl.gz":                 {Data: []byte{0x1f, 0x8b}},
		"world.json":                    {Data: []byte(`{}`)},
		"README.md":                     {Data: []byte(strings.Join(phrase.raw, " "))},
	})
	if err != nil {
		t.Fatal(err)
	}
	if findings := a.Audit(sources); len(findings) != 0 {
		t.Errorf("a v1 grammar copy is reported as leaking: %v", findings)
	}
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
