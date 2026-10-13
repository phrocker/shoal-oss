// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package eval

import (
	"bytes"
	"encoding/json"
	"io"
	"sort"
	"strings"

	"github.com/phrocker/shoal-oss/pkg/lexicon"
)

// The leak audit (docs/router-evaluation-v2.md, "Leak prevention") checks
// that no cue source carries a phrasing that only the sealed test split
// holds. v1's erratum found the cue "exposed" in a grammar although only a
// test phrasing contained it; this audit makes that a test failure.
//
// Text is compared after lexicon.Tokenize, the router's own normalization.
// Entity mentions (node names and aliases of the fixture world) are
// abstracted to a slot token, as are {slot} placeholders in grammar patterns,
// and optional [..] groups in patterns are expanded.
//
// A test-derived unit is a leak when it occurs in a cue source and nowhere in
// the frozen baseline corpus (for a phrase or template: not inside any
// baseline text or pattern either): the pre-registration corpus that existed before
// the test split was authored (v1's cases of every split, v1's grammars and
// the world). Four units are checked:
//
//   - phrase: a whole test text of three or more tokens, verbatim;
//   - template: a whole test text with its mentions abstracted (at least
//     three tokens, two of them literal);
//   - word: a word of a test text that the baseline does not have;
//   - bigram: an adjacent pair of a test text, mentions abstracted, that the
//     baseline does not have.
//
// Findings name the source, the check, a test case ID and the offending
// unit, quoting only what the source itself contains.

// slotToken stands for an abstracted mention or a {slot} placeholder.
// Tokenize never produces it.
const slotToken = "\x01"

// maxExpansions bounds the expansion of one pattern's optional groups.
const maxExpansions = 4096

// LeakSource is one cue source: a named list of strings.
type LeakSource struct {
	Name    string
	Strings []string
	// PhraseOnly marks a general-purpose resource not written for this
	// evaluation; only the phrase and template checks apply to it.
	PhraseOnly bool
}

// LeakFinding is one leak. Its Token is what the source contains.
type LeakFinding struct {
	Source string
	Check  string
	Case   string
	Token  string
}

func (f LeakFinding) String() string {
	return f.Source + ": " + f.Check + " " + quoteToken(f.Token) + " (test case " + f.Case + ")"
}

func quoteToken(t string) string {
	return "\"" + strings.ReplaceAll(t, slotToken, "{slot}") + "\""
}

type auditText struct {
	id       string
	raw      []string
	abstract []string
	// known: the baseline corpus already holds the text (verbatim) or its
	// template (abstracted) somewhere, as a whole or inside a longer text
	// or pattern, so neither is a leak.
	rawKnown, abstractKnown bool
}

// LeakAuditor holds the sealed test split's units and the baseline corpus.
type LeakAuditor struct {
	entities         [][]string
	test             []auditText
	baselineWords    map[string]bool
	baselineBigrams  map[[2]string]bool
	baselineTemplate map[string]bool
	testOnlyWords    map[string]string
	testOnlyBigrams  map[[2]string]string
}

// NewLeakAuditor builds an auditor. entities are the world's mention surface
// forms; baselineTexts are case texts and baselineSources cue strings of the
// frozen baseline corpus; test is the sealed split.
func NewLeakAuditor(entities, baselineTexts, baselineSources []string, test []Case) *LeakAuditor {
	a := &LeakAuditor{
		baselineWords: map[string]bool{}, baselineBigrams: map[[2]string]bool{}, baselineTemplate: map[string]bool{},
		testOnlyWords: map[string]string{}, testOnlyBigrams: map[[2]string]string{},
	}
	for _, e := range entities {
		if tokens := words(e); len(tokens) > 0 {
			a.entities = append(a.entities, tokens)
		}
	}
	// Longest surface form first, so "payments team" wins over "payments".
	sort.SliceStable(a.entities, func(i, j int) bool { return len(a.entities[i]) > len(a.entities[j]) })
	var baseRaw, baseAbstract [][]string
	for _, text := range baselineTexts {
		raw := words(text)
		abstract := a.abstract(raw)
		a.addBaseline(raw, abstract)
		a.baselineTemplate[strings.Join(abstract, " ")] = true
		baseRaw, baseAbstract = append(baseRaw, raw), append(baseAbstract, abstract)
	}
	for _, s := range baselineSources {
		for _, seq := range sourceSequences(s) {
			a.addBaseline(literals(seq), seq)
			baseRaw, baseAbstract = append(baseRaw, literals(seq)), append(baseAbstract, seq)
		}
	}
	for _, c := range test {
		raw := words(c.Text)
		t := auditText{id: c.ID, raw: raw, abstract: a.abstract(raw)}
		for i := range baseRaw {
			t.rawKnown = t.rawKnown || contains(baseRaw[i], t.raw)
			t.abstractKnown = t.abstractKnown || contains(baseAbstract[i], t.abstract)
		}
		a.test = append(a.test, t)
		for _, w := range raw {
			if !a.baselineWords[w] {
				if _, seen := a.testOnlyWords[w]; !seen {
					a.testOnlyWords[w] = c.ID
				}
			}
		}
		for _, b := range bigrams(t.abstract) {
			if !a.baselineBigrams[b] {
				if _, seen := a.testOnlyBigrams[b]; !seen {
					a.testOnlyBigrams[b] = c.ID
				}
			}
		}
	}
	return a
}

func (a *LeakAuditor) addBaseline(raw, abstract []string) {
	for _, w := range raw {
		a.baselineWords[w] = true
	}
	for _, b := range bigrams(abstract) {
		a.baselineBigrams[b] = true
	}
}

// TestOnlyWords is the number of distinct test-split words the baseline
// corpus lacks; a descriptive count that names none of them.
func (a *LeakAuditor) TestOnlyWords() int { return len(a.testOnlyWords) }

// FreshTemplates reports how many test texts have an abstracted form the
// baseline corpus lacks, out of all test texts.
func (a *LeakAuditor) FreshTemplates() (fresh, total int) {
	for _, t := range a.test {
		if !a.baselineTemplate[strings.Join(t.abstract, " ")] {
			fresh++
		}
	}
	return fresh, len(a.test)
}

// Audit returns every leak of the test split into sources, sorted.
func (a *LeakAuditor) Audit(sources []LeakSource) []LeakFinding {
	seen := map[LeakFinding]bool{}
	var out []LeakFinding
	add := func(f LeakFinding) {
		if !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	for _, src := range sources {
		for _, s := range src.Strings {
			for _, seq := range sourceSequences(s) {
				lits := literals(seq)
				for _, t := range a.test {
					if len(t.raw) >= 3 && !t.rawKnown && contains(lits, t.raw) {
						add(LeakFinding{Source: src.Name, Check: "phrase", Case: t.id, Token: strings.Join(t.raw, " ")})
					}
					if len(t.abstract) >= 3 && countLiteral(t.abstract) >= 2 && !t.abstractKnown && contains(seq, t.abstract) {
						add(LeakFinding{Source: src.Name, Check: "template", Case: t.id, Token: strings.Join(t.abstract, " ")})
					}
				}
				if src.PhraseOnly {
					continue
				}
				for _, w := range seq {
					if id, ok := a.testOnlyWords[w]; ok {
						add(LeakFinding{Source: src.Name, Check: "word", Case: id, Token: w})
					}
				}
				for _, b := range bigrams(seq) {
					if id, ok := a.testOnlyBigrams[b]; ok {
						add(LeakFinding{Source: src.Name, Check: "bigram", Case: id, Token: b[0] + " " + b[1]})
					}
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		x, y := out[i], out[j]
		if x.Source != y.Source {
			return x.Source < y.Source
		}
		if x.Check != y.Check {
			return x.Check < y.Check
		}
		if x.Token != y.Token {
			return x.Token < y.Token
		}
		return x.Case < y.Case
	})
	return out
}

// abstract replaces each entity mention, longest first, with slotToken.
func (a *LeakAuditor) abstract(raw []string) []string {
	var out []string
	for i := 0; i < len(raw); {
		matched := 0
		for _, e := range a.entities {
			if len(e) <= len(raw)-i && equalTokens(raw[i:i+len(e)], e) {
				matched = len(e)
				break
			}
		}
		if matched > 0 {
			out = append(out, slotToken)
			i += matched
			continue
		}
		out = append(out, raw[i])
		i++
	}
	return out
}

func words(s string) []string {
	var out []string
	for _, t := range lexicon.Tokenize(s) {
		out = append(out, t.Text)
	}
	return out
}

// sourceSequences tokenizes a cue string: each expansion of its optional
// [..] groups, with every {..} placeholder as slotToken.
func sourceSequences(s string) [][]string {
	var out [][]string
	for _, e := range expandOptional(s) {
		var seq []string
		rest := e
		for {
			open := strings.IndexByte(rest, '{')
			if open < 0 {
				seq = append(seq, words(rest)...)
				break
			}
			end := strings.IndexByte(rest[open:], '}')
			if end < 0 {
				seq = append(seq, words(rest)...)
				break
			}
			seq = append(seq, words(rest[:open])...)
			seq = append(seq, slotToken)
			rest = rest[open+end+1:]
		}
		if len(seq) > 0 {
			out = append(out, seq)
		}
	}
	return out
}

// expandOptional expands optional [..] groups, nested or not. A string with
// unbalanced brackets, or more than maxExpansions expansions, is returned
// with its brackets removed (every optional part present) as well, so the
// audit still sees it whole.
func expandOptional(s string) []string {
	if !strings.ContainsAny(s, "[]") {
		return []string{s}
	}
	alts, rest, ok := expandSeq(s)
	if !ok || rest != "" || len(alts) > maxExpansions {
		return []string{strings.NewReplacer("[", " ", "]", " ").Replace(s)}
	}
	return alts
}

// expandSeq expands s up to an unmatched ']' and returns the remainder after
// it (or "" at the end).
func expandSeq(s string) (alts []string, rest string, ok bool) {
	alts = []string{""}
	for len(s) > 0 {
		switch s[0] {
		case ']':
			return alts, s, true
		case '[':
			inner, after, ok := expandSeq(s[1:])
			if !ok || len(after) == 0 || after[0] != ']' {
				return nil, "", false
			}
			inner = append(inner, " ")
			var next []string
			for _, a := range alts {
				for _, b := range inner {
					next = append(next, a+" "+b+" ")
					if len(next) > maxExpansions {
						return nil, "", false
					}
				}
			}
			alts = next
			s = after[1:]
		default:
			i := strings.IndexAny(s, "[]")
			if i < 0 {
				i = len(s)
			}
			for k := range alts {
				alts[k] += s[:i]
			}
			s = s[i:]
		}
	}
	return alts, "", true
}

func literals(seq []string) []string {
	var out []string
	for _, t := range seq {
		if t != slotToken {
			out = append(out, t)
		}
	}
	return out
}

func countLiteral(seq []string) int { return len(literals(seq)) }

func bigrams(seq []string) [][2]string {
	var out [][2]string
	for i := 0; i+1 < len(seq); i++ {
		if seq[i] == slotToken && seq[i+1] == slotToken {
			continue
		}
		out = append(out, [2]string{seq[i], seq[i+1]})
	}
	return out
}

func equalTokens(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// contains reports whether needle occurs contiguously in hay.
func contains(hay, needle []string) bool {
	for i := 0; i+len(needle) <= len(hay); i++ {
		if equalTokens(hay[i:i+len(needle)], needle) {
			return true
		}
	}
	return false
}

// JSONStrings returns every object key and string value in a JSON document
// (or in each document of a JSON-lines stream), skipping the values, though
// not the keys, of the named fields. Comments in a fixture are prose for
// readers, so the baseline skips them; a cue source skips nothing.
func JSONStrings(doc []byte, skipValuesOf ...string) ([]string, error) {
	skip := map[string]bool{}
	for _, k := range skipValuesOf {
		skip[k] = true
	}
	var out []string
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			out = append(out, x)
		case []any:
			for _, e := range x {
				walk(e)
			}
		case map[string]any:
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				out = append(out, k)
				if !skip[k] {
					walk(x[k])
				}
			}
		}
	}
	d := json.NewDecoder(bytes.NewReader(doc))
	for {
		var v any
		if err := d.Decode(&v); err == io.EOF {
			return out, nil
		} else if err != nil {
			return nil, err
		}
		walk(v)
	}
}

// SurfaceForms are the world's mention surface forms: every node's name and
// aliases.
func (w *World) SurfaceForms() []string {
	var out []string
	for _, n := range w.file.Nodes {
		out = append(out, n.Name)
		out = append(out, n.Aliases...)
	}
	return out
}
