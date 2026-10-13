// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package router

import (
	"encoding/json"
	"strings"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// FeatureSchemaV2ID names router v2's pair features: router.pair/v1's nine,
// unchanged and in the same positions, followed by features that do not
// depend on a pattern covering the text (docs/router-evaluation-v2.md,
// "What v2 is expected to change"):
//
//   - cue scoring: vocabulary evidence weighted by how few visible candidates
//     share a word, compared across the candidates of one text;
//   - partial, slot-anchored pattern matching: a pattern's literals and slots
//     matched in order with gaps allowed, slots only over whole mentions or
//     enum phrases, so word order still tells "what depends on X" from "what
//     does X depend on";
//   - slot satisfiability from the lexicon: whether the mentions the caller
//     may see can fill the target's required slots, using the graph-derived
//     lookup templates' subject concepts for lookups.
//
// Words are compared by a fixed suffix-stripping stem (stem), so an
// inflection of a grammar word counts as that word. Like v1's, every feature
// is a scope-free number in [0, 1]: the weights cannot memorize an entity,
// target or caller. Features that compare candidates compare only the
// caller's visible catalog, so a hidden target changes nothing.
const FeatureSchemaV2ID = "router.pair/v2"

// Feature indices of router.pair/v2 after the v1 block.
const (
	// FeatureV2CueStem is 1 when a content word of the text has the stem of
	// one of the candidate's cues.
	FeatureV2CueStem = NumFeatures + iota
	// FeatureV2Recall is the share of the text's content words (not in a
	// mention, not a function word) whose stem is in the candidate's
	// vocabulary.
	FeatureV2Recall
	// FeatureV2EvidenceShare is the candidate's share of the text's
	// vocabulary evidence over all visible candidates. A content word in the
	// candidate's vocabulary adds 1/df, where df is the number of visible
	// candidates whose vocabulary has its stem, and as much again if it is
	// one of the candidate's cues.
	FeatureV2EvidenceShare
	// FeatureV2EvidenceTop is 1 when the candidate's evidence is positive and
	// strictly the largest.
	FeatureV2EvidenceTop
	// FeatureV2EvidenceLead is, for the top candidate, the share of its
	// evidence by which it leads the next one; 0 otherwise.
	FeatureV2EvidenceLead
	// FeatureV2EvidenceDeficit is, for any other candidate, the share of the
	// best other candidate's evidence it lacks; 0 for the top candidate.
	FeatureV2EvidenceDeficit
	// FeatureV2PartialBest is the best pattern's share of required elements
	// (literals and slots outside optional groups) matched in order, with
	// gaps allowed.
	FeatureV2PartialBest
	// FeatureV2PartialFull is 1 when some pattern's required elements all
	// match in order, with gaps allowed.
	FeatureV2PartialFull
	// FeatureV2PartialCover is the share of the text's tokens the best
	// partial match consumes.
	FeatureV2PartialCover
	// FeatureV2SlotFill is the share of the target's required inputs that
	// distinct fitting mentions or enum phrases of the text could fill; 1
	// when it requires none.
	FeatureV2SlotFill
	// FeatureV2MentionFit is the share of the text's mentions that fit one of
	// the candidate's node slots; with no mentions, 1 if the candidate has no
	// node slot and 0 otherwise.
	FeatureV2MentionFit
	// FeatureV2Unknown is the share of the text's content words whose stem no
	// visible candidate's vocabulary has. It is the same for every candidate
	// of one text.
	FeatureV2Unknown
	// NumFeaturesV2 is the router.pair/v2 vector width.
	NumFeaturesV2
)

// FeatureNamesV2 lists router.pair/v2's features in index order.
var FeatureNamesV2 = func() [NumFeaturesV2]string {
	var out [NumFeaturesV2]string
	copy(out[:], FeatureNames[:])
	copy(out[NumFeatures:], []string{
		"cue_stem", "stem_recall", "evidence_share", "evidence_top", "evidence_lead",
		"evidence_deficit", "partial_best", "partial_full", "partial_cover",
		"slot_fill", "mention_fit", "unknown_share",
	})
	return out
}()

// candidateV2 is what router.pair/v2 precomputes for one candidate.
type candidateV2 struct {
	// vocab and cues are stems of the content words of the candidate's
	// vocabulary and of its cues.
	vocab, cues map[string]bool
	// required are the slots the target's schema requires, by index;
	// unplaced counts required fields that no slot fills.
	required []int
	unplaced int
	// concepts are a lookup's subject concepts.
	concepts map[shoal.ID]bool
	// patterns are the grammar's patterns without their optional groups.
	patterns [][]element
}

func prepareV2(c *candidate) candidateV2 {
	g := c.grammar
	v := candidateV2{vocab: map[string]bool{}, cues: map[string]bool{}, concepts: map[shoal.ID]bool{}}
	for word := range c.vocab {
		if !functionWords[word] {
			v.vocab[stem(word)] = true
		}
	}
	for _, cue := range g.cues {
		if !functionWords[cue] {
			v.cues[stem(cue)] = true
		}
	}
	for _, p := range g.patterns {
		var required []element
		for _, e := range p.elems {
			if e.kind != elemOptional {
				required = append(required, e)
			}
		}
		v.patterns = append(v.patterns, required)
	}
	target := c.executors[0]
	switch c.kind {
	case KindLookup:
		for _, id := range target.SubjectConcepts {
			v.concepts[id] = true
		}
		for i, s := range g.slots {
			if s.name == LookupSubjectSlot {
				v.required = append(v.required, i)
			}
		}
		if len(v.required) == 0 {
			v.unplaced = 1
		}
	default:
		schema := target.SlotSchema
		if c.kind == KindAction && target.Action != nil {
			schema = target.Action.InputSchema
		}
		var root struct {
			Required []string `json:"required"`
		}
		_ = json.Unmarshal(schema, &root)
		placed := topLevel(g.input)
		index := map[string]int{}
		for i, s := range g.slots {
			index[s.name] = i
		}
		for _, field := range root.Required {
			var value string
			if raw, ok := placed[field]; ok && json.Unmarshal(raw, &value) == nil && len(value) > 1 && value[0] == '$' {
				if i, ok := index[value[1:]]; ok {
					v.required = append(v.required, i)
					continue
				}
			}
			v.unplaced++
		}
	}
	return v
}

func stemFrequencies(candidates []candidate) map[string]int {
	df := map[string]int{}
	for _, c := range candidates {
		for s := range c.v2.vocab {
			df[s]++
		}
	}
	return df
}

// stem strips a few English inflections: a plural or third-person "s",
// "ies", "ing" and "ed", then a doubled final consonant and a final "e". It
// is fixed, knows no word, and maps a word and its inflections to one stem
// often enough ("restarts", "restarting", "restarted"; "scale", "scaling").
func stem(w string) string {
	n := len(w)
	switch {
	case n > 4 && strings.HasSuffix(w, "ies"):
		w = w[:n-3] + "y"
	case n > 4 && strings.HasSuffix(w, "sses"):
		w = w[:n-2]
	case n > 3 && strings.HasSuffix(w, "s") && !strings.HasSuffix(w, "ss") && !strings.HasSuffix(w, "us"):
		w = w[:n-1]
	}
	n = len(w)
	switch {
	case n > 5 && strings.HasSuffix(w, "ing"):
		w = w[:n-3]
	case n > 4 && strings.HasSuffix(w, "ed"):
		w = w[:n-2]
	}
	n = len(w)
	if n > 3 && w[n-1] == w[n-2] && consonant(w[n-1]) && !strings.ContainsRune("lsz", rune(w[n-1])) {
		w = w[:n-1]
	}
	n = len(w)
	if n > 3 && w[n-1] == 'e' {
		w = w[:n-1]
	}
	return w
}

func consonant(b byte) bool {
	return b >= 'a' && b <= 'z' && !strings.ContainsRune("aeiouy", rune(b))
}

// fitsSlot reports whether mention m could fill slot s of candidate c: for a
// lookup, a node of one of the template's subject concepts; otherwise a node
// of a kind the slot accepts. An ambiguous mention fits when any of its
// nodes does, so the target is still chosen and slot filling then abstains.
func (a *Analysis) fitsSlot(c *candidate, s slotSpec, mi int) bool {
	m := a.text.mentions[mi]
	if c.kind == KindLookup {
		for _, id := range m.NodeIDs {
			if concept, ok := a.concepts[id]; ok && c.v2.concepts[concept] {
				return true
			}
		}
		return false
	}
	return s.kindFits(m)
}

// enumPresent reports whether an enum phrase of s occurs outside mentions,
// comparing stems.
func enumPresent(t text, stems []string, s slotSpec) bool {
	for _, v := range s.enum {
		for _, phrase := range v.phrases {
			for p := 0; p+len(phrase) <= len(t.tokens); p++ {
				if phraseStemsAt(t, stems, phrase, p) {
					return true
				}
			}
		}
	}
	return false
}

func phraseStemsAt(t text, stems []string, phrase []string, p int) bool {
	for k, token := range phrase {
		if t.inMention[p+k] || stems[p+k] != stem(token) {
			return false
		}
	}
	return true
}

type partialScore struct{ matched, consumed int }

func (x partialScore) better(y partialScore) bool {
	if x.matched != y.matched {
		return x.matched > y.matched
	}
	return x.consumed > y.consumed
}

// partial is the best in-order match of one pattern's required elements,
// with gaps allowed in the text and elements allowed to go unmatched: the
// most elements matched, then the most tokens consumed. A literal matches a
// token outside mentions with its stem; a node slot only a whole mention
// that fits it; an enum slot one of its phrases.
func (a *Analysis) partial(c *candidate, elems []element, stems []string) partialScore {
	t := a.text
	n := len(t.tokens)
	// next[j] is the best score of the remaining elements from position j.
	next := make([]partialScore, n+1)
	cur := make([]partialScore, n+1)
	for i := len(elems) - 1; i >= 0; i-- {
		e := elems[i]
		for j := n; j >= 0; j-- {
			best := next[j]
			if j < n {
				if cur[j+1].better(best) {
					// Skipping text token j, still matching from element i.
					best = cur[j+1]
				}
				try := func(length int) {
					if j+length > n {
						return
					}
					s := partialScore{1 + next[j+length].matched, length + next[j+length].consumed}
					if s.better(best) {
						best = s
					}
				}
				switch e.kind {
				case elemLiteral:
					if !t.inMention[j] && stems[j] == stem(e.token) {
						try(1)
					}
				case elemSlot:
					s := c.grammar.slots[e.slot]
					if s.isEnum() {
						for _, v := range s.enum {
							for _, phrase := range v.phrases {
								if j+len(phrase) <= n && phraseStemsAt(t, stems, phrase, j) {
									try(len(phrase))
								}
							}
						}
					} else if mi, ok := t.mentionAt[j]; ok && a.fitsSlot(c, s, mi) {
						m := t.mentions[mi]
						try(m.TokenSpan.End - m.TokenSpan.Start)
					}
				}
			}
			cur[j] = best
		}
		next, cur = cur, next
	}
	return next[0]
}

// featurizeV2 computes every candidate's router.pair/v2 vector. It runs
// after the v1 features, which it copies unchanged.
func (a *Analysis) featurizeV2() {
	t := a.text
	stems := make([]string, len(t.tokens))
	var content []int
	for i, token := range t.tokens {
		stems[i] = stem(token)
		if !t.inMention[i] && !functionWords[token] {
			content = append(content, i)
		}
	}
	known := 0
	for _, p := range content {
		if a.catalog.stemDF[stems[p]] > 0 {
			known++
		}
	}
	evidence := make([]float64, len(a.cands))
	for ci := range a.cands {
		c := &a.catalog.candidates[ci]
		f := &a.cands[ci].featuresV2
		copy(f[:NumFeatures], a.cands[ci].features[:])
		inVocab := 0
		var e float64
		for _, p := range content {
			s := stems[p]
			if c.v2.cues[s] {
				f[FeatureV2CueStem] = 1
			}
			if !c.v2.vocab[s] {
				continue
			}
			inVocab++
			w := 1 / float64(a.catalog.stemDF[s])
			e = float64(e + w)
			if c.v2.cues[s] {
				e = float64(e + w)
			}
		}
		evidence[ci] = e
		f[FeatureV2Recall] = ratio(inVocab, len(content))

		var best partialScore
		bestShare := 0.0
		for _, elems := range c.v2.patterns {
			if len(elems) == 0 {
				continue
			}
			s := a.partial(c, elems, stems)
			share := float64(s.matched) / float64(len(elems))
			if share > bestShare || (share == bestShare && s.consumed > best.consumed) {
				best, bestShare = s, share
			}
		}
		f[FeatureV2PartialBest] = quantize(bestShare)
		if bestShare == 1 {
			f[FeatureV2PartialFull] = 1
		}
		f[FeatureV2PartialCover] = ratio(best.consumed, len(t.tokens))

		filled := 0
		used := map[int]bool{}
		for _, si := range c.v2.required {
			s := c.grammar.slots[si]
			if s.isEnum() {
				if enumPresent(t, stems, s) {
					filled++
				}
				continue
			}
			for mi := range t.mentions {
				if !used[mi] && a.fitsSlot(c, s, mi) {
					used[mi] = true
					filled++
					break
				}
			}
		}
		if total := len(c.v2.required) + c.v2.unplaced; total == 0 {
			f[FeatureV2SlotFill] = 1
		} else {
			f[FeatureV2SlotFill] = ratio(filled, total)
		}

		nodeSlots, fitting := 0, 0
		for _, s := range c.grammar.slots {
			if !s.isEnum() {
				nodeSlots++
			}
		}
		for mi := range t.mentions {
			for _, s := range c.grammar.slots {
				if !s.isEnum() && a.fitsSlot(c, s, mi) {
					fitting++
					break
				}
			}
		}
		switch {
		case len(t.mentions) > 0:
			f[FeatureV2MentionFit] = ratio(fitting, len(t.mentions))
		case nodeSlots == 0:
			f[FeatureV2MentionFit] = 1
		}
		f[FeatureV2Unknown] = ratio(len(content)-known, len(content))
	}
	var total float64
	for _, e := range evidence {
		total = float64(total + e)
	}
	for ci, e := range evidence {
		f := &a.cands[ci].featuresV2
		other := 0.0
		for cj, x := range evidence {
			if cj != ci && x > other {
				other = x
			}
		}
		if total > 0 {
			f[FeatureV2EvidenceShare] = quantize(e / total)
		}
		switch {
		case e > 0 && e > other:
			f[FeatureV2EvidenceTop] = 1
			f[FeatureV2EvidenceLead] = quantize((e - other) / e)
		case other > 0:
			f[FeatureV2EvidenceDeficit] = quantize((other - e) / other)
		}
	}
}
