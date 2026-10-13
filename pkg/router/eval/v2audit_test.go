// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package eval

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/router"
)

// Checks on router v2's own artefacts, over v2's train and dev cases only.
// Nothing here names the sealed split.

// v2InheritedWords are the grammar words no train or dev text has, listed
// with their reason as rule 5 asks. Each is an enum phrase kept unchanged
// from v1's grammars (an allowed cue source); v1's erratum lists them as
// occurring in no fixture text.
var v2InheritedWords = map[string]bool{
	"sev": true, "1": true, "2": true, "3": true, // open_incident severity phrases "sev 1" to "sev 3"
	"forcefully": true, "graceful": true, // restart_service mode phrases
}

// TestV2GrammarWordsHaveProvenance is rule 5's word audit
// (docs/router-evaluation-v2.md, "Leak prevention"): every word of a v2
// grammar (cue, pattern literal or enum phrase) occurs in some v2 train or
// dev text. v2's train split holds v1's 292 cases, so v1's texts count.
func TestV2GrammarWordsHaveProvenance(t *testing.T) {
	_, cases := loadV2(t)
	seen := map[string]bool{}
	for _, c := range cases.all {
		for _, w := range words(c.Text) {
			seen[w] = true
		}
	}
	fsys := os.DirFS(v2Dir)
	files, err := fs.Glob(fsys, path.Join("grammars", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no v2 grammars (%v)", err)
	}
	for _, f := range files {
		b, err := fs.ReadFile(fsys, f)
		if err != nil {
			t.Fatal(err)
		}
		var g struct {
			Cues     []string `json:"cues"`
			Patterns []string `json:"patterns"`
			Slots    []struct {
				Enum map[string][]string `json:"enum"`
			} `json:"slots"`
		}
		if err := json.Unmarshal(b, &g); err != nil {
			t.Fatal(err)
		}
		texts := append([]string(nil), g.Cues...)
		for _, p := range g.Patterns {
			for _, seq := range sourceSequences(p) {
				texts = append(texts, strings.Join(literals(seq), " "))
			}
		}
		for _, s := range g.Slots {
			for _, phrases := range s.Enum {
				texts = append(texts, phrases...)
			}
		}
		var missing []string
		for _, text := range texts {
			for _, w := range words(text) {
				if !seen[w] && !v2InheritedWords[w] {
					missing = append(missing, w)
				}
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			t.Errorf("%s: words in no train or dev text: %v", f, missing)
		}
	}
}

// TestV2CasesWellFormed checks v2's train and dev labels against the world,
// as TestV2TestLabelsWellFormed checks the sealed split's: the caller
// exists, an answerable case's target is visible to its caller, its slots are
// exactly what the target's schema allows with every required slot, node
// slots name visible nodes of the kind they hold, and enum slots hold
// declared values. An abstention names a closed reason and an existing
// target, and has no slots. IDs and (text, caller, split) are unique.
func TestV2CasesWellFormed(t *testing.T) {
	w, cases := loadV2(t)
	nodeKind, nodeSource := map[string]string{}, map[string]string{}
	for _, n := range w.file.Nodes {
		nodeKind[n.Key], nodeSource[n.Key] = n.Kind, n.Source
	}
	relation := map[string]worldRelationship{}
	for _, r := range w.file.Relationships {
		relation[r.Key] = r
	}
	reasons := map[router.Reason]bool{}
	for _, r := range router.Reasons {
		reasons[r] = true
	}
	all := map[string]bool{}
	visible := map[string]map[string]router.Target{}
	for _, caller := range w.Callers() {
		visible[caller] = map[string]router.Target{}
		for _, target := range w.Targets(caller) {
			visible[caller][target.Ref.Key()] = target
			all[target.Ref.Key()] = true
		}
	}
	ids := map[string]bool{}
	seen := map[[3]string]bool{}
	counts := map[string]int{}
	for _, c := range cases.all {
		fail := func(format string, args ...any) {
			t.Errorf("case %s: %s", c.ID, fmt.Sprintf(format, args...))
		}
		if c.Split != "train" && c.Split != "dev" {
			fail("split %q is not train or dev", c.Split)
		}
		counts[c.Split]++
		if ids[c.ID] {
			fail("repeats an ID")
		}
		ids[c.ID] = true
		key := [3]string{strings.ToLower(c.Text), c.Caller, c.Split}
		if seen[key] {
			fail("repeats a (text, caller) pair in its split")
		}
		seen[key] = true
		targets, ok := visible[c.Caller]
		if !ok {
			fail("unknown caller")
			continue
		}
		for _, tag := range c.Tags {
			if !v2Tags[tag] {
				fail("unknown tag %q", tag)
			}
		}
		e := c.Expected
		if e.Kind == router.KindAbstain {
			if !reasons[e.Reason] {
				fail("abstention reason %q is not a router reason", e.Reason)
			}
			if e.Target != "" && !all[e.Target] {
				fail("abstention names a target that does not exist")
			}
			if len(e.Slots) != 0 {
				fail("an abstention has slots")
			}
			continue
		}
		if e.Reason != "" {
			fail("an answerable case has a reason")
		}
		target, ok := targets[e.Target]
		if !ok || target.Ref.Kind != e.Kind {
			fail("target is not a visible %s target of its caller", e.Kind)
			continue
		}
		var schema slotSchema
		nodeKinds := map[string]string{}
		var err error
		switch e.Kind {
		case router.KindAction:
			err = json.Unmarshal(target.Action.InputSchema, &schema)
		case router.KindDecision:
			err = json.Unmarshal(target.SlotSchema, &schema)
		case router.KindLookup:
			r := relation[target.Ref.Lookup.RelationKey]
			ends := r.From
			if target.Ref.Lookup.Direction == "in" {
				ends = r.To
			}
			if len(ends) != 1 {
				fail("lookup relation has %d subject kinds", len(ends))
				continue
			}
			schema.Required = []string{"subject"}
			schema.Properties = map[string]struct {
				Enum []string `json:"enum"`
			}{"subject": {}}
			nodeKinds["subject"] = ends[0]
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range schema.Required {
			if _, ok := e.Slots[name]; !ok {
				fail("required slot %s is missing", name)
			}
		}
		for name, value := range e.Slots {
			prop, ok := schema.Properties[name]
			if !ok {
				fail("slot %s is not in the target's schema", name)
				continue
			}
			if len(prop.Enum) > 0 {
				found := false
				for _, v := range prop.Enum {
					found = found || v == value
				}
				if !found {
					fail("slot %s holds an undeclared enum value", name)
				}
				continue
			}
			kind, ok := nodeKind[value]
			if !ok {
				fail("slot %s names no node", name)
				continue
			}
			if !w.sees(c.Caller, nodeSource[value]) {
				fail("slot %s names a node its caller cannot see", name)
			}
			want := nodeKinds[name]
			if want == "" {
				want = name
			}
			if kind != want {
				fail("slot %s names a %s node, not a %s", name, kind, want)
			}
		}
	}
	if counts["train"] == 0 || counts["dev"] == 0 {
		t.Fatalf("v2 cases: %d train, %d dev", counts["train"], counts["dev"])
	}
	// The train split's texts are disjoint from dev's, so dev measures
	// phrasings the model was not trained on.
	train := map[string]bool{}
	for _, c := range cases.all {
		if c.Split == "train" {
			train[strings.ToLower(c.Text)] = true
		}
	}
	for _, c := range cases.all {
		if c.Split == "dev" && train[strings.ToLower(c.Text)] {
			t.Errorf("dev case %s repeats a train text", c.ID)
		}
	}
}
