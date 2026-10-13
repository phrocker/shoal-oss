// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package eval

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/phrocker/shoal-oss/pkg/router"
)

const (
	v2Dir     = "../testdata/eval/v2"
	v2Archive = v2Dir + "/test.jsonl.gz"
	v2Doc     = "../../../docs/router-evaluation-v2.md"
)

// v2BaselineSHA256 pins the frozen baseline corpus the leak audit measures
// against (v1's cases of every split, v1's grammars and v1's world): see
// baselineDigest. Editing any of those files would let a test word into the
// baseline and through the audit, so they are frozen with the split.
const v2BaselineSHA256 = "0c815b732417ce02f25aca477d1f4a798eb8834aebbca54d68477e45b2e7f333"

// These tests touch the sealed split only to check its seal, its labels
// against the world, its freshness and the leak audit. None of them routes a
// case or runs the baseline, the analysis or the lexicon on it. Their
// failure messages name case IDs, never texts.

func sha(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// openArchive checks the committed archive's digest and decompresses it.
func openArchive(archive []byte) ([]byte, error) {
	if got := sha(archive); got != V2TestArchiveSHA256 {
		return nil, fmt.Errorf("test.jsonl.gz has SHA-256 %s, not the registered %s", got, V2TestArchiveSHA256)
	}
	zr, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return nil, err
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	return plain, zr.Close()
}

func casesFrom(t testing.TB, plain []byte) *Cases {
	t.Helper()
	cs, err := LoadCases(fstest.MapFS{"cases.jsonl": {Data: plain}})
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

// sealedV2 loads the sealed split, failing unless its archive and content
// digests are the registered ones.
func sealedV2(t testing.TB) *Cases {
	t.Helper()
	archive, err := os.ReadFile(v2Archive)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := openArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	if got := sha(plain); got != V2TestDigest {
		t.Fatalf("decompressed v2 test split has SHA-256 %s, not the registered %s", got, V2TestDigest)
	}
	cs := casesFrom(t, plain)
	if err := cs.CheckV2Seal(); err != nil {
		t.Fatal(err)
	}
	return cs
}

// TestV2TestSplitSealed recomputes the sealed split's digests and checks
// that no gate releases it: Split and v1's PreRegistered refuse it, and
// PreRegisteredV2 refuses it until the run opens.
func TestV2TestSplitSealed(t *testing.T) {
	cs := sealedV2(t)
	if got := cs.Digest("test"); got != V2TestDigest {
		t.Fatalf("Digest(test) = %s, want %s", got, V2TestDigest)
	}
	if got, err := cs.Split("test"); !errors.Is(err, ErrHeldOut) || got != nil {
		t.Fatalf("Split(test) = %d cases, %v", len(got), err)
	}
	if got, err := cs.PreRegistered("test"); err == nil || got != nil {
		t.Fatalf("v1's gate released the v2 test split: %d cases", len(got))
	}
	got, err := cs.PreRegisteredV2()
	if V2TestRunOpen {
		if err != nil || len(got) != V2TestCases {
			t.Fatalf("PreRegisteredV2 = %d cases, %v", len(got), err)
		}
	} else if !errors.Is(err, ErrV2Sealed) || got != nil {
		t.Fatalf("PreRegisteredV2 before the run = %d cases, %v", len(got), err)
	}
}

// TestV2SealRefusesAnyChange is the seal's mutation check: a changed label,
// a dropped, added, duplicated or reordered case, a non-test case, or a
// changed archive byte is refused.
func TestV2SealRefusesAnyChange(t *testing.T) {
	archive, err := os.ReadFile(v2Archive)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := openArchive(archive)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(plain), "\n")
	if lines[len(lines)-1] != "" {
		t.Fatal("the sealed split does not end with a newline")
	}
	lines = lines[:len(lines)-1]
	relabel := func(i int, edit func(map[string]any)) string {
		var v map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &v); err != nil {
			t.Fatal(err)
		}
		edit(v)
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(b) + "\n"
	}
	join := func(ls []string) []byte { return []byte(strings.Join(ls, "")) }
	with := func(i int, line string) []byte {
		c := append([]string(nil), lines...)
		c[i] = line
		return join(c)
	}
	mutations := map[string][]byte{
		"label": with(0, relabel(0, func(v map[string]any) {
			v["expected"].(map[string]any)["kind"] = "abstain"
		})),
		"text":      with(1, relabel(1, func(v map[string]any) { v["text"] = v["text"].(string) + " now" })),
		"split":     with(2, relabel(2, func(v map[string]any) { v["split"] = "dev" })),
		"dropped":   join(lines[1:]),
		"added":     join(append(append([]string(nil), lines...), relabel(0, func(v map[string]any) { v["id"] = "v2t-9999" }))),
		"duplicate": join(append(append([]string(nil), lines[:len(lines)-1]...), lines[0])),
		"reordered": join(append(append([]string(nil), lines[1:]...), lines[0])),
		"spacing":   with(3, " "+lines[3]),
	}
	for name, mutated := range mutations {
		if bytes.Equal(mutated, plain) {
			t.Fatalf("%s: mutation changed nothing", name)
		}
		if sha(mutated) == V2TestDigest {
			t.Fatalf("%s: mutated split has the registered digest", name)
		}
		cs, err := LoadCases(fstest.MapFS{"cases.jsonl": {Data: mutated}})
		if err != nil {
			continue // refused at parse time
		}
		if err := cs.CheckV2Seal(); err == nil {
			t.Errorf("%s: a changed split passed the seal", name)
		}
		if got, err := cs.PreRegisteredV2(); err == nil || got != nil {
			t.Errorf("%s: a changed split was released", name)
		}
	}
	flipped := append([]byte(nil), archive...)
	flipped[len(flipped)/2] ^= 1
	if _, err := openArchive(flipped); err == nil {
		t.Error("an archive with a changed byte was accepted")
	}
}

// v2ProtocolSHA256 pins the pre-registration's fixed part: the SHA-256 of
// docs/router-evaluation-v2.md up to its "## Amendments" heading. Later
// changes may only append amendments, results and errata below it.
const v2ProtocolSHA256 = "7805afbe974f785afe3694bf204a8162b84afbc808359a9cca416e80fd348b0b"

// TestV2DocRecordsTheSeal: the pre-registration records every digest the
// code pins, so neither can change without the other, and its protocol text
// is pinned.
func TestV2DocRecordsTheSeal(t *testing.T) {
	doc, err := os.ReadFile(v2Doc)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"V2TestDigest": V2TestDigest, "V2TestArchiveSHA256": V2TestArchiveSHA256,
		"V2WorldSHA256": V2WorldSHA256, "v2BaselineSHA256": v2BaselineSHA256,
		"V2TestCases":            fmt.Sprintf("| %d |", V2TestCases),
		"grandfather digest":     grandfatherDigest(goLiteralsAtRegistration),
		"grandfather size":       fmt.Sprintf("%d grandfathered units", len(goLiteralsAtRegistration)),
		"G2 threshold":           fmt.Sprintf("at least %d of %d", v2G2Min, v2Unanswerable),
		"S2 threshold":           fmt.Sprintf("at least %d of 191", v2CoverageTarget),
		"frozen non-Go file set": fmt.Sprintf("%d frozen non-Go files", len(frozenNonGo)),
	} {
		if !bytes.Contains(doc, []byte(want)) {
			t.Errorf("%s: %s is not recorded in %s", name, want, v2Doc)
		}
	}
	protocol, _, found := bytes.Cut(doc, []byte("\n## Amendments\n"))
	if !found {
		t.Fatal("the doc has no Amendments section")
	}
	if got := sha(protocol); got != v2ProtocolSHA256 {
		t.Errorf("the protocol above \"## Amendments\" changed (SHA-256 %s); it is fixed at the seal", got)
	}
}

// TestV2WorldFrozen: v2's world is v1's, byte for byte, at the registered
// digest.
func TestV2WorldFrozen(t *testing.T) {
	v2, err := os.ReadFile(filepath.Join(v2Dir, "world.json"))
	if err != nil {
		t.Fatal(err)
	}
	v1, err := os.ReadFile(filepath.Join(fixtureDir, "world.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := sha(v2); got != V2WorldSHA256 {
		t.Fatalf("v2 world.json has SHA-256 %s, not the registered %s", got, V2WorldSHA256)
	}
	if !bytes.Equal(v1, v2) {
		t.Fatal("v2 world.json differs from v1's")
	}
}

// baselineFiles are the frozen baseline corpus's files, sorted.
func baselineFiles(t testing.TB) []string {
	t.Helper()
	grammars, err := filepath.Glob(filepath.Join(fixtureDir, "grammars", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	files := append([]string{filepath.Join(fixtureDir, "cases.jsonl"), filepath.Join(fixtureDir, "world.json")}, grammars...)
	sort.Strings(files)
	return files
}

// baselineDigest is the SHA-256 over "<path relative to v1>\n<file SHA-256>\n"
// for each baseline file, sorted by path.
func baselineDigest(t testing.TB) string {
	t.Helper()
	h := sha256.New()
	for _, f := range baselineFiles(t) {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		rel, err := filepath.Rel(fixtureDir, f)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(h, "%s\n%s\n", filepath.ToSlash(rel), sha(b))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// TestV2BaselineFrozen pins the baseline corpus.
func TestV2BaselineFrozen(t *testing.T) {
	if got := baselineDigest(t); got != v2BaselineSHA256 {
		t.Fatalf("the v1 baseline corpus has digest %s, not the registered %s", got, v2BaselineSHA256)
	}
}

var v2Tags = map[string]bool{
	"injection": true, "hidden_target": true, "hidden_node": true, "nonexistent_target": true,
	"nonexistent_node": true, "missing_slot": true, "ambiguous_mention": true, "ambiguous_executor": true,
	"slot_mismatch": true, "unanswerable": true,
}

type slotSchema struct {
	Required   []string `json:"required"`
	Properties map[string]struct {
		Enum []string `json:"enum"`
	} `json:"properties"`
}

// TestV2TestLabelsWellFormed checks each sealed label against the world
// alone: the caller exists, an answerable case's target is visible to its
// caller, its slots are exactly what the target's schema allows with every
// required slot, node slots name visible nodes of a fitting kind, and enum
// slots hold declared values. An abstention names a closed reason and a
// target that exists. Nothing is routed.
func TestV2TestLabelsWellFormed(t *testing.T) {
	cs := sealedV2(t)
	w, err := LoadWorld(os.DirFS(v2Dir))
	if err != nil {
		t.Fatal(err)
	}
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
	seen := map[[2]string]bool{}
	answerable := 0
	for i, c := range cs.all {
		fail := func(format string, args ...any) {
			t.Errorf("case %s: %s", c.ID, fmt.Sprintf(format, args...))
		}
		if want := fmt.Sprintf("v2t-%04d", i+1); c.ID != want {
			fail("ID is not %s", want)
		}
		if seen[[2]string{c.Text, c.Caller}] {
			fail("repeats a (text, caller) pair")
		}
		seen[[2]string{c.Text, c.Caller}] = true
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
		answerable++
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
		default:
			fail("unknown kind %q", e.Kind)
			continue
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
				want = name // service, environment and team slots name their kind
			}
			if kind != want {
				fail("slot %s names a %s node, not a %s", name, kind, want)
			}
		}
	}
	if len(cs.all)-answerable < 1 || answerable < 1 {
		t.Fatalf("degenerate split: %d answerable of %d", answerable, len(cs.all))
	}
	t.Logf("v2 test split: %d cases, %d answerable, %d unanswerable", len(cs.all), answerable, len(cs.all)-answerable)
}
