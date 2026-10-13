// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tablet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/phrocker/shoal-oss/internal/cclient"
	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/iterrt"
	"github.com/phrocker/shoal-oss/internal/storage"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
)

// leafSet expands a lineage into explicit leaf names, for the set model.
func leafSet(l Lineage) map[string]bool {
	out := make(map[string]bool)
	for origin, ranges := range l.Leaves {
		for _, r := range ranges {
			for seq := r[0]; seq <= r[1]; seq++ {
				out[fmt.Sprintf("%s/%d", origin, seq)] = true
			}
		}
	}
	for _, id := range l.IDs {
		out["id:"+id] = true
	}
	return out
}

func randomLineage(rng *rand.Rand) Lineage {
	l := Lineage{Leaves: map[string][]LeafRange{}}
	for _, origin := range []string{"a", "b"} {
		for n := rng.Intn(4); n > 0; n-- {
			lo := uint64(rng.Intn(20))
			l.Leaves[origin] = append(l.Leaves[origin], LeafRange{lo, lo + uint64(rng.Intn(4))})
		}
	}
	for n := rng.Intn(3); n > 0; n-- {
		l.IDs = append(l.IDs, fmt.Sprint(rng.Intn(5)))
	}
	out, ok := l.Normalize()
	if !ok {
		panic("random lineage malformed")
	}
	return out
}

// TestLineageOpsMatchSetModel checks the range-set algebra against explicit
// leaf sets.
func TestLineageOpsMatchSetModel(t *testing.T) {
	rng := rand.New(rand.NewSource(704))
	for i := 0; i < 2000; i++ {
		a, b := randomLineage(rng), randomLineage(rng)
		sa, sb := leafSet(a), leafSet(b)
		union, inter, diff := map[string]bool{}, map[string]bool{}, map[string]bool{}
		for k := range sa {
			union[k] = true
			if sb[k] {
				inter[k] = true
			} else {
				diff[k] = true
			}
		}
		for k := range sb {
			union[k] = true
		}
		if got := leafSet(a.Union(b)); !reflect.DeepEqual(got, union) {
			t.Fatalf("union %v %v = %v", a, b, got)
		}
		if got := leafSet(a.Intersect(b)); !reflect.DeepEqual(got, inter) {
			t.Fatalf("intersect %v %v = %v, want %v", a, b, got, inter)
		}
		if got := leafSet(a.Subtract(b)); !reflect.DeepEqual(got, diff) {
			t.Fatalf("subtract %v %v = %v, want %v", a, b, got, diff)
		}
		if got, want := a.Intersects(b), len(inter) > 0; got != want {
			t.Fatalf("intersects %v %v = %v", a, b, got)
		}
		if got, want := a.Covers(b), len(inter) == len(sb); got != want {
			t.Fatalf("covers %v %v = %v", a, b, got)
		}
		if got, want := a.SameLeaves(b), reflect.DeepEqual(sa, sb); got != want {
			t.Fatalf("same %v %v = %v", a, b, got)
		}
		// Normal form: a union is canonical, so equal sets are equal values.
		u1, u2 := a.Union(b), b.Union(a)
		if !reflect.DeepEqual(u1, u2) {
			t.Fatalf("union not canonical: %v vs %v", u1, u2)
		}
	}
	if _, ok := (Lineage{Leaves: map[string][]LeafRange{"a": {{3, 2}}}}).Normalize(); ok {
		t.Fatal("inverted range accepted")
	}
	if _, ok := (Lineage{IDs: []string{""}}).Normalize(); ok {
		t.Fatal("empty id accepted")
	}
	runs := singleLeaf("o", 1).Union(singleLeaf("o", 2)).Union(singleLeaf("o", 3))
	if runs.Size() != 1 {
		t.Fatalf("consecutive leaves should be one range, got %v", runs)
	}
}

// TestLineageDeriveIsDeterministicAndDisjoint: a stamped export's leaves
// are stable per salt and never equal the source's.
func TestLineageDeriveIsDeterministicAndDisjoint(t *testing.T) {
	l := singleLeaf("o", 1).Union(OpaqueLineage("v1:x"))
	a1, a2, b := l.Derive("A"), l.Derive("A"), l.Derive("B")
	if !a1.SameLeaves(a2) {
		t.Fatal("derive not deterministic")
	}
	if a1.Intersects(l) || a1.Intersects(b) {
		t.Fatalf("derived lineage shares leaves: %v %v %v", l, a1, b)
	}
}

// TestMultiRunCompactionKeepsCrossRunTombstone: a tombstone in one embedding
// run that deletes a cell in another run must survive a mixed-state
// compaction. Each run compacts to its own output, so neither output is the
// tablet's whole data: dropping the tombstone (FullMajorCompaction) in run
// B's output would resurrect the cell run A's output still holds (#704).
func TestMultiRunCompactionKeepsCrossRunTombstone(t *testing.T) {
	tb, err := Open(t.TempDir(), Options{Backend: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Close()
	write := func(state embeddingspace.FileState, mutate func(m *cclient.Mutation)) {
		t.Helper()
		if err := tb.SetDefaultEmbedding(state); err != nil {
			t.Fatal(err)
		}
		m, err := cclient.NewMutation([]byte("row"))
		if err != nil {
			t.Fatal(err)
		}
		mutate(m)
		if err := tb.Write([]*cclient.Mutation{m}); err != nil {
			t.Fatal(err)
		}
		if err := tb.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	a, b := embeddingspace.Has("space-a"), embeddingspace.Has("space-b")
	write(a, func(m *cclient.Mutation) { m.Put([]byte("cf"), []byte("cq"), nil, 1, []byte("doomed")) })
	write(b, func(m *cclient.Mutation) { m.Delete([]byte("cf"), []byte("cq"), nil, 2) })
	visible := func() int {
		t.Helper()
		src, closeAll, err := tb.Source(iterrt.IteratorEnvironment{Scope: iterrt.ScopeScan})
		if err != nil {
			t.Fatal(err)
		}
		defer closeAll()
		del := iterrt.NewDeletingIterator()
		if err := del.Init(src, nil, iterrt.IteratorEnvironment{Scope: iterrt.ScopeScan}); err != nil {
			t.Fatal(err)
		}
		if err := del.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
			t.Fatal(err)
		}
		n := 0
		for del.HasTop() {
			if !del.GetTopKey().Deleted {
				n++
			}
			if err := del.Next(); err != nil {
				t.Fatal(err)
			}
		}
		return n
	}
	if n := visible(); n != 0 {
		t.Fatalf("before compaction %d cells visible, want 0", n)
	}
	if err := tb.Compact(nil); err != nil {
		t.Fatal(err)
	}
	if got := tb.FileCount(); got != 2 {
		t.Fatalf("mixed-state compaction produced %d files, want one per run (2)", got)
	}
	if n := visible(); n != 0 {
		t.Fatalf("after compaction %d cells visible, want 0 (tombstone dropped across runs)", n)
	}
}

// strayFixture opens a tablet under a frozen clock and drops an untracked
// object at the name the clock would give the next file — what an export
// without a ProducerID shipping into this directory looks like.
func strayFixture(t *testing.T, stray string) (*Tablet, *memory.Backend, string) {
	t.Helper()
	withFrozenClock(t, 5000)
	backend := memory.New()
	dir := t.TempDir()
	tb, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tb.Close() })
	path := filepath.Join(dir, stray)
	backend.Put(path, []byte("shipped"))
	return tb, backend, path
}

func objectBytes(t *testing.T, b storage.Backend, path string) []byte {
	t.Helper()
	data, err := storage.ReadAll(context.Background(), b, path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestFlushSkipsStrayObjectName: a flush never writes over an object shipped
// into its directory after Open (#704, low).
func TestFlushSkipsStrayObjectName(t *testing.T) {
	tb, backend, stray := strayFixture(t, fmt.Sprintf("F%013d.rf", 5000))
	writeAndFlush(t, tb, "v")
	if got := objectBytes(t, backend, stray); !bytes.Equal(got, []byte("shipped")) {
		t.Fatal("flush replaced the shipped object")
	}
	if files := tb.RFiles(); len(files) != 1 || files[0] == stray {
		t.Fatalf("files = %v", files)
	}
}

// TestCompactionSkipsStrayObjectName: likewise for a compaction output.
func TestCompactionSkipsStrayObjectName(t *testing.T) {
	tb, backend, _ := strayFixture(t, "unused")
	writeAndFlush(t, tb, "v1")
	writeAndFlush(t, tb, "v2")
	stray := filepath.Join(tb.dir, fmt.Sprintf("C%013d-000.rf", tb.nextNameMillis()))
	backend.Put(stray, []byte("shipped"))
	if err := tb.Compact(nil); err != nil {
		t.Fatal(err)
	}
	if got := objectBytes(t, backend, stray); !bytes.Equal(got, []byte("shipped")) {
		t.Fatal("compaction replaced the shipped object")
	}
	if files := tb.RFiles(); len(files) != 1 || files[0] == stray {
		t.Fatalf("files = %v", files)
	}
}

func readManifest(t *testing.T, b storage.Backend, dir string) immutableManifest {
	t.Helper()
	var m immutableManifest
	if err := json.Unmarshal(objectBytes(t, b, filepath.Join(dir, "files.json")), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// TestFlushAndCompactionRecordLineage: each flush is a leaf of its own, a
// compaction output holds the union of its inputs', and both survive a
// reopen (a new session draws a new origin but keeps recorded lineage).
func TestFlushAndCompactionRecordLineage(t *testing.T) {
	backend := memory.New()
	dir := t.TempDir()
	tb, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	writeAndFlush(t, tb, "a")
	writeAndFlush(t, tb, "b")
	files, err := tb.FilesWithLineage()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].Lineage.Intersects(files[1].Lineage) || files[0].Lineage.IsEmpty() {
		t.Fatalf("flush lineages = %+v", files)
	}
	want := files[0].Lineage.Union(files[1].Lineage)
	if err := tb.Compact(nil); err != nil {
		t.Fatal(err)
	}
	compacted, err := tb.FilesWithLineage()
	if err != nil {
		t.Fatal(err)
	}
	if len(compacted) != 1 || !compacted[0].Lineage.SameLeaves(want) {
		t.Fatalf("compaction lineage = %+v, want %+v", compacted, want)
	}
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, err := reopened.FilesWithLineage()
	if err != nil {
		t.Fatal(err)
	}
	if !after[0].Lineage.SameLeaves(want) {
		t.Fatalf("lineage after reopen = %+v", after)
	}
	writeAndFlush(t, reopened, "c")
	grown, _ := reopened.FilesWithLineage()
	if grown[1].Lineage.Intersects(want) {
		t.Fatal("a new session reissued a leaf name")
	}
}

// TestFilesWithLineagePersistsAssignment: a file with no recorded lineage
// (an older binary's) gets one when it is first exported, and that one is
// written before it is handed out, so a reopen exports the same leaves.
func TestFilesWithLineagePersistsAssignment(t *testing.T) {
	backend := memory.New()
	dir := t.TempDir()
	paths := putFiles(backend, dir, "F0000000000100.rf")
	if err := persistImmutableManifest(backend, dir, immutableState{active: paths}); err != nil {
		t.Fatal(err)
	}
	tb, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	first, err := tb.FilesWithLineage()
	if err != nil {
		t.Fatal(err)
	}
	if first[0].Lineage.IsEmpty() {
		t.Fatal("no lineage assigned")
	}
	if l := readManifest(t, backend, dir).Lineage["F0000000000100.rf"]; !l.SameLeaves(first[0].Lineage) {
		t.Fatalf("assigned lineage not persisted: %+v", l)
	}
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, err := reopened.FilesWithLineage()
	if err != nil {
		t.Fatal(err)
	}
	if !again[0].Lineage.SameLeaves(first[0].Lineage) {
		t.Fatalf("re-export lineage %+v, first %+v", again[0].Lineage, first[0].Lineage)
	}
}

// TestOversizedLineageBecomesFreshLeaf: a lineage past MaxLineageEntries is
// replaced by a fresh leaf, so lineage never grows without bound.
func TestOversizedLineageBecomesFreshLeaf(t *testing.T) {
	tb := &Tablet{}
	big := Lineage{Leaves: map[string][]LeafRange{}}
	for i := 0; i <= MaxLineageEntries; i++ {
		big.Leaves["o"] = append(big.Leaves["o"], LeafRange{uint64(2 * i), uint64(2 * i)})
	}
	got := tb.capLineageLocked(big)
	if got.Size() != 1 || got.Intersects(big) {
		t.Fatalf("capped lineage = %+v", got)
	}
	small := singleLeaf("o", 1)
	if got := tb.capLineageLocked(small); !got.SameLeaves(small) {
		t.Fatal("small lineage replaced")
	}
	// An oversized incoming lineage is not trusted either.
	if got := incomingLineage(ImportedFile{Source: "s", Lineage: &big}, nil); !got.SameLeaves(OpaqueLineage("s")) {
		t.Fatalf("oversized incoming lineage = %+v", got)
	}
}

// registerFixture is a tablet holding the given live files and lineages.
type regFile struct {
	name    string
	lineage Lineage
}

func registerFixture(t *testing.T, live ...regFile) (*Tablet, *memory.Backend) {
	t.Helper()
	backend := memory.New()
	dir := "/tablet"
	tb := &Tablet{dir: dir, backend: backend, obsolete: map[string]struct{}{},
		sources: map[string]string{}, absorbed: map[string]struct{}{}, lineage: map[string]Lineage{}}
	for _, f := range live {
		path := putFiles(backend, dir, f.name)[0]
		tb.files = append(tb.files, path)
		tb.lineage[f.name] = f.lineage
	}
	return tb, backend
}

func incoming(backend *memory.Backend, name, source string, l *Lineage) ImportedFile {
	path := putFiles(backend, "/tablet", name)[0]
	return ImportedFile{Path: path, Source: source, Lineage: l}
}

func leaves(origin string, lo, hi uint64) *Lineage {
	l := Lineage{Leaves: map[string][]LeafRange{origin: {{lo, hi}}}}
	return &l
}

func present(backend *memory.Backend, path string) bool {
	ok, _ := objectExists(backend, path)
	return ok
}

// TestRegisterLineageRules covers each rule of RegisterImmutableFiles for a
// file whose producer identity is new.
func TestRegisterLineageRules(t *testing.T) {
	p1, p2 := *leaves("p", 1, 1), *leaves("p", 2, 2)
	local := *leaves("d", 1, 1)
	cases := []struct {
		name     string
		incoming func(b *memory.Backend) []ImportedFile
		want     []string // live files after, oldest first
		removed  []string // incoming objects not kept
	}{
		{
			name: "all held is held",
			incoming: func(b *memory.Backend) []ImportedFile {
				return []ImportedFile{incoming(b, "c12.rf", "c12", leaves("p", 1, 2))}
			},
			want:    []string{"p1.rf", "local.rf", "p2.rf"},
			removed: []string{"c12.rf"},
		},
		{
			name: "a subset of the held leaves is held",
			incoming: func(b *memory.Backend) []ImportedFile {
				return []ImportedFile{incoming(b, "c1.rf", "c1", leaves("p", 1, 1))}
			},
			want:    []string{"p1.rf", "local.rf", "p2.rf"},
			removed: []string{"c1.rf"},
		},
		{
			name: "none held is appended",
			incoming: func(b *memory.Backend) []ImportedFile {
				return []ImportedFile{incoming(b, "p4.rf", "p4", leaves("p", 4, 4))}
			},
			want: []string{"p1.rf", "local.rf", "p2.rf", "p4.rf"},
		},
		{
			// Known behaviour, pinned so that splitting such a file (the
			// follow-up to #704) changes it deliberately.
			name: "some held is appended newest",
			incoming: func(b *memory.Backend) []ImportedFile {
				return []ImportedFile{incoming(b, "c13.rf", "c13", leaves("p", 1, 3))}
			},
			want: []string{"p1.rf", "local.rf", "p2.rf", "c13.rf"},
		},
		{
			name: "a file held only after an earlier one of the same call is held",
			incoming: func(b *memory.Backend) []ImportedFile {
				return []ImportedFile{
					incoming(b, "p3.rf", "p3", leaves("p", 3, 3)),
					incoming(b, "c13.rf", "c13", leaves("p", 1, 3)),
				}
			},
			want:    []string{"p1.rf", "local.rf", "p2.rf", "p3.rf"},
			removed: []string{"c13.rf"},
		},
		{
			name: "a lineage-less file is new",
			incoming: func(b *memory.Backend) []ImportedFile {
				return []ImportedFile{incoming(b, "old.rf", "old", nil)}
			},
			want: []string{"p1.rf", "local.rf", "p2.rf", "old.rf"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tb, backend := registerFixture(t,
				regFile{"p1.rf", p1}, regFile{"local.rf", local}, regFile{"p2.rf", p2})
			files := tc.incoming(backend)
			if _, err := tb.RegisterImmutableFiles(files); err != nil {
				t.Fatal(err)
			}
			if got := baseNames(tb.RFiles()); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("files = %v, want %v", got, tc.want)
			}
			for _, name := range tc.removed {
				if present(backend, filepath.Join("/tablet", name)) {
					t.Fatalf("redundant %s not removed", name)
				}
			}
			// Re-registering the same manifest changes nothing.
			again := tc.incoming(backend)
			if _, err := tb.RegisterImmutableFiles(again); err != nil {
				t.Fatal(err)
			}
			if got := baseNames(tb.RFiles()); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("re-register files = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRegisterLearnsLineageOfHeldIdentity: a file imported before its
// producer sent lineage is named by its producer identity alone; when a
// later manifest names it with leaves, the tablet learns them, so the
// producer's compaction of that file is recognised as held.
func TestRegisterLearnsLineageOfHeldIdentity(t *testing.T) {
	tb, backend := registerFixture(t)
	old := incoming(backend, "old.rf", "s-old", nil)
	if _, err := tb.RegisterImmutableFiles([]ImportedFile{old}); err != nil {
		t.Fatal(err)
	}
	// A local write after the import.
	tb.files = append(tb.files, putFiles(backend, "/tablet", "local.rf")...)
	tb.lineage["local.rf"] = *leaves("d", 1, 1)
	if _, err := tb.RegisterImmutableFiles([]ImportedFile{incoming(backend, "old2.rf", "s-old", leaves("p", 1, 1))}); err != nil {
		t.Fatal(err)
	}
	before := baseNames(tb.RFiles())
	if _, err := tb.RegisterImmutableFiles([]ImportedFile{incoming(backend, "c.rf", "s-c", leaves("p", 1, 1))}); err != nil {
		t.Fatal(err)
	}
	if got := baseNames(tb.RFiles()); !reflect.DeepEqual(got, before) {
		t.Fatalf("compaction of a held file was added: %v (before %v)", got, before)
	}
}

// TestPublishRecordsLineageAndHoldsRepeats: a first import records each
// file's lineage and, like a merge import, holds a file whose leaves an
// earlier file of the same manifest already holds.
func TestPublishRecordsLineageAndHoldsRepeats(t *testing.T) {
	backend := memory.New()
	dir := "/tablet"
	base := incoming(backend, "c.rf", "c", leaves("p", 1, 2))
	repeat := incoming(backend, "c2.rf", "c2", leaves("p", 2, 2))
	if err := PublishImmutableFiles(backend, dir, []ImportedFile{base, repeat}); err != nil {
		t.Fatal(err)
	}
	m := readManifest(t, backend, dir)
	if !reflect.DeepEqual(m.Active, []string{"c.rf"}) {
		t.Fatalf("active = %v", m.Active)
	}
	if present(backend, repeat.Path) {
		t.Fatal("redundant object kept")
	}
	if l := m.Lineage["c.rf"]; !l.SameLeaves(*leaves("p", 1, 2)) {
		t.Fatalf("lineage = %+v", l)
	}
}
