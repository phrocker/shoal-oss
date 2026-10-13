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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/phrocker/shoal-oss/internal/storage"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
)

func TestLocalFileNameMillis(t *testing.T) {
	for name, want := range map[string]int64{
		"F0001700000000000.rf":          1700000000000,
		"F0001700000000000.parquet":     1700000000000,
		"C0001700000000001-000.rf":      1700000000001,
		"C0001700000000001-012.parquet": 1700000000001,
		"F17000000000001.rf":            17000000000001, // wider than %013d
	} {
		if got, ok := LocalFileNameMillis(name); !ok || got != want {
			t.Errorf("%s = %d,%v want %d", name, got, ok, want)
		}
	}
	for _, name := range []string{
		"", "F", "F123.rf", "F0001700000000000", "F0001700000000000.rfx",
		"C0001700000000001.rf", "C0001700000000001-.rf", "C0001700000000001-00.rf",
		"p~F0001700000000000.rf", ".shoal-import-aa.rf", "G0001700000000000.rf",
		"F0000000000000000.rf", "F1234567890123456789.rf",
	} {
		if _, ok := LocalFileNameMillis(name); ok {
			t.Errorf("%q parsed as a local name", name)
		}
	}
}

// TestMintedInstancePersistsAndRedraws: a tablet records the instance it
// minted each live file under; the instance survives a reopen, is redrawn
// when files.json loses it (an older binary's rewrite) or the directory
// identity changes, and files minted earlier keep their old instance.
func TestMintedInstancePersistsAndRedraws(t *testing.T) {
	backend := memory.New()
	dir := t.TempDir()
	tb, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	writeAndFlush(t, tb, "a")
	first := tb.RFileLineage()
	if len(first) != 1 || first[0].Instance == "" {
		t.Fatalf("flush not minted: %+v", first)
	}
	instance := first[0].Instance
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}

	tb, err = Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	writeAndFlush(t, tb, "b")
	if err := tb.Compact(nil); err != nil {
		t.Fatal(err)
	}
	compacted := tb.RFileLineage()
	if len(compacted) != 1 || compacted[0].Instance != instance {
		t.Fatalf("reopened tablet changed instance: %+v want %s", compacted, instance)
	}

	// A different directory identity (the directory moved or was copied)
	// starts a new instance for new names; the old file keeps its own.
	tb.mu.Lock()
	tb.dirIdentity += "|moved"
	tb.mu.Unlock()
	writeAndFlush(t, tb, "c")
	moved := tb.RFileLineage()
	if len(moved) != 2 || moved[0].Instance != instance || moved[1].Instance == "" || moved[1].Instance == instance {
		t.Fatalf("after directory change: %+v (old instance %s)", moved, instance)
	}
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}

	// An older binary drops instance and minted: nothing is minted any
	// more, and the next name gets a fresh instance.
	manifestPath := filepath.Join(dir, "files.json")
	data, err := storage.ReadAll(context.Background(), backend, manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	if m["minted"] == nil || m["instance"] == nil {
		t.Fatalf("files.json lacks instance/minted: %s", data)
	}
	delete(m, "minted")
	delete(m, "instance")
	delete(m, "instance_dir")
	out, _ := json.Marshal(m)
	backend.Put(manifestPath, out)
	tb, err = Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Close()
	writeAndFlush(t, tb, "d")
	rolled := tb.RFileLineage()
	if len(rolled) != 3 || rolled[0].Instance != "" || rolled[1].Instance != "" {
		t.Fatalf("files minted before the rewrite still carry an instance: %+v", rolled)
	}
	if rolled[2].Instance == "" || rolled[2].Instance == instance || rolled[2].Instance == moved[1].Instance {
		t.Fatalf("new name not under a fresh instance: %+v", rolled)
	}
}

// TestReplacedDirectoryAtSamePathRedrawsInstance: a directory replaced at
// the same path (a copy or restore put in its place, a clone on another
// host) is a different directory, so new names get a new instance even
// though the path and files.json are unchanged.
func TestReplacedDirectoryAtSamePathRedrawsInstance(t *testing.T) {
	backend := memory.New()
	dir := filepath.Join(t.TempDir(), "tablet")
	if platformDirIdentity(t.TempDir()) == "" {
		t.Skip("no device/inode identity on this platform")
	}
	tb, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	writeAndFlush(t, tb, "a")
	instance := tb.RFileLineage()[0].Instance
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(dir, dir+".orig"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	tb, err = Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Close()
	writeAndFlush(t, tb, "b")
	files := tb.RFileLineage()
	if len(files) != 2 || files[0].Instance != instance || files[1].Instance == instance {
		t.Fatalf("replaced directory kept minting under %s: %+v", instance, files)
	}
}

// TestRefreshAdoptedStrayIsNotMinted: a stray "F<ms>.rf" another engine
// shipped into the directory has a local-looking name but was not minted
// here, so it carries no instance (its name is not in this tablet's
// sequence).
func TestRefreshAdoptedStrayIsNotMinted(t *testing.T) {
	backend := memory.New()
	dir := t.TempDir()
	tb, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Close()
	writeAndFlush(t, tb, "a")
	stray := filepath.Join(dir, "F0000000000005.rf")
	data, err := storage.ReadAll(context.Background(), backend, tb.RFiles()[0])
	if err != nil {
		t.Fatal(err)
	}
	backend.Put(stray, data)
	if _, err := tb.RefreshFiles(); err != nil {
		t.Fatal(err)
	}
	files := tb.RFileLineage()
	if len(files) != 2 || files[0].Instance == "" || files[1].Path != stray || files[1].Instance != "" {
		t.Fatalf("refresh lineage %+v", files)
	}
}

// lineageTablet is a bare tablet for registration tests.
func lineageTablet(backend *memory.Backend) *Tablet {
	return &Tablet{dir: "/tablet", backend: backend, obsolete: map[string]struct{}{}}
}

// shipped stages one incoming producer file: nonce makes the staged
// object unique, ms is its producer name, epoch its lineage epoch.
func shipped(backend *memory.Backend, id string, ms int64, epoch string) ImportedFile {
	path := filepath.Join("/tablet", ".shoal-import-"+id+"-"+fmt.Sprint(ms)+"-"+epoch+".rf")
	backend.Put(path, []byte("x"))
	return ImportedFile{
		Path:    path,
		Source:  "src:" + epoch + ":" + id,
		Lineage: ProducerLineage{Producer: "t/t-0000/p", Epoch: epoch, Millis: ms},
	}
}

func register(t *testing.T, tb *Tablet, files ...ImportedFile) []string {
	t.Helper()
	adopted, err := tb.RegisterImmutableFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	return adopted
}

// TestLineageRetiresAbsorbedAndHoldsReplays: absorbed entries with a
// lineage are kept while the producer still lists them, dropped once a
// snapshot of a higher-or-equal name does not, and a replay of the dropped
// file afterwards is held by the mark — never appended.
func TestLineageRetiresAbsorbedAndHoldsReplays(t *testing.T) {
	backend := memory.New()
	tb := lineageTablet(backend)
	const e = "epoch1"
	// Snapshot 1: [F100, F200].
	s1 := []ImportedFile{shipped(backend, "f100", 100, e), shipped(backend, "f200", 200, e)}
	if got := register(t, tb, s1...); len(got) != 2 {
		t.Fatalf("snapshot 1 adopted %d", len(got))
	}
	// The consumer compacts both away (simulated: what Compact records).
	tb.absorbed = map[string]struct{}{}
	tb.absorbedLineage = map[string]ProducerLineage{}
	for _, f := range s1 {
		tb.absorbed[f.Source] = struct{}{}
		tb.absorbedLineage[f.Source] = f.Lineage
	}
	tb.files, tb.sources, tb.sourceLineage = nil, map[string]string{}, map[string]ProducerLineage{}

	// Snapshot 2 still lists F200 (live at the producer) but not F100,
	// which a producer compaction (C300) retired.
	s2 := []ImportedFile{shipped(backend, "c300", 300, e), shipped(backend, "f200", 200, e)}
	s2[1].Source = s1[1].Source
	register(t, tb, s2...)
	if _, ok := tb.absorbed[s1[0].Source]; ok {
		t.Fatal("retired F100 still absorbed explicitly")
	}
	if _, ok := tb.absorbed[s1[1].Source]; !ok {
		t.Fatal("F200, still listed by the producer, was pruned")
	}
	if tb.marks[lineageKey{"t/t-0000/p", e}] != 300 {
		t.Fatalf("mark %v", tb.marks)
	}
	before := append([]string(nil), tb.files...)

	// Replay snapshot 1: F100 is held by the mark, F200 by identity.
	replay := []ImportedFile{shipped(backend, "f100", 100, e), shipped(backend, "f200", 200, e)}
	replay[0].Source, replay[1].Source = s1[0].Source, s1[1].Source
	if got := register(t, tb, replay...); len(got) != 0 {
		t.Fatalf("replay appended %v", got)
	}
	if fmt.Sprint(tb.files) != fmt.Sprint(before) {
		t.Fatalf("replay changed files: %v", baseNames(tb.files))
	}
	for _, f := range replay {
		if _, err := backend.Open(context.Background(), f.Path); err == nil {
			t.Fatalf("redundant replayed object %s kept", f.Path)
		}
	}

	// A file the consumer never saw, below the mark, is one the producer
	// retired before snapshot 2 (F250 between the snapshots): held.
	if got := register(t, tb, shipped(backend, "f250", 250, e)); len(got) != 0 {
		t.Fatalf("retired unseen file appended: %v", got)
	}
	// Above the mark it is new.
	if got := register(t, tb, shipped(backend, "f400", 400, e)); len(got) != 1 {
		t.Fatalf("new file not appended: %v", got)
	}

	// Another epoch of the same producer (recreated or copied directory)
	// has its own mark: its names below 300 are new, not held.
	if got := register(t, tb, shipped(backend, "g100", 100, "epoch2")); len(got) != 1 {
		t.Fatalf("other epoch's file held by this epoch's mark: %v", got)
	}
	// So does a file with no lineage (no producer prefix / epoch).
	plain := shipped(backend, "h100", 100, e)
	plain.Lineage = ProducerLineage{}
	if got := register(t, tb, plain); len(got) != 1 {
		t.Fatalf("file without lineage held by a mark: %v", got)
	}
}

// TestLineageKeepsEntriesItCannotRetire: an absorbed entry is only retired
// by a snapshot of ITS lineage reaching its name. Entries without a
// lineage (no prefix, older producers, legacy content digests) are never
// pruned.
func TestLineageKeepsEntriesItCannotRetire(t *testing.T) {
	backend := memory.New()
	tb := lineageTablet(backend)
	tb.absorbed = map[string]struct{}{
		"content:abc": {}, "no-lineage": {}, "later": {}, "other-epoch": {},
	}
	tb.absorbedLineage = map[string]ProducerLineage{
		"later":       {Producer: "t/t-0000/p", Epoch: "e", Millis: 500},
		"other-epoch": {Producer: "t/t-0000/p", Epoch: "f", Millis: 10},
	}
	for i := 0; i < 50; i++ {
		register(t, tb, shipped(backend, fmt.Sprint("n", i), int64(100+i), "e"))
	}
	for identity := range map[string]struct{}{"content:abc": {}, "no-lineage": {}, "later": {}, "other-epoch": {}} {
		if _, ok := tb.absorbed[identity]; !ok {
			t.Fatalf("%s pruned", identity)
		}
	}
	register(t, tb, shipped(backend, "top", 500, "e"))
	if _, ok := tb.absorbed["later"]; ok {
		t.Fatal("later not retired by a snapshot reaching its name")
	}
}

// TestPublishAndLegacyClaimsRecordLineage: a table's first import
// (PublishImmutableFiles) records its files' lineages and starts the marks
// from them, and a legacy import claimed by a file with a lineage takes
// the lineage too — otherwise those entries could never be retired.
func TestPublishAndLegacyClaimsRecordLineage(t *testing.T) {
	backend := memory.New()
	dir := "/tablet"
	first := shipped(backend, "a", 100, "e")
	if err := PublishImmutableFiles(backend, dir, []ImportedFile{first}); err != nil {
		t.Fatal(err)
	}
	found, err := discoverImmutableFiles(backend, dir, false)
	if err != nil {
		t.Fatal(err)
	}
	if found.sourceLineage[filepath.Base(first.Path)] != first.Lineage {
		t.Fatalf("published lineage %v", found.sourceLineage)
	}
	if found.marks[lineageKey{"t/t-0000/p", "e"}] != 100 {
		t.Fatalf("published marks %v", found.marks)
	}

	legacyLive, legacyGone := importPath('a', "1"), importPath('b', "1")
	gone, _ := legacyContentIdentity(legacyGone)
	putFiles(backend, dir, filepath.Base(legacyLive))
	tb := &Tablet{dir: dir, backend: backend, obsolete: map[string]struct{}{},
		files: []string{legacyLive}, absorbed: map[string]struct{}{gone: {}}}
	claimLive := ImportedFile{Path: importPath('a', "2"), Source: "src:live",
		Lineage: ProducerLineage{Producer: "t/t-0000/p", Epoch: "e", Millis: 200}}
	claimGone := ImportedFile{Path: importPath('b', "2"), Source: "src:gone",
		Lineage: ProducerLineage{Producer: "t/t-0000/p", Epoch: "e", Millis: 300}}
	putFiles(backend, dir, filepath.Base(claimLive.Path), filepath.Base(claimGone.Path))
	if got := register(t, tb, claimLive, claimGone); len(got) != 0 {
		t.Fatalf("legacy claims appended %v", got)
	}
	if tb.sourceLineage[filepath.Base(legacyLive)] != claimLive.Lineage {
		t.Fatalf("live legacy claim lineage %v", tb.sourceLineage)
	}
	if tb.absorbedLineage["src:gone"] != claimGone.Lineage {
		t.Fatalf("absorbed legacy claim lineage %v", tb.absorbedLineage)
	}
}

// TestLegacyClaimBeforeMark: a replayed file below the mark that is also
// the producer file of a legacy (content-digest) entry claims that entry,
// so a later, genuinely new byte-identical file cannot claim it and is
// appended as newest instead of being taken as held.
func TestLegacyClaimBeforeMark(t *testing.T) {
	backend := memory.New()
	dir := "/tablet"
	digest, _ := legacyContentIdentity(importPath('a', "0"))
	tb := &Tablet{dir: dir, backend: backend, obsolete: map[string]struct{}{},
		absorbed: map[string]struct{}{digest: {}},
		marks:    map[lineageKey]int64{{"t/t-0000/p", "e"}: 500}}
	replayed := ImportedFile{Path: importPath('a', "1"), Source: "src:old",
		Lineage: ProducerLineage{Producer: "t/t-0000/p", Epoch: "e", Millis: 100}}
	putFiles(backend, dir, filepath.Base(replayed.Path))
	if got := register(t, tb, replayed); len(got) != 0 {
		t.Fatalf("replay appended %v", got)
	}
	if _, ok := tb.absorbed[digest]; ok {
		t.Fatal("replayed producer file did not claim its legacy entry")
	}
	fresh := ImportedFile{Path: importPath('a', "2"), Source: "src:new",
		Lineage: ProducerLineage{Producer: "t/t-0000/p", Epoch: "e", Millis: 600}}
	putFiles(backend, dir, filepath.Base(fresh.Path))
	if got := register(t, tb, fresh); len(got) != 1 {
		t.Fatalf("new byte-identical file not appended: %v", got)
	}
}

// TestLineageStatePersists: marks and lineages round-trip through
// files.json, and a reopened tablet still holds a replay by its mark.
func TestLineageStatePersists(t *testing.T) {
	backend := memory.New()
	dir := t.TempDir()
	tb, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	ship := func(id string, ms int64) ImportedFile {
		path := filepath.Join(dir, ".shoal-import-"+id+"-"+fmt.Sprint(ms)+".rf")
		backend.Put(path, []byte("x"))
		return ImportedFile{Path: path, Source: "src:" + id,
			Lineage: ProducerLineage{Producer: "t/t-0000/p", Epoch: "e", Millis: ms}}
	}
	register(t, tb, ship("a", 100), ship("b", 200))
	tb.mu.Lock()
	tb.absorbed = map[string]struct{}{"src:z": {}}
	tb.absorbedLineage = map[string]ProducerLineage{"src:z": {Producer: "t/t-0000/p", Epoch: "e", Millis: 300}}
	if err := persistImmutableManifest(backend, dir, tb.state(tb.files, tb.obsolete)); err != nil {
		t.Fatal(err)
	}
	tb.mu.Unlock()
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, Options{Backend: backend})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.marks[lineageKey{"t/t-0000/p", "e"}] != 200 {
		t.Fatalf("marks %v", reopened.marks)
	}
	if len(reopened.sourceLineage) != 2 || reopened.absorbedLineage["src:z"].Millis != 300 {
		t.Fatalf("lineage %v / %v", reopened.sourceLineage, reopened.absorbedLineage)
	}
	if got := register(t, reopened, ship("retired", 150)); len(got) != 0 {
		t.Fatalf("reopened tablet appended a file below its mark: %v", got)
	}
}
