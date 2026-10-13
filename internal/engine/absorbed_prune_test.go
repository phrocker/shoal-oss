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

package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/engine"
	"github.com/phrocker/shoal-oss/internal/storage"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
)

// Tests for #701: files.json absorbed_sources stays bounded, with replay
// idempotence kept by per-producer name lineage marks.

const pruneProducerID = "pa"

func exportAs(t *testing.T, src *engine.Engine, b *memory.Backend, dstDir, producerID string) *engine.RFileExportManifest {
	t.Helper()
	m, err := src.ExportRFiles(context.Background(), overwriteTable, b, engine.RFileExportOptions{
		DestinationRoot: dstDir, EngineVersion: "test", ProducerID: producerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// consumerFilesJSON decodes the consumer tablet's files.json.
func consumerFilesJSON(t *testing.T, b *memory.Backend, dstDir string) map[string]json.RawMessage {
	t.Helper()
	data, err := storage.ReadAll(context.Background(), b,
		filepath.Join(dstDir, overwriteTable, "t-0000", "files.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func absorbedCount(t *testing.T, b *memory.Backend, dstDir string) int {
	t.Helper()
	raw, ok := consumerFilesJSON(t, b, dstDir)["absorbed_sources"]
	if !ok {
		return 0
	}
	var absorbed []string
	if err := json.Unmarshal(raw, &absorbed); err != nil {
		t.Fatal(err)
	}
	return len(absorbed)
}

// openProducer opens a producer engine at dir whose overwrite table's name
// high-water mark starts at highWater (0: the clock), so a test can make a
// recreated producer issue names at or below an earlier one's.
func openProducer(t *testing.T, dir string, highWater int64) *engine.Engine {
	t.Helper()
	eng := openOverwriteEngine(t, dir, nil)
	if highWater == 0 {
		return eng
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	manifest := fmt.Sprintf(`{"version":1,"ordered":true,"name_high_water":%d,"active":[]}`, highWater)
	if err := os.WriteFile(filepath.Join(dir, overwriteTable, "t-0000", "files.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Open(dir, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return eng
}

func importManifest(t *testing.T, dst *engine.Engine, m *engine.RFileExportManifest) {
	t.Helper()
	if err := dst.ImportRFileManifest(context.Background(), m); err != nil {
		t.Fatal(err)
	}
}

func compactConsumer(t *testing.T, dst *engine.Engine) {
	t.Helper()
	if err := dst.Flush(overwriteTable); err != nil {
		t.Fatal(err)
	}
	if err := dst.Compact(overwriteTable, nil); err != nil {
		t.Fatal(err)
	}
}

// TestAbsorbedSourcesBoundedAcrossImportCompactCycles: a producer flushes
// (and now and then compacts) and ships every cycle; the consumer imports
// and compacts every cycle, and reopens now and then. With a ProducerID the
// absorbed set stays bounded by the producer's live files; without one
// (names are not per-producer, so there is no lineage) every compacted
// import keeps its explicit entry, as before. Either way every old manifest
// replayed afterwards is a no-op.
func TestAbsorbedSourcesBoundedAcrossImportCompactCycles(t *testing.T) {
	const cycles = 60
	for _, producerID := range []string{pruneProducerID, ""} {
		t.Run(fmt.Sprintf("producer=%q", producerID), func(t *testing.T) {
			src := openOverwriteEngine(t, filepath.Join(t.TempDir(), "src"), nil)
			defer src.Close()
			b := memory.New()
			dstDir := filepath.Join(t.TempDir(), "dst")
			dst, err := engine.Open(dstDir, engine.Options{Backend: b})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = dst.Close() }()
			reopen := func() {
				if err := dst.Close(); err != nil {
					t.Fatal(err)
				}
				if dst, err = engine.Open(dstDir, engine.Options{Backend: b}); err != nil {
					t.Fatal(err)
				}
			}
			var manifests []*engine.RFileExportManifest
			last := ""
			maxAbsorbed := 0
			for i := 0; i < cycles; i++ {
				last = fmt.Sprintf("v%03d", i)
				putLatest(t, src, "cell", last)
				step := stepFlush
				if i%4 == 3 {
					step = stepCompact
				}
				applyStep(t, src, step)
				m := exportAs(t, src, b, dstDir, producerID)
				manifests = append(manifests, m)
				importManifest(t, dst, m)
				if i > 0 {
					compactConsumer(t, dst)
				}
				if i%15 == 14 {
					reopen()
				}
				if got := readLatestOn(t, dst, "cell"); got != last {
					t.Fatalf("cycle %d: read %q, want %q", i, got, last)
				}
				if n := absorbedCount(t, b, dstDir); n > maxAbsorbed {
					maxAbsorbed = n
				}
			}
			if producerID != "" && maxAbsorbed > 8 {
				t.Fatalf("absorbed_sources reached %d entries over %d cycles, want bounded by the producer's live files", maxAbsorbed, cycles)
			}
			if producerID == "" && absorbedCount(t, b, dstDir) < cycles/2 {
				t.Fatalf("without a ProducerID explicit entries must be kept; have %d", absorbedCount(t, b, dstDir))
			}
			before := absorbedCount(t, b, dstDir)
			// Replay every old manifest, newest first, then oldest first.
			for pass := 0; pass < 2; pass++ {
				for j := range manifests {
					m := manifests[len(manifests)-1-j]
					if pass == 1 {
						m = manifests[j]
					}
					importManifest(t, dst, m)
					if got := readLatestOn(t, dst, "cell"); got != last {
						t.Fatalf("replay of manifest %d: read %q, want %q (stale read)", len(manifests)-1-j, got, last)
					}
				}
				reopen()
			}
			if got := absorbedCount(t, b, dstDir); got > before {
				t.Fatalf("replays grew absorbed_sources from %d to %d", before, got)
			}
			versions := readLatest(t, dst, "cell", cycles*4)
			if len(versions) == 0 || versions[0] != last {
				t.Fatalf("versions %v", versions)
			}
		})
	}
}

// TestUnimportedOldManifestOfRetiredFilesIsHeld: a manifest the consumer
// never imported, whose files the producer has since compacted away, adds
// nothing when it arrives after a newer snapshot: its cells are already
// here in the producer's compaction output.
func TestUnimportedOldManifestOfRetiredFilesIsHeld(t *testing.T) {
	src := openOverwriteEngine(t, filepath.Join(t.TempDir(), "src"), nil)
	defer src.Close()
	b := memory.New()
	dstDir := filepath.Join(t.TempDir(), "dst")
	dst, err := engine.Open(dstDir, engine.Options{Backend: b})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	putLatest(t, src, "cell", "old")
	applyStep(t, src, stepFlush)
	stale := exportAs(t, src, b, dstDir, pruneProducerID)
	putLatest(t, src, "cell", "new")
	applyStep(t, src, stepCompact)
	importManifest(t, dst, exportAs(t, src, b, dstDir, pruneProducerID))
	importManifest(t, dst, stale)
	if got := readLatestOn(t, dst, "cell"); got != "new" {
		t.Fatalf("late arrival of a retired file: read %q, want new", got)
	}
}

// TestRecreatedProducerReissuedNamesNotDropped: the producer's data
// directory is recreated from empty and issues exactly the names it issued
// before (the clock-behind case #693 documents). The new directory has a
// new name epoch, so its files are new to the consumer, not held by the
// old epoch's mark.
func TestRecreatedProducerReissuedNamesNotDropped(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "src")
	highWater := time.Now().Add(time.Hour).UnixMilli()
	src := openProducer(t, srcDir, highWater)
	b := memory.New()
	dstDir := filepath.Join(t.TempDir(), "dst")
	dst, err := engine.Open(dstDir, engine.Options{Backend: b})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	var firstNames []string
	for i := 0; i < 4; i++ {
		putLatest(t, src, "cell", fmt.Sprintf("old%d", i))
		applyStep(t, src, stepFlush)
		m := exportAs(t, src, b, dstDir, pruneProducerID)
		if i == 0 {
			for _, f := range m.RFiles {
				firstNames = append(firstNames, f.RelativePath)
			}
		}
		importManifest(t, dst, m)
		compactConsumer(t, dst)
	}
	applyStep(t, src, stepCompact)
	importManifest(t, dst, exportAs(t, src, b, dstDir, pruneProducerID))
	compactConsumer(t, dst)
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(srcDir); err != nil {
		t.Fatal(err)
	}

	src = openProducer(t, srcDir, highWater)
	defer src.Close()
	putLatest(t, src, "cell", "fresh")
	applyStep(t, src, stepFlush)
	m := exportAs(t, src, b, dstDir, pruneProducerID)
	if len(m.RFiles) != 1 || m.RFiles[0].RelativePath != firstNames[0] {
		t.Fatalf("premise: recreated producer should reissue %v, shipped %+v", firstNames, m.RFiles)
	}
	importManifest(t, dst, m)
	if got := readLatestOn(t, dst, "cell"); got != "fresh" {
		t.Fatalf("recreated producer's file dropped: read %q, want fresh", got)
	}
}

// TestProducerEpochAcrossReopenAndMove: a reopened producer keeps its name
// epoch; a moved one mints new names under a new epoch while its old files
// keep theirs, so a consumer that pruned their entries still holds them.
// Exports without a ProducerID carry no epoch.
func TestProducerEpochAcrossReopenAndMove(t *testing.T) {
	base := t.TempDir()
	srcDir := filepath.Join(base, "src")
	src := openOverwriteEngine(t, srcDir, nil)
	b := memory.New()
	dstDir := filepath.Join(t.TempDir(), "dst")
	dst, err := engine.Open(dstDir, engine.Options{Backend: b})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	putLatest(t, src, "cell", "a")
	applyStep(t, src, stepFlush)
	first := exportAs(t, src, b, dstDir, pruneProducerID)
	if len(first.RFiles) != 1 || first.RFiles[0].ProducerEpoch == "" {
		t.Fatalf("minted file exported without an epoch: %+v", first.RFiles)
	}
	epoch := first.RFiles[0].ProducerEpoch
	if plain := exportAs(t, src, b, filepath.Join(t.TempDir(), "plain"), ""); plain.RFiles[0].ProducerEpoch != "" {
		t.Fatalf("export without a ProducerID carries an epoch")
	}
	importManifest(t, dst, first)
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	src, err = engine.Open(srcDir, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	putLatest(t, src, "cell", "b")
	applyStep(t, src, stepFlush)
	m := exportAs(t, src, b, dstDir, pruneProducerID)
	if len(m.RFiles) != 2 || m.RFiles[0].ProducerEpoch != epoch || m.RFiles[1].ProducerEpoch != epoch {
		t.Fatalf("reopen changed the epoch: %+v (want %s)", m.RFiles, epoch)
	}
	importManifest(t, dst, m)
	putLatest(t, dst, "cell", "local")
	compactConsumer(t, dst)

	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(base, "moved")
	if err := os.Rename(srcDir, moved); err != nil {
		t.Fatal(err)
	}
	src, err = engine.Open(moved, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	putLatest(t, src, "other", "x")
	applyStep(t, src, stepFlush)
	m = exportAs(t, src, b, dstDir, pruneProducerID)
	if len(m.RFiles) != 3 || m.RFiles[0].ProducerEpoch != epoch || m.RFiles[1].ProducerEpoch != epoch ||
		m.RFiles[2].ProducerEpoch == "" || m.RFiles[2].ProducerEpoch == epoch {
		t.Fatalf("after a move: %+v (old epoch %s)", m.RFiles, epoch)
	}
	importManifest(t, dst, m)
	if got := readLatestOn(t, dst, "cell"); got != "local" {
		t.Fatalf("re-import after the producer moved = %q, want local", got)
	}
}

// TestProducerRollbackKeepsLiveFilesHeld: an absorbed entry is only pruned
// once the producer has retired the file. A producer rolled back to an older
// binary loses its epoch (files.json drops instance and minted) and
// re-exports its still-live files without one; the consumer has to match
// them by identity, so it must not have pruned them — not when a snapshot
// still listed them, nor when an older snapshot (replayed) predates them.
func TestProducerRollbackKeepsLiveFilesHeld(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "src")
	src := openOverwriteEngine(t, srcDir, nil)
	b := memory.New()
	dstDir := filepath.Join(t.TempDir(), "dst")
	dst, err := engine.Open(dstDir, engine.Options{Backend: b})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	putLatest(t, src, "cell", "a")
	applyStep(t, src, stepFlush)
	putLatest(t, src, "cell", "b")
	applyStep(t, src, stepFlush)
	first := exportAs(t, src, b, dstDir, pruneProducerID)
	importManifest(t, dst, first)
	putLatest(t, dst, "cell", "local1")
	compactConsumer(t, dst)
	putLatest(t, src, "cell", "c")
	applyStep(t, src, stepFlush)
	importManifest(t, dst, exportAs(t, src, b, dstDir, pruneProducerID))
	putLatest(t, dst, "cell", "local2")
	compactConsumer(t, dst)
	importManifest(t, dst, first) // replay: predates the third file
	if got := readLatestOn(t, dst, "cell"); got != "local2" {
		t.Fatalf("before rollback = %q", got)
	}

	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(srcDir, overwriteTable, "t-0000", "files.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"instance", "instance_dir", "minted"} {
		delete(m, k)
	}
	out, _ := json.Marshal(m)
	if err := os.WriteFile(manifestPath, out, 0o644); err != nil {
		t.Fatal(err)
	}
	src, err = engine.Open(srcDir, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	rolled := exportAs(t, src, b, dstDir, pruneProducerID)
	for _, f := range rolled.RFiles {
		if f.ProducerEpoch != "" {
			t.Fatalf("premise: rolled-back producer still exports an epoch: %+v", f)
		}
	}
	importManifest(t, dst, rolled)
	if got := readLatestOn(t, dst, "cell"); got != "local2" {
		t.Fatalf("re-export after a producer rollback = %q, want local2 (stale read)", got)
	}
}

// TestIncrementalSyncReshipsOnEpochChange: a shipped file is reused by path
// only under the epoch it was shipped with. Changing the stamp changes the
// bytes and the epoch, so the file is shipped again rather than listed
// under the new epoch with the old bytes.
func TestIncrementalSyncReshipsOnEpochChange(t *testing.T) {
	ctx := context.Background()
	src := openOverwriteEngine(t, filepath.Join(t.TempDir(), "src"), nil)
	defer src.Close()
	putLatest(t, src, "cell", "a")
	applyStep(t, src, stepFlush)
	b := memory.New()
	opts := engine.RFileExportOptions{DestinationRoot: "/dst", ProducerID: pruneProducerID, StampVisibilityLabel: "one"}
	first, err := src.ExportRFilesIncremental(ctx, overwriteTable, b, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	same, err := src.ExportRFilesIncremental(ctx, overwriteTable, b, opts, first.State)
	if err != nil {
		t.Fatal(err)
	}
	if len(same.Skipped) != 1 || same.Manifest.RFiles[0].ProducerEpoch != first.Manifest.RFiles[0].ProducerEpoch {
		t.Fatalf("unchanged tick: skipped %v, epochs %q/%q", same.Skipped,
			first.Manifest.RFiles[0].ProducerEpoch, same.Manifest.RFiles[0].ProducerEpoch)
	}
	opts.StampVisibilityLabel = "two"
	changed, err := src.ExportRFilesIncremental(ctx, overwriteTable, b, opts, same.State)
	if err != nil {
		t.Fatal(err)
	}
	if len(changed.Uploaded) != 1 || changed.Manifest.RFiles[0].ProducerEpoch == first.Manifest.RFiles[0].ProducerEpoch ||
		changed.Manifest.RFiles[0].SHA256 == first.Manifest.RFiles[0].SHA256 {
		t.Fatalf("stamp change: uploaded %v, entry %+v", changed.Uploaded, changed.Manifest.RFiles[0])
	}
}

// pruneModelSnapshot is one export as the model sees it.
type pruneModelSnapshot struct {
	manifest *engine.RFileExportManifest
	ids      []string // producer identity (relative path + sha256) per file
	values   []string // the cell value each file resolves to
	seq      int      // export order within the producer instance
	instance int
}

func (s *pruneModelSnapshot) lists(id string) bool {
	for _, have := range s.ids {
		if have == id {
			return true
		}
	}
	return false
}

// TestAbsorbedPruningRandomizedModel drives a producer and a consumer
// through random flushes, compactions, exports, imports of the latest and
// of old manifests, consumer-local writes, reopens and producer
// recreations (with names reissued below the old epoch's mark), and checks
// every read against a model:
//
//   - an incoming producer file is appended as newest (latest arrival)
//     unless the consumer registered it before, or it is superseded: a
//     snapshot of the same producer instance taken after the file first
//     appeared, which no longer lists it, was registered;
//   - a consumer compaction collapses everything into one file.
//
// It also checks the absorbed set stays bounded.
func TestAbsorbedPruningRandomizedModel(t *testing.T) {
	seeds := 12
	if testing.Short() {
		seeds = 6
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		seed := seed
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			runPruneModel(t, seed, 110)
		})
	}
}

func runPruneModel(t *testing.T, seed int64, ops int) {
	rng := rand.New(rand.NewSource(seed))
	base := t.TempDir()
	srcDir := filepath.Join(base, "src")
	futureMark := time.Now().Add(time.Hour).UnixMilli()
	instance := 0
	src := openProducer(t, srcDir, futureMark)
	defer func() { _ = src.Close() }()
	b := memory.New()
	dstDir := filepath.Join(base, "dst")
	dst, err := engine.Open(dstDir, engine.Options{Backend: b})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dst.Close() }()

	var (
		producerFiles []string // model of the producer's live files: values, oldest first
		snapshots     []*pruneModelSnapshot
		seq           int
		firstSeq      = map[string]int{}
		consumer      []string // model of the consumer's live files: values
		registered    = map[string]bool{}
		regSnaps      []*pruneModelSnapshot
		writes        int
		tableExists   bool
		log           []string
	)
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("seed %d after %v: %s", seed, log, fmt.Sprintf(format, args...))
	}
	write := func(eng *engine.Engine, prefix string) string {
		writes++
		v := fmt.Sprintf("%s%d", prefix, writes)
		putLatest(t, eng, "cell", v)
		return v
	}
	export := func() {
		m := exportAs(t, src, b, dstDir, pruneProducerID)
		if len(m.RFiles) != len(producerFiles) {
			fail("export lists %d files, model %d", len(m.RFiles), len(producerFiles))
		}
		seq++
		s := &pruneModelSnapshot{manifest: m, seq: seq, instance: instance}
		for i, f := range m.RFiles {
			id := f.RelativePath + "\x00" + f.SHA256
			s.ids = append(s.ids, id)
			s.values = append(s.values, producerFiles[i])
			if _, ok := firstSeq[id]; !ok {
				firstSeq[id] = seq
			}
		}
		snapshots = append(snapshots, s)
	}
	superseded := func(id string, inst int) bool {
		for _, r := range regSnaps {
			if r.instance == inst && r.seq > firstSeq[id] && !r.lists(id) {
				return true
			}
		}
		return false
	}
	importSnap := func(s *pruneModelSnapshot) {
		importManifest(t, dst, s.manifest)
		tableExists = true
		for i, id := range s.ids {
			if registered[id] || superseded(id, s.instance) {
				continue
			}
			consumer = append(consumer, s.values[i])
		}
		for _, id := range s.ids {
			registered[id] = true
		}
		regSnaps = append(regSnaps, s)
		if n, limit := absorbedCount(t, b, dstDir), 8*(instance+2); n > limit {
			fail("absorbed_sources has %d entries, want at most %d", n, limit)
		}
	}
	check := func() {
		if len(consumer) == 0 {
			return
		}
		if got, want := readLatestOn(t, dst, "cell"), consumer[len(consumer)-1]; got != want {
			fail("consumer read %q, model %q (model files %v)", got, want, consumer)
		}
	}

	producerFiles = append(producerFiles, write(src, "p"))
	applyStep(t, src, stepFlush)
	export()
	importSnap(snapshots[0])
	log = append(log, "init")
	for op := 0; op < ops; op++ {
		r := rng.Intn(100)
		switch {
		case r < 28:
			producerFiles = append(producerFiles, write(src, "p"))
			applyStep(t, src, stepFlush)
			log = append(log, "pflush")
		case r < 36:
			if len(producerFiles) >= 2 {
				if err := src.Compact(overwriteTable, nil); err != nil {
					t.Fatal(err)
				}
				producerFiles = []string{producerFiles[len(producerFiles)-1]}
				log = append(log, "pcompact")
			}
		case r < 52:
			export()
			log = append(log, "export")
		case r < 66:
			importSnap(snapshots[len(snapshots)-1])
			log = append(log, fmt.Sprint("import", len(snapshots)-1))
		case r < 76:
			i := rng.Intn(len(snapshots))
			importSnap(snapshots[i])
			log = append(log, fmt.Sprint("replay", i))
		case r < 84:
			if tableExists {
				consumer = append(consumer, write(dst, "d"))
				if err := dst.Flush(overwriteTable); err != nil {
					t.Fatal(err)
				}
				log = append(log, "dflush")
			}
		case r < 93:
			if tableExists {
				compactConsumer(t, dst)
				if len(consumer) >= 2 {
					consumer = []string{consumer[len(consumer)-1]}
				}
				log = append(log, "dcompact")
			}
		case r < 96:
			if err := dst.Close(); err != nil {
				t.Fatal(err)
			}
			if dst, err = engine.Open(dstDir, engine.Options{Backend: b}); err != nil {
				t.Fatal(err)
			}
			log = append(log, "dreopen")
		case r < 98:
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			if src, err = engine.Open(srcDir, engine.Options{}); err != nil {
				t.Fatal(err)
			}
			log = append(log, "preopen")
		default:
			if instance >= 2 {
				continue
			}
			// Recreate the producer from empty, issuing names below the
			// old instance's (all of which sit above futureMark).
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(srcDir); err != nil {
				t.Fatal(err)
			}
			instance++
			src = openProducer(t, srcDir, futureMark-int64(400*instance))
			producerFiles = nil
			producerFiles = append(producerFiles, write(src, "p"))
			applyStep(t, src, stepFlush)
			log = append(log, "precreate")
		}
		check()
	}
}
