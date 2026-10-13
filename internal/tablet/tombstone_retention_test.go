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
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/cclient"
	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/iterrt"
	"github.com/phrocker/shoal-oss/internal/storage"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
)

// Tests for exported-tombstone retention (#708; see tombstone_retention.go).

const testRetention = 8 * time.Hour

// withExportClock pins the export clock to *now for the test.
func withExportClock(t *testing.T, now *int64) {
	t.Helper()
	saved := exportClock
	exportClock = func() int64 { return *now }
	t.Cleanup(func() { exportClock = saved })
}

func openRetaining(t *testing.T, dir string, backend storage.Backend) *Tablet {
	t.Helper()
	tb, err := Open(dir, Options{Backend: backend, TombstoneRetention: testRetention})
	if err != nil {
		t.Fatal(err)
	}
	return tb
}

func putRow(t *testing.T, tb *Tablet, row string, ts int64) {
	t.Helper()
	m, err := cclient.NewMutation([]byte(row))
	if err != nil {
		t.Fatal(err)
	}
	m.Put([]byte("cf"), []byte("cq"), nil, ts, []byte("v"))
	if err := tb.Write([]*cclient.Mutation{m}); err != nil {
		t.Fatal(err)
	}
}

func deleteRow(t *testing.T, tb *Tablet, row string, ts int64) {
	t.Helper()
	m, err := cclient.NewMutation([]byte(row))
	if err != nil {
		t.Fatal(err)
	}
	m.Delete([]byte("cf"), []byte("cq"), nil, ts)
	if err := tb.Write([]*cclient.Mutation{m}); err != nil {
		t.Fatal(err)
	}
}

func flushTablet(t *testing.T, tb *Tablet) {
	t.Helper()
	if err := tb.Flush(); err != nil {
		t.Fatal(err)
	}
}

func compactTablet(t *testing.T, tb *Tablet) {
	t.Helper()
	if err := tb.Compact(nil); err != nil {
		t.Fatal(err)
	}
}

func exportTablet(t *testing.T, tb *Tablet) {
	t.Helper()
	if _, err := tb.ExportLineage(); err != nil {
		t.Fatal(err)
	}
}

// tombstoneRows returns the rows the tablet holds a tombstone for, and
// whether any live cell is visible after deletes are applied.
func tombstoneRows(t *testing.T, tb *Tablet) (map[string]bool, bool) {
	t.Helper()
	env := iterrt.IteratorEnvironment{Scope: iterrt.ScopeScan}
	src, closeAll, err := tb.Source(env)
	if err != nil {
		t.Fatal(err)
	}
	defer closeAll()
	if err := src.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
		t.Fatal(err)
	}
	rows := map[string]bool{}
	for src.HasTop() {
		if src.GetTopKey().Deleted {
			rows[string(src.GetTopKey().Row)] = true
		}
		if err := src.Next(); err != nil {
			t.Fatal(err)
		}
	}
	src2, close2, err := tb.Source(env)
	if err != nil {
		t.Fatal(err)
	}
	defer close2()
	del := iterrt.NewDeletingIterator()
	if err := del.Init(src2, map[string]string{iterrt.DeletingOptionPropagate: "false"}, env); err != nil {
		t.Fatal(err)
	}
	if err := del.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
		t.Fatal(err)
	}
	return rows, del.HasTop()
}

func readManifest(t *testing.T, backend storage.Backend, dir string) immutableManifest {
	t.Helper()
	data, err := storage.ReadAll(context.Background(), backend, filepath.Join(dir, "files.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m immutableManifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// TestExportLineageRecordsDurably: an export records every listed file's
// first export time and the tablet's exported lineage in files.json before
// it returns; a later export keeps the first time; a reopen and a refresh
// keep the record.
func TestExportLineageRecordsDurably(t *testing.T) {
	now := int64(1_000_000)
	withExportClock(t, &now)
	backend := memory.New()
	dir := t.TempDir()
	tb := openRetaining(t, dir, backend)
	putRow(t, tb, "x", 1)
	flushTablet(t, tb)
	lineage, err := tb.ExportLineage()
	if err != nil {
		t.Fatal(err)
	}
	if len(lineage) != 1 {
		t.Fatalf("lineage = %v", lineage)
	}
	name := filepath.Base(lineage[0].Path)
	m := readManifest(t, backend, dir)
	if !m.ExportTracked || !m.ExportedLineage || m.Exported[name] != now {
		t.Fatalf("manifest after export: tracked=%v lineage=%v exported=%v", m.ExportTracked, m.ExportedLineage, m.Exported)
	}
	first := now
	now += 1000
	putRow(t, tb, "y", 2)
	flushTablet(t, tb)
	exportTablet(t, tb)
	m = readManifest(t, backend, dir)
	if m.Exported[name] != first || len(m.Exported) != 2 {
		t.Fatalf("second export: exported=%v, want %s kept at %d", m.Exported, name, first)
	}
	if _, err := tb.RefreshFiles(); err != nil {
		t.Fatal(err)
	}
	if !tb.exportedLineage || tb.exported[name] != first {
		t.Fatalf("refresh lost the record: lineage=%v exported=%v", tb.exportedLineage, tb.exported)
	}
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}
	tb = openRetaining(t, dir, backend)
	defer tb.Close()
	if !tb.exportedLineage || tb.exported[name] != first {
		t.Fatalf("reopen lost the record: lineage=%v exported=%v", tb.exportedLineage, tb.exported)
	}
}

// manifestFailBackend fails writes of files.json while failing is set.
type manifestFailBackend struct {
	*memory.Backend
	failing bool
}

var errManifestWrite = errors.New("manifest write refused")

func (b *manifestFailBackend) Create(ctx context.Context, path string) (storage.Writer, error) {
	if b.failing && filepath.Base(path) == "files.json" {
		return nil, errManifestWrite
	}
	return b.Backend.Create(ctx, path)
}

// TestExportLineageFailsWhenRecordFails: an export whose record cannot be
// written must not list files to ship: a consumer would then hold cells
// whose deletes a compaction could still drop.
func TestExportLineageFailsWhenRecordFails(t *testing.T) {
	backend := &manifestFailBackend{Backend: memory.New()}
	tb := openRetaining(t, t.TempDir(), backend)
	defer tb.Close()
	putRow(t, tb, "x", 1)
	flushTablet(t, tb)
	backend.failing = true
	if lineage, err := tb.ExportLineage(); !errors.Is(err, errManifestWrite) || lineage != nil {
		t.Fatalf("ExportLineage = %v, %v; want no files and the write error", lineage, err)
	}
	if tb.exportedLineage {
		t.Fatal("an unrecorded export marked the tablet")
	}
	backend.failing = false
	exportTablet(t, tb)
	if !tb.exportedLineage {
		t.Fatal("export after recovery not recorded")
	}
}

// TestFullCompactionKeepsExportedTombstones: once the tablet has been
// exported, a full compaction keeps tombstones in a tombstone-only output,
// pending (no horizon) until that output is exported, then for the
// retention horizon from that export.
func TestFullCompactionKeepsExportedTombstones(t *testing.T) {
	now := int64(1_000_000_000)
	withExportClock(t, &now)
	backend := memory.New()
	dir := t.TempDir()
	tb := openRetaining(t, dir, backend)
	defer func() { _ = tb.Close() }()
	putRow(t, tb, "x", 1)
	putRow(t, tb, "y", 1)
	flushTablet(t, tb)
	exportTablet(t, tb)
	deleteRow(t, tb, "x", 2)
	flushTablet(t, tb)
	compactTablet(t, tb)
	rows, _ := tombstoneRows(t, tb)
	if !rows["x"] {
		t.Fatal("full compaction of an exported tablet dropped the tombstone")
	}
	if got := tb.FileCount(); got != 2 {
		t.Fatalf("FileCount = %d, want the main output plus one tombstone output", got)
	}
	// Pending: however long it waits unexported, it is kept.
	now += 10 * testRetention.Milliseconds()
	compactTablet(t, tb)
	if rows, _ := tombstoneRows(t, tb); !rows["x"] {
		t.Fatal("pending tombstone dropped before it was ever exported")
	}
	// Exported now: kept within the horizon, across a reopen.
	exportTablet(t, tb)
	exportedAt := now
	now = exportedAt + testRetention.Milliseconds() - 1
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}
	tb = openRetaining(t, dir, backend)
	compactTablet(t, tb)
	if rows, _ := tombstoneRows(t, tb); !rows["x"] {
		t.Fatal("tombstone dropped inside the horizon")
	}
	// Past the horizon it is dropped, and the data stays deleted.
	now = exportedAt + testRetention.Milliseconds()
	compactTablet(t, tb)
	rows, live := tombstoneRows(t, tb)
	if len(rows) != 0 {
		t.Fatalf("tombstones %v kept past the horizon", rows)
	}
	if !live {
		t.Fatal("y lost")
	}
	if got := tb.FileCount(); got != 1 {
		t.Fatalf("FileCount = %d after expiry, want 1", got)
	}
}

// TestNeverExportedTabletDropsTombstones: without an export a full
// compaction drops tombstones as before; with retention disabled so does an
// exported one.
func TestNeverExportedTabletDropsTombstones(t *testing.T) {
	for _, tc := range []struct {
		name      string
		export    bool
		retention time.Duration
	}{
		{"never exported", false, 0},
		{"retention disabled", true, TombstoneRetentionDisabled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tb, err := Open(t.TempDir(), Options{Backend: memory.New(), TombstoneRetention: tc.retention})
			if err != nil {
				t.Fatal(err)
			}
			defer tb.Close()
			putRow(t, tb, "x", 1)
			flushTablet(t, tb)
			if tc.export {
				exportTablet(t, tb)
			}
			deleteRow(t, tb, "x", 2)
			flushTablet(t, tb)
			compactTablet(t, tb)
			if rows, live := tombstoneRows(t, tb); len(rows) != 0 || live {
				t.Fatalf("tombstones %v, live %v; want neither", rows, live)
			}
			if got := tb.FileCount(); got != 1 {
				t.Fatalf("FileCount = %d, want 1", got)
			}
		})
	}
}

// TestTombstoneRetentionIsBounded: a steady trickle of deletes, each
// exported and followed by a full compaction, must neither keep tombstones
// forever (one output recording the latest export time would) nor drop one
// inside the horizon, and the number of tombstone outputs stays small.
func TestTombstoneRetentionIsBounded(t *testing.T) {
	now := int64(1_000_000_000)
	withExportClock(t, &now)
	tb := openRetaining(t, t.TempDir(), memory.New())
	defer tb.Close()
	putRow(t, tb, "keep", 1)
	flushTablet(t, tb)
	exportTablet(t, tb)
	step := testRetention.Milliseconds() / 8
	deletedAt := map[string]int64{} // row -> export time of its tombstone
	for i := 0; i < 48; i++ {
		row := fmt.Sprintf("r%02d", i)
		putRow(t, tb, row, 1)
		deleteRow(t, tb, row, 2)
		flushTablet(t, tb)
		exportTablet(t, tb)
		deletedAt[row] = now
		compactTablet(t, tb)
		rows, _ := tombstoneRows(t, tb)
		for r, at := range deletedAt {
			age := now - at
			switch {
			case age < testRetention.Milliseconds() && !rows[r]:
				t.Fatalf("step %d: %s dropped at age %dms, inside the horizon", i, r, age)
			case age > testRetention.Milliseconds()*5/4+step && rows[r]:
				t.Fatalf("step %d: %s kept at age %dms, past the bound", i, r, age)
			}
		}
		if files := tb.FileCount(); files > 1+tombstoneBuckets+2 {
			t.Fatalf("step %d: %d files", i, files)
		}
		now += step
	}
}

// downgradeManifest rewrites dir's files.json as an older binary would:
// the export record and the tombstone-free list are dropped.
func downgradeManifest(t *testing.T, backend storage.Backend, dir string) {
	t.Helper()
	m := readManifest(t, backend, dir)
	m.ExportTracked, m.Exported, m.ExportedLineage, m.TombstoneFree = false, nil, false, nil
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAll(context.Background(), backend, filepath.Join(dir, "files.json"), data); err != nil {
		t.Fatal(err)
	}
}

// liveRows returns the rows a delete-aware read sees.
func liveRows(t *testing.T, tb *Tablet) map[string]bool {
	t.Helper()
	env := iterrt.IteratorEnvironment{Scope: iterrt.ScopeScan}
	src, closeAll, err := tb.Source(env)
	if err != nil {
		t.Fatal(err)
	}
	defer closeAll()
	del := iterrt.NewDeletingIterator()
	if err := del.Init(src, map[string]string{iterrt.DeletingOptionPropagate: "false"}, env); err != nil {
		t.Fatal(err)
	}
	if err := del.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
		t.Fatal(err)
	}
	rows := map[string]bool{}
	for del.HasTop() {
		rows[string(del.GetTopKey().Row)] = true
		if err := del.Next(); err != nil {
			t.Fatal(err)
		}
	}
	return rows
}

func writeLatest(t *testing.T, tb *Tablet, row string, del bool) {
	t.Helper()
	m, err := cclient.NewMutation([]byte(row))
	if err != nil {
		t.Fatal(err)
	}
	if del {
		m.DeleteLatest([]byte("cf"), []byte("cq"), nil)
	} else {
		m.PutLatest([]byte("cf"), []byte("cq"), nil, []byte("v"))
	}
	if err := tb.Write([]*cclient.Mutation{m}); err != nil {
		t.Fatal(err)
	}
}

// TestUpgradeFromUntrackedManifest: a files.json from an older binary (or
// none) does not say what was exported. The files it lists are treated as
// exported when first read, so their tombstones are kept for one horizon;
// the tablet itself is NOT marked exported, so for a tablet that never
// exports (a store, a pure consumer) data written after the upgrade behaves
// exactly as before: its deletes are dropped at the next full compaction and
// a re-put after a compacted DeleteLatest is visible. A real producer is
// marked by its first export and keeps them as usual.
func TestUpgradeFromUntrackedManifest(t *testing.T) {
	for _, kind := range []string{"never exported", "pure consumer", "producer"} {
		t.Run(kind, func(t *testing.T) {
			now := int64(1_000_000_000)
			withExportClock(t, &now)
			backend := memory.New()
			dir := t.TempDir()
			tb := openRetaining(t, dir, backend)
			if kind == "pure consumer" {
				// The legacy tombstone arrived in an import.
				src := openRetaining(t, t.TempDir(), backend)
				putRow(t, src, "x", 1)
				deleteRow(t, src, "x", 2)
				flushTablet(t, src)
				imported := filepath.Join(dir, importNamePrefix+"legacy.rf")
				backend.Put(imported, mustReadAll(t, backend, src.RFiles()[0]))
				_ = src.Close()
				if _, err := tb.RegisterImmutableFiles([]ImportedFile{{Path: imported, Source: "p:x"}}); err != nil {
					t.Fatal(err)
				}
			} else {
				putRow(t, tb, "x", 1)
				flushTablet(t, tb)
				deleteRow(t, tb, "x", 2)
				flushTablet(t, tb)
			}
			if err := tb.Close(); err != nil {
				t.Fatal(err)
			}
			downgradeManifest(t, backend, dir)

			tb = openRetaining(t, dir, backend)
			defer func() { _ = tb.Close() }()
			if tb.exportedLineage {
				t.Fatal("an untracked tablet was marked exported")
			}
			if kind == "producer" {
				exportTablet(t, tb)
			}
			putRow(t, tb, "q1", 1)
			flushTablet(t, tb)
			deleteRow(t, tb, "q1", 2)
			flushTablet(t, tb)
			writeLatest(t, tb, "r", false)
			writeLatest(t, tb, "r", true)
			flushTablet(t, tb)
			compactTablet(t, tb)
			rows, _ := tombstoneRows(t, tb)
			if !rows["x"] {
				t.Fatal("the legacy tombstone (possibly exported) was dropped at once")
			}
			if exported := kind == "producer"; rows["q1"] != exported || rows["r"] != exported {
				t.Fatalf("post-upgrade tombstones %v; kept = %v, want %v", rows, rows["q1"], exported)
			}
			// A re-put after the compacted DeleteLatest is visible unless the
			// tablet really exported (then the kept tombstone shadows it).
			writeLatest(t, tb, "r", false)
			if visible := liveRows(t, tb)["r"]; visible == (kind == "producer") {
				t.Fatalf("re-put visible = %v", visible)
			}
			// After one horizon nothing pending-forever remains on a tablet
			// that never exports; a producer keeps only what it has not yet
			// exported.
			for i := 0; i < 5; i++ {
				now += 30 * 24 * time.Hour.Milliseconds()
				if kind == "producer" {
					exportTablet(t, tb)
				}
				compactTablet(t, tb)
			}
			rows, _ = tombstoneRows(t, tb)
			if kind != "producer" && len(rows) != 0 {
				t.Fatalf("tombstones %v kept 150 days after the upgrade", rows)
			}
			if kind == "producer" && len(rows) != 0 {
				t.Fatalf("producer kept %v past the horizon of their export", rows)
			}
		})
	}
}

// TestMultiRunCompactionOutputsStartPending: a multi-run compaction (mixed
// embedding states) is not full, so it keeps every tombstone in its outputs;
// the outputs do not carry their inputs' export times, so the tombstones
// start pending and are kept until exported again.
func TestMultiRunCompactionOutputsStartPending(t *testing.T) {
	now := int64(1_000_000_000)
	withExportClock(t, &now)
	tb := openRetaining(t, t.TempDir(), memory.New())
	defer tb.Close()
	a, b := embeddingspace.Has("space-a"), embeddingspace.Has("space-b")
	for _, state := range []embeddingspace.FileState{a, b} {
		if err := tb.SetDefaultEmbedding(state); err != nil {
			t.Fatal(err)
		}
		putRow(t, tb, "x"+state.String(), 1)
		deleteRow(t, tb, "d"+state.String(), 2)
		flushTablet(t, tb)
	}
	exportTablet(t, tb)
	if len(tb.exported) != 2 {
		t.Fatalf("premise: exported = %v", tb.exported)
	}
	compactTablet(t, tb)
	if got := tb.FileCount(); got != 2 {
		t.Fatalf("premise: multi-run compaction wrote %d files, want 2", got)
	}
	if len(tb.exported) != 0 {
		t.Fatalf("outputs carry export times %v; want pending", tb.exported)
	}
	if len(tb.tombstoneFree) != 0 {
		t.Fatalf("outputs holding tombstones recorded tombstone-free: %v", tb.tombstoneFree)
	}
	if rows, _ := tombstoneRows(t, tb); len(rows) != 2 {
		t.Fatalf("multi-run compaction kept tombstones %v, want both", rows)
	}
}

// TestTombstoneFreeRecordsOnlyFilesWithoutTombstones: files.json lists as
// tombstone-free exactly the files written without one — a flush of a
// memtable without deletes and the main output of a full compaction — and
// not a flush with a delete, a tombstone output, or an import. The list
// survives a reopen.
func TestTombstoneFreeRecordsOnlyFilesWithoutTombstones(t *testing.T) {
	backend := memory.New()
	dir := t.TempDir()
	tb := openRetaining(t, dir, backend)
	free := func() map[string]bool {
		t.Helper()
		out := map[string]bool{}
		for _, path := range tb.RFiles() {
			_, ok := tb.tombstoneFree[filepath.Base(path)]
			out[filepath.Base(path)] = ok
		}
		return out
	}
	putRow(t, tb, "x", 1)
	flushTablet(t, tb)
	deleteRow(t, tb, "x", 2)
	flushTablet(t, tb)
	files := tb.RFiles()
	got := free()
	if !got[filepath.Base(files[0])] || got[filepath.Base(files[1])] {
		t.Fatalf("after flushes: %v; want only the delete-free flush", got)
	}
	exportTablet(t, tb)
	compactTablet(t, tb)
	files = tb.RFiles()
	if len(files) != 2 {
		t.Fatalf("premise: %d files", len(files))
	}
	got = free()
	if !got[filepath.Base(files[0])] || got[filepath.Base(files[1])] {
		t.Fatalf("after compaction: %v; want the main output only", got)
	}
	imported := filepath.Join(dir, importNamePrefix+"imp.rf")
	backend.Put(imported, mustReadAll(t, backend, files[1]))
	if _, err := tb.RegisterImmutableFiles([]ImportedFile{{Path: imported, Source: "p:1"}}); err != nil {
		t.Fatal(err)
	}
	if free()[filepath.Base(imported)] {
		t.Fatal("an import was recorded tombstone-free")
	}
	want := free()
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}
	tb = openRetaining(t, dir, backend)
	defer tb.Close()
	if got := free(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("after reopen %v, want %v", got, want)
	}
}

// TestPublishKeepsExportedLineage: publishing an import generation over a
// directory whose earlier generation was exported keeps the directory's
// exported lineage; over a fresh, untracked or never-exported one it does
// not set it.
func TestPublishKeepsExportedLineage(t *testing.T) {
	for _, tc := range []struct {
		name, previous string
		want           bool
	}{
		{"exported", `{"version":1,"ordered":true,"export_tracked":true,"exported_lineage":true,"active":[]}`, true},
		{"untracked with files", `{"version":1,"ordered":true,"active":["F0000000004000.rf"]}`, false},
		{"tracked, never exported", `{"version":1,"ordered":true,"export_tracked":true,"active":["F0000000004000.rf"]}`, false},
		{"none", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := memory.New()
			dir := "/tablet"
			if tc.previous != "" {
				backend.Put(filepath.Join(dir, "files.json"), []byte(tc.previous))
			}
			putFiles(backend, dir, "F0000000004000.rf")
			imported := putFiles(backend, dir, ".shoal-import-aa.rf")
			if err := PublishImmutableFiles(backend, dir, []ImportedFile{{Path: imported[0], Source: "s1"}}); err != nil {
				t.Fatal(err)
			}
			found, err := discoverImmutableFiles(backend, dir, false)
			if err != nil {
				t.Fatal(err)
			}
			if found.exportedLineage != tc.want {
				t.Fatalf("exported lineage = %v, want %v", found.exportedLineage, tc.want)
			}
			if len(found.exported) != 0 {
				t.Fatalf("an import was recorded as exported: %v", found.exported)
			}
		})
	}
}

func mustReadAll(t *testing.T, b storage.Backend, path string) []byte {
	t.Helper()
	data, err := storage.ReadAll(context.Background(), b, path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestRecompactionWithKeptTombstonesIsNoOp: a tablet holding only the main
// output and kept-tombstone outputs of one full compaction has nothing to
// compact while nothing is new and nothing expires; a new file or an
// expiring tombstone output makes the compaction run again.
func TestRecompactionWithKeptTombstonesIsNoOp(t *testing.T) {
	now := int64(1_000_000_000)
	withExportClock(t, &now)
	tb := openRetaining(t, t.TempDir(), memory.New())
	defer tb.Close()
	putRow(t, tb, "x", 1)
	putRow(t, tb, "y", 1)
	flushTablet(t, tb)
	exportTablet(t, tb)
	deleteRow(t, tb, "x", 2)
	flushTablet(t, tb)
	compactTablet(t, tb)
	after := fmt.Sprint(tb.RFiles())
	if len(tb.RFiles()) != 2 {
		t.Fatalf("premise: %v", after)
	}
	compactTablet(t, tb)
	if got := fmt.Sprint(tb.RFiles()); got != after {
		t.Fatalf("idle recompaction rewrote %s as %s", after, got)
	}
	exportTablet(t, tb) // the pending output gets an export time
	compactTablet(t, tb)
	if got := fmt.Sprint(tb.RFiles()); got != after {
		t.Fatalf("recompaction after an export rewrote %s as %s", after, got)
	}
	putRow(t, tb, "z", 1)
	flushTablet(t, tb)
	compactTablet(t, tb)
	if got := fmt.Sprint(tb.RFiles()); got == after || len(tb.RFiles()) != 2 {
		t.Fatalf("a new file was not compacted: %s", got)
	}
	// A compaction output of another engine, shipped into this directory
	// without a ProducerID and adopted by a refresh, is new too, although
	// its name looks like one of ours.
	other := openRetaining(t, t.TempDir(), memory.New())
	putRow(t, other, "w", 1)
	flushTablet(t, other)
	putRow(t, other, "w2", 1)
	flushTablet(t, other)
	compactTablet(t, other)
	stray := filepath.Join(tb.dir, "C0000000000001-000.rf")
	tb.backend.(*memory.Backend).Put(stray, mustReadAll(t, other.backend, other.RFiles()[0]))
	_ = other.Close()
	if _, err := tb.RefreshFiles(); err != nil {
		t.Fatal(err)
	}
	if tb.FileCount() != 3 {
		t.Fatalf("premise: stray not adopted, %v", tb.RFiles())
	}
	compactTablet(t, tb)
	if tb.FileCount() != 2 || !liveRows(t, tb)["w"] {
		t.Fatalf("an adopted stray was not compacted: %v", tb.RFiles())
	}
	if rows, _ := tombstoneRows(t, tb); !rows["x"] {
		t.Fatal("tombstone lost")
	}
	now += testRetention.Milliseconds()
	compactTablet(t, tb)
	if rows, _ := tombstoneRows(t, tb); len(rows) != 0 || tb.FileCount() != 1 {
		t.Fatalf("expiring tombstone output not compacted: %v, %d files", rows, tb.FileCount())
	}
}

// TestUpgradedMultiRunOutputsKeepAssumedExportTime: on an upgraded tablet
// that never exports, a multi-run compaction's outputs carry their inputs'
// assumed export time, so their tombstones still get one horizon instead of
// being dropped as never exported.
func TestUpgradedMultiRunOutputsKeepAssumedExportTime(t *testing.T) {
	now := int64(1_000_000_000)
	withExportClock(t, &now)
	backend := memory.New()
	dir := t.TempDir()
	tb := openRetaining(t, dir, backend)
	a, b := embeddingspace.Has("space-a"), embeddingspace.Has("space-b")
	for _, state := range []embeddingspace.FileState{a, b} {
		if err := tb.SetDefaultEmbedding(state); err != nil {
			t.Fatal(err)
		}
		deleteRow(t, tb, "d"+state.String(), 2)
		flushTablet(t, tb)
	}
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}
	downgradeManifest(t, backend, dir)
	tb = openRetaining(t, dir, backend)
	defer tb.Close()
	assumed := now
	now += 1000
	compactTablet(t, tb)
	if len(tb.exported) != 2 {
		t.Fatalf("multi-run outputs carry %v; want the assumed export time", tb.exported)
	}
	for name, at := range tb.exported {
		if at != assumed {
			t.Fatalf("%s carries %d, want %d", name, at, assumed)
		}
	}
}

// TestRefreshLoadsAnotherWritersExportRecord: RefreshFiles reloads the export
// record and tombstone-free list from files.json, so a tablet handle whose
// directory another handle exported keeps the tombstones that export needs.
func TestRefreshLoadsAnotherWritersExportRecord(t *testing.T) {
	now := int64(1_000_000_000)
	withExportClock(t, &now)
	backend := memory.New()
	dir := t.TempDir()
	reader := openRetaining(t, dir, backend)
	defer reader.Close()
	writer := openRetaining(t, dir, backend)
	putRow(t, writer, "x", 1)
	flushTablet(t, writer)
	exportTablet(t, writer)
	deleteRow(t, writer, "x", 2)
	flushTablet(t, writer)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.RefreshFiles(); err != nil {
		t.Fatal(err)
	}
	if !reader.exportedLineage || len(reader.exported) != 1 || len(reader.tombstoneFree) != 1 {
		t.Fatalf("refresh: lineage=%v exported=%v free=%v", reader.exportedLineage, reader.exported, reader.tombstoneFree)
	}
	compactTablet(t, reader)
	if rows, _ := tombstoneRows(t, reader); !rows["x"] {
		t.Fatal("a refreshed handle dropped the tombstone another handle's export needs")
	}
}
