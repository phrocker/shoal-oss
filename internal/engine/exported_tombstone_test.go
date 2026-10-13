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
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/cclient"
	"github.com/phrocker/shoal-oss/internal/engine"
	"github.com/phrocker/shoal-oss/internal/iterrt"
	"github.com/phrocker/shoal-oss/internal/localwal"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
	"github.com/phrocker/shoal-oss/internal/tablet"
)

// Issue #708: a delete that a producer's full major compaction drops before
// it is ever shipped must still reach a consumer that holds the deleted cell.

func deleteLatest(t *testing.T, eng *engine.Engine, row string) {
	t.Helper()
	m, err := cclient.NewMutation([]byte(row))
	if err != nil {
		t.Fatal(err)
	}
	m.DeleteLatest([]byte("cf"), []byte("cq"), nil)
	if err := eng.Write(overwriteTable, []*cclient.Mutation{m}); err != nil {
		t.Fatal(err)
	}
}

// liveView is what a delete-aware reader sees: row -> newest live value.
func liveView(t *testing.T, eng *engine.Engine) map[string]string {
	t.Helper()
	return liveViewAs(t, eng, nil)
}

// liveViewAs is liveView for a reader holding auths (nil: system context).
func liveViewAs(t *testing.T, eng *engine.Engine, auths [][]byte) map[string]string {
	t.Helper()
	sc, err := eng.Scan(overwriteTable, iterrt.InfiniteRange(), engine.ScanOptions{
		Authorizations: auths,
		Stack: []iterrt.IterSpec{
			{Name: iterrt.IterDeleting, Options: map[string]string{iterrt.DeletingOptionPropagate: "false"}},
			{Name: iterrt.IterVersioning, Options: map[string]string{iterrt.VersioningOption: "1"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()
	out := map[string]string{}
	for sc.Next() {
		out[string(sc.Key().Row)] = string(sc.Value())
		if err := sc.Advance(); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// rawTombstones counts the tombstones a raw scan of eng surfaces.
func rawTombstones(t *testing.T, eng *engine.Engine) int {
	t.Helper()
	sc, err := eng.Scan(overwriteTable, iterrt.InfiniteRange(), engine.ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()
	n := 0
	for sc.Next() {
		if sc.Key().Deleted {
			n++
		}
		if err := sc.Advance(); err != nil {
			t.Fatal(err)
		}
	}
	return n
}

func exportFull(t *testing.T, src *engine.Engine, b *memory.Backend, root string) *engine.RFileExportManifest {
	t.Helper()
	m, err := src.ExportRFiles(context.Background(), overwriteTable, b, engine.RFileExportOptions{
		DestinationRoot: root,
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func importManifest(t *testing.T, dst *engine.Engine, m *engine.RFileExportManifest) {
	t.Helper()
	if err := dst.ImportRFileManifest(context.Background(), m); err != nil {
		t.Fatal(err)
	}
}

func compactTable(t *testing.T, eng *engine.Engine) {
	t.Helper()
	if err := eng.Flush(overwriteTable); err != nil {
		t.Fatal(err)
	}
	if err := eng.Compact(overwriteTable, nil); err != nil {
		t.Fatal(err)
	}
}

// openConsumer opens an engine at root on b, the destination exports to it
// ship into.
func openConsumer(t *testing.T, b *memory.Backend, root string, opts engine.Options) *engine.Engine {
	t.Helper()
	opts.Backend = b
	eng, err := engine.Open(root, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

func wantView(t *testing.T, who string, eng *engine.Engine, want map[string]string) {
	t.Helper()
	if got := liveView(t, eng); !reflect.DeepEqual(got, want) {
		t.Fatalf("%s reads %v, want %v", who, got, want)
	}
}

// TestExportedDeleteSurvivesProducerFullCompaction is the issue's repro: P
// puts x and flushes, D imports it; P deletes x, flushes and compacts (a
// full major compaction); D imports again and must read nothing.
func TestExportedDeleteSurvivesProducerFullCompaction(t *testing.T) {
	b := memory.New()
	root := filepath.Join(t.TempDir(), "dst")
	src := openOverwriteEngine(t, filepath.Join(t.TempDir(), "src"), nil)
	defer src.Close()
	dst := openConsumer(t, b, root, engine.Options{})

	putLatest(t, src, "x", "1")
	applyStep(t, src, stepFlush)
	importManifest(t, dst, exportFull(t, src, b, root))
	wantView(t, "premise: consumer", dst, map[string]string{"x": "1"})

	deleteLatest(t, src, "x")
	applyStep(t, src, stepFlush)
	if err := src.Compact(overwriteTable, nil); err != nil {
		t.Fatal(err)
	}
	wantView(t, "producer", src, map[string]string{})
	importManifest(t, dst, exportFull(t, src, b, root))
	wantView(t, "consumer", dst, map[string]string{})

	// Reopened, the consumer still reads nothing, and so does the producer.
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
	wantView(t, "reopened consumer", openConsumer(t, b, root, engine.Options{}), map[string]string{})
}

// TestIncrementalSyncDeliversDeleteWhoseFlushWasSkipped: incremental sync,
// where the tombstone's flush is never shipped on its own — either no tick
// ran between the delete's flush and the compaction, or a tick ran but the
// lagging consumer skipped that manifest. Also with the producer reopened
// between the export and the delete (the export record is durable), and
// with a tenant-stamped export.
func TestIncrementalSyncDeliversDeleteWhoseFlushWasSkipped(t *testing.T) {
	for _, tc := range []struct {
		name              string
		tickBeforeCompact bool
		reopen            bool
		stamp             string
	}{
		{name: "no tick"},
		{name: "tick not imported", tickBeforeCompact: true},
		{name: "producer reopened", reopen: true},
		{name: "stamped", stamp: "tenantA"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := memory.New()
			root := filepath.Join(t.TempDir(), "dst")
			opts := engine.RFileExportOptions{DestinationRoot: root, ProducerID: "pa", StampVisibilityLabel: tc.stamp}
			srcDir := filepath.Join(t.TempDir(), "src")
			src := openOverwriteEngine(t, srcDir, nil)
			defer func() { _ = src.Close() }()
			dst := openConsumer(t, b, root, engine.Options{})
			var auths [][]byte
			if tc.stamp != "" {
				auths = [][]byte{[]byte(tc.stamp)}
			}
			want := func(who string, eng *engine.Engine, view map[string]string) {
				t.Helper()
				if got := liveViewAs(t, eng, auths); !reflect.DeepEqual(got, view) {
					t.Fatalf("%s reads %v, want %v", who, got, view)
				}
			}

			putLatest(t, src, "x", "1")
			putLatest(t, src, "y", "1")
			applyStep(t, src, stepFlush)
			res := syncTick(t, src, b, opts, nil)
			importManifest(t, dst, res.Manifest)
			want("premise: consumer", dst, map[string]string{"x": "1", "y": "1"})

			if tc.reopen {
				if err := src.Close(); err != nil {
					t.Fatal(err)
				}
				var err error
				if src, err = engine.Open(srcDir, engine.Options{}); err != nil {
					t.Fatal(err)
				}
			}
			deleteLatest(t, src, "x")
			applyStep(t, src, stepFlush)
			state := res.State
			if tc.tickBeforeCompact {
				state = syncTick(t, src, b, opts, state).State // not imported
			}
			if err := src.Compact(overwriteTable, nil); err != nil {
				t.Fatal(err)
			}
			res = syncTick(t, src, b, opts, roundTripState(t, state))
			importManifest(t, dst, res.Manifest)
			want("consumer", dst, map[string]string{"y": "1"})
		})
	}
}

// TestChainedSyncPropagatesDelete: P exports to M, M re-exports to D. P's
// delete is dropped by P's compaction, then by M's; D must still lose x.
// M is itself a producer, so its full compaction keeps the tombstones it
// imported from P.
func TestChainedSyncPropagatesDelete(t *testing.T) {
	ctx := context.Background()
	bm, bd := memory.New(), memory.New()
	rootM := filepath.Join(t.TempDir(), "m")
	rootD := filepath.Join(t.TempDir(), "d")
	src := openOverwriteEngine(t, filepath.Join(t.TempDir(), "src"), nil)
	defer src.Close()
	mid := openConsumer(t, bm, rootM, engine.Options{})
	dst := openConsumer(t, bd, rootD, engine.Options{})
	optsM := engine.RFileExportOptions{DestinationRoot: rootM, ProducerID: "p"}
	optsD := engine.RFileExportOptions{DestinationRoot: rootD, ProducerID: "m"}
	var stateM, stateD *engine.SyncState
	relay := func() {
		t.Helper()
		res, err := src.ExportRFilesIncremental(ctx, overwriteTable, bm, optsM, stateM)
		if err != nil {
			t.Fatal(err)
		}
		stateM = res.State
		importManifest(t, mid, res.Manifest)
		res, err = mid.ExportRFilesIncremental(ctx, overwriteTable, bd, optsD, stateD)
		if err != nil {
			t.Fatal(err)
		}
		stateD = res.State
		importManifest(t, dst, res.Manifest)
	}

	putLatest(t, src, "x", "1")
	putLatest(t, src, "y", "1")
	applyStep(t, src, stepFlush)
	relay()
	wantView(t, "premise: D", dst, map[string]string{"x": "1", "y": "1"})

	deleteLatest(t, src, "x")
	applyStep(t, src, stepFlush)
	compactTable(t, src)
	res, err := src.ExportRFilesIncremental(ctx, overwriteTable, bm, optsM, stateM)
	if err != nil {
		t.Fatal(err)
	}
	stateM = res.State
	importManifest(t, mid, res.Manifest)
	compactTable(t, mid)
	wantView(t, "M", mid, map[string]string{"y": "1"})
	if rawTombstones(t, mid) == 0 {
		t.Fatal("M, which exports, dropped the tombstone its consumer needs")
	}
	relay()
	wantView(t, "D", dst, map[string]string{"y": "1"})
}

// TestConsumerThatDoesNotExportDropsImportedTombstones: a pure consumer
// holds the only copy of what it imported, so its full compaction may drop
// the tombstones it imported (bounded growth). A never-exported producer
// drops them too, as before.
func TestConsumerThatDoesNotExportDropsImportedTombstones(t *testing.T) {
	b := memory.New()
	root := filepath.Join(t.TempDir(), "dst")
	src := openOverwriteEngine(t, filepath.Join(t.TempDir(), "src"), nil)
	defer src.Close()
	dst := openConsumer(t, b, root, engine.Options{})

	putLatest(t, src, "x", "1")
	applyStep(t, src, stepFlush)
	importManifest(t, dst, exportFull(t, src, b, root))
	deleteLatest(t, src, "x")
	compactTable(t, src)
	importManifest(t, dst, exportFull(t, src, b, root))
	if rawTombstones(t, dst) == 0 {
		t.Fatal("premise: the consumer imported the tombstone")
	}
	compactTable(t, dst)
	wantView(t, "consumer", dst, map[string]string{})
	if n := rawTombstones(t, dst); n != 0 {
		t.Fatalf("a consumer that never exports kept %d tombstones", n)
	}

	local := openOverwriteEngine(t, filepath.Join(t.TempDir(), "local"), nil)
	defer local.Close()
	putLatest(t, local, "x", "1")
	applyStep(t, local, stepFlush)
	deleteLatest(t, local, "x")
	compactTable(t, local)
	if n := rawTombstones(t, local); n != 0 {
		t.Fatalf("a never-exported tablet kept %d tombstones", n)
	}
}

// TestTombstoneRetentionHorizonExpires: past the retention horizon a full
// compaction drops an exported tablet's tombstones; disabling retention
// drops them at once (the pre-#708 behaviour).
func TestTombstoneRetentionHorizonExpires(t *testing.T) {
	for _, tc := range []struct {
		name      string
		retention time.Duration
		keptFirst bool
	}{
		{"horizon", 300 * time.Millisecond, true},
		{"disabled", tablet.TombstoneRetentionDisabled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := memory.New()
			root := filepath.Join(t.TempDir(), "dst")
			srcDir := filepath.Join(t.TempDir(), "src")
			src, err := engine.Open(srcDir, engine.Options{TombstoneRetention: tc.retention})
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			if err := src.CreateTable(overwriteTable, engine.TableOptions{}); err != nil {
				t.Fatal(err)
			}
			putLatest(t, src, "x", "1")
			applyStep(t, src, stepFlush)
			exportFull(t, src, b, root)
			deleteLatest(t, src, "x")
			compactTable(t, src)
			if kept := rawTombstones(t, src) > 0; kept != tc.keptFirst {
				t.Fatalf("tombstone kept = %v before any export of it, want %v", kept, tc.keptFirst)
			}
			if !tc.keptFirst {
				return
			}
			// Exported now; within the horizon a compaction still keeps it.
			exportFull(t, src, b, root)
			compactTable(t, src)
			if rawTombstones(t, src) == 0 {
				t.Fatal("tombstone dropped inside the horizon")
			}
			// Past the horizon (plus its bucket width) it is dropped.
			time.Sleep(tc.retention + tc.retention/2 + 50*time.Millisecond)
			compactTable(t, src)
			if n := rawTombstones(t, src); n != 0 {
				t.Fatalf("%d tombstones kept past the horizon", n)
			}
			wantView(t, "producer", src, map[string]string{})
		})
	}
}

// --- randomized export/import model with deletes ---------------------------

func putAt(t *testing.T, eng *engine.Engine, row string, ts int64, value string) {
	t.Helper()
	m, err := cclient.NewMutation([]byte(row))
	if err != nil {
		t.Fatal(err)
	}
	m.Put([]byte("cf"), []byte("cq"), nil, ts, []byte(value))
	if err := eng.Write(overwriteTable, []*cclient.Mutation{m}); err != nil {
		t.Fatal(err)
	}
}

func deleteAt(t *testing.T, eng *engine.Engine, row string, ts int64) {
	t.Helper()
	m, err := cclient.NewMutation([]byte(row))
	if err != nil {
		t.Fatal(err)
	}
	m.Delete([]byte("cf"), []byte("cq"), nil, ts)
	if err := eng.Write(overwriteTable, []*cclient.Mutation{m}); err != nil {
		t.Fatal(err)
	}
}

// TestExportImportModelWithDeletes drives a producer P through random puts,
// deletes, flushes, compactions, reopens and incremental sync ticks, relays
// through an intermediate engine M that imports, compacts and re-exports, to
// a consumer D that imports (sometimes skipping manifests) and compacts.
// Every write carries a fresh timestamp, so the expected view is the model's:
// a row's newest put, unless a later delete removed it. After every relay M
// and D must read exactly what P reads.
func TestExportImportModelWithDeletes(t *testing.T) {
	ctx := context.Background()
	// The WAL's fsync dominates the run time and is not under test.
	fast := engine.Options{WALSyncMode: localwal.SyncOff}
	seeds := 16
	if testing.Short() {
		seeds = 8
	}
	rows := []string{"a", "b", "c", "d"}
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(seed)))
			srcDir := filepath.Join(t.TempDir(), "src")
			bm, bd := memory.New(), memory.New()
			rootM := filepath.Join(t.TempDir(), "m")
			rootD := filepath.Join(t.TempDir(), "d")
			src, err := engine.Open(srcDir, fast)
			if err != nil {
				t.Fatal(err)
			}
			if err := src.CreateTable(overwriteTable, engine.TableOptions{}); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = src.Close() }()
			mid := openConsumer(t, bm, rootM, fast)
			dst := openConsumer(t, bd, rootD, fast)
			optsM := engine.RFileExportOptions{DestinationRoot: rootM, ProducerID: "p"}
			optsD := engine.RFileExportOptions{DestinationRoot: rootD, ProducerID: "m"}
			var stateM, stateD *engine.SyncState
			model := map[string]string{}
			var trace []string
			check := func(who string, eng *engine.Engine) {
				t.Helper()
				if got := liveView(t, eng); !reflect.DeepEqual(got, model) {
					t.Fatalf("%s reads %v, want %v\ntrace: %v", who, got, model, trace)
				}
			}
			var ts int64
			var midReady, dstReady bool
			for step := 0; step < 40; step++ {
				switch op := rng.Intn(12); {
				case op < 4:
					ts++
					row := rows[rng.Intn(len(rows))]
					value := fmt.Sprintf("v%d", ts)
					putAt(t, src, row, ts, value)
					model[row] = value
					trace = append(trace, "put "+row)
				case op < 6:
					ts++
					row := rows[rng.Intn(len(rows))]
					deleteAt(t, src, row, ts)
					delete(model, row)
					trace = append(trace, "del "+row)
				case op < 7:
					applyStep(t, src, stepFlush)
					trace = append(trace, "flush")
				case op < 8:
					compactTable(t, src)
					trace = append(trace, "compactP")
				case op < 9:
					if err := src.Close(); err != nil {
						t.Fatal(err)
					}
					if src, err = engine.Open(srcDir, fast); err != nil {
						t.Fatal(err)
					}
					trace = append(trace, "reopenP")
				case op < 10:
					// Each compacts only once it holds the table.
					if midReady {
						compactTable(t, mid)
					}
					if dstReady {
						compactTable(t, dst)
					}
					trace = append(trace, "compactMD")
				default:
					// A tick P -> M; M forwards to D, and D imports — unless
					// it lags this round.
					res, err := src.ExportRFilesIncremental(ctx, overwriteTable, bm, optsM, stateM)
					if err != nil {
						t.Fatal(err)
					}
					stateM = roundTripState(t, res.State)
					importManifest(t, mid, res.Manifest)
					midReady = true
					if rng.Intn(2) == 0 {
						compactTable(t, mid)
					}
					res, err = mid.ExportRFilesIncremental(ctx, overwriteTable, bd, optsD, stateD)
					if err != nil {
						t.Fatal(err)
					}
					stateD = res.State
					if rng.Intn(3) == 0 {
						trace = append(trace, "tick(D lags)")
						continue
					}
					importManifest(t, dst, res.Manifest)
					dstReady = true
					trace = append(trace, "tick")
					check("P", src)
					check("M", mid)
					check("D", dst)
				}
			}
			check("P", src)
			// A final relay brings everyone level.
			res, err := src.ExportRFilesIncremental(ctx, overwriteTable, bm, optsM, stateM)
			if err != nil {
				t.Fatal(err)
			}
			importManifest(t, mid, res.Manifest)
			res, err = mid.ExportRFilesIncremental(ctx, overwriteTable, bd, optsD, stateD)
			if err != nil {
				t.Fatal(err)
			}
			importManifest(t, dst, res.Manifest)
			check("M", mid)
			check("D", dst)
		})
	}
}
