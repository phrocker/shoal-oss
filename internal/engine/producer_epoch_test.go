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
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/engine"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
)

// Part of #701: producer instance epochs; incremental sync re-ships a file
// shipped under a different producer instance.

// openNamedProducer opens a producer engine at dir whose overwrite table's
// name high-water mark starts at highWater (0: the clock), so a test can make
// a recreated or restored producer reissue names it already used.
func openNamedProducer(t *testing.T, dir string, highWater int64) *engine.Engine {
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

func syncTick(t *testing.T, src *engine.Engine, b *memory.Backend, opts engine.RFileExportOptions, prior *engine.SyncState) *engine.IncrementalExportResult {
	t.Helper()
	res, err := src.ExportRFilesIncremental(context.Background(), overwriteTable, b, opts, prior)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// roundTripState passes the state through JSON, as a sync loop persisting
// it between runs does.
func roundTripState(t *testing.T, s *engine.SyncState) *engine.SyncState {
	t.Helper()
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var out engine.SyncState
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func importInto(t *testing.T, b *memory.Backend, dstDir string, m *engine.RFileExportManifest) *engine.Engine {
	t.Helper()
	dst, err := engine.Open(dstDir, engine.Options{Backend: b})
	if err != nil {
		t.Fatal(err)
	}
	if err := dst.ImportRFileManifest(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	return dst
}

// TestRecreatedProducerStaleSyncStateReships: a producer's data directory is
// recreated from empty and reissues exactly the name of a file the previous
// directory shipped. A sync that resumes from the old SyncState must ship the
// new file, not reuse the old directory's copy by path (main drops the new
// write). With and without a ProducerID.
func TestRecreatedProducerStaleSyncStateReships(t *testing.T) {
	for _, producerID := range []string{"pa", ""} {
		t.Run(fmt.Sprintf("producer=%q", producerID), func(t *testing.T) {
			srcDir := filepath.Join(t.TempDir(), "src")
			highWater := time.Now().Add(time.Hour).UnixMilli()
			b := memory.New()
			dstDir := filepath.Join(t.TempDir(), "dst")
			opts := engine.RFileExportOptions{DestinationRoot: dstDir, ProducerID: producerID}

			src := openNamedProducer(t, srcDir, highWater)
			putLatest(t, src, "cell", "old")
			applyStep(t, src, stepFlush)
			state := roundTripState(t, syncTick(t, src, b, opts, nil).State)
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(srcDir); err != nil {
				t.Fatal(err)
			}

			src = openNamedProducer(t, srcDir, highWater)
			defer src.Close()
			putLatest(t, src, "cell", "fresh")
			applyStep(t, src, stepFlush)
			res := syncTick(t, src, b, opts, state)
			if len(res.Manifest.RFiles) != 1 || len(state.Shipped) != 1 {
				t.Fatalf("premise: one file each, got %d", len(res.Manifest.RFiles))
			}
			for rel := range state.Shipped {
				if res.Manifest.RFiles[0].RelativePath != rel {
					t.Fatalf("premise: recreated producer should reissue %s, shipped %s", rel, res.Manifest.RFiles[0].RelativePath)
				}
			}
			if len(res.Uploaded) != 1 || len(res.Skipped) != 0 {
				t.Fatalf("reissued name reused by path: uploaded %v skipped %v", res.Uploaded, res.Skipped)
			}
			dst := importInto(t, b, dstDir, res.Manifest)
			defer dst.Close()
			if got := readLatestOn(t, dst, "cell"); got != "fresh" {
				t.Fatalf("consumer read %q, want fresh", got)
			}
		})
	}
}

// TestRestoredInPlaceProducerStaleSyncStateReships: the data directory is
// restored in place from a backup (rm -rf, then copy back to the same path:
// on ext4 the directory inode can even be reused) and reissues the name of
// a file shipped after the backup was taken. The sync resuming from the
// state written before the restore must ship the restored directory's file.
func TestRestoredInPlaceProducerStaleSyncStateReships(t *testing.T) {
	for _, producerID := range []string{"pa", ""} {
		t.Run(fmt.Sprintf("producer=%q", producerID), func(t *testing.T) {
			base := t.TempDir()
			srcDir := filepath.Join(base, "src")
			bak := filepath.Join(base, "bak")
			b := memory.New()
			dstDir := filepath.Join(t.TempDir(), "dst")
			opts := engine.RFileExportOptions{DestinationRoot: dstDir, ProducerID: producerID}

			src := openNamedProducer(t, srcDir, time.Now().Add(time.Hour).UnixMilli())
			putLatest(t, src, "cell", "v1")
			applyStep(t, src, stepFlush)
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			copyTree(t, srcDir, bak)
			src, err := engine.Open(srcDir, engine.Options{})
			if err != nil {
				t.Fatal(err)
			}
			putLatest(t, src, "cell", "v2")
			applyStep(t, src, stepFlush)
			state := roundTripState(t, syncTick(t, src, b, opts, nil).State)
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(srcDir); err != nil {
				t.Fatal(err)
			}
			copyTree(t, bak, srcDir)

			src, err = engine.Open(srcDir, engine.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			putLatest(t, src, "cell", "v3")
			applyStep(t, src, stepFlush)
			res := syncTick(t, src, b, opts, state)
			if len(res.Manifest.RFiles) != 2 {
				t.Fatalf("premise: two files, got %d", len(res.Manifest.RFiles))
			}
			if _, ok := state.Shipped[res.Manifest.RFiles[1].RelativePath]; !ok {
				t.Fatalf("premise: restored producer should reissue a shipped name, minted %s", res.Manifest.RFiles[1].RelativePath)
			}
			if len(res.Skipped) != 1 || len(res.Uploaded) != 1 || res.Uploaded[0] != res.Manifest.RFiles[1].RelativePath {
				t.Fatalf("restored producer: uploaded %v skipped %v", res.Uploaded, res.Skipped)
			}
			dst := importInto(t, b, dstDir, res.Manifest)
			defer dst.Close()
			if got := readLatestOn(t, dst, "cell"); got != "v3" {
				t.Fatalf("consumer read %q, want v3", got)
			}
		})
	}
}

// TestIncrementalSyncReshipsOnStampChange: a stamp change rewrites the bytes
// under the same name, so the next tick ships the file again instead of
// listing the previous tenant's bytes. With and without a ProducerID. An
// unchanged tick, including one after the producer reopens (a new process
// instance), still reuses every file.
func TestIncrementalSyncReshipsOnStampChange(t *testing.T) {
	for _, producerID := range []string{"pa", ""} {
		t.Run(fmt.Sprintf("producer=%q", producerID), func(t *testing.T) {
			srcDir := filepath.Join(t.TempDir(), "src")
			src := openOverwriteEngine(t, srcDir, nil)
			putLatest(t, src, "cell", "a")
			applyStep(t, src, stepFlush)
			b := memory.New()
			opts := engine.RFileExportOptions{DestinationRoot: "/dst", ProducerID: producerID, StampVisibilityLabel: "one"}
			first := syncTick(t, src, b, opts, nil)
			if err := src.Close(); err != nil {
				t.Fatal(err)
			}
			src, err := engine.Open(srcDir, engine.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			same := syncTick(t, src, b, opts, roundTripState(t, first.State))
			if len(same.Skipped) != 1 || len(same.Uploaded) != 0 {
				t.Fatalf("unchanged tick after a reopen re-shipped: uploaded %v", same.Uploaded)
			}
			opts.StampVisibilityLabel = "two"
			changed := syncTick(t, src, b, opts, roundTripState(t, same.State))
			if len(changed.Uploaded) != 1 || changed.Manifest.RFiles[0].SHA256 == first.Manifest.RFiles[0].SHA256 {
				t.Fatalf("stamp change: uploaded %v, entry %+v", changed.Uploaded, changed.Manifest.RFiles[0])
			}
			opts.StampMode = "whenEmpty"
			mode := syncTick(t, src, b, opts, roundTripState(t, changed.State))
			if len(mode.Uploaded) != 1 {
				t.Fatalf("stamp mode change reused the copy: skipped %v", mode.Skipped)
			}
		})
	}
}

// TestSyncStateWithoutKeysReshipsOnce: a SyncState written before shipKeys
// were recorded cannot tell which instance a copy came from, so its files
// are shipped once more; the tick after that reuses them.
func TestSyncStateWithoutKeysReshipsOnce(t *testing.T) {
	src := openOverwriteEngine(t, filepath.Join(t.TempDir(), "src"), nil)
	defer src.Close()
	putLatest(t, src, "cell", "a")
	applyStep(t, src, stepFlush)
	b := memory.New()
	opts := engine.RFileExportOptions{DestinationRoot: "/dst", ProducerID: "pa"}
	first := syncTick(t, src, b, opts, nil)
	legacy := roundTripState(t, first.State)
	legacy.ShippedKeys = nil
	again := syncTick(t, src, b, opts, legacy)
	if len(again.Uploaded) != 1 {
		t.Fatalf("keyless state reused a copy: skipped %v", again.Skipped)
	}
	settled := syncTick(t, src, b, opts, roundTripState(t, again.State))
	if len(settled.Skipped) != 1 || len(settled.Uploaded) != 0 {
		t.Fatalf("after the re-ship: uploaded %v", settled.Uploaded)
	}
}

// TestExportProducerEpoch: a full export records the producer epoch for
// files the producer minted, with a ProducerID only; it differs across a
// reopen for new names and stays for old ones.
func TestExportProducerEpoch(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "src")
	src := openOverwriteEngine(t, srcDir, nil)
	putLatest(t, src, "cell", "a")
	applyStep(t, src, stepFlush)
	b := memory.New()
	plain, err := src.ExportRFiles(context.Background(), overwriteTable, b, engine.RFileExportOptions{DestinationRoot: "/plain"})
	if err != nil {
		t.Fatal(err)
	}
	if plain.RFiles[0].ProducerEpoch != "" {
		t.Fatalf("epoch without a ProducerID: %+v", plain.RFiles[0])
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	src, err = engine.Open(srcDir, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	putLatest(t, src, "cell", "b")
	applyStep(t, src, stepFlush)
	m, err := src.ExportRFiles(context.Background(), overwriteTable, b, engine.RFileExportOptions{DestinationRoot: "/named", ProducerID: "pa"})
	if err != nil {
		t.Fatal(err)
	}
	if len(m.RFiles) != 2 || m.RFiles[0].ProducerEpoch == "" || m.RFiles[1].ProducerEpoch == "" ||
		m.RFiles[0].ProducerEpoch == m.RFiles[1].ProducerEpoch {
		t.Fatalf("epochs %+v", m.RFiles)
	}
}
