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

package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/phrocker/shoal-oss/internal/cclient"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
	"github.com/phrocker/shoal-oss/internal/tablet"
)

func lineagePut(t *testing.T, e *Engine, row, value string) {
	t.Helper()
	m, err := cclient.NewMutation([]byte(row))
	if err != nil {
		t.Fatal(err)
	}
	m.PutLatest([]byte("cf"), []byte("cq"), nil, []byte(value))
	if err := e.Write("t", []*cclient.Mutation{m}); err != nil {
		t.Fatal(err)
	}
}

// deltaFixture: a producer exported once, then flushed new data and
// compacted, and exported again; it returns both manifests.
func deltaFixture(t *testing.T) (*Engine, *memory.Backend, RFileExportOptions, *RFileExportManifest, *RFileExportManifest) {
	t.Helper()
	ctx := context.Background()
	e, err := Open(filepath.Join(t.TempDir(), "p"), Options{Backend: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.CreateTable("t", TableOptions{}); err != nil {
		t.Fatal(err)
	}
	dst := memory.New()
	opts := RFileExportOptions{DestinationRoot: "/dst", EngineVersion: "test"}
	lineagePut(t, e, "a", "1")
	first, err := e.ExportRFiles(ctx, "t", dst, opts)
	if err != nil {
		t.Fatal(err)
	}
	lineagePut(t, e, "b", "2")
	if err := e.Flush("t"); err != nil {
		t.Fatal(err)
	}
	if err := e.Compact("t", nil); err != nil {
		t.Fatal(err)
	}
	second, err := e.ExportRFiles(ctx, "t", dst, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Deltas) != 1 {
		t.Fatalf("fixture: deltas = %d, want 1", len(second.Deltas))
	}
	return e, dst, opts, first, second
}

// TestExportDeltaRefusesOverlappingFile: a file that may hold a leaf's cells
// twice cannot be split by subtracting each sent leaf once.
func TestExportDeltaRefusesOverlappingFile(t *testing.T) {
	e, dst, opts, first, second := deltaFixture(t)
	entries := append([]RFileExportFile(nil), second.RFiles...)
	if got := e.buildExportDeltas(context.Background(), dst, opts, entries, first.RFiles); len(got) != 1 {
		t.Fatalf("control: deltas = %d, want 1", len(got))
	}
	overlapping := entries[0].Lineage.Clone()
	overlapping.Overlap = true
	entries[0].Lineage = &overlapping
	if got := e.buildExportDeltas(context.Background(), dst, opts, entries, first.RFiles); len(got) != 0 {
		t.Fatalf("deltas for an overlapping file = %d, want 0", len(got))
	}
}

// TestExportDeltaNeedsKnownSentLeaves: a sent file without lineage (an
// older producer's manifest) leaves the sent leaves unknown, so no delta.
func TestExportDeltaNeedsKnownSentLeaves(t *testing.T) {
	e, dst, opts, first, second := deltaFixture(t)
	prior := append(append([]RFileExportFile(nil), first.RFiles...),
		RFileExportFile{TabletIndex: 0, RelativePath: "t/t-0000/F0.rf", DestinationPath: "/dst/t/t-0000/F0.rf"})
	if got := e.buildExportDeltas(context.Background(), dst, opts, second.RFiles, prior); len(got) != 0 {
		t.Fatalf("deltas = %d, want 0 with an unknown sent file", len(got))
	}
}

// TestExportDeltaRefusesInexactBasis: the sent files must hold exactly the
// sent leaves of the file, each once — a sent file with a leaf outside it,
// or two sent files sharing a leaf, make the subtraction wrong.
func TestExportDeltaRefusesInexactBasis(t *testing.T) {
	e, dst, opts, first, second := deltaFixture(t)
	entry := second.RFiles[0]
	sent := first.RFiles[0]
	outside := sent.Lineage.Union(tablet.OpaqueLineage("elsewhere"))
	partial := sent
	partial.Lineage = &outside
	if _, ok, err := e.exportDelta(context.Background(), dst, opts, entry, []RFileExportFile{partial}); ok || err != nil {
		t.Fatalf("basis with a leaf outside the file: ok=%v err=%v", ok, err)
	}
	twice := sent
	if _, ok, err := e.exportDelta(context.Background(), dst, opts, entry, []RFileExportFile{sent, twice}); ok || err != nil {
		t.Fatalf("basis holding a leaf twice: ok=%v err=%v", ok, err)
	}
}

// TestReadVerifiedRejectsChangedObject: a sent object whose bytes changed
// since it was listed is not used as a delta basis.
func TestReadVerifiedRejectsChangedObject(t *testing.T) {
	b := memory.New()
	b.Put("/x.rf", []byte("abcd"))
	sum := sha256.Sum256([]byte("abcd"))
	entry := RFileExportFile{DestinationPath: "/x.rf", Size: 4, SHA256: hex.EncodeToString(sum[:])}
	if _, err := readVerified(context.Background(), b, entry); err != nil {
		t.Fatal(err)
	}
	b.Put("/x.rf", []byte("abce"))
	if _, err := readVerified(context.Background(), b, entry); err == nil {
		t.Fatal("changed object accepted")
	}
}

// TestManifestDeltasNeedBaseInSameTablet: a delta is only ever a delta of
// a listed file of its own tablet.
func TestManifestDeltasNeedBaseInSameTablet(t *testing.T) {
	m := &RFileExportManifest{
		RFiles: []RFileExportFile{{TabletIndex: 0, RelativePath: "t/t-0000/C1.rf"}},
		Deltas: []RFileExportFile{
			{TabletIndex: 0, RelativePath: "t/t-0000/C1~d1.rf", DeltaOf: "t/t-0000/C1.rf"},
			{TabletIndex: 1, RelativePath: "t/t-0001/C1~d2.rf", DeltaOf: "t/t-0000/C1.rf"},
			{TabletIndex: 0, RelativePath: "t/t-0000/C9~d3.rf", DeltaOf: "t/t-0000/C9.rf"},
		},
	}
	deltas, byBase := manifestDeltas(m)
	if len(deltas) != 1 || deltas[0].RelativePath != "t/t-0000/C1~d1.rf" || len(byBase[0]) != 1 {
		t.Fatalf("deltas = %+v, byBase = %v", deltas, byBase)
	}
}

// TestIncrementalStateWithoutLineageGetsIt: a SyncState written by an older
// binary has no lineage; reusing a shipped entry refreshes it, so the next
// tick can compute a delta against it.
func TestIncrementalStateWithoutLineageGetsIt(t *testing.T) {
	ctx := context.Background()
	e, err := Open(filepath.Join(t.TempDir(), "p"), Options{Backend: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.CreateTable("t", TableOptions{}); err != nil {
		t.Fatal(err)
	}
	dst := memory.New()
	opts := RFileExportOptions{DestinationRoot: "/dst", EngineVersion: "test"}
	lineagePut(t, e, "a", "1")
	res, err := e.ExportRFilesIncremental(ctx, "t", dst, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	for rel, entry := range res.State.Shipped {
		entry.Lineage = nil
		res.State.Shipped[rel] = entry
	}
	res, err = e.ExportRFilesIncremental(ctx, "t", dst, opts, res.State)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range res.State.Shipped {
		if entry.Lineage == nil {
			t.Fatal("reused entry kept no lineage")
		}
	}
	lineagePut(t, e, "b", "2")
	if err := e.Flush("t"); err != nil {
		t.Fatal(err)
	}
	if err := e.Compact("t", nil); err != nil {
		t.Fatal(err)
	}
	res, err = e.ExportRFilesIncremental(ctx, "t", dst, opts, res.State)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Manifest.Deltas) != 1 {
		t.Fatalf("deltas = %d, want 1", len(res.Manifest.Deltas))
	}
}
