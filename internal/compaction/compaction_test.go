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

package compaction

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/phrocker/shoal/internal/iterrt"
	"github.com/phrocker/shoal/internal/rfile"
	"github.com/phrocker/shoal/internal/rfile/bcfile"
	"github.com/phrocker/shoal/internal/rfile/bcfile/block"
	"github.com/phrocker/shoal/internal/rfile/wire"
)

// kv is one cell in test input/expectation.
type kv struct {
	K *wire.Key
	V string
}

// mk builds a key. Visibility is left empty — compaction scopes never
// filter by visibility, so it is not exercised here.
func mk(row, cf, cq string, ts int64) *wire.Key {
	return &wire.Key{
		Row:             []byte(row),
		ColumnFamily:    []byte(cf),
		ColumnQualifier: []byte(cq),
		Timestamp:       ts,
	}
}

// buildRFile writes cells (which the caller must supply already sorted)
// into an RFile image. This is the synthetic-input generator: the
// composer's job is to read N of these and produce one.
func buildRFile(t *testing.T, cells []kv) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := rfile.NewWriter(&buf, rfile.WriterOptions{})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i, c := range cells {
		if err := w.Append(c.K, []byte(c.V)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

// drainRFile reopens an RFile image and returns every cell, in file
// order. Used to verify the composer's output (roundtrip read).
func drainRFile(t *testing.T, image []byte) []kv {
	t.Helper()
	bc, err := bcfile.NewReader(bytes.NewReader(image), int64(len(image)))
	if err != nil {
		t.Fatalf("bcfile.NewReader: %v", err)
	}
	r, err := rfile.Open(bc, block.Default())
	if err != nil {
		t.Fatalf("rfile.Open: %v", err)
	}
	defer r.Close()

	var out []kv
	for {
		k, v, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		out = append(out, kv{K: k, V: string(v)})
	}
}

func assertCells(t *testing.T, got, want []kv) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("cell count: got %d, want %d\ngot:  %s\nwant: %s",
			len(got), len(want), fmtCells(got), fmtCells(want))
	}
	for i := range got {
		if !got[i].K.Equal(want[i].K) {
			t.Errorf("cell %d key: got %+v, want %+v", i, got[i].K, want[i].K)
		}
		if got[i].V != want[i].V {
			t.Errorf("cell %d value: got %q, want %q", i, got[i].V, want[i].V)
		}
	}
}

func fmtCells(cells []kv) string {
	var b bytes.Buffer
	for _, c := range cells {
		b.WriteString(string(c.K.Row))
		b.WriteByte(':')
		b.WriteString(string(c.K.ColumnFamily))
		b.WriteByte(':')
		b.WriteString(c.V)
		b.WriteByte(' ')
	}
	return b.String()
}

// TestCompact_IdentitySingleFile: empty stack, one input — the output is
// a faithful copy of the input (the C0/C1 identity-compaction case).
func TestCompact_IdentitySingleFile(t *testing.T) {
	in := []kv{
		{mk("a", "cf", "cq", 10), "va"},
		{mk("b", "cf", "cq", 10), "vb"},
		{mk("c", "cf", "cq", 10), "vc"},
	}
	res, err := Compact(Spec{
		Inputs: []Input{{Name: "f1", Bytes: buildRFile(t, in)}},
		Scope:  iterrt.ScopeMajc,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.EntriesWritten != 3 {
		t.Errorf("EntriesWritten = %d, want 3", res.EntriesWritten)
	}
	assertCells(t, drainRFile(t, res.Output), in)
}

// TestCompact_MergeTwoFiles: two inputs with interleaved rows — the
// MergingIterator must produce one globally-sorted output.
func TestCompact_MergeTwoFiles(t *testing.T) {
	f1 := []kv{
		{mk("a", "cf", "q", 5), "a"},
		{mk("c", "cf", "q", 5), "c"},
		{mk("e", "cf", "q", 5), "e"},
	}
	f2 := []kv{
		{mk("b", "cf", "q", 5), "b"},
		{mk("d", "cf", "q", 5), "d"},
		{mk("f", "cf", "q", 5), "f"},
	}
	res, err := Compact(Spec{
		Inputs: []Input{
			{Name: "f1", Bytes: buildRFile(t, f1)},
			{Name: "f2", Bytes: buildRFile(t, f2)},
		},
		Scope: iterrt.ScopeMajc,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	want := []kv{
		{mk("a", "cf", "q", 5), "a"},
		{mk("b", "cf", "q", 5), "b"},
		{mk("c", "cf", "q", 5), "c"},
		{mk("d", "cf", "q", 5), "d"},
		{mk("e", "cf", "q", 5), "e"},
		{mk("f", "cf", "q", 5), "f"},
	}
	assertCells(t, drainRFile(t, res.Output), want)
}

// TestCompact_VersioningStack: the same coordinate appears in two files
// at different timestamps. A VersioningIterator(maxVersions=1) above the
// merge must keep only the newest. This exercises the full
// merge → BuildStack → write pipeline.
func TestCompact_VersioningStack(t *testing.T) {
	// Newer file.
	f1 := []kv{
		{mk("k1", "cf", "q", 200), "new1"},
		{mk("k2", "cf", "q", 200), "new2"},
	}
	// Older file — same coordinates, lower timestamps.
	f2 := []kv{
		{mk("k1", "cf", "q", 100), "old1"},
		{mk("k2", "cf", "q", 100), "old2"},
	}
	res, err := Compact(Spec{
		Inputs: []Input{
			{Name: "new", Bytes: buildRFile(t, f1)},
			{Name: "old", Bytes: buildRFile(t, f2)},
		},
		Stack: []iterrt.IterSpec{
			{Name: iterrt.IterVersioning, Options: map[string]string{iterrt.VersioningOption: "1"}},
		},
		Scope:               iterrt.ScopeMajc,
		FullMajorCompaction: true,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.EntriesWritten != 2 {
		t.Errorf("EntriesWritten = %d, want 2 (older versions dropped)", res.EntriesWritten)
	}
	want := []kv{
		{mk("k1", "cf", "q", 200), "new1"},
		{mk("k2", "cf", "q", 200), "new2"},
	}
	assertCells(t, drainRFile(t, res.Output), want)
}

// TestCompact_VersioningKeepsTwo: maxVersions=2 keeps both versions of a
// coordinate, newest-first, confirming the stack option is threaded.
func TestCompact_VersioningKeepsTwo(t *testing.T) {
	f1 := []kv{{mk("k1", "cf", "q", 300), "v300"}}
	f2 := []kv{{mk("k1", "cf", "q", 200), "v200"}}
	f3 := []kv{{mk("k1", "cf", "q", 100), "v100"}}
	res, err := Compact(Spec{
		Inputs: []Input{
			{Name: "f1", Bytes: buildRFile(t, f1)},
			{Name: "f2", Bytes: buildRFile(t, f2)},
			{Name: "f3", Bytes: buildRFile(t, f3)},
		},
		Stack: []iterrt.IterSpec{
			{Name: iterrt.IterVersioning, Options: map[string]string{iterrt.VersioningOption: "2"}},
		},
		Scope: iterrt.ScopeMajc,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	want := []kv{
		{mk("k1", "cf", "q", 300), "v300"},
		{mk("k1", "cf", "q", 200), "v200"},
	}
	assertCells(t, drainRFile(t, res.Output), want)
}

// TestCompact_MultiBlockInput: a small BlockSize forces the input RFile
// into many data blocks; the composer must read across block boundaries
// and re-emit every cell.
func TestCompact_MultiBlockInput(t *testing.T) {
	var in []kv
	for i := 0; i < 500; i++ {
		row := []byte{'r', byte('0' + i/100), byte('0' + (i/10)%10), byte('0' + i%10)}
		in = append(in, kv{
			K: &wire.Key{Row: row, ColumnFamily: []byte("cf"), ColumnQualifier: []byte("q"), Timestamp: 1},
			V: "val",
		})
	}
	var buf bytes.Buffer
	w, err := rfile.NewWriter(&buf, rfile.WriterOptions{BlockSize: 256})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i, c := range in {
		if err := w.Append(c.K, []byte(c.V)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	res, err := Compact(Spec{
		Inputs: []Input{{Name: "multi", Bytes: buf.Bytes()}},
		Scope:  iterrt.ScopeMajc,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.EntriesWritten != 500 {
		t.Errorf("EntriesWritten = %d, want 500", res.EntriesWritten)
	}
	assertCells(t, drainRFile(t, res.Output), in)
}

// TestCompact_EmptyInputs: a zero-input spec produces a valid, empty
// RFile. cmd/shoal-compactor rejects this earlier via ErrNoInputs, but
// the composer itself must not panic.
func TestCompact_EmptyInputs(t *testing.T) {
	res, err := Compact(Spec{Scope: iterrt.ScopeMajc})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.EntriesWritten != 0 {
		t.Errorf("EntriesWritten = %d, want 0", res.EntriesWritten)
	}
	if got := drainRFile(t, res.Output); len(got) != 0 {
		t.Errorf("drained %d cells from empty compaction, want 0", len(got))
	}
}

// TestCompact_RejectsEmptyInputBytes: an Input with no bytes is a
// caller error — a job pointing at a file shoal could not fetch.
func TestCompact_RejectsEmptyInputBytes(t *testing.T) {
	_, err := Compact(Spec{
		Inputs: []Input{{Name: "missing", Bytes: nil}},
		Scope:  iterrt.ScopeMajc,
	})
	if err == nil {
		t.Fatal("expected error for empty input bytes")
	}
}

// TestCompact_UnknownIterator: an iterator name not in BuildStack's set
// must fail the compaction rather than silently skipping the iterator.
func TestCompact_UnknownIterator(t *testing.T) {
	_, err := Compact(Spec{
		Inputs: []Input{{Name: "f1", Bytes: buildRFile(t, []kv{{mk("a", "cf", "q", 1), "v"}})}},
		Stack:  []iterrt.IterSpec{{Name: "no-such-iterator"}},
		Scope:  iterrt.ScopeMajc,
	})
	if err == nil {
		t.Fatal("expected error for unknown iterator")
	}
}

// wideRFile builds an input big enough to flush several data blocks
// (DefaultBlockSize is 100 KiB), so a compaction over it exercises the
// budget check inside the append loop rather than only the one after
// Close.
func wideRFile(t *testing.T, cells int) []byte {
	t.Helper()
	// Values that do not compress well, so "none" and "snappy" outputs
	// stay the same order of magnitude and the test is not measuring the
	// codec.
	val := make([]byte, 1024)
	for i := range val {
		val[i] = byte(i * 31)
	}
	kvs := make([]kv, 0, cells)
	for i := range cells {
		kvs = append(kvs, kv{mk(fmt.Sprintf("row%08d", i), "cf", "q", 10), string(val)})
	}
	return buildRFile(t, kvs)
}

// TestCompact_OutputBudgetStopsALongCompaction: the cap is enforced
// while cells are being appended, not only at the end — a compaction
// that would retain far more than the budget must abandon early rather
// than build the whole image first.
func TestCompact_OutputBudgetStopsALongCompaction(t *testing.T) {
	const cells = 400
	_, err := Compact(Spec{
		Inputs:         []Input{{Name: "wide", Bytes: wideRFile(t, cells)}},
		Scope:          iterrt.ScopeMajc,
		Codec:          block.CodecNone,
		MaxOutputBytes: 64 * 1024,
	})
	if !errors.Is(err, ErrOutputTooLarge) {
		t.Fatalf("Compact err = %v, want ErrOutputTooLarge", err)
	}

	var size, written int64
	if _, e := fmt.Sscanf(err.Error(), "compaction: output reached %d bytes after %d cells", &size, &written); e != nil {
		t.Fatalf("error %q does not report size and cell count: %v", err, e)
	}
	if written >= cells {
		t.Errorf("gave up after %d of %d cells; the budget should stop the append loop, not just the final image", written, cells)
	}
	if size <= 64*1024 {
		t.Errorf("reported size %d is within the 65536-byte budget", size)
	}
}

// TestCompact_OutputBudgetCatchesTheClosingFlush: a small compaction
// never flushes a block while appending, so its whole image appears when
// the writer closes. The budget has to be rechecked there or a job that
// looked free cell-by-cell escapes it entirely.
func TestCompact_OutputBudgetCatchesTheClosingFlush(t *testing.T) {
	in := []kv{
		{mk("a", "cf", "q", 10), "va"},
		{mk("b", "cf", "q", 10), "vb"},
		{mk("c", "cf", "q", 10), "vc"},
	}
	_, err := Compact(Spec{
		Inputs:         []Input{{Name: "f1", Bytes: buildRFile(t, in)}},
		Scope:          iterrt.ScopeMajc,
		MaxOutputBytes: 16,
	})
	if !errors.Is(err, ErrOutputTooLarge) {
		t.Fatalf("Compact err = %v, want ErrOutputTooLarge", err)
	}
	if !strings.Contains(err.Error(), "after 3 cells") {
		t.Errorf("error %q should report all 3 cells appended before the close-time check", err)
	}
}

// TestCompact_OutputBudgetAllowsWhatFits: the same shape under a budget
// the output fits inside still produces every cell, so the check cannot
// be truncating good compactions.
func TestCompact_OutputBudgetAllowsWhatFits(t *testing.T) {
	in := []kv{
		{mk("a", "cf", "q", 10), "va"},
		{mk("b", "cf", "q", 10), "vb"},
	}
	res, err := Compact(Spec{
		Inputs:         []Input{{Name: "f1", Bytes: buildRFile(t, in)}},
		Scope:          iterrt.ScopeMajc,
		MaxOutputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	assertCells(t, drainRFile(t, res.Output), in)
}

// TestCompact_ZeroOutputBudgetIsUnlimited: zero keeps the pre-existing
// behaviour, which every other caller of this package relies on.
func TestCompact_ZeroOutputBudgetIsUnlimited(t *testing.T) {
	res, err := Compact(Spec{
		Inputs: []Input{{Name: "wide", Bytes: wideRFile(t, 200)}},
		Scope:  iterrt.ScopeMajc,
		Codec:  block.CodecNone,
	})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.EntriesWritten != 200 {
		t.Errorf("EntriesWritten = %d, want 200", res.EntriesWritten)
	}
	if len(res.Output) <= 64*1024 {
		t.Fatalf("output is %d bytes; the fixture must exceed the budget the other tests set", len(res.Output))
	}
}
