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
	"io"
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
