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

// Package compaction is shoal's compaction-stack composer: the "read N
// RFiles → apply the iterator stack → write one RFile" core of Bet 1
// (design doc, "What the compactor actually has to do", component 4).
//
// It is deliberately decoupled from the coordinator. A compaction is a
// pure function of (input RFiles, iterator stack, scope) → output RFile,
// so this package is fully testable offline with synthetic RFiles — no
// coordinator, no ZK, no metadata. cmd/shoal-compactor wires it to the
// CompactionCoordinator job-poll loop; the metadata commit of the output
// file is a separate, manager-side step (see cmd/shoal-compactor).
//
// The stack order mirrors the JVM tserver's SortedKeyValueIterator
// pipeline: a MergingIterator over the per-file RFileSource leaves, then
// the user/system iterator stack built by iterrt.BuildStack on top
// (VersioningIterator → user iterators). The top of the stack is drained
// into an rfile.Writer.
package compaction

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/phrocker/shoal/internal/iterrt"
	"github.com/phrocker/shoal/internal/rfile"
	"github.com/phrocker/shoal/internal/rfile/bcfile"
	"github.com/phrocker/shoal/internal/rfile/bcfile/block"
)

// Input is one RFile feeding a compaction. Bytes is the whole RFile
// image; Name is a human label used only in error messages (typically
// the metadata file entry, e.g. the tablet-relative path).
//
// The whole-image-in-memory shape matches how shoal already pulls RFiles
// for reads (internal/storage fetches the object, the reader works over
// a bytes.Reader). A streaming variant is a later optimisation; it does
// not change the composer's contract.
type Input struct {
	Name  string
	Bytes []byte
}

// Spec describes a single compaction unit fully: the inputs, the
// iterator stack to apply, the scope, and the output RFile's encoding.
type Spec struct {
	// Inputs are the RFiles to merge, in any order — the MergingIterator
	// sorts across them. An empty Inputs list produces an empty (but
	// valid) output RFile.
	Inputs []Input

	// Stack is the iterator stack applied above the merge, bottom-first
	// (Stack[0] sits directly on the MergingIterator). Empty Stack is an
	// identity compaction: every cell passes through untouched, which is
	// the C0/C1 "identity compaction" behaviour.
	Stack []iterrt.IterSpec

	// Scope is the compaction context handed to every iterator's Init.
	// For a real compaction this is ScopeMinc or ScopeMajc; the composer
	// itself does not care which.
	Scope iterrt.IteratorScope

	// FullMajorCompaction is threaded into IteratorEnvironment. True only
	// when the output is the tablet's sole remaining file, which is the
	// only time a delete-aware iterator may drop a tombstone. Maps to the
	// coordinator job's PropagateDeletes flag (inverted): a job that says
	// "do not propagate deletes" is a full major compaction.
	FullMajorCompaction bool

	// Codec is the output RFile's block compression codec ("none", "gz",
	// "snappy"). Empty defaults to "snappy" for good throughput/size balance.
	Codec string

	// BlockSize overrides the output writer's data-block size threshold.
	// Zero uses rfile.DefaultBlockSize.
	BlockSize int

	// AdjacencyEdgeCF, when non-empty, is forwarded to the output
	// writer so the compacted RFile carries a shoal.adjacency out-edge
	// index for cells in this column family. Empty disables it.
	AdjacencyEdgeCF string
}

// Result reports what a Compact call produced.
type Result struct {
	// Output is the written RFile image.
	Output []byte
	// EntriesWritten is the cell count drained into the output — the
	// number of cells the iterator stack surfaced, which may be less
	// than the sum of input entries (versioning/filtering drops cells).
	EntriesWritten int64
}

// Compact runs one compaction described by spec and returns the output
// RFile bytes. It is the offline-testable core; it performs no I/O
// beyond the in-memory buffers it is handed.
//
// Pipeline:
//  1. open each Input as an rfile.Reader → wrap as an iterrt.RFileSource
//  2. merge the leaves with iterrt.MergingIterator
//  3. stack iterrt.BuildStack(merge, spec.Stack, env) on top
//  4. Seek the stack to the full range and drain every cell into an
//     rfile.Writer
//
// Cells reach the writer already in non-decreasing Key order because the
// MergingIterator emits them sorted and the stack above it is
// order-preserving (versioning/visibility only drop cells, never reorder).
func Compact(spec Spec) (*Result, error) {
	leaves := make([]iterrt.SortedKeyValueIterator, 0, len(spec.Inputs))
	closers := make([]io.Closer, 0, len(spec.Inputs))
	defer func() {
		for _, c := range closers {
			_ = c.Close()
		}
	}()

	env := iterrt.IteratorEnvironment{
		Scope:               spec.Scope,
		FullMajorCompaction: spec.FullMajorCompaction,
	}

	for _, in := range spec.Inputs {
		src, rdr, err := openInputSource(in, env)
		if err != nil {
			return nil, err
		}
		closers = append(closers, rdr)
		leaves = append(leaves, src)
	}

	merge := iterrt.NewMergingIterator(leaves...)
	if err := merge.Init(nil, nil, env); err != nil {
		return nil, fmt.Errorf("compaction: merge init: %w", err)
	}

	// Wrap source in DeletingIterator BEFORE applying user iterators —
	// matches Java's FileCompactor.compactLocalityGroup, which builds
	// the stack as: source → DeletingIterator → user iterators. Without
	// this, any tombstone in the input passes through to user iterators
	// (e.g. LatentEdgeDiscoveryIterator) as a live cell. Symptom on
	// graph_vidx: latentEdge buffers vertices whose Java-side equivalent
	// was already skipped, yielding a ~14% delta in emitted link cells
	// on tablets where vertices have been deleted.
	//
	// The propagateDeletes flag is computed from env.Scope +
	// FullMajorCompaction (see DeletingIterator's Init contract):
	// dropping tombstones is safe only when the output is the tablet's
	// sole file (FullMajorCompaction at ScopeMajc). Everything else
	// preserves them so a later compaction can apply suppression
	// against RFiles this stack didn't see.
	delIter := iterrt.NewDeletingIterator()
	if err := delIter.Init(merge, nil, env); err != nil {
		return nil, fmt.Errorf("compaction: deleting iter init: %w", err)
	}

	top, err := iterrt.BuildStack(delIter, spec.Stack, env)
	if err != nil {
		return nil, fmt.Errorf("compaction: build stack: %w", err)
	}
	if err := top.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
		return nil, fmt.Errorf("compaction: seek: %w", err)
	}

	codec := spec.Codec
	if codec == "" {
		codec = block.CodecSnappy
	}

	var buf bytes.Buffer
	w, err := rfile.NewWriter(&buf, rfile.WriterOptions{
		Codec:           codec,
		BlockSize:       spec.BlockSize,
		AdjacencyEdgeCF: spec.AdjacencyEdgeCF,
	})
	if err != nil {
		return nil, fmt.Errorf("compaction: new writer: %w", err)
	}

	var written int64
	for top.HasTop() {
		if err := w.Append(top.GetTopKey(), top.GetTopValue()); err != nil {
			return nil, fmt.Errorf("compaction: append cell %d: %w", written, err)
		}
		written++
		if err := top.Next(); err != nil {
			return nil, fmt.Errorf("compaction: advance after cell %d: %w", written, err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("compaction: close writer: %w", err)
	}

	return &Result{Output: buf.Bytes(), EntriesWritten: written}, nil
}

// openInputSource opens one Input as an iterrt leaf. The returned closer
// is the rfile.Reader; the caller closes it once the compaction drains.
//
// The RFileSource gets an opener so DeepCopy works — the MergingIterator
// does not DeepCopy its sources today, but a future stack (a parent that
// re-seeks its source) might, and an opener that re-derives a reader
// from the same in-memory image is free to provide.
func openInputSource(in Input, env iterrt.IteratorEnvironment) (*iterrt.RFileSource, io.Closer, error) {
	if len(in.Bytes) == 0 {
		return nil, nil, fmt.Errorf("compaction: input %q is empty", in.Name)
	}

	open := func() (*rfile.Reader, error) {
		bc, err := bcfile.NewReader(bytes.NewReader(in.Bytes), int64(len(in.Bytes)))
		if err != nil {
			return nil, fmt.Errorf("compaction: bcfile open %q: %w", in.Name, err)
		}
		r, err := rfile.Open(bc, block.Default())
		if err != nil {
			return nil, fmt.Errorf("compaction: rfile open %q: %w", in.Name, err)
		}
		return r, nil
	}

	rdr, err := open()
	if err != nil {
		return nil, nil, err
	}
	src := iterrt.NewRFileSource(rdr, open)
	if err := src.Init(nil, nil, env); err != nil {
		_ = rdr.Close()
		return nil, nil, fmt.Errorf("compaction: source init %q: %w", in.Name, err)
	}
	return src, rdr, nil
}

// ErrNoInputs is returned by callers that treat a zero-input spec as a
// programming error. Compact itself accepts it (and produces an empty
// RFile); cmd/shoal-compactor rejects it before calling Compact because
// the coordinator should never assign a job with no files.
var ErrNoInputs = errors.New("compaction: spec has no input files")
