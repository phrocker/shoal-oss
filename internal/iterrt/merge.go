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

package iterrt

import (
	"container/heap"
	"errors"
)

// MergingIterator merges N sorted SortedKeyValueIterator sources into one
// sorted stream — a compaction's "read N RFiles in order" primitive. It
// is a Go port of the merge semantics in scanserver/scan.go's
// fileIter/iterHeap, rebuilt against the iterrt interface so it composes
// with the rest of the runtime.
//
// Ordering is wire.Key.Compare (PartialKey ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL),
// so for cells at the same coordinate the newest timestamp sorts first
// and a tombstone sorts before its matching live value — exactly the
// order a VersioningIterator stacked above expects.
//
// The merge does NOT dedupe or drop anything: every cell from every
// source is surfaced. Version-capping and delete handling belong to
// iterators stacked on top (VersioningIterator), matching how Java's
// compaction stack composes.
type MergingIterator struct {
	sources []SortedKeyValueIterator
	h       mergeHeap
	err     error
}

// NewMergingIterator builds a merge over sources. The sources are NOT
// Init'd or Seek'd here — call Init then Seek on the MergingIterator,
// which fans both out. A zero-source merge is valid and immediately
// exhausted.
func NewMergingIterator(sources ...SortedKeyValueIterator) *MergingIterator {
	return &MergingIterator{sources: sources}
}

// Init wires every source. The merge itself takes a nil top-level source
// (its inputs are the variadic sources from the constructor); a non-nil
// source argument is a usage error. options/env are passed through to
// each source unchanged.
func (m *MergingIterator) Init(source SortedKeyValueIterator, options map[string]string, env IteratorEnvironment) error {
	if source != nil {
		return errors.New("iterrt: MergingIterator takes its sources via NewMergingIterator, not Init")
	}
	for i, s := range m.sources {
		if err := s.Init(nil, options, env); err != nil {
			return err
		}
		_ = i
	}
	return nil
}

// Seek fans the seek out to every source, then rebuilds the heap over
// whichever sources have a top.
func (m *MergingIterator) Seek(r Range, columnFamilies [][]byte, inclusive bool) error {
	m.err = nil
	m.h = m.h[:0]
	for _, s := range m.sources {
		if err := s.Seek(r, columnFamilies, inclusive); err != nil {
			m.err = err
			return err
		}
		if s.HasTop() {
			m.h = append(m.h, s)
		}
	}
	heap.Init(&m.h)
	return nil
}

// Next advances the source that currently owns the top, then re-heaps.
func (m *MergingIterator) Next() error {
	if m.err != nil {
		return m.err
	}
	if len(m.h) == 0 {
		return errors.New("iterrt: MergingIterator.Next called without a top")
	}
	top := m.h[0]
	if err := top.Next(); err != nil {
		m.err = err
		return err
	}
	if top.HasTop() {
		heap.Fix(&m.h, 0)
	} else {
		heap.Pop(&m.h)
	}
	return nil
}

// HasTop reports whether any source still has a cell.
func (m *MergingIterator) HasTop() bool {
	return m.err == nil && len(m.h) > 0
}

// GetTopKey returns the smallest key across all sources.
func (m *MergingIterator) GetTopKey() *Key {
	if len(m.h) == 0 {
		return nil
	}
	return m.h[0].GetTopKey()
}

// GetTopValue returns the value paired with GetTopKey.
func (m *MergingIterator) GetTopValue() []byte {
	if len(m.h) == 0 {
		return nil
	}
	return m.h[0].GetTopValue()
}

// DeepCopy returns an independent merge: every source is DeepCopy'd, so
// the copy can seek without disturbing the original.
func (m *MergingIterator) DeepCopy(env IteratorEnvironment) SortedKeyValueIterator {
	cp := &MergingIterator{sources: make([]SortedKeyValueIterator, len(m.sources))}
	for i, s := range m.sources {
		cp.sources[i] = s.DeepCopy(env)
	}
	return cp
}

// mergeHeap is a min-heap of sources ordered by current top key. Only
// sources with HasTop() == true are ever in the heap.
type mergeHeap []SortedKeyValueIterator

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	return h[i].GetTopKey().Compare(h[j].GetTopKey()) < 0
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) { *h = append(*h, x.(SortedKeyValueIterator)) }

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
