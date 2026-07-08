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
	"sort"
	"testing"
)

func seekAll(t *testing.T, it SortedKeyValueIterator) {
	t.Helper()
	if err := it.Init(nil, nil, IteratorEnvironment{}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := it.Seek(InfiniteRange(), nil, false); err != nil {
		t.Fatalf("Seek: %v", err)
	}
}

// TestMerge_TwoSortedStreams: merging two key-disjoint sorted streams
// yields one fully-sorted stream.
func TestMerge_TwoSortedStreams(t *testing.T) {
	a := newSliceSource(
		kv{mk("r1", "cf", "a", "", 10), []byte("a1")},
		kv{mk("r3", "cf", "a", "", 10), []byte("a3")},
	)
	b := newSliceSource(
		kv{mk("r2", "cf", "a", "", 10), []byte("b2")},
		kv{mk("r4", "cf", "a", "", 10), []byte("b4")},
	)
	m := NewMergingIterator(a, b)
	seekAll(t, m)
	got, err := drain(m)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if gs := valStrings(got); !eqStr(gs, []string{"a1", "b2", "a3", "b4"}) {
		t.Fatalf("got %v", gs)
	}
}

// TestMerge_OverlappingCoordinatesTimestampOrder: when two sources carry
// the same coordinate at different timestamps, the merge interleaves
// them newest-first (wire.Key.Compare puts the newer timestamp first).
func TestMerge_OverlappingCoordinatesTimestampOrder(t *testing.T) {
	a := newSliceSource(
		kv{mk("r1", "cf", "a", "", 30), []byte("a-v30")},
		kv{mk("r1", "cf", "a", "", 10), []byte("a-v10")},
	)
	b := newSliceSource(
		kv{mk("r1", "cf", "a", "", 20), []byte("b-v20")},
	)
	m := NewMergingIterator(a, b)
	seekAll(t, m)
	got, err := drain(m)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if gs := valStrings(got); !eqStr(gs, []string{"a-v30", "b-v20", "a-v10"}) {
		t.Fatalf("got %v, want newest-first interleave", gs)
	}
}

// TestMerge_FeedsVersioning: a MergingIterator under a VersioningIterator
// is the compaction-stack core — N files merged then version-capped.
func TestMerge_FeedsVersioning(t *testing.T) {
	a := newSliceSource(
		kv{mk("r1", "cf", "a", "", 30), []byte("v30")},
		kv{mk("r1", "cf", "a", "", 10), []byte("v10")},
	)
	b := newSliceSource(
		kv{mk("r1", "cf", "a", "", 20), []byte("v20")},
		kv{mk("r2", "cf", "a", "", 5), []byte("r2")},
	)
	m := NewMergingIterator(a, b)
	v := NewVersioningIterator()
	if err := v.Init(m, map[string]string{VersioningOption: "1"}, IteratorEnvironment{Scope: ScopeMajc}); err != nil {
		t.Fatalf("vers Init: %v", err)
	}
	// MergingIterator is the source; Init it explicitly (Versioning.Init
	// does not fan Init into its source).
	if err := m.Init(nil, nil, IteratorEnvironment{Scope: ScopeMajc}); err != nil {
		t.Fatalf("merge Init: %v", err)
	}
	if err := v.Seek(InfiniteRange(), nil, false); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	got, err := drain(v)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if gs := valStrings(got); !eqStr(gs, []string{"v30", "r2"}) {
		t.Fatalf("got %v, want newest-per-coord [v30 r2]", gs)
	}
}

// TestMerge_Empty: a zero-source merge is valid and immediately exhausted.
func TestMerge_Empty(t *testing.T) {
	m := NewMergingIterator()
	seekAll(t, m)
	if m.HasTop() {
		t.Fatal("empty merge should have no top")
	}
}

// TestMerge_DeepCopyIndependent: a DeepCopy'd merge seeks without
// disturbing the original's position.
func TestMerge_DeepCopyIndependent(t *testing.T) {
	a := newSliceSource(kv{mk("r1", "cf", "a", "", 10), []byte("a1")})
	b := newSliceSource(kv{mk("r2", "cf", "a", "", 10), []byte("b2")})
	m := NewMergingIterator(a, b)
	seekAll(t, m)
	if err := m.Next(); err != nil { // advance original past r1
		t.Fatalf("Next: %v", err)
	}
	cp := m.DeepCopy(IteratorEnvironment{})
	if err := cp.Seek(InfiniteRange(), nil, false); err != nil {
		t.Fatalf("copy Seek: %v", err)
	}
	got, err := drain(cp)
	if err != nil {
		t.Fatalf("copy drain: %v", err)
	}
	if gs := valStrings(got); !eqStr(gs, []string{"a1", "b2"}) {
		t.Fatalf("copy got %v, want full stream", gs)
	}
	// Original still positioned at r2.
	if !m.HasTop() || string(m.GetTopValue()) != "b2" {
		t.Fatalf("original disturbed by DeepCopy")
	}
}

// TestMerge_ManySourcesFuzzParity: merge of K random sorted streams
// equals a global sort of all cells.
func TestMerge_ManySourcesFuzzParity(t *testing.T) {
	const k = 6
	var all []kv
	srcs := make([]SortedKeyValueIterator, k)
	for s := 0; s < k; s++ {
		var cells []kv
		for i := s; i < 200; i += k {
			c := kv{mk(rowN(i), "cf", "a", "", int64(1000-i)), []byte(rowN(i))}
			cells = append(cells, c)
			all = append(all, c)
		}
		srcs[s] = newSliceSource(cells...)
	}
	m := NewMergingIterator(srcs...)
	seekAll(t, m)
	got, err := drain(m)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].k.Compare(all[j].k) < 0 })
	if len(got) != len(all) {
		t.Fatalf("count %d != %d", len(got), len(all))
	}
	for i := range all {
		if got[i].k.Compare(all[i].k) != 0 {
			t.Fatalf("cell %d: merge %+v != sorted %+v", i, got[i].k, all[i].k)
		}
	}
}

func rowN(i int) string {
	const digits = "0123456789"
	b := []byte("r000")
	b[1] = digits[(i/100)%10]
	b[2] = digits[(i/10)%10]
	b[3] = digits[i%10]
	return string(b)
}
