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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

// MaxLineageEntries bounds a Lineage: the number of leaf ranges plus opaque
// ids it may hold. A lineage that would grow past it is not recorded; the
// file it describes gets a fresh leaf of its own instead (see capLineage),
// so its consumers treat it as data they have never seen and append it as
// newest, which is the behaviour before lineage existed (#704).
const MaxLineageEntries = 256

// LeafRange is an inclusive range [lo, hi] of leaf sequence numbers.
type LeafRange [2]uint64

// Lineage names the data an immutable file holds, as a set of LEAVES.
//
// A leaf is one unit of data arriving at a tablet: a flush, or an import
// that came with no lineage of its own. It is named by the issuing tablet
// session's random origin and a sequence number, so a run of flushes is one
// compact range. A compaction output's lineage is the union of its inputs',
// so lineage is transitive: compacting outputs of earlier compactions, or
// re-exporting an imported file (chained sync, echo-back), keeps naming the
// same leaves. An importer compares the leaves it already holds with an
// incoming file's to tell data it has already received from new data.
//
// IDs are opaque leaves: the producer identity of an import whose producer
// sent no lineage (an older producer), which is then the only name its data
// has.
type Lineage struct {
	Leaves map[string][]LeafRange `json:"leaves,omitempty"`
	IDs    []string               `json:"ids,omitempty"`
}

// newOrigin returns a random leaf origin. Each tablet session (Open) draws
// its own, so leaf names never repeat even if a data directory is copied, or
// restored from an older backup, and written again.
func newOrigin() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("tablet: read random origin: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

func singleLeaf(origin string, seq uint64) Lineage {
	return Lineage{Leaves: map[string][]LeafRange{origin: {{seq, seq}}}}
}

// OpaqueLineage is the lineage of data known only by an opaque id.
func OpaqueLineage(id string) Lineage {
	return Lineage{IDs: []string{id}}
}

// IsEmpty reports whether l names no leaf.
func (l Lineage) IsEmpty() bool {
	return len(l.Leaves) == 0 && len(l.IDs) == 0
}

// Size is the number of entries (ranges plus ids) l holds.
func (l Lineage) Size() int {
	n := len(l.IDs)
	for _, ranges := range l.Leaves {
		n += len(ranges)
	}
	return n
}

// Clone returns a deep copy of l.
func (l Lineage) Clone() Lineage {
	var out Lineage
	if len(l.Leaves) > 0 {
		out.Leaves = make(map[string][]LeafRange, len(l.Leaves))
		for origin, ranges := range l.Leaves {
			out.Leaves[origin] = append([]LeafRange(nil), ranges...)
		}
	}
	if len(l.IDs) > 0 {
		out.IDs = append([]string(nil), l.IDs...)
	}
	return out
}

// Normalize returns l in canonical form — ranges sorted, merged and
// non-empty, ids sorted and unique — or false when l is malformed (a range
// with lo > hi, or an empty origin or id).
func (l Lineage) Normalize() (Lineage, bool) {
	var out Lineage
	for origin, ranges := range l.Leaves {
		if origin == "" {
			return Lineage{}, false
		}
		for _, r := range ranges {
			if r[0] > r[1] {
				return Lineage{}, false
			}
		}
		merged := normalizeRanges(append([]LeafRange(nil), ranges...))
		if len(merged) == 0 {
			continue
		}
		if out.Leaves == nil {
			out.Leaves = make(map[string][]LeafRange, len(l.Leaves))
		}
		out.Leaves[origin] = merged
	}
	if len(l.IDs) > 0 {
		ids := append([]string(nil), l.IDs...)
		sort.Strings(ids)
		for i, id := range ids {
			if id == "" {
				return Lineage{}, false
			}
			if i == 0 || id != ids[i-1] {
				out.IDs = append(out.IDs, id)
			}
		}
	}
	return out, true
}

func normalizeRanges(ranges []LeafRange) []LeafRange {
	if len(ranges) == 0 {
		return nil
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i][0] < ranges[j][0] })
	out := ranges[:1]
	for _, r := range ranges[1:] {
		last := &out[len(out)-1]
		// Merge overlapping and adjacent ranges (hi+1 == lo).
		if r[0] <= last[1] || (last[1] != ^uint64(0) && r[0] == last[1]+1) {
			if r[1] > last[1] {
				last[1] = r[1]
			}
			continue
		}
		out = append(out, r)
	}
	return out
}

// Union returns l ∪ o.
func (l Lineage) Union(o Lineage) Lineage {
	out := l.Clone()
	for origin, ranges := range o.Leaves {
		if out.Leaves == nil {
			out.Leaves = make(map[string][]LeafRange, len(o.Leaves))
		}
		out.Leaves[origin] = normalizeRanges(append(out.Leaves[origin], ranges...))
	}
	if len(o.IDs) > 0 {
		out.IDs = mergeIDs(out.IDs, o.IDs)
	}
	return out
}

func mergeIDs(a, b []string) []string {
	set := make(map[string]struct{}, len(a)+len(b))
	for _, id := range a {
		set[id] = struct{}{}
	}
	for _, id := range b {
		set[id] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Intersect returns l ∩ o.
func (l Lineage) Intersect(o Lineage) Lineage {
	var out Lineage
	for origin, ranges := range l.Leaves {
		other, ok := o.Leaves[origin]
		if !ok {
			continue
		}
		var hits []LeafRange
		i, j := 0, 0
		for i < len(ranges) && j < len(other) {
			lo, hi := maxU64(ranges[i][0], other[j][0]), minU64(ranges[i][1], other[j][1])
			if lo <= hi {
				hits = append(hits, LeafRange{lo, hi})
			}
			if ranges[i][1] < other[j][1] {
				i++
			} else {
				j++
			}
		}
		if len(hits) > 0 {
			if out.Leaves == nil {
				out.Leaves = make(map[string][]LeafRange)
			}
			out.Leaves[origin] = hits
		}
	}
	if len(l.IDs) > 0 && len(o.IDs) > 0 {
		set := make(map[string]struct{}, len(o.IDs))
		for _, id := range o.IDs {
			set[id] = struct{}{}
		}
		for _, id := range l.IDs {
			if _, ok := set[id]; ok {
				out.IDs = append(out.IDs, id)
			}
		}
	}
	return out
}

// Subtract returns l \ o.
func (l Lineage) Subtract(o Lineage) Lineage {
	var out Lineage
	for origin, ranges := range l.Leaves {
		remaining := append([]LeafRange(nil), ranges...)
		for _, cut := range o.Leaves[origin] {
			var next []LeafRange
			for _, r := range remaining {
				if cut[1] < r[0] || cut[0] > r[1] {
					next = append(next, r)
					continue
				}
				if cut[0] > r[0] {
					next = append(next, LeafRange{r[0], cut[0] - 1})
				}
				if cut[1] < r[1] {
					next = append(next, LeafRange{cut[1] + 1, r[1]})
				}
			}
			remaining = next
		}
		if len(remaining) > 0 {
			if out.Leaves == nil {
				out.Leaves = make(map[string][]LeafRange)
			}
			out.Leaves[origin] = remaining
		}
	}
	if len(l.IDs) > 0 {
		set := make(map[string]struct{}, len(o.IDs))
		for _, id := range o.IDs {
			set[id] = struct{}{}
		}
		for _, id := range l.IDs {
			if _, ok := set[id]; !ok {
				out.IDs = append(out.IDs, id)
			}
		}
	}
	return out
}

// Intersects reports whether l and o share a leaf.
func (l Lineage) Intersects(o Lineage) bool {
	return !l.Intersect(o).IsEmpty()
}

// Covers reports whether every leaf of o is in l (o ⊆ l).
func (l Lineage) Covers(o Lineage) bool {
	return o.Subtract(l).IsEmpty()
}

// SameLeaves reports whether l and o name exactly the same leaves.
func (l Lineage) SameLeaves(o Lineage) bool {
	return l.Covers(o) && o.Covers(l)
}

// Derive maps every leaf of l into a namespace keyed by salt. An export
// that rewrites cells (a visibility stamp) holds different cells than its
// source, so it must not be mistaken for it: the same salt maps the same
// leaf to the same derived leaf, and ranges stay ranges.
func (l Lineage) Derive(salt string) Lineage {
	derive := func(kind, name string) string {
		sum := sha256.Sum256([]byte(kind + "\x00" + salt + "\x00" + name))
		return hex.EncodeToString(sum[:16])
	}
	var out Lineage
	for origin, ranges := range l.Leaves {
		if out.Leaves == nil {
			out.Leaves = make(map[string][]LeafRange, len(l.Leaves))
		}
		out.Leaves[derive("origin", origin)] = append([]LeafRange(nil), ranges...)
	}
	for _, id := range l.IDs {
		out.IDs = append(out.IDs, "d:"+derive("id", id))
	}
	sort.Strings(out.IDs)
	return out
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func minU64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
