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

package tserver

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// Extent identifies one tablet: the table it belongs to and the row range it
// covers. The range is half-open the way Accumulo's KeyExtent is — the prev
// end row is exclusive and the end row is inclusive, so a tablet covers
// (PrevEndRow, EndRow].
//
// A nil bound is infinite: nil PrevEndRow starts at the beginning of the
// table, nil EndRow runs to its end. Nil and empty are different — an empty
// (non-nil) bound is the empty row, not infinity.
type Extent struct {
	TableID    string
	PrevEndRow []byte
	EndRow     []byte
}

// Validate reports whether the extent is a well-formed tablet identity. An
// extent with no table, or whose bounds are inverted or empty, cannot name a
// tablet and is refused rather than tracked.
func (e Extent) Validate() error {
	if e.TableID == "" {
		return fmt.Errorf("%w: empty table id", ErrInvalidExtent)
	}
	if e.PrevEndRow != nil && e.EndRow != nil && bytes.Compare(e.PrevEndRow, e.EndRow) >= 0 {
		return fmt.Errorf("%w: prev end row %q is not below end row %q",
			ErrInvalidExtent, e.PrevEndRow, e.EndRow)
	}
	return nil
}

// Equal reports whether two extents name the same tablet. Bound identity
// includes nil-vs-empty: (nil, "m"] and ("", "m"] are different tablets.
func (e Extent) Equal(other Extent) bool {
	return e.TableID == other.TableID &&
		boundsEqual(e.PrevEndRow, other.PrevEndRow) &&
		boundsEqual(e.EndRow, other.EndRow)
}

// Overlaps reports whether two extents of the same table share any row. It is
// what stops a stale pre-split (or post-merge) extent from being hosted
// alongside the tablets that replaced it: the ranges intersect, so accepting
// both would host the same rows twice.
func (e Extent) Overlaps(other Extent) bool {
	if e.TableID != other.TableID {
		return false
	}
	return !endsAtOrBelow(e.EndRow, other.PrevEndRow) &&
		!endsAtOrBelow(other.EndRow, e.PrevEndRow)
}

// String renders the extent in Accumulo's KeyExtent form —
// "tableId;endRow;prevEndRow" with "<" for an infinite bound — so operators
// can match Shoal's logs against a Java tserver's.
func (e Extent) String() string {
	return e.TableID + ";" + boundString(e.EndRow) + ";" + boundString(e.PrevEndRow)
}

// key is the canonical map key for an extent. The table id is length-prefixed
// and the bounds are hex encoded with an explicit infinity marker, so no two
// distinct extents can collide.
func (e Extent) key() string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(len(e.TableID)))
	b.WriteByte(':')
	b.WriteString(e.TableID)
	writeBoundKey(&b, e.PrevEndRow)
	writeBoundKey(&b, e.EndRow)
	return b.String()
}

// clone deep-copies the extent's bounds so tracked state cannot be mutated
// through a slice the caller still holds.
func (e Extent) clone() Extent {
	return Extent{
		TableID:    e.TableID,
		PrevEndRow: cloneBound(e.PrevEndRow),
		EndRow:     cloneBound(e.EndRow),
	}
}

func cloneBound(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func writeBoundKey(b *strings.Builder, bound []byte) {
	if bound == nil {
		b.WriteString(";<")
		return
	}
	b.WriteString(";=")
	b.WriteString(hex.EncodeToString(bound))
}

func boundString(bound []byte) string {
	if bound == nil {
		return "<"
	}
	return string(bound)
}

func boundsEqual(a, b []byte) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return bytes.Equal(a, b)
}

// endsAtOrBelow reports whether an inclusive upper bound sits at or below an
// exclusive lower bound, i.e. the two ranges do not meet. An infinite upper
// bound is above everything and an infinite lower bound is below everything,
// so neither can separate two ranges.
func endsAtOrBelow(end, prev []byte) bool {
	if end == nil || prev == nil {
		return false
	}
	return bytes.Compare(end, prev) <= 0
}

// compareExtents orders extents by table, then by range. Infinite bounds sort
// to the outside: a nil prev end row is below every row, a nil end row is
// above every row.
func compareExtents(a, b Extent) int {
	if c := strings.Compare(a.TableID, b.TableID); c != 0 {
		return c
	}
	if c := compareLowerBounds(a.PrevEndRow, b.PrevEndRow); c != 0 {
		return c
	}
	return compareUpperBounds(a.EndRow, b.EndRow)
}

func compareLowerBounds(a, b []byte) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return -1
	case b == nil:
		return 1
	default:
		return bytes.Compare(a, b)
	}
}

func compareUpperBounds(a, b []byte) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return 1
	case b == nil:
		return -1
	default:
		return bytes.Compare(a, b)
	}
}
