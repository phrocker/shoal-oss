package accumulo

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/phrocker/shoal/internal/thrift/gen/data"
)

// Range describes a row-bounded scan range.
type Range struct {
	startKey       *Key
	endKey         *Key
	startRowOnly   bool
	endRowOnly     bool
	startInclusive bool
	endInclusive   bool
}

// NewRange constructs a range. A nil start or end row is unbounded.
func NewRange(startRow []byte, startInclusive bool, endRow []byte, endInclusive bool) (*Range, error) {
	start := cloneRow(startRow)
	end := cloneRow(endRow)
	if start != nil && end != nil && bytes.Compare(end, start) < 0 {
		return nil, fmt.Errorf("accumulo: range end row %q precedes start row %q", end, start)
	}
	return &Range{
		startKey:       keyForRow(start),
		endKey:         keyForRow(end),
		startRowOnly:   start != nil,
		endRowOnly:     end != nil,
		startInclusive: startInclusive,
		endInclusive:   endInclusive,
	}, nil
}

// NewKeyRange constructs a range with full Accumulo key bounds. A nil start or
// end key is unbounded.
func NewKeyRange(start *Key, startInclusive bool, end *Key, endInclusive bool) (*Range, error) {
	startCopy := cloneKey(start)
	endCopy := cloneKey(end)
	if startCopy != nil && endCopy != nil && compareKeys(*endCopy, *startCopy) < 0 {
		return nil, errors.New("accumulo: range end key precedes start key")
	}
	return &Range{
		startKey:       startCopy,
		endKey:         endCopy,
		startInclusive: startInclusive,
		endInclusive:   endInclusive,
	}, nil
}

// NewRangeRow constructs an inclusive range containing one row.
func NewRangeRow(row []byte) (*Range, error) {
	if len(row) == 0 {
		return nil, errors.New("accumulo: range row is empty")
	}
	return NewRange(row, true, row, true)
}

// InfiniteRange constructs the unbounded range (-infinity, +infinity).
func InfiniteRange() *Range {
	return &Range{startInclusive: true}
}

// StartRow returns a defensive copy of the lower row bound.
func (r *Range) StartRow() []byte {
	if r == nil {
		return nil
	}
	if r.startKey == nil {
		return nil
	}
	return cloneRow(r.startKey.Row)
}

// EndRow returns a defensive copy of the upper row bound.
func (r *Range) EndRow() []byte {
	if r == nil {
		return nil
	}
	if r.endKey == nil {
		return nil
	}
	return cloneRow(r.endKey.Row)
}

// StartKey returns a defensive copy of the full lower key bound. It is nil for
// an unbounded start.
func (r *Range) StartKey() *Key {
	if r == nil {
		return nil
	}
	return cloneKey(r.startKey)
}

// EndKey returns a defensive copy of the full upper key bound. It is nil for
// an unbounded end.
func (r *Range) EndKey() *Key {
	if r == nil {
		return nil
	}
	return cloneKey(r.endKey)
}

// StartInclusive reports whether the lower row bound is included.
func (r *Range) StartInclusive() bool {
	return r != nil && r.startInclusive
}

// EndInclusive reports whether the upper row bound is included.
func (r *Range) EndInclusive() bool {
	return r != nil && r.endInclusive
}

func (r *Range) routingRow() []byte {
	if r.startKey == nil {
		return nil
	}
	if r.startInclusive || !r.startRowOnly {
		return cloneRow(r.startKey.Row)
	}
	row := make([]byte, len(r.startKey.Row)+1)
	copy(row, r.startKey.Row)
	return row
}

func (r *Range) fitsTablet(tablet Tablet) bool {
	if r.endKey == nil {
		return tablet.Extent.EndRow == nil
	}
	if tablet.Extent.EndRow == nil {
		return true
	}
	rangeStop := r.endKey.Row
	if r.endInclusive {
		rangeStop = append(cloneRow(rangeStop), 0)
	}
	tabletStop := append(cloneRow(tablet.Extent.EndRow), 0)
	return bytes.Compare(rangeStop, tabletStop) <= 0
}

func (r *Range) toThrift() *data.TRange {
	out := &data.TRange{
		StartKeyInclusive: r.startInclusive,
		StopKeyInclusive:  r.endInclusive,
		InfiniteStartKey:  r.startKey == nil,
		InfiniteStopKey:   r.endKey == nil,
	}
	if r.startKey != nil {
		out.Start = keyToThrift(r.startKey)
	}
	if r.endKey != nil {
		out.Stop = keyToThrift(r.endKey)
		if r.endRowOnly && r.endInclusive {
			out.Stop.Row = append(out.Stop.Row, 0)
			out.StopKeyInclusive = false
		}
	}
	return out
}

func keyForRow(row []byte) *Key {
	if row == nil {
		return nil
	}
	return &Key{Row: cloneRow(row)}
}

func keyToThrift(key *Key) *data.TKey {
	if key == nil {
		return nil
	}
	return &data.TKey{
		Row:           cloneRow(key.Row),
		ColFamily:     cloneRow(key.ColumnFamily),
		ColQualifier:  cloneRow(key.ColumnQualifier),
		ColVisibility: cloneRow(key.ColumnVisibility),
		Timestamp:     key.Timestamp,
	}
}

func cloneKey(key *Key) *Key {
	if key == nil {
		return nil
	}
	return &Key{
		Row:              cloneRow(key.Row),
		ColumnFamily:     cloneRow(key.ColumnFamily),
		ColumnQualifier:  cloneRow(key.ColumnQualifier),
		ColumnVisibility: cloneRow(key.ColumnVisibility),
		Timestamp:        key.Timestamp,
	}
}

func compareKeys(left, right Key) int {
	for _, pair := range [][2][]byte{
		{left.Row, right.Row},
		{left.ColumnFamily, right.ColumnFamily},
		{left.ColumnQualifier, right.ColumnQualifier},
		{left.ColumnVisibility, right.ColumnVisibility},
	} {
		if comparison := bytes.Compare(pair[0], pair[1]); comparison != 0 {
			return comparison
		}
	}
	switch {
	case left.Timestamp > right.Timestamp:
		return -1
	case left.Timestamp < right.Timestamp:
		return 1
	default:
		return 0
	}
}
