package accumulo

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/phrocker/shoal/internal/thrift/gen/data"
)

// Range describes a row-bounded scan range.
type Range struct {
	startRow       []byte
	endRow         []byte
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
		startRow:       start,
		endRow:         end,
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
	return cloneRow(r.startRow)
}

// EndRow returns a defensive copy of the upper row bound.
func (r *Range) EndRow() []byte {
	if r == nil {
		return nil
	}
	return cloneRow(r.endRow)
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
	if r.startRow == nil || r.startInclusive {
		return cloneRow(r.startRow)
	}
	row := make([]byte, len(r.startRow)+1)
	copy(row, r.startRow)
	return row
}

func (r *Range) fitsTablet(tablet Tablet) bool {
	if r.endRow == nil {
		return tablet.Extent.EndRow == nil
	}
	if tablet.Extent.EndRow == nil {
		return true
	}
	rangeStop := r.endRow
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
		InfiniteStartKey:  r.startRow == nil,
		InfiniteStopKey:   r.endRow == nil,
	}
	if r.startRow != nil {
		out.Start = &data.TKey{Row: cloneRow(r.startRow)}
	}
	if r.endRow != nil {
		end := cloneRow(r.endRow)
		if r.endInclusive {
			end = append(end, 0)
			out.StopKeyInclusive = false
		}
		out.Stop = &data.TKey{Row: end}
	}
	return out
}
