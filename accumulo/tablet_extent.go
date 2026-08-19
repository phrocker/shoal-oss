package accumulo

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

// MetadataTableID is the table that holds tablet metadata rows.
const MetadataTableID = "!0"

// ErrInvalidTabletExtent reports an extent that cannot describe a tablet.
var ErrInvalidTabletExtent = errors.New("accumulo: invalid tablet extent")

// RootTabletExtent is the extent of the tablet that holds the metadata table's
// own entries, which is where tablet lookups start.
var RootTabletExtent = TabletExtent{
	TableID: MetadataTableID,
	EndRow:  []byte(MetadataEntry(MetadataTableID, nil)),
}

// MetadataEntry renders the metadata row that names a tablet: "<table>;<row>"
// for a bounded tablet and "<table><" for the last tablet of the table, which
// has no end row.
func MetadataEntry(tableID string, endRow []byte) string {
	if len(endRow) == 0 {
		return tableID + "<"
	}
	return tableID + ";" + string(endRow)
}

// NewTabletExtent builds an extent. A nil or empty endRow means the tablet has
// no upper bound and a nil or empty prevRow means it has no lower bound, which
// is how the discovery layer already reads the two fields. Both slices are
// copied.
func NewTabletExtent(tableID string, endRow, prevRow []byte) (TabletExtent, error) {
	if tableID == "" {
		return TabletExtent{}, fmt.Errorf("%w: table id is empty", ErrInvalidTabletExtent)
	}
	if len(endRow) > 0 && len(prevRow) > 0 && bytes.Compare(prevRow, endRow) >= 0 {
		return TabletExtent{}, fmt.Errorf(
			"%w: previous end row %q is not before end row %q",
			ErrInvalidTabletExtent,
			prevRow,
			endRow,
		)
	}
	return TabletExtent{
		TableID: tableID,
		EndRow:  cloneRow(endRow),
		PrevRow: cloneRow(prevRow),
	}, nil
}

// ParseTabletExtent decodes a metadata row - "<table>;<row>" or "<table><" -
// into an extent, taking the previous end row from the caller. The row is
// searched byte by byte, so a table id or end row of any length decodes.
func ParseTabletExtent(metadataRow string, prevRow []byte) (TabletExtent, error) {
	if metadataRow == "" {
		return TabletExtent{}, fmt.Errorf("%w: metadata row is empty", ErrInvalidTabletExtent)
	}
	if strings.HasSuffix(metadataRow, "<") {
		return NewTabletExtent(metadataRow[:len(metadataRow)-1], nil, prevRow)
	}
	separator := strings.IndexByte(metadataRow, ';')
	if separator < 0 {
		return TabletExtent{}, fmt.Errorf(
			"%w: metadata row %q contains neither ';' nor '<'",
			ErrInvalidTabletExtent,
			metadataRow,
		)
	}
	return NewTabletExtent(
		metadataRow[:separator],
		[]byte(metadataRow[separator+1:]),
		prevRow,
	)
}

// Clone returns a copy that shares no storage with the receiver.
func (e TabletExtent) Clone() TabletExtent {
	return TabletExtent{
		TableID: e.TableID,
		EndRow:  cloneRow(e.EndRow),
		PrevRow: cloneRow(e.PrevRow),
	}
}

// SetTableID replaces the table id.
func (e *TabletExtent) SetTableID(tableID string) { e.TableID = tableID }

// SetPrevRow replaces the previous end row with a copy of prevRow.
func (e *TabletExtent) SetPrevRow(prevRow []byte) { e.PrevRow = cloneRow(prevRow) }

// MetadataEntry renders the metadata row that names this tablet.
func (e TabletExtent) MetadataEntry() string { return MetadataEntry(e.TableID, e.EndRow) }

// Range returns the rows the tablet holds: everything after the previous end
// row up to and including the end row. An absent bound is unbounded.
func (e TabletExtent) Range() (*Range, error) {
	return NewRange(e.PrevRow, false, e.EndRow, true)
}

// Contains reports whether row falls inside the tablet.
func (e TabletExtent) Contains(row []byte) bool {
	if len(e.PrevRow) > 0 && bytes.Compare(row, e.PrevRow) <= 0 {
		return false
	}
	return len(e.EndRow) == 0 || bytes.Compare(row, e.EndRow) <= 0
}

// Compare orders two extents by table id, then by end row, then by previous
// end row. An absent end row is the tablet with no upper bound, so it sorts
// last; an absent previous end row is the tablet with no lower bound, so it
// sorts first. It returns a negative number, zero, or a positive number.
func (e TabletExtent) Compare(other TabletExtent) int {
	if order := strings.Compare(e.TableID, other.TableID); order != 0 {
		return order
	}
	if order := compareUpperBound(e.EndRow, other.EndRow); order != 0 {
		return order
	}
	return compareLowerBound(e.PrevRow, other.PrevRow)
}

// compareUpperBound orders end rows, treating an absent bound as the largest.
func compareUpperBound(left, right []byte) int {
	switch {
	case len(left) == 0 && len(right) == 0:
		return 0
	case len(left) == 0:
		return 1
	case len(right) == 0:
		return -1
	default:
		return bytes.Compare(left, right)
	}
}

// compareLowerBound orders previous end rows, treating an absent bound as the
// smallest.
func compareLowerBound(left, right []byte) int {
	switch {
	case len(left) == 0 && len(right) == 0:
		return 0
	case len(left) == 0:
		return -1
	case len(right) == 0:
		return 1
	default:
		return bytes.Compare(left, right)
	}
}

// Less reports whether the extent sorts before other under Compare.
func (e TabletExtent) Less(other TabletExtent) bool { return e.Compare(other) < 0 }

// Equal reports whether two extents describe the same tablet. It is
// Compare(other) == 0, so ordering and equality never disagree.
func (e TabletExtent) Equal(other TabletExtent) bool { return e.Compare(other) == 0 }

// String renders the extent in the pinned Sharkbite form, where an absent
// bound is written as "<".
func (e TabletExtent) String() string {
	return fmt.Sprintf(
		"tableId:%s end:%s prev:%s",
		e.TableID,
		boundText(e.EndRow),
		boundText(e.PrevRow),
	)
}

func boundText(bound []byte) string {
	if len(bound) == 0 {
		return "<"
	}
	return string(bound)
}
