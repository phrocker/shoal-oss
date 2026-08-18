package accumulo

import (
	"bytes"
	"testing"
)

func TestKeyRangePreservesFullBounds(t *testing.T) {
	start := &Key{
		Row:              []byte("row"),
		ColumnFamily:     []byte("cf"),
		ColumnQualifier:  []byte("a"),
		ColumnVisibility: []byte("A"),
		Timestamp:        9,
	}
	end := &Key{
		Row:              []byte("row"),
		ColumnFamily:     []byte("cf"),
		ColumnQualifier:  []byte("z"),
		ColumnVisibility: []byte("B"),
		Timestamp:        3,
	}
	scanRange, err := NewKeyRange(start, false, end, true)
	if err != nil {
		t.Fatal(err)
	}
	start.Row[0] = 'X'
	end.ColumnQualifier[0] = 'X'

	wire := scanRange.toThrift()
	if wire.InfiniteStartKey || wire.InfiniteStopKey {
		t.Fatal("bounded key range was marked infinite")
	}
	if wire.StartKeyInclusive || !wire.StopKeyInclusive {
		t.Fatalf(
			"inclusivity = start %v, stop %v",
			wire.StartKeyInclusive,
			wire.StopKeyInclusive,
		)
	}
	if got := string(wire.Start.Row); got != "row" {
		t.Fatalf("start row = %q, want row", got)
	}
	if got := string(wire.Start.ColQualifier); got != "a" {
		t.Fatalf("start qualifier = %q, want a", got)
	}
	if got := string(wire.Stop.ColQualifier); got != "z" {
		t.Fatalf("stop qualifier = %q, want z", got)
	}
	if wire.Start.Timestamp != 9 || wire.Stop.Timestamp != 3 {
		t.Fatalf("timestamps = %d, %d", wire.Start.Timestamp, wire.Stop.Timestamp)
	}

	startCopy := scanRange.StartKey()
	startCopy.Row[0] = 'Y'
	if got := string(scanRange.StartKey().Row); got != "row" {
		t.Fatalf("range start mutated through accessor: %q", got)
	}
}

func TestKeyRangeUsesAccumuloTimestampOrdering(t *testing.T) {
	newer := &Key{Row: []byte("row"), Timestamp: 10}
	older := &Key{Row: []byte("row"), Timestamp: 1}
	if _, err := NewKeyRange(newer, true, older, true); err != nil {
		t.Fatalf("newer-to-older range rejected: %v", err)
	}
	if _, err := NewKeyRange(older, true, newer, true); err == nil {
		t.Fatal("older-to-newer range accepted despite descending timestamp order")
	}
}

func TestRowRangeInclusiveEndStillIncludesWholeRow(t *testing.T) {
	scanRange, err := NewRange([]byte("a"), true, []byte("z"), true)
	if err != nil {
		t.Fatal(err)
	}
	wire := scanRange.toThrift()
	if wire.StopKeyInclusive {
		t.Fatal("inclusive row endpoint was not normalized to an exclusive successor")
	}
	if !bytes.Equal(wire.Stop.Row, []byte{'z', 0}) {
		t.Fatalf("stop row = %q, want z\\x00", wire.Stop.Row)
	}
}

func TestKeyRangeRejectsReversedFields(t *testing.T) {
	start := &Key{Row: []byte("row"), ColumnFamily: []byte("z")}
	end := &Key{Row: []byte("row"), ColumnFamily: []byte("a")}
	if _, err := NewKeyRange(start, true, end, true); err == nil {
		t.Fatal("reversed full-key range accepted")
	}
}
