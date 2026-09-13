package parquetfile

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/phrocker/shoal-oss/internal/iterrt"
	"github.com/phrocker/shoal-oss/internal/rfile/wire"
)

func TestEncodeDecodePreservesAccumuloKey(t *testing.T) {
	cells := []iterrt.Cell{
		{
			Key: &wire.Key{
				Row:              []byte{0x00, 0xff},
				ColumnFamily:     []byte("cf"),
				ColumnQualifier:  []byte("cq"),
				ColumnVisibility: []byte("private"),
				Timestamp:        42,
				Deleted:          true,
			},
			Value: []byte{0xfe, 0x00},
		},
	}

	src := iterrt.NewSliceSource(cells)
	if err := src.Init(nil, nil, iterrt.IteratorEnvironment{}); err != nil {
		t.Fatal(err)
	}
	if err := src.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
		t.Fatal(err)
	}

	data, count, err := Encode(src)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
	got, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].Key.Equal(cells[0].Key) || !bytes.Equal(got[0].Value, cells[0].Value) {
		t.Fatalf("round trip = %+v", got)
	}
}

func TestEncodeCompressesRepeatedCells(t *testing.T) {
	const (
		cellCount           = 8192
		timestampBytes      = 8
		deletedBytes        = 1
		minCompressionRatio = 4 // Repeated fields should shrink to less than one quarter.
	)
	cells := make([]iterrt.Cell, cellCount)
	var rawSize int
	for i := range cells {
		cells[i] = iterrt.Cell{
			Key: &wire.Key{
				Row:              []byte(fmt.Sprintf("obs:project:%06d", i)),
				ColumnFamily:     []byte("crawl"),
				ColumnQualifier:  []byte("title"),
				ColumnVisibility: []byte(""),
				Timestamp:        42,
			},
			Value: []byte("A repeated observation title"),
		}
		rawSize += len(cells[i].Key.Row) +
			len(cells[i].Key.ColumnFamily) +
			len(cells[i].Key.ColumnQualifier) +
			len(cells[i].Key.ColumnVisibility) +
			timestampBytes + deletedBytes + len(cells[i].Value)
	}
	src := iterrt.NewSliceSource(cells)
	if err := src.Init(nil, nil, iterrt.IteratorEnvironment{}); err != nil {
		t.Fatal(err)
	}
	if err := src.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
		t.Fatal(err)
	}

	data, count, err := Encode(src)
	if err != nil {
		t.Fatal(err)
	}
	if count != cellCount {
		t.Fatalf("count = %d, want %d", count, cellCount)
	}
	if len(data)*minCompressionRatio >= rawSize {
		t.Fatalf("encoded size = %d, want less than one quarter of raw size %d", len(data), rawSize)
	}
	got, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(cells) {
		t.Fatalf("decoded cells = %d, want %d", len(got), len(cells))
	}
	for i := range cells {
		if !got[i].Key.Equal(cells[i].Key) || !bytes.Equal(got[i].Value, cells[i].Value) {
			t.Fatalf("cell %d did not round-trip", i)
		}
	}
}
