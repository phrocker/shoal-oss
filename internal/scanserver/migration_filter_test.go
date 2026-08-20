package scanserver

import (
	"testing"

	"github.com/phrocker/shoal/internal/metadata"
	"github.com/phrocker/shoal/internal/rfile/wire"
)

func TestHasMigrationProcessorFiltersRows(t *testing.T) {
	processor := newMetadataColumnFilter(metadata.CFServer, "migration")
	processor.offer(&wire.Key{
		Row: []byte("1;a"), ColumnFamily: []byte(metadata.CFTabletSection),
		ColumnQualifier: []byte(metadata.CQPrevRow),
	}, []byte{0})
	processor.offer(&wire.Key{
		Row: []byte("1;b"), ColumnFamily: []byte(metadata.CFServer),
		ColumnQualifier: []byte("migration"),
	}, []byte("tserver:9997[session]"))
	processor.offer(&wire.Key{
		Row: []byte("1;b"), ColumnFamily: []byte(metadata.CFTabletSection),
		ColumnQualifier: []byte(metadata.CQPrevRow),
	}, []byte{1, 'a'})

	got := processor.drain()
	if len(got) != 2 {
		t.Fatalf("migration filter returned %d cells, want 2", len(got))
	}
	for _, cell := range got {
		if string(cell.Key.Row) != "1;b" {
			t.Fatalf("migration filter returned row %q", cell.Key.Row)
		}
	}
}
