package metadata

import (
	"bytes"
	"testing"
)

func TestRootTabletExtent(t *testing.T) {
	e := rootTabletExtent()
	if !bytes.Equal(e.Table, []byte(RootTableID)) {
		t.Errorf("Table = %q, want %q", e.Table, RootTableID)
	}
	if e.EndRow != nil {
		t.Errorf("EndRow = %v, want nil", e.EndRow)
	}
	if e.PrevEndRow != nil {
		t.Errorf("PrevEndRow = %v, want nil", e.PrevEndRow)
	}
}

func TestTabletExtent(t *testing.T) {
	in := TabletInfo{
		TableID: "2k",
		EndRow:  []byte("k"),
		PrevRow: nil,
	}
	e := tabletExtent(in)
	if !bytes.Equal(e.Table, []byte("2k")) {
		t.Errorf("Table = %q", e.Table)
	}
	if !bytes.Equal(e.EndRow, []byte("k")) {
		t.Errorf("EndRow = %q", e.EndRow)
	}
	if e.PrevEndRow != nil {
		t.Errorf("PrevEndRow = %v, want nil", e.PrevEndRow)
	}
}

func TestTabletExtent_LastTablet(t *testing.T) {
	in := TabletInfo{TableID: "2k", EndRow: nil, PrevRow: []byte("p")}
	e := tabletExtent(in)
	if e.EndRow != nil {
		t.Errorf("EndRow = %v, want nil for default tablet", e.EndRow)
	}
	if !bytes.Equal(e.PrevEndRow, []byte("p")) {
		t.Errorf("PrevEndRow = %q", e.PrevEndRow)
	}
}

func TestFullRange(t *testing.T) {
	r := fullRange()
	if !r.InfiniteStartKey || !r.InfiniteStopKey {
		t.Errorf("expected both ends infinite, got %+v", r)
	}
	if r.Start != nil || r.Stop != nil {
		t.Errorf("expected nil Start/Stop, got %+v / %+v", r.Start, r.Stop)
	}
}
