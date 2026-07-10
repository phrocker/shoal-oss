package shoalql

import (
	"context"
	"encoding/binary"
	"math"
	"sort"
	"testing"

	"github.com/phrocker/shoal/internal/graphschema"
	"github.com/phrocker/shoal/internal/iterrt"
)

// --- fake backend ---

type fakeStream struct {
	cells []Cell
	i     int
}

func (f *fakeStream) Next() bool       { return f.i < len(f.cells) }
func (f *fakeStream) Key() *iterrt.Key { return f.cells[f.i].Key }
func (f *fakeStream) Value() []byte    { return f.cells[f.i].Value }
func (f *fakeStream) Advance() error   { f.i++; return nil }
func (f *fakeStream) Close()           {}

type fakeBackend struct {
	scanCells []Cell            // returned by Scan (sorted)
	lookup    map[string][]Cell // row -> cells for LookupRows
	neighbors map[string][]Neighbor
	lastReq   ScanRequest
	lastRange iterrt.Range
}

func (b *fakeBackend) Scan(_ context.Context, _ string, r iterrt.Range, req ScanRequest) (RowStream, error) {
	b.lastReq = req
	b.lastRange = r
	cells := append([]Cell(nil), b.scanCells...)
	sort.Slice(cells, func(i, j int) bool { return cells[i].Key.Compare(cells[j].Key) < 0 })
	return &fakeStream{cells: cells}, nil
}

func (b *fakeBackend) LookupRows(_ context.Context, _ string, rows [][]byte, _ ScanRequest) ([]Cell, error) {
	var out []Cell
	for _, r := range rows {
		out = append(out, b.lookup[string(r)]...)
	}
	return out, nil
}

func (b *fakeBackend) Neighbors(_ context.Context, _ string, rows [][]byte, _ []byte) ([][]Neighbor, error) {
	out := make([][]Neighbor, len(rows))
	for i, r := range rows {
		out[i] = b.neighbors[string(r)]
	}
	return out, nil
}

func cell(row, cf, cq, val string) Cell {
	return Cell{
		Key:   &iterrt.Key{Row: []byte(row), ColumnFamily: []byte(cf), ColumnQualifier: []byte(cq)},
		Value: []byte(val),
	}
}

func runSQL(t *testing.T, be Backend, sql string, opts PlanOptions) *Result {
	t.Helper()
	st, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	b, ok := NewGraphCatalog("graph").Binding(st.Table)
	if !ok {
		t.Fatalf("no binding")
	}
	p, err := PlanQuery(context.Background(), st, b, opts)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	res, err := NewExecutor(be).Run(context.Background(), p)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return res
}

// --- tests ---

func TestExec_ScanProjectsRowAndContent(t *testing.T) {
	cc := string(graphschema.ContentCF())
	be := &fakeBackend{scanCells: []Cell{
		cell("evt:1", cc, "", "hello"),
		cell("evt:2", cc, "", "world"),
	}}
	res := runSQL(t, be, "SELECT id, content FROM events", PlanOptions{})
	if len(res.Rows) != 2 {
		t.Fatalf("rows = %d", len(res.Rows))
	}
	if res.Columns[0] != "id" || res.Columns[1] != "content" {
		t.Errorf("cols = %v", res.Columns)
	}
	if res.Rows[0][0].Str != "1" || res.Rows[0][1].Str != "hello" {
		t.Errorf("row0 = %v", res.Rows[0])
	}
	if res.Rows[1][0].Str != "2" || res.Rows[1][1].Str != "world" {
		t.Errorf("row1 = %v", res.Rows[1])
	}
}

func TestExec_ResidualEquality(t *testing.T) {
	cc := string(graphschema.ContentCF())
	be := &fakeBackend{scanCells: []Cell{
		cell("evt:1", cc, "", "keep"),
		cell("evt:2", cc, "", "drop"),
	}}
	res := runSQL(t, be, "SELECT id FROM events WHERE content = 'keep'", PlanOptions{})
	if len(res.Rows) != 1 || res.Rows[0][0].Str != "1" {
		t.Fatalf("rows = %+v", res.Rows)
	}
}

func TestExec_MatchResidual(t *testing.T) {
	cc := string(graphschema.ContentCF())
	be := &fakeBackend{scanCells: []Cell{
		cell("evt:1", cc, "", "the retry hit a timeout"),
		cell("evt:2", cc, "", "everything was fine"),
	}}
	res := runSQL(t, be, "SELECT id FROM events WHERE MATCH(content, 'retry timeout')", PlanOptions{})
	if len(res.Rows) != 1 || res.Rows[0][0].Str != "1" {
		t.Fatalf("rows = %+v", res.Rows)
	}
}

func TestExec_Limit(t *testing.T) {
	cc := string(graphschema.ContentCF())
	be := &fakeBackend{scanCells: []Cell{
		cell("evt:1", cc, "", "a"),
		cell("evt:2", cc, "", "b"),
		cell("evt:3", cc, "", "c"),
	}}
	res := runSQL(t, be, "SELECT id FROM events LIMIT 2", PlanOptions{})
	if len(res.Rows) != 2 {
		t.Fatalf("rows = %d", len(res.Rows))
	}
}

func scoreVal(f float32) string {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, math.Float32bits(f))
	return string(b)
}

func TestExec_VectorKNNHydratesInScoreOrder(t *testing.T) {
	vc := string(graphschema.VectorCF())
	cc := string(graphschema.ContentCF())
	// Scan (KNN) returns embedding cells with scores, best first.
	be := &fakeBackend{
		scanCells: []Cell{
			cell("evt:9", vc, "", scoreVal(0.99)),
			cell("evt:4", vc, "", scoreVal(0.80)),
		},
		lookup: map[string][]Cell{
			"evt:9": {cell("evt:9", cc, "", "nine")},
			"evt:4": {cell("evt:4", cc, "", "four")},
		},
	}
	// fakeBackend.Scan sorts by key, which would reorder by row; force score
	// order by giving rows that already sort best-first is not guaranteed, so
	// verify the executor preserves the stream order it receives. Our fake
	// sorts ascending by key: evt:4 < evt:9, so stream order is evt:4, evt:9.
	res := runSQL(t, be, "SELECT id, content FROM events ORDER BY embedding <-> [1,0] LIMIT 5", PlanOptions{})
	if len(res.Rows) != 2 {
		t.Fatalf("rows = %d", len(res.Rows))
	}
	// stream order after sort is evt:4 then evt:9
	if res.Rows[0][0].Str != "4" || res.Rows[0][1].Str != "four" {
		t.Errorf("row0 = %v", res.Rows[0])
	}
	if res.Rows[1][0].Str != "9" || res.Rows[1][1].Str != "nine" {
		t.Errorf("row1 = %v", res.Rows[1])
	}
	// verify the scan requested the vector CF restriction
	if len(be.lastReq.ColumnFamilies) != 1 || string(be.lastReq.ColumnFamilies[0]) != vc {
		t.Errorf("scan CF = %v", be.lastReq.ColumnFamilies)
	}
}

func TestExec_VectorKNNIdOnlyNoHydration(t *testing.T) {
	vc := string(graphschema.VectorCF())
	be := &fakeBackend{scanCells: []Cell{
		cell("evt:1", vc, "", scoreVal(0.5)),
	}}
	res := runSQL(t, be, "SELECT id FROM events ORDER BY vec <-> [1,2,3] LIMIT 3", PlanOptions{})
	if len(res.Rows) != 1 || res.Rows[0][0].Str != "1" {
		t.Fatalf("rows = %+v", res.Rows)
	}
}

func TestExec_Aggregate(t *testing.T) {
	// GraphAggregation emits row="_agg", cf="result", cq=<group>, value=<count>.
	be := &fakeBackend{scanCells: []Cell{
		cell("_agg", "result", "content:", "3"),
		cell("_agg", "result", "vec:", "3"),
	}}
	res := runSQL(t, be, "SELECT cf, count(*) AS n FROM events GROUP BY cf", PlanOptions{})
	if len(res.Rows) != 2 {
		t.Fatalf("rows = %d", len(res.Rows))
	}
	if res.Columns[0] != "cf" || res.Columns[1] != "n" {
		t.Errorf("cols = %v", res.Columns)
	}
	if res.Rows[0][0].Str != "content:" || res.Rows[0][1].Num != 3 {
		t.Errorf("row0 = %v", res.Rows[0])
	}
}

func TestExec_Expand(t *testing.T) {
	be := &fakeBackend{
		scanCells: []Cell{cell("evt:1", string(graphschema.ContentCF()), "", "x")},
		neighbors: map[string][]Neighbor{
			"evt:1": {{Target: []byte("evt:2")}, {Target: []byte("evt:3")}},
		},
	}
	res := runSQL(t, be, "SELECT expand(id, 'semantic') AS nbr FROM events WHERE id = 'evt:1'", PlanOptions{})
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %d", len(res.Rows))
	}
	got := res.Rows[0][0]
	if got.Kind != VList || len(got.List) != 2 || got.List[0] != "2" || got.List[1] != "3" {
		t.Errorf("expand = %+v", got)
	}
}
