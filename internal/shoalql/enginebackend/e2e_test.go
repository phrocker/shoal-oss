package enginebackend

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	"github.com/phrocker/shoal/internal/cclient"
	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/graphschema"
	"github.com/phrocker/shoal/internal/shoalql"
)

func packVec(v ...float32) []byte {
	b := make([]byte, 4*len(v))
	for i, f := range v {
		binary.BigEndian.PutUint32(b[i*4:], math.Float32bits(f))
	}
	return b
}

// writeEvent writes one event row with a content cell and an embedding cell.
func writeEvent(t *testing.T, eng *engine.Engine, id, content string, vec []float32) {
	t.Helper()
	m, _ := cclient.NewMutation([]byte(graphschema.EventRowPrefix + id))
	m.PutLatest(graphschema.ContentCF(), []byte{}, nil, []byte(content))
	m.PutLatest(graphschema.VectorCF(), []byte{}, nil, packVec(vec...))
	if err := eng.Write("graph", []*cclient.Mutation{m}); err != nil {
		t.Fatal(err)
	}
}

func newEngineWithEvents(t *testing.T) (*engine.Engine, *shoalql.Executor, shoalql.Catalog) {
	t.Helper()
	eng, err := engine.Open(t.TempDir(), engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CreateTable("graph", engine.TableOptions{}); err != nil {
		t.Fatal(err)
	}
	writeEvent(t, eng, "0001", "retry hit a timeout", []float32{1, 0})
	writeEvent(t, eng, "0002", "everything was fine", []float32{0, 1})
	writeEvent(t, eng, "0003", "another timeout retry loop", []float32{0.9, 0.1})
	if err := eng.Flush("graph"); err != nil {
		t.Fatal(err)
	}
	exec := shoalql.NewExecutor(New(eng))
	return eng, exec, shoalql.NewGraphCatalog("graph")
}

func runE2E(t *testing.T, cat shoalql.Catalog, exec *shoalql.Executor, sql string, opts shoalql.PlanOptions) *shoalql.Result {
	t.Helper()
	stmt, err := shoalql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	b, ok := cat.Binding(stmt.Table)
	if !ok {
		t.Fatalf("no binding for %q", stmt.Table)
	}
	p, err := shoalql.PlanQuery(context.Background(), stmt, b, opts)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	res, err := exec.Run(context.Background(), p)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return res
}

func TestE2E_ScanAndProject(t *testing.T) {
	eng, exec, cat := newEngineWithEvents(t)
	defer eng.Close()

	res := runE2E(t, cat, exec, "SELECT id, content FROM events", shoalql.PlanOptions{})
	if len(res.Rows) != 3 {
		t.Fatalf("rows = %d", len(res.Rows))
	}
	if res.Rows[0][0].Str != "0001" || res.Rows[0][1].Str != "retry hit a timeout" {
		t.Errorf("row0 = %v", res.Rows[0])
	}
}

func TestE2E_IdRange(t *testing.T) {
	eng, exec, cat := newEngineWithEvents(t)
	defer eng.Close()

	res := runE2E(t, cat, exec, "SELECT id FROM events WHERE id >= '0002'", shoalql.PlanOptions{})
	if len(res.Rows) != 2 {
		t.Fatalf("rows = %d: %+v", len(res.Rows), res.Rows)
	}
	if res.Rows[0][0].Str != "0002" || res.Rows[1][0].Str != "0003" {
		t.Errorf("rows = %+v", res.Rows)
	}
}

func TestE2E_MatchResidual(t *testing.T) {
	eng, exec, cat := newEngineWithEvents(t)
	defer eng.Close()

	res := runE2E(t, cat, exec, "SELECT id FROM events WHERE MATCH(content, 'timeout retry')", shoalql.PlanOptions{})
	if len(res.Rows) != 2 {
		t.Fatalf("rows = %d: %+v", len(res.Rows), res.Rows)
	}
}

func TestE2E_VectorKNN(t *testing.T) {
	eng, exec, cat := newEngineWithEvents(t)
	defer eng.Close()

	// Query near [1,0]; events 0001 and 0003 are closest by cosine.
	res := runE2E(t, cat, exec, "SELECT id, content FROM events ORDER BY embedding <-> [1, 0] LIMIT 2", shoalql.PlanOptions{})
	if len(res.Rows) != 2 {
		t.Fatalf("rows = %d: %+v", len(res.Rows), res.Rows)
	}
	got := map[string]bool{res.Rows[0][0].Str: true, res.Rows[1][0].Str: true}
	if !got["0001"] || !got["0003"] {
		t.Errorf("expected 0001 and 0003 nearest, got %+v", res.Rows)
	}
	// content must be hydrated (non-empty)
	for _, r := range res.Rows {
		if r[1].Str == "" {
			t.Errorf("content not hydrated: %+v", r)
		}
	}
}

func TestE2E_AggregateCountByCF(t *testing.T) {
	eng, exec, cat := newEngineWithEvents(t)
	defer eng.Close()

	res := runE2E(t, cat, exec, "SELECT cf, count(*) AS n FROM events GROUP BY cf", shoalql.PlanOptions{})
	// two CFs: content and vec, each with 3 cells.
	counts := map[string]float64{}
	for _, r := range res.Rows {
		counts[r[0].Str] = r[1].Num
	}
	if counts[string(graphschema.ContentCF())] != 3 || counts[string(graphschema.VectorCF())] != 3 {
		t.Errorf("counts = %+v", counts)
	}
}
