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

package iterrt

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func initGraphRank(t *testing.T, src SortedKeyValueIterator, opts map[string]string) *GraphRankIterator {
	t.Helper()
	g := NewGraphRankIterator()
	if err := g.Init(src, opts, IteratorEnvironment{Scope: ScopeMajc, FullMajorCompaction: true}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := g.Seek(InfiniteRange(), nil, false); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	return g
}

func sortGraphRankInput(cells []kv) []kv {
	out := append([]kv(nil), cells...)
	sort.Slice(out, func(i, j int) bool { return out[i].k.Compare(out[j].k) < 0 })
	return out
}

func graphRankRankCells(t *testing.T, cells []kv, cf, cq string) map[string]kv {
	t.Helper()
	out := map[string]kv{}
	for _, cell := range cells {
		if string(cell.k.ColumnFamily) != cf || string(cell.k.ColumnQualifier) != cq {
			continue
		}
		row := string(cell.k.Row)
		if _, dup := out[row]; dup {
			t.Fatalf("duplicate rank cell for row %q", row)
		}
		if _, err := strconv.ParseFloat(string(cell.v), 64); err != nil {
			t.Fatalf("rank value for row %q is not a float: %q", row, cell.v)
		}
		out[row] = cell
	}
	return out
}

func graphRankFloat(t *testing.T, ranks map[string]kv, row string) float64 {
	t.Helper()
	cell, ok := ranks[row]
	if !ok {
		t.Fatalf("missing rank for %q", row)
	}
	v, err := strconv.ParseFloat(string(cell.v), 64)
	if err != nil {
		t.Fatalf("rank %q for %q: %v", cell.v, row, err)
	}
	return v
}

func assertClose(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-12 {
		t.Fatalf("%s: got %.17g want %.17g", name, got, want)
	}
}

func serializedGraphRankCells(cells []kv) string {
	var b strings.Builder
	for _, cell := range cells {
		fmt.Fprintf(&b, "%x|%x|%x|%x|%d|%t|%x\n",
			cell.k.Row,
			cell.k.ColumnFamily,
			cell.k.ColumnQualifier,
			cell.k.ColumnVisibility,
			cell.k.Timestamp,
			cell.k.Deleted,
			cell.v)
	}
	return b.String()
}

func TestGraphRank_EmitsPageRankForIncomingEdges(t *testing.T) {
	cells := sortGraphRankInput([]kv{
		{mk("A", "V", "_label", "visA", 10), []byte("alpha")},
		{mk("A", "E_link", "B", "", 11), []byte("{}")},
		{mk("B", "V", "_label", "visB", 12), []byte("bravo")},
		{mk("C", "V", "_label", "visC", 13), []byte("charlie")},
		{mk("C", "E_link", "B", "", 14), []byte("{}")},
	})
	g := initGraphRank(t, newSliceSource(cells...), map[string]string{GraphRankMaxIterations: "1"})
	got, err := drain(g)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	ranks := graphRankRankCells(t, got, "V", "_rank")
	if len(ranks) != 3 {
		t.Fatalf("rank count = %d, want 3", len(ranks))
	}
	assertClose(t, "rank A", graphRankFloat(t, ranks, "A"), 0.05)
	assertClose(t, "rank B", graphRankFloat(t, ranks, "B"), 0.6166666666666667)
	assertClose(t, "rank C", graphRankFloat(t, ranks, "C"), 0.05)
	if string(ranks["B"].k.ColumnVisibility) != "visB" {
		t.Fatalf("rank B visibility = %q, want %q", ranks["B"].k.ColumnVisibility, "visB")
	}
}

func TestGraphRank_SourceCellsPassThroughUnchanged(t *testing.T) {
	cells := sortGraphRankInput([]kv{
		{mk("A", "V", "_label", "visA", 10), []byte("alpha")},
		{mk("A", "V", "color", "visProp", 9), []byte("blue")},
		{mk("A", "E_link", "B", "edgeVis", 8), []byte(`{"w":1}`)},
		{mk("B", "V", "_label", "", 7), []byte("bravo")},
	})
	g := initGraphRank(t, newSliceSource(cells...), map[string]string{GraphRankMaxIterations: "1"})
	got, err := drain(g)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	for _, want := range cells {
		found := false
		for _, candidate := range got {
			if want.k.Compare(candidate.k) == 0 && bytes.Equal(want.v, candidate.v) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("source cell did not pass through unchanged: key=%+v value=%q", want.k, want.v)
		}
	}
}

func TestGraphRank_EmptyInput(t *testing.T) {
	g := initGraphRank(t, newSliceSource(), nil)
	got, err := drain(g)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty input emitted %d cells", len(got))
	}
}

func TestGraphRank_SingleDanglingVertex(t *testing.T) {
	cells := sortGraphRankInput([]kv{{mk("solo", "V", "_label", "soloVis", 41), []byte("solo")}})
	g := initGraphRank(t, newSliceSource(cells...), map[string]string{GraphRankMaxIterations: "2"})
	got, err := drain(g)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	ranks := graphRankRankCells(t, got, "V", "_rank")
	if len(ranks) != 1 {
		t.Fatalf("rank count = %d, want 1", len(ranks))
	}
	assertClose(t, "single dangling rank", graphRankFloat(t, ranks, "solo"), 0.15)
}

func TestGraphRank_DisconnectedVerticesLeakDanglingRank(t *testing.T) {
	cells := sortGraphRankInput([]kv{
		{mk("A", "V", "_label", "", 1), []byte("alpha")},
		{mk("B", "V", "_label", "", 2), []byte("bravo")},
	})
	g := initGraphRank(t, newSliceSource(cells...), map[string]string{GraphRankMaxIterations: "2"})
	got, err := drain(g)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	ranks := graphRankRankCells(t, got, "V", "_rank")
	assertClose(t, "rank A", graphRankFloat(t, ranks, "A"), 0.075)
	assertClose(t, "rank B", graphRankFloat(t, ranks, "B"), 0.075)
}

func TestGraphRank_EdgeTypeFilter(t *testing.T) {
	cells := sortGraphRankInput([]kv{
		{mk("A", "V", "_label", "", 10), []byte("alpha")},
		{mk("A", "E_keep", "B", "", 11), []byte("{}")},
		{mk("B", "V", "_label", "", 12), []byte("bravo")},
		{mk("C", "V", "_label", "", 13), []byte("charlie")},
		{mk("C", "E_skip", "B", "", 14), []byte("{}")},
	})
	g := initGraphRank(t, newSliceSource(cells...), map[string]string{
		GraphRankEdgeType:      "keep",
		GraphRankMaxIterations: "1",
	})
	got, err := drain(g)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	ranks := graphRankRankCells(t, got, "V", "_rank")
	assertClose(t, "filtered rank B", graphRankFloat(t, ranks, "B"), 0.3333333333333333)
}

func TestGraphRank_MaxVerticesSkipsComputation(t *testing.T) {
	cells := sortGraphRankInput([]kv{
		{mk("A", "V", "_label", "", 1), []byte("alpha")},
		{mk("B", "V", "_label", "", 2), []byte("bravo")},
		{mk("C", "V", "_label", "", 3), []byte("charlie")},
	})
	g := initGraphRank(t, newSliceSource(cells...), map[string]string{GraphRankMaxVertices: "2"})
	got, err := drain(g)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if ranks := graphRankRankCells(t, got, "V", "_rank"); len(ranks) != 0 {
		t.Fatalf("rank count with maxVertices exceeded = %d, want 0", len(ranks))
	}
	if len(got) != len(cells) {
		t.Fatalf("output count = %d, want originals only (%d)", len(got), len(cells))
	}
}

func TestGraphRank_DerivedTimestamps(t *testing.T) {
	cells := sortGraphRankInput([]kv{
		{mk("A", "V", "_label", "", 10), []byte("alpha")},
		{mk("A", "E_link", "B", "", 21), []byte("{}")},
		{mk("B", "V", "_label", "", 20), []byte("bravo")},
	})
	g := initGraphRank(t, newSliceSource(cells...), map[string]string{GraphRankMaxIterations: "1"})
	got, err := drain(g)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	for row, rank := range graphRankRankCells(t, got, "V", "_rank") {
		if rank.k.Timestamp != 22 {
			t.Fatalf("rank %s timestamp = %d, want max source timestamp + 1 = 22", row, rank.k.Timestamp)
		}
	}
}

func TestGraphRank_DeterministicAcrossRuns(t *testing.T) {
	cells := sortGraphRankInput([]kv{
		{mk("A", "V", "_label", "visA", 10), []byte("alpha")},
		{mk("A", "E_link", "B", "", 11), []byte("{}")},
		{mk("B", "V", "_label", "visB", 12), []byte("bravo")},
		{mk("B", "E_link", "C", "", 13), []byte("{}")},
		{mk("C", "V", "_label", "visC", 14), []byte("charlie")},
		{mk("C", "E_link", "A", "", 15), []byte("{}")},
	})
	var want string
	for i := 0; i < 20; i++ {
		g := initGraphRank(t, newSliceSource(cells...), map[string]string{GraphRankMaxIterations: "7"})
		got, err := drain(g)
		if err != nil {
			t.Fatalf("run %d drain: %v", i, err)
		}
		serialized := serializedGraphRankCells(got)
		if i == 0 {
			want = serialized
			continue
		}
		if serialized != want {
			t.Fatalf("run %d output differed\nwant:\n%s\ngot:\n%s", i, want, serialized)
		}
	}
}

func TestGraphRank_OutputSortedByKey(t *testing.T) {
	cells := sortGraphRankInput([]kv{
		{mk("A", "V", "_label", "", 1), []byte("alpha")},
		{mk("A", "E_link", "B", "", 2), []byte("{}")},
		{mk("B", "V", "_label", "", 3), []byte("bravo")},
	})
	g := initGraphRank(t, newSliceSource(cells...), map[string]string{GraphRankMaxIterations: "1"})
	got, err := drain(g)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].k.Compare(got[i].k) > 0 {
			t.Fatalf("output not sorted at index %d: %+v then %+v", i, got[i-1].k, got[i].k)
		}
	}
}

func TestGraphRank_CustomSchemaOptions(t *testing.T) {
	cells := sortGraphRankInput([]kv{
		{mk("A", "VX", "name", "visA", 1), []byte("alpha")},
		{mk("A", "OUT:rel", "B", "", 2), []byte("{}")},
		{mk("B", "VX", "name", "visB", 3), []byte("bravo")},
	})
	g := initGraphRank(t, newSliceSource(cells...), map[string]string{
		GraphRankVertexCF:      "VX",
		GraphRankEdgeCFPrefix:  "OUT:",
		GraphRankLabelCQ:       "name",
		GraphRankRankCQ:        "score",
		GraphRankEdgeType:      "rel",
		GraphRankMaxIterations: "1",
	})
	got, err := drain(g)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	ranks := graphRankRankCells(t, got, "VX", "score")
	if len(ranks) != 2 {
		t.Fatalf("rank count = %d, want 2", len(ranks))
	}
	if string(ranks["A"].k.ColumnVisibility) != "visA" || string(ranks["B"].k.ColumnVisibility) != "visB" {
		t.Fatalf("custom label visibility not preserved: A=%q B=%q", ranks["A"].k.ColumnVisibility, ranks["B"].k.ColumnVisibility)
	}
}

func TestGraphRank_BadOptionsRejected(t *testing.T) {
	tests := []struct {
		option string
		value  string
	}{
		{GraphRankDampingFactor, "not-a-float"},
		{GraphRankMaxIterations, "not-an-int"},
		{GraphRankMaxVertices, "not-an-int"},
		{GraphRankConvergenceThreshold, "not-a-float"},
	}
	for _, tt := range tests {
		t.Run(tt.option, func(t *testing.T) {
			err := NewGraphRankIterator().Init(newSliceSource(), map[string]string{tt.option: tt.value}, IteratorEnvironment{Scope: ScopeMajc})
			want := fmt.Sprintf("iterrt: GraphRankIterator bad %s=%q", tt.option, tt.value)
			if err == nil || err.Error() != want {
				t.Fatalf("Init error = %v, want %q", err, want)
			}
		})
	}
}

func TestGraphRank_BuildStackRegistered(t *testing.T) {
	cells := sortGraphRankInput([]kv{
		{mk("A", "V", "_label", "", 1), []byte("alpha")},
		{mk("B", "V", "_label", "", 2), []byte("bravo")},
		{mk("A", "E_link", "B", "", 3), []byte("{}")},
	})
	stack, err := BuildStack(newSliceSource(cells...), []IterSpec{
		{Name: IterGraphRank, Options: map[string]string{GraphRankMaxIterations: "1"}},
	}, IteratorEnvironment{Scope: ScopeMajc, FullMajorCompaction: true})
	if err != nil {
		t.Fatalf("BuildStack: %v", err)
	}
	if err := stack.Seek(InfiniteRange(), nil, false); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	got, err := drain(stack)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if ranks := graphRankRankCells(t, got, "V", "_rank"); len(ranks) != 2 {
		t.Fatalf("rank count via stack = %d, want 2", len(ranks))
	}
}

func TestGraphRank_DeepCopy(t *testing.T) {
	cells := sortGraphRankInput([]kv{
		{mk("A", "V", "_label", "", 1), []byte("alpha")},
		{mk("B", "V", "_label", "", 2), []byte("bravo")},
		{mk("A", "E_link", "B", "", 3), []byte("{}")},
	})
	original := NewGraphRankIterator()
	if err := original.Init(newSliceSource(cells...), map[string]string{GraphRankMaxIterations: "1"}, IteratorEnvironment{Scope: ScopeMajc}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	copy := original.DeepCopy(IteratorEnvironment{Scope: ScopeMajc})
	if err := original.Seek(InfiniteRange(), nil, false); err != nil {
		t.Fatalf("original Seek: %v", err)
	}
	if err := copy.Seek(InfiniteRange(), nil, false); err != nil {
		t.Fatalf("copy Seek: %v", err)
	}
	gotOriginal, err := drain(original)
	if err != nil {
		t.Fatalf("original drain: %v", err)
	}
	gotCopy, err := drain(copy)
	if err != nil {
		t.Fatalf("copy drain: %v", err)
	}
	if serializedGraphRankCells(gotOriginal) != serializedGraphRankCells(gotCopy) {
		t.Fatalf("DeepCopy output differed\noriginal:\n%s\ncopy:\n%s", serializedGraphRankCells(gotOriginal), serializedGraphRankCells(gotCopy))
	}
}
