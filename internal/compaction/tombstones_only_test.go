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

package compaction

import (
	"testing"

	"github.com/phrocker/shoal-oss/internal/iterrt"
	"github.com/phrocker/shoal-oss/internal/rfile/wire"
)

func tomb(row string, ts int64) *wire.Key {
	k := mk(row, "cf", "q", ts)
	k.Deleted = true
	return k
}

// TestCompact_TombstonesOnly: the output holds exactly the newest tombstone
// of each deleted coordinate, even at FullMajorCompaction (where a normal
// compaction drops them), and no live cell.
func TestCompact_TombstonesOnly(t *testing.T) {
	newer := []kv{
		{tomb("a", 9), ""},
		{mk("b", "cf", "q", 7), "b"},
		{tomb("c", 4), ""},
	}
	older := []kv{
		{tomb("a", 3), ""},
		{mk("a", "cf", "q", 2), "a"},
		{mk("c", "cf", "q", 3), "c"},
		{mk("d", "cf", "q", 1), "d"},
	}
	inputs := []Input{
		{Name: "newer", Bytes: buildRFile(t, newer)},
		{Name: "older", Bytes: buildRFile(t, older)},
	}
	for _, full := range []bool{true, false} {
		res, err := Compact(Spec{
			Inputs:              inputs,
			Scope:               iterrt.ScopeMajc,
			FullMajorCompaction: full,
			TombstonesOnly:      true,
		})
		if err != nil {
			t.Fatalf("full=%v: %v", full, err)
		}
		assertCells(t, drainRFile(t, res.Output), []kv{
			{tomb("a", 9), ""},
			{tomb("c", 4), ""},
		})
	}
	// A compaction that keeps deletes reports how many it wrote, in both
	// formats.
	for _, format := range []string{"rfile", "parquet"} {
		res, err := Compact(Spec{Inputs: inputs, Scope: iterrt.ScopeMajc, OutputFormat: format})
		if err != nil {
			t.Fatal(err)
		}
		if res.TombstonesWritten != 2 || res.EntriesWritten != 4 {
			t.Fatalf("%s: wrote %d tombstones of %d cells, want 2 of 4", format, res.TombstonesWritten, res.EntriesWritten)
		}
	}
	// The same inputs in a normal full compaction drop every tombstone.
	res, err := Compact(Spec{Inputs: inputs, Scope: iterrt.ScopeMajc, FullMajorCompaction: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.TombstonesWritten != 0 {
		t.Fatalf("full compaction reports %d tombstones", res.TombstonesWritten)
	}
	assertCells(t, drainRFile(t, res.Output), []kv{
		{mk("b", "cf", "q", 7), "b"},
		{mk("d", "cf", "q", 1), "d"},
	})
	if _, err := Compact(Spec{
		Inputs:         inputs,
		Scope:          iterrt.ScopeMajc,
		TombstonesOnly: true,
		Stack:          []iterrt.IterSpec{{Name: iterrt.IterVersioning}},
	}); err == nil {
		t.Fatal("TombstonesOnly with a stack was accepted")
	}
}
