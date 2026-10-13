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

package engine_test

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestLineageSyncModel drives producers and a consumer through random
// writes, flushes, compactions, reopens and syncs, and checks every read
// against an arrival model of the producers' files:
//
//   - a producer file is a set of leaves (its flushes) and, per cell, the
//     producer's newest value among them; a compaction merges all files;
//   - a sync delivers the producer's files in order. A file all of whose
//     leaves the consumer already holds changes nothing — its cells keep
//     their arrival time. Any other file arrives now, as a whole: its cells
//     overwrite the consumer's view. That includes a file only partly held
//     (new data compacted with data the consumer has), whose old cells are
//     knowingly re-ranked newest too — the documented behaviour until such
//     files are split (follow-up to #704);
//   - a consumer write arrives when it is made. The consumer flushes before
//     each import: an unflushed local write ranks above every immutable
//     file, including one imported after the write.
func TestLineageSyncModel(t *testing.T) {
	seeds := 150
	if testing.Short() {
		seeds = 20
	}
	for seed := 0; seed < seeds; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runLineageModel(t, int64(seed))
		})
	}
}

// modelFile is one producer file: the leaves it holds and its newest value
// per cell.
type modelFile struct {
	leaves map[int]bool
	cells  map[string]string
}

type modelProducer struct {
	node   *lnode
	opts   syncOpts
	latest map[string]string // the producer's own reads
	mem    map[string]string // unflushed writes
	files  []modelFile       // oldest first
	leaf   int
	held   map[int]bool // leaves the consumer holds
}

func (p *modelProducer) flush() {
	if len(p.mem) == 0 {
		return
	}
	p.leaf++
	p.files = append(p.files, modelFile{leaves: map[int]bool{p.leaf: true}, cells: p.mem})
	p.mem = map[string]string{}
}

func (p *modelProducer) compact() {
	if len(p.files) < 2 {
		return // a single file of the table's format is left as is
	}
	merged := modelFile{leaves: map[int]bool{}, cells: map[string]string{}}
	for _, f := range p.files {
		for l := range f.leaves {
			merged.leaves[l] = true
		}
		for k, v := range f.cells {
			merged.cells[k] = v
		}
	}
	p.files = []modelFile{merged}
}

func runLineageModel(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	keys := []string{"k0", "k1", "k2", "k3", "k4"}
	d := newLNode(t, "d")
	expected := map[string]string{}
	var producers []*modelProducer
	for i := 0; i < 1+int(seed%2); i++ {
		name := fmt.Sprintf("p%d", i)
		producers = append(producers, &modelProducer{
			node:   newLNode(t, name),
			opts:   syncOpts{producerID: name, incremental: rng.Intn(2) == 0},
			latest: map[string]string{},
			mem:    map[string]string{},
			held:   map[int]bool{},
		})
	}
	var trace []string
	value := 0
	check := func() {
		t.Helper()
		got := d.read(t)
		for _, k := range keys {
			if got[k] != expected[k] {
				t.Fatalf("consumer %s = %q, want %q\ntrace: %v", k, got[k], expected[k], trace)
			}
		}
		for _, p := range producers {
			pg := p.node.read(t)
			for _, k := range keys {
				if pg[k] != p.latest[k] {
					t.Fatalf("%s %s = %q, want %q\ntrace: %v", p.node.name, k, pg[k], p.latest[k], trace)
				}
			}
		}
	}
	for step := 0; step < 50; step++ {
		p := producers[rng.Intn(len(producers))]
		op := rng.Intn(100)
		var name string
		switch {
		case op < 25:
			k := keys[rng.Intn(len(keys))]
			value++
			v := fmt.Sprintf("%s:%d", p.node.name, value)
			p.node.put(t, k, v)
			p.latest[k] = v
			p.mem[k] = v
			name = p.node.name + " put " + k + "=" + v
		case op < 32:
			p.node.flush(t)
			p.flush()
			name = p.node.name + " flush"
		case op < 42:
			p.node.compact(t)
			p.compact()
			name = p.node.name + " compact"
		case op < 45:
			p.node.reopen(t) // Close flushes
			p.flush()
			name = p.node.name + " reopen"
		case op < 60:
			k := keys[rng.Intn(len(keys))]
			value++
			v := fmt.Sprintf("d:%d", value)
			d.put(t, k, v)
			expected[k] = v
			name = "d put " + k + "=" + v
		case op < 65:
			d.flush(t)
			name = "d flush"
		case op < 72:
			d.compact(t)
			name = "d compact"
		case op < 75:
			d.reopen(t)
			name = "d reopen"
		default:
			d.flush(t)
			syncNodes(t, p.node, d, p.opts)
			p.flush() // an export flushes first
			for _, f := range p.files {
				all := true
				for l := range f.leaves {
					all = all && p.held[l]
				}
				if all {
					continue
				}
				for k, v := range f.cells {
					expected[k] = v
				}
				for l := range f.leaves {
					p.held[l] = true
				}
			}
			name = "sync " + p.node.name
		}
		trace = append(trace, name)
		check()
	}
}
