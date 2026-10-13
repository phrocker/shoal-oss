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

// TestDeltaKeepsLocalWriteBetweenImports: the consumer overwrote a cell
// between two imports from the producer; the producer then compacts both
// with new data. Ranking the output at any one position would either bury
// the new data or lift the old cell above the local write; the delta does
// neither.
func TestDeltaKeepsLocalWriteBetweenImports(t *testing.T) {
	for _, incremental := range []bool{false, true} {
		for _, dCompacts := range []bool{false, true} {
			t.Run(fmt.Sprintf("incremental=%v/d-compacts=%v", incremental, dCompacts), func(t *testing.T) {
				p, d := newLNode(t, "p"), newLNode(t, "d")
				o := syncOpts{producerID: "p", incremental: incremental}
				p.put(t, "cell", "p-old")
				syncNodes(t, p, d, o)
				d.put(t, "cell", "local")
				d.flush(t)
				p.put(t, "other", "o")
				syncNodes(t, p, d, o)
				if dCompacts {
					d.compact(t)
				}
				p.put(t, "fresh", "f")
				p.flush(t)
				p.compact(t)
				syncNodes(t, p, d, o)
				d.expect(t, "cell", "local")
				d.expect(t, "fresh", "f")
				d.expect(t, "other", "o")
				// The producer overwrites the cell again: that is new data.
				p.put(t, "cell", "p-new")
				p.flush(t)
				p.compact(t)
				syncNodes(t, p, d, o)
				d.expect(t, "cell", "p-new")
				d.reopen(t)
				d.expect(t, "cell", "p-new")
				d.expect(t, "fresh", "f")
			})
		}
	}
}

// TestLineageSyncModel drives producers and a consumer through random
// writes, flushes, compactions, reopens and syncs, and checks every read
// against the arrival model: the consumer reads, per cell, the value that
// arrived last — a local write when it is made, a producer's write when the
// first sync after it delivers it — and data already delivered keeps its
// arrival time however the producer recompacts it. The consumer flushes
// before each import: an unflushed local write ranks above every immutable
// file, including one imported after the write.
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

type modelProducer struct {
	node      *lnode
	opts      syncOpts
	latest    map[string]string
	seq       map[string]int
	delivered map[string]int
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
			node:      newLNode(t, name),
			opts:      syncOpts{producerID: name, incremental: rng.Intn(2) == 0},
			latest:    map[string]string{},
			seq:       map[string]int{},
			delivered: map[string]int{},
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
			p.seq[k]++
			name = p.node.name + " put " + k + "=" + v
		case op < 32:
			p.node.flush(t)
			name = p.node.name + " flush"
		case op < 42:
			p.node.compact(t)
			name = p.node.name + " compact"
		case op < 45:
			p.node.reopen(t)
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
			for k, s := range p.seq {
				if s > p.delivered[k] {
					expected[k] = p.latest[k]
					p.delivered[k] = s
				}
			}
			name = "sync " + p.node.name
		}
		trace = append(trace, name)
		check()
	}
}
