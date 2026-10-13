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
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/phrocker/shoal-oss/internal/engine"
	"github.com/phrocker/shoal-oss/internal/iterrt"
	"github.com/phrocker/shoal-oss/internal/storage/memory"
)

// Issue #704: a producer's compaction output holds only data a consumer
// already imported, but has a new producer identity. These tests check that
// it is recognised by its lineage and does not re-rank old cells above
// newer local or other-producer writes.

// lnode is one engine in a sync topology, on its own memory backend.
type lnode struct {
	name    string
	dir     string
	backend *memory.Backend
	eng     *engine.Engine
	// sync state per destination, for incremental exports
	states map[string]*engine.SyncState
}

func newLNode(t *testing.T, name string) *lnode {
	t.Helper()
	n := &lnode{name: name, dir: filepath.Join(t.TempDir(), name), backend: memory.New(),
		states: map[string]*engine.SyncState{}}
	eng, err := engine.Open(n.dir, engine.Options{Backend: n.backend})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CreateTable(overwriteTable, engine.TableOptions{}); err != nil {
		t.Fatal(err)
	}
	n.eng = eng
	t.Cleanup(func() { _ = n.eng.Close() })
	return n
}

func (n *lnode) reopen(t *testing.T) {
	t.Helper()
	if err := n.eng.Close(); err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Open(n.dir, engine.Options{Backend: n.backend})
	if err != nil {
		t.Fatal(err)
	}
	n.eng = eng
}

func (n *lnode) put(t *testing.T, row, value string) { t.Helper(); putLatest(t, n.eng, row, value) }

func (n *lnode) flush(t *testing.T) {
	t.Helper()
	if err := n.eng.Flush(overwriteTable); err != nil {
		t.Fatal(err)
	}
}

func (n *lnode) compact(t *testing.T) {
	t.Helper()
	if err := n.eng.Compact(overwriteTable, nil); err != nil {
		t.Fatal(err)
	}
}

// read returns every row's newest value.
func (n *lnode) read(t *testing.T) map[string]string {
	t.Helper()
	sc, err := n.eng.Scan(overwriteTable, iterrt.InfiniteRange(), engine.ScanOptions{
		Stack: []iterrt.IterSpec{{
			Name:    iterrt.IterVersioning,
			Options: map[string]string{iterrt.VersioningOption: "1"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()
	out := map[string]string{}
	for sc.Next() {
		out[string(sc.Key().Row)] = string(sc.Value())
		if err := sc.Advance(); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func (n *lnode) expect(t *testing.T, row, want string) {
	t.Helper()
	if got := n.read(t)[row]; got != want {
		t.Fatalf("%s reads %s = %q, want %q", n.name, row, got, want)
	}
}

type syncOpts struct {
	producerID  string
	stamp       string
	incremental bool
	// edit, when set, rewrites the manifest before import (simulating an
	// older producer).
	edit func(m *engine.RFileExportManifest)
}

// export ships from's table into to's backend (an inbox per producer).
func export(t *testing.T, from, to *lnode, o syncOpts) *engine.RFileExportManifest {
	t.Helper()
	ctx := context.Background()
	// An import registers files where the export put them, so the
	// destination root is the consumer's engine directory; each producer
	// writes its own manifest there.
	root := to.dir
	manifest := filepath.Join(to.dir, "manifest-"+from.name+"-"+o.stamp+".json")
	opts := engine.RFileExportOptions{
		DestinationRoot: root, ManifestPath: manifest, EngineVersion: "test",
		ProducerID: o.producerID, StampVisibilityLabel: o.stamp,
	}
	var m *engine.RFileExportManifest
	if o.incremental {
		res, err := from.eng.ExportRFilesIncremental(ctx, overwriteTable, to.backend, opts, from.states[manifest])
		if err != nil {
			t.Fatal(err)
		}
		from.states[manifest] = res.State
		m = res.Manifest
	} else {
		var err error
		m, err = from.eng.ExportRFiles(ctx, overwriteTable, to.backend, opts)
		if err != nil {
			t.Fatal(err)
		}
	}
	if o.edit != nil {
		o.edit(m)
	}
	return m
}

func importInto(t *testing.T, to *lnode, m *engine.RFileExportManifest) {
	t.Helper()
	if err := to.eng.ImportRFileManifest(context.Background(), m); err != nil {
		t.Fatal(err)
	}
}

func syncNodes(t *testing.T, from, to *lnode, o syncOpts) *engine.RFileExportManifest {
	t.Helper()
	m := export(t, from, to, o)
	importInto(t, to, m)
	return m
}

// TestProducerCompactionKeepsLocalWriteNewest is the issue's first repro:
// the consumer overwrites an imported cell, the producer compacts with no
// new data, and the re-import must not bring the old value back.
func TestProducerCompactionKeepsLocalWriteNewest(t *testing.T) {
	for _, incremental := range []bool{false, true} {
		for salt := 0; salt < 3; salt++ {
			name := fmt.Sprintf("incremental=%v/salt=%d", incremental, salt)
			t.Run(name, func(t *testing.T) {
				p, d := newLNode(t, "p"), newLNode(t, "d")
				o := syncOpts{incremental: incremental}
				p.put(t, "cell", "producer-old")
				p.flush(t)
				for i := 0; i < salt; i++ {
					p.put(t, fmt.Sprintf("salt%d", i), "s")
					p.flush(t)
				}
				syncNodes(t, p, d, o)
				d.expect(t, "cell", "producer-old")
				d.put(t, "cell", "local-newer")
				d.flush(t)
				p.compact(t)
				syncNodes(t, p, d, o)
				d.expect(t, "cell", "local-newer")
				// The producer's own view is unchanged.
				p.expect(t, "cell", "producer-old")
				// Stable across a re-import and a reopen.
				syncNodes(t, p, d, o)
				d.reopen(t)
				syncNodes(t, p, d, o)
				d.expect(t, "cell", "local-newer")
				// New producer data after the compaction is a new file.
				p.put(t, "fresh", "f")
				syncNodes(t, p, d, o)
				d.expect(t, "fresh", "f")
				d.expect(t, "cell", "local-newer")
			})
		}
	}
}

// TestPartlyHeldOutputRanksNewest pins the known behaviour for a producer
// compaction that merges data the consumer holds with data it does not:
// the output is appended newest, as before lineage, so its new cells win —
// and so do its already-held cells, over a later local write. No single
// position is right for such a file; splitting it is the follow-up to #704,
// which must change this test deliberately.
func TestPartlyHeldOutputRanksNewest(t *testing.T) {
	p, d := newLNode(t, "p"), newLNode(t, "d")
	o := syncOpts{producerID: "p"}
	p.put(t, "cell", "producer-old")
	syncNodes(t, p, d, o)
	d.put(t, "cell", "local-newer")
	d.flush(t)
	p.put(t, "fresh", "f")
	p.flush(t)
	p.compact(t)
	syncNodes(t, p, d, o)
	d.expect(t, "fresh", "f")
	d.expect(t, "cell", "producer-old")
}

// TestFanInProducerCompactionKeepsOtherProducerNewer is the issue's second
// repro: X's old value, then Y's newer one, then X compacts.
func TestFanInProducerCompactionKeepsOtherProducerNewer(t *testing.T) {
	for salt := 0; salt < 3; salt++ {
		for _, dCompacts := range []bool{false, true} {
			name := fmt.Sprintf("salt=%d/d-compacts=%v", salt, dCompacts)
			t.Run(name, func(t *testing.T) {
				x, y, d := newLNode(t, "x"), newLNode(t, "y"), newLNode(t, "d")
				ox, oy := syncOpts{producerID: "x"}, syncOpts{producerID: "y"}
				x.put(t, "cell", "x-old")
				for i := 0; i < salt; i++ {
					x.flush(t)
					x.put(t, fmt.Sprintf("xsalt%d", i), "s")
				}
				syncNodes(t, x, d, ox)
				y.put(t, "cell", "y-newer")
				syncNodes(t, y, d, oy)
				d.expect(t, "cell", "y-newer")
				if dCompacts {
					d.compact(t)
				}
				x.compact(t)
				syncNodes(t, x, d, ox)
				d.expect(t, "cell", "y-newer")
				d.reopen(t)
				syncNodes(t, x, d, ox)
				syncNodes(t, y, d, oy)
				d.expect(t, "cell", "y-newer")
			})
		}
	}
}

// TestEchoBackKeepsProducerNewest is the issue's third repro: P ships v1 to
// D, writes v2, and D's export (holding v1) comes back to P.
func TestEchoBackKeepsProducerNewest(t *testing.T) {
	cases := []struct{ dLocal, dCompacts bool }{{false, false}, {true, false}, {false, true}}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("d-local=%v/d-compacts=%v", tc.dLocal, tc.dCompacts), func(t *testing.T) {
			p, d := newLNode(t, "p"), newLNode(t, "d")
			p.put(t, "cell", "v1")
			syncNodes(t, p, d, syncOpts{producerID: "p"})
			p.put(t, "cell", "v2")
			p.flush(t)
			if tc.dLocal {
				d.put(t, "other", "from-d")
				d.flush(t)
			}
			if tc.dCompacts {
				d.compact(t)
			}
			syncNodes(t, d, p, syncOpts{producerID: "d"})
			p.expect(t, "cell", "v2")
			if tc.dLocal {
				p.expect(t, "other", "from-d")
			}
			// And back again: D gets v2, P's echo of D's data is held.
			syncNodes(t, p, d, syncOpts{producerID: "p"})
			d.expect(t, "cell", "v2")
			syncNodes(t, d, p, syncOpts{producerID: "d"})
			p.expect(t, "cell", "v2")
			p.reopen(t)
			syncNodes(t, d, p, syncOpts{producerID: "d"})
			p.expect(t, "cell", "v2")
		})
	}
}

// TestChainedSyncKeepsLineage: P -> M -> D. M's compactions of P's data,
// and P's own, are recognised by D through M, and D also holds them when it
// imports straight from P.
func TestChainedSyncKeepsLineage(t *testing.T) {
	p, m, d := newLNode(t, "p"), newLNode(t, "m"), newLNode(t, "d")
	op, om := syncOpts{producerID: "p"}, syncOpts{producerID: "m"}
	p.put(t, "cell", "p1")
	p.flush(t)
	p.put(t, "pkeep", "p")
	syncNodes(t, p, m, op)
	syncNodes(t, m, d, om)
	d.expect(t, "cell", "p1")
	d.put(t, "cell", "d-local")
	d.flush(t)
	m.compact(t)
	syncNodes(t, m, d, om)
	d.expect(t, "cell", "d-local")
	// P compacts; M holds every leaf of the output, and so does D.
	p.compact(t)
	syncNodes(t, p, m, op)
	syncNodes(t, m, d, om)
	syncNodes(t, p, d, op)
	d.expect(t, "cell", "d-local")
	// New P data travels through M as new data, then M compacts again.
	p.put(t, "pnew", "n")
	syncNodes(t, p, m, op)
	syncNodes(t, m, d, om)
	m.compact(t)
	syncNodes(t, m, d, om)
	d.reopen(t)
	syncNodes(t, m, d, om)
	syncNodes(t, p, d, op)
	d.expect(t, "cell", "d-local")
	d.expect(t, "pnew", "n")
	d.expect(t, "pkeep", "p")
}

// TestStampedExportIsNotHeldAsUnstamped: a visibility-stamped export holds
// different cells than the source, so its lineage must not make the
// unstamped data look already received.
func TestStampedExportIsNotHeldAsUnstamped(t *testing.T) {
	p, d := newLNode(t, "p"), newLNode(t, "d")
	p.put(t, "cell", "v")
	syncNodes(t, p, d, syncOpts{producerID: "p", stamp: "tenant"})
	if got := d.cells(t); len(got) != 1 || got[0] != "cell[tenant]=v" {
		t.Fatalf("after stamped import: %v", got)
	}
	syncNodes(t, p, d, syncOpts{producerID: "p2"})
	if got := d.cells(t); len(got) != 2 || got[0] != "cell[]=v" {
		t.Fatalf("after unstamped import: %v, want the unstamped cell too", got)
	}
}

// cells lists every visible cell as row[cv]=value, in key order.
func (n *lnode) cells(t *testing.T) []string {
	t.Helper()
	sc, err := n.eng.Scan(overwriteTable, iterrt.InfiniteRange(), engine.ScanOptions{
		Stack: []iterrt.IterSpec{{
			Name:    iterrt.IterVersioning,
			Options: map[string]string{iterrt.VersioningOption: "1"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()
	var out []string
	for sc.Next() {
		out = append(out, fmt.Sprintf("%s[%s]=%s", sc.Key().Row, sc.Key().ColumnVisibility, sc.Value()))
		if err := sc.Advance(); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// TestOlderProducerManifestThenLineage: a manifest without lineage (an
// older producer) imports as before; once the producer sends lineage, the
// consumer learns it for the files it holds, and the producer's later
// compaction is recognised.
func TestOlderProducerManifestThenLineage(t *testing.T) {
	p, d := newLNode(t, "p"), newLNode(t, "d")
	strip := func(m *engine.RFileExportManifest) {
		for i := range m.RFiles {
			m.RFiles[i].Lineage = nil
		}
	}
	p.put(t, "cell", "producer-old")
	p.flush(t)
	p.put(t, "salt", "s")
	syncNodes(t, p, d, syncOpts{edit: strip})
	d.put(t, "cell", "local-newer")
	d.flush(t)
	syncNodes(t, p, d, syncOpts{}) // same files, now with lineage
	p.compact(t)
	syncNodes(t, p, d, syncOpts{})
	d.expect(t, "cell", "local-newer")
	// Manifests with lineage stripped keep the pre-#704 behaviour: the
	// compaction output is new data, appended newest.
	q, e := newLNode(t, "q"), newLNode(t, "e")
	q.put(t, "cell", "q-old")
	q.flush(t)
	q.put(t, "salt", "s")
	syncNodes(t, q, e, syncOpts{edit: strip})
	e.put(t, "cell", "local")
	e.flush(t)
	q.compact(t)
	syncNodes(t, q, e, syncOpts{edit: strip})
	e.expect(t, "cell", "q-old")
}
