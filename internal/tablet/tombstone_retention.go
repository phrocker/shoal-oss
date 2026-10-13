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

package tablet

import (
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/internal/compaction"
	"github.com/phrocker/shoal-oss/internal/storage"
)

// Exported tombstones (#708).
//
// A full major compaction (one output covering the whole tablet) drops
// tombstones, together with the cells they delete. That is Accumulo's
// contract and it rests on one invariant: a tombstone may be dropped only
// when no copy of the data it shadows exists outside this tablet. An
// exported tablet breaks it: consumers that imported the tablet's files
// hold copies of its cells, and a delete that a compaction drops before it
// is shipped never reaches them.
//
// So the tablet records exports. Each export marks every live file it
// lists, before anything is copied, with the time the file was first
// exported (files.json "exported"), and marks the tablet itself
// ("exported_lineage"; compaction outputs carry the lineage, so later data
// is covered too). From then on a full compaction still writes its main
// output as before (tombstones applied and dropped, so the data it carries
// is clean), and writes the tombstones consumers may need beside it, into
// tombstone-only outputs, grouped by when their input was exported:
//
//   - an input not exported yet ("pending") keeps its tombstones however
//     old they are: no consumer has seen them, and some consumer may hold
//     cells they delete. They go into one pending output (the newest
//     tombstone per coordinate, so it grows only with the coordinates
//     deleted since the last export), which is exported, and so starts its
//     horizon, with the next export;
//   - an input first exported at e keeps them while now-e < retention, the
//     consumer import horizon, and drops them after it;
//   - kept tombstones of inputs exported in the same window of
//     retention/4 share one output, which records the latest of their
//     export times. A tombstone is therefore kept at least retention after
//     its first export, and at most about 1.25 x retention plus the time to
//     the next full compaction; the tablet holds at most about five such
//     outputs plus the pending one. (One output for all kept tombstones,
//     recording the latest time, would let a steady trickle of deletes keep
//     every tombstone forever.)
//
// Finding the tombstones to keep is a second merge, over the inputs that may
// hold any: files this tablet wrote and knows hold none (files.json
// "tombstone_free": a flush of a memtable without deletes, or a compaction
// output that wrote none, such as every main output) are skipped.
//
// A tombstone-only file can sit anywhere in the recency order: a tombstone
// never has the same key as a live cell (wire.Key orders the delete flag),
// and copies of one tombstone key are interchangeable.
//
// A kept tombstone keeps doing what tombstones do: it shadows every cell of
// its coordinate with a timestamp at or below its own, including one written
// after it (a DeleteLatest followed by a PutLatest of the same cell). Before
// #708 a full compaction ended that on the producer only; consumers holding
// the tombstone kept shadowing. Now both do, until the tombstone expires.
//
// The retention horizon is the contract with consumers: a consumer must
// import a producer's manifests within it of their being written, or it can
// miss deletes. A consumer's own full compaction may drop tombstones it
// imported, because it holds the only copy of what it imported unless it
// exports too. A consumer that re-exports (chained sync, P to M to D) is a
// producer for its own consumers: its export marks its files, imported
// ones included, and its compactions keep the tombstones as above.
//
// Only full compactions drop tombstones; a multi-run compaction (mixed
// embedding states) keeps all of them in its outputs, which start pending.

// DefaultTombstoneRetention is the consumer import horizon used when
// Options.TombstoneRetention is zero.
const DefaultTombstoneRetention = 7 * 24 * time.Hour

// TombstoneRetentionDisabled, as Options.TombstoneRetention, drops tombstones
// at every full compaction even on an exported tablet (the behaviour before
// #708). A consumer holding a deleted cell then keeps it.
const TombstoneRetentionDisabled time.Duration = -1

// tombstoneBuckets is how many export-time windows one retention horizon is
// divided into.
const tombstoneBuckets = 4

// exportClock is the clock export times are taken from and compared with
// (a variable so tests can move it).
var exportClock = func() int64 { return time.Now().UnixMilli() }

// retentionMillis is the tablet's retention horizon in milliseconds, or 0
// when retention is disabled.
func (t *Tablet) retentionMillis() int64 {
	switch r := t.opts.TombstoneRetention; {
	case r < 0:
		return 0
	case r == 0:
		return DefaultTombstoneRetention.Milliseconds()
	default:
		if ms := r.Milliseconds(); ms > 0 {
			return ms
		}
		return 1
	}
}

// assumeExportedUntracked handles a generation whose exports were not
// recorded: no files.json, or one an older binary wrote. Any of its files
// may have been exported, so each is treated as exported now. Its tombstones
// are then kept for one horizon from the first time this binary reads it,
// unless a write records that first.
func assumeExportedUntracked(state *immutableState) {
	if len(state.active) == 0 {
		return
	}
	now := exportClock()
	state.exportedLineage = true
	for _, path := range state.active {
		state.exported[filepath.Base(path)] = now
	}
}

// ExportLineage is RFileLineage for an export: it also records, durably and
// BEFORE returning, that every listed file has been exported (keeping an
// earlier export time) and that this tablet's lineage has been. Recording
// first is what makes the export safe: a compaction that runs after this
// call, while the files are still being copied, already keeps their
// tombstones. An export that then fails only keeps tombstones longer.
func (t *Tablet) ExportLineage() ([]MintedFile, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := exportClock()
	exported := make(map[string]int64, len(t.files))
	// An export of no files ships nothing a later delete could shadow.
	changed := !t.exportedLineage && len(t.files) > 0
	for _, path := range t.files {
		name := filepath.Base(path)
		if at := t.exported[name]; at > 0 {
			exported[name] = at
			continue
		}
		exported[name] = now
		changed = true
	}
	if changed {
		next := t.state(t.files, t.obsolete)
		next.exported = exported
		next.exportedLineage = true
		if err := persistImmutableManifest(t.backend, t.dir, next); err != nil {
			if storage.IsCommittedWriteError(err) {
				t.exported, t.exportedLineage = exported, true
			}
			return nil, fmt.Errorf("tablet: record export: %w", err)
		}
		t.exported, t.exportedLineage = exported, true
	}
	out := make([]MintedFile, len(t.files))
	for i, path := range t.files {
		out[i] = MintedFile{Path: path, Instance: t.minted[filepath.Base(path)]}
	}
	return out, nil
}

// tombstoneGroup is one tombstone-only output a full compaction writes:
// the inputs whose tombstones it keeps (newest first) and the export time
// it records (0: pending).
type tombstoneGroup struct {
	exportedAt int64
	inputs     []compaction.Input
}

// planRetainedTombstones groups the inputs of a full compaction (oldest
// first) by the tombstone output their tombstones go to. Inputs past the
// horizon are left out: their tombstones are dropped. It returns nil when
// the tablet keeps no tombstones (never exported, or retention disabled).
// Caller holds t.mu.
func (t *Tablet) planRetainedTombstones(inputs []compaction.Input) []tombstoneGroup {
	horizon := t.retentionMillis()
	if !t.exportedLineage || horizon == 0 {
		return nil
	}
	width := horizon / tombstoneBuckets
	if width < 1 {
		width = 1
	}
	now := exportClock()
	const pending = int64(-1)
	groups := make(map[int64]*tombstoneGroup)
	for i := len(inputs) - 1; i >= 0; i-- { // newest first
		in := inputs[i]
		if _, free := t.tombstoneFree[filepath.Base(in.Name)]; free {
			continue // nothing to keep
		}
		at := t.exported[filepath.Base(in.Name)]
		bucket := pending
		if at > 0 {
			if now-at >= horizon {
				continue // past the horizon: dropped
			}
			bucket = at / width
		}
		group := groups[bucket]
		if group == nil {
			group = &tombstoneGroup{}
			groups[bucket] = group
		}
		if at > group.exportedAt {
			group.exportedAt = at
		}
		group.inputs = append(group.inputs, in)
	}
	buckets := make([]int64, 0, len(groups))
	for bucket := range groups {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })
	out := make([]tombstoneGroup, 0, len(buckets))
	for _, bucket := range buckets {
		out = append(out, *groups[bucket])
	}
	return out
}
