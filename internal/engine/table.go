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

package engine

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/phrocker/shoal/internal/cclient"
	"github.com/phrocker/shoal/internal/iterrt"
	"github.com/phrocker/shoal/internal/localwal"
	"github.com/phrocker/shoal/internal/storage"
	"github.com/phrocker/shoal/internal/tablet"
)

// TableOptions configures a table at creation time.
type TableOptions struct {
	// Splits defines the tablet boundaries. Each split point is a row
	// key that becomes the exclusive end of one tablet and the inclusive
	// start of the next. An empty/nil Splits creates a single tablet
	// covering the entire key space.
	//
	// Example: Splits = ["event:", "knowledge:"] creates 3 tablets:
	//   tablet 0: (-inf, "event:")
	//   tablet 1: ["event:", "knowledge:")
	//   tablet 2: ["knowledge:", +inf)
	Splits [][]byte

	// TabletOptions is passed to each tablet at creation time.
	TabletOptions tablet.Options
}

// PrefixSplit is a convenience constructor for Splits based on row key
// prefixes. Given ("event:", "knowledge:"), it returns split points
// that create tablets for each prefix range plus one catch-all.
//
// The prefixes are sorted lexicographically. Example:
//
//	PrefixSplit("entity:", "event:", "knowledge:")
//
// creates 4 tablets:
//
//	(-inf, "entity:")    — anything before "entity:" (rare)
//	["entity:", "event:") — all entity nodes
//	["event:", "knowledge:") — all event nodes
//	["knowledge:", +inf) — all knowledge nodes + anything after
func PrefixSplit(prefixes ...string) [][]byte {
	splits := make([][]byte, len(prefixes))
	for i, p := range prefixes {
		splits[i] = []byte(p)
	}
	sort.Slice(splits, func(i, j int) bool {
		return bytes.Compare(splits[i], splits[j]) < 0
	})
	return splits
}

// table is the internal representation of a named table. It owns N
// tablets and routes mutations/scans based on split points.
type table struct {
	name    string
	dir     string
	tablets []*tablet.Tablet
	splits  [][]byte // sorted split points; len(tablets) == len(splits)+1
	logger  *slog.Logger
}

// createTable creates a new table on disk with the configured splits. notify,
// when non-nil, is invoked (with this table's name) each time one of its
// tablets writes a new RFile via flush or compaction.
func createTable(dir, name string, opts TableOptions, logger *slog.Logger, rfCache *tablet.Cache, notify func(table, kind, file string)) (*table, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("table: mkdir %s: %w", dir, err)
	}

	splits := opts.Splits
	if splits == nil {
		splits = [][]byte{}
	}
	// Ensure splits are sorted
	sort.Slice(splits, func(i, j int) bool {
		return bytes.Compare(splits[i], splits[j]) < 0
	})

	tabletCount := len(splits) + 1
	tablets := make([]*tablet.Tablet, tabletCount)
	tabletOpts := opts.TabletOptions
	tabletOpts.Cache = rfCache
	if tabletOpts.Logger == nil {
		tabletOpts.Logger = logger
	}
	tabletOpts.OnRFile = tabletNotify(name, notify)
	for i := 0; i < tabletCount; i++ {
		tabletDir := filepath.Join(dir, fmt.Sprintf("t-%04d", i))
		t, err := tablet.Open(tabletDir, tabletOpts)
		if err != nil {
			// Close any already-opened tablets
			for j := 0; j < i; j++ {
				tablets[j].Close()
			}
			return nil, fmt.Errorf("table: open tablet %d: %w", i, err)
		}
		tablets[i] = t
	}

	logger.Info("table created",
		slog.String("name", name),
		slog.Int("tablets", tabletCount),
		slog.Int("splits", len(splits)))

	return &table{
		name:    name,
		dir:     dir,
		tablets: tablets,
		splits:  splits,
		logger:  logger,
	}, nil
}

// refreshFiles re-discovers every tablet's on-disk RFiles, merging any newly
// arrived files (e.g. shipped by another producer into a shared destination)
// into this open table. Returns the total RFile count across tablets.
func (t *table) refreshFiles() (int, error) {
	total := 0
	for _, tb := range t.tablets {
		n, err := tb.RefreshFiles()
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// tabletNotify adapts an engine-level (table, kind, file) publisher into the
// per-tablet (kind, file) callback, capturing the owning table's name. Returns
// nil when notify is nil so tablets skip the call entirely.
func tabletNotify(name string, notify func(table, kind, file string)) func(kind, file string) {
	if notify == nil {
		return nil
	}
	return func(kind, file string) { notify(name, kind, file) }
}

// openTable opens an existing table from disk. It discovers tablets
// by scanning subdirectories named t-NNNN. notify, when non-nil, is invoked
// (with this table's name) each time one of its tablets writes a new RFile.
func openTable(dir, name string, logger *slog.Logger, rfCache *tablet.Cache, walSyncMode localwal.SyncMode, walSyncInterval time.Duration, backend storage.Backend, notify func(table, kind, file string)) (*table, error) {
	// TODO: persist and reload splits from a manifest file.
	// For now, open all t-NNNN directories as tablets with no split routing.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("table: readdir %s: %w", dir, err)
	}

	var tabletDirs []string
	for _, e := range entries {
		if e.IsDir() && len(e.Name()) >= 2 && e.Name()[0] == 't' && e.Name()[1] == '-' {
			tabletDirs = append(tabletDirs, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(tabletDirs)

	if len(tabletDirs) == 0 {
		// Legacy: single tablet without t-NNNN directory
		tabletDirs = []string{dir}
	}

	tablets := make([]*tablet.Tablet, len(tabletDirs))
	for i, td := range tabletDirs {
		t, err := tablet.Open(td, tablet.Options{
			Logger:          logger,
			Cache:           rfCache,
			WALSyncMode:     walSyncMode,
			WALSyncInterval: walSyncInterval,
			Backend:         backend,
			OnRFile:         tabletNotify(name, notify),
		})
		if err != nil {
			for j := 0; j < i; j++ {
				tablets[j].Close()
			}
			return nil, fmt.Errorf("table: open tablet %s: %w", td, err)
		}
		tablets[i] = t
	}

	logger.Info("table opened",
		slog.String("name", name),
		slog.Int("tablets", len(tablets)))

	return &table{
		name:    name,
		dir:     dir,
		tablets: tablets,
		logger:  logger,
	}, nil
}

// routeTablet returns the tablet index for a given row key based on
// the split points. Binary search: O(log S) where S = len(splits).
func (t *table) routeTablet(row []byte) int {
	if len(t.splits) == 0 {
		return 0
	}
	// Find the first split point > row → that's the tablet index.
	idx := sort.Search(len(t.splits), func(i int) bool {
		return bytes.Compare(t.splits[i], row) > 0
	})
	return idx
}

// write routes each mutation to its tablet and applies them.
// Mutations to different tablets are batched and written in parallel.
func (t *table) write(mutations []*cclient.Mutation) error {
	if len(t.tablets) == 1 {
		return t.tablets[0].Write(mutations)
	}

	// Bucket mutations by tablet
	buckets := make([][]*cclient.Mutation, len(t.tablets))
	for _, m := range mutations {
		idx := t.routeTablet(m.Row())
		buckets[idx] = append(buckets[idx], m)
	}

	// Write to each tablet in parallel
	var wg sync.WaitGroup
	errs := make([]error, len(t.tablets))
	for i, batch := range buckets {
		if len(batch) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, muts []*cclient.Mutation) {
			defer wg.Done()
			errs[idx] = t.tablets[idx].Write(muts)
		}(i, batch)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// lookupRows resolves many rows through the per-tablet LookupRows fast
// path, bucketing rows to their owning tablet so each tablet's source
// stack is opened once. Sequential across tablets; the parallel frontier
// path builds on this primitive.
func (t *table) lookupRows(rows [][]byte, opts ScanOptions, visit func(idx int, key *iterrt.Key, value []byte)) error {
	env := iterrt.IteratorEnvironment{
		Scope:          iterrt.ScopeScan,
		Authorizations: opts.Authorizations,
	}

	if len(t.tablets) == 1 {
		return t.tablets[0].LookupRows(rows, opts.ColumnFamilies, opts.ColumnFamiliesInclusive, opts.Stack, env, visit)
	}

	buckets := make([][]int, len(t.tablets))
	for i, row := range rows {
		ti := t.routeTablet(row)
		buckets[ti] = append(buckets[ti], i)
	}

	for ti, idxs := range buckets {
		if len(idxs) == 0 {
			continue
		}
		subRows := make([][]byte, len(idxs))
		for j, gi := range idxs {
			subRows[j] = rows[gi]
		}
		err := t.tablets[ti].LookupRows(subRows, opts.ColumnFamilies, opts.ColumnFamiliesInclusive, opts.Stack, env, func(local int, k *iterrt.Key, v []byte) {
			visit(idxs[local], k, v)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// neighbors resolves the out-edges of many rows, fanning out across
// NumCPU workers. Each row routes to its owning tablet, whose Neighbors
// serves the lookup from the shoal.adjacency index when available. The
// per-row independence makes frontier expansion embarrassingly parallel.
func (t *table) neighbors(rows [][]byte, edgeCF []byte, opts ScanOptions) ([][]Neighbor, error) {
	env := iterrt.IteratorEnvironment{
		Scope:          iterrt.ScopeScan,
		Authorizations: opts.Authorizations,
	}
	results := make([][]Neighbor, len(rows))
	errs := make([]error, len(rows))

	workers := runtime.NumCPU()
	if workers > len(rows) {
		workers = len(rows)
	}
	if workers <= 1 {
		for i, row := range rows {
			results[i], errs[i] = t.neighborsOne(row, edgeCF, env)
		}
	} else {
		var wg sync.WaitGroup
		jobs := make(chan int, len(rows))
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range jobs {
					results[i], errs[i] = t.neighborsOne(rows[i], edgeCF, env)
				}
			}()
		}
		for i := range rows {
			jobs <- i
		}
		close(jobs)
		wg.Wait()
	}

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

// neighborsOne resolves a single row's out-edges via its owning tablet
// and maps the tablet's edges to engine Neighbors.
func (t *table) neighborsOne(row, edgeCF []byte, env iterrt.IteratorEnvironment) ([]Neighbor, error) {
	tb := t.tablets[t.routeTablet(row)]
	edges, err := tb.Neighbors(row, edgeCF, env)
	if err != nil {
		return nil, err
	}
	if len(edges) == 0 {
		return nil, nil
	}
	out := make([]Neighbor, len(edges))
	for i, e := range edges {
		out[i] = Neighbor{Target: e.CQ, Value: e.Value, Timestamp: e.Timestamp, Vis: e.Vis}
	}
	return out, nil
}

// scan builds a merged scanner across all tablets whose range overlaps
// the requested scan range. Each tablet is scanned in its own goroutine.
func (t *table) scan(r iterrt.Range, opts ScanOptions) (*Scanner, error) {
	env := iterrt.IteratorEnvironment{
		Scope:          iterrt.ScopeScan,
		Authorizations: opts.Authorizations,
	}

	if len(t.tablets) == 1 {
		sc, err := t.tablets[0].Scan(r, opts.ColumnFamilies, opts.ColumnFamiliesInclusive, opts.Stack, env)
		if err != nil {
			return nil, err
		}
		// Wrap the single tablet scanner in an engine Scanner.
		// The tablet scanner's SKVI is already seek'd.
		return &Scanner{
			scanners: []*tablet.Scanner{sc},
			merge:    scannerToSKVI(sc),
		}, nil
	}

	// Multi-tablet: scan each overlapping tablet in parallel, then merge.
	type result struct {
		idx     int
		scanner *tablet.Scanner
		err     error
	}

	ch := make(chan result, len(t.tablets))
	for i := range t.tablets {
		go func(idx int) {
			sc, err := t.tablets[idx].Scan(r, opts.ColumnFamilies, opts.ColumnFamiliesInclusive, opts.Stack, env)
			ch <- result{idx, sc, err}
		}(i)
	}

	scanners := make([]*tablet.Scanner, 0, len(t.tablets))
	leaves := make([]iterrt.SortedKeyValueIterator, 0, len(t.tablets))
	var firstErr error

	for range t.tablets {
		res := <-ch
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
			}
			continue
		}
		scanners = append(scanners, res.scanner)
		leaves = append(leaves, scannerToSKVI(res.scanner))
	}

	if firstErr != nil {
		for _, sc := range scanners {
			sc.Close()
		}
		return nil, firstErr
	}

	if len(leaves) == 1 {
		return &Scanner{
			scanners: scanners,
			merge:    leaves[0],
		}, nil
	}

	merge := iterrt.NewMergingIterator(leaves...)
	if err := merge.Init(nil, nil, env); err != nil {
		for _, sc := range scanners {
			sc.Close()
		}
		return nil, err
	}
	// The individual tablet scanners are already seek'd, so we seek
	// the merge to infinite range — it just picks the smallest top.
	if err := merge.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
		for _, sc := range scanners {
			sc.Close()
		}
		return nil, err
	}

	return &Scanner{
		scanners: scanners,
		merge:    merge,
	}, nil
}

// scanHosted hosts a top-of-stack iterator above a re-seekable whole-table
// merge. It builds a MergingIterator over every tablet's Source (raw merged
// memtable + RFiles), version-caps across the whole table, then applies the
// caller's topStack — e.g. the term-index pushdown iterator. This is required
// for iterators that re-seek across the entire row space: the per-tablet,
// forward-only scan path cannot resolve a posting row in one tablet to a
// primary row in another.
//
// Version-capping with a single VersioningIterator above the cross-tablet
// merge is correct because tablets partition the row space — every cell
// coordinate lives in exactly one tablet, so all versions of a coordinate are
// adjacent in the merged stream.
func (t *table) scanHosted(r iterrt.Range, opts ScanOptions, topStack []iterrt.IterSpec) (*Scanner, error) {
	env := iterrt.IteratorEnvironment{
		Scope:          iterrt.ScopeScan,
		Authorizations: opts.Authorizations,
	}

	leaves := make([]iterrt.SortedKeyValueIterator, 0, len(t.tablets))
	var closers []func()
	cleanup := func() {
		for _, c := range closers {
			c()
		}
	}

	for _, tb := range t.tablets {
		src, closer, err := tb.Source(env)
		if err != nil {
			cleanup()
			return nil, err
		}
		leaves = append(leaves, src)
		closers = append(closers, closer)
	}

	merge := iterrt.NewMergingIterator(leaves...)
	if err := merge.Init(nil, nil, env); err != nil {
		cleanup()
		return nil, err
	}

	stack := append([]iterrt.IterSpec{
		{Name: iterrt.IterVersioning, Options: map[string]string{iterrt.VersioningOption: "1"}},
	}, topStack...)

	top, err := iterrt.BuildStack(merge, stack, env)
	if err != nil {
		cleanup()
		return nil, err
	}

	if err := top.Seek(r, opts.ColumnFamilies, opts.ColumnFamiliesInclusive); err != nil {
		cleanup()
		return nil, err
	}

	return &Scanner{merge: top, closers: closers}, nil
}

// flush forces all tablets to flush their memtables.
func (t *table) flush() error {
	var wg sync.WaitGroup
	errs := make([]error, len(t.tablets))
	for i := range t.tablets {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = t.tablets[idx].Flush()
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// compact runs compaction on all tablets in parallel.
func (t *table) compact(stack []iterrt.IterSpec) error {
	var wg sync.WaitGroup
	errs := make([]error, len(t.tablets))
	for i := range t.tablets {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = t.tablets[idx].Compact(stack)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *table) close() error {
	var firstErr error
	for i, tab := range t.tablets {
		if err := tab.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("tablet %d: %w", i, err)
		}
	}
	return firstErr
}

// scannerToSKVI wraps a tablet.Scanner as an iterrt.SortedKeyValueIterator
// so it can feed a MergingIterator for multi-tablet scans.
type scannerSKVI struct {
	sc *tablet.Scanner
}

func scannerToSKVI(sc *tablet.Scanner) iterrt.SortedKeyValueIterator {
	return &scannerSKVI{sc: sc}
}

func (s *scannerSKVI) Init(_ iterrt.SortedKeyValueIterator, _ map[string]string, _ iterrt.IteratorEnvironment) error {
	return nil
}

func (s *scannerSKVI) Seek(_ iterrt.Range, _ [][]byte, _ bool) error {
	// Already seek'd by the tablet.Scan call.
	return nil
}

func (s *scannerSKVI) Next() error {
	return s.sc.Advance()
}

func (s *scannerSKVI) HasTop() bool {
	return s.sc.Next()
}

func (s *scannerSKVI) GetTopKey() *iterrt.Key {
	return s.sc.Key()
}

func (s *scannerSKVI) GetTopValue() []byte {
	return s.sc.Value()
}

func (s *scannerSKVI) DeepCopy(_ iterrt.IteratorEnvironment) iterrt.SortedKeyValueIterator {
	// Multi-tablet merge doesn't DeepCopy its sources.
	return nil
}
