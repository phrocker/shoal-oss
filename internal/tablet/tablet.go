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

// Package tablet is the embedded engine's unit of storage. A Tablet owns
// one active memtable, a local WAL, and a set of on-disk RFiles. It
// supports concurrent reads (multiple Scanner goroutines) with serialized
// writes (one writer at a time via mutex).
//
// The Tablet does not know about tables or split policies — that
// abstraction lives in the engine package. A Tablet is responsible for:
//
//  1. Accepting mutations into its memtable (and WAL for durability).
//  2. Flushing the memtable to a new RFile when a size threshold is hit.
//  3. Serving scans by merging the memtable + on-disk RFiles through
//     the SKVI iterator stack.
//  4. Running compactions (merge N RFiles through an iterator stack →
//     1 output RFile).
//  5. Recovering from crashes by replaying the WAL.
package tablet

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phrocker/shoal/internal/cclient"
	"github.com/phrocker/shoal/internal/compaction"
	"github.com/phrocker/shoal/internal/iterrt"
	"github.com/phrocker/shoal/internal/localwal"
	"github.com/phrocker/shoal/internal/rfile"
	"github.com/phrocker/shoal/internal/rfile/adjacency"
	"github.com/phrocker/shoal/internal/rfile/bcfile/block"
	"github.com/phrocker/shoal/internal/rfile/wire"
	"github.com/phrocker/shoal/internal/storage"
	"github.com/phrocker/shoal/internal/storage/local"
	"github.com/phrocker/shoal/internal/visfilter"
)

// DefaultFlushThreshold is the cell count at which the memtable is
// automatically flushed to an RFile. 256K cells balances memory use
// against write amplification for bulk ingest workloads.
const DefaultFlushThreshold = 256_000

// Tablet is one range of a table's key space.
type Tablet struct {
	mu      sync.RWMutex
	dir     string
	active  *skiplistMemtable
	files   []string // sorted list of RFile paths/keys, oldest first
	wal     *localwal.WAL
	seq     atomic.Int64
	logger  *slog.Logger
	opts    Options
	backend storage.Backend // object store for RFile bytes (default: local FS)
}

// Options configures a Tablet.
type Options struct {
	// FlushThreshold is the cell count that triggers an automatic flush.
	// Zero defaults to DefaultFlushThreshold.
	FlushThreshold int

	// Logger for tablet operations. Nil uses slog.Default().
	Logger *slog.Logger

	// Cache (optional) shares immutable RFile bytes and decompressed
	// blocks across scans. Nil disables caching (every scan re-reads
	// and re-inflates each RFile). Engines pass one shared Cache to
	// every tablet so the byte budget is global.
	Cache *Cache

	// AdjacencyEdgeCF, when non-empty, makes flush + compaction emit a
	// shoal.adjacency out-edge index over cells in this column family.
	// It lets Neighbors answer "out-edges of row" via a binary search +
	// contiguous slice read instead of a full merge/versioning scan.
	// Empty disables the index (every tablet behaves as before).
	AdjacencyEdgeCF string

	// WALSyncMode selects the durability tier for the write-ahead log.
	// The zero value is localwal.SyncFull (fsync every write — safest).
	// SyncNormal/SyncOff move the fsync off the per-write hot path; pair
	// SyncNormal with WALSyncInterval to bound the data-loss window.
	WALSyncMode localwal.SyncMode

	// WALSyncInterval, when > 0, enables group-commit: a background
	// goroutine fsyncs the WAL at most once per interval whenever writes
	// are pending. Only meaningful under SyncNormal/SyncOff. Zero leaves
	// fsync timing entirely to the SyncMode.
	WALSyncInterval time.Duration

	// Backend is the object store RFiles are written to and read from.
	// Nil defaults to the local filesystem (storage/local), preserving the
	// historical on-disk behavior. A memory or cloud backend lets the same
	// tablet keep its WAL local while flushing immutable RFiles elsewhere.
	// The WAL is always local regardless of this setting.
	Backend storage.Backend

	// OnRFile, when set, is invoked after a flush or compaction writes a new
	// immutable RFile, with the event kind ("flush" | "compact") and the new
	// RFile's base name. It enables event-driven shipping (sync as soon as an
	// RFile lands) instead of polling. It is called while the tablet lock is
	// held, so the callback MUST NOT block or call back into the tablet;
	// engines wire it to a non-blocking publish.
	OnRFile func(kind, file string)
}

// Open opens or creates a tablet in dir. On startup, it:
//  1. Reads the file manifest to discover existing RFiles.
//  2. Opens or creates the WAL.
//  3. Replays any unflushed WAL entries into the memtable.
func Open(dir string, opts Options) (*Tablet, error) {
	if opts.FlushThreshold <= 0 {
		opts.FlushThreshold = DefaultFlushThreshold
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("tablet: mkdir %s: %w", dir, err)
	}

	backend := opts.Backend
	if backend == nil {
		backend = local.New()
	}

	t := &Tablet{
		dir:     dir,
		active:  newSkiplistMemtable(),
		opts:    opts,
		logger:  opts.Logger,
		backend: backend,
	}

	// Discover existing RFiles via the backend manifest (a directory
	// listing for the local FS; a prefix scan for memory/cloud stores).
	keys, err := listRFiles(backend, dir)
	if err != nil {
		return nil, fmt.Errorf("tablet: list %s: %w", dir, err)
	}
	t.files = keys
	sort.Strings(t.files)

	// Open WAL
	walPath := filepath.Join(dir, "wal.log")
	t.wal, err = localwal.Open(walPath,
		localwal.WithSyncMode(opts.WALSyncMode),
		localwal.WithSyncInterval(opts.WALSyncInterval),
	)
	if err != nil {
		return nil, fmt.Errorf("tablet: open wal: %w", err)
	}

	// Replay WAL
	replayed, err := t.wal.Replay(func(m *cclient.Mutation) error {
		t.ingestMutation(m)
		return nil
	})
	if err != nil {
		t.wal.Close()
		return nil, fmt.Errorf("tablet: replay wal: %w", err)
	}
	if replayed > 0 {
		t.logger.Info("wal replay complete",
			slog.String("dir", dir),
			slog.Int("mutations", replayed),
			slog.Int("cells", t.active.Len()))
	}

	return t, nil
}

// Write applies mutations to the memtable and WAL. If the memtable
// exceeds the flush threshold after this write, it is automatically
// flushed to a new RFile.
func (t *Tablet) Write(mutations []*cclient.Mutation) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// WAL first — crash-safe
	if _, err := t.wal.Append(mutations); err != nil {
		return fmt.Errorf("tablet: wal append: %w", err)
	}

	for _, m := range mutations {
		t.ingestMutation(m)
	}

	if t.active.Len() >= t.opts.FlushThreshold {
		if err := t.flushLocked(); err != nil {
			return fmt.Errorf("tablet: auto-flush: %w", err)
		}
	}
	return nil
}

// Scan returns a Scanner over this tablet's data — the merge of the
// active memtable and all on-disk RFiles, filtered through the given
// iterator stack. The Scanner is valid until Close is called.
//
// columnFamilies + inclusive follow the SKVI Seek contract: pass nil
// with inclusive=false for a full scan.
func (t *Tablet) Scan(r iterrt.Range, columnFamilies [][]byte, inclusive bool, stack []iterrt.IterSpec, env iterrt.IteratorEnvironment) (*Scanner, error) {
	merge, closeAll, err := t.Source(env)
	if err != nil {
		return nil, err
	}

	// Stack user iterators on top
	top, err := iterrt.BuildStack(merge, stack, env)
	if err != nil {
		closeAll()
		return nil, fmt.Errorf("tablet: build stack: %w", err)
	}

	// Seek to the requested range
	if err := top.Seek(r, columnFamilies, inclusive); err != nil {
		closeAll()
		return nil, fmt.Errorf("tablet: seek: %w", err)
	}

	return &Scanner{iter: top, closers: []func(){closeAll}}, nil
}

// LookupRows fetches the cells of many rows over a SINGLE re-seekable
// source stack, re-Seeking it per row instead of rebuilding the
// memtable+RFile merge and iterator stack on every lookup (what a Scan
// per row would do). This amortizes the dominant per-lookup cost across
// the whole batch — the foundation for fast graph-style point reads.
//
// rows[i] is looked up as a whole-row range and visit is invoked for
// every cell, with idx = the row's position in rows. Rows are visited in
// ascending key order (not input order) so the shared cursor only seeks
// forward; callers that need input order use the idx argument. The
// key/value passed to visit are only valid for that call — copy anything
// retained. columnFamilies + inclusive follow the SKVI Seek contract.
func (t *Tablet) LookupRows(rows [][]byte, columnFamilies [][]byte, inclusive bool, stack []iterrt.IterSpec, env iterrt.IteratorEnvironment, visit func(idx int, key *iterrt.Key, value []byte)) error {
	if len(rows) == 0 {
		return nil
	}

	merge, closeAll, err := t.Source(env)
	if err != nil {
		return err
	}
	defer closeAll()

	top, err := iterrt.BuildStack(merge, stack, env)
	if err != nil {
		return fmt.Errorf("tablet: build stack: %w", err)
	}

	// Visit rows in ascending key order so the shared cursor seeks
	// monotonically forward through the LSM merge.
	order := make([]int, len(rows))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		return bytes.Compare(rows[order[a]], rows[order[b]]) < 0
	})

	for _, idx := range order {
		row := rows[idx]
		// Whole-row range: [{row}, {row\x00}) — a bare {row} key sorts
		// before the row's non-empty-CF cells so can't be an inclusive
		// upper bound.
		start := &wire.Key{Row: row}
		end := &wire.Key{Row: rowSuccessor(row)}
		r := iterrt.Range{Start: start, StartInclusive: true, End: end, EndInclusive: false}
		if err := top.Seek(r, columnFamilies, inclusive); err != nil {
			return fmt.Errorf("tablet: lookup seek: %w", err)
		}
		for top.HasTop() {
			visit(idx, top.GetTopKey(), top.GetTopValue())
			if err := top.Next(); err != nil {
				return fmt.Errorf("tablet: lookup next: %w", err)
			}
		}
	}
	return nil
}

// rowSuccessor returns the smallest row key strictly greater than row,
// formed by appending a 0x00 byte. Used as an exclusive upper bound to
// cover every cell of a single row.
func rowSuccessor(row []byte) []byte {
	out := make([]byte, len(row)+1)
	copy(out, row)
	return out
}

// Neighbors returns the resolved out-edges of row for the given edge
// column family — exactly the cells a Scan over (row, edgeCF) with
// version + delete resolution would yield, but served from the
// shoal.adjacency CSR index when every on-disk file carries one. The
// active memtable is always scanned (it has no index); its edges and any
// non-indexed file's edges fall back to a row-range Scan.
//
// Resolution matches Accumulo column semantics: per (columnQualifier,
// visibility) the newest timestamp wins; a winning delete tombstone
// suppresses the edge. Returned edges are sorted by columnQualifier and
// own their bytes (safe to retain).
func (t *Tablet) Neighbors(row, edgeCF []byte, env iterrt.IteratorEnvironment) ([]adjacency.Edge, error) {
	t.mu.RLock()
	memIter := t.active.Iterator()
	filesCopy := make([]string, len(t.files))
	copy(filesCopy, t.files)
	t.mu.RUnlock()

	// winners[(cq,vis)] = newest version seen. consider applies the
	// newest-wins + delete-on-tie rule; identical bytes considered twice
	// (e.g. a file's index and a fallback scan) is harmless. Cells the
	// caller is not authorized to see are dropped before resolution, so
	// the index path matches a scan through the visibility filter.
	var eval *visfilter.Evaluator
	if env.Scope == iterrt.ScopeScan && env.Authorizations != nil {
		eval = visfilter.NewEvaluator(visfilter.NewAuthorizations(env.Authorizations...))
	}
	type col struct{ cq, vis string }
	winners := make(map[col]adjacency.Edge)
	consider := func(e adjacency.Edge) {
		if eval != nil && !eval.Visible(e.Vis) {
			return
		}
		k := col{string(e.CQ), string(e.Vis)}
		cur, ok := winners[k]
		if !ok || e.Timestamp > cur.Timestamp || (e.Timestamp == cur.Timestamp && e.Deleted) {
			winners[k] = e
		}
	}

	// Try the index path for every on-disk file. If any file lacks an
	// adjacency index, fall back to a full row-range Scan (covers all
	// files + memtable) and skip the per-file/memtable work below.
	allIndexed := true
	for _, path := range filesCopy {
		sf, err := t.sharedForPath(path)
		if err != nil {
			return nil, fmt.Errorf("tablet: neighbors open %s: %w", path, err)
		}
		edges, ok := sf.Neighbors(row)
		if !ok {
			allIndexed = false
			break
		}
		for i := range edges {
			consider(cloneEdge(edges[i]))
		}
	}

	if allIndexed {
		// Indexed files done; the memtable has no index, so scan it.
		if err := scanEdgesInto(memIter, row, edgeCF, env, consider); err != nil {
			return nil, fmt.Errorf("tablet: neighbors memtable scan: %w", err)
		}
	} else {
		// Reset accumulation and take the merged-scan path for everything.
		winners = make(map[col]adjacency.Edge)
		merge, closeAll, err := t.Source(env)
		if err != nil {
			return nil, err
		}
		defer closeAll()
		if err := scanEdgesInto(merge, row, edgeCF, env, consider); err != nil {
			return nil, fmt.Errorf("tablet: neighbors scan: %w", err)
		}
	}

	out := make([]adjacency.Edge, 0, len(winners))
	for _, e := range winners {
		if e.Deleted {
			continue
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if c := bytes.Compare(out[i].CQ, out[j].CQ); c != 0 {
			return c < 0
		}
		return bytes.Compare(out[i].Vis, out[j].Vis) < 0
	})
	return out, nil
}

// scanEdgesInto seeks src to row's edgeCF cells and feeds each as an Edge
// into consider. src is any re-seekable SKVI (memtable iterator or merged
// source); it is left positioned past the range.
func scanEdgesInto(src iterrt.SortedKeyValueIterator, row, edgeCF []byte, env iterrt.IteratorEnvironment, consider func(adjacency.Edge)) error {
	start := &wire.Key{Row: row}
	end := &wire.Key{Row: rowSuccessor(row)}
	r := iterrt.Range{Start: start, StartInclusive: true, End: end, EndInclusive: false}
	if err := src.Seek(r, [][]byte{edgeCF}, true); err != nil {
		return err
	}
	for src.HasTop() {
		k := src.GetTopKey()
		if bytes.Equal(k.ColumnFamily, edgeCF) {
			consider(adjacency.Edge{
				CQ:        cloneBytes(k.ColumnQualifier),
				Value:     cloneBytes(src.GetTopValue()),
				Timestamp: k.Timestamp,
				Deleted:   k.Deleted,
				Vis:       cloneBytes(k.ColumnVisibility),
			})
		}
		if err := src.Next(); err != nil {
			return err
		}
	}
	return nil
}

// cloneEdge deep-copies an Edge so the result is safe to retain after the
// backing index slice is gone.
func cloneEdge(e adjacency.Edge) adjacency.Edge {
	return adjacency.Edge{
		CQ:        cloneBytes(e.CQ),
		Value:     cloneBytes(e.Value),
		Timestamp: e.Timestamp,
		Deleted:   e.Deleted,
		Vis:       cloneBytes(e.Vis),
	}
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// Source returns the tablet's merged, re-seekable SKVI over a snapshot of its
// current memtable + on-disk RFiles, with no user iterator stack applied and
// not yet seeked. The returned closer releases the RFile readers and must be
// called when the caller is done with the source.
//
// Unlike Scan (which applies a stack and seeks, returning a forward-only
// Scanner), Source hands back the raw re-seekable merge. It exists so the
// engine can host iterators that re-seek across the whole table — e.g. the
// term-index pushdown, where a posting row and its referenced primary rows
// may fall in different tablets — by merging every tablet's Source and
// stacking the iterator above that cross-tablet merge.
func (t *Tablet) Source(env iterrt.IteratorEnvironment) (iterrt.SortedKeyValueIterator, func(), error) {
	t.mu.RLock()
	// Snapshot memtable iterator + file list under read lock
	memIter := t.active.Iterator()
	filesCopy := make([]string, len(t.files))
	copy(filesCopy, t.files)
	t.mu.RUnlock()

	// Build leaf iterators: one from memtable + one per RFile
	leaves := []iterrt.SortedKeyValueIterator{memIter}
	var closers []func()

	for _, path := range filesCopy {
		src, closer, err := t.openRFileSource(path, env)
		if err != nil {
			// Clean up any already-opened readers
			for _, c := range closers {
				c()
			}
			return nil, nil, fmt.Errorf("tablet: open %s: %w", path, err)
		}
		leaves = append(leaves, src)
		closers = append(closers, closer)
	}

	// Merge all leaves
	merge := iterrt.NewMergingIterator(leaves...)
	if err := merge.Init(nil, nil, env); err != nil {
		for _, c := range closers {
			c()
		}
		return nil, nil, fmt.Errorf("tablet: merge init: %w", err)
	}

	closeAll := func() {
		for _, c := range closers {
			c()
		}
	}
	return merge, closeAll, nil
}

// Flush forces the memtable to disk as a new RFile and truncates the WAL.
func (t *Tablet) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.flushLocked()
}

// Compact merges all on-disk RFiles through the given iterator stack
// into a single output RFile. This is where application-specific iterators
// (decay, pruning, dedup) run.
func (t *Tablet) Compact(stack []iterrt.IterSpec) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.files) < 2 {
		return nil // nothing to compact
	}

	// Read all input RFiles (served from cache when warm)
	inputs := make([]compaction.Input, 0, len(t.files))
	for _, path := range t.files {
		data, err := t.fileBytes(path)
		if err != nil {
			return fmt.Errorf("tablet: read %s for compaction: %w", path, err)
		}
		inputs = append(inputs, compaction.Input{Name: path, Bytes: data})
	}

	result, err := compaction.Compact(compaction.Spec{
		Inputs:              inputs,
		Stack:               stack,
		Scope:               iterrt.ScopeMajc,
		FullMajorCompaction: true,
		AdjacencyEdgeCF:     t.opts.AdjacencyEdgeCF,
	})
	if err != nil {
		return fmt.Errorf("tablet: compact: %w", err)
	}

	// Write output RFile
	outName := fmt.Sprintf("C%013d.rf", time.Now().UnixMilli())
	outPath := filepath.Join(t.dir, outName)
	if err := storage.WriteAll(context.Background(), t.backend, outPath, result.Output); err != nil {
		return fmt.Errorf("tablet: write compacted: %w", err)
	}

	// Remove old files
	oldFiles := t.files
	t.files = []string{outPath}
	for _, old := range oldFiles {
		t.opts.Cache.Drop(old)
		removeObject(t.backend, old)
	}

	t.logger.Info("compaction complete",
		slog.Int("inputs", len(oldFiles)),
		slog.Int64("entries", result.EntriesWritten),
		slog.String("output", outName))
	if t.opts.OnRFile != nil {
		t.opts.OnRFile("compact", outName)
	}

	return nil
}

// FileCount returns the number of on-disk RFiles.
func (t *Tablet) FileCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.files)
}

// RFiles returns a snapshot of the tablet's immutable RFile paths/keys.
func (t *Tablet) RFiles() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]string, len(t.files))
	copy(out, t.files)
	return out
}

// RefreshFiles re-discovers this tablet's on-disk RFiles through the backend
// and replaces the tracked file set. It is how an import merges newly arrived
// RFiles (e.g. shipped by another producer into a shared destination) into an
// already-open tablet without a full reopen. Re-listing is inherently
// idempotent and deduped: each object appears once regardless of how many
// times RefreshFiles runs, so re-importing an unchanged manifest is a no-op.
// Returns the number of RFiles now tracked.
func (t *Tablet) RefreshFiles() (int, error) {
	keys, err := listRFiles(t.backend, t.dir)
	if err != nil {
		return 0, fmt.Errorf("tablet: refresh list %s: %w", t.dir, err)
	}
	sort.Strings(keys)
	t.mu.Lock()
	t.files = keys
	n := len(t.files)
	t.mu.Unlock()
	return n, nil
}

// MemtableSize returns the cell count in the active memtable.
func (t *Tablet) MemtableSize() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.active.Len()
}

// Close flushes any pending data and closes the WAL.
func (t *Tablet) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.active.Len() > 0 {
		if err := t.flushLocked(); err != nil {
			t.logger.Warn("flush on close failed", slog.String("err", err.Error()))
		}
	}
	return t.wal.Close()
}

// flushLocked writes the active memtable to a new RFile, resets the
// memtable, and truncates the WAL. Caller holds t.mu write lock.
func (t *Tablet) flushLocked() error {
	if t.active.Len() == 0 {
		return nil
	}

	iter := t.active.Iterator()
	if err := iter.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
		return fmt.Errorf("flush: seek: %w", err)
	}

	// Write to buffer, then atomically to file
	var buf bytes.Buffer
	w, err := rfile.NewWriter(&buf, rfile.WriterOptions{Codec: block.CodecSnappy, AdjacencyEdgeCF: t.opts.AdjacencyEdgeCF})
	if err != nil {
		return fmt.Errorf("flush: new writer: %w", err)
	}
	var count int64
	for iter.HasTop() {
		if err := w.Append(iter.GetTopKey(), iter.GetTopValue()); err != nil {
			return fmt.Errorf("flush: append cell %d: %w", count, err)
		}
		count++
		if err := iter.Next(); err != nil {
			return fmt.Errorf("flush: next after cell %d: %w", count, err)
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("flush: close writer: %w", err)
	}

	outName := fmt.Sprintf("F%013d.rf", time.Now().UnixMilli())
	outPath := filepath.Join(t.dir, outName)
	if err := storage.WriteAll(context.Background(), t.backend, outPath, buf.Bytes()); err != nil {
		return fmt.Errorf("flush: write %s: %w", outPath, err)
	}

	t.files = append(t.files, outPath)
	t.active = newSkiplistMemtable()

	if err := t.wal.Truncate(); err != nil {
		return fmt.Errorf("flush: truncate wal: %w", err)
	}

	t.logger.Info("flush complete",
		slog.Int64("cells", count),
		slog.String("file", outName),
		slog.Int("total_files", len(t.files)))
	if t.opts.OnRFile != nil {
		t.opts.OnRFile("flush", outName)
	}
	return nil
}

// ingestMutation inserts a mutation's cells into the active memtable.
func (t *Tablet) ingestMutation(m *cclient.Mutation) {
	for _, c := range m.Cells() {
		t.active.Insert(c.Key, c.Value)
	}
}

// fileBytes returns the bytes of an RFile, served from the shared cache
// when present, otherwise faulted directly from the tablet's backend.
// Centralizing the read here keeps the nil-Cache path on the tablet's
// configured backend instead of silently defaulting to local.
func (t *Tablet) fileBytes(path string) ([]byte, error) {
	if t.opts.Cache != nil {
		return t.opts.Cache.fileBytes(path)
	}
	return storage.ReadAll(context.Background(), t.backend, path)
}

// sharedForPath returns the parse-once SharedFile for an RFile, via the
// cache when present, else built fresh from backend-loaded bytes.
func (t *Tablet) sharedForPath(path string) (*rfile.SharedFile, error) {
	if t.opts.Cache != nil {
		return t.opts.Cache.sharedForPath(path)
	}
	data, err := storage.ReadAll(context.Background(), t.backend, path)
	if err != nil {
		return nil, err
	}
	return (*Cache)(nil).sharedFile(path, data, nil)
}

// openRFileSource opens one RFile as an SKVI leaf, returning the iterator
// and a closer function. Bytes come from the tablet's backend (served from
// the shared cache when warm); the reader shares a decompressed-block
// cache keyed by path when caching is enabled. RFiles are immutable by
// path, so the shared bytes slice is safe to wrap in concurrent read-only
// readers.
func (t *Tablet) openRFileSource(path string, env iterrt.IteratorEnvironment) (iterrt.SortedKeyValueIterator, func(), error) {
	data, err := t.fileBytes(path)
	if err != nil {
		return nil, nil, err
	}
	c := t.opts.Cache
	blocks := c.blockCache()

	// Parse the RFile index + collect leaves once (memoized per path),
	// then build each cursor over that shared immutable state. This skips
	// the bcfile/index parse and the full leaf re-collection that would
	// otherwise run on every Seek — the dominant per-lookup cost.
	sf, err := c.sharedFile(path, data, blocks)
	if err != nil {
		return nil, nil, err
	}

	open := func() (*rfile.Reader, error) {
		var opts []rfile.OpenOption
		if blocks != nil {
			opts = append(opts, rfile.WithBlockCache(blocks, path))
		}
		return rfile.NewReaderFromShared(sf, block.Default(), opts...), nil
	}

	rdr, err := open()
	if err != nil {
		return nil, nil, err
	}

	src := iterrt.NewRFileSource(rdr, open)
	if err := src.Init(nil, nil, env); err != nil {
		rdr.Close()
		return nil, nil, err
	}
	return src, func() { rdr.Close() }, nil
}

// listRFiles discovers a tablet's RFiles under dir through the backend's
// Lister capability (a prefix scan for memory/cloud, a directory listing
// for local). Falls back to an os.ReadDir for a backend without Lister.
// Only ".rf" objects are returned (the WAL and other files are ignored).
func listRFiles(b storage.Backend, dir string) ([]string, error) {
	var keys []string
	if lister, ok := b.(storage.Lister); ok {
		ks, err := lister.List(context.Background(), dir)
		if err != nil {
			return nil, err
		}
		keys = ks
	} else {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if !e.IsDir() {
				keys = append(keys, filepath.Join(dir, e.Name()))
			}
		}
	}
	out := keys[:0]
	for _, k := range keys {
		if filepath.Ext(k) == ".rf" {
			out = append(out, k)
		}
	}
	return out, nil
}

// removeObject deletes an RFile through the backend's Remover capability,
// falling back to os.Remove. Best-effort: a failed delete leaves an
// orphan object but does not corrupt the live file set.
func removeObject(b storage.Backend, path string) {
	if r, ok := b.(storage.Remover); ok {
		_ = r.Remove(context.Background(), path)
		return
	}
	_ = os.Remove(path)
}

// Scanner is a pull-based iterator over scan results.
type Scanner struct {
	iter    iterrt.SortedKeyValueIterator
	closers []func()
}

// Next reports whether there is a current key/value pair.
func (s *Scanner) Next() bool {
	return s.iter.HasTop()
}

// Key returns the current key. Valid until Advance is called.
func (s *Scanner) Key() *wire.Key {
	return s.iter.GetTopKey()
}

// Value returns the current value. Valid until Advance is called.
func (s *Scanner) Value() []byte {
	return s.iter.GetTopValue()
}

// Advance moves to the next key/value pair.
func (s *Scanner) Advance() error {
	return s.iter.Next()
}

// Close releases all resources held by the scanner.
func (s *Scanner) Close() {
	for _, c := range s.closers {
		c()
	}
}
