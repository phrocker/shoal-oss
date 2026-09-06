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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phrocker/shoal-oss/internal/cclient"
	"github.com/phrocker/shoal-oss/internal/compaction"
	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/iterrt"
	"github.com/phrocker/shoal-oss/internal/localwal"
	"github.com/phrocker/shoal-oss/internal/parquetfile"
	"github.com/phrocker/shoal-oss/internal/rfile"
	"github.com/phrocker/shoal-oss/internal/rfile/adjacency"
	"github.com/phrocker/shoal-oss/internal/rfile/bcfile"
	"github.com/phrocker/shoal-oss/internal/rfile/bcfile/block"
	"github.com/phrocker/shoal-oss/internal/rfile/wire"
	"github.com/phrocker/shoal-oss/internal/storage"
	"github.com/phrocker/shoal-oss/internal/storage/local"
	"github.com/phrocker/shoal-oss/internal/visfilter"
)

// DefaultFlushThreshold is the cell count at which the memtable is
// automatically flushed to an RFile. 256K cells balances memory use
// against write amplification for bulk ingest workloads.
const DefaultFlushThreshold = 256_000

var ErrEmbeddingStateChangeWithUnflushedData = errors.New(
	"tablet: cannot change default embedding state with unflushed data")

var ErrMixedEmbeddingCompactionStack = errors.New(
	"tablet: iterator stack requires a whole-table view across mixed embedding spaces")

type FileFormat string

const (
	FormatRFile   FileFormat = "rfile"
	FormatParquet FileFormat = "parquet"
)

func ParseFileFormat(value string) (FileFormat, error) {
	switch FileFormat(value) {
	case "", FormatRFile:
		return FormatRFile, nil
	case FormatParquet:
		return FormatParquet, nil
	default:
		return "", fmt.Errorf("tablet: unsupported file format %q (want rfile or parquet)", value)
	}
}

// Tablet is one range of a table's key space.
type Tablet struct {
	mu       sync.RWMutex
	dir      string
	active   *skiplistMemtable
	files    []string // sorted list of RFile paths/keys, oldest first
	wal      *localwal.WAL
	seq      atomic.Int64
	logger   *slog.Logger
	opts     Options
	backend  storage.Backend // object store for RFile bytes (default: local FS)
	obsolete map[string]struct{}
}

// ConditionKind identifies the predicate evaluated by a conditional mutation.
type ConditionKind int

const (
	ConditionAbsent ConditionKind = iota + 1
	ConditionValueEquals
	// ConditionLatestValueAndTimestampEquals compares both the value and
	// timestamp of the newest live version. It is the generation-fenced CAS
	// predicate used by the embedded Explorer transaction store.
	ConditionLatestValueAndTimestampEquals
)

// Condition targets one cell coordinate in the mutation's row. Timestamp nil
// selects the newest version; a non-nil timestamp selects that exact version
// except for ConditionLatestValueAndTimestampEquals, which checks that the
// newest live version has the supplied timestamp and value.
type Condition struct {
	ColumnFamily     []byte
	ColumnQualifier  []byte
	ColumnVisibility []byte
	Timestamp        *int64
	Kind             ConditionKind
	Value            []byte
}

// ConditionalMutation atomically evaluates Conditions and applies Mutation
// under the tablet's writer lock.
type ConditionalMutation struct {
	Mutation   *cclient.Mutation
	Conditions []Condition
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

	// FileFormat selects the immutable file format written by flush and
	// compaction. Existing RFile and Parquet files can be read together.
	// The zero value preserves the historical RFile behavior.
	FileFormat FileFormat

	// DefaultEmbedding is the actual embedding state written into each new
	// immutable file. The zero value writes no claim and reads as unknown.
	DefaultEmbedding embeddingspace.FileState

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
	format, err := ParseFileFormat(string(opts.FileFormat))
	if err != nil {
		return nil, err
	}
	opts.FileFormat = format
	if opts.DefaultEmbedding != (embeddingspace.FileState{}) {
		if err := opts.DefaultEmbedding.Validate(); err != nil {
			return nil, fmt.Errorf("tablet: invalid default embedding state: %w", err)
		}
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
	keys, obsolete, err := discoverImmutableFiles(backend, dir, false)
	if err != nil {
		return nil, fmt.Errorf("tablet: list %s: %w", dir, err)
	}
	t.files = keys
	t.obsolete = obsolete
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
	return t.writeLocked(mutations)
}

func (t *Tablet) writeLocked(mutations []*cclient.Mutation) error {
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

// ConditionalWrite evaluates and applies each mutation in request order. The
// result slice aligns with mutations; false means its conditions did not match.
func (t *Tablet) ConditionalWrite(mutations []ConditionalMutation) ([]bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	results := make([]bool, len(mutations))
	for i, mutation := range mutations {
		matched, err := t.conditionsMatchLocked(mutation.Mutation.Row(), mutation.Conditions)
		if err != nil {
			return nil, err
		}
		if !matched {
			continue
		}
		if _, err := t.wal.Append([]*cclient.Mutation{mutation.Mutation}); err != nil {
			return nil, fmt.Errorf("tablet: wal append: %w", err)
		}
		t.ingestMutation(mutation.Mutation)
		results[i] = true
	}

	if t.active.Len() >= t.opts.FlushThreshold {
		if err := t.flushLocked(); err != nil {
			return nil, fmt.Errorf("tablet: auto-flush: %w", err)
		}
	}
	return results, nil
}

func (t *Tablet) conditionsMatchLocked(row []byte, conditions []Condition) (bool, error) {
	if len(conditions) == 0 {
		return true, nil
	}
	source, closeAll, err := t.sourceLocked(iterrt.IteratorEnvironment{Scope: iterrt.ScopeScan})
	if err != nil {
		return false, err
	}
	defer closeAll()

	for _, condition := range conditions {
		start := &wire.Key{
			Row:              row,
			ColumnFamily:     condition.ColumnFamily,
			ColumnQualifier:  condition.ColumnQualifier,
			ColumnVisibility: condition.ColumnVisibility,
			Timestamp:        math.MaxInt64,
			Deleted:          true,
		}
		if condition.Timestamp != nil && condition.Kind != ConditionLatestValueAndTimestampEquals {
			start.Timestamp = *condition.Timestamp
		}
		if err := source.Seek(iterrt.Range{
			Start: start, StartInclusive: true, InfiniteEnd: true,
		}, nil, false); err != nil {
			return false, fmt.Errorf("tablet: condition seek: %w", err)
		}

		exists := false
		var value []byte
		if source.HasTop() {
			key := source.GetTopKey()
			sameCoordinate := bytes.Equal(key.Row, row) &&
				bytes.Equal(key.ColumnFamily, condition.ColumnFamily) &&
				bytes.Equal(key.ColumnQualifier, condition.ColumnQualifier) &&
				bytes.Equal(key.ColumnVisibility, condition.ColumnVisibility)
			sameTimestamp := condition.Timestamp == nil || key.Timestamp == *condition.Timestamp
			if sameCoordinate && sameTimestamp && !key.Deleted {
				exists = true
				value = source.GetTopValue()
			}
		}

		switch condition.Kind {
		case ConditionAbsent:
			if exists {
				return false, nil
			}
		case ConditionValueEquals:
			if !exists || !bytes.Equal(value, condition.Value) {
				return false, nil
			}
		case ConditionLatestValueAndTimestampEquals:
			if condition.Timestamp == nil || !exists ||
				source.GetTopKey().Timestamp != *condition.Timestamp ||
				!bytes.Equal(value, condition.Value) {
				return false, nil
			}
		default:
			return false, fmt.Errorf("tablet: unsupported condition kind %d", condition.Kind)
		}
	}
	return true, nil
}

// Scan returns a Scanner over this tablet's data — the merge of the
// active memtable and all on-disk RFiles, filtered through the given
// iterator stack. The Scanner is valid until Close is called.
//
// columnFamilies + inclusive follow the SKVI Seek contract: pass nil
// with inclusive=false for a full scan.
func (t *Tablet) Scan(r iterrt.Range, columnFamilies [][]byte, inclusive bool, stack []iterrt.IterSpec, env iterrt.IteratorEnvironment) (*Scanner, error) {
	for _, spec := range stack {
		if spec.Name == iterrt.IterVectorKNN {
			return nil, fmt.Errorf(
				"%w: vectorKNN requires engine-hosted metadata validation",
				embeddingspace.ErrQueryMetadataMissing)
		}
	}
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
		if fileFormat(path) == FormatParquet {
			allIndexed = false
			break
		}
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
	return t.SourceContext(context.Background(), env)
}

// SourceContext is Source with cancellation propagated to immutable-file
// opens and reads during source construction.
func (t *Tablet) SourceContext(
	ctx context.Context,
	env iterrt.IteratorEnvironment,
) (iterrt.SortedKeyValueIterator, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.RLock()
	source, closeSource, err := t.sourceLockedContext(ctx, env)
	if err != nil {
		t.mu.RUnlock()
		return nil, nil, err
	}
	var once sync.Once
	return source, func() {
		once.Do(func() {
			closeSource()
			t.mu.RUnlock()
		})
	}, nil
}

// SnapshotSourceContext returns a source whose memtable component is copied
// while holding the tablet lock. Later writes cannot join the returned scan.
func (t *Tablet) SnapshotSourceContext(
	ctx context.Context,
	env iterrt.IteratorEnvironment,
) (iterrt.SortedKeyValueIterator, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	mem := t.active.Iterator()
	if err := mem.Init(nil, nil, env); err != nil {
		return nil, nil, err
	}
	if err := mem.Seek(iterrt.InfiniteRange(), nil, false); err != nil {
		return nil, nil, err
	}
	var cells []iterrt.Cell
	for mem.HasTop() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		cells = append(cells, iterrt.Cell{
			Key:   mem.GetTopKey().Clone(),
			Value: append([]byte(nil), mem.GetTopValue()...),
		})
		if err := mem.Next(); err != nil {
			return nil, nil, err
		}
	}
	memSnapshot := iterrt.NewSliceSource(cells)
	if err := memSnapshot.Init(nil, nil, env); err != nil {
		return nil, nil, err
	}
	return t.sourceWithMemLockedContext(ctx, env, memSnapshot)
}

func (t *Tablet) sourceLocked(env iterrt.IteratorEnvironment) (iterrt.SortedKeyValueIterator, func(), error) {
	return t.sourceLockedContext(context.Background(), env)
}

func (t *Tablet) sourceLockedContext(
	ctx context.Context,
	env iterrt.IteratorEnvironment,
) (iterrt.SortedKeyValueIterator, func(), error) {
	memIter := t.active.Iterator()
	return t.sourceWithMemLockedContext(ctx, env, memIter)
}

func (t *Tablet) sourceWithMemLockedContext(
	ctx context.Context,
	env iterrt.IteratorEnvironment,
	memIter iterrt.SortedKeyValueIterator,
) (iterrt.SortedKeyValueIterator, func(), error) {
	filesCopy := append([]string(nil), t.files...)

	// Build leaf iterators: one from memtable + one per RFile
	leaves := []iterrt.SortedKeyValueIterator{memIter}
	var closers []func()

	for _, path := range filesCopy {
		if err := ctx.Err(); err != nil {
			for _, c := range closers {
				c()
			}
			return nil, nil, err
		}
		src, closer, err := t.openFileSourceContext(ctx, path, env)
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

// Flush forces the memtable to a new immutable RFile or Parquet file and truncates the WAL.
func (t *Tablet) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.flushLocked()
}

// Compact merges all immutable files through the given iterator stack
// into one file in the table's configured format. This is where application-specific iterators
// (decay, pruning, dedup) run.
func (t *Tablet) Compact(stack []iterrt.IterSpec) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.files) == 0 || (len(t.files) == 1 && fileFormat(t.files[0]) == t.opts.FileFormat) {
		return nil // nothing to compact
	}

	type inputGroup struct {
		state  embeddingspace.FileState
		inputs []compaction.Input
	}
	groupsByState := make(map[string]*inputGroup)
	for _, path := range t.files {
		data, err := t.fileBytes(path)
		if err != nil {
			return fmt.Errorf("tablet: read %s for compaction: %w", path, err)
		}
		state, err := embeddingStateFromImage(path, data)
		if err != nil {
			return fmt.Errorf("tablet: read embedding state %s: %w", path, err)
		}
		key := state.String()
		group := groupsByState[key]
		if group == nil {
			group = &inputGroup{state: state}
			groupsByState[key] = group
		}
		group.inputs = append(group.inputs, compaction.Input{
			Name: path, Bytes: data, MetadataEmbedding: state,
		})
	}
	groupKeys := make([]string, 0, len(groupsByState))
	for key := range groupsByState {
		groupKeys = append(groupKeys, key)
	}
	sort.Strings(groupKeys)
	if len(groupKeys) > 1 && len(stack) > 0 {
		return ErrMixedEmbeddingCompactionStack
	}

	type compactedOutput struct {
		name    string
		path    string
		entries int64
	}
	outputs := make([]compactedOutput, 0, len(groupKeys))
	cleanupOutputs := func() {
		for _, output := range outputs {
			_ = removeObject(t.backend, output.path)
		}
	}
	existingNames := cloneObsolete(t.obsolete)
	for _, path := range t.files {
		existingNames[filepath.Base(path)] = struct{}{}
	}
	base := uniqueCompactionBase(
		time.Now().UnixMilli(),
		len(groupKeys),
		t.opts.FileFormat.extension(),
		existingNames,
	)
	for index, key := range groupKeys {
		group := groupsByState[key]
		result, err := compaction.Compact(compaction.Spec{
			Inputs:              group.inputs,
			Stack:               stack,
			Scope:               iterrt.ScopeMajc,
			FullMajorCompaction: len(groupKeys) == 1,
			AdjacencyEdgeCF:     t.opts.AdjacencyEdgeCF,
			OutputFormat:        string(t.opts.FileFormat),
		})
		if err != nil {
			cleanupOutputs()
			return fmt.Errorf("tablet: compact %s: %w", group.state, err)
		}
		outName := fmt.Sprintf(
			"C%013d-%03d%s", base, index, t.opts.FileFormat.extension())
		outPath := filepath.Join(t.dir, outName)
		if err := storage.WriteAll(
			context.Background(), t.backend, outPath, result.Output); err != nil {
			cleanupOutputs()
			return fmt.Errorf("tablet: write compacted: %w", err)
		}
		outputs = append(outputs, compactedOutput{
			name: outName, path: outPath, entries: result.EntriesWritten,
		})
	}

	oldFiles := t.files
	obsolete := cloneObsolete(t.obsolete)
	for _, old := range oldFiles {
		obsolete[filepath.Base(old)] = struct{}{}
	}
	outputPaths := make([]string, len(outputs))
	for index, output := range outputs {
		outputPaths[index] = output.path
	}
	if err := persistImmutableManifest(
		t.backend, t.dir, outputPaths, obsolete); err != nil {
		if storage.IsCommittedWriteError(err) {
			t.files = outputPaths
			t.obsolete = obsolete
			return fmt.Errorf("tablet: publish compacted generation committed with cleanup error: %w", err)
		}
		cleanupOutputs()
		return fmt.Errorf("tablet: publish compacted generation: %w", err)
	}
	t.files = outputPaths
	t.obsolete = obsolete
	for _, old := range oldFiles {
		if t.opts.Cache != nil {
			t.opts.Cache.Drop(old)
		}
		if err := removeObject(t.backend, old); err == nil {
			delete(t.obsolete, filepath.Base(old))
		} else {
			t.logger.Warn("compaction cleanup deferred",
				slog.String("file", old),
				slog.String("err", err.Error()))
		}
	}
	if err := persistImmutableManifest(t.backend, t.dir, t.files, t.obsolete); err != nil {
		t.logger.Warn("compaction cleanup manifest update failed", slog.String("err", err.Error()))
	}

	t.logger.Info("compaction complete",
		slog.Int("inputs", len(oldFiles)),
		slog.Int("outputs", len(outputs)))
	if t.opts.OnRFile != nil {
		for _, output := range outputs {
			t.opts.OnRFile("compact", output.name)
		}
	}

	return nil
}

func uniqueCompactionBase(
	base int64,
	outputs int,
	extension string,
	existing map[string]struct{},
) int64 {
	for {
		collision := false
		for index := 0; index < outputs; index++ {
			name := fmt.Sprintf("C%013d-%03d%s", base, index, extension)
			if _, found := existing[name]; found {
				collision = true
				break
			}
		}
		if !collision {
			return base
		}
		base++
	}
}

func embeddingStateFromImage(
	path string,
	data []byte,
) (embeddingspace.FileState, error) {
	if fileFormat(path) == FormatParquet {
		return parquetfile.ReadEmbeddingSpaceMetadata(
			bytes.NewReader(data), int64(len(data)))
	}
	reader, err := bcfile.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return embeddingspace.FileState{}, err
	}
	return rfile.ReadEmbeddingSpaceMetadata(reader, block.Default())
}

// FileCount returns the number of immutable files.
func (t *Tablet) FileCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.files)
}

// RFiles returns a snapshot of the tablet's immutable file paths/keys.
// The name is retained for API compatibility; entries may be RFile or Parquet.
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
	t.mu.Lock()
	defer t.mu.Unlock()
	keys, obsolete, err := discoverImmutableFiles(t.backend, t.dir, true)
	if err != nil {
		return 0, fmt.Errorf("tablet: refresh list %s: %w", t.dir, err)
	}
	sort.Strings(keys)
	if err := persistImmutableManifest(t.backend, t.dir, keys, obsolete); err != nil {
		return 0, fmt.Errorf("tablet: refresh manifest %s: %w", t.dir, err)
	}
	t.files = keys
	t.obsolete = obsolete
	return len(t.files), nil
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

	data, count, err := t.encode(iter)
	if err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	outName := fmt.Sprintf("F%013d%s", time.Now().UnixMilli(), t.opts.FileFormat.extension())
	outPath := filepath.Join(t.dir, outName)
	if err := storage.WriteAll(context.Background(), t.backend, outPath, data); err != nil {
		return fmt.Errorf("flush: write %s: %w", outPath, err)
	}

	files := append(append([]string(nil), t.files...), outPath)
	if err := persistImmutableManifest(t.backend, t.dir, files, t.obsolete); err != nil {
		if storage.IsCommittedWriteError(err) {
			t.files = files
			t.active = newSkiplistMemtable()
			if truncateErr := t.wal.Truncate(); truncateErr != nil {
				return errors.Join(
					fmt.Errorf("flush: generation committed with cleanup error: %w", err),
					fmt.Errorf("flush: truncate wal after committed generation: %w", truncateErr),
				)
			}
			return fmt.Errorf("flush: generation committed with cleanup error: %w", err)
		}
		_ = removeObject(t.backend, outPath)
		return fmt.Errorf("flush: publish generation: %w", err)
	}
	t.files = files
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

func (t *Tablet) encode(iter iterrt.SortedKeyValueIterator) ([]byte, int64, error) {
	if t.opts.FileFormat == FormatParquet {
		return parquetfile.EncodeWithOptions(
			iter,
			parquetfile.EncodeOptions{EmbeddingSpace: t.opts.DefaultEmbedding},
		)
	}
	var buf bytes.Buffer
	w, err := rfile.NewWriter(&buf, rfile.WriterOptions{
		Codec:           block.CodecSnappy,
		AdjacencyEdgeCF: t.opts.AdjacencyEdgeCF,
		EmbeddingSpace:  t.opts.DefaultEmbedding,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("new rfile writer: %w", err)
	}
	var count int64
	for iter.HasTop() {
		if err := w.Append(iter.GetTopKey(), iter.GetTopValue()); err != nil {
			return nil, count, fmt.Errorf("append cell %d: %w", count, err)
		}
		count++
		if err := iter.Next(); err != nil {
			return nil, count, fmt.Errorf("next after cell %d: %w", count-1, err)
		}
	}
	if err := w.Close(); err != nil {
		return nil, count, fmt.Errorf("close rfile writer: %w", err)
	}
	return buf.Bytes(), count, nil
}

func (f FileFormat) extension() string {
	if f == FormatParquet {
		return ".parquet"
	}
	return ".rf"
}

func fileFormat(path string) FileFormat {
	if filepath.Ext(path) == ".parquet" {
		return FormatParquet
	}
	return FormatRFile
}

// SetFileFormat changes the format used by subsequent flushes and compactions.
// Existing immutable files remain readable, enabling online mixed-format migration.
func (t *Tablet) SetFileFormat(format FileFormat) error {
	parsed, err := ParseFileFormat(string(format))
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.opts.FileFormat = parsed
	t.mu.Unlock()
	return nil
}

// SetDefaultEmbedding changes the actual embedding state stamped on future
// flushes. Existing immutable files keep their own self-describing state.
func (t *Tablet) SetDefaultEmbedding(state embeddingspace.FileState) error {
	if state != (embeddingspace.FileState{}) {
		if err := state.Validate(); err != nil {
			return err
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if cells := t.active.Len(); cells > 0 {
		return fmt.Errorf(
			"%w: tablet has %d unflushed cells",
			ErrEmbeddingStateChangeWithUnflushedData, cells)
	}
	t.opts.DefaultEmbedding = state
	return nil
}

// EmbeddingFileState is one immutable file's footer-level embedding state.
type EmbeddingFileState struct {
	Path  string
	State embeddingspace.FileState
}

// EmbeddingStateSnapshot reads every immutable file's footer while holding a
// consistent tablet file-set snapshot. UnflushedCells are reported separately
// because a memtable has no immutable file metadata and must fail exact vector
// queries closed.
func (t *Tablet) EmbeddingStateSnapshot(
	ctx context.Context,
) ([]EmbeddingFileState, int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]EmbeddingFileState, 0, len(t.files))
	for _, path := range t.files {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		file, err := t.backend.Open(ctx, path)
		if err != nil {
			return nil, 0, fmt.Errorf("tablet: open embedding metadata %s: %w", path, err)
		}
		var state embeddingspace.FileState
		if fileFormat(path) == FormatParquet {
			state, err = parquetfile.ReadEmbeddingSpaceMetadata(file, file.Size())
		} else {
			var bc *bcfile.Reader
			bc, err = bcfile.NewReader(file, file.Size())
			if err == nil {
				state, err = rfile.ReadEmbeddingSpaceMetadata(bc, block.Default())
			}
		}
		closeErr := file.Close()
		if err != nil {
			return nil, 0, fmt.Errorf("tablet: read embedding metadata %s: %w", path, err)
		}
		if closeErr != nil {
			return nil, 0, fmt.Errorf("tablet: close embedding metadata %s: %w", path, closeErr)
		}
		out = append(out, EmbeddingFileState{Path: path, State: state})
	}
	return out, t.active.Len(), nil
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

// openFileSource opens one immutable file as an SKVI leaf, returning the iterator
// and a closer function. Bytes come from the tablet's backend (served from
// the shared cache when warm); the reader shares a decompressed-block
// cache keyed by path when caching is enabled. RFiles are immutable by
// path, so the shared bytes slice is safe to wrap in concurrent read-only
// readers.
func (t *Tablet) openFileSource(path string, env iterrt.IteratorEnvironment) (iterrt.SortedKeyValueIterator, func(), error) {
	return t.openFileSourceContext(context.Background(), path, env)
}

func (t *Tablet) openFileSourceContext(
	ctx context.Context,
	path string,
	env iterrt.IteratorEnvironment,
) (iterrt.SortedKeyValueIterator, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if fileFormat(path) == FormatParquet {
		open := func() (storage.File, error) {
			return t.backend.Open(ctx, path)
		}
		file, err := open()
		if err != nil {
			return nil, nil, err
		}
		src, err := parquetfile.NewSource(file, open)
		if err != nil {
			return nil, nil, err
		}
		if err := src.Init(nil, nil, env); err != nil {
			_ = src.Close()
			return nil, nil, err
		}
		return src, func() { _ = src.Close() }, nil
	}
	var data []byte
	var err error
	if t.opts.Cache != nil {
		data, err = t.opts.Cache.fileBytesContext(ctx, path)
	} else {
		data, err = storage.ReadAll(ctx, t.backend, path)
	}
	if err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
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

const immutableManifestVersion = 1

type immutableManifest struct {
	Version  int      `json:"version"`
	Active   []string `json:"active"`
	Obsolete []string `json:"obsolete,omitempty"`
}

func discoverImmutableFiles(b storage.Backend, dir string, adoptUnknown bool) ([]string, map[string]struct{}, error) {
	keys, err := listTabletObjects(b, dir)
	if err != nil {
		return nil, nil, err
	}
	available := make(map[string]string)
	manifestPresent := false
	for _, key := range keys {
		switch filepath.Base(key) {
		case "files.json":
			manifestPresent = true
		default:
			if ext := filepath.Ext(key); ext == ".rf" || ext == ".parquet" {
				available[filepath.Base(key)] = key
			}
		}
	}
	obsolete := make(map[string]struct{})
	if !manifestPresent {
		active := make([]string, 0, len(available))
		for _, key := range available {
			active = append(active, key)
		}
		return active, obsolete, nil
	}
	data, err := storage.ReadAll(context.Background(), b, filepath.Join(dir, "files.json"))
	if err != nil {
		return nil, nil, fmt.Errorf("read immutable manifest: %w", err)
	}
	var manifest immutableManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, nil, fmt.Errorf("decode immutable manifest: %w", err)
	}
	if manifest.Version != immutableManifestVersion {
		return nil, nil, fmt.Errorf("unsupported immutable manifest version %d", manifest.Version)
	}
	for _, name := range manifest.Obsolete {
		obsolete[name] = struct{}{}
	}
	active := make([]string, 0, len(manifest.Active))
	known := make(map[string]struct{}, len(manifest.Active))
	for _, name := range manifest.Active {
		path, ok := available[name]
		if !ok {
			return nil, nil, fmt.Errorf("authoritative immutable file %q is missing", name)
		}
		active = append(active, path)
		known[name] = struct{}{}
	}
	if adoptUnknown {
		for name, path := range available {
			if _, ok := known[name]; ok {
				continue
			}
			if _, retired := obsolete[name]; !retired {
				active = append(active, path)
			}
		}
	}
	return active, obsolete, nil
}

func listTabletObjects(b storage.Backend, dir string) ([]string, error) {
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
	return keys, nil
}

func persistImmutableManifest(b storage.Backend, dir string, active []string, obsolete map[string]struct{}) error {
	manifest := immutableManifest{Version: immutableManifestVersion}
	for _, path := range active {
		manifest.Active = append(manifest.Active, filepath.Base(path))
	}
	for name := range obsolete {
		manifest.Obsolete = append(manifest.Obsolete, name)
	}
	sort.Strings(manifest.Active)
	sort.Strings(manifest.Obsolete)
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode immutable manifest: %w", err)
	}
	return storage.WriteAll(context.Background(), b, filepath.Join(dir, "files.json"), append(data, '\n'))
}

func cloneObsolete(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for name := range in {
		out[name] = struct{}{}
	}
	return out
}

// PublishImmutableFiles records the authoritative immutable generation for a
// tablet directory. Importers call it after checksum verification so stale
// objects already present under the destination prefix are not rediscovered.
func PublishImmutableFiles(b storage.Backend, dir string, active []string) error {
	keys, err := listTabletObjects(b, dir)
	if err != nil {
		return err
	}
	activeNames := make(map[string]struct{}, len(active))
	for _, path := range active {
		activeNames[filepath.Base(path)] = struct{}{}
	}
	obsolete := make(map[string]struct{})
	for _, key := range keys {
		if ext := filepath.Ext(key); ext == ".rf" || ext == ".parquet" {
			name := filepath.Base(key)
			if _, ok := activeNames[name]; !ok {
				obsolete[name] = struct{}{}
			}
		}
	}
	return persistImmutableManifest(b, dir, active, obsolete)
}

// RegisterImmutableFiles adds only checksum-verified import objects to the
// authoritative generation. The returned paths became authoritative even when
// the returned error reports cleanup after a committed manifest write.
func (t *Tablet) RegisterImmutableFiles(paths []string) ([]string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	active := append([]string(nil), t.files...)
	adopted := make([]string, 0, len(paths))
	seen := make(map[string]string, len(active)+len(paths))
	for _, path := range active {
		seen[immutableFileIdentity(path)] = path
	}
	for _, path := range paths {
		identity := immutableFileIdentity(path)
		if existing, ok := seen[identity]; ok {
			if existing != path {
				if err := removeObject(t.backend, path); err != nil {
					return nil, fmt.Errorf("tablet: remove redundant immutable file %s: %w", path, err)
				}
			}
			continue
		}
		active = append(active, path)
		adopted = append(adopted, path)
		seen[identity] = path
		delete(t.obsolete, filepath.Base(path))
	}
	sort.Strings(active)
	if err := persistImmutableManifest(t.backend, t.dir, active, t.obsolete); err != nil {
		if storage.IsCommittedWriteError(err) {
			t.files = active
			return adopted, err
		}
		return nil, err
	}
	t.files = active
	return adopted, nil
}

func immutableFileIdentity(path string) string {
	const importPrefix = ".shoal-import-"
	name := filepath.Base(path)
	if !strings.HasPrefix(name, importPrefix) {
		return path
	}
	encoded := strings.TrimPrefix(name, importPrefix)
	if len(encoded) < sha256.Size*2 {
		return path
	}
	digest := encoded[:sha256.Size*2]
	if _, err := hex.DecodeString(digest); err != nil {
		return path
	}
	if len(encoded) == len(digest) || encoded[len(digest)] != '-' && encoded[len(digest)] != '.' {
		return path
	}
	return importPrefix + strings.ToLower(digest) + filepath.Ext(name)
}

func removeObject(b storage.Backend, path string) error {
	r, ok := b.(storage.Remover)
	if !ok {
		return storage.ErrRemoverUnsupported
	}
	return r.Remove(context.Background(), path)
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
