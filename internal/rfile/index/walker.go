package index

import (
	"errors"
	"fmt"
	"sort"

	"github.com/phrocker/shoal/internal/rfile/wire"
	"github.com/phrocker/shoal/internal/rfile/bcfile"
)

// LevelReader fetches a child IndexBlock by its BCFile region. The
// non-leaf entries inside an IndexBlock describe child blocks via
// (Offset, CompressedSize, RawSize); the walker hands those tuples to
// the LevelReader to materialize the next level down.
//
// Implementations are responsible for fetching the bytes from the
// BCFile, decompressing them with the right codec, and parsing as an
// IndexBlock. The walker is codec-agnostic — it doesn't know whether
// the bytes were gzipped, snappied, or stored raw.
type LevelReader interface {
	ReadIndexBlock(region bcfile.BlockRegion) (*IndexBlock, error)
}

// LevelReaderFunc is a closure-style adapter so callers can pass a
// function literal where a LevelReader is expected.
type LevelReaderFunc func(region bcfile.BlockRegion) (*IndexBlock, error)

// ReadIndexBlock satisfies LevelReader.
func (f LevelReaderFunc) ReadIndexBlock(region bcfile.BlockRegion) (*IndexBlock, error) {
	return f(region)
}

// ErrPastEnd is returned by Seek when the target key is beyond the last
// entry in the index — i.e. there's no data block that could contain it.
// Distinct from "key not found": for an RFile, Seek returns the entry
// pointing at the block where key WOULD be if present, even if the actual
// row is missing. Past-end means there's no such block at all.
var ErrPastEnd = errors.New("rfile/index: target key is past the last index entry")

// Walker walks a multi-level index tree from a preserved root IndexBlock,
// fetching child blocks on demand via the LevelReader. Stateless across
// Seek calls — each Seek descends from root.
type Walker struct {
	root *IndexBlock
	lr   LevelReader
}

// NewWalker constructs a Walker over root. lr may be nil iff root is
// the only level (Level == 0); for multi-level trees the walker will
// call lr to fetch each child block.
func NewWalker(root *IndexBlock, lr LevelReader) *Walker {
	return &Walker{root: root, lr: lr}
}

// Seek descends the tree to find the leaf IndexEntry whose data block
// covers target. Returns the entry on success; returns ErrPastEnd if
// target sorts after every key in the index.
//
// Java equivalent: Reader.Node.lookup (MultiLevelIndex.java:616-641).
// Mirrors the exact comparator behaviour: an IndexEntry's key is the
// LAST key in its referenced block, so we want the smallest entry whose
// key >= target.
func (w *Walker) Seek(target *wire.Key) (*IndexEntry, error) {
	cur := w.root
	for {
		entries := EntriesOf(cur)
		idx, err := binarySearchKey(entries, target)
		if err != nil {
			return nil, err
		}
		if idx == entries.Len() {
			// Past end at this level. At root this means target > everything.
			// At a non-root level it would violate the invariant that the
			// parent's chosen entry's key bounds this child — Java throws
			// IllegalStateException here, and we mirror that.
			if cur == w.root {
				return nil, ErrPastEnd
			}
			return nil, fmt.Errorf("rfile/index: walker invariant violation: target past end at non-root level %d", cur.Level)
		}
		entry, err := entries.At(idx)
		if err != nil {
			return nil, err
		}
		if cur.Level == 0 {
			return entry, nil
		}
		// Non-leaf: descend.
		if w.lr == nil {
			return nil, fmt.Errorf("rfile/index: multi-level index requires LevelReader (level=%d)", cur.Level)
		}
		child, err := w.lr.ReadIndexBlock(bcfile.BlockRegion{
			Offset:         entry.Offset,
			CompressedSize: entry.CompressedSize,
			RawSize:        entry.RawSize,
		})
		if err != nil {
			return nil, fmt.Errorf("rfile/index: descend to level %d: %w", cur.Level-1, err)
		}
		cur = child
	}
}

// binarySearchKey finds the smallest index i in entries where
// entries[i].Key >= target. Returns Len() if no such entry exists.
//
// Uses KeyAt for the comparator — that decodes only the Key portion
// of each candidate entry, skipping the trailing sizes.
func binarySearchKey(entries *Entries, target *wire.Key) (int, error) {
	n := entries.Len()
	var searchErr error
	idx := sort.Search(n, func(i int) bool {
		if searchErr != nil {
			return true // short-circuit further iterations
		}
		k, err := entries.KeyAt(i)
		if err != nil {
			searchErr = err
			return true
		}
		return k.Compare(target) >= 0
	})
	if searchErr != nil {
		return 0, searchErr
	}
	return idx, nil
}

// IterateLeaves walks every leaf-level IndexEntry in tree order (left
// to right), invoking fn for each. fn returning a non-nil error halts
// the walk and surfaces that error from IterateLeaves.
//
// For a single-level tree (Level == 0 root) this just iterates the
// root entries. For a multi-level tree it descends fully into each
// non-leaf entry, recursively iterating leaves.
//
// Note: this implementation is sequential and synchronous — Phase 3c
// will compose it with the prefetcher so DATA-block fetches overlap
// with consumer work, but the index walk itself is single-threaded.
// (The index is small relative to data, so prefetching index blocks
// is rarely the bottleneck.)
func (w *Walker) IterateLeaves(fn func(*IndexEntry) error) error {
	return iterateLeaves(w.root, w.lr, fn)
}

func iterateLeaves(block *IndexBlock, lr LevelReader, fn func(*IndexEntry) error) error {
	entries := EntriesOf(block)
	n := entries.Len()
	for i := 0; i < n; i++ {
		entry, err := entries.At(i)
		if err != nil {
			return err
		}
		if block.Level == 0 {
			if err := fn(entry); err != nil {
				return err
			}
			continue
		}
		if lr == nil {
			return fmt.Errorf("rfile/index: multi-level iterate requires LevelReader (level=%d)", block.Level)
		}
		child, err := lr.ReadIndexBlock(bcfile.BlockRegion{
			Offset:         entry.Offset,
			CompressedSize: entry.CompressedSize,
			RawSize:        entry.RawSize,
		})
		if err != nil {
			return fmt.Errorf("rfile/index: descend to level %d: %w", block.Level-1, err)
		}
		if err := iterateLeaves(child, lr, fn); err != nil {
			return err
		}
	}
	return nil
}

// Root returns the root IndexBlock the walker was constructed with.
// Convenience for tests + introspection.
func (w *Walker) Root() *IndexBlock { return w.root }
