// Package storage abstracts the underlying file store that shoal reads
// RFiles from. Each backend (local FS, in-memory test fixture, GCS) lives
// in its own sub-package — callers who only need local don't pull in the
// GCS client deps.
//
// Why no Alluxio? The cluster historically routed RFile reads through
// Alluxio as a cache layer over GCS, but that adds an operational
// dependency (Alluxio master/workers) that shoal's read-fleet design
// is supposed to remove. Direct-to-GCS is the V0 target. If we want
// caching later we'll do it in shoal's own block cache, where we can
// pair it with the prefetcher.
//
// Backend usage:
//
//	be := gcs.New(ctx, ...)         // or local.New(), memory.New()
//	f, err := be.Open(ctx, path)
//	defer f.Close()
//	bc, err := bcfile.NewReader(f, f.Size())
//	r, err := rfile.Open(bc, block.Default())
//
// File satisfies io.ReaderAt + io.Closer + Size, which is exactly what
// bcfile.NewReader needs.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// File is an open backend object. Random-access reads via ReadAt; total
// length via Size; resource release via Close.
//
// ReadAt semantics follow io.ReaderAt: reads exactly len(p) bytes or
// returns a non-nil error. Short reads at end-of-file return io.EOF
// alongside the partial fill — the same contract as os.File.ReadAt.
type File interface {
	io.ReaderAt
	io.Closer

	// Size returns the total byte length of the underlying object.
	// Zero is a valid size (empty file).
	Size() int64
}

// Backend opens a File by path. Path semantics are backend-specific:
//
//   - local:  filesystem path (relative or absolute)
//   - memory: an arbitrary string key registered via Put
//   - gcs:    "gs://bucket/object/path" or just "bucket/object/path"
//
// Implementations are safe for concurrent Open calls. Returned Files
// are independently safe for concurrent ReadAt — no shared mutable state.
type Backend interface {
	Open(ctx context.Context, path string) (File, error)
}

// Writer is a write-only handle to a backend object. Streaming writes
// via io.Writer; close to flush. Not all backends support writing; use
// the WritableBackend type-assertion / interface to discover.
type Writer interface {
	io.Writer
	io.Closer
}

// WritableBackend is a Backend that also supports creating + replacing
// objects. Local + Memory implement this; GCS does not yet (would need
// a streaming writer; can be added when we need GCS-side egress).
type WritableBackend interface {
	Backend
	// Create opens path for writing. Replaces any existing object with
	// the same path. Returned Writer must be Closed to commit; until
	// then Open(path) may not see the new bytes.
	Create(ctx context.Context, path string) (Writer, error)
}

// Lister is a Backend that can enumerate object paths under a prefix.
// Used for manifest discovery (which RFiles a tablet owns) on startup.
// Local + Memory implement it. Cloud backends satisfy it via a list API.
//
// List returns the full paths (keys) of objects directly under prefix.
// Ordering is unspecified; callers sort if they need determinism.
type Lister interface {
	Backend
	List(ctx context.Context, prefix string) ([]string, error)
}

// Remover is a Backend that can delete an object by path. Used to drop a
// compaction's input RFiles once the merged output is durable. Local +
// Memory implement it. Deleting a non-existent path is not an error.
type Remover interface {
	Backend
	Remove(ctx context.Context, path string) error
}

// ErrNotFound is the sentinel for "the requested path doesn't exist."
// Backends should wrap their own not-found errors with errors.Join or
// fmt.Errorf("%w: ...", ErrNotFound, ...) so callers can errors.Is-test
// without backend-specific imports.
var ErrNotFound = errors.New("storage: not found")

// ErrReadOnly is returned when Create is called on a Backend that does
// not implement WritableBackend.
var ErrReadOnly = errors.New("storage: backend is read-only")

// Copy copies an object from src to dst across (potentially different)
// backends. Useful for "pull a small RFile from GCS to local disk for
// debugging" workflows. dst must be a WritableBackend.
//
// Reads are issued in 64KB chunks via ReadAt; backends that bill per
// request (GCS) absorb at most ceil(size/64KB) round trips. Increase
// the chunk size for large files if perf matters.
func Copy(ctx context.Context, src Backend, srcPath string, dst Backend, dstPath string) (int64, error) {
	wb, ok := dst.(WritableBackend)
	if !ok {
		return 0, ErrReadOnly
	}
	in, err := src.Open(ctx, srcPath)
	if err != nil {
		return 0, fmt.Errorf("copy: open src %s: %w", srcPath, err)
	}
	defer in.Close()
	out, err := wb.Create(ctx, dstPath)
	if err != nil {
		return 0, fmt.Errorf("copy: create dst %s: %w", dstPath, err)
	}
	defer out.Close()

	const chunk = 64 * 1024
	buf := make([]byte, chunk)
	var off int64
	for off < in.Size() {
		want := int64(chunk)
		if off+want > in.Size() {
			want = in.Size() - off
		}
		n, err := in.ReadAt(buf[:want], off)
		if err != nil && !errors.Is(err, io.EOF) {
			return off, fmt.Errorf("copy: read off=%d: %w", off, err)
		}
		if n > 0 {
			if _, werr := out.Write(buf[:n]); werr != nil {
				return off, fmt.Errorf("copy: write off=%d: %w", off, werr)
			}
			off += int64(n)
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	return off, nil
}

// ReadAll opens path on b and reads the whole object into a single byte
// slice via ReadAt. This is the "pull-through" read used when an RFile is
// faulted into the local byte cache: one object fetch, fully resident.
// For large objects where only a few blocks are needed, prefer wiring the
// File's ReadAt directly into the reader instead of ReadAll.
func ReadAll(ctx context.Context, b Backend, path string) ([]byte, error) {
	f, err := b.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	size := f.Size()
	if size == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	var off int64
	for off < size {
		n, err := f.ReadAt(buf[off:], off)
		off += int64(n)
		if err != nil {
			if errors.Is(err, io.EOF) && off >= size {
				break
			}
			return nil, fmt.Errorf("readall: read %s off=%d: %w", path, off, err)
		}
	}
	return buf, nil
}

// WriteAll creates path on b (which must be a WritableBackend) and writes
// data in one shot, committing on Close. Used to publish an immutable
// RFile produced by a flush or compaction. Returns ErrReadOnly if b can't
// write.
func WriteAll(ctx context.Context, b Backend, path string, data []byte) error {
	wb, ok := b.(WritableBackend)
	if !ok {
		return ErrReadOnly
	}
	w, err := wb.Create(ctx, path)
	if err != nil {
		return fmt.Errorf("writeall: create %s: %w", path, err)
	}
	if _, err := w.Write(data); err != nil {
		w.Close()
		return fmt.Errorf("writeall: write %s: %w", path, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("writeall: close %s: %w", path, err)
	}
	return nil
}
