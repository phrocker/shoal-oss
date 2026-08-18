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

const transferChunkSize = 64 * 1024

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

// Aborter is an optional Writer capability that abandons an in-progress write
// without publishing it. Copy and WriteAll call Abort on unsuccessful paths
// when available; writers that do not implement Aborter retain their Close
// semantics during cleanup.
type Aborter interface {
	Abort() error
}

// WritableBackend is a Backend that also supports creating + replacing
// objects. Returned Writers commit on Close; if they also implement Aborter,
// callers can abandon an unsuccessful write without publishing partial data.
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
// the chunk size for large files if perf matters. ctx is polled before
// each backend read and write; if a backend call is already blocked,
// Copy waits for it to return before observing ctx.Err(). On an unsuccessful
// path, Copy aborts the destination writer when it supports Aborter.
func Copy(ctx context.Context, src Backend, srcPath string, dst Backend, dstPath string) (written int64, err error) {
	wb, ok := dst.(WritableBackend)
	if !ok {
		return 0, ErrReadOnly
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	in, err := src.Open(ctx, srcPath)
	if err != nil {
		return 0, fmt.Errorf("copy: open src %s: %w", srcPath, err)
	}
	defer in.Close()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	out, err := wb.Create(ctx, dstPath)
	if err != nil {
		return 0, fmt.Errorf("copy: create dst %s: %w", dstPath, err)
	}
	needsCleanup := true
	defer func() {
		if needsCleanup {
			err = cleanupUnsuccessfulWrite(err, out)
		}
	}()

	buf := make([]byte, transferChunkSize)
	for written < in.Size() {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		want := int64(transferChunkSize)
		if written+want > in.Size() {
			want = in.Size() - written
		}
		n, err := in.ReadAt(buf[:want], written)
		if err != nil && !errors.Is(err, io.EOF) {
			return written, fmt.Errorf("copy: read off=%d: %w", written, err)
		}
		if n > 0 {
			if err := ctx.Err(); err != nil {
				return written, err
			}
			writeOff := written
			wrote, werr := out.Write(buf[:n])
			written += int64(wrote)
			if werr != nil {
				return written, fmt.Errorf("copy: write off=%d: %w", writeOff, werr)
			}
			if wrote != n {
				return written, fmt.Errorf("copy: write off=%d: %w", writeOff, io.ErrShortWrite)
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
	}
	if err := ctx.Err(); err != nil {
		return written, err
	}
	needsCleanup = false
	if err := out.Close(); err != nil {
		return written, fmt.Errorf("copy: close dst %s: %w", dstPath, err)
	}
	return written, nil
}

// ReadAll opens path on b and reads the whole object into a single byte
// slice via ReadAt. This is the "pull-through" read used when an RFile is
// faulted into the local byte cache: one object fetch, fully resident.
// For large objects where only a few blocks are needed, prefer wiring the
// File's ReadAt directly into the reader instead of ReadAll. ctx is polled
// before each backend read; once a read is already blocked, ReadAll waits for
// it to return before observing ctx.Err().
func ReadAll(ctx context.Context, b Backend, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
// write. ctx is polled between chunk writes; a write already blocked in the
// backend may still finish its current chunk, and if that completes the object,
// WriteAll still waits for that backend call to return before observing
// ctx.Err(). On an unsuccessful path, WriteAll aborts the writer when it
// supports Aborter.
func WriteAll(ctx context.Context, b Backend, path string, data []byte) (err error) {
	wb, ok := b.(WritableBackend)
	if !ok {
		return ErrReadOnly
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w, err := wb.Create(ctx, path)
	if err != nil {
		return fmt.Errorf("writeall: create %s: %w", path, err)
	}
	needsCleanup := true
	defer func() {
		if needsCleanup {
			err = cleanupUnsuccessfulWrite(err, w)
		}
	}()
	for off := 0; off < len(data); {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(off+transferChunkSize, len(data))
		writeOff := off
		n, err := w.Write(data[off:end])
		off += n
		if err != nil {
			return fmt.Errorf("writeall: write %s off=%d: %w", path, writeOff, err)
		}
		if n != end-writeOff {
			return fmt.Errorf("writeall: write %s off=%d: %w", path, writeOff, io.ErrShortWrite)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	needsCleanup = false
	if err := w.Close(); err != nil {
		return fmt.Errorf("writeall: close %s: %w", path, err)
	}
	return nil
}

func cleanupUnsuccessfulWrite(primaryErr error, w Writer) error {
	if primaryErr == nil {
		return nil
	}
	var cleanupErr error
	if aborter, ok := w.(Aborter); ok {
		cleanupErr = aborter.Abort()
	} else {
		cleanupErr = w.Close()
	}
	if cleanupErr == nil {
		return primaryErr
	}
	return errors.Join(primaryErr, cleanupErr)
}
