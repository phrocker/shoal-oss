package rfile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/phrocker/shoal/internal/rfile"
	"github.com/phrocker/shoal/internal/rfile/bcfile/block"
	"github.com/phrocker/shoal/internal/rfile/wire"
)

// Writer appends entries to a new RFile in key order and finalizes it on
// Close. It is the Go equivalent of Sharkbite's SequentialRFile write side,
// which RFileOperations.openForWrite returns.
type Writer struct {
	mu       sync.Mutex
	inner    *rfile.Writer
	file     *os.File
	path     string
	lastKey  *wire.Key
	closed   bool
	closeErr error
	entries  int64
}

// WriterOptions controls how Create lays out the file. The zero value uses
// Shoal's defaults, which is what Sharkbite's openForWrite(path) produces.
type WriterOptions struct {
	// Codec is the block compression algorithm: "none" (default) or "gz".
	// An unregistered name is ErrUnsupportedCodec and is rejected before the
	// file is created.
	Codec string

	// BlockSize is the uncompressed byte threshold at which a data block is
	// flushed. Zero uses Shoal's default.
	BlockSize int
}

// Create opens path for writing and returns a writer positioned before the
// first entry, mirroring Sharkbite's RFileOperations.openForWrite. The file is
// created, or truncated if it already exists, and it is only a valid RFile
// once Close returns without error.
//
// Options are validated before the file is touched, so a rejected codec cannot
// destroy an existing RFile at path.
func Create(ctx context.Context, path string, opts WriterOptions) (*Writer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, fmt.Errorf("%w: path is required", ErrInvalidPath)
	}
	if opts.Codec != "" && !block.DefaultCompressor().Has(opts.Codec) {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedCodec, opts.Codec)
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("rfile: create %s: %w", path, err)
	}
	inner, err := rfile.NewWriter(file, rfile.WriterOptions{
		Codec:     opts.Codec,
		BlockSize: opts.BlockSize,
	})
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("rfile: create %s: %w", path, err)
	}
	return &Writer{inner: inner, file: file, path: path}, nil
}

// Append writes one entry, mirroring Sharkbite's SequentialRFile.append.
// Entries must be appended in strictly increasing key order; anything else is
// ErrOutOfOrder, where Sharkbite's append returns false or corrupts the file.
//
// The entry's key and value are copied, so the caller may reuse its buffers.
func (w *Writer) Append(ctx context.Context, entry Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	key := internalKey(entry.Key)
	key.Deleted = entry.Deleted
	if w.lastKey != nil && compareKeys(key, w.lastKey) <= 0 {
		return fmt.Errorf(
			"%w: %q after %q",
			ErrOutOfOrder, entry.Key.Row, w.lastKey.Row,
		)
	}
	if err := w.inner.Append(key, append([]byte(nil), entry.Value...)); err != nil {
		return fmt.Errorf("rfile: append to %s: %w", w.path, err)
	}
	w.lastKey = key
	w.entries++
	return nil
}

// AddLocalityGroup reports ErrLocalityGroupUnsupported. Sharkbite exposes
// SequentialRFile.addLocalityGroup(name), which starts a new named group in
// the file being written, but Shoal's writer emits only the default locality
// group; a silent no-op would let a caller believe its families were separated
// when they were not, and an RFile whose groups were mis-declared is unreadable
// by Accumulo.
func (w *Writer) AddLocalityGroup(name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	return fmt.Errorf("%w: cannot start group %q", ErrLocalityGroupUnsupported, name)
}

// Entries reports how many entries have been appended so far.
func (w *Writer) Entries() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.entries
}

// Close finalizes the file: it writes the index and trailer, then releases the
// handle. It is idempotent, and Append afterwards reports ErrClosed. Until
// Close returns without error the file on disk is not a readable RFile.
//
// A failed finalization is remembered: every later Close returns the same
// error, so a caller cannot mistake a malformed file for a complete one.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return w.closeErr
	}
	w.closed = true
	var errs []error
	if err := w.inner.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := w.file.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		w.closeErr = fmt.Errorf("rfile: close %s: %w", w.path, errors.Join(errs...))
	}
	return w.closeErr
}

// compareKeys orders two keys exactly as Accumulo does, including the deleted
// bit: at one coordinate and timestamp a tombstone sorts before the live entry,
// so appending the pair in that order is valid.
func compareKeys(a, b *wire.Key) int { return a.Compare(b) }
