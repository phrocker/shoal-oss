// Package local is a storage.Backend over the local filesystem. Useful
// for dev cycles where shoal runs against RFiles dumped from a cluster,
// and for tests that want to exercise the bcfile/rfile stack without
// any cloud deps.
package local

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/phrocker/shoal/internal/storage"
)

// Backend opens files from the OS filesystem. Stateless — share a
// single value across goroutines.
type Backend struct{}

// New returns a Backend ready to Open files by path.
func New() *Backend { return &Backend{} }

// Open opens path read-only and returns a storage.File backed by the
// underlying os.File. Returns an error wrapping storage.ErrNotFound
// when the path doesn't exist.
func (b *Backend) Open(_ context.Context, path string) (storage.File, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", storage.ErrNotFound, path)
		}
		return nil, fmt.Errorf("local: open %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("local: stat %s: %w", path, err)
	}
	return &file{f: f, size: info.Size()}, nil
}

// Create opens path for writing in O_CREATE|O_WRONLY|O_TRUNC mode and
// returns a Writer over the resulting *os.File. Parent directories are
// created with 0755 if they don't already exist — matches "mkdir -p"
// behavior so callers don't have to pre-create the path tree.
func (b *Backend) Create(_ context.Context, path string) (storage.Writer, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("local: mkdir %s: %w", dir, err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("local: create %s: %w", path, err)
	}
	return f, nil
}

// List enumerates the regular files directly under prefix (a directory
// path), returning their full paths. Mirrors the os.ReadDir-based RFile
// discovery the tablet did before storage was abstracted. A non-existent
// prefix yields an empty list, not an error (an empty tablet dir).
func (b *Backend) List(_ context.Context, prefix string) ([]string, error) {
	entries, err := os.ReadDir(prefix)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("local: readdir %s: %w", prefix, err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() {
			out = append(out, filepath.Join(prefix, e.Name()))
		}
	}
	return out, nil
}

// Remove deletes path. A missing path is not an error.
func (b *Backend) Remove(_ context.Context, path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("local: remove %s: %w", path, err)
	}
	return nil
}

// file is the local-filesystem File implementation. *os.File already
// satisfies io.ReaderAt + io.Closer; we just attach a cached Size.
type file struct {
	f    *os.File
	size int64
}

func (l *file) ReadAt(p []byte, off int64) (int, error) { return l.f.ReadAt(p, off) }
func (l *file) Close() error                            { return l.f.Close() }
func (l *file) Size() int64                             { return l.size }
