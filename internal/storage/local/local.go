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

	"github.com/google/uuid"
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

// Create opens a temporary sibling file for writing and commits it into path
// on Close. Existing regular-file permissions are preserved; new files use
// 0644 subject to the process umask. Parent directories are created with 0755
// if they don't already exist — matches "mkdir -p" behavior so callers don't
// have to pre-create the path tree.
func (b *Backend) Create(_ context.Context, path string) (storage.Writer, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("local: mkdir %s: %w", dir, err)
		}
	}
	ops := osReplacementOps{}
	mode, preserveMode, err := replacementTargetMode(ops, path)
	if err != nil {
		return nil, err
	}
	temp := filepath.Join(filepath.Dir(path), filepath.Base(path)+".shoal-tmp-"+uuid.NewString())
	f, err := os.OpenFile(temp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("local: create temporary file for %s: %w", path, err)
	}
	if preserveMode {
		if err := ops.Chmod(temp, mode); err != nil {
			_ = f.Close()
			_ = ops.Remove(temp)
			return nil, fmt.Errorf("local: preserve permissions for %s: %w", path, err)
		}
	}
	return &writer{
		file:   f,
		temp:   f.Name(),
		target: path,
		ops:    ops,
	}, nil
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

type writer struct {
	file       *os.File
	temp       string
	target     string
	ops        replacementOps
	closed     bool
	aborted    bool
	fileClosed bool
	state      replacementState
}

type replacementState uint8

const (
	replacementStaged replacementState = iota
	replacementPublished
	replacementCommitted
	replacementUnabortable
)

type replacementOps interface {
	Lstat(string) (os.FileInfo, error)
	Chmod(string, os.FileMode) error
	Rename(string, string) error
	Remove(string) error
}

type osReplacementOps struct{}

func (osReplacementOps) Lstat(name string) (os.FileInfo, error) { return os.Lstat(name) }
func (osReplacementOps) Chmod(name string, mode os.FileMode) error {
	return os.Chmod(name, mode)
}
func (osReplacementOps) Rename(oldPath, newPath string) error { return os.Rename(oldPath, newPath) }
func (osReplacementOps) Remove(name string) error             { return os.Remove(name) }

func replacementTargetMode(ops replacementOps, target string) (os.FileMode, bool, error) {
	info, err := ops.Lstat(target)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("local: inspect existing file %s: %w", target, err)
	}
	if !info.Mode().IsRegular() {
		return 0, false, fmt.Errorf("local: replace %s: target is not a regular file", target)
	}
	return info.Mode().Perm(), true, nil
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed || w.aborted {
		return 0, fmt.Errorf("local: write after close")
	}
	return w.file.Write(p)
}

func (w *writer) Close() error {
	if w.aborted {
		return fmt.Errorf("local: writer already aborted")
	}
	if w.closed {
		return nil
	}
	if err := w.file.Close(); err != nil {
		w.fileClosed = true
		_ = os.Remove(w.temp)
		return fmt.Errorf("local: close temporary file %s: %w", w.temp, err)
	}
	w.fileClosed = true

	if err := w.commitReplacement(); err != nil {
		return err
	}
	w.closed = true
	return nil
}

func (w *writer) commitReplacement() error {
	mode, preserveMode, err := replacementTargetMode(w.ops, w.target)
	if err != nil {
		return err
	}
	if preserveMode {
		if err := w.ops.Chmod(w.temp, mode); err != nil {
			return fmt.Errorf("local: preserve permissions for %s: %w", w.target, err)
		}
	}

	backup := w.target + ".shoal-backup-" + uuid.NewString()
	hadOld := true
	if err := w.ops.Rename(w.target, backup); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("local: preserve existing file %s: %w", w.target, err)
		}
		hadOld = false
	}

	if err := w.ops.Rename(w.temp, w.target); err != nil {
		publishErr := fmt.Errorf("local: publish %s: %w", w.target, err)
		if hadOld {
			if restoreErr := w.ops.Rename(backup, w.target); restoreErr != nil {
				w.state = replacementUnabortable
				publishErr = errors.Join(
					publishErr,
					fmt.Errorf("local: restore existing file %s from %s: %w", w.target, backup, restoreErr),
				)
			}
		}
		return publishErr
	}
	w.state = replacementPublished
	if hadOld {
		if err := w.ops.Remove(backup); err != nil && !errors.Is(err, fs.ErrNotExist) {
			cleanupErr := fmt.Errorf("local: remove replacement backup %s: %w", backup, err)
			if rollbackErr := w.rollbackPublishedReplacement(backup); rollbackErr != nil {
				return errors.Join(cleanupErr, rollbackErr)
			}
			return cleanupErr
		}
	}
	w.state = replacementCommitted
	return nil
}

func (w *writer) rollbackPublishedReplacement(backup string) error {
	if err := w.ops.Rename(w.target, w.temp); err != nil {
		return fmt.Errorf("local: roll back published file %s: %w", w.target, err)
	}
	if err := w.ops.Rename(backup, w.target); err != nil {
		restoreErr := fmt.Errorf("local: restore existing file %s from %s: %w", w.target, backup, err)
		if republishErr := w.ops.Rename(w.temp, w.target); republishErr != nil {
			w.state = replacementUnabortable
			return errors.Join(
				restoreErr,
				fmt.Errorf("local: restore published file %s after rollback failure: %w", w.target, republishErr),
			)
		}
		w.state = replacementPublished
		return restoreErr
	}
	w.state = replacementStaged
	return nil
}

func (w *writer) Abort() error {
	if w.aborted {
		return nil
	}
	if w.closed {
		return fmt.Errorf("local: writer already closed")
	}
	if w.state != replacementStaged {
		return fmt.Errorf("local: replacement for %s cannot be safely aborted in state %d", w.target, w.state)
	}
	w.aborted = true

	var abortErr error
	if !w.fileClosed {
		err := w.file.Close()
		w.fileClosed = true
		if err != nil {
			abortErr = fmt.Errorf("local: close temporary file %s: %w", w.temp, err)
		}
	}
	if err := w.ops.Remove(w.temp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		abortErr = errors.Join(abortErr, fmt.Errorf("local: remove temporary file %s: %w", w.temp, err))
	}
	return abortErr
}
