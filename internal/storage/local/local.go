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
// on Close without first removing or moving an existing target. Existing
// regular-file mode bits are preserved everywhere; on Unix we also preserve
// owner/group and extended attributes (including xattr-backed ACLs such as
// Linux POSIX ACLs) when the platform exposes them. New files use 0644 subject
// to the process umask. Parent directories are created with 0755 if they don't
// already exist — matches "mkdir -p" behavior so callers don't have to
// pre-create the path tree.
func (b *Backend) Create(_ context.Context, path string) (storage.Writer, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("local: mkdir %s: %w", dir, err)
		}
	}
	ops := osReplacementOps{}
	temp := filepath.Join(filepath.Dir(path), filepath.Base(path)+".shoal-tmp-"+uuid.NewString())
	f, err := os.OpenFile(temp, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("local: create temporary file for %s: %w", path, err)
	}
	if _, err := preserveExistingMetadata(ops, temp, path); err != nil {
		_ = f.Close()
		_ = ops.Remove(temp)
		return nil, err
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
)

type replacementOps interface {
	Lstat(string) (os.FileInfo, error)
	Chmod(string, os.FileMode) error
	Remove(string) error
	AtomicReplace(temp, target, backup string, hadOld bool) error
	AtomicRestore(target, backup string) error
}

type osReplacementOps struct{}

func (osReplacementOps) Lstat(name string) (os.FileInfo, error) { return os.Lstat(name) }
func (osReplacementOps) Chmod(name string, mode os.FileMode) error {
	return os.Chmod(name, mode)
}
func (osReplacementOps) Remove(name string) error { return os.Remove(name) }
func (osReplacementOps) AtomicReplace(temp, target, backup string, hadOld bool) error {
	return platformAtomicReplace(temp, target, backup, hadOld)
}
func (osReplacementOps) AtomicRestore(target, backup string) error {
	return platformAtomicRestore(target, backup)
}

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

func preserveExistingMetadata(ops replacementOps, temp, target string) (bool, error) {
	mode, hadOld, err := replacementTargetMode(ops, target)
	if err != nil || !hadOld {
		return hadOld, err
	}
	if err := ops.Chmod(temp, mode); err != nil {
		return true, fmt.Errorf("local: preserve permissions for %s: %w", target, err)
	}
	if err := preservePlatformMetadata(temp, target); err != nil {
		return true, err
	}
	return true, nil
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
		_ = w.ops.Remove(w.temp)
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
	hadOld, err := preserveExistingMetadata(w.ops, w.temp, w.target)
	if err != nil {
		return err
	}

	backup := w.target + ".shoal-backup-" + uuid.NewString()
	if err := w.ops.AtomicReplace(w.temp, w.target, backup, hadOld); err != nil {
		publishErr := fmt.Errorf("local: publish %s: %w", w.target, err)
		if hadOld {
			if cleanupErr := w.ops.Remove(backup); cleanupErr != nil && !errors.Is(cleanupErr, fs.ErrNotExist) {
				publishErr = errors.Join(
					publishErr,
					fmt.Errorf("local: remove unused replacement backup %s: %w", backup, cleanupErr),
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
	if err := w.ops.AtomicRestore(w.target, backup); err != nil {
		w.state = replacementPublished
		return fmt.Errorf("local: atomically restore existing file %s from %s: %w", w.target, backup, err)
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
