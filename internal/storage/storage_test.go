package storage_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/phrocker/shoal/internal/storage"
	"github.com/phrocker/shoal/internal/storage/local"
	"github.com/phrocker/shoal/internal/storage/memory"
)

// readOnlyBackend wraps a Backend without WritableBackend implementation,
// so we can verify Copy errors cleanly.
type readOnlyBackend struct{ inner storage.Backend }

func (r readOnlyBackend) Open(ctx context.Context, p string) (storage.File, error) {
	return r.inner.Open(ctx, p)
}

func TestCopy_MemoryToMemory(t *testing.T) {
	src := memory.New()
	src.Put("/src/x", []byte("the contents of x"))
	dst := memory.New()

	n, err := storage.Copy(context.Background(), src, "/src/x", dst, "/dst/x")
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len("the contents of x")) {
		t.Errorf("Copy returned n=%d, want %d", n, len("the contents of x"))
	}
	f, err := dst.Open(context.Background(), "/dst/x")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := make([]byte, f.Size())
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if string(got) != "the contents of x" {
		t.Errorf("got %q", got)
	}
}

func TestCopy_MemoryToLocal(t *testing.T) {
	src := memory.New()
	body := bytes.Repeat([]byte{0x55, 0xaa}, 5000) // 10KB — exercises the
	// chunk loop more than once (chunk = 64KB so this fits in 1, but we
	// still need >0 iterations to validate)
	src.Put("/big", body)

	dir := t.TempDir()
	dst := local.New()
	dstPath := filepath.Join(dir, "out", "blob.bin")

	n, err := storage.Copy(context.Background(), src, "/big", dst, dstPath)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(body)) {
		t.Errorf("n = %d, want %d", n, len(body))
	}
	// Read back via the local backend.
	f, err := dst.Open(context.Background(), dstPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := make([]byte, f.Size())
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("local file content mismatch (size=%d)", f.Size())
	}
}

func TestCopy_LargeFile_MultipleChunks(t *testing.T) {
	// 200KB — forces 4 chunk-loop iterations (chunk = 64KB).
	body := bytes.Repeat([]byte{0x42}, 200*1024)
	src := memory.New()
	src.Put("/big", body)
	dst := memory.New()

	n, err := storage.Copy(context.Background(), src, "/big", dst, "/dst")
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(body)) {
		t.Errorf("n = %d, want %d", n, len(body))
	}
	f, _ := dst.Open(context.Background(), "/dst")
	defer f.Close()
	if f.Size() != int64(len(body)) {
		t.Errorf("dst size = %d, want %d", f.Size(), len(body))
	}
}

func TestCopy_RejectsNonWritableDst(t *testing.T) {
	src := memory.New()
	src.Put("/x", []byte("data"))
	dst := readOnlyBackend{inner: memory.New()} // wraps memory but hides WritableBackend

	_, err := storage.Copy(context.Background(), src, "/x", dst, "/dst")
	if !errors.Is(err, storage.ErrReadOnly) {
		t.Errorf("err = %v, want ErrReadOnly", err)
	}
}

func TestCopy_SrcNotFound(t *testing.T) {
	src := memory.New()
	dst := memory.New()
	_, err := storage.Copy(context.Background(), src, "/missing", dst, "/wherever")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("err = %v, want chain to storage.ErrNotFound", err)
	}
}

func TestCopy_EmptyFile(t *testing.T) {
	src := memory.New()
	src.Put("/empty", nil)
	dst := memory.New()
	n, err := storage.Copy(context.Background(), src, "/empty", dst, "/dst")
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("n = %d, want 0", n)
	}
	f, err := dst.Open(context.Background(), "/dst")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if f.Size() != 0 {
		t.Errorf("dst size = %d, want 0", f.Size())
	}
}
