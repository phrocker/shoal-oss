//go:build linux || darwin

package local

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestLocal_ReplacementPreservesExtendedAttributes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target")
	if err := os.WriteFile(path, []byte("old"), 0o640); err != nil {
		t.Fatal(err)
	}

	const attr = "user.shoal-preserve"
	want := []byte("value")
	if err := unix.Setxattr(path, attr, want, 0); err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.EPERM) {
			t.Skipf("extended attributes unavailable: %v", err)
		}
		t.Fatal(err)
	}

	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := getXattr(path, attr)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("xattr %s = %q, want %q", attr, got, want)
	}
}
