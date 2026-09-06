package dirlock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAcquireRejectsCanonicalDuplicateAndReleases(t *testing.T) {
	directory, err := os.MkdirTemp(".", ".dirlock-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	relative, err := filepath.Rel(".", directory)
	if err != nil {
		t.Fatal(err)
	}
	first, err := Acquire(relative, ".store.lock")
	if err != nil {
		t.Fatal(err)
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := Acquire(absolute, ".store.lock"); !errors.Is(err, ErrLocked) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("duplicate acquire = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("idempotent close = %v", err)
	}
	reopened, err := Acquire(absolute, ".store.lock")
	if err != nil {
		t.Fatalf("reopen after release = %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireRejectsInvalidLockName(t *testing.T) {
	if lock, err := Acquire(".", `nested\lock`); err == nil {
		_ = lock.Close()
		t.Fatal("nested lock name succeeded")
	}
}
