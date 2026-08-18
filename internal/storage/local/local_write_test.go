package local

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/phrocker/shoal/internal/storage"
)

func TestLocal_CreateAndReadBack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	be := New()

	w, err := be.Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("hello, write side")
	if _, err := w.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// File now exists with the bytes we wrote.
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("disk content = %q, want %q", got, body)
	}
}

func TestLocal_CreateMakesParentDirs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deep", "nested", "tree", "out.bin")
	be := New()

	w, err := be.Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("nested")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("Create did not produce file: %v", err)
	}
}

func TestLocal_CreateReplacesExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.bin")
	if err := os.WriteFile(path, []byte("old contents that are longer"), 0o644); err != nil {
		t.Fatal(err)
	}

	be := New()
	w, err := be.Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Errorf("got %q, want \"new\" (Create should truncate)", got)
	}
}

func TestLocal_CreateUsesOpenFileModeSubjectToUmask(t *testing.T) {
	dir := t.TempDir()
	referencePath := filepath.Join(dir, "reference")
	reference, err := os.OpenFile(referencePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := reference.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "created")
	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	referenceInfo, err := os.Stat(referencePath)
	if err != nil {
		t.Fatal(err)
	}
	createdInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := createdInfo.Mode().Perm(), referenceInfo.Mode().Perm(); got != want {
		t.Fatalf("created mode = %04o, want OpenFile(0644) mode %04o", got, want)
	}
}

func TestLocal_CreateReplacementPreservesExistingMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "replaced")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalInfo, err := os.Stat(path)
	if err != nil {
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

	replacedInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := replacedInfo.Mode().Perm(), originalInfo.Mode().Perm(); got != want {
		t.Fatalf("replacement mode = %04o, want existing mode %04o", got, want)
	}
}

func TestLocal_CreateRejectsDirectoryTarget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}

	w, err := New().Create(context.Background(), path)
	if err == nil || !strings.Contains(err.Error(), "not a regular file") {
		if w != nil {
			_ = w.(storage.Aborter).Abort()
		}
		t.Fatalf("Create error = %v, want non-regular target rejection", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("target mode = %v, want directory preserved", info.Mode())
	}
}

func TestLocal_BackupRemovalFailureRollsBackReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	localWriter := w.(*writer)
	removeErr := errors.New("injected backup removal failure")
	localWriter.ops = failBackupRemoveOps{replacementOps: localWriter.ops, err: removeErr}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if !errors.Is(err, removeErr) {
		t.Fatalf("Close error = %v, want %v", err, removeErr)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Fatalf("target contents = %q, want original data", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if gotMode, wantMode := info.Mode().Perm(), originalInfo.Mode().Perm(); gotMode != wantMode {
		t.Fatalf("target mode = %04o, want original mode %04o", gotMode, wantMode)
	}
	if err := w.(storage.Aborter).Abort(); err != nil {
		t.Fatalf("Abort after rollback: %v", err)
	}
	if matches, err := filepath.Glob(path + ".shoal-*"); err != nil {
		t.Fatal(err)
	} else if len(matches) != 0 {
		t.Fatalf("replacement artifacts remain: %v", matches)
	}
}

func TestLocal_AbortPreservesExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.bin")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	be := New()
	w, err := be.Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	aborter, ok := w.(storage.Aborter)
	if !ok {
		t.Fatal("writer does not implement storage.Aborter")
	}
	if err := aborter.Abort(); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Fatalf("got %q, want original file preserved", got)
	}
}

func TestLocal_AbortAfterFailedCloseRemovesTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}

	localWriter := w.(*writer)
	temp := localWriter.temp
	if err := localWriter.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err == nil {
		t.Fatal("Close succeeded after the temporary file was closed")
	}
	if err := w.(storage.Aborter).Abort(); err != nil {
		t.Fatalf("Abort after failed Close: %v", err)
	}
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatalf("temporary file still exists after Abort: %v", err)
	}
}

type failBackupRemoveOps struct {
	replacementOps
	err error
}

func (o failBackupRemoveOps) Remove(name string) error {
	if strings.Contains(name, ".shoal-backup-") {
		return o.err
	}
	return o.replacementOps.Remove(name)
}
