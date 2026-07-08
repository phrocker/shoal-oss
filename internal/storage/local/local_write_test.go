package local

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
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
