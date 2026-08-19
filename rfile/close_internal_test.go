package rfile

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/phrocker/shoal/accumulo"
)

// TestWriterCloseRepeatsItsFailure pins that a failed finalization is never
// reported as success by a later Close: the index and trailer are written by
// Close, so a caller that saw nil from the second call would treat a truncated
// file as complete.
func TestWriterCloseRepeatsItsFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "closefail.rf")
	writer, err := Create(context.Background(), path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Append(context.Background(), Entry{
		Key:   accumulo.Key{Row: []byte("row1"), ColumnFamily: []byte("cf"), Timestamp: 10},
		Value: []byte("v1"),
	}); err != nil {
		t.Fatal(err)
	}
	// Closing the handle underneath the writer makes the trailer write fail,
	// which is what a revoked handle or a full disk looks like.
	if err := writer.file.Close(); err != nil {
		t.Fatal(err)
	}

	first := writer.Close()
	if first == nil {
		t.Fatal("Close over a released handle returned nil")
	}
	second := writer.Close()
	if second == nil {
		t.Fatal("the second Close reported success after the first failed")
	}
	if first.Error() != second.Error() {
		t.Fatalf("Close reported %q then %q; the failure must be remembered", first, second)
	}
}

// TestReaderCloseRepeatsItsFailure pins the same contract on the read side.
func TestReaderCloseRepeatsItsFailure(t *testing.T) {
	boom := errors.New("release failed")
	calls := 0
	reader := &Reader{closers: []func() error{func() error {
		calls++
		return boom
	}}}

	first := reader.Close()
	if !errors.Is(first, boom) {
		t.Fatalf("Close = %v, want the closer's error", first)
	}
	second := reader.Close()
	if !errors.Is(second, boom) {
		t.Fatalf("the second Close = %v, want the remembered error", second)
	}
	if calls != 1 {
		t.Fatalf("the closer ran %d times, want exactly 1", calls)
	}
}
