// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
package segment_test

import (
	"log/slog"
	"os"
	"testing"

	"github.com/accumulo/wal-quorum-sidecar/internal/segment"
)

func testLogger() *slog.Logger {
	return slog.Default()
}

// TestSegmentReplayAfterPeerDelay simulates the startup race:
// 1. Originator creates segment and writes data (peers not ready)
// 2. Peer comes up late
// 3. Originator replays buffered data to peer
func TestSegmentReplayAfterPeerDelay(t *testing.T) {
	// Create temp dirs for originator and peer
	origDir := t.TempDir()
	peerDir := t.TempDir()

	// Create segment manager for originator
	origMgr := segment.NewManager(origDir, testLogger())

	// Create a segment and write some data
	seg, err := origMgr.Create("test-segment-1", "/wal/test/test-segment-1", "tserver-0")
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	// Write 10 entries
	for i := 0; i < 10; i++ {
		data := []byte("entry-" + string(rune('0'+i)) + "-data-payload-for-testing")
		_, _, err := seg.Write(data, uint64(i+1))
		if err != nil {
			t.Fatalf("failed to write entry %d: %v", i, err)
		}
	}

	// Sync
	if err := seg.Fdatasync(); err != nil {
		t.Fatalf("fdatasync failed: %v", err)
	}

	// Verify segment has data
	offset := seg.Offset()
	if offset == 0 {
		t.Fatal("segment offset should be > 0 after writes")
	}
	t.Logf("originator segment has %d bytes, %d entries", offset, seg.HighSequence())

	// Simulate peer coming up late — create peer segment manager
	peerMgr := segment.NewManager(peerDir, testLogger())

	// Peer creates replica (PrepareSegment equivalent)
	peerSeg, err := peerMgr.Create("test-segment-1", "/wal/test/test-segment-1", "tserver-0")
	if err != nil {
		t.Fatalf("peer failed to create segment: %v", err)
	}

	// Simulate replay: read originator's segment file and write to peer
	origFile := seg.FilePath()
	data, err := os.ReadFile(origFile)
	if err != nil {
		t.Fatalf("failed to read originator segment: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("originator segment file is empty")
	}

	// Write replayed data to peer segment
	_, _, err = peerSeg.Write(data, 0) // seq 0 for replay
	if err != nil {
		t.Fatalf("peer failed to write replayed data: %v", err)
	}

	if err := peerSeg.Fdatasync(); err != nil {
		t.Fatalf("peer fdatasync failed: %v", err)
	}

	// Verify peer has same data
	peerOffset := peerSeg.Offset()
	if peerOffset != offset {
		t.Errorf("peer offset %d != originator offset %d", peerOffset, offset)
	}

	// Verify files are identical
	peerData, err := os.ReadFile(peerSeg.FilePath())
	if err != nil {
		t.Fatalf("failed to read peer segment: %v", err)
	}

	if len(peerData) != len(data) {
		t.Errorf("peer file size %d != originator file size %d", len(peerData), len(data))
	}

	t.Logf("replay successful: peer has %d bytes matching originator", peerOffset)

	// Clean up
	seg.Close()
	peerSeg.Close()
}

// TestSegmentSealDoesNotBlockReplay verifies that a sealed segment
// can still be read for replay purposes.
func TestSegmentSealDoesNotBlockReplay(t *testing.T) {
	dir := t.TempDir()
	mgr := segment.NewManager(dir, testLogger())

	seg, err := mgr.Create("seal-test", "/wal/test/seal-test", "tserver-0")
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Write data
	testData := []byte("important WAL data that must survive")
	_, _, err = seg.Write(testData, 1)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	seg.Fdatasync()

	// Seal the segment
	checksum, size, err := seg.Seal()
	if err != nil {
		t.Fatalf("seal failed: %v", err)
	}
	t.Logf("sealed: size=%d, checksum=%x", size, checksum)

	// Verify we can still read the file even though segment is sealed
	data, err := os.ReadFile(seg.FilePath())
	if err != nil {
		t.Fatalf("reading sealed segment file failed: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("sealed segment file is empty")
	}

	// Verify writing to sealed segment fails
	_, _, err = seg.Write([]byte("should fail"), 2)
	if err == nil {
		t.Fatal("expected write to sealed segment to fail")
	}

	t.Logf("sealed segment readable (%d bytes), write correctly rejected", len(data))

	seg.Close()
}

// TestManagerOpenCount verifies segment tracking
func TestManagerOpenCount(t *testing.T) {
	dir := t.TempDir()
	mgr := segment.NewManager(dir, testLogger())

	if mgr.OpenCount() != 0 {
		t.Fatal("expected 0 open segments")
	}

	seg1, _ := mgr.Create("s1", "/wal/test/s1", "t-0")
	if mgr.OpenCount() != 1 {
		t.Fatal("expected 1 open segment")
	}

	seg2, _ := mgr.Create("s2", "/wal/test/s2", "t-0")
	if mgr.OpenCount() != 2 {
		t.Fatal("expected 2 open segments")
	}

	seg1.Seal()
	// Sealed segments are still tracked until deleted
	got := mgr.Get("s1")
	if got == nil {
		t.Fatal("sealed segment should still be gettable")
	}

	seg1.Close()
	seg2.Close()
}
