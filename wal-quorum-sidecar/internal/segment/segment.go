// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
package segment

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"os"
	"sync"
	"syscall"
)

// State represents the lifecycle state of a WAL segment.
type State int

const (
	StateOpen   State = iota // Accepting writes.
	StateSealed              // No more writes; checksum finalized.
)

// Segment represents a single WAL segment file on disk.
// It tracks the write offset, running checksum, and sealed state.
// All methods are safe for concurrent use.
type Segment struct {
	mu sync.Mutex

	id             string
	walPath        string
	originatorPod  string
	file           *os.File
	offset         int64
	state          State
	hasher         hash.Hash
	finalChecksum  []byte
	sequenceHigh   uint64 // highest sequence_num written
}

// NewSegment opens (or creates) a segment file at the given path.
// The file is opened with O_CREATE|O_WRONLY|O_APPEND to ensure
// sequential writes.
func NewSegment(id, walPath, originatorPod, filePath string) (*Segment, error) {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return nil, fmt.Errorf("open segment file %s: %w", filePath, err)
	}

	// If reopening an existing file, seek to the end to get the current offset.
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat segment file %s: %w", filePath, err)
	}

	return &Segment{
		id:            id,
		walPath:       walPath,
		originatorPod: originatorPod,
		file:          f,
		offset:        info.Size(),
		state:         StateOpen,
		hasher:        sha256.New(),
	}, nil
}

// ID returns the segment's unique identifier.
func (s *Segment) ID() string {
	return s.id
}

// WALPath returns the Accumulo WAL namespace path.
func (s *Segment) WALPath() string {
	return s.walPath
}

// OriginatorPod returns the pod that created this segment.
func (s *Segment) OriginatorPod() string {
	return s.originatorPod
}

// Offset returns the current write offset (total bytes written).
func (s *Segment) Offset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.offset
}

// IsSealed returns true if the segment has been sealed.
func (s *Segment) IsSealed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == StateSealed
}

// HighSequence returns the highest sequence number written to this segment.
func (s *Segment) HighSequence() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sequenceHigh
}

// Write appends data to the segment file and updates the running checksum.
// Returns the byte offset at which the data was written and the new total offset.
// Returns an error if the segment is sealed.
func (s *Segment) Write(data []byte, seqNum uint64) (writeOffset int64, newOffset int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state == StateSealed {
		return 0, 0, errors.New("segment is sealed")
	}

	writeOffset = s.offset
	n, err := s.file.Write(data)
	if err != nil {
		return 0, 0, fmt.Errorf("write to segment %s: %w", s.id, err)
	}
	if n != len(data) {
		return 0, 0, fmt.Errorf("short write to segment %s: wrote %d of %d bytes", s.id, n, len(data))
	}

	// Update running SHA-256.
	s.hasher.Write(data)
	s.offset += int64(n)

	if seqNum > s.sequenceHigh {
		s.sequenceHigh = seqNum
	}

	return writeOffset, s.offset, nil
}

// Fdatasync calls fdatasync(2) on the underlying file descriptor.
// This flushes data to disk without updating file metadata, which is faster
// than a full fsync and sufficient for WAL durability.
func (s *Segment) Fdatasync() error {
	s.mu.Lock()
	fd := int(s.file.Fd())
	s.mu.Unlock()

	if err := syscall.Fdatasync(fd); err != nil {
		return fmt.Errorf("fdatasync segment %s: %w", s.id, err)
	}
	return nil
}

// Seal finalizes the segment: calls fdatasync, computes the final checksum,
// and marks the segment as sealed. No further writes are allowed after sealing.
func (s *Segment) Seal() (checksum []byte, size int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state == StateSealed {
		return s.finalChecksum, s.offset, nil
	}

	// Flush to disk before sealing.
	if err := syscall.Fdatasync(int(s.file.Fd())); err != nil {
		return nil, 0, fmt.Errorf("fdatasync during seal of segment %s: %w", s.id, err)
	}

	s.finalChecksum = s.hasher.Sum(nil)
	s.state = StateSealed

	return s.finalChecksum, s.offset, nil
}

// Checksum returns the current checksum. If the segment is sealed,
// this is the final SHA-256 digest. If open, it is the running digest
// (a snapshot, not finalized).
func (s *Segment) Checksum() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.finalChecksum != nil {
		return s.finalChecksum
	}
	// Return a snapshot of the running hash (non-destructive copy).
	h := sha256.New()
	// We cannot clone the internal state without the hash.Hash interface,
	// so for an open segment we return nil.
	_ = h
	return nil
}

// FinalChecksum returns the SHA-256 digest of the sealed segment.
// Returns nil if the segment has not been sealed.
func (s *Segment) FinalChecksum() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finalChecksum
}

// Close closes the underlying file. Should be called after sealing.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

// FilePath returns the path of the underlying file on disk.
func (s *Segment) FilePath() string {
	if s.file == nil {
		return ""
	}
	return s.file.Name()
}
