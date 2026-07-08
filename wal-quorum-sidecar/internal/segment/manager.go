// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
package segment

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// Manager creates, tracks, and manages WAL segments.
// It is the central registry that maps segment IDs to Segment instances.
type Manager struct {
	mu       sync.RWMutex
	walDir   string
	segments map[string]*Segment
	logger   *slog.Logger
}

// NewManager creates a Manager that stores segments under walDir.
func NewManager(walDir string, logger *slog.Logger) *Manager {
	return &Manager{
		walDir:   walDir,
		segments: make(map[string]*Segment),
		logger:   logger.With("component", "segment-manager"),
	}
}

// Create opens a new segment with the given ID and WAL path.
// The segment file is placed at <walDir>/<segmentID>.wal.
// Returns an error if a segment with the same ID already exists.
func (m *Manager) Create(id, walPath, originatorPod string) (*Segment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.segments[id]; exists {
		return nil, fmt.Errorf("segment %s already exists", id)
	}

	filePath := filepath.Join(m.walDir, id+".wal")
	seg, err := NewSegment(id, walPath, originatorPod, filePath)
	if err != nil {
		return nil, err
	}

	m.segments[id] = seg
	m.logger.Info("segment created", "id", id, "path", filePath, "originator", originatorPod)
	return seg, nil
}

// Get returns the segment with the given ID, or nil if not found.
func (m *Manager) Get(id string) *Segment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.segments[id]
}

// GetOrLoad returns the segment with the given ID. If not in the in-memory
// map, it checks for the segment file on disk and loads it. This is needed
// for recovery: after a pod restart, replicated segment files exist on disk
// but aren't registered in memory. Returns nil if no file exists either.
func (m *Manager) GetOrLoad(id string) *Segment {
	// Fast path: already in memory.
	m.mu.RLock()
	if seg, ok := m.segments[id]; ok {
		m.mu.RUnlock()
		return seg
	}
	m.mu.RUnlock()

	// Check if the file exists on disk.
	filePath := filepath.Join(m.walDir, id+".wal")
	if _, err := os.Stat(filePath); err != nil {
		return nil
	}

	// Load the segment from the existing file.
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check under write lock.
	if seg, ok := m.segments[id]; ok {
		return seg
	}

	seg, err := NewSegment(id, "", "", filePath)
	if err != nil {
		m.logger.Error("failed to load segment from disk", "id", id, "error", err)
		return nil
	}

	m.segments[id] = seg
	m.logger.Info("loaded segment from disk", "id", id, "path", filePath, "size", seg.Offset())
	return seg
}

// Remove removes a segment from the manager's tracking (does NOT delete the file).
// Returns the removed segment or nil if not found.
func (m *Manager) Remove(id string) *Segment {
	m.mu.Lock()
	defer m.mu.Unlock()

	seg, ok := m.segments[id]
	if !ok {
		return nil
	}
	delete(m.segments, id)
	return seg
}

// Delete removes a segment from tracking AND deletes the file on disk.
func (m *Manager) Delete(id string) error {
	seg := m.Remove(id)
	if seg == nil {
		return fmt.Errorf("segment %s not found", id)
	}

	_ = seg.Close()
	filePath := seg.FilePath()
	if filePath == "" {
		return nil
	}
	if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove segment file %s: %w", filePath, err)
	}
	m.logger.Info("segment deleted", "id", id, "path", filePath)
	return nil
}

// OpenCount returns the number of segments currently in the Open state.
func (m *Manager) OpenCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, seg := range m.segments {
		if !seg.IsSealed() {
			count++
		}
	}
	return count
}

// SealedCount returns the number of segments currently in the Sealed state.
func (m *Manager) SealedCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, seg := range m.segments {
		if seg.IsSealed() {
			count++
		}
	}
	return count
}

// AllSegments returns a snapshot of all tracked segments.
func (m *Manager) AllSegments() []*Segment {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Segment, 0, len(m.segments))
	for _, seg := range m.segments {
		result = append(result, seg)
	}
	return result
}

// SealAll seals every open segment. Used during graceful shutdown.
// Errors are accumulated and returned as a combined error.
func (m *Manager) SealAll(ctx context.Context) error {
	m.mu.RLock()
	openSegs := make([]*Segment, 0)
	for _, seg := range m.segments {
		if !seg.IsSealed() {
			openSegs = append(openSegs, seg)
		}
	}
	m.mu.RUnlock()

	var errs []error
	for _, seg := range openSegs {
		select {
		case <-ctx.Done():
			errs = append(errs, ctx.Err())
			return errors.Join(errs...)
		default:
		}

		_, _, err := seg.Seal()
		if err != nil {
			m.logger.Error("failed to seal segment during shutdown", "id", seg.ID(), "error", err)
			errs = append(errs, fmt.Errorf("seal %s: %w", seg.ID(), err))
		} else {
			m.logger.Info("sealed segment during shutdown", "id", seg.ID())
		}
	}
	return errors.Join(errs...)
}

// ListDir scans the WAL directory and returns info about each .wal file found.
// This is used by the peer ListSegments RPC for segments that may not be in the
// in-memory map (e.g. after a restart).
func (m *Manager) ListDir() ([]SegmentFileInfo, error) {
	entries, err := os.ReadDir(m.walDir)
	if err != nil {
		return nil, fmt.Errorf("read WAL directory %s: %w", m.walDir, err)
	}

	var result []SegmentFileInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) < 5 || name[len(name)-4:] != ".wal" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		segID := name[:len(name)-4]
		seg := m.Get(segID)
		sealed := false
		if seg != nil {
			sealed = seg.IsSealed()
		}
		result = append(result, SegmentFileInfo{
			ID:     segID,
			Size:   info.Size(),
			Sealed: sealed,
		})
	}
	return result, nil
}

// SegmentFilePath returns the full path for a segment file given its ID.
func (m *Manager) SegmentFilePath(id string) string {
	return filepath.Join(m.walDir, id+".wal")
}

// SegmentFileInfo holds metadata about a segment file on disk.
type SegmentFileInfo struct {
	ID     string
	Size   int64
	Sealed bool
}
