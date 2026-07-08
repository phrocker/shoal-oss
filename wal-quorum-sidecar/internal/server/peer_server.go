// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/accumulo/wal-quorum-sidecar/internal/segment"
	pb "github.com/accumulo/wal-quorum-sidecar/proto/qwalpb"
)

const (
	// readChunkSize is the size of each chunk when streaming segment data back
	// for recovery reads. 1 MB matches the buffer pool allocation.
	readChunkSize = 1 << 20
)

// PeerServer implements the WalQuorumPeer gRPC service.
// It handles sidecar-to-sidecar communication for replication, recovery, and
// segment lifecycle management over TCP (default port 9710).
type PeerServer struct {
	pb.UnimplementedWalQuorumPeerServer

	mgr    *segment.Manager
	logger *slog.Logger
}

// NewPeerServer creates a new PeerServer.
func NewPeerServer(mgr *segment.Manager, logger *slog.Logger) *PeerServer {
	return &PeerServer{
		mgr:    mgr,
		logger: logger.With("component", "peer-server"),
	}
}

// Register adds this service to a gRPC server.
func (s *PeerServer) Register(srv *grpc.Server) {
	pb.RegisterWalQuorumPeerServer(srv, s)
}

// PrepareSegment creates a replica segment file on this peer, ready to receive
// replicated entries from the originator.
func (s *PeerServer) PrepareSegment(ctx context.Context, req *pb.PrepareSegmentRequest) (*pb.PrepareSegmentResponse, error) {
	if req.GetSegmentId() == nil {
		return nil, status.Error(codes.InvalidArgument, "segment_id is required")
	}

	segID := req.GetSegmentId().GetUuid()
	walPath := req.GetSegmentId().GetWalPath()
	originator := req.GetOriginatorPod()

	s.logger.Info("preparing replica segment",
		"segment_id", segID,
		"originator", originator,
		"expected_size", req.GetExpectedSize(),
	)

	_, err := s.mgr.Create(segID, walPath, originator)
	if err != nil {
		// Treat "already exists" as success — the segment was likely created
		// via auto-prepare during ReplicateEntries.
		if strings.Contains(err.Error(), "already exists") {
			s.logger.Debug("segment already exists, treating as success", "segment_id", segID)
			return &pb.PrepareSegmentResponse{Success: true}, nil
		}
		s.logger.Error("failed to prepare replica segment", "segment_id", segID, "error", err)
		return &pb.PrepareSegmentResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.PrepareSegmentResponse{Success: true}, nil
}

// ReplicateEntries is a bidirectional streaming RPC. The originator sidecar
// streams WAL entries; this peer writes them to its local replica and acks.
func (s *PeerServer) ReplicateEntries(stream pb.WalQuorumPeer_ReplicateEntriesServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "recv error: %v", err)
		}

		segID := req.GetSegmentId().GetUuid()
		seg := s.mgr.Get(segID)
		if seg == nil {
			return status.Errorf(codes.NotFound, "replica segment %s not found — call PrepareSegment first", segID)
		}

		_, newOffset, err := seg.Write(req.GetData(), req.GetSequenceNum())
		if err != nil {
			s.logger.Error("replicate write failed",
				"segment_id", segID,
				"seq", req.GetSequenceNum(),
				"error", err,
			)
			return status.Errorf(codes.Internal, "replica write failed: %v", err)
		}

		resp := &pb.ReplicateEntryResponse{
			SegmentId:       req.GetSegmentId(),
			AckedSequenceNum: req.GetSequenceNum(),
			PersistedOffset: newOffset,
		}
		if err := stream.Send(resp); err != nil {
			return status.Errorf(codes.Internal, "send replicate ack error: %v", err)
		}
	}
}

// SealSegment seals a replica segment and verifies that the final checksum
// matches the expected checksum from the originator.
func (s *PeerServer) SealSegment(ctx context.Context, req *pb.SealSegmentRequest) (*pb.SealSegmentResponse, error) {
	if req.GetSegmentId() == nil {
		return nil, status.Error(codes.InvalidArgument, "segment_id is required")
	}

	segID := req.GetSegmentId().GetUuid()
	seg := s.mgr.GetOrLoad(segID)
	if seg == nil {
		return nil, status.Errorf(codes.NotFound, "segment %s not found", segID)
	}

	actualChecksum, actualSize, err := seg.Seal()
	if err != nil {
		s.logger.Error("seal failed on replica", "segment_id", segID, "error", err)
		return &pb.SealSegmentResponse{
			Success:        false,
			Error:          err.Error(),
			ActualChecksum: nil,
			ActualSize:     0,
		}, nil
	}

	resp := &pb.SealSegmentResponse{
		Success:        true,
		ActualChecksum: actualChecksum,
		ActualSize:     actualSize,
	}

	// Verify checksum if the originator provided one.
	expectedChecksum := req.GetExpectedChecksum()
	if len(expectedChecksum) > 0 && !bytes.Equal(actualChecksum, expectedChecksum) {
		s.logger.Error("checksum mismatch on seal",
			"segment_id", segID,
			"expected_size", req.GetExpectedSize(),
			"actual_size", actualSize,
		)
		resp.Success = false
		resp.Error = "checksum mismatch: replica data diverged from originator"
	}

	if req.GetExpectedSize() > 0 && actualSize != req.GetExpectedSize() {
		s.logger.Error("size mismatch on seal",
			"segment_id", segID,
			"expected_size", req.GetExpectedSize(),
			"actual_size", actualSize,
		)
		resp.Success = false
		resp.Error = resp.Error + "; size mismatch"
	}

	// NOTE: Do NOT queue GCS upload here. The segment must stay on disk
	// until the LogSorter reads it for WAL recovery. The originator's sidecar
	// (or the LogCloser returning 0) handles the recovery flow. If we upload
	// and the purger deletes the local copy, the LogSorter can't read it.

	return resp, nil
}

// ReadSegment streams a segment file back to the caller in chunks.
// This is used for recovery reads when a TServer needs WAL data from a peer.
func (s *PeerServer) ReadSegment(req *pb.ReadSegmentRequest, stream pb.WalQuorumPeer_ReadSegmentServer) error {
	if req.GetSegmentId() == nil {
		return status.Error(codes.InvalidArgument, "segment_id is required")
	}

	segID := req.GetSegmentId().GetUuid()
	filePath := s.mgr.SegmentFilePath(segID)

	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return status.Errorf(codes.NotFound, "segment file %s not found", segID)
		}
		return status.Errorf(codes.Internal, "open segment file: %v", err)
	}
	defer f.Close()

	// Seek to requested offset.
	startOffset := req.GetOffset()
	if startOffset > 0 {
		if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
			return status.Errorf(codes.Internal, "seek to offset %d: %v", startOffset, err)
		}
	}

	maxBytes := req.GetMaxBytes()
	buf := make([]byte, readChunkSize)
	var totalRead int64
	currentOffset := startOffset

	for {
		readSize := readChunkSize
		if maxBytes > 0 {
			remaining := maxBytes - totalRead
			if remaining <= 0 {
				break
			}
			if int64(readSize) > remaining {
				readSize = int(remaining)
			}
		}

		n, err := f.Read(buf[:readSize])
		if n > 0 {
			isLast := err == io.EOF || (maxBytes > 0 && totalRead+int64(n) >= maxBytes)
			chunk := &pb.SegmentChunk{
				Data:   buf[:n],
				Offset: currentOffset,
				Last:   isLast,
			}
			if sendErr := stream.Send(chunk); sendErr != nil {
				return status.Errorf(codes.Internal, "send chunk: %v", sendErr)
			}
			totalRead += int64(n)
			currentOffset += int64(n)

			if isLast {
				return nil
			}
		}
		if err == io.EOF {
			// If we haven't sent anything yet, send an empty last chunk.
			if totalRead == 0 {
				if sendErr := stream.Send(&pb.SegmentChunk{
					Data:   nil,
					Offset: currentOffset,
					Last:   true,
				}); sendErr != nil {
					return status.Errorf(codes.Internal, "send empty chunk: %v", sendErr)
				}
			}
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Internal, "read segment file: %v", err)
		}
	}

	return nil
}

// ListSegments scans the WAL directory and returns metadata about each segment.
func (s *PeerServer) ListSegments(ctx context.Context, req *pb.ListSegmentsRequest) (*pb.ListSegmentsResponse, error) {
	files, err := s.mgr.ListDir()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list segments: %v", err)
	}

	var infos []*pb.SegmentInfo
	for _, f := range files {
		// If originator filter is set, check against in-memory segment metadata.
		if req.GetOriginatorPod() != "" {
			seg := s.mgr.Get(f.ID)
			if seg == nil || seg.OriginatorPod() != req.GetOriginatorPod() {
				continue
			}
		}

		info := &pb.SegmentInfo{
			SegmentId: &pb.SegmentId{Uuid: f.ID},
			Size:      f.Size,
			Sealed:    f.Sealed,
		}

		// Enrich with in-memory metadata if available.
		if seg := s.mgr.Get(f.ID); seg != nil {
			info.OriginatorPod = seg.OriginatorPod()
			info.SegmentId.WalPath = seg.WALPath()
		}

		infos = append(infos, info)
	}

	return &pb.ListSegmentsResponse{Segments: infos}, nil
}

// PurgeSegment deletes a segment file from this peer. Should only be called
// after the segment has been durably uploaded to GCS.
func (s *PeerServer) PurgeSegment(ctx context.Context, req *pb.PurgeSegmentRequest) (*pb.PurgeSegmentResponse, error) {
	if req.GetSegmentId() == nil {
		return nil, status.Error(codes.InvalidArgument, "segment_id is required")
	}

	segID := req.GetSegmentId().GetUuid()
	s.logger.Info("purging segment", "segment_id", segID)

	if err := s.mgr.Delete(segID); err != nil {
		s.logger.Error("purge failed", "segment_id", segID, "error", err)
		return &pb.PurgeSegmentResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.PurgeSegmentResponse{Success: true}, nil
}
