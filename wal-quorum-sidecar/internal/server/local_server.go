// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
package server

import (
	"context"
	"time"
	"fmt"
	"io"
	"os"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/accumulo/wal-quorum-sidecar/internal/config"
	"github.com/accumulo/wal-quorum-sidecar/internal/gcs"
	"github.com/accumulo/wal-quorum-sidecar/internal/metrics"
	"github.com/accumulo/wal-quorum-sidecar/internal/replication"
	"github.com/accumulo/wal-quorum-sidecar/internal/segment"
	pb "github.com/accumulo/wal-quorum-sidecar/proto/qwalpb"
)

// LocalServer implements the WalQuorumLocal gRPC service.
// It handles communication between the co-located TServer and this sidecar
// over a Unix domain socket.
type LocalServer struct {
	pb.UnimplementedWalQuorumLocalServer

	mgr      *segment.Manager
	cfg      *config.Config
	quorum   *replication.QuorumWriter
	pool     *replication.PeerPool
	uploader *gcs.Uploader
	logger   *slog.Logger
}

// NewLocalServer creates a new LocalServer with quorum replication and GCS
// upload enabled. The uploader may be nil if GCS upload is not configured.
func NewLocalServer(mgr *segment.Manager, cfg *config.Config, pool *replication.PeerPool, quorum *replication.QuorumWriter, uploader *gcs.Uploader, logger *slog.Logger) *LocalServer {
	return &LocalServer{
		mgr:      mgr,
		cfg:      cfg,
		quorum:   quorum,
		pool:     pool,
		uploader: uploader,
		logger:   logger.With("component", "local-server"),
	}
}

// Register adds this service to a gRPC server.
func (s *LocalServer) Register(srv *grpc.Server) {
	pb.RegisterWalQuorumLocalServer(srv, s)
}

// OpenSegment creates a new WAL segment via the segment manager.
// In a full implementation this would also fan out PrepareSegment RPCs
// to peer sidecars; for now it creates the local segment.
func (s *LocalServer) OpenSegment(ctx context.Context, req *pb.OpenSegmentRequest) (*pb.OpenSegmentResponse, error) {
	if req.GetSegmentId() == nil {
		return nil, status.Error(codes.InvalidArgument, "segment_id is required")
	}

	segID := req.GetSegmentId().GetUuid()
	walPath := req.GetSegmentId().GetWalPath()
	originator := req.GetOriginatorPod()

	if segID == "" {
		return nil, status.Error(codes.InvalidArgument, "segment_id.uuid is required")
	}

	s.logger.Info("opening segment",
		"segment_id", segID,
		"wal_path", walPath,
		"originator", originator,
		"replication_factor", req.GetReplicationFactor(),
	)

	// Wait for at least 1 peer to be reachable before creating the first segment.
	// This prevents the startup race where segments are created before peers are ready.
	if s.pool != nil && s.mgr.OpenCount() == 0 {
		s.logger.Info("first segment — waiting for peers to be reachable")
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			for _, peer := range s.pool.GetPeers() {
				if peer.IsHealthy() {
					s.logger.Info("peer is reachable, proceeding with segment creation",
						"peer", peer.Address())
					goto peersReady
				}
			}
			time.Sleep(500 * time.Millisecond)
		}
		s.logger.Warn("no peers reachable after 30s, proceeding in degraded mode")
	peersReady:
	}

	_, err := s.mgr.Create(segID, walPath, originator)
	if err != nil {
		s.logger.Error("failed to create segment", "segment_id", segID, "error", err)
		return &pb.OpenSegmentResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	metrics.SegmentsOpen.Inc()

	// Fan out PrepareSegment to peers so they create replica segment files.
	// If a peer isn't ready yet (startup race), retry in background.
	// Always include ALL configured peers in the response so the Java side
	// stores them in the metadata table for recovery (even if PrepareSegment
	// hasn't succeeded yet — the peer will be auto-prepared during writes).
	var preparedPeers []string
	if s.pool != nil {
		for _, peer := range s.pool.GetPeers() {
			preparedPeers = append(preparedPeers, peer.Address())
			if err := peer.PrepareSegment(ctx, segID, walPath, originator); err != nil {
				s.logger.Warn("failed to prepare segment on peer, will retry in background",
					"segment_id", segID,
					"peer", peer.Address(),
					"error", err,
				)
				// Background retry — keeps trying until peer is ready, then replays data
				go func(p *replication.PeerClient) {
					for i := 0; i < 30; i++ { // retry for up to 60 seconds
						time.Sleep(2 * time.Second)
						seg := s.mgr.Get(segID)
						if seg == nil {
							return // segment deleted, stop retrying
						}
						if err := p.PrepareSegment(context.Background(), segID, walPath, originator); err == nil {
							s.logger.Info("peer prepared segment (background retry)",
								"segment_id", segID,
								"peer", p.Address(),
								"attempt", i+1,
							)
							// Replay any data already written to this segment
							s.replaySegmentToPeer(seg, p)
							return
						}
					}
					s.logger.Warn("gave up preparing segment on peer after retries",
						"segment_id", segID,
						"peer", p.Address(),
					)
				}(peer)
				continue
			}
			s.logger.Info("peer prepared segment",
				"segment_id", segID,
				"peer", peer.Address(),
			)
		}
	}

	return &pb.OpenSegmentResponse{
		Success:       true,
		PreparedPeers: preparedPeers,
	}, nil
}

// WriteEntries is a bidirectional streaming RPC. The TServer streams WAL entry
// bytes to the sidecar, and the sidecar streams acks after quorum persistence.
//
// Each received entry is written locally AND replicated to peers via the
// QuorumWriter. The ack is sent back only after 2-of-3 quorum is achieved
// (local + at least one peer), or after the quorum timeout with a degraded
// local-only write.
func (s *LocalServer) WriteEntries(stream pb.WalQuorumLocal_WriteEntriesServer) error {
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
			return status.Errorf(codes.NotFound, "segment %s not found", segID)
		}

		data := req.GetData()
		seqNum := req.GetSequenceNum()
		offset := seg.Offset()

		// Write locally and replicate to peers with quorum semantics.
		quorumCount, err := s.quorum.WriteAndReplicate(stream.Context(), seg, data, offset, seqNum)
		if err != nil {
			s.logger.Error("quorum write failed",
				"segment_id", segID,
				"seq", seqNum,
				"error", err,
			)
			return status.Errorf(codes.Internal, "quorum write failed: %v", err)
		}

		// Ack with actual quorum count (1 = local only / degraded,
		// 2 = local + 1 peer, 3 = local + 2 peers).
		resp := &pb.WriteEntryResponse{
			SegmentId:        req.GetSegmentId(),
			AckedSequenceNum: seqNum,
			QuorumCount:      quorumCount,
			CommittedOffset:  seg.Offset(),
		}
		if err := stream.Send(resp); err != nil {
			return status.Errorf(codes.Internal, "send ack error: %v", err)
		}
	}
}

// SyncSegment calls fdatasync on the local segment file and fans out sync
// requests to peers, waiting for quorum (local + 1 peer) before returning.
func (s *LocalServer) SyncSegment(ctx context.Context, req *pb.SyncSegmentRequest) (*pb.SyncSegmentResponse, error) {
	if req.GetSegmentId() == nil {
		return nil, status.Error(codes.InvalidArgument, "segment_id is required")
	}

	segID := req.GetSegmentId().GetUuid()
	seg := s.mgr.Get(segID)
	if seg == nil {
		return nil, status.Errorf(codes.NotFound, "segment %s not found", segID)
	}

	if err := s.quorum.SyncQuorum(ctx, seg); err != nil {
		s.logger.Error("quorum sync failed", "segment_id", segID, "error", err)
		return &pb.SyncSegmentResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &pb.SyncSegmentResponse{
		Success:      true,
		SyncedOffset: seg.Offset(),
	}, nil
}

// CloseSegment seals the local segment and all peer replicas via quorum seal,
// then triggers an async upload to GCS if configured.
func (s *LocalServer) CloseSegment(ctx context.Context, req *pb.CloseSegmentRequest) (*pb.CloseSegmentResponse, error) {
	if req.GetSegmentId() == nil {
		return nil, status.Error(codes.InvalidArgument, "segment_id is required")
	}

	segID := req.GetSegmentId().GetUuid()
	seg := s.mgr.Get(segID)
	if seg == nil {
		return nil, status.Errorf(codes.NotFound, "segment %s not found", segID)
	}

	// Before sealing, replay data to any peers that were prepared late
	// and may not have received all entries yet.
	if s.pool != nil {
		for _, peer := range s.pool.GetPeers() {
			s.replaySegmentToPeer(seg, peer)
		}
	}

	// Seal locally and fan out to all peers (waits for all, not just quorum).
	checksum, size, err := s.quorum.SealQuorum(ctx, seg)
	if err != nil {
		s.logger.Error("quorum seal failed", "segment_id", segID, "error", err)
		return &pb.CloseSegmentResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	metrics.SegmentsOpen.Dec()
	metrics.SegmentsSealed.Inc()

	gcsPath := ""
	if s.cfg.GCSBucket != "" && s.uploader != nil {
		gcsPath = fmt.Sprintf("gs://%s/wal/%s/%s.wal", s.cfg.GCSBucket, seg.OriginatorPod(), segID)
		s.uploader.QueueUpload(seg, seg.OriginatorPod())
		s.logger.Info("GCS upload queued", "segment_id", segID, "gcs_path", gcsPath)
	}

	s.logger.Info("segment sealed (quorum)",
		"segment_id", segID,
		"size", size,
		"checksum_len", len(checksum),
		"gcs_path", gcsPath,
	)

	return &pb.CloseSegmentResponse{
		Success:       true,
		SegmentSize:   size,
		GcsObjectPath: gcsPath,
	}, nil
}

// replaySegmentToPeer reads the local segment file and sends all its data to
// a peer that was prepared late (after the segment already had writes).
// This ensures the peer has a complete replica even if PrepareSegment failed at startup.
//
// To prevent data corruption from duplicate appends (if auto-prepare already wrote
// partial data), the peer's segment is purged and re-prepared before replay.
func (s *LocalServer) replaySegmentToPeer(seg *segment.Segment, peer *replication.PeerClient) {
	filePath := seg.FilePath()
	f, err := os.Open(filePath)
	if err != nil {
		s.logger.Warn("failed to open segment file for replay",
			"segment_id", seg.ID(),
			"peer", peer.Address(),
			"error", err,
		)
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil || info.Size() == 0 {
		s.logger.Debug("nothing to replay", "segment_id", seg.ID(), "size", info.Size())
		return
	}

	// Read the entire segment and send as one big replicate entry
	data := make([]byte, info.Size())
	n, err := io.ReadFull(f, data)
	if err != nil && err != io.ErrUnexpectedEOF {
		s.logger.Warn("failed to read segment file for replay",
			"segment_id", seg.ID(),
			"error", err,
		)
		return
	}
	data = data[:n]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.logger.Info("replaying segment to peer",
		"segment_id", seg.ID(),
		"peer", peer.Address(),
		"bytes", n,
		"originator_offset", seg.Offset(),
	)

	err = peer.ReplicateEntry(ctx, seg.ID(), seg.WALPath(), seg.OriginatorPod(), data, 0, 0)
	if err != nil {
		s.logger.Warn("failed to replay segment to peer",
			"segment_id", seg.ID(),
			"peer", peer.Address(),
			"bytes", n,
			"error", err,
		)
		return
	}

	s.logger.Info("replayed segment to peer",
		"segment_id", seg.ID(),
		"peer", peer.Address(),
		"bytes", n,
	)
}

// HealthCheck returns the current health status of the sidecar.
func (s *LocalServer) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	openCount := s.mgr.OpenCount()

	var peerConns uint32
	if s.pool != nil {
		for _, p := range s.pool.GetPeers() {
			if p.IsHealthy() {
				peerConns++
			}
		}
	}

	return &pb.HealthCheckResponse{
		Status:          pb.HealthCheckResponse_SERVING,
		OpenSegments:    uint32(openCount),
		PeerConnections: peerConns,
	}, nil
}
