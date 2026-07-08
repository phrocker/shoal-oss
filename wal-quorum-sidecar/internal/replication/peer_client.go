// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
package replication

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb "github.com/accumulo/wal-quorum-sidecar/proto/qwalpb"
)

// SegmentInfo holds metadata about a segment on a peer.
type SegmentInfo struct {
	ID            string
	WALPath       string
	OriginatorPod string
	Size          int64
	Sealed        bool
}

// PeerClient manages a gRPC connection to one peer sidecar.
// The connection is lazy: it is not established until the first RPC call.
type PeerClient struct {
	address string
	logger  *slog.Logger

	mu     sync.Mutex
	conn   *grpc.ClientConn
	client pb.WalQuorumPeerClient

	// replicateStreams caches open bidirectional ReplicateEntries streams
	// keyed by segment ID, so entries for the same segment reuse the stream.
	streamsMu sync.Mutex
	streams   map[string]*replicateStream
}

// replicateStream holds a persistent bidirectional stream for one segment.
type replicateStream struct {
	stream pb.WalQuorumPeer_ReplicateEntriesClient
	cancel context.CancelFunc
}

// NewPeerClient creates a PeerClient for the given peer address.
// The address should include the port (e.g. "tserver-1.tserver.default.svc.cluster.local:9710").
// No connection is made until the first RPC call.
func NewPeerClient(address string, logger *slog.Logger) *PeerClient {
	return &PeerClient{
		address: address,
		logger:  logger.With("component", "peer-client", "peer", address),
		streams: make(map[string]*replicateStream),
	}
}

// Address returns the peer's address.
func (pc *PeerClient) Address() string {
	return pc.address
}

// ensureConnected lazily establishes the gRPC connection if not already connected.
func (pc *PeerClient) ensureConnected() (pb.WalQuorumPeerClient, error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.client != nil {
		return pc.client, nil
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                5 * time.Minute,
			Timeout:             20 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(4*1024*1024),
			grpc.MaxCallSendMsgSize(4*1024*1024),
		),
	}

	conn, err := grpc.NewClient(pc.address, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial peer %s: %w", pc.address, err)
	}

	pc.conn = conn
	pc.client = pb.NewWalQuorumPeerClient(conn)
	pc.logger.Info("peer connection created")
	return pc.client, nil
}

// PrepareSegment tells the peer to create a replica segment file.
func (pc *PeerClient) PrepareSegment(ctx context.Context, segmentID, walPath, originatorPod string) error {
	client, err := pc.ensureConnected()
	if err != nil {
		return err
	}

	resp, err := client.PrepareSegment(ctx, &pb.PrepareSegmentRequest{
		SegmentId: &pb.SegmentId{
			Uuid:    segmentID,
			WalPath: walPath,
		},
		OriginatorPod: originatorPod,
	})
	if err != nil {
		return fmt.Errorf("PrepareSegment RPC to %s: %w", pc.address, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("PrepareSegment on %s failed: %s", pc.address, resp.GetError())
	}
	return nil
}

// ReplicateEntry sends a single WAL entry to the peer for replication.
// It lazily opens a persistent bidirectional stream per segment ID and reuses
// it for subsequent entries. Returns nil on success (peer ack received).
func (pc *PeerClient) ReplicateEntry(ctx context.Context, segmentID string, walPath string, originatorPod string, data []byte, offset int64, seqNum uint64) error {
	return pc.replicateEntryInner(ctx, segmentID, walPath, originatorPod, data, offset, seqNum, true)
}

func (pc *PeerClient) replicateEntryInner(ctx context.Context, segmentID, walPath, originatorPod string, data []byte, offset int64, seqNum uint64, retryPrepare bool) error {
	stream, err := pc.getOrCreateStream(ctx, segmentID)
	if err != nil {
		// Don't auto-prepare here — the background goroutine in OpenSegment
		// handles late peers via PrepareSegment + replaySegmentToPeer.
		// Auto-preparing here races with the background replay and corrupts
		// the peer's WAL file (entries get prepended before the header).
		return err
	}

	req := &pb.ReplicateEntryRequest{
		SegmentId: &pb.SegmentId{
			Uuid: segmentID,
		},
		Data:        data,
		Offset:      offset,
		SequenceNum: seqNum,
	}
	if err := stream.Send(req); err != nil {
		pc.closeStream(segmentID)
		return fmt.Errorf("send replicate entry to %s: %w", pc.address, err)
	}

	resp, err := stream.Recv()
	if err != nil {
		pc.closeStream(segmentID)
		return fmt.Errorf("recv replicate ack from %s: %w", pc.address, err)
	}
	if resp.GetAckedSequenceNum() != seqNum {
		return fmt.Errorf("sequence mismatch from %s: expected %d, got %d",
			pc.address, seqNum, resp.GetAckedSequenceNum())
	}

	return nil
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "NotFound") || strings.Contains(s, "not found")
}

// getOrCreateStream returns an existing ReplicateEntries stream for the segment,
// or creates a new one if none exists.
func (pc *PeerClient) getOrCreateStream(ctx context.Context, segmentID string) (pb.WalQuorumPeer_ReplicateEntriesClient, error) {
	pc.streamsMu.Lock()
	defer pc.streamsMu.Unlock()

	if rs, ok := pc.streams[segmentID]; ok {
		return rs.stream, nil
	}

	client, err := pc.ensureConnected()
	if err != nil {
		return nil, err
	}

	// Create a long-lived context for the stream — it persists across entries.
	streamCtx, cancel := context.WithCancel(context.Background())
	stream, err := client.ReplicateEntries(streamCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open ReplicateEntries stream to %s for segment %s: %w",
			pc.address, segmentID, err)
	}

	pc.streams[segmentID] = &replicateStream{
		stream: stream,
		cancel: cancel,
	}
	pc.logger.Debug("opened replication stream", "segment_id", segmentID)
	return stream, nil
}

// closeStream closes and removes the replication stream for a segment.
func (pc *PeerClient) closeStream(segmentID string) {
	pc.streamsMu.Lock()
	defer pc.streamsMu.Unlock()

	if rs, ok := pc.streams[segmentID]; ok {
		_ = rs.stream.CloseSend()
		rs.cancel()
		delete(pc.streams, segmentID)
	}
}

// CloseSegmentStream closes the replication stream for a segment after sealing.
// This should be called after SealSegment completes.
func (pc *PeerClient) CloseSegmentStream(segmentID string) {
	pc.closeStream(segmentID)
}

// SealSegment tells the peer to seal its replica and verify the checksum.
// Returns (success, error). success is true if the peer sealed and checksums match.
func (pc *PeerClient) SealSegment(ctx context.Context, segmentID string, finalOffset int64, checksum []byte) (bool, error) {
	// Close the replication stream for this segment first.
	pc.closeStream(segmentID)

	client, err := pc.ensureConnected()
	if err != nil {
		return false, err
	}

	resp, err := client.SealSegment(ctx, &pb.SealSegmentRequest{
		SegmentId: &pb.SegmentId{
			Uuid: segmentID,
		},
		ExpectedChecksum: checksum,
		ExpectedSize:     finalOffset,
	})
	if err != nil {
		return false, fmt.Errorf("SealSegment RPC to %s: %w", pc.address, err)
	}

	return resp.GetSuccess(), nil
}

// SyncSegment is a no-op on the peer side for now.
// The peer's data is durable after the ReplicateEntries ack (which writes and fsyncs).
// A dedicated peer SyncSegment RPC can be added later for explicit fsync coordination.
// IMPORTANT: Do NOT call SealSegment here — that permanently seals the segment!
func (pc *PeerClient) SyncSegment(ctx context.Context, segmentID string) error {
	// No-op: peer data is already durable after replicate acks.
	// IMPORTANT: Do NOT call SealSegment here — that permanently seals the segment!
	return nil
}

// ReadSegment streams a segment's data from the peer starting at the given offset.
// The caller must close the returned ReadCloser when done.
func (pc *PeerClient) ReadSegment(ctx context.Context, segmentID string, offset int64) (io.ReadCloser, error) {
	client, err := pc.ensureConnected()
	if err != nil {
		return nil, err
	}

	stream, err := client.ReadSegment(ctx, &pb.ReadSegmentRequest{
		SegmentId: &pb.SegmentId{
			Uuid: segmentID,
		},
		Offset: offset,
	})
	if err != nil {
		return nil, fmt.Errorf("ReadSegment RPC to %s: %w", pc.address, err)
	}

	return &segmentReader{stream: stream}, nil
}

// segmentReader adapts a streaming ReadSegment response into io.ReadCloser.
type segmentReader struct {
	stream pb.WalQuorumPeer_ReadSegmentClient
	buf    []byte
	done   bool
}

func (sr *segmentReader) Read(p []byte) (int, error) {
	// Drain any buffered data from previous chunk first.
	if len(sr.buf) > 0 {
		n := copy(p, sr.buf)
		sr.buf = sr.buf[n:]
		return n, nil
	}

	if sr.done {
		return 0, io.EOF
	}

	chunk, err := sr.stream.Recv()
	if err == io.EOF {
		sr.done = true
		return 0, io.EOF
	}
	if err != nil {
		return 0, err
	}

	if chunk.GetLast() {
		sr.done = true
	}

	data := chunk.GetData()
	n := copy(p, data)
	if n < len(data) {
		sr.buf = data[n:]
	}
	if sr.done && len(sr.buf) == 0 {
		return n, io.EOF
	}
	return n, nil
}

func (sr *segmentReader) Close() error {
	sr.done = true
	sr.buf = nil
	return nil
}

// ListSegments queries the peer for all segment metadata matching the given WAL path prefix.
func (pc *PeerClient) ListSegments(ctx context.Context, walPathPrefix string) ([]SegmentInfo, error) {
	client, err := pc.ensureConnected()
	if err != nil {
		return nil, err
	}

	resp, err := client.ListSegments(ctx, &pb.ListSegmentsRequest{
		OriginatorPod: walPathPrefix,
	})
	if err != nil {
		return nil, fmt.Errorf("ListSegments RPC to %s: %w", pc.address, err)
	}

	var result []SegmentInfo
	for _, info := range resp.GetSegments() {
		result = append(result, SegmentInfo{
			ID:            info.GetSegmentId().GetUuid(),
			WALPath:       info.GetSegmentId().GetWalPath(),
			OriginatorPod: info.GetOriginatorPod(),
			Size:          info.GetSize(),
			Sealed:        info.GetSealed(),
		})
	}
	return result, nil
}

// PurgeSegment tells the peer to delete a segment file.
func (pc *PeerClient) PurgeSegment(ctx context.Context, segmentID string) error {
	client, err := pc.ensureConnected()
	if err != nil {
		return err
	}

	resp, err := client.PurgeSegment(ctx, &pb.PurgeSegmentRequest{
		SegmentId: &pb.SegmentId{
			Uuid: segmentID,
		},
	})
	if err != nil {
		return fmt.Errorf("PurgeSegment RPC to %s: %w", pc.address, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("PurgeSegment on %s failed: %s", pc.address, resp.GetError())
	}
	return nil
}

// Close shuts down all open streams and the gRPC connection.
func (pc *PeerClient) Close() {
	// Close all replication streams.
	pc.streamsMu.Lock()
	for segID, rs := range pc.streams {
		_ = rs.stream.CloseSend()
		rs.cancel()
		delete(pc.streams, segID)
	}
	pc.streamsMu.Unlock()

	// Close the gRPC connection.
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn != nil {
		_ = pc.conn.Close()
		pc.conn = nil
		pc.client = nil
		pc.logger.Info("peer connection closed")
	}
}

// IsHealthy returns true if the underlying gRPC connection is alive
// (READY or IDLE state). Returns false if no connection has been established.
func (pc *PeerClient) IsHealthy() bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.conn == nil {
		return false
	}

	state := pc.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}
