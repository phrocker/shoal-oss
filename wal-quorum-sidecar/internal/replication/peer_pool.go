// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
package replication

import (
	"log/slog"

	"github.com/accumulo/wal-quorum-sidecar/internal/config"
	"github.com/accumulo/wal-quorum-sidecar/internal/metrics"
)

// PeerPool discovers and manages connections to all peer sidecars.
// Peers are determined at startup from the StatefulSet DNS convention
// provided by the config. No dynamic discovery is performed.
type PeerPool struct {
	peers  []*PeerClient
	logger *slog.Logger
}

// NewPeerPool creates a PeerPool with PeerClients for each peer address
// from the config. Connections are lazy and not established until the first
// RPC call on each client.
func NewPeerPool(cfg *config.Config, logger *slog.Logger) *PeerPool {
	peerAddrs := cfg.Peers()
	logger = logger.With("component", "peer-pool")

	peers := make([]*PeerClient, 0, len(peerAddrs))
	for _, addr := range peerAddrs {
		pc := NewPeerClient(addr, logger)
		peers = append(peers, pc)
		logger.Info("peer registered", "address", addr)
	}

	metrics.PeerConnections.Set(float64(len(peers)))
	logger.Info("peer pool initialized", "peer_count", len(peers))

	return &PeerPool{
		peers:  peers,
		logger: logger,
	}
}

// GetPeers returns all peer clients. The caller can check IsHealthy()
// on each to filter to only reachable peers.
func (pp *PeerPool) GetPeers() []*PeerClient {
	return pp.peers
}

// GetHealthyPeers returns only the peers whose gRPC connections are alive.
// Note: a peer that has never been contacted will show as unhealthy (no
// connection yet). Use GetPeers() if you want all peers including those
// with lazy connections.
func (pp *PeerPool) GetHealthyPeers() []*PeerClient {
	healthy := make([]*PeerClient, 0, len(pp.peers))
	for _, p := range pp.peers {
		if p.IsHealthy() {
			healthy = append(healthy, p)
		}
	}
	return healthy
}

// PeerCount returns the total number of configured peers.
func (pp *PeerPool) PeerCount() int {
	return len(pp.peers)
}

// Close shuts down all peer connections.
func (pp *PeerPool) Close() {
	for _, p := range pp.peers {
		p.Close()
	}
	metrics.PeerConnections.Set(0)
	pp.logger.Info("peer pool closed")
}
