// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"

	"google.golang.org/grpc"

	"github.com/phrocker/shoal/internal/embedpb"
	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/obs"
)

// serveConfig configures a shoal-embed "serve" runtime: the gRPC data-plane
// listener plus the HTTP observability surface (/healthz, /readyz, /stats,
// /metrics) used by container/Kubernetes probes and Prometheus scraping.
type serveConfig struct {
	DataDir string

	// GRPCAddress is the bind host:port for the gRPC data plane, e.g.
	// "127.0.0.1:9876" (loopback-only, the CLI default) or "0.0.0.0:9876"
	// (reachable from other pods — required for a Kubernetes Service to
	// route to it).
	GRPCAddress string

	// MetricsAddress is the bind host:port for the observability HTTP
	// server. Same loopback-vs-0.0.0.0 tradeoff as GRPCAddress.
	MetricsAddress string

	Logger *slog.Logger
}

// serveHandle owns the engine, listeners, and servers started by
// startServe. Shutdown is two-phase — Drain then Stop — so an orchestrator's
// readiness probe (polling /readyz) has a chance to observe "not ready" and
// stop routing new work before the listeners actually close:
//
//	h, _ := startServe(cfg)
//	go h.Serve()
//	// ... on SIGTERM:
//	h.Drain()
//	h.Stop(ctx)
type serveHandle struct {
	// GRPCAddr and MetricsAddr are the actual bound addresses (useful when
	// the configured address used port 0 for an OS-assigned port, e.g. in
	// tests).
	GRPCAddr    string
	MetricsAddr string

	eng     *engine.Engine
	obs     *obs.Server
	grpcSrv *grpc.Server
	grpcLis net.Listener
	httpSrv *http.Server
	httpLis net.Listener
	logger  *slog.Logger
}

// startServe opens the engine, binds the gRPC and HTTP listeners, registers
// the ShoalEmbed and observability handlers, and marks the server ready. It
// does not block or start accepting connections — call Serve for that.
func startServe(cfg serveConfig) (*serveHandle, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	eng, err := engine.Open(cfg.DataDir, engine.Options{Logger: logger})
	if err != nil {
		return nil, fmt.Errorf("open engine: %w", err)
	}

	grpcLis, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		_ = eng.Close()
		return nil, fmt.Errorf("grpc listen: %w", err)
	}

	httpLis, err := net.Listen("tcp", cfg.MetricsAddress)
	if err != nil {
		_ = grpcLis.Close()
		_ = eng.Close()
		return nil, fmt.Errorf("metrics listen: %w", err)
	}

	grpcSrv := grpc.NewServer()
	embedpb.RegisterShoalEmbedServer(grpcSrv, newEmbedServer(eng))

	obsSrv := obs.NewServer(eng)
	httpSrv := &http.Server{Handler: obsSrv.Handler()}

	// The engine is open and both listeners are bound: declare ready. This
	// mirrors "shoal-embed up", which flips the same latch once its default
	// table is provisioned.
	obsSrv.SetReady(true)

	return &serveHandle{
		GRPCAddr:    grpcLis.Addr().String(),
		MetricsAddr: httpLis.Addr().String(),
		eng:         eng,
		obs:         obsSrv,
		grpcSrv:     grpcSrv,
		grpcLis:     grpcLis,
		httpSrv:     httpSrv,
		httpLis:     httpLis,
		logger:      logger,
	}, nil
}

// Serve runs the HTTP observability server in the background and the gRPC
// data-plane server in the foreground, blocking until the gRPC server stops
// (via Stop, or a fatal accept error).
func (h *serveHandle) Serve() error {
	go func() {
		if err := h.httpSrv.Serve(h.httpLis); err != nil && err != http.ErrServerClosed {
			h.logger.Error("observability server stopped", slog.String("err", err.Error()))
		}
	}()
	return h.grpcSrv.Serve(h.grpcLis)
}

// Drain marks the server not-ready without closing anything. Call this
// first, on receipt of a shutdown signal: a Kubernetes readiness probe
// polling /readyz observes the change and the endpoint controller removes
// the pod from the Service before Stop closes the listeners, so in-flight
// work can finish without new work arriving behind it. Safe to call once,
// before Stop.
func (h *serveHandle) Drain() {
	h.obs.SetReady(false)
}

// Stop gracefully closes the HTTP server, then the gRPC server, then the
// engine. Call Drain first so an orchestrator's readiness probe has a
// chance to observe the drained state.
//
// Shutdown is bounded by ctx end-to-end: grpc.Server.GracefulStop blocks
// until all in-flight RPCs finish and — unlike http.Server.Shutdown —
// accepts no deadline of its own, so a stuck client (or a long scan) could
// otherwise hang shutdown forever. Stop races GracefulStop against ctx and,
// if ctx expires first, force-closes the HTTP server and force-stops the
// gRPC server (aborting any remaining in-flight RPCs). This is what makes
// --drain-timeout an actual bound rather than a best-effort hint, so a
// Pod's terminationGracePeriodSeconds only has to cover it plus a small
// margin instead of an unbounded wait.
func (h *serveHandle) Stop(ctx context.Context) {
	if err := h.httpSrv.Shutdown(ctx); err != nil {
		if err == context.DeadlineExceeded {
			h.logger.Warn("drain deadline exceeded; forcing http close")
			_ = h.httpSrv.Close()
		} else {
			h.logger.Warn("observability server shutdown", slog.String("err", err.Error()))
		}
	}

	gracefulDone := make(chan struct{})
	go func() {
		h.grpcSrv.GracefulStop()
		close(gracefulDone)
	}()
	select {
	case <-gracefulDone:
	case <-ctx.Done():
		h.logger.Warn("drain deadline exceeded; forcing grpc stop", slog.String("err", ctx.Err().Error()))
		// Stop aborts in-flight RPCs and closes remaining connections,
		// which unblocks the GracefulStop call above too (they share the
		// same connection-tracking state), so this wait stays bounded.
		h.grpcSrv.Stop()
		<-gracefulDone
	}

	if err := h.eng.Close(); err != nil {
		h.logger.Warn("engine close", slog.String("err", err.Error()))
	}
}
