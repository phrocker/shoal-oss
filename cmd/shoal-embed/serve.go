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
	"os"
	"sync"
	"sync/atomic"
	"time"

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
	// server (/healthz, /readyz, /stats, /metrics). Same loopback-vs-
	// 0.0.0.0 tradeoff as GRPCAddress. Empty disables the HTTP surface
	// entirely: callers that don't ask for it (e.g. a bare `shoal-embed
	// serve --port N`, matching the top-level README's quick-start) don't
	// gain a second listening port as a side effect of upgrading — see
	// cmdServe's flag handling, which only sets this when the caller
	// explicitly passed --metrics-port or --metrics-address.
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
//
// Production callers should use RunUntilSignal instead of driving Serve /
// Drain / Stop directly: it owns the ordering above AND waits for Stop to
// fully finish before returning, which calling Serve directly does not (see
// RunUntilSignal's doc comment).
type serveHandle struct {
	// GRPCAddr and MetricsAddr are the actual bound addresses (useful when
	// the configured address used port 0 for an OS-assigned port, e.g. in
	// tests).
	GRPCAddr    string
	MetricsAddr string

	eng      *engine.Engine
	obs      *obs.Server
	grpcSrv  *grpc.Server
	grpcLis  net.Listener
	httpSrv  *http.Server
	httpLis  net.Listener
	logger   *slog.Logger
	inFlight *unaryInFlight
}

// unaryInFlight counts currently-executing unary RPCs so Stop can report,
// rather than silently block on, the case that matters most for a bounded
// drain: Write/Flush/Compact run a single synchronous engine call and
// (unlike the streaming Scan handler) don't observe transport
// cancellation, so force-stopping the gRPC server does not interrupt one
// already in progress — Stop still has to wait for it to return before
// closing the engine, or risk closing it out from under that call.
type unaryInFlight struct{ n atomic.Int64 }

func (u *unaryInFlight) intercept(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	u.n.Add(1)
	defer u.n.Add(-1)
	return handler(ctx, req)
}

func (u *unaryInFlight) count() int64 { return u.n.Load() }

// startServe opens the engine, binds the gRPC and HTTP listeners, registers
// the ShoalEmbed and observability handlers, and marks the server ready. It
// does not block or start accepting connections — call Serve for that. The
// HTTP observability listener is only bound when cfg.MetricsAddress is
// non-empty; see its doc comment.
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

	var httpLis net.Listener
	if cfg.MetricsAddress != "" {
		httpLis, err = net.Listen("tcp", cfg.MetricsAddress)
		if err != nil {
			_ = grpcLis.Close()
			_ = eng.Close()
			return nil, fmt.Errorf("metrics listen: %w", err)
		}
	}

	inFlight := &unaryInFlight{}
	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(inFlight.intercept))
	embedpb.RegisterShoalEmbedServer(grpcSrv, newEmbedServer(eng))

	obsSrv := obs.NewServer(eng)
	// obsSrv tracks the ready/not-ready latch regardless of whether the
	// HTTP surface is enabled, so Drain/SetReady never need to special-case
	// a disabled httpSrv — only the listener and *http.Server are
	// conditional.
	var httpSrv *http.Server
	if httpLis != nil {
		httpSrv = &http.Server{Handler: obsSrv.Handler()}
	}

	// The engine is open and the gRPC listener is bound: declare ready.
	// This mirrors "shoal-embed up", which flips the same latch once its
	// default table is provisioned.
	obsSrv.SetReady(true)

	metricsAddr := ""
	if httpLis != nil {
		metricsAddr = httpLis.Addr().String()
	}

	return &serveHandle{
		GRPCAddr:    grpcLis.Addr().String(),
		MetricsAddr: metricsAddr,
		eng:         eng,
		obs:         obsSrv,
		grpcSrv:     grpcSrv,
		grpcLis:     grpcLis,
		httpSrv:     httpSrv,
		httpLis:     httpLis,
		logger:      logger,
		inFlight:    inFlight,
	}, nil
}

// Serve runs the HTTP observability server in the background and the gRPC
// data-plane server in the foreground, blocking until the gRPC server stops
// (via Stop, or a fatal accept error). The HTTP server only runs if the
// observability surface was enabled (cfg.MetricsAddress was non-empty).
func (h *serveHandle) Serve() error {
	if h.httpSrv != nil {
		go func() {
			if err := h.httpSrv.Serve(h.httpLis); err != nil && err != http.ErrServerClosed {
				h.logger.Error("observability server stopped", slog.String("err", err.Error()))
			}
		}()
	}
	return h.grpcSrv.Serve(h.grpcLis)
}

// Drain marks the server not-ready without closing anything. Call this
// first, on receipt of a shutdown signal, and give any readiness-polling
// consumer (an orchestrator, a load balancer, or the Accumulo-model
// manager/coordinator that actually owns shard-to-writer routing) a brief
// quiesce window to observe /readyz before Stop closes the listeners —
// RunUntilSignal does this for you. Whether that observation actually
// removes this pod from routed traffic depends on what's consuming
// /readyz; a Service with publishNotReadyAddresses: true (as the write-tier
// headless Service intentionally is, for StatefulSet peer-DNS stability)
// keeps publishing this pod's address regardless. Safe to call once,
// before Stop.
func (h *serveHandle) Drain() {
	h.obs.SetReady(false)
}

// Stop gracefully stops the HTTP and gRPC servers, then closes the engine.
// Call Drain first so a readiness-polling consumer has a chance to observe
// the drained state.
//
// ctx bounds two things precisely: how long new connections keep being
// accepted, and how long a stuck-but-cancellable in-flight RPC (a
// streaming Scan blocked on client backpressure, for example, which
// notices its stream erroring out) is allowed to keep running before being
// force-aborted. http.Server.Shutdown and grpc.Server.GracefulStop run
// concurrently under the same ctx — not sequentially — so a slow /stats or
// /metrics request cannot eat into the budget meant for draining gRPC
// writes, and vice versa. If ctx expires first, Stop force-closes the HTTP
// server and force-stops the gRPC server (aborting connections and
// cancellable RPCs).
//
// ctx does NOT bound a unary RPC (Write, Flush, Compact) that is already
// executing: those run a single synchronous engine call, ignore their
// request context today, and don't observe transport cancellation the way
// a streaming Send/Recv does, so force-stopping the connection cannot
// interrupt one already in progress. Stop still waits for it before
// closing the engine — closing the engine while a call is still writing to
// it is a correctness hazard --drain-timeout is not worth trading away —
// so a pathologically slow or stuck engine call can extend shutdown past
// --drain-timeout. Stop logs how many such calls are still outstanding
// when the deadline fires so this shows up as an operator-visible warning
// rather than a silent, unexplained delay. Making those engine calls
// themselves cancellation-aware would close this gap fully; that's a
// deeper engine-layer change and is intentionally not attempted here.
func (h *serveHandle) Stop(ctx context.Context) {
	var httpDone sync.WaitGroup
	if h.httpSrv != nil {
		httpDone.Add(1)
		go func() {
			defer httpDone.Done()
			if err := h.httpSrv.Shutdown(ctx); err != nil {
				if err == context.DeadlineExceeded {
					h.logger.Warn("drain deadline exceeded; forcing http close")
					_ = h.httpSrv.Close()
				} else {
					h.logger.Warn("observability server shutdown", slog.String("err", err.Error()))
				}
			}
		}()
	}

	gracefulDone := make(chan struct{})
	go func() {
		h.grpcSrv.GracefulStop()
		close(gracefulDone)
	}()
	select {
	case <-gracefulDone:
	case <-ctx.Done():
		if n := h.inFlight.count(); n > 0 {
			h.logger.Warn("drain deadline exceeded; forcing new-work rejection, but waiting for non-cancellable unary RPC(s) to finish before closing the engine",
				slog.Int64("inFlightUnaryRPCs", n), slog.String("err", ctx.Err().Error()))
		} else {
			h.logger.Warn("drain deadline exceeded; forcing grpc stop", slog.String("err", ctx.Err().Error()))
		}
		// Stop aborts in-flight RPCs and closes remaining connections,
		// which unblocks the GracefulStop call above too (they share the
		// same connection-tracking state). This unblocks any streaming
		// call watching for transport errors immediately; a unary call
		// already inside the engine is unaffected and still has to return
		// on its own (see the doc comment above), so this wait is not
		// itself bounded by ctx in that case.
		h.grpcSrv.Stop()
		<-gracefulDone
	}
	httpDone.Wait()

	if err := h.eng.Close(); err != nil {
		h.logger.Warn("engine close", slog.String("err", err.Error()))
	}
}

// RunUntilSignal is the production entry point for a shoal-embed serve
// process: it blocks running the server until sigCh delivers a shutdown
// signal, then performs the full two-phase drain — Drain, an optional
// quiesce pause (letting a readiness-polling consumer observe /readyz
// before anything actually stops accepting work), and a Stop bounded by
// drainTimeout — and does not return until that entire sequence, including
// the final engine close, has completed.
//
// That last part fixes a real early-exit race: Serve only reflects
// grpc.Server's own internal stop bookkeeping — it says nothing about the
// *additional* work this package's Stop wrapper does around that (waiting
// for the HTTP server to finish shutting down, then closing the engine).
// Those can still be in progress, e.g. blocked on a slow HTTP request or a
// non-cancellable unary RPC, when Serve returns. A caller that reacts to
// Serve returning by exiting the process (returning from main, for
// example) can therefore terminate every other goroutine, including the
// one still finishing Stop, before that work is done. RunUntilSignal
// waits for the shutdown sequence itself — Drain, quiesce, and Stop
// returning — to finish before returning, so its caller never can.
func (h *serveHandle) RunUntilSignal(sigCh <-chan os.Signal, quiesce, drainTimeout time.Duration) error {
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-sigCh
		h.logger.Info("draining")
		h.Drain()
		if quiesce > 0 {
			time.Sleep(quiesce)
		}
		ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		defer cancel()
		h.Stop(ctx)
	}()

	err := h.Serve()
	if err == nil {
		// Serve only returns nil once the gRPC listener has been stopped,
		// which in this function only happens after sigCh fires above —
		// wait for that goroutine to actually finish Stop (HTTP shutdown
		// and engine close included) before returning, per the doc
		// comment.
		<-shutdownDone
	}
	return err
}
