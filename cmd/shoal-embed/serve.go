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
				// http.Server.Shutdown returns ctx.Err() (either
				// DeadlineExceeded or, for a caller-cancelable ctx,
				// Canceled) if ctx is done before every connection
				// finishes closing gracefully. Force-close on either —
				// checking only for DeadlineExceeded would leave active
				// HTTP handlers/connections open past an explicit cancel,
				// letting Stop return with the HTTP surface still up.
				if ctx.Err() != nil {
					h.logger.Warn("drain deadline exceeded or canceled; forcing http close", slog.String("err", err.Error()))
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
// That covers one real early-exit race: Serve only reflects
// grpc.Server's own internal stop bookkeeping — it says nothing about the
// *additional* work this package's Stop wrapper does around that (waiting
// for the HTTP server to finish shutting down, then closing the engine).
// Those can still be in progress, e.g. blocked on a slow HTTP request or a
// non-cancellable unary RPC, when Serve returns. A caller that reacts to
// Serve returning by exiting the process (returning from main, for
// example) can therefore terminate every other goroutine, including the
// one still finishing Stop, before that work is done.
//
// It also covers the same race from the other direction: Serve can return
// a non-nil error *before* sigCh ever fires — either because the signal
// arrives (or, as here, was already queued) early enough that this
// method's own Drain/quiesce/Stop sequence stops the gRPC server before
// Serve is even called (grpc.Server.Serve returns grpc.ErrServerStopped
// immediately if the server is already stopped when it's invoked), or
// because of a genuine, signal-independent fatal accept error. Both the
// signal arriving and Serve returning are therefore raced against each
// other explicitly, via select, rather than left to chance: Serve runs in
// its own goroutine so a pending signal — even one already queued before
// this method is called at all — can be observed without first waiting
// for Serve's goroutine to be scheduled and to actually execute, so a
// signal that raced ahead of Serve is reliably still recognized as the
// reason for shutting down rather than mistaken for a Serve failure.
// Whichever wins, this method runs the Drain/quiesce/Stop sequence
// exactly once and always waits for it to fully finish — including the
// engine close, and Serve's goroutine actually returning — before
// returning, so a caller can never see this method return and exit while
// that work is still in progress. A signal-triggered shutdown always
// reports success (nil), even if Serve's own return raced into a non-nil
// error as a side effect of that same shutdown; a genuinely
// signal-independent Serve failure still reports its real error, after
// the resulting Drain/Stop has completed so the engine and HTTP server
// are never left open.
func (h *serveHandle) RunUntilSignal(sigCh <-chan os.Signal, quiesce, drainTimeout time.Duration) error {
	serveErrCh := make(chan error, 1)
	go func() { serveErrCh <- h.Serve() }()

	var serveErr error
	signaled := false
	select {
	case <-sigCh:
		signaled = true
	case serveErr = <-serveErrCh:
	}

	h.logger.Info("draining")
	h.Drain()
	if quiesce > 0 {
		time.Sleep(quiesce)
	}
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	h.Stop(ctx)

	if !signaled {
		return serveErr
	}
	// The signal branch above never consumed serveErrCh. Stop's
	// GracefulStop/Stop calls guarantee Serve returns shortly if it
	// hasn't already, so this always resolves promptly — wait for it so
	// this method doesn't return until Serve's goroutine, including the
	// HTTP accept loop it starts, has actually exited too.
	<-serveErrCh
	return nil
}
