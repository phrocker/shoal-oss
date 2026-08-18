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
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/phrocker/shoal/internal/embedpb"
)

// newRawTestServeHandle starts a serveHandle bound to OS-assigned loopback
// ports without starting its accept loops. Most tests want
// newTestServeHandle instead; this exists for tests (RunUntilSignal's) that
// need to drive Serve indirectly via a method that calls it internally,
// where calling Serve a second time here too would race it.
func newRawTestServeHandle(t *testing.T) *serveHandle {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := startServe(serveConfig{
		DataDir:        t.TempDir(),
		GRPCAddress:    "127.0.0.1:0",
		MetricsAddress: "127.0.0.1:0",
		Logger:         logger,
	})
	if err != nil {
		t.Fatalf("startServe: %v", err)
	}
	return h
}

// newTestServeHandle starts a serveHandle bound to OS-assigned loopback
// ports and runs its accept loops in the background, returning the handle
// plus the channel Serve's return value will arrive on. It performs no
// teardown, so callers control the Drain/Stop sequence themselves.
func newTestServeHandle(t *testing.T) (*serveHandle, <-chan error) {
	t.Helper()
	h := newRawTestServeHandle(t)
	serveErrCh := make(chan error, 1)
	go func() { serveErrCh <- h.Serve() }()
	return h, serveErrCh
}

// startTestServe starts a serveHandle and arranges for it to be drained and
// stopped when the test completes, for tests that only need a running
// server and don't themselves exercise the Drain/Stop sequence.
func startTestServe(t *testing.T) *serveHandle {
	t.Helper()
	h, serveErrCh := newTestServeHandle(t)
	t.Cleanup(func() {
		h.Drain()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.Stop(ctx)
		select {
		case <-serveErrCh:
		case <-time.After(5 * time.Second):
			t.Error("Serve did not return within 5s of Stop")
		}
	})
	return h
}

// waitForStatus polls url until it returns wantStatus or the deadline
// elapses, so tests don't race the Serve goroutine's accept loop startup.
func waitForStatus(t *testing.T, url string, wantStatus int) *http.Response {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err != nil {
			lastErr = err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if resp.StatusCode == wantStatus {
			return resp
		}
		resp.Body.Close()
		lastErr = fmt.Errorf("got status %d", resp.StatusCode)
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("GET %s never returned %d: %v", url, wantStatus, lastErr)
	return nil
}

func TestStartServeReadyHealthAndMetrics(t *testing.T) {
	h := startTestServe(t)

	resp := waitForStatus(t, "http://"+h.MetricsAddr+"/readyz", http.StatusOK)
	resp.Body.Close()

	resp, err := http.Get("http://" + h.MetricsAddr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", resp.StatusCode)
	}

	resp, err = http.Get("http://" + h.MetricsAddr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /metrics body: %v", err)
	}
	if !strings.Contains(string(body), "shoal_tables") {
		t.Errorf("/metrics missing shoal_tables gauge:\n%s", body)
	}

	conn, err := grpc.NewClient(h.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()
	client := embedpb.NewShoalEmbedClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Status(ctx, &embedpb.StatusRequest{}); err != nil {
		t.Fatalf("Status RPC: %v", err)
	}
}

func TestServeHandleDrainThenStop(t *testing.T) {
	h, serveErrCh := newTestServeHandle(t)

	// Ready before draining.
	resp := waitForStatus(t, "http://"+h.MetricsAddr+"/readyz", http.StatusOK)
	resp.Body.Close()

	// Drain flips readiness without closing the listeners: the endpoint
	// stays reachable (an orchestrator's readiness probe must still be able
	// to observe the not-ready state) but now reports not-ready.
	h.Drain()
	resp, err := http.Get("http://" + h.MetricsAddr + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz after Drain: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("/readyz after Drain = %d, want 503 (not ready but still reachable)", resp.StatusCode)
	}

	// Stop closes the listeners and the engine.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h.Stop(ctx)

	select {
	case err := <-serveErrCh:
		if err != nil {
			t.Errorf("Serve() returned %v, want nil after a clean Stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return within 5s of Stop")
	}

	if _, err := http.Get("http://" + h.MetricsAddr + "/readyz"); err == nil {
		t.Error("GET /readyz succeeded after Stop, want connection error (listener closed)")
	}
}

// startStuckStreamingScan starts a real streaming Scan RPC against h, using
// a client pinned to a small static (non-BDP-adaptive) flow-control
// window, and deliberately stops reading after the first message so the
// still-running server-side handler blocks on send backpressure trying to
// deliver the rest — a real in-flight, non-cancellable-by-closing-the-
// connection-alone RPC that only unblocks once the connection itself is
// torn down (e.g. by conn.Close, or by the server force-stopping). Callers
// use this to exercise Stop's/RunUntilSignal's force-path deterministically
// instead of racing a real slow client.
func startStuckStreamingScan(t *testing.T, h *serveHandle) {
	t.Helper()
	const staticWindow = 128 * 1024 // bytes; disables BDP-based auto-growth.
	conn, err := grpc.NewClient(h.GRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialWindowSize(staticWindow),
		grpc.WithInitialConnWindowSize(staticWindow),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	client := embedpb.NewShoalEmbedClient(conn)

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer setupCancel()
	if _, err := client.CreateTable(setupCtx, &embedpb.CreateTableRequest{Table: "t"}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// rows*valueSize is several times staticWindow, so once the client stops
	// reading, the handler's remaining Send calls block on flow control
	// well before finishing — a real in-flight RPC, not a race.
	const rows, valueSize = 100, 8192
	value := []byte(strings.Repeat("v", valueSize))
	muts := make([]*embedpb.Mutation, rows)
	for i := 0; i < rows; i++ {
		muts[i] = &embedpb.Mutation{
			Row: []byte(fmt.Sprintf("row-%04d", i)),
			Entries: []*embedpb.Entry{{
				ColumnFamily:    []byte("cf"),
				ColumnQualifier: []byte("cq"),
				Timestamp:       1,
				Value:           value,
			}},
		}
	}
	if _, err := client.Write(setupCtx, &embedpb.WriteRequest{Table: "t", Mutations: muts}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	scanCtx, scanCancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(scanCancel)
	stream, err := client.Scan(scanCtx, &embedpb.ScanRequest{Table: "t", BatchSize: 1})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first Scan Recv: %v", err)
	}
	// Deliberately never call Recv again: give the still-running handler
	// goroutine time to fill the pinned window and block in Send.
	time.Sleep(200 * time.Millisecond)
}

// TestServeHandleStopIsBoundedByDrainTimeout guards a real bug found while
// implementing Stop: grpc.Server.GracefulStop blocks until every in-flight
// RPC finishes and accepts no deadline of its own, so a single slow or
// stuck client could otherwise hang Stop — and shutdown — forever,
// regardless of --drain-timeout. It starts a real streaming Scan RPC over a
// client pinned to a small static (non-BDP-adaptive) flow-control window,
// deliberately stops reading after the first message so the still-running
// handler blocks on backpressure trying to send the rest, then asserts Stop
// still returns close to the short deadline it was given instead of
// hanging until the client resumes reading (it never does here).
func TestServeHandleStopIsBoundedByDrainTimeout(t *testing.T) {
	h, serveErrCh := newTestServeHandle(t)
	startStuckStreamingScan(t, h)

	h.Drain()
	const drainDeadline = 150 * time.Millisecond
	stopCtx, stopCancel := context.WithTimeout(context.Background(), drainDeadline)
	defer stopCancel()

	start := time.Now()
	h.Stop(stopCtx)
	elapsed := time.Since(start)

	// Without the force-stop fallback this would hang until the client
	// resumed reading (it never does) or the process was killed. A bounded
	// Stop must return within a small multiple of its deadline instead.
	if elapsed > 2*time.Second {
		t.Errorf("Stop(ctx) took %v with a %v deadline; want bounded (force-stop fallback did not engage)", elapsed, drainDeadline)
	}

	select {
	case <-serveErrCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return within 5s of a forced Stop")
	}
}

// TestRunUntilSignalWaitsForFullShutdownBeforeReturning guards the critical
// bug the Copilot review's "suppressed" finding identified: the pre-fix
// main.go reacted to Serve() returning by letting main() return right
// behind it, from a goroutine that ran the drain/stop sequence but was
// never joined — so the process could exit mid-drain, before eng.Close
// ever ran. grpc.Server.Serve's return is driven entirely by the gRPC
// server's own internal bookkeeping and is independent of the HTTP side
// and of Stop's own subsequent work (waiting for the HTTP shutdown to
// finish, then closing the engine) — with an otherwise-idle gRPC server,
// Serve can return within microseconds of Stop being called. This test
// makes that gap observable: it swaps in a test-controlled HTTP handler
// that blocks a request indefinitely (via the unexported httpSrv field —
// this file is in package main), which deterministically forces Stop's
// httpSrv.Shutdown to block for the full --drain-timeout budget while the
// idle gRPC server has nothing to wait for. If RunUntilSignal returned as
// soon as Serve unblocked (the bug) rather than waiting for the whole
// shutdown sequence — including that HTTP wait and the subsequent engine
// close — to finish, it would return in microseconds instead of waiting
// out the deadline.
func TestRunUntilSignalWaitsForFullShutdownBeforeReturning(t *testing.T) {
	h := newRawTestServeHandle(t)

	orig := h.httpSrv.Handler
	requestReceived := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseFn()
	h.httpSrv.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__test_slow__" {
			close(requestReceived)
			<-release
			w.WriteHeader(http.StatusOK)
			return
		}
		orig.ServeHTTP(w, r)
	})

	sigCh := make(chan os.Signal, 1)
	const drainDeadline = 150 * time.Millisecond

	runErrCh := make(chan error, 1)
	go func() { runErrCh <- h.RunUntilSignal(sigCh, 0, drainDeadline) }()

	// Wait for RunUntilSignal's internal Serve call to actually start
	// accepting before using the server or sending the signal.
	waitForStatus(t, "http://"+h.MetricsAddr+"/readyz", http.StatusOK).Body.Close()

	// Start (and deliberately never finish, until releaseFn is called) the
	// slow request so its connection stays "active" — never idle — from
	// net/http's point of view.
	slowDone := make(chan struct{})
	go func() {
		resp, err := http.Get("http://" + h.MetricsAddr + "/__test_slow__")
		if err == nil {
			resp.Body.Close()
		}
		close(slowDone)
	}()
	<-requestReceived

	start := time.Now()
	sigCh <- syscall.SIGTERM

	select {
	case err := <-runErrCh:
		elapsed := time.Since(start)
		if err != nil {
			t.Errorf("RunUntilSignal returned %v, want nil after a clean shutdown", err)
		}
		// The stuck request forces Stop's httpDone.Wait() to block until
		// ctx's drainDeadline expires and httpSrv.Close() force-closes it.
		// If RunUntilSignal returned as soon as the (otherwise idle) gRPC
		// server's Serve call unblocked (the bug) rather than waiting for
		// the whole shutdown sequence to finish, elapsed would be far
		// under drainDeadline instead.
		if elapsed < drainDeadline {
			t.Errorf("RunUntilSignal returned after %v, want at least the %v drain deadline (returned before the shutdown sequence, including engine close, finished)", elapsed, drainDeadline)
		}
	case <-time.After(5 * time.Second):
		releaseFn()
		t.Fatal("RunUntilSignal did not return within 5s of SIGTERM")
	}

	releaseFn()
	<-slowDone

	if _, err := http.Get("http://" + h.MetricsAddr + "/readyz"); err == nil {
		t.Error("GET /readyz succeeded after RunUntilSignal returned, want connection error (listener closed)")
	}
}

// TestRunUntilSignalAppliesQuiesceDelay proves the quiesce pause runs
// strictly between Drain and Stop: /readyz must already report not-ready,
// while the gRPC port must still accept a brand new connection and RPC,
// throughout the quiesce window. That ordering is what actually gives a
// readiness-polling consumer a chance to react before anything stops
// accepting work — a quiesce implemented after Stop (or not at all) would
// fail this.
func TestRunUntilSignalAppliesQuiesceDelay(t *testing.T) {
	h := newRawTestServeHandle(t)
	sigCh := make(chan os.Signal, 1)
	const quiesce = 1500 * time.Millisecond
	const drainDeadline = 5 * time.Second

	runErrCh := make(chan error, 1)
	go func() { runErrCh <- h.RunUntilSignal(sigCh, quiesce, drainDeadline) }()
	waitForStatus(t, "http://"+h.MetricsAddr+"/readyz", http.StatusOK).Body.Close()

	sigCh <- syscall.SIGTERM

	// Drain runs synchronously as soon as the signal is received, before
	// the quiesce sleep — so /readyz should flip to 503 almost
	// immediately. Poll briefly, well inside the quiesce window, rather
	// than reusing the shared 5s-deadline waitForStatus helper: a slow
	// poll here must not itself eat into the window the rest of this test
	// depends on.
	pollDeadline := time.Now().Add(250 * time.Millisecond)
	var readyzStatus int
	for time.Now().Before(pollDeadline) {
		resp, err := http.Get("http://" + h.MetricsAddr + "/readyz")
		if err == nil {
			readyzStatus = resp.StatusCode
			resp.Body.Close()
			if readyzStatus == http.StatusServiceUnavailable {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if readyzStatus != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d shortly after SIGTERM, want 503 (Drain runs before the quiesce sleep)", readyzStatus)
	}

	// Still well inside the quiesce window: the gRPC listener must still be
	// open and accept a brand new connection and RPC, since Stop (which
	// closes it) only runs once quiesce elapses.
	conn, err := grpc.NewClient(h.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient during quiesce window: %v", err)
	}
	defer conn.Close()
	client := embedpb.NewShoalEmbedClient(conn)
	rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer rpcCancel()
	if _, err := client.Status(rpcCtx, &embedpb.StatusRequest{}); err != nil {
		t.Errorf("Status RPC during quiesce window: %v, want gRPC port still accepting connections", err)
	}

	select {
	case err := <-runErrCh:
		if err != nil {
			t.Errorf("RunUntilSignal returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunUntilSignal did not return within 5s")
	}
}

// TestUnaryInFlightInterceptorTracksActiveCalls is a direct unit test of
// unaryInFlight's counting logic — no real gRPC transport needed — proving
// the count reflects exactly the handlers currently running, which is the
// signal Stop's deadline-exceeded log line depends on for the Write/Flush/
// Compact case it can't otherwise detect.
func TestUnaryInFlightInterceptorTracksActiveCalls(t *testing.T) {
	var u unaryInFlight
	if got := u.count(); got != 0 {
		t.Fatalf("count() before any calls = %d, want 0", got)
	}

	handlerEntered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		_, _ = u.intercept(context.Background(), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, req any) (any, error) {
			close(handlerEntered)
			<-release
			return nil, nil
		})
		close(done)
	}()

	<-handlerEntered
	if got := u.count(); got != 1 {
		t.Errorf("count() while handler is running = %d, want 1", got)
	}

	close(release)
	<-done

	if got := u.count(); got != 0 {
		t.Errorf("count() after handler returns = %d, want 0", got)
	}
}
