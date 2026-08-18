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
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/phrocker/shoal/internal/embedpb"
)

// newTestServeHandle starts a serveHandle bound to OS-assigned loopback
// ports and runs its accept loops in the background, returning the handle
// plus the channel Serve's return value will arrive on. It performs no
// teardown, so callers control the Drain/Stop sequence themselves.
func newTestServeHandle(t *testing.T) (*serveHandle, <-chan error) {
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

	const staticWindow = 128 * 1024 // bytes; disables BDP-based auto-growth.
	conn, err := grpc.NewClient(h.GRPCAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithInitialWindowSize(staticWindow),
		grpc.WithInitialConnWindowSize(staticWindow),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()
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
	defer scanCancel()
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
