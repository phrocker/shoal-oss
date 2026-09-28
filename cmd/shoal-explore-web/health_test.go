// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestHealthSurfaceAnswersTheProbeTheWorkspaceRefuses is the reason this
// listener exists. The workspace handler refuses any request whose Host is not
// an exactly configured authority, and an orchestrator probe addresses the pod
// by an authority no static configuration can name. Here -allowed-host names an
// external authority, as a real deployment behind a Service must, and the probe
// arrives addressed to the socket instead.
//
// The two assertions are one claim: the same request that the workspace port
// answers 421 is answered 200 by the health port.
func TestHealthSurfaceAnswersTheProbeTheWorkspaceRefuses(t *testing.T) {
	workspaceURL, healthURL, stop := startWorkspaceWithHealth(t,
		"-allowed-host", "shoal.example.test")
	defer stop()

	workspaceHost := strings.TrimPrefix(workspaceURL, "http://")
	if status := probeStatus(t, workspaceURL+"/api/v1/auth-config", workspaceHost); status != http.StatusMisdirectedRequest {
		t.Fatalf("workspace port answered a socket-addressed probe with %d, want %d: "+
			"the host-authority gate is not refusing it, so this test no longer "+
			"demonstrates why the health listener exists",
			status, http.StatusMisdirectedRequest)
	}
	healthHost := strings.TrimPrefix(healthURL, "http://")
	if status := probeStatus(t, healthURL+"/readyz", healthHost); status != http.StatusOK {
		t.Fatalf("health port answered a socket-addressed probe with %d, want 200", status)
	}
}

// TestHealthSurfaceServesOnlyItsTwoRoutes pins the surface closed. A path that
// exists on the workspace handler must not exist here, or the health listener
// becomes an unauthenticated way to reach the workspace.
func TestHealthSurfaceServesOnlyItsTwoRoutes(t *testing.T) {
	_, healthURL, stop := startWorkspaceWithHealth(t)
	defer stop()

	host := strings.TrimPrefix(healthURL, "http://")
	for _, path := range []string{
		"/", "/api/v1/meta", "/api/v1/auth-config", "/api/v1/documents",
		"/assets/app.js", "/healthz/../api/v1/meta",
	} {
		if status := probeStatus(t, healthURL+path, host); status != http.StatusNotFound {
			t.Fatalf("health port served %q with %d, want 404", path, status)
		}
	}
}

// TestHealthSurfaceIsAbsentUnlessRequested keeps the default unchanged: an
// operator who does not ask for a second listener does not get one.
func TestHealthSurfaceIsAbsentUnlessRequested(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output := &lockedBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{
			"-data", t.TempDir(), "-listen", "127.0.0.1:0", "-dev-auth",
		}, output)
	}()
	waitForListeningURL(t, output)
	if strings.Contains(output.String(), "Health surface") {
		t.Fatalf("health surface started without -health-address: %q", output.String())
	}
	cancel()
	waitForRunToReturn(t, done)
}

// TestHealthAddressBindFailureIsFatal proves the failure is reported rather
// than degraded into a workspace that serves while every probe is refused
// because nothing is listening.
func TestHealthAddressBindFailureIsFatal(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{
			"-data", t.TempDir(), "-listen", "127.0.0.1:0", "-dev-auth",
			"-health-address", occupied.Addr().String(),
		}, &lockedBuffer{})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run returned nil for an unbindable health address")
		}
		if !strings.Contains(err.Error(), occupied.Addr().String()) {
			t.Fatalf("error does not name the address that failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run kept serving with no health listener")
	}
}

// TestHealthHandlerSeparatesLivenessFromReadiness covers the three states the
// integration tests cannot hold still: not-yet-ready, ready, and draining.
//
// The draining case is the one that matters operationally. Readiness must drop
// so the endpoints controller stops routing new work, while liveness must not,
// because a pod shedding traffic on purpose has not failed and restarting it
// throws away the graceful close.
func TestHealthHandlerSeparatesLivenessFromReadiness(t *testing.T) {
	state := &healthState{}
	handler := newHealthHandler(state)

	for _, probe := range []struct {
		name       string
		transition func()
		readiness  int
		liveness   int
	}{
		{"before serving", func() {}, http.StatusServiceUnavailable, http.StatusOK},
		{"serving", state.markReady, http.StatusOK, http.StatusOK},
		{"draining", state.markDraining, http.StatusServiceUnavailable, http.StatusOK},
	} {
		probe.transition()
		if got := handlerStatus(t, handler, "/readyz"); got != probe.readiness {
			t.Fatalf("%s: /readyz = %d, want %d", probe.name, got, probe.readiness)
		}
		if got := handlerStatus(t, handler, "/healthz"); got != probe.liveness {
			t.Fatalf("%s: /healthz = %d, want %d", probe.name, got, probe.liveness)
		}
	}
}

// TestHealthResponsesCarryNoWorkspaceState pins the disclosure property: the
// bodies are fixed strings. A future readiness check that consults the corpus
// or the policy catalog must not reach an unauthenticated prober through here.
func TestHealthResponsesCarryNoWorkspaceState(t *testing.T) {
	state := &healthState{}
	state.markReady()
	handler := newHealthHandler(state)
	for path, want := range map[string]string{"/healthz": "ok\n", "/readyz": "ready\n"} {
		recorder := recordHealth(t, handler, path)
		if body := recorder.body; body != want {
			t.Fatalf("%s body = %q, want %q", path, body, want)
		}
		if got := recorder.header.Get("Content-Type"); got != "text/plain; charset=utf-8" {
			t.Fatalf("%s content type = %q", path, got)
		}
	}
}

// TestDrainDropsReadinessBeforeTheWorkspaceStopsAccepting tests the wiring,
// not the bit. TestHealthHandlerSeparatesLivenessFromReadiness sets the state
// by hand and would keep passing if shutdown never touched it; this asserts the
// order the shutdown path actually executes, by reading readiness at the moment
// the workspace is asked to stop.
func TestDrainDropsReadinessBeforeTheWorkspaceStopsAccepting(t *testing.T) {
	state := &healthState{}
	state.markReady()
	workspace := &recordingServer{state: state}

	if err := drain(context.Background(), state, workspace, nil); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !workspace.calledShutdown {
		t.Fatal("drain never shut the workspace down")
	}
	if workspace.readyAtShutdown {
		t.Fatal("the workspace was shut down while still reporting ready: " +
			"the endpoints controller had no not-ready reading to act on, so " +
			"requests routed during the gap arrive at a closed listener")
	}
}

// TestDrainReportsTheWorkspaceErrorOverTheHealthClose pins which failure wins.
func TestDrainReportsTheWorkspaceErrorOverTheHealthClose(t *testing.T) {
	state := &healthState{}
	state.markReady()
	want := errors.New("workspace close failed")
	err := drain(context.Background(), state, &recordingServer{state: state, err: want}, nil)
	if !errors.Is(err, want) {
		t.Fatalf("drain error = %v, want %v", err, want)
	}
}

type recordingServer struct {
	state           *healthState
	err             error
	calledShutdown  bool
	readyAtShutdown bool
}

func (s *recordingServer) Shutdown(context.Context) error {
	s.calledShutdown = true
	s.readyAtShutdown = s.state.ready.Load()
	return s.err
}

// TestHealthServerReportsAServeLoopThatDiedOnItsOwn covers the failure the
// roleops shutdown path had (#382): a serve loop that returns an unexpected
// error, where the error was consumed by whichever reader won a race and never
// reached the caller.
//
// Closing the listener directly, rather than through Shutdown, is what makes
// the error survive: once Shutdown has been called, net/http reports every
// subsequent accept failure as ErrServerClosed, which is exactly the graceful
// case this must not be confused with.
func TestHealthServerReportsAServeLoopThatDiedOnItsOwn(t *testing.T) {
	health, err := startHealthServer("127.0.0.1:0", &healthState{})
	if err != nil {
		t.Fatal(err)
	}
	if err := health.listener.Close(); err != nil {
		t.Fatal(err)
	}
	<-health.done

	shutdownErr := health.shutdown(context.Background())
	if shutdownErr == nil {
		t.Fatal("shutdown reported success for a serve loop that failed: " +
			"the error was recorded by the goroutine and never returned")
	}
	if errors.Is(shutdownErr, http.ErrServerClosed) {
		t.Fatalf("shutdown reported the graceful close instead of the failure: %v", shutdownErr)
	}
}

// TestHealthShutdownJoinsTheServeLoop is the other half, and the one the test
// above cannot make: that shutdown waits for the goroutine rather than reading
// a field the goroutine has not written yet.
//
// The listener blocks in Accept for a fixed delay, so the serve loop is
// provably still running when Shutdown returns. A shutdown that joins cannot
// return until the loop has finished; one that does not returns while the
// goroutine is still in flight, which is both a lost error and a data race on
// serveErr.
func TestHealthShutdownJoinsTheServeLoop(t *testing.T) {
	restore := stubHealthListener(t, &slowFailingListener{
		delay: 250 * time.Millisecond, err: errors.New("health listener failed"),
	})
	defer restore()

	health, err := startHealthServer("127.0.0.1:0", &healthState{})
	if err != nil {
		t.Fatal(err)
	}
	if err := health.shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-health.done:
	default:
		t.Fatal("shutdown returned while the serve loop was still running: " +
			"serveErr is read without waiting for the goroutine that writes it")
	}
}

// slowFailingListener blocks in Accept for a fixed delay and then fails. The
// error is a plain error rather than a net.Error, so net/http treats it as
// fatal and returns instead of retrying.
type slowFailingListener struct {
	delay time.Duration
	err   error
}

func (l *slowFailingListener) Accept() (net.Conn, error) {
	time.Sleep(l.delay)
	return nil, l.err
}
func (l *slowFailingListener) Close() error   { return nil }
func (l *slowFailingListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

func stubHealthListener(t *testing.T, listener net.Listener) func() {
	t.Helper()
	previous := listenHealthTCP
	listenHealthTCP = func(string, string) (net.Listener, error) { return listener, nil }
	return func() { listenHealthTCP = previous }
}

var healthSurfacePattern = regexp.MustCompile(`Health surface listening at (http://\S+)`)

// startWorkspaceWithHealth runs a workspace with both listeners on ephemeral
// ports and returns their URLs plus a stop function that waits for a clean
// shutdown.
func startWorkspaceWithHealth(t *testing.T, extraArgs ...string) (string, string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	output := &lockedBuffer{}
	done := make(chan error, 1)
	args := append([]string{
		"-data", t.TempDir(), "-listen", "127.0.0.1:0", "-dev-auth",
		"-health-address", "127.0.0.1:0",
	}, extraArgs...)
	go func() { done <- run(ctx, args, output) }()

	workspaceURL := waitForListeningURL(t, output)
	healthURL := ""
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if match := healthSurfacePattern.FindStringSubmatch(output.String()); match != nil {
			healthURL = match[1]
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if healthURL == "" {
		cancel()
		t.Fatalf("health surface never reported an address: %q", output.String())
	}
	return workspaceURL, healthURL, func() {
		cancel()
		waitForRunToReturn(t, done)
	}
}

func waitForRunToReturn(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return")
	}
}

// probeStatus issues a GET with an explicit Host, which is how a kubelet probe
// differs from a browser request: the authority names the socket rather than
// the external name the deployment is configured for.
func probeStatus(t *testing.T, url, host string) int {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = host
	client := http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("GET %s with Host %s: %v", url, host, err)
	}
	defer response.Body.Close()
	return response.StatusCode
}

type healthRecorder struct {
	status int
	body   string
	header http.Header
}

func recordHealth(t *testing.T, handler http.Handler, path string) healthRecorder {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()

	response, err := http.Get(fmt.Sprintf("http://%s%s", listener.Addr(), path))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body := make([]byte, 64)
	read, _ := response.Body.Read(body)
	return healthRecorder{
		status: response.StatusCode,
		body:   string(body[:read]),
		header: response.Header,
	}
}

func handlerStatus(t *testing.T, handler http.Handler, path string) int {
	t.Helper()
	return recordHealth(t, handler, path).status
}
