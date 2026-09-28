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
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// The health surface is deliberately a second listener rather than two more
// routes on the workspace handler.
//
// The workspace handler refuses any request whose Host is not an exactly
// matching configured authority (webapi.Handler.ServeHTTP consults
// hostAuthority.permits before routing, authentication, or anything else). A
// kubelet probe addresses the pod by its runtime-assigned IP, which no
// statically configured authority list can name, so every probe against the
// workspace port answers 421 and the pod never becomes ready. Widening the
// authority list to admit the pod IP would weaken the control that exists to
// stop misdirected and DNS-rebound requests, to fix an operational problem.
//
// A separate listener keeps that control untouched. It also separates the two
// audiences: the workspace port serves callers who hold a decision, and the
// health port serves an orchestrator that holds nothing and must learn
// nothing. This mirrors the read fleet, which already answers /healthz and
// /readyz on its metrics listener rather than on its Thrift port.
//
// The surface discloses nothing. Both routes answer with a status code and a
// fixed string; neither consults the corpus, the policy catalog, the
// authenticator, or the request. An unauthenticated prober learns that a Shoal
// workspace process is up, which the open TCP port already tells it.

// healthState is the readiness bit shared between the serving path and the
// health handler. It is false until the workspace is actually serving and
// false again as soon as shutdown begins, so a draining pod is removed from
// Service endpoints before it stops accepting connections.
type healthState struct {
	ready atomic.Bool
}

func (s *healthState) markReady()    { s.ready.Store(true) }
func (s *healthState) markDraining() { s.ready.Store(false) }

// newHealthHandler builds the health mux. Any path other than the two routes
// is a 404 from the mux, so the surface cannot grow by accident.
func newHealthHandler(state *healthState) http.Handler {
	mux := http.NewServeMux()
	// Liveness answers for the process, not the workspace. It stays 200 while
	// draining: a pod that is shedding traffic on purpose must not be killed
	// and restarted for it.
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writeHealth(writer, http.StatusOK, "ok")
	})
	// Readiness answers for the workspace listener. It is 503 both before the
	// workspace serves and after shutdown begins.
	mux.HandleFunc("GET /readyz", func(writer http.ResponseWriter, _ *http.Request) {
		if !state.ready.Load() {
			writeHealth(writer, http.StatusServiceUnavailable, "not ready")
			return
		}
		writeHealth(writer, http.StatusOK, "ready")
	})
	return mux
}

func writeHealth(writer http.ResponseWriter, status int, body string) {
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(status)
	_, _ = writer.Write([]byte(body + "\n"))
}

// healthServer owns the health listener and its serve loop.
//
// done is closed exactly once, when the serve loop returns, and serveErr is
// written before that close. Shutdown therefore waits on a single signal and
// reads a settled value, rather than racing a watcher and a caller for the
// same error over a channel — the failure mode fixed in the roleops shutdown
// path (#382).
type healthServer struct {
	listener net.Listener
	server   *http.Server
	done     chan struct{}
	serveErr error
}

// startHealthServer binds the health address and begins serving.
//
// It is called only after the corpus, policy catalog and workspace handler are
// constructed. Binding later is the point: the health port accepting a
// connection at all already means construction finished, so a probe cannot see
// a ready-looking socket while the workspace is still opening its corpus. The
// workspace listener does not have that property, because it is bound early so
// an address the workspace may not serve is refused before the corpus opens.
func startHealthServer(address string, state *healthState) (*healthServer, error) {
	listener, err := listenHealthTCP("tcp", address)
	if err != nil {
		return nil, err
	}
	health := &healthServer{
		listener: listener,
		server: &http.Server{
			Handler:           newHealthHandler(state),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       30 * time.Second,
		},
		done: make(chan struct{}),
	}
	go func() {
		defer close(health.done)
		err := health.server.Serve(listener)
		if !errors.Is(err, http.ErrServerClosed) {
			health.serveErr = err
		}
	}()
	return health, nil
}

// listenHealthTCP is a variable so tests can prove a bind failure is reported
// rather than swallowed.
var listenHealthTCP = net.Listen

// address reports the resolved listen address, which differs from the
// requested one whenever the request named port zero.
func (h *healthServer) address() string {
	if h == nil {
		return ""
	}
	return h.listener.Addr().String()
}

// shutdown stops the health server and reports the first error that mattered:
// a serve loop that died on its own outranks a slow graceful close, because it
// is the one that says the surface stopped answering for a reason nobody asked
// for.
func (h *healthServer) shutdown(ctx context.Context) error {
	if h == nil {
		return nil
	}
	closeErr := h.server.Shutdown(ctx)
	<-h.done
	if h.serveErr != nil {
		return h.serveErr
	}
	return closeErr
}

// gracefulServer is the part of *http.Server that drain needs, so a test can
// observe the readiness bit at the moment the workspace is asked to stop.
type gracefulServer interface {
	Shutdown(context.Context) error
}

// drain runs the shutdown sequence in the order the readiness contract
// requires.
//
// Readiness drops first, before the workspace is asked to stop. That ordering
// is the whole point: the endpoints controller needs a not-ready reading to
// stop routing new requests here, and it can only get one while this process
// is still answering probes. Shutting the workspace down first would close the
// door on in-flight work that was routed during the gap.
//
// The health surface closes last, after the workspace has finished its
// graceful close, so the not-ready answer stays available for as long as there
// is a process to ask.
func drain(ctx context.Context, state *healthState, workspace gracefulServer, health *healthServer) error {
	state.markDraining()
	closeErr := workspace.Shutdown(ctx)
	if healthErr := health.shutdown(ctx); closeErr == nil {
		closeErr = healthErr
	}
	return closeErr
}
