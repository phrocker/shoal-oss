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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an
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
	"sync/atomic"
	"time"

	"github.com/phrocker/shoal-oss/internal/healthsurface"
)

type outcomeClass int

const (
	outcomeAllowed outcomeClass = iota
	outcomeAllowedWithObligations
	outcomePolicyDenied
	outcomePlaneUnreachable
	outcomeObligationUnsatisfiable
	outcomeUpstreamFailed
	outcomeClassCount
)

var outcomeNames = [...]string{
	"allowed",
	"allowed_with_obligations",
	"policy_denied",
	"plane_unreachable",
	"obligation_unsatisfiable",
	"upstream_failed",
}

type proxyMetrics struct {
	outcomes       [outcomeClassCount]atomic.Uint64
	reportFailures atomic.Uint64
}

func (p *proxy) recordOutcome(outcome outcomeClass) {
	if p.metrics != nil && outcome >= 0 && outcome < outcomeClassCount {
		p.metrics.outcomes[outcome].Add(1)
	}
}

func (p *proxy) recordReportFailure() {
	if p.metrics != nil {
		p.metrics.reportFailures.Add(1)
	}
}

func (p *proxy) metricsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		metrics := p.metrics
		if metrics == nil {
			metrics = &proxyMetrics{}
		}
		_, _ = fmt.Fprintln(writer, "# HELP shoal_llm_gateway_requests_total Requests by fixed terminal outcome class.")
		_, _ = fmt.Fprintln(writer, "# TYPE shoal_llm_gateway_requests_total counter")
		for outcome, name := range outcomeNames {
			_, _ = fmt.Fprintf(writer,
				"shoal_llm_gateway_requests_total{outcome=%q} %d\n",
				name, metrics.outcomes[outcome].Load())
		}
		_, _ = fmt.Fprintln(writer, "# HELP shoal_llm_gateway_report_failures_total Admission reports the decision plane did not acknowledge.")
		_, _ = fmt.Fprintln(writer, "# TYPE shoal_llm_gateway_report_failures_total counter")
		_, _ = fmt.Fprintf(writer, "shoal_llm_gateway_report_failures_total %d\n",
			metrics.reportFailures.Load())
	})
	return mux
}

type metricsHTTPServer struct {
	listener net.Listener
	server   *http.Server
	done     chan struct{}
	serveErr error
}

func startMetricsServer(address string, handler http.Handler) (*metricsHTTPServer, error) {
	listener, err := listenTCP("tcp", address)
	if err != nil {
		return nil, err
	}
	server := &metricsHTTPServer{
		listener: listener,
		server: &http.Server{
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       30 * time.Second,
		},
		done: make(chan struct{}),
	}
	go func() {
		defer close(server.done)
		if err := server.server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			server.serveErr = err
		}
	}()
	return server, nil
}

func (s *metricsHTTPServer) Address() string {
	if s == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *metricsHTTPServer) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	closeErr := s.server.Shutdown(ctx)
	<-s.done
	if s.serveErr != nil {
		return s.serveErr
	}
	return closeErr
}

var _ healthsurface.GracefulServer = (*metricsHTTPServer)(nil)
