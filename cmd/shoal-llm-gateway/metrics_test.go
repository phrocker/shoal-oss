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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	admissionapi "github.com/phrocker/shoal-oss/pkg/admission/api"
)

func TestMetricsAndCompletionsSurfacesStaySeparate(t *testing.T) {
	plane := newFakePlane(t, admissionapi.OutcomeAllowed, nil)
	upstream := newFakeUpstream(t)
	governed, _ := newTestProxy(t, plane, upstream)

	for _, surface := range []struct {
		name    string
		handler http.Handler
		method  string
		path    string
	}{
		{"completions does not serve metrics", governed.routes(), http.MethodGet, "/metrics"},
		{"metrics does not serve completions", governed.metricsHandler(), http.MethodPost, "/v1/chat/completions"},
	} {
		t.Run(surface.name, func(t *testing.T) {
			request := httptest.NewRequest(surface.method, surface.path, nil)
			request.Host = "example.test"
			response := httptest.NewRecorder()
			surface.handler.ServeHTTP(response, request)
			if response.Code != http.StatusNotFound {
				t.Fatalf("%s %s = %d, want 404", surface.method, surface.path, response.Code)
			}
		})
	}
}

func TestMetricsExposeEveryFixedOutcomeClass(t *testing.T) {
	governed := &proxy{metrics: &proxyMetrics{}}
	for outcome := range outcomeNames {
		governed.recordOutcome(outcomeClass(outcome))
	}
	response := httptest.NewRecorder()
	governed.metricsHandler().ServeHTTP(
		response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, outcome := range outcomeNames {
		if !strings.Contains(response.Body.String(),
			`outcome="`+outcome+`"} 1`) {
			t.Errorf("metrics omitted or failed to count %q:\n%s",
				outcome, response.Body.String())
		}
	}
}

func TestMetricsAddressStartsAnIndependentListener(t *testing.T) {
	originalListen := listenTCP
	listeners := make(chan net.Listener, 2)
	listenTCP = func(network, address string) (net.Listener, error) {
		listener, err := originalListen(network, address)
		if err == nil {
			listeners <- listener
		}
		return listener, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	finished := false
	go func() {
		done <- run(ctx, proxyArgs(
			"-listen", "127.0.0.1:0",
			"-allowed-host", "example.test",
			"-metrics-address", "127.0.0.1:0",
		), io.Discard)
	}()
	defer func() {
		cancel()
		if !finished {
			<-done
		}
		listenTCP = originalListen
	}()

	var requestListener, metricsListener net.Listener
	for i := 0; i < 2; i++ {
		select {
		case listener := <-listeners:
			if i == 0 {
				requestListener = listener
			} else {
				metricsListener = listener
			}
		case <-time.After(3 * time.Second):
			t.Fatal("gateway did not start both listeners")
		}
	}
	if requestListener.Addr().String() == metricsListener.Addr().String() {
		t.Fatalf("listeners share address %s", requestListener.Addr())
	}

	for _, probe := range []struct {
		name     string
		listener net.Listener
		method   string
		path     string
		want     int
	}{
		{"scrape on metrics listener", metricsListener, http.MethodGet, "/metrics", http.StatusOK},
		{"completions route absent from metrics listener", metricsListener, http.MethodPost, "/v1/chat/completions", http.StatusNotFound},
		{"metrics route absent from completions listener", requestListener, http.MethodGet, "/metrics", http.StatusNotFound},
	} {
		t.Run(probe.name, func(t *testing.T) {
			request, err := http.NewRequest(
				probe.method, "http://"+probe.listener.Addr().String()+probe.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Host = "example.test"
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != probe.want {
				t.Fatalf("status = %d, want %d", response.StatusCode, probe.want)
			}
		})
	}

	cancel()
	select {
	case err := <-done:
		finished = true
		if err != nil {
			t.Fatalf("gateway shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("gateway did not shut down")
	}
	for _, listener := range []net.Listener{requestListener, metricsListener} {
		connection, err := net.DialTimeout("tcp", listener.Addr().String(), 100*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			t.Fatalf("listener %s remained open after shutdown", listener.Addr())
		}
	}
}

func TestMetricsCountFixedOutcomeClassesAndReportFailures(t *testing.T) {
	tests := []struct {
		name     string
		plane    func(*fakePlane)
		upstream func(*fakeUpstream)
		body     string
		want     string
	}{
		{
			name:     "allowed",
			plane:    func(*fakePlane) {},
			upstream: func(*fakeUpstream) {},
			body:     plainCall,
			want:     `outcome="allowed"} 1`,
		},
		{
			name:     "policy denied",
			plane:    func(plane *fakePlane) { plane.outcome = admissionapi.OutcomeDenied },
			upstream: func(*fakeUpstream) {},
			body:     plainCall,
			want:     `outcome="policy_denied"} 1`,
		},
		{
			name:     "plane unreachable",
			plane:    func(plane *fakePlane) { plane.status = http.StatusServiceUnavailable },
			upstream: func(*fakeUpstream) {},
			body:     plainCall,
			want:     `outcome="plane_unreachable"} 1`,
		},
		{
			name: "obligation unsatisfiable",
			plane: func(plane *fakePlane) {
				plane.outcome = admissionapi.OutcomeObligated
				plane.withhold = []string{docNeverDeclared}
			},
			upstream: func(*fakeUpstream) {},
			body: `{"model":"gpt","messages":[{"role":"user","content":"hi"}],` +
				`"shoal_references":["` + docA + `"]}`,
			want: `outcome="obligation_unsatisfiable"} 1`,
		},
		{
			name:     "upstream failed",
			plane:    func(*fakePlane) {},
			upstream: func(upstream *fakeUpstream) { upstream.status = http.StatusBadGateway },
			body:     plainCall,
			want:     `outcome="upstream_failed"} 1`,
		},
		{
			name:     "report failed",
			plane:    func(plane *fakePlane) { plane.reportStatus = http.StatusInternalServerError },
			upstream: func(*fakeUpstream) {},
			body:     plainCall,
			want:     `shoal_llm_gateway_report_failures_total 1`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plane := newFakePlane(t, admissionapi.OutcomeAllowed, nil)
			test.plane(plane)
			upstream := newFakeUpstream(t)
			test.upstream(upstream)
			governed, _ := newTestProxy(t, plane, upstream)
			post(t, governed, test.body)

			response := httptest.NewRecorder()
			governed.metricsHandler().ServeHTTP(
				response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if response.Code != http.StatusOK {
				t.Fatalf("metrics status = %d", response.Code)
			}
			if !strings.Contains(response.Body.String(), test.want) {
				t.Fatalf("metrics missing %q:\n%s", test.want, response.Body.String())
			}
			for _, content := range []string{"hello", docA, docNeverDeclared} {
				if strings.Contains(response.Body.String(), content) {
					t.Fatalf("metrics exposed caller or policy content %q: %s",
						content, response.Body.String())
				}
			}
		})
	}
}
