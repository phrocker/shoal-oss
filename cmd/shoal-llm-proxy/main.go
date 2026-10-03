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

// Command shoal-llm-proxy governs an OpenAI-compatible endpoint by asking Shoal
// before each call and reporting what happened after it.
//
// It needs no cooperation from the caller: anything that speaks the API is
// governed by changing one base URL. That is the property nothing else in Shoal
// has — /api/v1/ask requires a caller built for Shoal.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/phrocker/shoal-oss/internal/healthsurface"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "shoal-llm-proxy: %v\n", err)
		os.Exit(1)
	}
}

var listenTCP = net.Listen

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("shoal-llm-proxy", flag.ContinueOnError)
	flags.SetOutput(output)
	listen := flags.String("listen", "127.0.0.1:8100",
		"OpenAI-compatible listen address")
	healthAddress := flags.String("health-address", "",
		"Optional separate listener serving GET /healthz and GET /readyz for "+
			"orchestrator probes. Empty disables it. It is a second listener "+
			"because the request surface answers only the completions route "+
			"and a probe must not be mistaken for a call")
	admissionURL := flags.String("admission-url", "",
		"Base URL of the Shoal workspace whose admission surface decides each "+
			"call. Required: without a decision plane this is a plain relay, "+
			"and a relay that cannot be told apart from an enforcement point "+
			"is worse than no enforcement point")
	admissionTokenEnv := flags.String("admission-token-env", "SHOAL_ADMISSION_TOKEN",
		"Environment variable read at request time holding the bearer token "+
			"this proxy presents to the workspace")
	upstreamBaseURL := flags.String("upstream-base-url", "",
		"The real OpenAI-compatible provider this proxy forwards to")
	upstreamKeyEnv := flags.String("upstream-api-key-env", "SHOAL_UPSTREAM_API_KEY",
		"Environment variable read at request time holding the upstream credential")
	agentID := flags.String("agent-id", "",
		"Registered descriptor this proxy admits against")
	agentGeneration := flags.Int64("agent-generation", 0,
		"Generation of the registered descriptor; must be positive")
	capability := flags.String("capability", "",
		"Registered capability name")
	action := flags.String("action", "",
		"Registered action name")
	sourceID := flags.String("source-id", "",
		"Scope source ID, unpadded base64url")
	policyID := flags.String("policy-id", "",
		"Scope policy ID, unpadded base64url")
	lease := flags.Duration("lease", time.Minute,
		"Admission lease. A call must be reported within it or the grant shows "+
			"as outstanding")
	requestTimeout := flags.Duration("request-timeout", 2*time.Minute,
		"Upstream request timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*admissionURL) == "" {
		return errors.New("-admission-url is required")
	}
	if strings.TrimSpace(*upstreamBaseURL) == "" {
		return errors.New("-upstream-base-url is required")
	}
	if strings.TrimSpace(*agentID) == "" || *agentGeneration <= 0 {
		return errors.New("-agent-id and a positive -agent-generation are required")
	}
	if strings.TrimSpace(*capability) == "" || strings.TrimSpace(*action) == "" {
		return errors.New("-capability and -action are required")
	}
	if *lease <= 0 || *requestTimeout <= 0 {
		return errors.New("-lease and -request-timeout must be positive")
	}
	source, err := decodeID("-source-id", *sourceID)
	if err != nil {
		return err
	}
	policy, err := decodeID("-policy-id", *policyID)
	if err != nil {
		return err
	}
	base, err := absoluteURL(*admissionURL)
	if err != nil {
		return fmt.Errorf("-admission-url %v", err)
	}

	// Credentials are read per request, not captured at start. A rotated
	// secret then takes effect without a restart, and the value is never held
	// in the proxy's own state where a crash dump would carry it.
	logf := func(format string, values ...any) {
		fmt.Fprintf(output, format+"\n", values...)
	}
	admission := &admissionClient{
		base:       base,
		http:       &http.Client{Timeout: *requestTimeout},
		credential: credentialFromEnv(*admissionTokenEnv),
		agentID:    *agentID, agentGeneration: *agentGeneration,
		capability: *capability, action: *action,
		sourceID: source, policyID: policy, lease: *lease,
	}
	governed, err := newProxy(
		admission, *upstreamBaseURL, credentialFromEnv(*upstreamKeyEnv),
		*requestTimeout, time.Now, logf)
	if err != nil {
		return err
	}

	listener, err := listenTCP("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listen, err)
	}
	server := &http.Server{
		Handler:           governed.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      0, // streamed responses have no useful write bound
		IdleTimeout:       60 * time.Second,
	}
	fmt.Fprintf(output, "Shoal LLM proxy listening at http://%s\n", listener.Addr())
	fmt.Fprintf(output, "Admitting against %s as %s/%s\n",
		base.String(), *capability, *action)

	state := &healthsurface.State{}
	health := (*healthsurface.Server)(nil)
	if address := strings.TrimSpace(*healthAddress); address != "" {
		health, err = healthsurface.Start(address, state)
		if err != nil {
			listener.Close()
			return fmt.Errorf("listen on %s: %w", address, err)
		}
		fmt.Fprintf(output, "Health surface listening at http://%s\n", health.Address())
	}

	shutdownDone := make(chan error, 1)
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shutdownDone <- healthsurface.Drain(shutdown, state, server, health)
	}()
	state.MarkReady()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return <-shutdownDone
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = healthsurface.Drain(shutdown, state, server, health)
	return err
}

// credentialFromEnv reads a secret at use time.
func credentialFromEnv(name string) func() (string, error) {
	return func() (string, error) {
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			return "", fmt.Errorf("%s is empty", name)
		}
		return value, nil
	}
}
