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

// Command shoal-explore-web serves the optional local Explorer workspace.
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
	"syscall"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/model"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "shoal-explore-web: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("shoal-explore-web", flag.ContinueOnError)
	backend := flags.String("backend", "embedded", "Explorer backend: embedded or remote")
	data := flags.String("data", ".shoal/explorer", "Explorer corpus directory")
	listen := flags.String("listen", "127.0.0.1:8080", "HTTP listen address")
	remote := flags.String("remote", "", "Remote Explorer web API URL for -backend remote")
	embeddingProvider := flags.String(
		"embedding-provider", "",
		"Optional embedded vector provider: fake, ollama, or openai",
	)
	embeddingModel := flags.String(
		"embedding-model", "",
		"Embedding model name for -embedding-provider",
	)
	embeddingBaseURL := flags.String(
		"embedding-base-url", "",
		"Embedding provider base URL for ollama/openai",
	)
	embeddingAPIKeyEnv := flags.String(
		"embedding-api-key-env", "OPENAI_API_KEY",
		"Environment variable read at request time for openai credentials",
	)
	embeddingDimensions := flags.Int(
		"embedding-dimensions", 0,
		"Fake embedding dimensions; zero uses the provider default",
	)
	if err := flags.Parse(args); err != nil {
		return err
	}

	embedding, err := embeddingConfig{
		provider:   *embeddingProvider,
		model:      *embeddingModel,
		baseURL:    *embeddingBaseURL,
		apiKeyEnv:  *embeddingAPIKeyEnv,
		dimensions: *embeddingDimensions,
	}.embedder()
	if err != nil {
		return err
	}
	service, cleanup, err := openService(*backend, *data, *remote, embedding)
	if err != nil {
		return err
	}
	defer cleanup()

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listen, err)
	}
	handler, err := webapi.NewHandler(service, listener.Addr().String())
	if err != nil {
		listener.Close()
		return err
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	fmt.Fprintf(output, "Shoal Explorer listening at http://%s\n", listener.Addr())
	shutdownDone := make(chan error, 1)
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownDone <- server.Shutdown(shutdown)
	}()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return <-shutdownDone
	}
	return err
}

type embeddingConfig struct {
	provider   string
	model      string
	baseURL    string
	apiKeyEnv  string
	dimensions int
}

func (c embeddingConfig) embedder() (model.Embedder, error) {
	switch c.provider {
	case "":
		return nil, nil
	case "fake":
		return model.FakeEmbedder{Dimensions: c.dimensions, Model: c.model}, nil
	case "ollama":
		return model.NewOllamaEmbedder(model.OllamaConfig{
			BaseURL: c.baseURL,
			Model:   c.model,
		})
	case "openai":
		return model.NewOpenAIEmbedder(model.OpenAIConfig{
			BaseURL:        c.baseURL,
			EmbeddingModel: c.model,
			Credentials:    envCredentialResolver(c.apiKeyEnv),
		})
	default:
		return nil, fmt.Errorf("unknown embedding provider %q", c.provider)
	}
}

type envCredentialResolver string

func (r envCredentialResolver) ResolveCredential(context.Context) ([]byte, error) {
	if r == "" {
		return nil, model.ErrInvalidConfig
	}
	value := os.Getenv(string(r))
	if value == "" {
		return nil, model.ErrCredential
	}
	return []byte(value), nil
}

func (r envCredentialResolver) CacheIdentity() (string, error) {
	if r == "" {
		return "", model.ErrInvalidConfig
	}
	return "env:" + string(r), nil
}

func openService(
	backend, data, remote string, embedder model.Embedder,
) (webapi.Service, func(), error) {
	switch backend {
	case "embedded":
		corpus, err := explorer.OpenWithOptions(data, explorer.Options{
			Embedder: embedder,
		})
		if err != nil {
			return nil, func() {}, err
		}
		service, err := webapi.NewEmbeddedService(corpus)
		if err != nil {
			corpus.Close()
			return nil, func() {}, err
		}
		return service, func() { corpus.Close() }, nil
	case "remote":
		service, err := webapi.NewRemoteService(remote, nil)
		if err != nil {
			return nil, func() {}, err
		}
		return service, func() {}, nil
	default:
		return nil, func() {}, fmt.Errorf("unknown backend %q", backend)
	}
}
