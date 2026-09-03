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
	"path/filepath"
	"syscall"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
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

// listenTCP opens the workspace listener. It is a variable so tests can prove
// that a refused address is never bound, and that a listener whose resolved
// address is wider than the requested one is still refused and closed.
var listenTCP = net.Listen

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("shoal-explore-web", flag.ContinueOnError)
	backend := flags.String("backend", "embedded", "Explorer backend: embedded or remote")
	data := flags.String("data", ".shoal/explorer", "Explorer corpus directory")
	listen := flags.String("listen", "127.0.0.1:8080", "HTTP listen address")
	remote := flags.String("remote", "", "Remote Explorer web API URL for -backend remote")
	embeddingProvider := flags.String(
		"embedding-provider", "",
		"Optional embedded vector provider: fake, lexical, ollama, openai, or voyage",
	)
	embeddingModel := flags.String(
		"embedding-model", "",
		"Embedding model name for -embedding-provider",
	)
	embeddingBaseURL := flags.String(
		"embedding-base-url", "",
		"Embedding provider base URL for ollama/openai/voyage",
	)
	embeddingAPIKeyEnv := flags.String(
		"embedding-api-key-env", "OPENAI_API_KEY",
		"Environment variable read at request time for openai/voyage credentials",
	)
	embeddingDimensions := flags.Int(
		"embedding-dimensions", 0,
		"Embedding dimensions; required for ollama/openai, zero uses fake default",
	)
	developmentAuth := flags.Bool(
		"dev-auth", false,
		"Authenticate every request as a fixed development principal; "+
			"refused unless the resolved listen address is loopback-only",
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

	// The requested address is classified and refused before anything is
	// bound, so an address the workspace may not serve never opens a socket
	// and never prompts an operator to approve exposure the program has
	// already decided against.
	if _, err := selectAuthenticator(*developmentAuth, *listen, time.Now); err != nil {
		return err
	}
	listener, err := listenTCP("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *listen, err)
	}
	// Defence in depth. The resolved listener address is authoritative: it may
	// be wider than the requested one, and the check above may be incomplete.
	// Identity is decided from it, and a refusal closes the listener here,
	// before the corpus is opened and before any request can be served.
	authenticator, err := selectAuthenticator(
		*developmentAuth, listener.Addr().String(), time.Now)
	if err != nil {
		listener.Close()
		return err
	}
	authority := auth.NewAuthority()
	// The policy catalog is durable, so documents ingested by this build stay
	// authorized across restarts (issue #284). The gate below still allows a
	// development-only, one-time migration for a corpus whose documents were
	// ingested before the catalog was durable, for -dev-auth on loopback and
	// nothing else.
	backfill := newDevelopmentBackfill(
		authenticator, listener.Addr().String(), authority.Binder())
	opened, err := openService(ctx, serviceConfig{
		backend:  *backend,
		data:     *data,
		remote:   *remote,
		embedder: embedding,
		resolver: authority.Resolver(),
		clock:    time.Now,
		backfill: backfill,
	})
	if err != nil {
		listener.Close()
		return err
	}
	service, cleanup := opened.service, opened.close
	defer cleanup()

	handler, err := webapi.NewAuthenticatedHandler(
		service, listener.Addr().String(), authenticator, authority.Binder())
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
	if *developmentAuth {
		fmt.Fprintf(
			output,
			"Authenticating every request as development principal %s\n",
			developmentSubject,
		)
	}
	if *backend == "embedded" {
		if backfill != nil {
			fmt.Fprintf(
				output,
				"Granted %d pre-existing document(s) in %s to %s: a "+
					"development-only, one-time migration for -dev-auth on "+
					"loopback of documents ingested before the policy catalog "+
					"was durable. The catalog now persists, so these "+
					"registrations survive restarts (issue #284)\n",
				opened.backfilled, *data, developmentSubject,
			)
		} else {
			fmt.Fprintf(
				output,
				"Policy catalog is durable in %s: documents this build "+
					"ingests stay authorized across restarts. Documents "+
					"ingested before the catalog was durable stay hidden "+
					"until re-registered (issue #284)\n",
				policyStoreDir(*data),
			)
		}
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
	case "lexical":
		if c.dimensions <= 0 {
			return nil, fmt.Errorf("embedding dimensions are required for lexical")
		}
		return model.NewLexicalEmbedder(model.LexicalConfig{
			Dimensions: c.dimensions,
			Model:      c.model,
		})
	case "voyage":
		if c.dimensions <= 0 {
			return nil, fmt.Errorf("embedding dimensions are required for voyage")
		}
		return model.NewVoyageEmbedder(model.VoyageConfig{
			BaseURL:          c.baseURL,
			Model:            c.model,
			Dimensions:       c.dimensions,
			APICredentialEnv: c.apiKeyEnv,
		})
	case "ollama":
		if c.dimensions <= 0 {
			return nil, fmt.Errorf("embedding dimensions are required for ollama")
		}
		return model.NewOllamaEmbedder(model.OllamaConfig{
			BaseURL:    c.baseURL,
			Model:      c.model,
			Dimensions: c.dimensions,
		})
	case "openai":
		if c.dimensions <= 0 {
			return nil, fmt.Errorf("embedding dimensions are required for openai")
		}
		return model.NewOpenAIEmbedder(model.OpenAIConfig{
			BaseURL:             c.baseURL,
			EmbeddingModel:      c.model,
			EmbeddingDimensions: c.dimensions,
			Credentials:         envCredentialResolver(c.apiKeyEnv),
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

// serviceConfig carries the backend selection together with the trusted
// authorization dependencies every backend must enforce.
type serviceConfig struct {
	backend  string
	data     string
	remote   string
	embedder model.Embedder
	resolver auth.Resolver
	clock    func() time.Time
	// backfill is nil unless the development principal is serving a loopback
	// listener. See developmentBackfill and issue #284.
	backfill *developmentBackfill
}

// openedService is the constructed workspace service together with what the
// startup backfill registered, so the operator can be told exactly what the
// development principal was granted.
type openedService struct {
	service    webapi.Service
	backfilled int
	close      func()
}

func openService(
	ctx context.Context,
	config serviceConfig,
) (openedService, error) {
	closed := openedService{close: func() {}}
	switch config.backend {
	case "embedded":
		corpus, err := explorer.OpenWithOptions(config.data, explorer.Options{
			Embedder: config.embedder,
		})
		if err != nil {
			return closed, err
		}
		// The policy catalog is durable and lives in a sibling directory, not
		// a subdirectory of the corpus: the corpus engine treats every
		// subdirectory as a table, so nesting the store there would corrupt
		// table discovery. Keying it to the corpus path lets registrations
		// survive a restart alongside the documents they authorize (#284).
		store, err := authorized.OpenDurablePolicyStore(policyStoreDir(config.data))
		if err != nil {
			corpus.Close()
			return closed, err
		}
		client, err := authorizedClient(corpus, store, config.resolver, config.clock)
		if err != nil {
			store.Close()
			corpus.Close()
			return closed, err
		}
		// The development-only backfill migrates a corpus whose documents were
		// ingested before the policy catalog was durable: their authorization
		// registrations are absent until re-registered once. A failure here is
		// fatal by design: the workspace must not serve a corpus it could not
		// finish authorizing.
		backfilled, err := config.backfill.run(ctx, client)
		if err != nil {
			store.Close()
			corpus.Close()
			return closed, err
		}
		service, err := webapi.NewEmbeddedService(client)
		if err != nil {
			store.Close()
			corpus.Close()
			return closed, err
		}
		return openedService{
			service:    service,
			backfilled: backfilled,
			close: func() {
				store.Close()
				corpus.Close()
			},
		}, nil
	case "remote":
		// The remote backend forwards workspace calls to an upstream Explorer
		// web API over HTTP and has no way to carry the caller's decision
		// across that hop: webapi.RemoteService is a workspace service, not an
		// explorer.Client, so authorized.Client cannot wrap it, and no
		// on-the-wire representation of auth.Decision exists yet. Serving it
		// would mean authenticating at this edge and then calling upstream
		// with no identity at all. Refuse rather than leave that path open.
		return closed, fmt.Errorf(
			"backend remote is unavailable: forwarding the caller's " +
				"authorization decision to an upstream Explorer is not " +
				"implemented, so the upstream call would carry no identity " +
				"(see issue #278, edge identity)")
	default:
		return closed, fmt.Errorf("unknown backend %q", config.backend)
	}
}

// policyStoreDir derives the durable policy catalog's directory from the corpus
// data directory. It is a sibling rather than a child because the corpus engine
// treats every subdirectory of the data directory as a table.
func policyStoreDir(data string) string {
	return filepath.Clean(data) + "-policy"
}

// authorizedClient wraps the corpus in the decision-enforcing Explorer client.
// The resolver reads the decision bound by the HTTP transport for the request
// being served, so authorization is per request rather than per process. The
// policy store is supplied by the caller so its lifetime is owned alongside the
// corpus.
func authorizedClient(
	corpus *explorer.Explorer,
	store authorized.PolicyStore,
	resolver auth.Resolver,
	clock func() time.Time,
) (*authorized.Client, error) {
	selector, err := authorized.NewStaticPolicySelector(
		workspaceSourceID, workspaceGrantPolicyID)
	if err != nil {
		return nil, err
	}
	scorer, _ := any(corpus).(authorized.VectorScorer)
	return authorized.NewClient(authorized.Config{
		Base:           corpus,
		VectorScorer:   scorer,
		Resolver:       resolver,
		PolicySelector: selector,
		PolicyStore:    store,
		GenerationReader: fixedGenerationReader{
			domain:     workspaceAuthorizationDomain,
			generation: workspacePolicyGeneration,
		},
		Clock: clock,
	})
}
