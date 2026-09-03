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
	"strings"
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
	stateDir := flags.String(
		"state-dir", "",
		"Recommended workspace state root. The corpus and durable policy "+
			"catalog are created as corpus/ and policy/ inside it, so mounting "+
			"this one directory as a volume persists everything a restart "+
			"needs. Overrides -data when set",
	)
	data := flags.String(
		"data", ".shoal/explorer",
		"Legacy Explorer corpus directory (used when -state-dir is unset). The "+
			"durable policy catalog is placed in a sibling directory; both must "+
			"be persisted for the workspace to survive a restart",
	)
	policyDirFlag := flags.String(
		"policy-dir", "",
		"Durable policy catalog directory. Overrides the location derived from "+
			"-state-dir or -data; must be persisted for registrations to "+
			"survive a restart",
	)
	listen := flags.String("listen", "127.0.0.1:8080", "HTTP listen address")
	allowedHost := flags.String(
		"allowed-host", "",
		"Comma-separated exact-match allow-list of external authorities (host "+
			"or host:port) an inbound request's Host/:authority must match; the "+
			"hostname compares case-insensitively and the port exactly. No "+
			"wildcard or suffix matching, and X-Forwarded-Host is never "+
			"trusted. Required for a non-loopback or wildcard -listen, whose "+
			"resolved socket address real client Host headers never carry; a "+
			"public bind refuses every request until this names the external "+
			"authority. Defaults to the resolved listen address; environment "+
			"fallback SHOAL_ALLOWED_HOST",
	)
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
	entraTenant := flags.String(
		"entra-tenant", "",
		"Microsoft Entra ID (Azure AD) tenant/directory ID. Derives the "+
			"expected issuer and OIDC discovery document. Enables the real "+
			"authenticator; environment fallback SHOAL_ENTRA_TENANT",
	)
	entraIssuer := flags.String(
		"entra-issuer", "",
		"Override the expected token issuer (exact match). Defaults to "+
			"https://login.microsoftonline.com/<tenant>/v2.0; environment "+
			"fallback SHOAL_ENTRA_ISSUER",
	)
	entraClientID := flags.String(
		"entra-client-id", "",
		"Application (client) ID the token audience must match exactly. "+
			"Required for the Entra authenticator; environment fallback "+
			"SHOAL_ENTRA_CLIENT_ID. A client secret is never accepted",
	)
	entraJWKSURI := flags.String(
		"entra-jwks-uri", "",
		"Override the JWKS URI instead of resolving it via OIDC discovery; "+
			"environment fallback SHOAL_ENTRA_JWKS_URI",
	)
	entraAllowedAlgs := flags.String(
		"entra-allowed-algs", "",
		"Comma-separated allowlist of asymmetric signing algorithms "+
			"(e.g. RS256,ES256). Defaults to RS256. HS* and none are always "+
			"rejected",
	)
	entraClockSkew := flags.Duration(
		"entra-clock-skew", 0,
		"Tolerated clock skew for token expiry/not-before checks; defaults to "+
			"60s and is capped at 5m",
	)
	entraReaderRoles := flags.String(
		"entra-reader-roles", "",
		"Comma-separated Entra app-role values granted read-only workspace "+
			"access; environment fallback SHOAL_ENTRA_READER_ROLES",
	)
	entraContributorRoles := flags.String(
		"entra-contributor-roles", "",
		"Comma-separated Entra app-role values granted read and ingest "+
			"access; environment fallback SHOAL_ENTRA_CONTRIBUTOR_ROLES",
	)
	if err := flags.Parse(args); err != nil {
		return err
	}

	entra := entraConfig{
		tenantID:          firstNonEmpty(*entraTenant, os.Getenv("SHOAL_ENTRA_TENANT")),
		issuer:            firstNonEmpty(*entraIssuer, os.Getenv("SHOAL_ENTRA_ISSUER")),
		audience:          firstNonEmpty(*entraClientID, os.Getenv("SHOAL_ENTRA_CLIENT_ID")),
		jwksURI:           firstNonEmpty(*entraJWKSURI, os.Getenv("SHOAL_ENTRA_JWKS_URI")),
		allowedAlgorithms: splitCommaList(*entraAllowedAlgs),
		clockSkew:         *entraClockSkew,
		readerRoles: splitCommaList(
			firstNonEmpty(*entraReaderRoles, os.Getenv("SHOAL_ENTRA_READER_ROLES"))),
		contributorRoles: splitCommaList(firstNonEmpty(
			*entraContributorRoles, os.Getenv("SHOAL_ENTRA_CONTRIBUTOR_ROLES"))),
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
	if _, err := selectAuthenticator(*developmentAuth, entra, *listen, time.Now); err != nil {
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
		*developmentAuth, entra, listener.Addr().String(), time.Now)
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
	corpusDir, policyDir := resolveWorkspacePaths(*stateDir, *data, *policyDirFlag)
	opened, err := openService(ctx, serviceConfig{
		backend:   *backend,
		data:      corpusDir,
		policyDir: policyDir,
		remote:    *remote,
		embedder:  embedding,
		resolver:  authority.Resolver(),
		clock:     time.Now,
		backfill:  backfill,
	})
	if err != nil {
		listener.Close()
		return err
	}
	service, cleanup := opened.service, opened.close
	defer cleanup()

	// The host-authority allow-list defaults to the resolved listen address,
	// preserving the local-first posture: a loopback bind serves only requests
	// whose Host is that loopback authority. A non-loopback or wildcard bind
	// resolves to an address (for example 0.0.0.0:<port>) that real client Host
	// headers never carry, so such a deployment must name its external
	// authority with -allowed-host or every request is refused — a fail-closed
	// default, safe but requiring explicit configuration behind a proxy.
	allowedAuthorities := splitCommaList(
		firstNonEmpty(*allowedHost, os.Getenv("SHOAL_ALLOWED_HOST")))
	if len(allowedAuthorities) == 0 {
		allowedAuthorities = []string{listener.Addr().String()}
	}

	handler, err := webapi.NewAuthenticatedHandler(
		service, authenticator, authority.Binder(), allowedAuthorities...)
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
	if entra.configured() {
		fmt.Fprintf(
			output,
			"Validating Microsoft Entra ID bearer tokens for audience %s; "+
				"unmapped callers receive no corpus access\n",
			entra.audience,
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
				opened.backfilled, corpusDir, developmentSubject,
			)
		} else {
			fmt.Fprintf(
				output,
				"Policy catalog is durable in %s: documents this build "+
					"ingests stay authorized across restarts. Persist both the "+
					"corpus (%s) and this policy directory for the workspace to "+
					"survive a restart. Documents ingested before the catalog "+
					"was durable stay hidden until re-registered (issue #284)\n",
				policyDir, corpusDir,
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
	backend string
	data    string
	// policyDir is the durable policy catalog directory. When empty it is
	// derived from data as a legacy sibling, so callers that set only data keep
	// working.
	policyDir string
	remote    string
	embedder  model.Embedder
	resolver  auth.Resolver
	clock     func() time.Time
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
		// The policy catalog is durable and lives in its own directory, not a
		// subdirectory of the corpus: the corpus engine treats every
		// subdirectory as a table, so nesting the store there would corrupt
		// table discovery. A caller that sets only data keeps the legacy
		// sibling layout; -state-dir/-policy-dir place both under one mount.
		policyDir := config.policyDir
		if policyDir == "" {
			policyDir = policyStoreDir(config.data)
		}
		store, err := authorized.OpenDurablePolicyStore(policyDir)
		if err != nil {
			corpus.Close()
			return closed, err
		}
		// A non-empty corpus paired with an empty policy catalog is the
		// signature of a lost or unmounted policy volume: the documents
		// survived but every authorization registration is gone. Refuse rather
		// than serve a silently under-authorized workspace. The development
		// backfill is the sanctioned way to (re)register a pre-durability
		// corpus, so this guard applies only in production (backfill == nil),
		// where the two situations are otherwise indistinguishable.
		if config.backfill == nil {
			if err := refuseSplitBrainStateDirectory(
				ctx, corpus, store, config.data, policyDir); err != nil {
				store.Close()
				corpus.Close()
				return closed, err
			}
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

// firstNonEmpty returns the first argument whose trimmed value is non-empty.
// It gives command-line flags precedence over their environment fallbacks.
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

// splitCommaList splits a comma-separated flag value into trimmed, non-empty
// items. An empty input yields a nil slice.
func splitCommaList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}

// resolveWorkspacePaths determines the corpus and policy directories from the
// three location flags. The recommended configuration is a single -state-dir
// that can be mounted as one volume: the corpus and durable policy catalog live
// as siblings inside it, so persisting the state root persists both. -data
// remains for backwards compatibility and derives a sibling policy directory;
// -policy-dir overrides the policy location explicitly and always wins.
func resolveWorkspacePaths(stateDir, dataDir, policyDir string) (corpus, policy string) {
	if stateDir != "" {
		corpus = filepath.Join(stateDir, "corpus")
		policy = filepath.Join(stateDir, "policy")
	} else {
		corpus = dataDir
		policy = policyStoreDir(dataDir)
	}
	if policyDir != "" {
		policy = policyDir
	}
	return corpus, policy
}

// refuseSplitBrainStateDirectory rejects a workspace whose corpus holds
// documents while the durable policy catalog holds no registrations. That pair
// is what a deployment sees after the policy volume is lost or left unmounted:
// the documents are present but nobody is authorized to see them. Serving it
// would present an empty or under-populated corpus and hide the cause, so the
// program refuses with the paths and remediation an operator needs.
func refuseSplitBrainStateDirectory(
	ctx context.Context,
	corpus *explorer.Explorer,
	store *authorized.DurablePolicyStore,
	corpusDir string,
	policyDir string,
) error {
	if store.HasRegistrations() {
		return nil
	}
	documents, err := corpus.Documents(ctx)
	if err != nil {
		return err
	}
	if len(documents) == 0 {
		return nil
	}
	return fmt.Errorf(
		"refusing to serve a split-brain workspace: corpus %s holds %d "+
			"document(s) but the durable policy catalog %s holds no "+
			"authorization registrations. This is the signature of a lost or "+
			"unmounted policy volume; every registration was dropped and the "+
			"workspace would serve an empty or under-populated corpus. Restore "+
			"the policy directory from the same volume as the corpus (use "+
			"-state-dir so both persist under one mount), or, for a corpus "+
			"ingested before the catalog was durable, run once with -dev-auth "+
			"on a loopback listener to re-register it (issue #284)",
		corpusDir, len(documents), policyDir)
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
