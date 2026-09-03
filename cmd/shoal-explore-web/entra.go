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
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// The Entra authenticator validates Microsoft Entra ID (Azure AD) OIDC bearer
// tokens and mints one trusted decision per request. A validated token proves
// identity only; it never confers corpus authority by itself. Authority is
// derived from an explicit, configurable, fail-closed mapping of Entra app
// roles to workspace operations, so an authenticated caller with no recognised
// role sees no corpus content at all.
const (
	// entraActor names the application identity that mints Entra decisions so
	// it is unmistakable in audit output.
	entraActor shoal.ID = "shoal-explore-web-entra"
	// entraAuditPurpose records why an Entra decision exists.
	entraAuditPurpose = "entra-authenticated workspace access"
	// entraSubjectPrefix namespaces the token subject so it can never collide
	// with the development principal or any other actor's identifiers.
	entraSubjectPrefix = "entra:"

	// defaultEntraClockSkew tolerates small clock differences between this
	// instance and the issuer without widening the validity window materially.
	defaultEntraClockSkew = 60 * time.Second
	// maxEntraClockSkew bounds the configurable skew so a misconfiguration can
	// never turn expiry validation into a rubber stamp.
	maxEntraClockSkew = 5 * time.Minute

	// entraJWKSMinRefreshInterval is the shortest interval between JWKS
	// fetches. A cache miss for an unknown key identifier triggers at most one
	// fetch per interval, so a remote attacker presenting tokens with random
	// key identifiers cannot induce an unbounded fetch storm. Legitimate key
	// rotation is still picked up within this interval.
	entraJWKSMinRefreshInterval = 5 * time.Minute
	// entraHTTPTimeout bounds every discovery and JWKS fetch.
	entraHTTPTimeout = 10 * time.Second
	// entraJWKSMaxBytes bounds a JWKS or discovery response so a hostile or
	// broken endpoint cannot exhaust memory.
	entraJWKSMaxBytes = 1 << 20
)

// supportedEntraAlgorithms is the set of asymmetric signing algorithms the
// authenticator will ever accept. It deliberately excludes "none" and every
// symmetric (HMAC) algorithm: accepting an HMAC algorithm would let an attacker
// present a token signed with the issuer's public key as if it were the shared
// secret (an algorithm-confusion attack), and "none" would accept unsigned
// tokens outright.
var supportedEntraAlgorithms = map[string]struct{}{
	"RS256": {}, "RS384": {}, "RS512": {},
	"PS256": {}, "PS384": {}, "PS512": {},
	"ES256": {}, "ES384": {}, "ES512": {},
}

// Package-level sentinel errors are returned by the internal validation path so
// a test can assert the *specific* reason a token was rejected, not merely that
// some error occurred. They are stable pointer identities; errors.Is matches
// them by identity because shoal.Error defines no Is method. The public
// Authenticate method never returns these directly: it collapses every failure
// to a generic denial (see entraDenied) so no reason and no token-derived
// detail crosses the trust boundary. These carry only fixed strings and never
// embed the raw token.
var (
	// errMissingBearer is returned when no usable bearer credential is present.
	errMissingBearer = shoal.NewError(
		shoal.ErrorUnauthorized, "authorization bearer token is required")
	// errMissingKeyID is returned when the token header carries no key id.
	errMissingKeyID = shoal.NewError(
		shoal.ErrorUnauthorized, "token is missing a key identifier")
	// errUnexpectedSigningMethod is returned by the keyfunc's signing-method
	// switch default arm: the resolved method is not one of the accepted
	// asymmetric families. This is one of the two independent algorithm guards
	// (the other is the parser's WithValidMethods allowlist).
	errUnexpectedSigningMethod = shoal.NewError(
		shoal.ErrorUnauthorized, "unexpected token signing method")
	// errKeyMethodMismatch is returned when the resolved key type does not
	// match the token's signing-method family (e.g. an RSA method resolving a
	// non-RSA key).
	errKeyMethodMismatch = shoal.NewError(
		shoal.ErrorUnauthorized, "signing key does not match the signing method")
	// errNoMatchingKey is returned when no signing key matches the token's key
	// identifier, including when the fetch-storm guard refuses a fresh fetch.
	errNoMatchingKey = shoal.NewError(
		shoal.ErrorUnauthorized, "no signing key matches the token key identifier")
	// errMissingSubject is returned when a verified token carries no usable
	// subject or object identifier.
	errMissingSubject = shoal.NewError(
		shoal.ErrorUnauthorized, "token has no usable subject")
	// errMissingExpiry is returned when a verified token carries no expiry.
	errMissingExpiry = shoal.NewError(
		shoal.ErrorUnauthorized, "token has no expiry")
)

// entraReaderOperations is the authority granted to a mapped reader. It permits
// discovery and retrieval but not ingestion.
var entraReaderOperations = []auth.Operation{
	auth.OperationList,
	auth.OperationRead,
	auth.OperationConnect,
	auth.OperationNeighborhood,
	auth.OperationRetrieve,
}

// entraContributorOperations additionally permits ingestion. It matches the
// ceiling the workspace UI needs.
var entraContributorOperations = []auth.Operation{
	auth.OperationIngest,
	auth.OperationList,
	auth.OperationRead,
	auth.OperationConnect,
	auth.OperationNeighborhood,
	auth.OperationRetrieve,
}

// entraUnmappedOperations is the authority granted to an authenticated caller
// whose token carries no recognised role. It is deliberately the narrowest
// useful grant: the caller may enumerate, but is granted no source or policy,
// so every registered document is invisible to them. This is the fail-closed
// default: an absent or unrecognised role claim never widens access.
var entraUnmappedOperations = []auth.Operation{
	auth.OperationList,
}

// entraConfig is the operator-supplied configuration for the Entra
// authenticator. It is populated from flags and environment fallbacks in
// main.go. A client secret is never accepted: this validates bearer tokens, it
// does not perform any token-issuing flow.
type entraConfig struct {
	// tenantID is the Entra tenant (directory) identifier. It derives the
	// expected issuer and the OIDC discovery document when issuer/jwksURI are
	// not overridden.
	tenantID string
	// issuer, when set, overrides the issuer derived from tenantID. The token
	// issuer must match it exactly.
	issuer string
	// audience is the application (client) ID the token must be addressed to.
	// It is required.
	audience string
	// jwksURI, when set, overrides OIDC discovery and is fetched directly.
	jwksURI string
	// allowedAlgorithms restricts accepted signing algorithms. Empty means
	// RS256 only, which is what Entra v2 uses.
	allowedAlgorithms []string
	// clockSkew is the tolerated clock difference for expiry/not-before. Zero
	// means defaultEntraClockSkew.
	clockSkew time.Duration
	// readerRoles and contributorRoles map Entra app-role values to workspace
	// authority. A caller holding a contributor role may ingest; a reader may
	// not. A caller holding neither is authenticated but granted no corpus.
	readerRoles      []string
	contributorRoles []string

	// httpClient and clock are injected by tests; production leaves them nil.
	httpClient *http.Client
	clock      func() time.Time
}

// configured reports whether any Entra option was supplied. Any single option
// signals intent to run the real authenticator, so a partial configuration is
// reported as an error rather than silently ignored.
func (c entraConfig) configured() bool {
	return c.tenantID != "" || c.issuer != "" || c.audience != "" ||
		c.jwksURI != "" || len(c.readerRoles) > 0 || len(c.contributorRoles) > 0
}

// entraAuthenticator validates Entra bearer tokens against the issuer's JWKS
// and mints a conservatively-scoped decision per request.
type entraAuthenticator struct {
	parser           *jwt.Parser
	keys             *jwksCache
	expectedIssuer   string
	expectedAudience string
	readerRoles      map[string]struct{}
	contributorRoles map[string]struct{}
	clock            func() time.Time
}

func newEntraAuthenticator(
	config entraConfig,
	clock func() time.Time,
) (*entraAuthenticator, error) {
	if config.clock != nil {
		clock = config.clock
	}
	if clock == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "clock is required")
	}
	audience := strings.TrimSpace(config.audience)
	if audience == "" {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"-entra-client-id (the application/client ID the token is issued "+
				"for) is required for the Entra authenticator")
	}
	issuer := strings.TrimSpace(config.issuer)
	tenant := strings.TrimSpace(config.tenantID)
	if issuer == "" {
		if tenant == "" {
			return nil, shoal.NewError(
				shoal.ErrorInvalidArgument,
				"-entra-tenant or -entra-issuer is required for the Entra "+
					"authenticator")
		}
		issuer = "https://login.microsoftonline.com/" + tenant + "/v2.0"
	}

	algorithms, err := normalizeEntraAlgorithms(config.allowedAlgorithms)
	if err != nil {
		return nil, err
	}
	skew := config.clockSkew
	if skew == 0 {
		skew = defaultEntraClockSkew
	}
	if skew < 0 || skew > maxEntraClockSkew {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument,
			fmt.Sprintf(
				"-entra-clock-skew must be between 0 and %s", maxEntraClockSkew))
	}

	readerRoles, err := normalizeRoleSet("-entra-reader-roles", config.readerRoles)
	if err != nil {
		return nil, err
	}
	contributorRoles, err := normalizeRoleSet(
		"-entra-contributor-roles", config.contributorRoles)
	if err != nil {
		return nil, err
	}

	httpClient := config.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: entraHTTPTimeout}
	}

	algorithmNames := make([]string, 0, len(algorithms))
	for name := range algorithms {
		algorithmNames = append(algorithmNames, name)
	}

	parser := jwt.NewParser(
		jwt.WithValidMethods(algorithmNames),
		jwt.WithIssuer(issuer),
		jwt.WithAudience(audience),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(skew),
		jwt.WithTimeFunc(clock),
	)

	return &entraAuthenticator{
		parser: parser,
		keys: &jwksCache{
			issuer:             issuer,
			staticJWKSURI:      strings.TrimSpace(config.jwksURI),
			httpClient:         httpClient,
			clock:              clock,
			minRefreshInterval: entraJWKSMinRefreshInterval,
		},
		expectedIssuer:   issuer,
		expectedAudience: audience,
		readerRoles:      readerRoles,
		contributorRoles: contributorRoles,
		clock:            clock,
	}, nil
}

// normalizeEntraAlgorithms validates and de-duplicates the configured
// algorithm allowlist, rejecting any symmetric algorithm and "none".
func normalizeEntraAlgorithms(configured []string) (map[string]struct{}, error) {
	algorithms := make(map[string]struct{})
	for _, raw := range configured {
		name := strings.ToUpper(strings.TrimSpace(raw))
		if name == "" {
			continue
		}
		if _, ok := supportedEntraAlgorithms[name]; !ok {
			return nil, shoal.NewError(
				shoal.ErrorInvalidArgument,
				fmt.Sprintf(
					"signing algorithm %q is not an accepted asymmetric "+
						"algorithm; only RS/PS/ES family algorithms are allowed "+
						"(never HS* or none)", name))
		}
		algorithms[name] = struct{}{}
	}
	if len(algorithms) == 0 {
		algorithms["RS256"] = struct{}{}
	}
	return algorithms, nil
}

// normalizeRoleSet trims, validates, and de-duplicates a configured role list.
func normalizeRoleSet(flagName string, configured []string) (map[string]struct{}, error) {
	roles := make(map[string]struct{})
	for _, raw := range configured {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		roles[name] = struct{}{}
	}
	return roles, nil
}

// Authenticate validates the request's bearer token and mints one decision. It
// is the trust boundary: it collapses every validation failure to a single
// generic denial so no rejection reason and no token-derived detail (least of
// all the raw token) can cross into an error, log line, or HTTP response. The
// specific, token-free reason is available to in-package tests via authenticate.
func (a *entraAuthenticator) Authenticate(
	request *http.Request,
) (auth.Decision, error) {
	decision, err := a.authenticate(request)
	if err != nil {
		if shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
			// A programming error (e.g. a nil request) is not an authentication
			// failure; surface it as-is. It never contains a token.
			return auth.Decision{}, err
		}
		return auth.Decision{}, entraDenied()
	}
	return decision, nil
}

// authenticate performs the real validation and returns the specific,
// token-free reason on failure. It is unexported and used by Authenticate and
// by in-package tests that assert *which* guard rejected a token. None of the
// errors it returns embed the raw token.
func (a *entraAuthenticator) authenticate(
	request *http.Request,
) (auth.Decision, error) {
	if request == nil {
		return auth.Decision{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "request is required")
	}
	raw, err := bearerToken(request)
	if err != nil {
		return auth.Decision{}, err
	}
	ctx := request.Context()
	claims := &entraClaims{}
	keyFunc := a.keyFuncForContext(ctx)
	if _, err := a.parser.ParseWithClaims(raw, claims, keyFunc); err != nil {
		return auth.Decision{}, err
	}
	return a.mint(claims)
}

// keyFuncForContext returns a jwt.Keyfunc bound to the request context so JWKS
// fetches honour request cancellation.
//
// Algorithm safety rests on two independent, observable layers and neither is
// this keyfunc re-checking the header algorithm string (a third, redundant
// check would silently absorb the loss of either real layer, which is exactly
// how an algorithm-confusion test can pass while proving nothing):
//
//  1. The parser's jwt.WithValidMethods allowlist rejects any token whose
//     algorithm is outside the configured asymmetric set (HS*, none, ...)
//     before this keyfunc is ever called.
//  2. The signing-method switch below binds the resolved key type to the
//     method family and its default arm rejects anything else. If the
//     allowlist were removed, a forged HS256 token would reach here and this
//     switch would reject it via the default arm — a different, assertable
//     reason, so removing either layer changes an observable outcome.
func (a *entraAuthenticator) keyFuncForContext(ctx context.Context) jwt.Keyfunc {
	return func(token *jwt.Token) (interface{}, error) {
		kid, _ := token.Header["kid"].(string)
		if strings.TrimSpace(kid) == "" {
			return nil, errMissingKeyID
		}
		key, err := a.keys.keyForID(ctx, kid)
		if err != nil {
			return nil, err
		}
		switch token.Method.(type) {
		case *jwt.SigningMethodRSA, *jwt.SigningMethodRSAPSS:
			if _, ok := key.(*rsa.PublicKey); !ok {
				return nil, errKeyMethodMismatch
			}
		case *jwt.SigningMethodECDSA:
			if _, ok := key.(*ecdsa.PublicKey); !ok {
				return nil, errKeyMethodMismatch
			}
		default:
			return nil, errUnexpectedSigningMethod
		}
		return key, nil
	}
}

// mint maps validated claims to a conservatively-scoped decision.
func (a *entraAuthenticator) mint(claims *entraClaims) (auth.Decision, error) {
	subject := claims.subject()
	if subject == "" {
		return auth.Decision{}, errMissingSubject
	}
	expiry := claims.expiry()
	if expiry.IsZero() {
		return auth.Decision{}, errMissingExpiry
	}
	operations, sources, policies := a.authority(claims.Roles)
	requestID, err := newEntraRequestID()
	if err != nil {
		return auth.Decision{}, err
	}
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject:               shoal.ID(entraSubjectPrefix + subject),
		Actor:                 entraActor,
		AuthorizationDomain:   workspaceAuthorizationDomain,
		AllowedOperations:     operations,
		PermittedSourceIDs:    sources,
		PermittedPolicyIDs:    policies,
		PolicyGeneration:      workspacePolicyGeneration,
		AuthenticationExpires: expiry,
		RequestID:             requestID,
		AuditPurpose:          entraAuditPurpose,
	})
	if err != nil {
		return auth.Decision{}, err
	}
	return decision, nil
}

// authority maps the token's roles to operations and corpus grants. It is
// fail-closed: only an explicitly configured contributor or reader role grants
// visibility of the workspace source and grant policy. Any other case,
// including an absent roles claim, yields the unmapped grant with no corpus
// visibility.
func (a *entraAuthenticator) authority(
	roles []string,
) ([]auth.Operation, [][]byte, [][]byte) {
	if hasAnyRole(roles, a.contributorRoles) {
		return entraContributorOperations,
			[][]byte{workspaceSourceID},
			[][]byte{workspaceGrantPolicyID}
	}
	if hasAnyRole(roles, a.readerRoles) {
		return entraReaderOperations,
			[][]byte{workspaceSourceID},
			[][]byte{workspaceGrantPolicyID}
	}
	return entraUnmappedOperations, nil, nil
}

// hasAnyRole reports whether any token role is in the configured set.
func hasAnyRole(tokenRoles []string, configured map[string]struct{}) bool {
	if len(configured) == 0 {
		return false
	}
	for _, role := range tokenRoles {
		if _, ok := configured[strings.TrimSpace(role)]; ok {
			return true
		}
	}
	return false
}

// entraClaims are the claims the authenticator reads after the library has
// verified the signature, issuer, audience, and time bounds.
type entraClaims struct {
	jwt.RegisteredClaims
	// ObjectID is the immutable per-user identifier within the tenant. It is
	// preferred over Subject because it is stable across applications.
	ObjectID string `json:"oid"`
	// Roles are the Entra app-role values assigned to the caller.
	Roles []string `json:"roles"`
}

// subject returns the stable identifier for the caller, preferring the tenant
// object ID and falling back to the token subject.
func (c *entraClaims) subject() string {
	if id := strings.TrimSpace(c.ObjectID); id != "" {
		return id
	}
	return strings.TrimSpace(c.Subject)
}

// expiry returns the token expiry in UTC, or the zero time when absent.
func (c *entraClaims) expiry() time.Time {
	if c.ExpiresAt == nil {
		return time.Time{}
	}
	return c.ExpiresAt.Time.UTC()
}

// bearerToken extracts the bearer credential from the Authorization header. It
// returns a token-free sentinel that never contains the credential.
func bearerToken(request *http.Request) (string, error) {
	header := request.Header.Get("Authorization")
	if header == "" {
		return "", errMissingBearer
	}
	const prefix = "bearer "
	if len(header) <= len(prefix) ||
		!strings.EqualFold(header[:len(prefix)], prefix) {
		return "", errMissingBearer
	}
	token := strings.TrimSpace(header[len(prefix):])
	if token == "" {
		return "", errMissingBearer
	}
	return token, nil
}

// entraDenied is the single generic authentication failure. It carries no
// token-derived detail so a raw token can never appear in an error, log, or
// response.
func entraDenied() error {
	return shoal.NewError(shoal.ErrorUnauthorized, "authentication failed")
}

func newEntraRequestID() (shoal.ID, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", shoal.WrapError(
			shoal.ErrorUnavailable, "request identity unavailable", err)
	}
	return shoal.ID("entra-request-" + hex.EncodeToString(raw)), nil
}

// jwksCache resolves and caches the issuer's signing keys by key identifier.
// It refreshes on rotation but rate-limits fetches so an unknown key identifier
// cannot induce a fetch storm.
type jwksCache struct {
	issuer        string
	staticJWKSURI string
	httpClient    *http.Client
	clock         func() time.Time
	// minRefreshInterval is the shortest time between fetch attempts.
	minRefreshInterval time.Duration

	mu          sync.Mutex
	keys        map[string]crypto.PublicKey
	fetched     bool
	lastAttempt time.Time
}

// keyForID returns the public key for a key identifier, refreshing at most once
// per minRefreshInterval on a miss. The mutex is held across the fetch so
// concurrent misses collapse to a single refresh rather than a stampede.
func (c *jwksCache) keyForID(
	ctx context.Context,
	kid string,
) (crypto.PublicKey, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if key, ok := c.keys[kid]; ok {
		return key, nil
	}
	now := c.clock()
	if c.fetched && now.Sub(c.lastAttempt) < c.minRefreshInterval {
		// Refuse rather than fetch: a recent attempt already ran, so an
		// unknown key identifier fails closed instead of triggering a storm.
		return nil, errNoMatchingKey
	}
	c.lastAttempt = now
	c.fetched = true
	keys, err := c.fetch(ctx)
	if err != nil {
		return nil, err
	}
	c.keys = keys
	if key, ok := keys[kid]; ok {
		return key, nil
	}
	return nil, errNoMatchingKey
}

// fetch resolves the JWKS URI (via static override or OIDC discovery) and loads
// the current signing keys.
func (c *jwksCache) fetch(ctx context.Context) (map[string]crypto.PublicKey, error) {
	jwksURI := c.staticJWKSURI
	if jwksURI == "" {
		resolved, err := c.discoverJWKSURI(ctx)
		if err != nil {
			return nil, err
		}
		jwksURI = resolved
	}
	body, err := c.get(ctx, jwksURI)
	if err != nil {
		return nil, err
	}
	return parseJWKS(body)
}

// discoverJWKSURI reads the issuer's OIDC discovery document and returns its
// jwks_uri, rejecting a document whose issuer does not match the expected one.
func (c *jwksCache) discoverJWKSURI(ctx context.Context) (string, error) {
	discoveryURL := strings.TrimRight(c.issuer, "/") +
		"/.well-known/openid-configuration"
	body, err := c.get(ctx, discoveryURL)
	if err != nil {
		return "", err
	}
	var document struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(body, &document); err != nil {
		return "", shoal.WrapError(
			shoal.ErrorUnavailable, "invalid OIDC discovery document", err)
	}
	if document.Issuer != c.issuer {
		return "", shoal.NewError(
			shoal.ErrorUnauthorized,
			"OIDC discovery issuer does not match the configured issuer")
	}
	if strings.TrimSpace(document.JWKSURI) == "" {
		return "", shoal.NewError(
			shoal.ErrorUnavailable, "OIDC discovery document has no jwks_uri")
	}
	return document.JWKSURI, nil
}

// get performs a bounded HTTP GET and returns the response body.
func (c *jwksCache) get(ctx context.Context, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, shoal.WrapError(
			shoal.ErrorInvalidArgument, "invalid issuer metadata URL", err)
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, shoal.WrapError(
			shoal.ErrorUnavailable, "issuer metadata is unavailable", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable, "issuer metadata request failed")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, entraJWKSMaxBytes))
	if err != nil {
		return nil, shoal.WrapError(
			shoal.ErrorUnavailable, "reading issuer metadata failed", err)
	}
	return body, nil
}

// jsonWebKey is one entry in a JWKS. Only the fields needed to build an RSA or
// EC public key are decoded.
type jsonWebKey struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// parseJWKS builds a key-identifier-keyed map of public keys from a JWKS
// document. Entries without a key identifier or of an unsupported type are
// skipped rather than failing the whole set.
func parseJWKS(body []byte) (map[string]crypto.PublicKey, error) {
	var set struct {
		Keys []jsonWebKey `json:"keys"`
	}
	if err := json.Unmarshal(body, &set); err != nil {
		return nil, shoal.WrapError(
			shoal.ErrorUnavailable, "invalid JWKS document", err)
	}
	keys := make(map[string]crypto.PublicKey, len(set.Keys))
	for _, entry := range set.Keys {
		if strings.TrimSpace(entry.Kid) == "" {
			continue
		}
		switch entry.Kty {
		case "RSA":
			key, err := entry.rsaKey()
			if err != nil {
				continue
			}
			keys[entry.Kid] = key
		case "EC":
			key, err := entry.ecKey()
			if err != nil {
				continue
			}
			keys[entry.Kid] = key
		default:
			continue
		}
	}
	if len(keys) == 0 {
		return nil, shoal.NewError(
			shoal.ErrorUnavailable, "JWKS document has no usable signing keys")
	}
	return keys, nil
}

func (k jsonWebKey) rsaKey() (*rsa.PublicKey, error) {
	modulus, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil || len(modulus) == 0 {
		return nil, fmt.Errorf("invalid RSA modulus")
	}
	exponent, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil || len(exponent) == 0 {
		return nil, fmt.Errorf("invalid RSA exponent")
	}
	e := new(big.Int).SetBytes(exponent)
	if !e.IsInt64() || e.Int64() < 2 || e.Int64() > (1<<31-1) {
		return nil, fmt.Errorf("invalid RSA exponent range")
	}
	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(modulus),
		E: int(e.Int64()),
	}, nil
}

func (k jsonWebKey) ecKey() (*ecdsa.PublicKey, error) {
	var curve elliptic.Curve
	switch k.Crv {
	case "P-256":
		curve = elliptic.P256()
	case "P-384":
		curve = elliptic.P384()
	case "P-521":
		curve = elliptic.P521()
	default:
		return nil, fmt.Errorf("unsupported EC curve")
	}
	x, err := base64.RawURLEncoding.DecodeString(k.X)
	if err != nil || len(x) == 0 {
		return nil, fmt.Errorf("invalid EC x coordinate")
	}
	y, err := base64.RawURLEncoding.DecodeString(k.Y)
	if err != nil || len(y) == 0 {
		return nil, fmt.Errorf("invalid EC y coordinate")
	}
	key := &ecdsa.PublicKey{
		Curve: curve,
		X:     new(big.Int).SetBytes(x),
		Y:     new(big.Int).SetBytes(y),
	}
	if !curve.IsOnCurve(key.X, key.Y) {
		return nil, fmt.Errorf("EC point is not on the curve")
	}
	return key, nil
}
