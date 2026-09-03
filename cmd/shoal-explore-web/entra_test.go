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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
)

const (
	testTenant   = "00000000-0000-0000-0000-tenantfake00"
	testAudience = "11111111-1111-1111-1111-clientfake00"
	testKID      = "test-signing-key-1"
	testOID      = "22222222-2222-2222-2222-userfake0001"
)

// fakeIssuer is a hermetic OIDC issuer. It serves a discovery document and a
// JWKS from an httptest server and signs tokens with in-test RSA keys, so every
// test runs with no network access.
type fakeIssuer struct {
	server *httptest.Server
	keys   map[string]*rsa.PrivateKey

	mu            sync.Mutex
	discoveryHits int
	jwksHits      int
}

func newFakeIssuer(t *testing.T) *fakeIssuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	issuer := &fakeIssuer{keys: map[string]*rsa.PrivateKey{testKID: key}}
	mux := http.NewServeMux()
	mux.HandleFunc(
		"/.well-known/openid-configuration",
		func(writer http.ResponseWriter, _ *http.Request) {
			issuer.mu.Lock()
			issuer.discoveryHits++
			issuer.mu.Unlock()
			writer.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(writer).Encode(map[string]string{
				"issuer":   issuer.server.URL,
				"jwks_uri": issuer.server.URL + "/keys",
			})
		},
	)
	mux.HandleFunc(
		"/keys",
		func(writer http.ResponseWriter, _ *http.Request) {
			issuer.mu.Lock()
			issuer.jwksHits++
			issuer.mu.Unlock()
			writer.Header().Set("Content-Type", "application/json")
			writer.Write(issuer.jwksJSON())
		},
	)
	issuer.server = httptest.NewServer(mux)
	t.Cleanup(issuer.server.Close)
	return issuer
}

// jwksJSON renders the current public keys as a JWKS document.
func (f *fakeIssuer) jwksJSON() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	type webKey struct {
		Kty string `json:"kty"`
		Kid string `json:"kid"`
		Use string `json:"use"`
		Alg string `json:"alg"`
		N   string `json:"n"`
		E   string `json:"e"`
	}
	document := struct {
		Keys []webKey `json:"keys"`
	}{}
	for kid, key := range f.keys {
		document.Keys = append(document.Keys, webKey{
			Kty: "RSA",
			Kid: kid,
			Use: "sig",
			Alg: "RS256",
			N:   base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
			E: base64.RawURLEncoding.EncodeToString(
				big.NewInt(int64(key.PublicKey.E)).Bytes()),
		})
	}
	raw, _ := json.Marshal(document)
	return raw
}

func (f *fakeIssuer) discoveryCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.discoveryHits
}

// defaultClaims builds a valid claim set for the given wall-clock time.
func (f *fakeIssuer) defaultClaims(now time.Time) jwt.MapClaims {
	return jwt.MapClaims{
		"iss": f.server.URL,
		"aud": testAudience,
		"sub": "subject-fallback",
		"oid": testOID,
		"iat": now.Add(-time.Minute).Unix(),
		"nbf": now.Add(-time.Minute).Unix(),
		"exp": now.Add(time.Hour).Unix(),
	}
}

// signRS256 signs claims with the issuer's key for the given key identifier.
func (f *fakeIssuer) signRS256(t *testing.T, kid string, claims jwt.MapClaims) string {
	t.Helper()
	f.mu.Lock()
	key := f.keys[kid]
	f.mu.Unlock()
	if key == nil {
		// Sign with an ephemeral key so the kid is present but unverifiable.
		fresh, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("generate ephemeral key: %v", err)
		}
		key = fresh
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = kid
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}

// testConfig returns a config that resolves the issuer through OIDC discovery
// with an injected clock and the issuer's HTTP client (hermetic).
func (f *fakeIssuer) testConfig(clock func() time.Time) entraConfig {
	return entraConfig{
		issuer:     f.server.URL,
		audience:   testAudience,
		httpClient: f.server.Client(),
		clock:      clock,
	}
}

func fixedClock(at time.Time) func() time.Time {
	return func() time.Time { return at }
}

func bearerRequest(token string) *http.Request {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/meta", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	return request
}

func newTestAuthenticator(t *testing.T, config entraConfig) *entraAuthenticator {
	t.Helper()
	authenticator, err := newEntraAuthenticator(config, time.Now)
	if err != nil {
		t.Fatalf("newEntraAuthenticator: %v", err)
	}
	return authenticator
}

// TestEntraValidTokenIsUnmappedByDefault proves a validly-signed token
// authenticates but, absent a recognised role, receives no corpus visibility.
// This is the anti-broad-authority invariant: an authenticated user does not
// inherit the development principal's blanket grant.
func TestEntraValidTokenIsUnmappedByDefault(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	authenticator := newTestAuthenticator(t, issuer.testConfig(fixedClock(now)))
	token := issuer.signRS256(t, testKID, issuer.defaultClaims(now))

	decision, err := authenticator.Authenticate(bearerRequest(token))
	if err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}
	if decision.Subject() != "entra:"+testOID {
		t.Fatalf("subject = %q, want %q", decision.Subject(), "entra:"+testOID)
	}
	if len(decision.PermittedSourceIDs()) != 0 {
		t.Fatalf(
			"unmapped caller was granted %d source(s); an authenticated but "+
				"unmapped user must receive no corpus access",
			len(decision.PermittedSourceIDs()))
	}
	if len(decision.PermittedPolicyIDs()) != 0 {
		t.Fatalf("unmapped caller was granted %d policy grant(s)",
			len(decision.PermittedPolicyIDs()))
	}
	if operationsContain(decision.AllowedOperations(), auth.OperationIngest) {
		t.Fatal("unmapped caller was granted ingest")
	}
}

// TestEntraReaderRoleGrantsCorpusReadOnly proves a configured reader role
// unlocks corpus visibility but not ingestion.
func TestEntraReaderRoleGrantsCorpusReadOnly(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	config := issuer.testConfig(fixedClock(now))
	config.readerRoles = []string{"Shoal.Reader"}
	authenticator := newTestAuthenticator(t, config)

	claims := issuer.defaultClaims(now)
	claims["roles"] = []string{"Shoal.Reader"}
	decision, err := authenticator.Authenticate(
		bearerRequest(issuer.signRS256(t, testKID, claims)))
	if err != nil {
		t.Fatalf("reader token rejected: %v", err)
	}
	if !grantsWorkspaceSource(decision) {
		t.Fatal("reader was not granted the workspace source")
	}
	if operationsContain(decision.AllowedOperations(), auth.OperationIngest) {
		t.Fatal("reader was granted ingest")
	}
	if !operationsContain(decision.AllowedOperations(), auth.OperationRead) {
		t.Fatal("reader was not granted read")
	}
}

// TestEntraContributorRoleGrantsIngest proves a configured contributor role
// unlocks ingestion in addition to reads.
func TestEntraContributorRoleGrantsIngest(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	config := issuer.testConfig(fixedClock(now))
	config.contributorRoles = []string{"Shoal.Contributor"}
	authenticator := newTestAuthenticator(t, config)

	claims := issuer.defaultClaims(now)
	claims["roles"] = []string{"Shoal.Contributor"}
	decision, err := authenticator.Authenticate(
		bearerRequest(issuer.signRS256(t, testKID, claims)))
	if err != nil {
		t.Fatalf("contributor token rejected: %v", err)
	}
	if !grantsWorkspaceSource(decision) {
		t.Fatal("contributor was not granted the workspace source")
	}
	if !operationsContain(decision.AllowedOperations(), auth.OperationIngest) {
		t.Fatal("contributor was not granted ingest")
	}
}

// TestEntraUnrecognisedRoleGetsNoCorpus proves a role that is not configured is
// treated as unmapped: the claim never widens access on its own.
func TestEntraUnrecognisedRoleGetsNoCorpus(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	config := issuer.testConfig(fixedClock(now))
	config.readerRoles = []string{"Shoal.Reader"}
	authenticator := newTestAuthenticator(t, config)

	claims := issuer.defaultClaims(now)
	claims["roles"] = []string{"Some.Other.Role"}
	decision, err := authenticator.Authenticate(
		bearerRequest(issuer.signRS256(t, testKID, claims)))
	if err != nil {
		t.Fatalf("token rejected: %v", err)
	}
	if grantsWorkspaceSource(decision) {
		t.Fatal("an unrecognised role was granted corpus visibility")
	}
}

// TestEntraRejectsAlgNone proves an unsigned token (alg: none) is rejected.
// TestEntraRejectsAlgNone proves a token with "alg: none" is rejected, and
// asserts the *specific* reason: the parser's WithValidMethods allowlist
// refuses the method before the keyfunc runs, surfacing as
// jwt.ErrTokenSignatureInvalid. Asserting the reason (not merely "an error
// occurred") makes this test sensitive to the allowlist: if WithValidMethods
// is removed, the none token instead reaches the keyfunc and is rejected by the
// signing-method switch default arm (errUnexpectedSigningMethod, wrapped as
// ErrTokenUnverifiable), so this assertion fails — the guard is observable.
func TestEntraRejectsAlgNone(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	authenticator := newTestAuthenticator(t, issuer.testConfig(fixedClock(now)))

	token := jwt.NewWithClaims(jwt.SigningMethodNone, issuer.defaultClaims(now))
	token.Header["kid"] = testKID
	signed, err := token.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatalf("sign none token: %v", err)
	}
	_, err = authenticator.authenticate(bearerRequest(signed))
	if !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
		t.Fatalf(
			"alg:none was not rejected by the method allowlist "+
				"(want ErrTokenSignatureInvalid): %v", err)
	}
}

// TestEntraRejectsAlgorithmConfusion proves an HS256 token whose HMAC secret is
// the issuer's RSA public key is rejected — the classic RS256/HS256 confusion
// bypass. Like the alg:none test, it asserts the specific reason so the guard
// is observable: WithValidMethods refuses HS256 before the keyfunc, surfacing
// as jwt.ErrTokenSignatureInvalid. Remove that allowlist and the forged token
// reaches the keyfunc, which rejects it via the default arm with a *different*
// error (errUnexpectedSigningMethod / ErrTokenUnverifiable) — failing this
// assertion. So deleting either algorithm guard changes an observable outcome.
func TestEntraRejectsAlgorithmConfusion(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	authenticator := newTestAuthenticator(t, issuer.testConfig(fixedClock(now)))

	issuer.mu.Lock()
	publicN := issuer.keys[testKID].PublicKey.N.Bytes()
	issuer.mu.Unlock()

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, issuer.defaultClaims(now))
	token.Header["kid"] = testKID
	forged, err := token.SignedString(publicN)
	if err != nil {
		t.Fatalf("sign HS256 token: %v", err)
	}
	_, err = authenticator.authenticate(bearerRequest(forged))
	if !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
		t.Fatalf(
			"HS256 confusion token was not rejected by the method allowlist "+
				"(want ErrTokenSignatureInvalid): %v", err)
	}
}

// TestEntraKeyfuncRejectsMismatchedSigningMethod exercises the *second*
// algorithm guard directly: the keyfunc's signing-method switch. WithValidMethods
// short-circuits real requests before the keyfunc for a disallowed method, so
// the switch's default arm is only reachable by calling the keyfunc directly
// with a fabricated token. This test does exactly that, asserting the specific
// sentinels, so deleting the default arm (an HMAC method would then be handed
// the RSA key — the algorithm-confusion hole) fails this test, and deleting the
// key-type bind fails the second assertion.
func TestEntraKeyfuncRejectsMismatchedSigningMethod(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	authenticator := newTestAuthenticator(t, issuer.testConfig(fixedClock(now)))

	// Prime the JWKS cache with the issuer's real RSA key for testKID.
	valid := issuer.signRS256(t, testKID, issuer.defaultClaims(now))
	if _, err := authenticator.authenticate(bearerRequest(valid)); err != nil {
		t.Fatalf("priming token unexpectedly rejected: %v", err)
	}

	keyFunc := authenticator.keyFuncForContext(context.Background())

	hmacToken := &jwt.Token{
		Header: map[string]interface{}{"alg": "HS256", "kid": testKID},
		Method: jwt.SigningMethodHS256,
	}
	if _, err := keyFunc(hmacToken); !errors.Is(err, errUnexpectedSigningMethod) {
		t.Fatalf(
			"keyfunc did not reject an HMAC method via the switch default arm "+
				"(want errUnexpectedSigningMethod): %v", err)
	}

	ecToken := &jwt.Token{
		Header: map[string]interface{}{"alg": "ES256", "kid": testKID},
		Method: jwt.SigningMethodES256,
	}
	if _, err := keyFunc(ecToken); !errors.Is(err, errKeyMethodMismatch) {
		t.Fatalf(
			"keyfunc did not reject an ECDSA method against an RSA key "+
				"(want errKeyMethodMismatch): %v", err)
	}
}

// TestEntraRejectsWrongAudience proves a token addressed to another application
// is rejected. The audience check is owned by the library (configured via
// jwt.WithAudience); this test asserts the specific ErrTokenInvalidAudience so
// it fails if that option is removed rather than passing on any error.
func TestEntraRejectsWrongAudience(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	authenticator := newTestAuthenticator(t, issuer.testConfig(fixedClock(now)))

	claims := issuer.defaultClaims(now)
	claims["aud"] = "some-other-client-id"
	_, err := authenticator.authenticate(
		bearerRequest(issuer.signRS256(t, testKID, claims)))
	if !errors.Is(err, jwt.ErrTokenInvalidAudience) {
		t.Fatalf(
			"wrong audience not rejected as such "+
				"(want ErrTokenInvalidAudience): %v", err)
	}
}

// TestEntraRejectsWrongIssuer proves a token minted by another issuer is
// rejected even if it is otherwise well formed. The issuer check is owned by
// the library (jwt.WithIssuer); this asserts the specific ErrTokenInvalidIssuer.
func TestEntraRejectsWrongIssuer(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	authenticator := newTestAuthenticator(t, issuer.testConfig(fixedClock(now)))

	claims := issuer.defaultClaims(now)
	claims["iss"] = "https://login.microsoftonline.com/evil/v2.0"
	_, err := authenticator.authenticate(
		bearerRequest(issuer.signRS256(t, testKID, claims)))
	if !errors.Is(err, jwt.ErrTokenInvalidIssuer) {
		t.Fatalf(
			"wrong issuer not rejected as such "+
				"(want ErrTokenInvalidIssuer): %v", err)
	}
}

// TestEntraRejectsExpiredToken proves a token expired beyond the skew is
// rejected, while TestEntraAcceptsWithinClockSkew proves the skew is honoured.
// Expiry is owned by the library (jwt.WithExpirationRequired + WithLeeway);
// this asserts the specific ErrTokenExpired.
func TestEntraRejectsExpiredToken(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	authenticator := newTestAuthenticator(t, issuer.testConfig(fixedClock(now)))

	claims := issuer.defaultClaims(now)
	claims["exp"] = now.Add(-2 * time.Hour).Unix()
	_, err := authenticator.authenticate(
		bearerRequest(issuer.signRS256(t, testKID, claims)))
	if !errors.Is(err, jwt.ErrTokenExpired) {
		t.Fatalf("expired token not rejected as such (want ErrTokenExpired): %v", err)
	}
}

func TestEntraAcceptsWithinClockSkew(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	config := issuer.testConfig(fixedClock(now))
	config.clockSkew = 60 * time.Second
	authenticator := newTestAuthenticator(t, config)

	claims := issuer.defaultClaims(now)
	claims["exp"] = now.Add(-30 * time.Second).Unix()
	if _, err := authenticator.Authenticate(
		bearerRequest(issuer.signRS256(t, testKID, claims))); err != nil {
		t.Fatalf("a token expired within the skew was rejected: %v", err)
	}
}

// TestEntraRejectsNotYetValid proves a token whose nbf is in the future is
// rejected. The nbf check is owned by the library; this asserts the specific
// ErrTokenNotValidYet.
func TestEntraRejectsNotYetValid(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	authenticator := newTestAuthenticator(t, issuer.testConfig(fixedClock(now)))

	claims := issuer.defaultClaims(now)
	claims["nbf"] = now.Add(2 * time.Hour).Unix()
	_, err := authenticator.authenticate(
		bearerRequest(issuer.signRS256(t, testKID, claims)))
	if !errors.Is(err, jwt.ErrTokenNotValidYet) {
		t.Fatalf(
			"not-yet-valid token not rejected as such "+
				"(want ErrTokenNotValidYet): %v", err)
	}
}

// TestEntraRejectsBadSignature proves a token whose signature does not verify
// against the advertised key is rejected. The kid names a real key, but the
// token is signed by a different key. Signature verification is owned by the
// library once the keyfunc supplies the correct key; this asserts the specific
// ErrTokenSignatureInvalid so it fails if verification is somehow bypassed.
func TestEntraRejectsBadSignature(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	authenticator := newTestAuthenticator(t, issuer.testConfig(fixedClock(now)))

	attacker, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate attacker key: %v", err)
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, issuer.defaultClaims(now))
	token.Header["kid"] = testKID
	forged, err := token.SignedString(attacker)
	if err != nil {
		t.Fatalf("sign forged token: %v", err)
	}
	_, err = authenticator.authenticate(bearerRequest(forged))
	if !errors.Is(err, jwt.ErrTokenSignatureInvalid) {
		t.Fatalf(
			"bad signature not rejected as such "+
				"(want ErrTokenSignatureInvalid): %v", err)
	}
}

// TestEntraUnknownKidDoesNotStorm proves an unknown key identifier is rejected
// and, crucially, that a flood of unknown-kid tokens does not induce more than
// one JWKS refresh within the refresh interval. A remote attacker must not be
// able to turn token submission into a fetch amplifier.
func TestEntraUnknownKidDoesNotStorm(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	authenticator := newTestAuthenticator(t, issuer.testConfig(fixedClock(now)))

	for i := 0; i < 50; i++ {
		claims := issuer.defaultClaims(now)
		token := issuer.signRS256(t, "unknown-kid", claims)
		_, err := authenticator.authenticate(bearerRequest(token))
		if !errors.Is(err, errNoMatchingKey) {
			t.Fatalf(
				"unknown key identifier not rejected by our key lookup "+
					"(want errNoMatchingKey): %v", err)
		}
	}
	if hits := issuer.discoveryCount(); hits > 1 {
		t.Fatalf(
			"unknown key identifiers triggered %d metadata fetches; a remote "+
				"caller induced a refresh storm", hits)
	}
}

// TestEntraRotationIsPickedUpAfterInterval proves that after the refresh
// interval elapses, a rotated-in key is fetched and its tokens verify.
func TestEntraRotationIsPickedUpAfterInterval(t *testing.T) {
	issuer := newFakeIssuer(t)
	base := time.Now()
	clockValue := base
	config := issuer.testConfig(func() time.Time { return clockValue })
	authenticator := newTestAuthenticator(t, config)

	// Prime the cache with the original key.
	if _, err := authenticator.Authenticate(bearerRequest(
		issuer.signRS256(t, testKID, issuer.defaultClaims(clockValue)))); err != nil {
		t.Fatalf("initial token rejected: %v", err)
	}

	// Rotate in a new key.
	rotated, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rotated key: %v", err)
	}
	issuer.mu.Lock()
	issuer.keys["test-signing-key-2"] = rotated
	issuer.mu.Unlock()

	// Before the interval elapses the new kid is unknown and rejected.
	if _, err := authenticator.Authenticate(bearerRequest(
		issuer.signRS256(t, "test-signing-key-2", issuer.defaultClaims(clockValue)))); err == nil {
		t.Fatal("rotated key accepted before a refresh was permitted")
	}

	// After the interval elapses the refresh runs and the new key verifies.
	clockValue = base.Add(entraJWKSMinRefreshInterval + time.Second)
	if _, err := authenticator.Authenticate(bearerRequest(
		issuer.signRS256(t, "test-signing-key-2", issuer.defaultClaims(clockValue)))); err != nil {
		t.Fatalf("rotated key not accepted after refresh: %v", err)
	}
}

// TestEntraErrorNeverContainsRawToken proves a rejected token's raw value never
// appears in the returned error, for both an unparseable token and a
// structurally valid but unverifiable one.
func TestEntraErrorNeverContainsRawToken(t *testing.T) {
	issuer := newFakeIssuer(t)
	now := time.Now()
	authenticator := newTestAuthenticator(t, issuer.testConfig(fixedClock(now)))

	const garbage = "this-is-a-secret-raw-token-value-not-a-jwt"
	_, err := authenticator.Authenticate(bearerRequest(garbage))
	if err == nil {
		t.Fatal("a garbage token was accepted")
	}
	if strings.Contains(err.Error(), garbage) {
		t.Fatalf("error echoed the raw token: %v", err)
	}

	claims := issuer.defaultClaims(now)
	claims["aud"] = "wrong-audience"
	valid := issuer.signRS256(t, testKID, claims)
	_, err = authenticator.Authenticate(bearerRequest(valid))
	if err == nil {
		t.Fatal("a wrong-audience token was accepted")
	}
	if strings.Contains(err.Error(), valid) {
		t.Fatalf("error echoed the raw token: %v", err)
	}

	// The internal path returns the specific reason; assert even that
	// token-free error never embeds the credential.
	_, internalErr := authenticator.authenticate(bearerRequest(valid))
	if internalErr == nil {
		t.Fatal("a wrong-audience token was accepted on the internal path")
	}
	if strings.Contains(internalErr.Error(), valid) {
		t.Fatalf("specific error echoed the raw token: %v", internalErr)
	}
}

// TestEntraRejectsMissingOrMalformedBearer proves the header parsing fails
// closed without echoing any credential.
func TestEntraRejectsMissingOrMalformedBearer(t *testing.T) {
	issuer := newFakeIssuer(t)
	authenticator := newTestAuthenticator(t, issuer.testConfig(fixedClock(time.Now())))

	cases := []struct {
		name   string
		header string
		set    bool
	}{
		{"absent", "", false},
		{"empty", "", true},
		{"no scheme", "opaque-token", true},
		{"wrong scheme", "Basic dXNlcjpwYXNz", true},
		{"bearer without value", "Bearer ", true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/api/v1/meta", nil)
			if testCase.set {
				request.Header.Set("Authorization", testCase.header)
			}
			if _, err := authenticator.Authenticate(request); err == nil {
				t.Fatal("a request without a valid bearer token was accepted")
			}
		})
	}
}

// TestNewEntraAuthenticatorValidatesConfig proves construction fails closed on
// bad configuration without any network access.
func TestNewEntraAuthenticatorValidatesConfig(t *testing.T) {
	cases := []struct {
		name   string
		config entraConfig
	}{
		{"missing audience", entraConfig{tenantID: testTenant}},
		{"missing tenant and issuer", entraConfig{audience: testAudience}},
		{
			"symmetric algorithm",
			entraConfig{
				tenantID:          testTenant,
				audience:          testAudience,
				allowedAlgorithms: []string{"HS256"},
			},
		},
		{
			"none algorithm",
			entraConfig{
				tenantID:          testTenant,
				audience:          testAudience,
				allowedAlgorithms: []string{"none"},
			},
		},
		{
			"excessive skew",
			entraConfig{
				tenantID:  testTenant,
				audience:  testAudience,
				clockSkew: time.Hour,
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := newEntraAuthenticator(testCase.config, time.Now); err == nil {
				t.Fatal("an invalid configuration was accepted")
			}
		})
	}
}

// TestSelectAuthenticatorEntraAllowsNonLoopback proves the unlock: a real
// authenticator makes a public listener serviceable, which the development
// principal never could.
func TestSelectAuthenticatorEntraAllowsNonLoopback(t *testing.T) {
	config := entraConfig{tenantID: testTenant, audience: testAudience}
	for _, address := range []string{"0.0.0.0:8080", "10.0.0.5:8080", "[::]:8080"} {
		authenticator, err := selectAuthenticator(false, config, address, time.Now)
		if err != nil {
			t.Fatalf("Entra authenticator refused on %s: %v", address, err)
		}
		if _, ok := authenticator.(*entraAuthenticator); !ok {
			t.Fatalf("selectAuthenticator returned %T, want *entraAuthenticator", authenticator)
		}
	}
}

// TestSelectAuthenticatorRejectsBothAuthenticators proves that supplying both
// -dev-auth and Entra configuration is a hard configuration error rather than a
// silent preference of one.
func TestSelectAuthenticatorRejectsBothAuthenticators(t *testing.T) {
	config := entraConfig{tenantID: testTenant, audience: testAudience}
	authenticator, err := selectAuthenticator(true, config, "127.0.0.1:8080", time.Now)
	if err == nil {
		t.Fatal("supplying both -dev-auth and Entra configuration was accepted")
	}
	if authenticator != nil {
		t.Fatal("a refused configuration still returned an authenticator")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("unclear diagnostic: %v", err)
	}
}

// TestSelectAuthenticatorEntraMisconfiguredFailsClosed proves an incomplete
// Entra configuration refuses rather than falling back to anonymous serving.
func TestSelectAuthenticatorEntraMisconfiguredFailsClosed(t *testing.T) {
	config := entraConfig{tenantID: testTenant} // no audience
	authenticator, err := selectAuthenticator(false, config, "0.0.0.0:8080", time.Now)
	if err == nil {
		t.Fatal("an incomplete Entra configuration was accepted")
	}
	if authenticator != nil {
		t.Fatal("a refused configuration still returned an authenticator")
	}
}

// TestEntraAuthenticatorNeverGetsDevelopmentBackfill proves the production
// authenticator can never trigger the development corpus backfill.
func TestEntraAuthenticatorNeverGetsDevelopmentBackfill(t *testing.T) {
	config := entraConfig{tenantID: testTenant, audience: testAudience}
	authenticator, err := selectAuthenticator(false, config, "0.0.0.0:8080", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	backfill := newDevelopmentBackfill(
		authenticator, "127.0.0.1:8080", auth.NewAuthority().Binder())
	if backfill != nil {
		t.Fatal("the Entra authenticator was granted a development backfill")
	}
}

func operationsContain(operations []auth.Operation, target auth.Operation) bool {
	for _, operation := range operations {
		if operation == target {
			return true
		}
	}
	return false
}

func grantsWorkspaceSource(decision auth.Decision) bool {
	for _, source := range decision.PermittedSourceIDs() {
		if bytes.Equal(source, workspaceSourceID) {
			return true
		}
	}
	return false
}

var _ webapi.Authenticator = (*entraAuthenticator)(nil)
