// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package webapi

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	decisionapi "github.com/phrocker/shoal-oss/pkg/decision/api"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type registrationHTTPStub struct {
	calls     int
	selection decisionapi.RegistrationSelection
	key       []byte
	reply     decisionapi.RegistrationReceipt
	err       error
	during    func()
}

func (p *registrationHTTPStub) Register(_ context.Context, selection decisionapi.RegistrationSelection, key []byte) (decisionapi.RegistrationReceipt, error) {
	p.calls++
	p.selection = selection
	p.key = append([]byte(nil), key...)
	if p.during != nil {
		p.during()
	}
	return p.reply, p.err
}

func (p *registrationHTTPStub) Read(_ context.Context, _ shoal.ID) (decisionapi.RegistrationReceipt, error) {
	p.calls++
	if p.during != nil {
		p.during()
	}
	return p.reply, p.err
}

func registrationHTTPSelection() decisionapi.RegistrationSelection {
	return decisionapi.RegistrationSelection{
		ProfileID:         "profile:test",
		ProfileRevisionID: "profile-revision:test",
		Sources: []decisionapi.RegistrationSourceInput{{
			ObservationID: shoal.ID("observation:" + strings.Repeat("1", 64)),
			Bytes:         []byte("registration source bytes"),
		}},
	}
}

func registrationHTTPReceipt(selection decisionapi.RegistrationSelection) decisionapi.RegistrationReceipt {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	sourceHash := sha256.Sum256(selection.Sources[0].Bytes)
	id := shoal.ID("decision-registration:" + strings.Repeat("a", 64))
	return decisionapi.RegistrationReceipt{
		ID: id,
		Scope: decisionapi.RegistrationScope{
			Domain: []byte("private-domain"), SubjectID: "subject", ActorID: "actor",
			ClientID: "client", OnBehalfOf: []shoal.ID{},
		},
		State: "ready", Version: 2, FrozenSHA256: strings.Repeat("f", 64),
		CreatedAt: now.Add(time.Second), UpdatedAt: now.Add(2 * time.Second), ReadyAt: now.Add(2 * time.Second),
		Snapshot: decisionapi.RegistrationSnapshot{
			SelectionSHA256: strings.Repeat("b", 64),
			ProfileID:       selection.ProfileID, ProfileRevisionID: selection.ProfileRevisionID,
			BuilderID: "builder", RequestID: id, TaskID: "task", PictureID: "picture",
			PredictorID: "predictor", AcceptedAt: now, AuthenticationExpiresAt: now.Add(time.Hour),
			AuthorizationFingerprint: "auth-sha256:" + strings.Repeat("c", 64),
			RecordSHA256:             strings.Repeat("d", 64), RecordBytes: 128,
			Sources: []decisionapi.RegistrationSourcePin{{
				Mode: "imported", CollectorID: "collector", ObservationID: selection.Sources[0].ObservationID,
				ArtifactID: "artifact", EnrollmentID: "enrollment", AuthorityPolicyID: "authority-policy",
				Generation: 1, ArtifactSHA256: hex.EncodeToString(sourceHash[:]),
				SourceSHA256: strings.Repeat("e", 64), ReceivedAt: now.Add(-time.Second),
			}},
		},
	}
}

func registrationHTTPFixture(t *testing.T) (*Handler, *registrationHTTPStub, *auth.Authority, auth.Decision) {
	t.Helper()
	h, _, authority, decision := decisionHTTPFixture(t)
	selection := registrationHTTPSelection()
	provider := &registrationHTTPStub{reply: registrationHTTPReceipt(selection)}
	if err := h.MountRegistrations(provider, authority.Resolver()); err != nil {
		t.Fatal(err)
	}
	return h, provider, authority, decision
}

func registrationHTTPRequest(method string) *http.Request {
	selection := registrationHTTPSelection()
	path := RegistrationsRoute
	body := ""
	if method == http.MethodPost {
		encoded, err := decisionapi.EncodeRegistrationRequest(selection)
		if err != nil {
			panic(err)
		}
		body = string(encoded)
	} else {
		path += "/" + decisionapi.EncodeID(registrationHTTPReceipt(selection).ID)
	}
	request := httptest.NewRequest(method, "http://example.test"+path, strings.NewReader(body))
	if method == http.MethodPost {
		request.Header.Set("Idempotency-Key", decisionapi.EncodeKey([]byte{0, 255, 'k'}))
		request.Header.Set("Content-Type", "application/json")
	}
	return request
}

func TestRegistrationHTTPRoundTripAndDisclosure(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			handler, provider, _, _ := registrationHTTPFixture(t)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, registrationHTTPRequest(method))
			if recorder.Code != http.StatusOK || provider.calls != 1 ||
				recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("%d calls=%d %s", recorder.Code, provider.calls, recorder.Body)
			}
			receipt, err := decisionapi.DecodeRegistrationReceipt(recorder.Body.Bytes())
			if err != nil || receipt.ID != provider.reply.ID ||
				receipt.Snapshot.Sources[0].Mode != "imported" ||
				receipt.Snapshot.Sources[0].ArtifactSHA256 != provider.reply.Snapshot.Sources[0].ArtifactSHA256 {
				t.Fatalf("receipt mismatch: %v %#v", err, receipt)
			}
			if strings.Contains(recorder.Body.String(), string(registrationHTTPSelection().Sources[0].Bytes)) ||
				(len(provider.key) > 0 && strings.Contains(recorder.Body.String(), string(provider.key))) ||
				strings.Contains(recorder.Body.String(), "private-domain") {
				t.Fatal("raw source or authorization data disclosed")
			}
			if method == http.MethodPost && (provider.selection.ProfileID != registrationHTTPSelection().ProfileID ||
				provider.selection.Sources[0].ObservationID != registrationHTTPSelection().Sources[0].ObservationID ||
				string(provider.key) != string([]byte{0, 255, 'k'})) {
				t.Fatal("provider did not receive submitted selection and canonical key")
			}
		})
	}
}

func TestRegistrationHTTPMountRequiresTrustedTransport(t *testing.T) {
	handler, provider, authority, _ := registrationHTTPFixture(t)
	anonymous, _ := NewHandler(&stubWorkspaceService{}, "example.test")
	if anonymous.MountRegistrations(provider, authority.Resolver()) == nil ||
		handler.MountRegistrations(provider, authority.Resolver()) == nil {
		t.Fatal("unsafe or duplicate mount accepted")
	}
	var nilProvider *registrationHTTPStub
	if _, err := NewRegistrationHTTPHandler(nilProvider, authority.Resolver()); err == nil {
		t.Fatal("typed nil provider accepted")
	}
	if _, err := NewRegistrationHTTPHandler(provider, nil); err == nil {
		t.Fatal("nil resolver accepted")
	}
	if operation, ok := workspaceOperationForRequest(http.MethodPost, RegistrationsRoute); !ok ||
		operation != auth.OperationIngest {
		t.Fatal("registration writes are not workspace-bound")
	}
	if operation, ok := workspaceOperationForRequest(
		http.MethodGet, RegistrationsRoute+"/"+decisionapi.EncodeID(registrationHTTPReceipt(registrationHTTPSelection()).ID),
	); !ok || operation != auth.OperationRead {
		t.Fatal("registration reads are not workspace-bound")
	}
	standalone, _ := NewRegistrationHTTPHandler(provider, authority.Resolver())
	recorder := httptest.NewRecorder()
	standalone.ServeHTTP(recorder, registrationHTTPRequest(http.MethodPost))
	if recorder.Code != http.StatusUnauthorized || provider.calls != 0 {
		t.Fatalf("unbound request reached provider: %d calls=%d", recorder.Code, provider.calls)
	}
}

func TestRegistrationHTTPRejectsMalformedRequestsBeforeProvider(t *testing.T) {
	cases := map[string]func(*http.Request){
		"query":        func(r *http.Request) { r.URL.RawQuery = "x=1" },
		"empty-query":  func(r *http.Request) { r.URL.ForceQuery = true },
		"encoded-path": func(r *http.Request) { r.URL.RawPath = "/api/v1/%64ecision-registrations" },
		"item-post": func(r *http.Request) {
			r.URL.Path += "/" + decisionapi.EncodeID(registrationHTTPReceipt(registrationHTTPSelection()).ID)
		},
		"cookie":           func(r *http.Request) { r.Header["Cookie"] = []string{""} },
		"origin":           func(r *http.Request) { r.Header.Set("Origin", "http://evil.test") },
		"fetch-site":       func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") },
		"duplicate-origin": func(r *http.Request) { r.Header["Origin"] = []string{"http://example.test", "http://example.test"} },
		"missing-key":      func(r *http.Request) { r.Header.Del("Idempotency-Key") },
		"padded-key":       func(r *http.Request) { r.Header.Set("Idempotency-Key", "aw==") },
		"duplicate-key":    func(r *http.Request) { r.Header["idempotency-key"] = []string{"aw"} },
		"encoding":         func(r *http.Request) { r.Header.Set("Content-Encoding", "identity") },
		"content-type":     func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") },
		"request-bound":    func(r *http.Request) { r.ContentLength = decisionapi.MaxRegistrationRequestBytes + 1 },
		"body-bound": func(r *http.Request) {
			r.Body = io.NopCloser(strings.NewReader(strings.Repeat("x", decisionapi.MaxRegistrationRequestBytes+1)))
			r.ContentLength = -1
		},
		"source-bound": func(r *http.Request) {
			bytes := base64.StdEncoding.EncodeToString(make([]byte, decisionapi.MaxRegistrationSourceBytes+1))
			body := `{"schema":1,"selection":{"profile_id":"` + decisionapi.EncodeID("profile:test") +
				`","profile_revision_id":"` + decisionapi.EncodeID("profile-revision:test") +
				`","sources":[{"observation_id":"` + decisionapi.EncodeID(registrationHTTPSelection().Sources[0].ObservationID) +
				`","bytes":"` + bytes + `"}]}}`
			r.Body = io.NopCloser(strings.NewReader(body))
			r.ContentLength = -1
		},
		"malformed-json": func(r *http.Request) { r.Body = io.NopCloser(strings.NewReader("{")) },
		"unknown-key": func(r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(strings.NewReader(strings.Replace(string(b), `"schema":1`, `"schema":1,"unknown":true`, 1)))
		},
		"duplicate-key-json": func(r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(strings.NewReader(strings.Replace(string(b), `"schema":1`, `"schema":1,"schema":1`, 1)))
		},
		"wrong-case-key": func(r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(strings.NewReader(strings.Replace(string(b), `"schema"`, `"Schema"`, 1)))
		},
		"wrong-schema": func(r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(strings.NewReader(strings.Replace(string(b), `"schema":1`, `"schema":2`, 1)))
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			handler, provider, _, _ := registrationHTTPFixture(t)
			request := registrationHTTPRequest(http.MethodPost)
			mutate(request)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code < 400 || provider.calls != 0 ||
				recorder.Header().Get(CommitOutcomeHeader) != "" {
				t.Fatalf("%d calls=%d %s", recorder.Code, provider.calls, recorder.Body)
			}
		})
	}
	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			handler, provider, _, _ := registrationHTTPFixture(t)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, registrationHTTPRequest(method))
			if recorder.Code != http.StatusMethodNotAllowed || provider.calls != 0 {
				t.Fatalf("%d calls=%d", recorder.Code, provider.calls)
			}
		})
	}
}

func TestRegistrationHTTPRejectsMalformedReads(t *testing.T) {
	for _, kind := range []string{"key", "body", "query", "empty-query", "collection", "extra-path", "padding"} {
		t.Run(kind, func(t *testing.T) {
			handler, provider, _, _ := registrationHTTPFixture(t)
			request := registrationHTTPRequest(http.MethodGet)
			switch kind {
			case "key":
				request.Header["Idempotency-Key"] = []string{""}
			case "body":
				request.Body = io.NopCloser(strings.NewReader("x"))
			case "query":
				request.URL.RawQuery = "x=1"
			case "empty-query":
				request.URL.ForceQuery = true
			case "collection":
				request.URL.Path = RegistrationsRoute
			case "extra-path":
				request.URL.Path += "/other"
			case "padding":
				request.URL.Path += "="
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest || provider.calls != 0 ||
				recorder.Header().Get(CommitOutcomeHeader) != "" {
				t.Fatalf("%d calls=%d %s", recorder.Code, provider.calls, recorder.Body)
			}
		})
	}
}

func TestRegistrationHTTPPostFailuresAreIndeterminate(t *testing.T) {
	for _, kind := range []string{"provider", "not-found", "profile-binding", "source-binding", "bad-mode", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			handler, provider, _, _ := registrationHTTPFixture(t)
			request := registrationHTTPRequest(http.MethodPost)
			switch kind {
			case "provider":
				provider.err = errors.New("private internal detail")
			case "not-found":
				provider.err = shoal.NewError(shoal.ErrorNotFound, "private detail")
			case "profile-binding":
				provider.reply.Snapshot.ProfileID = "substituted-profile"
			case "source-binding":
				provider.reply.Snapshot.Sources[0].ArtifactSHA256 = strings.Repeat("9", 64)
			case "bad-mode":
				provider.reply.Snapshot.Sources[0].Mode = "grant"
			case "cancel":
				ctx, cancel := context.WithCancel(request.Context())
				request = request.WithContext(ctx)
				provider.during = cancel
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusServiceUnavailable || provider.calls != 1 ||
				recorder.Header().Get(CommitOutcomeHeader) != CommitOutcomeIndeterminate ||
				!strings.Contains(recorder.Body.String(), `"indeterminate":true`) {
				t.Fatalf("%d calls=%d %s", recorder.Code, provider.calls, recorder.Body)
			}
			if strings.Contains(recorder.Body.String(), "private") {
				t.Fatal("provider error leaked")
			}
		})
	}
}

func TestRegistrationHTTPAcceptsServerObservedSourceMode(t *testing.T) {
	handler, provider, _, _ := registrationHTTPFixture(t)
	provider.reply.Snapshot.Sources[0].Mode = "server_observed"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, registrationHTTPRequest(http.MethodPost))
	if recorder.Code != http.StatusOK || provider.calls != 1 {
		t.Fatalf("%d calls=%d %s", recorder.Code, provider.calls, recorder.Body)
	}
	receipt, err := decisionapi.DecodeRegistrationReceipt(recorder.Body.Bytes())
	if err != nil || receipt.Snapshot.Sources[0].Mode != "server_observed" {
		t.Fatalf("mode provenance mismatch: %v %#v", err, receipt.Snapshot.Sources)
	}
}

func TestRegistrationHTTPReadsOnlyReadyIdentityAndNeverBecomeIndeterminate(t *testing.T) {
	for _, kind := range []string{"provider-error", "wrong-id", "not-ready"} {
		t.Run(kind, func(t *testing.T) {
			handler, provider, _, _ := registrationHTTPFixture(t)
			switch kind {
			case "provider-error":
				provider.err = errors.New("private read failure")
			case "wrong-id":
				provider.reply.ID = shoal.ID("decision-registration:" + strings.Repeat("9", 64))
				provider.reply.Snapshot.RequestID = provider.reply.ID
			case "not-ready":
				provider.reply.State = "preparing"
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, registrationHTTPRequest(http.MethodGet))
			if recorder.Code < 400 || provider.calls != 1 ||
				recorder.Header().Get(CommitOutcomeHeader) != "" ||
				strings.Contains(recorder.Body.String(), "private") {
				t.Fatalf("%d calls=%d %s", recorder.Code, provider.calls, recorder.Body)
			}
		})
	}
}

func TestRegistrationHTTPResponseBounds(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			_, provider, authority, decision := registrationHTTPFixture(t)
			handler, err := NewRegistrationHTTPHandler(provider, authority.Resolver())
			if err != nil {
				t.Fatal(err)
			}
			request := registrationHTTPRequest(method)
			ctx, err := authority.Binder().Bind(request.Context(), decision)
			if err != nil {
				t.Fatal(err)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(workspaceResponseWriter{
				ResponseWriter: recorder, maxResponseBytes: 128,
				indeterminateOnOverflow: requestMayCommit(request.Method, request.URL.Path),
			}, request.WithContext(ctx))
			uncertain := method == http.MethodPost
			if recorder.Code != http.StatusServiceUnavailable || provider.calls != 1 ||
				(recorder.Header().Get(CommitOutcomeHeader) == CommitOutcomeIndeterminate) != uncertain {
				t.Fatalf("%d calls=%d outcome=%q", recorder.Code, provider.calls, recorder.Header().Get(CommitOutcomeHeader))
			}
			if strings.Contains(recorder.Body.String(), string(provider.reply.ID)) {
				t.Fatal("oversize response disclosed")
			}
		})
	}
}

type registrationResolver struct {
	decision auth.Decision
	err      error
	calls    int
	denyAt   int
}

func (r *registrationResolver) Resolve(context.Context) (auth.Decision, error) {
	r.calls++
	if r.err != nil || r.calls == r.denyAt {
		return auth.Decision{}, authenticationDenied()
	}
	return r.decision, nil
}

func TestRegistrationHTTPResolverFailsClosed(t *testing.T) {
	for _, kind := range []string{"delegated", "expired", "revoked", "revoked-after-call", "revoked-after-read"} {
		t.Run(kind, func(t *testing.T) {
			_, provider, authority, decision := registrationHTTPFixture(t)
			resolver := &registrationResolver{decision: decision}
			switch kind {
			case "delegated":
				delegated, err := auth.NewDecision(auth.DecisionConfig{
					Subject: "subject", Actor: "actor", OnBehalfOf: []shoal.ID{"delegator"},
					AuthorizationDomain: []byte("domain"), AllowedOperations: []auth.Operation{auth.OperationInvoke, auth.OperationRead},
					PolicyGeneration: 1, AuthenticationExpires: time.Now().Add(time.Hour), RequestID: "delegated-request",
				})
				if err != nil {
					t.Fatal(err)
				}
				resolver.decision = delegated
			case "expired":
				expired, err := auth.NewDecision(auth.DecisionConfig{
					Subject: "subject", Actor: "actor", AuthorizationDomain: []byte("domain"),
					AllowedOperations: []auth.Operation{auth.OperationInvoke, auth.OperationRead},
					PolicyGeneration:  1, AuthenticationExpires: time.Now().Add(-time.Hour), RequestID: "expired-request",
				})
				if err != nil {
					t.Fatal(err)
				}
				resolver.decision = expired
			case "revoked":
				resolver.err = errors.New("revoked")
			case "revoked-after-call", "revoked-after-read":
				resolver.denyAt = 3
			}
			handler, err := NewRegistrationHTTPHandler(provider, resolver)
			if err != nil {
				t.Fatal(err)
			}
			method := http.MethodPost
			if kind == "revoked-after-read" {
				method = http.MethodGet
			}
			request := registrationHTTPRequest(method)
			ctx, err := authority.Binder().Bind(request.Context(), decision)
			if err != nil {
				t.Fatal(err)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request.WithContext(ctx))
			expectedCalls := 0
			if kind == "revoked-after-call" || kind == "revoked-after-read" {
				expectedCalls = 1
			}
			uncertain := kind == "revoked-after-call"
			if provider.calls != expectedCalls || recorder.Code < 400 ||
				(recorder.Header().Get(CommitOutcomeHeader) == CommitOutcomeIndeterminate) != uncertain {
				t.Fatalf("calls=%d status=%d outcome=%q", provider.calls, recorder.Code, recorder.Header().Get(CommitOutcomeHeader))
			}
		})
	}
}
