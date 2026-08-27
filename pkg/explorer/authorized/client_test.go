/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package authorized_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

var allOperations = []auth.Operation{
	auth.OperationIngest,
	auth.OperationList,
	auth.OperationRead,
	auth.OperationConnect,
	auth.OperationNeighborhood,
	auth.OperationRetrieve,
}

type fakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *fakeClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

type generationReader struct {
	mu          sync.RWMutex
	generations map[string]int64
	err         error
}

func (r *generationReader) CurrentPolicyGeneration(
	ctx context.Context,
	domain []byte,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.err != nil {
		return 0, r.err
	}
	return r.generations[string(domain)], nil
}

func (r *generationReader) Set(domain []byte, generation int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.generations[string(domain)] = generation
}

func (r *generationReader) SetError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

type fixture struct {
	base      *explorer.Explorer
	store     *authorized.MemoryPolicyStore
	clock     *fakeClock
	reader    *generationReader
	authority *auth.Authority
	clientA   *authorized.Client
	clientB   *authorized.Client
	domain    []byte
	sourceA   []byte
	policyA   []byte
	sourceB   []byte
	policyB   []byte
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	now := time.Date(2026, time.August, 26, 18, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	authority, err := auth.NewAuthorityWithClock(clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	base, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	f := &fixture{
		base:      base,
		store:     authorized.NewMemoryPolicyStore(),
		clock:     clock,
		reader:    &generationReader{generations: make(map[string]int64)},
		authority: authority,
		domain:    []byte("domain-one"),
		sourceA:   []byte("source-a"),
		policyA:   []byte("policy-a"),
		sourceB:   []byte("source-b"),
		policyB:   []byte("policy-b"),
	}
	f.reader.Set(f.domain, 1)
	f.clientA = f.newClient(t, f.base, f.store, f.sourceA, f.policyA, nil)
	f.clientB = f.newClient(t, f.base, f.store, f.sourceB, f.policyB, nil)
	return f
}

func (f *fixture) newClient(
	t *testing.T,
	base explorer.Client,
	store authorized.PolicyStore,
	source, policy []byte,
	edgeSelector authorized.EdgePolicySelector,
) *authorized.Client {
	t.Helper()
	selector, err := authorized.NewStaticPolicySelector(source, policy)
	if err != nil {
		t.Fatal(err)
	}
	client, err := authorized.NewClient(authorized.Config{
		Base:               base,
		Resolver:           f.authority.Resolver(),
		PolicySelector:     selector,
		EdgePolicySelector: edgeSelector,
		PolicyStore:        store,
		GenerationReader:   f.reader,
		Clock:              f.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func (f *fixture) decision(
	t *testing.T,
	subject string,
	sources, policies [][]byte,
	operations []auth.Operation,
) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject:               shoal.ID(subject),
		Actor:                 shoal.ID(subject + "-actor"),
		AuthorizationDomain:   f.domain,
		AllowedOperations:     operations,
		PermittedSourceIDs:    sources,
		PermittedPolicyIDs:    policies,
		PolicyGeneration:      1,
		AuthenticationExpires: f.clock.Now().Add(time.Hour),
		RequestID:             shoal.ID(subject + "-request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

func (f *fixture) context(
	t *testing.T,
	decision auth.Decision,
) context.Context {
	t.Helper()
	ctx, err := f.authority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func (f *fixture) admin(t *testing.T) context.Context {
	t.Helper()
	return f.context(t, f.decision(
		t,
		"admin",
		[][]byte{f.sourceA, f.sourceB},
		[][]byte{f.policyA, f.policyB},
		allOperations,
	))
}

func (f *fixture) alice(t *testing.T) context.Context {
	t.Helper()
	return f.context(t, f.decision(
		t,
		"alice",
		[][]byte{f.sourceA},
		[][]byte{f.policyA},
		allOperations,
	))
}

func (f *fixture) bob(t *testing.T) context.Context {
	t.Helper()
	return f.context(t, f.decision(
		t,
		"bob",
		[][]byte{f.sourceB},
		[][]byte{f.policyB},
		allOperations,
	))
}

func TestAuthorizedVisibilityAndRetrievalProjection(t *testing.T) {
	f := newFixture(t)
	metadata := shoal.Metadata{
		"authorization_domain": "forged-domain",
		"source_id":            "source-b",
		"policy_id":            "policy-b",
	}
	originalMetadata := cloneMetadata(metadata)
	visible, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///visible.txt",
		Title:     "Visible",
		MediaType: explorer.MediaTypeText,
		Content:   "alpha",
		Metadata:  metadata,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(metadata, originalMetadata) {
		t.Fatal("Ingest mutated caller metadata")
	}
	hidden, err := f.clientB.Ingest(f.admin(t), explorer.Source{
		URI:       "file:///hidden.txt",
		Title:     "Hidden alpha beta",
		MediaType: explorer.MediaTypeText,
		Content:   "alpha beta",
	})
	if err != nil {
		t.Fatal(err)
	}

	alice := f.alice(t)
	summaries, err := f.clientA.Documents(alice)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 || summaries[0].Document.ID != visible.Document.ID {
		t.Fatalf("visible summaries = %#v", summaries)
	}

	hiddenView, err := f.clientB.Document(
		f.bob(t), hidden.Document.ID, hidden.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	hiddenSpanID := firstSpanID(t, hiddenView)
	hiddenErr := documentError(
		f.clientA, alice, hidden.Document.ID, hidden.Revision.ID)
	absentErr := documentError(f.clientA, alice, "absent-document", "absent-revision")
	if hiddenErr.Error() != absentErr.Error() ||
		!shoal.IsErrorCode(hiddenErr, shoal.ErrorNotFound) {
		t.Fatalf("hidden error %v differs from absent %v", hiddenErr, absentErr)
	}

	raw, err := f.base.Retrieve(context.Background(), retrieval.Request{
		Text: "alpha beta", TopK: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(raw.Results) != 1 ||
		raw.Results[0].Evidence[0].Citation.DocumentID != hidden.Document.ID {
		t.Fatalf("raw scorer did not prove hidden displacement: %#v", raw)
	}
	response, err := f.clientA.Retrieve(alice, retrieval.Request{
		Text: "alpha beta", TopK: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 ||
		response.Results[0].Evidence[0].Citation.DocumentID != visible.Document.ID {
		t.Fatalf("authorized retrieval = %#v", response)
	}

	empty, err := f.clientA.Retrieve(alice, retrieval.Request{
		Text: "alpha beta",
		Scope: retrieval.Scope{DocumentIDs: []shoal.ID{
			hidden.Document.ID,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Results) != 0 {
		t.Fatalf("hidden document scope broadened: %#v", empty)
	}
	empty, err = f.clientA.Retrieve(alice, retrieval.Request{
		Text: "alpha beta",
		Scope: retrieval.Scope{NodeIDs: []shoal.ID{
			hiddenSpanID,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Results) != 0 {
		t.Fatalf("hidden node scope broadened: %#v", empty)
	}

	documentScope := []shoal.ID{hidden.Document.ID, visible.Document.ID}
	nodeScope := []shoal.ID{hiddenSpanID}
	modes := []retrieval.Mode{retrieval.ModeLexical}
	request := retrieval.Request{
		Text:  "alpha beta",
		Modes: modes,
		Scope: retrieval.Scope{
			DocumentIDs: documentScope,
			NodeIDs:     nodeScope,
		},
	}
	documentCopy := append([]shoal.ID(nil), documentScope...)
	nodeCopy := append([]shoal.ID(nil), nodeScope...)
	modeCopy := append([]retrieval.Mode(nil), modes...)
	if _, err := f.clientA.Retrieve(alice, request); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(documentScope, documentCopy) ||
		!reflect.DeepEqual(nodeScope, nodeCopy) ||
		!reflect.DeepEqual(modes, modeCopy) {
		t.Fatal("Retrieve mutated caller slices")
	}
}

func TestResolverExpiryOperationAndSelectorFailures(t *testing.T) {
	f := newFixture(t)
	if _, err := f.clientA.Documents(context.Background()); !shoal.IsErrorCode(
		err, shoal.ErrorUnauthorized,
	) {
		t.Fatalf("missing context error = %v", err)
	}

	decision := f.decision(
		t,
		"forged",
		[][]byte{f.sourceA},
		[][]byte{f.policyA},
		allOperations,
	)
	otherAuthority, err := auth.NewAuthorityWithClock(f.clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	forged, err := otherAuthority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.clientA.Documents(forged); !shoal.IsErrorCode(
		err, shoal.ErrorUnauthorized,
	) {
		t.Fatalf("forged context error = %v", err)
	}
	canceled, cancel := context.WithCancel(f.alice(t))
	cancel()
	if _, err := f.clientA.Documents(canceled); !shoal.IsErrorCode(
		err, shoal.ErrorCanceled,
	) {
		t.Fatalf("cancellation error = %v", err)
	}
	deadline, cancelDeadline := context.WithDeadline(
		f.alice(t), time.Now().Add(-time.Second))
	defer cancelDeadline()
	if _, err := f.clientA.Documents(deadline); !shoal.IsErrorCode(
		err, shoal.ErrorDeadline,
	) {
		t.Fatalf("deadline error = %v", err)
	}

	readOnly := f.context(t, f.decision(
		t,
		"reader",
		[][]byte{f.sourceA},
		[][]byte{f.policyA},
		[]auth.Operation{auth.OperationRead},
	))
	if _, err := f.clientA.Documents(readOnly); !shoal.IsErrorCode(
		err, shoal.ErrorUnauthorized,
	) {
		t.Fatalf("operation denial = %v", err)
	}

	expiringDecision, err := auth.NewDecision(auth.DecisionConfig{
		Subject:               "expiring",
		Actor:                 "expiring-actor",
		AuthorizationDomain:   f.domain,
		AllowedOperations:     allOperations,
		PermittedSourceIDs:    [][]byte{f.sourceA},
		PermittedPolicyIDs:    [][]byte{f.policyA},
		PolicyGeneration:      1,
		AuthenticationExpires: f.clock.Now().Add(time.Minute),
		RequestID:             "expiring-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	expiring := f.context(t, expiringDecision)
	originalNow := f.clock.Now()
	f.clock.Set(originalNow.Add(2 * time.Minute))
	if _, err := f.clientA.Documents(expiring); !shoal.IsErrorCode(
		err, shoal.ErrorUnauthorized,
	) {
		t.Fatalf("expiry error = %v", err)
	}
	f.clock.Set(originalNow)

	maliciousSelector := authorized.PolicySelectorFunc(func(
		_ context.Context,
		decision auth.Decision,
		_ explorer.Source,
	) (auth.Policy, error) {
		return auth.NewPolicy(auth.PolicyConfig{
			AuthorizationDomain: decision.AuthorizationDomain(),
			SourceID:            f.sourceB,
			GrantPolicyID:       f.policyB,
			Epoch:               decision.PolicyGeneration(),
		})
	})
	client, err := authorized.NewClient(authorized.Config{
		Base:           f.base,
		Resolver:       f.authority.Resolver(),
		PolicySelector: maliciousSelector,
		EdgePolicySelector: authorized.EdgePolicySelectorFunc(func(
			_ context.Context,
			decision auth.Decision,
			_ graph.Edge,
		) (auth.Policy, error) {
			return auth.NewPolicy(auth.PolicyConfig{
				AuthorizationDomain: decision.AuthorizationDomain(),
				SourceID:            f.sourceA,
				GrantPolicyID:       f.policyA,
				Epoch:               decision.PolicyGeneration(),
			})
		}),
		PolicyStore:      f.store,
		GenerationReader: f.reader,
		Clock:            f.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	alice := f.alice(t)
	if _, err := client.Ingest(alice, explorer.Source{
		URI: "file:///rejected.txt", MediaType: explorer.MediaTypeText,
		Content: "must not commit",
	}); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("out-of-grant selector error = %v", err)
	}
	summaries, err := f.base.Documents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 0 {
		t.Fatalf("selector denial committed data: %#v", summaries)
	}
}

func TestPolicyStoreFailureRetryAndImmutablePolicyConflict(t *testing.T) {
	f := newFixture(t)
	failing := &failStore{PolicyStore: f.store, revisionFailures: 1}
	client := f.newClient(t, f.base, failing, f.sourceA, f.policyA, nil)
	source := explorer.Source{
		URI: "file:///retry.txt", MediaType: explorer.MediaTypeText,
		Content: "retry reconciliation",
	}
	if _, err := client.Ingest(f.admin(t), source); !shoal.IsErrorCode(
		err, shoal.ErrorUnavailable,
	) {
		t.Fatalf("first catalog failure = %v", err)
	}
	raw, err := f.base.Documents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 1 {
		t.Fatalf("base commit missing after catalog failure: %#v", raw)
	}
	result, err := client.Ingest(f.admin(t), source)
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != explorer.IngestUnchanged {
		t.Fatalf("retry disposition = %q", result.Disposition)
	}
	if _, err := client.Document(
		f.alice(t), result.Document.ID, result.Revision.ID); err != nil {
		t.Fatal(err)
	}

	conflicting := f.newClient(t, f.base, f.store, f.sourceB, f.policyB, nil)
	if _, err := conflicting.Ingest(f.admin(t), source); !shoal.IsErrorCode(
		err, shoal.ErrorConflict,
	) {
		t.Fatalf("immutable policy reuse error = %v", err)
	}
}

func TestHistoricalRevisionRetryIsIdempotent(t *testing.T) {
	f := newFixture(t)
	admin := f.admin(t)
	firstSource := explorer.Source{
		URI: "file:///history.txt", MediaType: explorer.MediaTypeText,
		Content: "first historical revision",
	}
	first, err := f.clientA.Ingest(admin, firstSource)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.clientA.Ingest(admin, explorer.Source{
		URI: "file:///history.txt", MediaType: explorer.MediaTypeText,
		Content: "second current revision",
	})
	if err != nil {
		t.Fatal(err)
	}
	retried, err := f.clientA.Ingest(admin, firstSource)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Disposition != explorer.IngestUnchanged ||
		retried.Revision.ID != first.Revision.ID {
		t.Fatalf("historical retry = %#v", retried)
	}
	summaries, err := f.clientA.Documents(f.alice(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 || summaries[0].Revision.ID != second.Revision.ID {
		t.Fatalf("historical retry changed current revision: %#v", summaries)
	}
}

func TestGenerationChangeAfterBaseOperation(t *testing.T) {
	f := newFixture(t)
	hooked := &hookClient{
		Client: f.base,
		afterIngest: func() {
			f.reader.Set(f.domain, 2)
		},
	}
	client := f.newClient(t, hooked, f.store, f.sourceA, f.policyA, nil)
	_, err := client.Ingest(f.admin(t), explorer.Source{
		URI: "file:///generation.txt", MediaType: explorer.MediaTypeText,
		Content: "generation changes after commit",
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("generation change error = %v", err)
	}
}

func TestUnreadableGenerationIsUnavailable(t *testing.T) {
	f := newFixture(t)
	if _, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///generation-unreadable.txt", MediaType: explorer.MediaTypeText,
		Content: "generation reader failure",
	}); err != nil {
		t.Fatal(err)
	}
	f.reader.SetError(errors.New("reader failed"))
	if _, err := f.clientA.Documents(f.alice(t)); !shoal.IsErrorCode(
		err, shoal.ErrorUnavailable,
	) {
		t.Fatalf("unreadable generation error = %v", err)
	}
}

func TestMaliciousRetrievalResultFailsClosed(t *testing.T) {
	f := newFixture(t)
	visible, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///visible-malicious.txt", MediaType: explorer.MediaTypeText,
		Content: "visible token",
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = visible
	hidden, err := f.clientB.Ingest(f.admin(t), explorer.Source{
		URI: "file:///hidden-malicious.txt", MediaType: explorer.MediaTypeText,
		Content: "hidden token",
	})
	if err != nil {
		t.Fatal(err)
	}
	hiddenResponse, err := f.base.Retrieve(context.Background(), retrieval.Request{
		Text: "hidden token",
		Scope: retrieval.Scope{DocumentIDs: []shoal.ID{
			hidden.Document.ID,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	malicious := &hookClient{
		Client: f.base,
		retrieve: func(
			context.Context,
			retrieval.Request,
		) (retrieval.Response, error) {
			return hiddenResponse, nil
		},
	}
	client := f.newClient(t, malicious, f.store, f.sourceA, f.policyA, nil)
	_, err = client.Retrieve(f.alice(t), retrieval.Request{Text: "hidden token"})
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("malicious result error = %v", err)
	}
}

func TestRestartWithReusedPolicyStore(t *testing.T) {
	dataDir := t.TempDir()
	base, err := explorer.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 26, 19, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now}
	authority, err := auth.NewAuthorityWithClock(clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	reader := &generationReader{generations: map[string]int64{"restart-domain": 1}}
	store := authorized.NewMemoryPolicyStore()
	selector, err := authorized.NewStaticPolicySelector(
		[]byte("restart-source"), []byte("restart-policy"))
	if err != nil {
		t.Fatal(err)
	}
	newClient := func(base explorer.Client) *authorized.Client {
		client, err := authorized.NewClient(authorized.Config{
			Base:             base,
			Resolver:         authority.Resolver(),
			PolicySelector:   selector,
			PolicyStore:      store,
			GenerationReader: reader,
			Clock:            clock.Now,
		})
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject:               "restart-user",
		Actor:                 "restart-actor",
		AuthorizationDomain:   []byte("restart-domain"),
		AllowedOperations:     allOperations,
		PermittedSourceIDs:    [][]byte{[]byte("restart-source")},
		PermittedPolicyIDs:    [][]byte{[]byte("restart-policy")},
		PolicyGeneration:      1,
		AuthenticationExpires: now.Add(time.Hour),
		RequestID:             "restart-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := authority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	client := newClient(base)
	result, err := client.Ingest(ctx, explorer.Source{
		URI: "file:///restart.txt", MediaType: explorer.MediaTypeText,
		Content: "survives wrapped client restart",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := base.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := explorer.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restarted := newClient(reopened)
	if _, err := restarted.Document(
		ctx, result.Document.ID, result.Revision.ID); err != nil {
		t.Fatal(err)
	}
	summaries, err := restarted.Documents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 {
		t.Fatalf("restarted summaries = %#v", summaries)
	}
}

func TestConcurrentAuthorizedAccess(t *testing.T) {
	f := newFixture(t)
	result, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///concurrent.txt", MediaType: explorer.MediaTypeText,
		Content: "concurrent authorized access",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := f.alice(t)
	var wait sync.WaitGroup
	errorsSeen := make(chan error, 96)
	for index := 0; index < 32; index++ {
		wait.Add(3)
		go func() {
			defer wait.Done()
			_, err := f.clientA.Documents(ctx)
			errorsSeen <- err
		}()
		go func() {
			defer wait.Done()
			_, err := f.clientA.Document(
				ctx, result.Document.ID, result.Revision.ID)
			errorsSeen <- err
		}()
		go func() {
			defer wait.Done()
			_, err := f.clientA.Retrieve(ctx, retrieval.Request{
				Text: "concurrent",
			})
			errorsSeen <- err
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
}

type failStore struct {
	authorized.PolicyStore
	mu               sync.Mutex
	revisionFailures int
	edgeFailures     int
}

func (s *failStore) PutRevision(
	ctx context.Context,
	registration authorized.RevisionRegistration,
) error {
	s.mu.Lock()
	if s.revisionFailures > 0 {
		s.revisionFailures--
		s.mu.Unlock()
		return errors.New("catalog unavailable")
	}
	s.mu.Unlock()
	return s.PolicyStore.PutRevision(ctx, registration)
}

func (s *failStore) PutEdge(
	ctx context.Context,
	registration authorized.EdgeRegistration,
) error {
	s.mu.Lock()
	if s.edgeFailures > 0 {
		s.edgeFailures--
		s.mu.Unlock()
		return errors.New("catalog unavailable")
	}
	s.mu.Unlock()
	return s.PolicyStore.PutEdge(ctx, registration)
}

type hookClient struct {
	explorer.Client
	afterIngest func()
	retrieve    func(context.Context, retrieval.Request) (retrieval.Response, error)
}

func (c *hookClient) Ingest(
	ctx context.Context,
	source explorer.Source,
) (explorer.IngestResult, error) {
	result, err := c.Client.Ingest(ctx, source)
	if c.afterIngest != nil {
		c.afterIngest()
	}
	return result, err
}

func (c *hookClient) Retrieve(
	ctx context.Context,
	request retrieval.Request,
) (retrieval.Response, error) {
	if c.retrieve != nil {
		return c.retrieve(ctx, request)
	}
	return c.Client.Retrieve(ctx, request)
}

func documentError(
	client explorer.Client,
	ctx context.Context,
	documentID, revisionID shoal.ID,
) error {
	_, err := client.Document(ctx, documentID, revisionID)
	return err
}

func firstSpanID(t *testing.T, view explorer.DocumentView) shoal.ID {
	t.Helper()
	var visit func(explorer.SectionView) shoal.ID
	visit = func(section explorer.SectionView) shoal.ID {
		if len(section.Spans) > 0 {
			return section.Spans[0].ID
		}
		for _, child := range section.Children {
			if id := visit(child); id != "" {
				return id
			}
		}
		return ""
	}
	id := visit(view.Root)
	if id == "" {
		t.Fatal("document has no span")
	}
	return id
}

func cloneMetadata(value shoal.Metadata) shoal.Metadata {
	cloned := make(shoal.Metadata, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}
