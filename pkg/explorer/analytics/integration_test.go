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

package analytics_test

import (
	"bytes"
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/analytics"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestAuthorizedAnalyticsHiddenTopologyDoesNotInfluenceOutput(t *testing.T) {
	fixture := newAnalyticsFixture(t)
	a1 := fixture.ingest(t, fixture.clientA, "memory://a1", "alpha")
	a2 := fixture.ingest(t, fixture.clientA, "memory://a2", "bravo")
	a3 := fixture.ingest(t, fixture.clientA, "memory://a3", "charlie")
	fixture.connect(t, fixture.clientA, "edge-a1-a2", a1, a2)
	fixture.connect(t, fixture.clientA, "edge-a1-a3", a1, a3)
	fixture.connect(t, fixture.clientA, "edge-a2-a3", a2, a3)

	service := fixture.service(t)
	request := analyticsRequest(a1)
	alice := fixture.readContext(t, "alice", fixture.sourceA, fixture.policyA, ontology.OntologyIdentity{})
	before, err := service.Run(alice, request)
	if err != nil {
		t.Fatal(err)
	}

	hidden := fixture.ingest(t, fixture.clientB, "memory://hidden", "hidden")
	fixture.connect(t, fixture.clientA, "edge-a1-hidden", a1, hidden)
	after, err := service.Run(alice, request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("hidden topology changed authorized analytics:\nbefore=%#v\nafter=%#v", before, after)
	}

	admin, err := service.Run(fixture.adminContext(t), request)
	if err != nil {
		t.Fatal(err)
	}
	if admin.Scope.NodeCount != before.Scope.NodeCount+1 ||
		admin.Scope.EdgeCount != before.Scope.EdgeCount+1 {
		t.Fatalf("intentional wider subgraph = %#v, narrow = %#v", admin.Scope, before.Scope)
	}
	if reflect.DeepEqual(admin.Nodes, before.Nodes) {
		t.Fatal("different authorized subgraphs produced identical node analytics")
	}

	visible := fixture.ingest(t, fixture.clientA, "memory://a4", "delta")
	fixture.connect(t, fixture.clientA, "edge-a3-a4", a3, visible)
	pinned := request
	pinned.SnapshotID = before.Scope.SnapshotID
	if _, err := service.Run(alice, pinned); !shoal.IsErrorCode(
		err, shoal.ErrorConflict,
	) {
		t.Fatalf("stale authorized snapshot error = %v", err)
	}
}

func TestAuthorizedAnalyticsSeparatesOntologyLensesAndRevocation(t *testing.T) {
	fixture := newAnalyticsFixture(t)
	a1 := fixture.ingest(t, fixture.clientA, "memory://a1", "alpha")
	a2 := fixture.ingest(t, fixture.clientA, "memory://a2", "bravo")
	fixture.connect(t, fixture.clientA, "edge-a1-a2", a1, a2)
	schema, err := ontology.NewOntologySchema("analytics", "Analytics", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	firstVersion, err := ontology.NewOntologyVersion(
		schema, "1", fixture.now, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	secondVersion, err := ontology.NewOntologyVersion(
		schema, "2", fixture.now.Add(time.Second), nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	firstLens, _ := ontology.NewOntologyIdentity(firstVersion)
	secondLens, _ := ontology.NewOntologyIdentity(secondVersion)
	service := fixture.service(t)
	request := analyticsRequest(a1)
	first, err := service.Run(
		fixture.readContext(t, "alice", fixture.sourceA, fixture.policyA, firstLens),
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Run(
		fixture.readContext(t, "alice", fixture.sourceA, fixture.policyA, secondLens),
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.Scope.Ontology == nil || second.Scope.Ontology == nil ||
		first.Scope.Ontology.VersionID == second.Scope.Ontology.VersionID ||
		first.Scope.AuthorizationFingerprint == second.Scope.AuthorizationFingerprint ||
		first.Scope.SnapshotID == second.Scope.SnapshotID {
		t.Fatalf("ontology lenses were not isolated:\nfirst=%#v\nsecond=%#v", first.Scope, second.Scope)
	}
	if !reflect.DeepEqual(first.Nodes, second.Nodes) {
		t.Fatal("ontology identity changed topology-only analytics")
	}

	fixture.generations.Set(fixture.domain, 2)
	_, err = service.Run(
		fixture.readContextAtGeneration(
			t, "stale", fixture.sourceA, fixture.policyA,
			ontology.OntologyIdentity{}, 1,
		),
		request,
	)
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("revoked generation error = %v", err)
	}
}

func TestAuthorizedAnalyticsRejectsIncompleteMaterialization(t *testing.T) {
	fixture := newAnalyticsFixture(t)
	a1 := fixture.ingest(t, fixture.clientA, "memory://a1", "alpha")
	a2 := fixture.ingest(t, fixture.clientA, "memory://a2", "bravo")
	a3 := fixture.ingest(t, fixture.clientA, "memory://a3", "charlie")
	fixture.connect(t, fixture.clientA, "edge-a1-a2", a1, a2)
	fixture.connect(t, fixture.clientA, "edge-a1-a3", a1, a3)
	for name, mutate := range map[string]func(*analytics.Request){
		"fanout": func(request *analytics.Request) {
			request.Scope.Fanout = 1
		},
		"nodes": func(request *analytics.Request) {
			request.Scope.MaxNodes = 2
		},
		"edges": func(request *analytics.Request) {
			request.Scope.MaxEdges = 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := analyticsRequest(a1)
			mutate(&request)
			_, err := fixture.service(t).Run(
				fixture.readContext(
					t, "alice", fixture.sourceA, fixture.policyA,
					ontology.OntologyIdentity{},
				),
				request,
			)
			if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
				t.Fatalf("incomplete materialization error = %v", err)
			}
		})
	}
}

func TestAuthorizedAnalyticsRequiresAnalyticsOperation(t *testing.T) {
	fixture := newAnalyticsFixture(t)
	seed := fixture.ingest(t, fixture.clientA, "memory://a1", "alpha")
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "reader", Actor: "reader-actor",
		AuthorizationDomain:   fixture.domain,
		AllowedOperations:     []auth.Operation{auth.OperationNeighborhood},
		PermittedSourceIDs:    [][]byte{fixture.sourceA},
		PermittedPolicyIDs:    [][]byte{fixture.policyA},
		PolicyGeneration:      1,
		AuthenticationExpires: fixture.now.Add(time.Hour),
		RequestID:             "reader-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := fixture.authority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.service(t).Run(
		ctx, analyticsRequest(seed),
	); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("missing analytics operation error = %v", err)
	}
}

func TestAuthorizedAnalyticsRecordingSeamIsExplicit(t *testing.T) {
	fixture := newAnalyticsFixture(t)
	seed := fixture.ingest(t, fixture.clientA, "memory://a1", "alpha")
	if _, err := analytics.NewService(analytics.Config{
		Source: fixture.clientA, Limits: analytics.DefaultLimits(),
		RequireRecording: true,
	}); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("missing required recorder error = %v", err)
	}
	recorder := &analyticsRecorder{}
	service, err := analytics.NewService(analytics.Config{
		Source: fixture.clientA, Limits: analytics.DefaultLimits(),
		Recorder: recorder, RequireRecording: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Run(
		fixture.readContext(
			t, "alice", fixture.sourceA, fixture.policyA, ontology.OntologyIdentity{}),
		analyticsRequest(seed),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Recording.Recorded || !result.Recording.Required ||
		recorder.calls != 1 || !recorder.recorded.Recording.Recorded {
		t.Fatalf("recording status = %#v, recorder = %#v", result.Recording, recorder)
	}
}

func TestAuthorizedAnalyticsRevalidatesGenerationAfterRecording(t *testing.T) {
	fixture := newAnalyticsFixture(t)
	seed := fixture.ingest(t, fixture.clientA, "memory://a1", "alpha")
	recorder := recorderFunc(func(
		context.Context,
		analytics.Request,
		analytics.Result,
	) error {
		fixture.generations.Set(fixture.domain, 2)
		return nil
	})
	service, err := analytics.NewService(analytics.Config{
		Source: fixture.clientA, Limits: analytics.DefaultLimits(),
		Recorder: recorder, RequireRecording: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Run(
		fixture.readContext(
			t, "alice", fixture.sourceA, fixture.policyA, ontology.OntologyIdentity{}),
		analyticsRequest(seed),
	)
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("generation change after recording error = %v", err)
	}
}

type analyticsFixture struct {
	now         time.Time
	base        *explorer.Explorer
	store       *authorized.MemoryPolicyStore
	authority   *auth.Authority
	generations *analyticsGenerationReader
	clientA     *authorized.Client
	clientB     *authorized.Client
	domain      []byte
	sourceA     []byte
	policyA     []byte
	sourceB     []byte
	policyB     []byte
}

type analyticsRecorder struct {
	calls    int
	recorded analytics.Result
}

type recorderFunc func(context.Context, analytics.Request, analytics.Result) error

func (f recorderFunc) RecordAnalytics(
	ctx context.Context,
	request analytics.Request,
	result analytics.Result,
) error {
	return f(ctx, request, result)
}

func (r *analyticsRecorder) RecordAnalytics(
	ctx context.Context,
	_ analytics.Request,
	result analytics.Result,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.calls++
	r.recorded = result
	return nil
}

func newAnalyticsFixture(t *testing.T) *analyticsFixture {
	t.Helper()
	now := time.Date(2026, time.September, 5, 18, 0, 0, 0, time.UTC)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	base, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = base.Close() })
	fixture := &analyticsFixture{
		now: now, base: base, store: authorized.NewMemoryPolicyStore(),
		authority: authority,
		generations: &analyticsGenerationReader{
			values: make(map[string]int64),
		},
		domain: []byte("domain"), sourceA: []byte("source-a"),
		policyA: []byte("policy-a"), sourceB: []byte("source-b"),
		policyB: []byte("policy-b"),
	}
	fixture.generations.Set(fixture.domain, 1)
	fixture.clientA = fixture.client(t, fixture.sourceA, fixture.policyA)
	fixture.clientB = fixture.client(t, fixture.sourceB, fixture.policyB)
	return fixture
}

func (f *analyticsFixture) client(
	t *testing.T,
	source []byte,
	policy []byte,
) *authorized.Client {
	t.Helper()
	selector, err := authorized.NewStaticPolicySelector(source, policy)
	if err != nil {
		t.Fatal(err)
	}
	client, err := authorized.NewClient(authorized.Config{
		Base: f.base, Resolver: f.authority.Resolver(),
		PolicySelector: selector, PolicyStore: f.store,
		GenerationReader: f.generations, Clock: func() time.Time { return f.now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func (f *analyticsFixture) ingest(
	t *testing.T,
	client *authorized.Client,
	uri string,
	content string,
) shoal.ID {
	t.Helper()
	result, err := client.Ingest(f.adminContext(t), explorer.Source{
		URI: uri, MediaType: explorer.MediaTypeText, Content: content,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result.Document.ID
}

func (f *analyticsFixture) connect(
	t *testing.T,
	client *authorized.Client,
	edgeID string,
	from shoal.ID,
	to shoal.ID,
) {
	t.Helper()
	if err := client.Connect(f.adminContext(t), graph.Edge{
		ID: shoal.ID(edgeID), From: from, To: to, Type: "related", Weight: 1,
	}); err != nil {
		t.Fatal(err)
	}
}

func (f *analyticsFixture) service(t *testing.T) *analytics.Service {
	t.Helper()
	service, err := analytics.NewService(analytics.Config{
		Source: f.clientA, Limits: analytics.DefaultLimits(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func (f *analyticsFixture) adminContext(t *testing.T) context.Context {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "admin", Actor: "admin-actor",
		AuthorizationDomain: f.domain,
		AllowedOperations: []auth.Operation{
			auth.OperationIngest, auth.OperationConnect, auth.OperationAnalyticsRead,
		},
		PermittedSourceIDs: [][]byte{f.sourceA, f.sourceB},
		PermittedPolicyIDs: [][]byte{f.policyA, f.policyB},
		PolicyGeneration:   1, AuthenticationExpires: f.now.Add(time.Hour),
		RequestID: "admin-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := f.authority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func (f *analyticsFixture) readContext(
	t *testing.T,
	subject string,
	source []byte,
	policy []byte,
	lens ontology.OntologyIdentity,
) context.Context {
	return f.readContextAtGeneration(t, subject, source, policy, lens, 1)
}

func (f *analyticsFixture) readContextAtGeneration(
	t *testing.T,
	subject string,
	source []byte,
	policy []byte,
	lens ontology.OntologyIdentity,
	generation int64,
) context.Context {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: shoal.ID(subject), Actor: shoal.ID(subject + "-actor"),
		AuthorizationDomain:   f.domain,
		AllowedOperations:     []auth.Operation{auth.OperationAnalyticsRead},
		PermittedSourceIDs:    [][]byte{source},
		PermittedPolicyIDs:    [][]byte{policy},
		PolicyGeneration:      generation,
		AuthenticationExpires: f.now.Add(time.Hour),
		RequestID:             shoal.ID(subject + "-request"),
		SelectedOntology:      lens,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := f.authority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func analyticsRequest(seed shoal.ID) analytics.Request {
	return analytics.Request{
		Scope: analytics.Scope{
			NodeIDs: []shoal.ID{seed}, Depth: 3,
			Direction: explorer.GraphDirectionOutgoing,
			Fanout:    8, MaxNodes: 16, MaxEdges: 32,
			MaxScannedEdgesPerNode: 256,
			EdgeTypes:              []string{"related"},
		},
	}
}

type analyticsGenerationReader struct {
	mu     sync.RWMutex
	values map[string]int64
}

func (r *analyticsGenerationReader) CurrentPolicyGeneration(
	ctx context.Context,
	domain []byte,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.values[string(domain)], nil
}

func (r *analyticsGenerationReader) Set(domain []byte, generation int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values[string(bytes.Clone(domain))] = generation
}
