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

package authorized

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// recordingNodeStore captures the exact identifier slice a batch lookup was
// asked for and returns a caller-supplied result, so deduplication and the
// handling of partial or over-broad results can be asserted directly.
type recordingNodeStore struct {
	PolicyStore
	requests     [][]shoal.ID
	result       map[shoal.ID]NodeRegistration
	err          error
	edgeRequests [][]shoal.ID
	edgeResult   map[shoal.ID]EdgeRegistration
	edgeErr      error
}

func (s *recordingNodeStore) Nodes(
	_ context.Context,
	nodeIDs []shoal.ID,
) (map[shoal.ID]NodeRegistration, error) {
	s.requests = append(s.requests, append([]shoal.ID(nil), nodeIDs...))
	if s.err != nil {
		return nil, s.err
	}
	return s.result, nil
}

func (s *recordingNodeStore) Edges(
	_ context.Context,
	edgeIDs []shoal.ID,
) (map[shoal.ID]EdgeRegistration, error) {
	s.edgeRequests = append(s.edgeRequests, append([]shoal.ID(nil), edgeIDs...))
	if s.edgeErr != nil {
		return nil, s.edgeErr
	}
	return s.edgeResult, nil
}

func batchTestRule(t *testing.T, source, policy string) AccessRule {
	t.Helper()
	authPolicy, err := auth.NewPolicy(auth.PolicyConfig{
		AuthorizationDomain: []byte("domain"),
		SourceID:            []byte(source),
		GrantPolicyID:       []byte(policy),
		Epoch:               1,
	})
	if err != nil {
		t.Fatal(err)
	}
	rule, err := NewAccessRule(authPolicy)
	if err != nil {
		t.Fatal(err)
	}
	return rule
}

func batchTestDecision(t *testing.T, now time.Time) auth.Decision {
	t.Helper()
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject:               "subject",
		Actor:                 "actor",
		AuthorizationDomain:   []byte("domain"),
		AllowedOperations:     []auth.Operation{auth.OperationNeighborhood},
		PermittedSourceIDs:    [][]byte{[]byte("source")},
		PermittedPolicyIDs:    [][]byte{[]byte("policy")},
		PolicyGeneration:      1,
		AuthenticationExpires: now.Add(time.Hour),
		RequestID:             "request",
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

// TestEdgeAllowsResolvedDeniesEndpointMissingFromPage is the core fail-closed
// assertion for batching: an endpoint that the batch result did not report is
// denied exactly as a point lookup reporting !ok denies it, whichever endpoint
// is missing and even when the edge rule itself allows.
func TestEdgeAllowsResolvedDeniesEndpointMissingFromPage(t *testing.T) {
	now := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)
	rule := batchTestRule(t, "source", "policy")
	decision := batchTestDecision(t, now)
	registration := EdgeRegistration{
		Edge: graph.Edge{
			ID: "edge", From: "node-from", To: "node-to", Type: "link", Weight: 1,
		},
		Rule: rule,
	}
	from := NodeRegistration{Rule: rule}
	to := NodeRegistration{Rule: rule}
	for name, resolved := range map[string]registeredNodes{
		"empty page":     {},
		"nil page":       nil,
		"missing source": {"node-to": to},
		"missing target": {"node-from": from},
	} {
		allowed, err := edgeAllowsResolved(
			resolved, registration, decision, auth.OperationNeighborhood, now)
		if err != nil {
			t.Fatalf("%s error = %v", name, err)
		}
		if allowed {
			t.Fatalf("%s authorized an edge whose endpoint was never resolved", name)
		}
	}
	allowed, err := edgeAllowsResolved(
		registeredNodes{"node-from": from, "node-to": to},
		registration, decision, auth.OperationNeighborhood, now)
	if err != nil || !allowed {
		t.Fatalf("fully resolved edge allowed = %v err = %v", allowed, err)
	}
}

// TestEdgeAllowsResolvedDeniesUnauthorizedEndpointRule confirms batching did not
// drop the endpoint rule evaluation itself.
func TestEdgeAllowsResolvedDeniesUnauthorizedEndpointRule(t *testing.T) {
	now := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)
	allowedRule := batchTestRule(t, "source", "policy")
	deniedRule := batchTestRule(t, "other-source", "other-policy")
	decision := batchTestDecision(t, now)
	registration := EdgeRegistration{
		Edge: graph.Edge{
			ID: "edge", From: "node-from", To: "node-to", Type: "link", Weight: 1,
		},
		Rule: allowedRule,
	}
	for name, resolved := range map[string]registeredNodes{
		"denied source": {
			"node-from": {Rule: deniedRule},
			"node-to":   {Rule: allowedRule},
		},
		"denied target": {
			"node-from": {Rule: allowedRule},
			"node-to":   {Rule: deniedRule},
		},
	} {
		allowed, err := edgeAllowsResolved(
			resolved, registration, decision, auth.OperationNeighborhood, now)
		if err != nil {
			t.Fatalf("%s error = %v", name, err)
		}
		if allowed {
			t.Fatalf("%s authorized an edge across an unauthorized endpoint", name)
		}
	}
}

// TestResolveNodesDeduplicatesAndConfinesResults asserts the batch carries one
// entry per distinct identifier and that registrations for identifiers that
// were never requested are discarded rather than admitted.
func TestResolveNodesDeduplicatesAndConfinesResults(t *testing.T) {
	rule := batchTestRule(t, "source", "policy")
	store := &recordingNodeStore{result: map[shoal.ID]NodeRegistration{
		"node-a":          {Rule: rule},
		"node-b":          {Rule: rule},
		"never-asked-for": {Rule: rule},
	}}
	client := &Client{policyStore: store}
	resolved, err := client.resolveNodes(context.Background(), []shoal.ID{
		"node-a", "node-b", "node-a", "node-c", "node-b", "node-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(store.requests) != 1 {
		t.Fatalf("batch lookups = %d, want exactly one", len(store.requests))
	}
	wantRequest := []shoal.ID{"node-a", "node-b", "node-c"}
	if !reflect.DeepEqual(store.requests[0], wantRequest) {
		t.Fatalf("batched identifiers = %#v, want %#v", store.requests[0], wantRequest)
	}
	if _, ok := resolved["never-asked-for"]; ok {
		t.Fatal("batch result admitted a registration that was never requested")
	}
	if _, ok := resolved["node-c"]; ok {
		t.Fatal("identifier absent from the batch result was reported as resolved")
	}
	if len(resolved) != 2 {
		t.Fatalf("resolved = %#v, want node-a and node-b only", resolved)
	}
}

// TestResolveNodesSkipsEmptyBatch keeps a page with nothing to resolve from
// spending a round trip.
func TestResolveNodesSkipsEmptyBatch(t *testing.T) {
	store := &recordingNodeStore{}
	client := &Client{policyStore: store}
	resolved, err := client.resolveNodes(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(store.requests) != 0 {
		t.Fatalf("batch lookups = %d, want none", len(store.requests))
	}
	if len(resolved) != 0 {
		t.Fatalf("resolved = %#v, want empty", resolved)
	}
}

// TestResolveNodesPropagatesStoreFailureAsRead confirms a failed batch surfaces
// as a catalog read failure instead of silently resolving to an empty page,
// which would otherwise read as a blanket deny with no error.
func TestResolveNodesPropagatesStoreFailureAsRead(t *testing.T) {
	store := &recordingNodeStore{err: catalogUnavailable()}
	client := &Client{policyStore: store}
	if _, err := client.resolveNodes(
		context.Background(), []shoal.ID{"node-a"},
	); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("batch failure error = %v", err)
	}
}

// TestMemoryPolicyStoreNodesMatchesNodeLoop pins the batch implementation to be
// exactly equivalent to calling Node for each identifier.
func TestMemoryPolicyStoreNodesMatchesNodeLoop(t *testing.T) {
	ctx := context.Background()
	rule := batchTestRule(t, "source", "policy")
	store := NewMemoryPolicyStore()
	if err := store.PutRevision(ctx, RevisionRegistration{
		DocumentID: "document",
		RevisionID: "revision",
		NodeIDs:    []shoal.ID{"document", "node-a"},
		ContentDigest: auth.DigestBytes(
			"test-content", []byte("revision")),
		Rule:    rule,
		Current: true,
	}); err != nil {
		t.Fatal(err)
	}
	requested := []shoal.ID{"document", "node-a", "node-a", "unregistered"}
	batch, err := store.Nodes(ctx, requested)
	if err != nil {
		t.Fatal(err)
	}
	expected := make(map[shoal.ID]NodeRegistration)
	for _, nodeID := range requested {
		registration, ok, err := store.Node(ctx, nodeID)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			if _, present := batch[nodeID]; present {
				t.Fatalf("batch reported unregistered node %q", nodeID)
			}
			continue
		}
		expected[nodeID] = registration
	}
	if !reflect.DeepEqual(batch, expected) {
		t.Fatalf("batch = %#v, want %#v", batch, expected)
	}
	if len(batch) != 2 {
		t.Fatalf("batch = %#v, want one entry per distinct registered node", batch)
	}
}

// TestMemoryPolicyStoreNodesRejectsInvalidInput mirrors Node's argument and
// context validation so batching cannot become a laxer entry point.
func TestMemoryPolicyStoreNodesRejectsInvalidInput(t *testing.T) {
	store := NewMemoryPolicyStore()
	if _, err := store.Nodes(
		context.Background(), []shoal.ID{"node-a", ""},
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("empty identifier error = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Nodes(cancelled, []shoal.ID{"node-a"}); err == nil {
		t.Fatal("cancelled context batch lookup succeeded")
	}
	var nilStore *MemoryPolicyStore
	if _, err := nilStore.Nodes(
		context.Background(), []shoal.ID{"node-a"},
	); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("nil store batch lookup error = %v", err)
	}
}

// TestMemoryPolicyStoreBatchFailureOrderMatchesPointLoop pins the failure
// ordering the batch doc comments promise. A Node loop over {"node-a", ""}
// against a nil store fails on the first identifier with catalog-unavailable,
// so the batch must report the same rather than validating the whole slice
// first and surfacing the later invalid identifier instead.
func TestMemoryPolicyStoreBatchFailureOrderMatchesPointLoop(t *testing.T) {
	ctx := context.Background()
	var nilStore *MemoryPolicyStore
	if _, err := nilStore.Nodes(
		ctx, []shoal.ID{"node-a", ""},
	); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("nil store node batch error = %v, want catalog unavailable", err)
	}
	if _, err := nilStore.Edges(
		ctx, []shoal.ID{"edge-a", ""},
	); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("nil store edge batch error = %v, want catalog unavailable", err)
	}
	// An empty request makes no per-identifier call, so the receiver check is
	// all that remains and must still reject.
	if _, err := nilStore.Nodes(ctx, nil); !shoal.IsErrorCode(
		err, shoal.ErrorUnavailable,
	) {
		t.Fatalf("nil store empty node batch error = %v", err)
	}
	if _, err := nilStore.Edges(ctx, nil); !shoal.IsErrorCode(
		err, shoal.ErrorUnavailable,
	) {
		t.Fatalf("nil store empty edge batch error = %v", err)
	}
	// A live store still reports an invalid identifier, so moving the receiver
	// check does not weaken argument validation.
	store := NewMemoryPolicyStore()
	if _, err := store.Nodes(
		ctx, []shoal.ID{"node-a", ""},
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("invalid node identifier error = %v", err)
	}
	if _, err := store.Edges(
		ctx, []shoal.ID{"edge-a", ""},
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("invalid edge identifier error = %v", err)
	}
}

// TestResolveEdgesDeduplicatesAndConfinesResults is the edge counterpart of the
// node batch guard: repeated identifiers cost one round trip, and a store that
// answers with identifiers the page never asked for cannot widen the page.
func TestResolveEdgesDeduplicatesAndConfinesResults(t *testing.T) {
	rule := batchTestRule(t, "source", "policy")
	store := &recordingNodeStore{edgeResult: map[shoal.ID]EdgeRegistration{
		"edge-a":          {Rule: rule},
		"edge-b":          {Rule: rule},
		"never-asked-for": {Rule: rule},
	}}
	client := &Client{policyStore: store}
	resolved, err := client.resolveEdges(context.Background(), []shoal.ID{
		"edge-a", "edge-b", "edge-a", "edge-c", "edge-b", "edge-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(store.edgeRequests) != 1 {
		t.Fatalf("batch lookups = %d, want exactly one", len(store.edgeRequests))
	}
	wantRequest := []shoal.ID{"edge-a", "edge-b", "edge-c"}
	if !reflect.DeepEqual(store.edgeRequests[0], wantRequest) {
		t.Fatalf("batched identifiers = %#v, want %#v", store.edgeRequests[0], wantRequest)
	}
	if _, ok := resolved["never-asked-for"]; ok {
		t.Fatal("batch result admitted a registration that was never requested")
	}
	if _, ok := resolved["edge-c"]; ok {
		t.Fatal("identifier absent from the batch result was reported as resolved")
	}
	if len(resolved) != 2 {
		t.Fatalf("resolved = %#v, want edge-a and edge-b only", resolved)
	}
}

// TestResolveEdgesSkipsEmptyBatch keeps a page with no candidate edges from
// spending a round trip.
func TestResolveEdgesSkipsEmptyBatch(t *testing.T) {
	store := &recordingNodeStore{}
	client := &Client{policyStore: store}
	resolved, err := client.resolveEdges(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(store.edgeRequests) != 0 {
		t.Fatalf("batch lookups = %d, want none", len(store.edgeRequests))
	}
	if len(resolved) != 0 {
		t.Fatalf("resolved = %#v, want empty", resolved)
	}
}

// TestResolveEdgesPropagatesStoreFailureAsRead confirms a failed edge batch
// surfaces as a catalog read failure rather than an empty result that would be
// indistinguishable from every edge being unregistered.
func TestResolveEdgesPropagatesStoreFailureAsRead(t *testing.T) {
	store := &recordingNodeStore{edgeErr: catalogUnavailable()}
	client := &Client{policyStore: store}
	if _, err := client.resolveEdges(
		context.Background(), []shoal.ID{"edge-a"},
	); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("batch failure error = %v", err)
	}
}

// TestMemoryPolicyStoreEdgesMatchesEdgeLoop pins the batch edge implementation
// to be equivalent to calling Edge for each identifier, including omitting
// unregistered identifiers rather than reporting a zero registration.
func TestMemoryPolicyStoreEdgesMatchesEdgeLoop(t *testing.T) {
	ctx := context.Background()
	rule := batchTestRule(t, "source", "policy")
	store := NewMemoryPolicyStore()
	if err := store.PutRevision(ctx, RevisionRegistration{
		DocumentID:    "document",
		RevisionID:    "revision",
		NodeIDs:       []shoal.ID{"document", "node-a"},
		ContentDigest: auth.DigestBytes("test-content", []byte("revision")),
		Rule:          rule,
		Current:       true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutEdge(ctx, EdgeRegistration{
		Edge: graph.Edge{
			ID: "edge-a", From: "document", To: "node-a", Type: "link",
		},
		DocumentID: "document", RevisionID: "revision", Rule: rule,
	}); err != nil {
		t.Fatal(err)
	}
	requested := []shoal.ID{"edge-a", "edge-a", "unregistered"}
	batch, err := store.Edges(ctx, requested)
	if err != nil {
		t.Fatal(err)
	}
	expected := make(map[shoal.ID]EdgeRegistration)
	for _, edgeID := range requested {
		registration, ok, err := store.Edge(ctx, edgeID)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			if _, present := batch[edgeID]; present {
				t.Fatalf("batch reported unregistered edge %q", edgeID)
			}
			continue
		}
		expected[edgeID] = registration
	}
	if !reflect.DeepEqual(batch, expected) {
		t.Fatalf("batch = %#v, want %#v", batch, expected)
	}
	if len(batch) != 1 {
		t.Fatalf("batch = %#v, want one entry per distinct registered edge", batch)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Edges(cancelled, []shoal.ID{"edge-a"}); err == nil {
		t.Fatal("cancelled context batch edge lookup succeeded")
	}
}
