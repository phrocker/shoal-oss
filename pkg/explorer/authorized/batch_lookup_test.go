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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// policyStoreTrips records how a page's node authorization reached the policy
// store. Point lookups and batch lookups are counted separately so a regression
// from one batch back to a lookup per edge endpoint is visible as a count, not
// merely as additional latency.
type policyStoreTrips struct {
	nodeCalls    int
	nodesCalls   int
	batchedIDs   int
	largestBatch int
}

func (t policyStoreTrips) total() int {
	return t.nodeCalls + t.nodesCalls
}

// countingPolicyStore is a test-only PolicyStore that counts policy-store round
// trips, optionally injects per-round-trip latency, and can withhold nodes from
// batch results so a partial batch can be exercised without mutating the
// catalog. Withholding deliberately does not affect point lookups: it models a
// batch that returned fewer registrations than were asked for.
type countingPolicyStore struct {
	authorized.PolicyStore
	mu             sync.Mutex
	trips          policyStoreTrips
	latency        time.Duration
	duplicateBatch bool
	hidden         map[shoal.ID]struct{}
}

func newCountingPolicyStore(inner authorized.PolicyStore) *countingPolicyStore {
	return &countingPolicyStore{
		PolicyStore: inner,
		hidden:      make(map[shoal.ID]struct{}),
	}
}

func (s *countingPolicyStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trips = policyStoreTrips{}
	s.duplicateBatch = false
}

func (s *countingPolicyStore) snapshot() policyStoreTrips {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trips
}

func (s *countingPolicyStore) sawDuplicateBatch() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.duplicateBatch
}

func (s *countingPolicyStore) setLatency(latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latency = latency
}

// withholdFromBatch drops the identifiers from every subsequent batch result
// while leaving point lookups intact, simulating a partial batch response.
func (s *countingPolicyStore) withholdFromBatch(nodeIDs ...shoal.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, nodeID := range nodeIDs {
		s.hidden[nodeID] = struct{}{}
	}
}

func (s *countingPolicyStore) Node(
	ctx context.Context,
	nodeID shoal.ID,
) (authorized.NodeRegistration, bool, error) {
	s.mu.Lock()
	s.trips.nodeCalls++
	latency := s.latency
	s.mu.Unlock()
	if latency > 0 {
		time.Sleep(latency)
	}
	return s.PolicyStore.Node(ctx, nodeID)
}

func (s *countingPolicyStore) Nodes(
	ctx context.Context,
	nodeIDs []shoal.ID,
) (map[shoal.ID]authorized.NodeRegistration, error) {
	seen := make(map[shoal.ID]struct{}, len(nodeIDs))
	duplicate := false
	for _, nodeID := range nodeIDs {
		if _, repeated := seen[nodeID]; repeated {
			duplicate = true
		}
		seen[nodeID] = struct{}{}
	}
	s.mu.Lock()
	s.trips.nodesCalls++
	s.trips.batchedIDs += len(nodeIDs)
	if len(nodeIDs) > s.trips.largestBatch {
		s.trips.largestBatch = len(nodeIDs)
	}
	s.duplicateBatch = s.duplicateBatch || duplicate
	latency := s.latency
	s.mu.Unlock()
	if latency > 0 {
		time.Sleep(latency)
	}
	resolved, err := s.PolicyStore.Nodes(ctx, nodeIDs)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for nodeID := range s.hidden {
		delete(resolved, nodeID)
	}
	return resolved, nil
}

// batchFixture ingests documents and returns their node identifiers along with
// a client whose policy store counts round trips.
func batchFixture(
	t *testing.T,
	documents int,
) (*fixture, *authorized.Client, *countingPolicyStore, []shoal.ID) {
	t.Helper()
	f := newFixture(t)
	admin := f.admin(t)
	nodeIDs := make([]shoal.ID, 0, documents)
	for index := 0; index < documents; index++ {
		result, err := f.clientA.Ingest(admin, explorer.Source{
			URI:       fmt.Sprintf("file:///batch-%d.txt", index),
			MediaType: explorer.MediaTypeText,
			Content:   fmt.Sprintf("batch document %d", index),
		})
		if err != nil {
			t.Fatal(err)
		}
		nodeIDs = append(nodeIDs, result.Document.ID)
	}
	counting := newCountingPolicyStore(f.store)
	client := f.newClient(t, f.base, counting, f.sourceA, f.policyA, nil)
	return f, client, counting, nodeIDs
}

func connectBatchEdge(
	t *testing.T,
	f *fixture,
	admin context.Context,
	edgeID shoal.ID,
	from, to shoal.ID,
) {
	t.Helper()
	if err := f.clientA.Connect(admin, graph.Edge{
		ID: edgeID, From: from, To: to, Type: "link", Weight: 1,
	}); err != nil {
		t.Fatal(err)
	}
}

// TestNeighborhoodBatchesEndpointLookupsPerPage pins the policy-store round
// trip count for an authorized neighborhood page. Doubling the edge count over
// the same node set must not change the number of round trips: authorization
// must cost one batch lookup for the page's distinct nodes rather than a lookup
// per edge endpoint.
func TestNeighborhoodBatchesEndpointLookupsPerPage(t *testing.T) {
	f, client, counting, documents := batchFixture(t, 4)
	admin := f.admin(t)
	for index := 1; index < len(documents); index++ {
		connectBatchEdge(
			t, f, admin, shoal.ID(fmt.Sprintf("star-%d", index)),
			documents[0], documents[index])
	}
	request := explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{documents[0]}, Depth: 2, EdgeTypes: []string{"link"},
	}

	counting.reset()
	sparse, err := client.Neighborhood(f.alice(t), request)
	if err != nil {
		t.Fatal(err)
	}
	sparseTrips := counting.snapshot()
	if len(sparse.Edges) != 3 || len(sparse.Nodes) != 4 {
		t.Fatalf("sparse neighborhood = %d nodes %d edges",
			len(sparse.Nodes), len(sparse.Edges))
	}

	connectBatchEdge(t, f, admin, "mesh-1-2", documents[1], documents[2])
	connectBatchEdge(t, f, admin, "mesh-1-3", documents[1], documents[3])
	connectBatchEdge(t, f, admin, "mesh-2-3", documents[2], documents[3])

	counting.reset()
	dense, err := client.Neighborhood(f.alice(t), request)
	if err != nil {
		t.Fatal(err)
	}
	denseTrips := counting.snapshot()
	if len(dense.Edges) != 6 || len(dense.Nodes) != 4 {
		t.Fatalf("dense neighborhood = %d nodes %d edges",
			len(dense.Nodes), len(dense.Edges))
	}

	if counting.sawDuplicateBatch() {
		t.Fatal("batch lookup repeated an identifier instead of deduplicating")
	}
	t.Logf("policy store round trips: 3 edges %+v, 6 edges %+v",
		sparseTrips, denseTrips)
	if denseTrips.nodesCalls != 1 {
		t.Fatalf("batch lookups = %d, want exactly one per page", denseTrips.nodesCalls)
	}
	if denseTrips.nodeCalls != len(request.NodeIDs) {
		t.Fatalf("point lookups = %d, want %d (seed authorization only)",
			denseTrips.nodeCalls, len(request.NodeIDs))
	}
	if sparseTrips != denseTrips {
		t.Fatalf(
			"round trips scale with edge count: 3 edges = %+v, 6 edges = %+v",
			sparseTrips, denseTrips)
	}
	if denseTrips.largestBatch != len(dense.Nodes) {
		t.Fatalf("batched identifiers = %d, want %d distinct page nodes",
			denseTrips.largestBatch, len(dense.Nodes))
	}
	// Resolving each endpoint individually costs one seed lookup, one lookup
	// per page node and two per admitted edge. Recording it keeps the
	// regression this test guards explicit.
	perEndpoint := len(request.NodeIDs) + len(dense.Nodes) + 2*len(dense.Edges)
	if denseTrips.total() >= perEndpoint {
		t.Fatalf("round trips = %d, want fewer than the %d per-endpoint lookups",
			denseTrips.total(), perEndpoint)
	}
}

// TestBoundedNeighborhoodBatchesEndpointLookupsPerPage pins the same round-trip
// bound for the bounded traversal path, which filters each scanned page.
func TestBoundedNeighborhoodBatchesEndpointLookupsPerPage(t *testing.T) {
	f, client, counting, documents := batchFixture(t, 4)
	admin := f.admin(t)
	for index := 1; index < len(documents); index++ {
		connectBatchEdge(
			t, f, admin, shoal.ID(fmt.Sprintf("bounded-%d", index)),
			documents[0], documents[index])
	}
	request := explorer.BoundedNeighborhoodRequest{
		NodeIDs: []shoal.ID{documents[0]}, Depth: 1, Fanout: 16, MaxNodes: 16,
		MaxScannedEdges: 16, EdgeTypes: []string{"link"},
		Direction: explorer.GraphDirectionOutgoing,
	}
	counting.reset()
	page, err := client.BoundedNeighborhood(f.alice(t), request)
	if err != nil {
		t.Fatal(err)
	}
	trips := counting.snapshot()
	if len(page.Neighborhood.Edges) != 3 {
		t.Fatalf("bounded page edges = %d, want 3", len(page.Neighborhood.Edges))
	}
	if counting.sawDuplicateBatch() {
		t.Fatal("bounded batch lookup repeated an identifier")
	}
	perEndpoint := len(request.NodeIDs) +
		len(page.Neighborhood.Nodes) + 2*len(page.Neighborhood.Edges)
	if trips.total() >= perEndpoint {
		t.Fatalf("bounded round trips = %+v, want fewer than %d per-endpoint lookups",
			trips, perEndpoint)
	}
	if trips.nodeCalls != len(request.NodeIDs) {
		t.Fatalf("bounded point lookups = %d, want %d (seed authorization only)",
			trips.nodeCalls, len(request.NodeIDs))
	}
}

// TestNeighborhoodRoundTripLatencyTracksBatchCount confirms the fake's injected
// latency is charged once per round trip, so the recorded counts are the real
// cost a distributed policy store would pay.
func TestNeighborhoodRoundTripLatencyTracksBatchCount(t *testing.T) {
	f, client, counting, documents := batchFixture(t, 4)
	admin := f.admin(t)
	for index := 1; index < len(documents); index++ {
		connectBatchEdge(
			t, f, admin, shoal.ID(fmt.Sprintf("latency-%d", index)),
			documents[0], documents[index])
	}
	const latency = 20 * time.Millisecond
	counting.reset()
	counting.setLatency(latency)
	started := time.Now()
	neighborhood, err := client.Neighborhood(f.alice(t), explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{documents[0]}, Depth: 2, EdgeTypes: []string{"link"},
	})
	elapsed := time.Since(started)
	counting.setLatency(0)
	if err != nil {
		t.Fatal(err)
	}
	trips := counting.snapshot()
	if len(neighborhood.Edges) != 3 {
		t.Fatalf("neighborhood edges = %d, want 3", len(neighborhood.Edges))
	}
	if elapsed < time.Duration(trips.total())*latency {
		t.Fatalf("elapsed %s is below the %d injected round trips",
			elapsed, trips.total())
	}
	perEndpoint := 1 + len(neighborhood.Nodes) + 2*len(neighborhood.Edges)
	if budget := time.Duration(perEndpoint) * latency; elapsed >= budget {
		t.Fatalf("elapsed %s reached the %s a lookup per endpoint would cost",
			elapsed, budget)
	}
	t.Logf("policy store round trips %+v cost %s at %s per round trip",
		trips, elapsed, latency)
}

// TestNeighborhoodDeniesNodeMissingFromBatchResult verifies that batching did
// not turn a missing node into an allow. A node the batch result omits must be
// excluded exactly as a point lookup reporting !ok excludes it, and every edge
// incident to it must be denied.
func TestNeighborhoodDeniesNodeMissingFromBatchResult(t *testing.T) {
	f, client, counting, documents := batchFixture(t, 4)
	admin := f.admin(t)
	for index := 1; index < len(documents); index++ {
		connectBatchEdge(
			t, f, admin, shoal.ID(fmt.Sprintf("missing-%d", index)),
			documents[0], documents[index])
	}
	request := explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{documents[0]}, Depth: 2, EdgeTypes: []string{"link"},
	}
	before, err := client.Neighborhood(f.alice(t), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Edges) != 3 || !hasNode(before, documents[1]) {
		t.Fatalf("baseline neighborhood = %#v", before)
	}

	counting.withholdFromBatch(documents[1])
	after, err := client.Neighborhood(f.alice(t), request)
	if err != nil {
		t.Fatal(err)
	}
	if hasNode(after, documents[1]) {
		t.Fatal("node absent from the batch result was authorized")
	}
	for _, edge := range after.Edges {
		if edge.From == documents[1] || edge.To == documents[1] {
			t.Fatalf("edge %q kept an endpoint absent from the batch result", edge.ID)
		}
	}
	if len(after.Edges) != 2 ||
		!hasNode(after, documents[2]) || !hasNode(after, documents[3]) {
		t.Fatalf("partial batch result denied more than the missing node: %#v", after)
	}
}

// TestBoundedNeighborhoodDeniesNodeMissingFromBatchResult repeats the
// fail-closed assertion on the bounded traversal path.
func TestBoundedNeighborhoodDeniesNodeMissingFromBatchResult(t *testing.T) {
	f, client, counting, documents := batchFixture(t, 4)
	admin := f.admin(t)
	for index := 1; index < len(documents); index++ {
		connectBatchEdge(
			t, f, admin, shoal.ID(fmt.Sprintf("bounded-missing-%d", index)),
			documents[0], documents[index])
	}
	request := explorer.BoundedNeighborhoodRequest{
		NodeIDs: []shoal.ID{documents[0]}, Depth: 1, Fanout: 16, MaxNodes: 16,
		MaxScannedEdges: 16, EdgeTypes: []string{"link"},
		Direction: explorer.GraphDirectionOutgoing,
	}
	counting.withholdFromBatch(documents[2])
	page, err := client.BoundedNeighborhood(f.alice(t), request)
	if err != nil {
		t.Fatal(err)
	}
	if hasNode(page.Neighborhood, documents[2]) {
		t.Fatal("bounded page authorized a node absent from the batch result")
	}
	for _, edge := range page.Neighborhood.Edges {
		if edge.From == documents[2] || edge.To == documents[2] {
			t.Fatalf("bounded page kept edge %q with a missing endpoint", edge.ID)
		}
	}
	if len(page.Neighborhood.Edges) != 2 {
		t.Fatalf("bounded page edges = %d, want 2", len(page.Neighborhood.Edges))
	}
}

// TestNeighborhoodDeniesEveryEdgeWhenBatchResultIsEmpty verifies a wholesale
// empty batch result authorizes nothing rather than degrading to an allow.
func TestNeighborhoodDeniesEveryEdgeWhenBatchResultIsEmpty(t *testing.T) {
	f, client, counting, documents := batchFixture(t, 3)
	admin := f.admin(t)
	for index := 1; index < len(documents); index++ {
		connectBatchEdge(
			t, f, admin, shoal.ID(fmt.Sprintf("empty-%d", index)),
			documents[0], documents[index])
	}
	counting.withholdFromBatch(documents...)
	_, err := client.Neighborhood(f.alice(t), explorer.NeighborhoodRequest{
		NodeIDs: []shoal.ID{documents[0]}, Depth: 2, EdgeTypes: []string{"link"},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInternal) {
		t.Fatalf("empty batch result error = %v, want an inconsistent-base denial", err)
	}
}
