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
	"reflect"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestGraphAuthorizationReachabilityAndConjunction(t *testing.T) {
	f := newFixture(t)
	admin := f.admin(t)
	first, err := f.clientA.Ingest(admin, explorer.Source{
		URI: "file:///graph-a.txt", MediaType: explorer.MediaTypeText,
		Content: "graph a",
	})
	if err != nil {
		t.Fatal(err)
	}
	hidden, err := f.clientB.Ingest(admin, explorer.Source{
		URI: "file:///graph-hidden.txt", MediaType: explorer.MediaTypeText,
		Content: "hidden intermediary",
	})
	if err != nil {
		t.Fatal(err)
	}
	last, err := f.clientA.Ingest(admin, explorer.Source{
		URI: "file:///graph-c.txt", MediaType: explorer.MediaTypeText,
		Content: "graph c",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, edge := range []graph.Edge{
		{
			ID: "a-to-hidden", From: first.Document.ID, To: hidden.Document.ID,
			Type: "link", Weight: 1,
		},
		{
			ID: "hidden-to-c", From: hidden.Document.ID, To: last.Document.ID,
			Type: "link", Weight: 1,
		},
	} {
		if err := f.clientA.Connect(admin, edge); err != nil {
			t.Fatal(err)
		}
	}

	nodeIDs := []shoal.ID{first.Document.ID}
	edgeTypes := []string{"link"}
	nodeCopy := append([]shoal.ID(nil), nodeIDs...)
	typeCopy := append([]string(nil), edgeTypes...)
	neighborhood, err := f.clientA.Neighborhood(
		f.alice(t),
		explorer.NeighborhoodRequest{
			NodeIDs: nodeIDs, Depth: 2, EdgeTypes: edgeTypes,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nodeIDs, nodeCopy) ||
		!reflect.DeepEqual(edgeTypes, typeCopy) {
		t.Fatal("Neighborhood mutated caller slices")
	}
	if hasNode(neighborhood, hidden.Document.ID) ||
		hasNode(neighborhood, last.Document.ID) ||
		len(neighborhood.Edges) != 0 {
		t.Fatalf("hidden intermediary bridged graph: %#v", neighborhood)
	}

	deniedEdge := graph.Edge{
		ID: "denied-endpoint", From: first.Document.ID, To: hidden.Document.ID,
		Type: "denied", Weight: 1, Properties: shoal.Metadata{"secret": "caller"},
	}
	propertiesCopy := cloneMetadata(deniedEdge.Properties)
	err = f.clientA.Connect(f.alice(t), deniedEdge)
	if !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("hidden endpoint error = %v", err)
	}
	if !reflect.DeepEqual(deniedEdge.Properties, propertiesCopy) {
		t.Fatal("Connect mutated caller properties")
	}
	raw, err := f.base.Neighborhood(context.Background(), explorer.NeighborhoodRequest{
		NodeIDs:   []shoal.ID{first.Document.ID},
		Depth:     1,
		EdgeTypes: []string{"denied"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(raw.Edges) != 0 {
		t.Fatalf("denied endpoint reached base: %#v", raw.Edges)
	}

	edgePolicyB, err := authorized.NewStaticPolicySelector(f.sourceB, f.policyB)
	if err != nil {
		t.Fatal(err)
	}
	guardedClient := f.newClient(
		t, f.base, f.store, f.sourceA, f.policyA, edgePolicyB)
	guarded := graph.Edge{
		ID:   "edge-conjunction",
		From: first.Document.ID, To: last.Document.ID,
		Type: "guarded", Weight: 1,
	}
	if err := guardedClient.Connect(admin, guarded); err != nil {
		t.Fatal(err)
	}
	aliceGraph, err := guardedClient.Neighborhood(
		f.alice(t),
		explorer.NeighborhoodRequest{
			NodeIDs: []shoal.ID{first.Document.ID}, Depth: 1,
			EdgeTypes: []string{"guarded"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(aliceGraph.Edges) != 0 || hasNode(aliceGraph, last.Document.ID) {
		t.Fatalf("edge policy conjunction was bypassed: %#v", aliceGraph)
	}
	adminGraph, err := guardedClient.Neighborhood(
		admin,
		explorer.NeighborhoodRequest{
			NodeIDs: []shoal.ID{first.Document.ID}, Depth: 1,
			EdgeTypes: []string{"guarded"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(adminGraph.Edges) != 1 || !hasNode(adminGraph, last.Document.ID) {
		t.Fatalf("authorized edge missing: %#v", adminGraph)
	}
}

func TestConnectCatalogFailureRetryReconciles(t *testing.T) {
	f := newFixture(t)
	first, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///edge-retry-a.txt", MediaType: explorer.MediaTypeText,
		Content: "edge retry a",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///edge-retry-b.txt", MediaType: explorer.MediaTypeText,
		Content: "edge retry b",
	})
	if err != nil {
		t.Fatal(err)
	}
	failing := &failStore{PolicyStore: f.store, edgeFailures: 1}
	client := f.newClient(t, f.base, failing, f.sourceA, f.policyA, nil)
	edge := graph.Edge{
		ID: "edge-retry", From: first.Document.ID, To: second.Document.ID,
		Type: "retry", Weight: 1,
	}
	if err := client.Connect(f.admin(t), edge); !shoal.IsErrorCode(
		err, shoal.ErrorUnavailable,
	) {
		t.Fatalf("first edge catalog failure = %v", err)
	}
	if err := client.Connect(f.admin(t), edge); err != nil {
		t.Fatal(err)
	}
	neighborhood, err := client.Neighborhood(
		f.alice(t),
		explorer.NeighborhoodRequest{
			NodeIDs: []shoal.ID{first.Document.ID},
			Depth:   1, EdgeTypes: []string{"retry"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(neighborhood.Edges) != 1 {
		t.Fatalf("reconciled edge missing: %#v", neighborhood)
	}
}

func TestCrossDomainConnectIsForbidden(t *testing.T) {
	f := newFixture(t)
	first, err := f.clientA.Ingest(f.admin(t), explorer.Source{
		URI: "file:///domain-one.txt", MediaType: explorer.MediaTypeText,
		Content: "domain one",
	})
	if err != nil {
		t.Fatal(err)
	}
	domainTwo := []byte("domain-two")
	sourceTwo := []byte("source-two")
	policyTwo := []byte("policy-two")
	f.reader.Set(domainTwo, 1)
	selector, err := authorized.NewStaticPolicySelector(sourceTwo, policyTwo)
	if err != nil {
		t.Fatal(err)
	}
	clientTwo, err := authorized.NewClient(authorized.Config{
		Base:             f.base,
		Resolver:         f.authority.Resolver(),
		PolicySelector:   selector,
		PolicyStore:      f.store,
		GenerationReader: f.reader,
		Clock:            f.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	decisionTwo, err := auth.NewDecision(auth.DecisionConfig{
		Subject:               "domain-two-user",
		Actor:                 "domain-two-actor",
		AuthorizationDomain:   domainTwo,
		AllowedOperations:     allOperations,
		PermittedSourceIDs:    [][]byte{sourceTwo},
		PermittedPolicyIDs:    [][]byte{policyTwo},
		PolicyGeneration:      1,
		AuthenticationExpires: f.clock.Now().Add(time.Hour),
		RequestID:             "domain-two-request",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctxTwo, err := f.authority.Binder().Bind(context.Background(), decisionTwo)
	if err != nil {
		t.Fatal(err)
	}
	second, err := clientTwo.Ingest(ctxTwo, explorer.Source{
		URI: "file:///domain-two.txt", MediaType: explorer.MediaTypeText,
		Content: "domain two",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = f.clientA.Connect(f.admin(t), graph.Edge{
		ID: "cross-domain", From: first.Document.ID, To: second.Document.ID,
		Type: "cross", Weight: 1,
	})
	if !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("cross-domain connect error = %v", err)
	}
}

func hasNode(neighborhood explorer.Neighborhood, id shoal.ID) bool {
	for _, node := range neighborhood.Nodes {
		if node.ID == id {
			return true
		}
	}
	return false
}
