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
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestDerivedEdgeRegistrationWinsOverALegacyApplicationEdge pins that an
// extracted or a materialized registration wins its ID over an application
// registration of other content or rule, and over a leftover reservation,
// instead of refusing the extraction or materialization for good (#732), as
// the base lets the derived edge win (#724). The derived registration
// replaces the application one whole, binding included, across a reopen,
// and the application edge can never take the ID back.
func TestDerivedEdgeRegistrationWinsOverALegacyApplicationEdge(t *testing.T) {
	for _, kind := range policyStoreKinds {
		for _, derivedKind := range []authorized.RegistrationKind{
			authorized.RegistrationExtracted, authorized.RegistrationMaterialized,
		} {
			for _, sameContent := range []bool{false, true} {
				name := fmt.Sprintf("%s/kind=%d/same-content=%v", kind.name, derivedKind, sameContent)
				t.Run(name, func(t *testing.T) {
					testDerivedEdgeWins(t, kind, derivedKind, sameContent)
				})
			}
		}
	}
}

func testDerivedEdgeWins(
	t *testing.T, kind policyStoreKind, derivedKind authorized.RegistrationKind,
	sameContent bool,
) {
	ctx := context.Background()
	f := newFixture(t)
	store, reopen := kind.open(t)
	derivedEdge := graph.Edge{
		ID: "derived-edge", From: "node-a", To: "node-b", Type: "uses", Weight: 1,
	}
	squat := derivedEdge
	if !sameContent {
		squat.Type, squat.Weight = "related", 0.5
	}
	application := authorized.EdgeRegistration{
		Edge: squat, Rule: f.sourceRule(t, f.sourceB, f.policyB),
		Kind: authorized.RegistrationApplication,
		SourceRevisions: []authorized.RevisionReference{
			{DocumentID: "doc-x", RevisionID: "rev-x"},
		},
		SourceRevisionsBound: true,
	}
	if err := store.ReserveEdge(ctx, application); err != nil {
		t.Fatal(err)
	}
	if err := store.PutEdge(ctx, application); err != nil {
		t.Fatal(err)
	}
	// A leftover reservation, as an ambiguous Connect failure leaves behind.
	leftover := application
	leftover.Edge.ID = "reserved-edge"
	if err := store.ReserveEdge(ctx, leftover); err != nil {
		t.Fatal(err)
	}
	derivedRule := f.sourceRule(t, f.sourceA, f.policyA)
	derived := func(edge graph.Edge) authorized.EdgeRegistration {
		return authorized.EdgeRegistration{
			Edge: edge, DocumentID: "doc-a", RevisionID: "rev-a",
			Rule: derivedRule, Kind: derivedKind,
		}
	}
	reserved := derivedEdge
	reserved.ID = leftover.Edge.ID
	for _, edge := range []graph.Edge{derivedEdge, reserved} {
		if err := store.PutEdge(ctx, derived(edge)); err != nil {
			t.Fatalf("derived registration of %s = %v", edge.ID, err)
		}
		if err := store.PutEdge(ctx, derived(edge)); err != nil {
			t.Fatalf("derived registration retry of %s = %v", edge.ID, err)
		}
	}
	require := func(stage string, store authorized.PolicyStore) {
		t.Helper()
		for _, edge := range []graph.Edge{derivedEdge, reserved} {
			got, ok, err := store.Edge(ctx, edge.ID)
			if err != nil || !ok || got.Kind != derivedKind ||
				got.Edge.Type != edge.Type || got.Edge.Weight != edge.Weight ||
				got.SourceRevisionsBound || len(got.SourceRevisions) != 0 ||
				got.Rule.String() != derivedRule.String() {
				t.Fatalf("%s: %s registered as %+v, %v, %v", stage, edge.ID, got, ok, err)
			}
		}
		for _, retry := range []authorized.EdgeRegistration{application, leftover} {
			if err := store.ReserveEdge(ctx, retry); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
				t.Fatalf("%s: reserve %s again = %v, want conflict", stage, retry.Edge.ID, err)
			}
			if err := store.PutEdge(ctx, retry); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
				t.Fatalf("%s: register %s again = %v, want conflict", stage, retry.Edge.ID, err)
			}
		}
	}
	require("after registration", store)
	store = reopen()
	require("after reopen", store)
}
