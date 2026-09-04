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
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestAuthorizedLatentAssertionWithholdsUnauthorizedTarget(t *testing.T) {
	f := newFixture(t)
	admin := f.admin(t)
	visible, err := f.clientA.Ingest(admin, explorer.Source{
		URI: "file:///latent-visible-source.txt", MediaType: explorer.MediaTypeText,
		Content: "visible source",
	})
	if err != nil {
		t.Fatal(err)
	}
	hidden, err := f.clientB.Ingest(admin, explorer.Source{
		URI: "file:///latent-hidden-target.txt", MediaType: explorer.MediaTypeText,
		Content: "hidden target",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.base.PutLatentLinkCells(admin, []explorer.LatentLinkCell{
		authorizedLatentCell(visible.Document.ID, hidden.Document.ID, 42),
	}); err != nil {
		t.Fatal(err)
	}

	alice, err := f.clientA.BoundedNeighborhood(
		f.alice(t),
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{visible.Document.ID},
			Depth:   1, Fanout: 8, MaxNodes: 8,
			EdgeTypes: []string{authorizedLatentEdgeType(t)},
			Direction: explorer.GraphDirectionOutgoing,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(alice.Neighborhood.Edges) != 0 ||
		len(alice.Neighborhood.Assertions) != 0 ||
		hasNode(alice.Neighborhood, hidden.Document.ID) {
		t.Fatalf("unauthorized target leaked through latent assertion: %+v", alice.Neighborhood)
	}

	all, err := f.clientA.BoundedNeighborhood(
		admin,
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{visible.Document.ID},
			Depth:   1, Fanout: 8, MaxNodes: 8,
			EdgeTypes: []string{authorizedLatentEdgeType(t)},
			Direction: explorer.GraphDirectionOutgoing,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Neighborhood.Assertions) != 1 ||
		all.Neighborhood.Assertions[0].Origin() != ontology.AssertionDerived ||
		!hasNode(all.Neighborhood, hidden.Document.ID) {
		t.Fatalf("admin did not see derived assertion: %+v", all.Neighborhood)
	}
}

func TestAuthorizedLatentAssertionWithholdsUnauthorizedSource(t *testing.T) {
	f := newFixture(t)
	admin := f.admin(t)
	hidden, err := f.clientB.Ingest(admin, explorer.Source{
		URI: "file:///latent-hidden-source.txt", MediaType: explorer.MediaTypeText,
		Content: "hidden source",
	})
	if err != nil {
		t.Fatal(err)
	}
	visible, err := f.clientA.Ingest(admin, explorer.Source{
		URI: "file:///latent-visible-target.txt", MediaType: explorer.MediaTypeText,
		Content: "visible target",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.base.PutLatentLinkCells(admin, []explorer.LatentLinkCell{
		authorizedLatentCell(hidden.Document.ID, visible.Document.ID, 42),
	}); err != nil {
		t.Fatal(err)
	}

	alice, err := f.clientA.BoundedNeighborhood(
		f.alice(t),
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{visible.Document.ID},
			Depth:   1, Fanout: 8, MaxNodes: 8,
			EdgeTypes: []string{authorizedLatentEdgeType(t)},
			Direction: explorer.GraphDirectionIncoming,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(alice.Neighborhood.Edges) != 0 ||
		len(alice.Neighborhood.Assertions) != 0 ||
		hasNode(alice.Neighborhood, hidden.Document.ID) {
		t.Fatalf("unauthorized source leaked through latent assertion: %+v", alice.Neighborhood)
	}

	all, err := f.clientA.BoundedNeighborhood(
		admin,
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{visible.Document.ID},
			Depth:   1, Fanout: 8, MaxNodes: 8,
			EdgeTypes: []string{authorizedLatentEdgeType(t)},
			Direction: explorer.GraphDirectionIncoming,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Neighborhood.Assertions) != 1 ||
		all.Neighborhood.Assertions[0].Origin() != ontology.AssertionDerived ||
		!hasNode(all.Neighborhood, hidden.Document.ID) {
		t.Fatalf("admin did not see incoming derived assertion: %+v", all.Neighborhood)
	}
}

func TestAuthorizedProducerProvenanceDoesNotAggregateHiddenAssertions(t *testing.T) {
	f := newFixture(t)
	admin := f.admin(t)
	source, err := f.clientA.Ingest(admin, explorer.Source{
		URI: "file:///producer-visible-source.txt", MediaType: explorer.MediaTypeText,
		Content: "visible producer source",
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err := f.clientA.Ingest(admin, explorer.Source{
		URI: "file:///producer-visible-target.txt", MediaType: explorer.MediaTypeText,
		Content: "visible producer target",
	})
	if err != nil {
		t.Fatal(err)
	}
	bobSource, err := f.clientB.Ingest(admin, explorer.Source{
		URI: "file:///producer-bob-source.txt", MediaType: explorer.MediaTypeText,
		Content: "bob producer source",
	})
	if err != nil {
		t.Fatal(err)
	}
	bobTarget, err := f.clientB.Ingest(admin, explorer.Source{
		URI: "file:///producer-bob-target.txt", MediaType: explorer.MediaTypeText,
		Content: "bob producer target",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.base.PutLatentLinkCells(admin, []explorer.LatentLinkCell{
		authorizedLatentCell(source.Document.ID, target.Document.ID, 42),
		authorizedLatentCell(bobSource.Document.ID, bobTarget.Document.ID, 43),
	}); err != nil {
		t.Fatal(err)
	}

	aliceSource, err := f.clientA.BoundedNeighborhood(
		f.alice(t),
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{source.Document.ID},
			Depth:   1, Fanout: 8, MaxNodes: 8,
			EdgeTypes: []string{authorizedLatentEdgeType(t)},
			Direction: explorer.GraphDirectionOutgoing,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(aliceSource.Neighborhood.Assertions) != 1 {
		t.Fatalf("alice source graph = %+v, want one derived assertion",
			aliceSource.Neighborhood)
	}
	assertionID := aliceSource.Neighborhood.Assertions[0].ID()
	aliceAssertion, err := f.clientA.BoundedNeighborhood(
		f.alice(t),
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{assertionID},
			Depth:   1, Fanout: 8, MaxNodes: 8,
			EdgeTypes: []string{graph.EdgeTypeProduced},
			Direction: explorer.GraphDirectionIncoming,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(aliceAssertion.Neighborhood.Edges) != 1 ||
		len(aliceAssertion.Neighborhood.Assertions) != 1 {
		t.Fatalf("alice assertion provenance = %+v, want one producer edge",
			aliceAssertion.Neighborhood)
	}
	producerID := aliceAssertion.Neighborhood.Edges[0].From
	aliceProducer, err := f.clientA.BoundedNeighborhood(
		f.alice(t),
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{producerID},
			Depth:   1, Fanout: 8, MaxNodes: 8,
			EdgeTypes: []string{graph.EdgeTypeProduced},
			Direction: explorer.GraphDirectionOutgoing,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(aliceProducer.Neighborhood.Edges) != 1 ||
		len(aliceProducer.Neighborhood.Assertions) != 1 ||
		aliceProducer.Neighborhood.Assertions[0].ID() != assertionID {
		t.Fatalf("alice producer graph = %+v, want only her produced assertion",
			aliceProducer.Neighborhood)
	}

	bobProducer, err := f.clientA.BoundedNeighborhood(
		f.bob(t),
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{producerID},
			Depth:   1, Fanout: 8, MaxNodes: 8,
			EdgeTypes: []string{graph.EdgeTypeProduced},
			Direction: explorer.GraphDirectionOutgoing,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(bobProducer.Neighborhood.Edges) != 1 ||
		len(bobProducer.Neighborhood.Assertions) != 1 ||
		bobProducer.Neighborhood.Assertions[0].ID() == assertionID {
		t.Fatalf("bob producer graph = %+v, want only bob's produced assertion",
			bobProducer.Neighborhood)
	}
	adminProducer, err := f.clientA.BoundedNeighborhood(
		admin,
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{producerID},
			Depth:   1, Fanout: 8, MaxNodes: 8,
			EdgeTypes: []string{graph.EdgeTypeProduced},
			Direction: explorer.GraphDirectionOutgoing,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(adminProducer.Neighborhood.Edges) != 2 ||
		len(adminProducer.Neighborhood.Assertions) != 2 {
		t.Fatalf("admin producer graph = %+v, want both produced assertions",
			adminProducer.Neighborhood)
	}
	noAccess := f.context(t, f.decision(
		t, "no-access", nil, nil, allOperations))
	_, err = f.clientA.BoundedNeighborhood(
		noAccess,
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{producerID},
			Depth:   1, Fanout: 8, MaxNodes: 8,
			EdgeTypes: []string{graph.EdgeTypeProduced},
			Direction: explorer.GraphDirectionOutgoing,
		},
	)
	if !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("no-access producer error = %v, want not found instead of empty results", err)
	}
	empty, err := f.clientA.BoundedNeighborhood(
		f.alice(t),
		explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{target.Document.ID},
			Depth:   1, Fanout: 8, MaxNodes: 8,
			EdgeTypes: []string{graph.EdgeTypeProduced},
			Direction: explorer.GraphDirectionOutgoing,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Neighborhood.Edges) != 0 {
		t.Fatalf("genuine empty provenance graph = %+v, want no edges", empty.Neighborhood)
	}
}

func authorizedLatentEdgeType(t *testing.T) string {
	t.Helper()
	projection, err := explorer.DefaultLatentLinkAssertionProjection()
	if err != nil {
		t.Fatal(err)
	}
	return string(projection.Predicate)
}

func authorizedLatentCell(source, target shoal.ID, timestamp int64) explorer.LatentLinkCell {
	return explorer.LatentLinkCell{
		Row:             []byte("cell-a:" + string(source)),
		ColumnFamily:    []byte("link"),
		ColumnQualifier: []byte(target),
		Timestamp:       timestamp,
		Value:           []byte("0.91"),
	}
}
