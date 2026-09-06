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
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestProducerDerivationEdgeRequiresCanonicalReconstruction(t *testing.T) {
	ctx := context.Background()
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	source, err := corpus.Ingest(ctx, explorer.Source{
		URI:       "file:///producer-proof-source.txt",
		MediaType: explorer.MediaTypeText, Content: "source",
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err := corpus.Ingest(ctx, explorer.Source{
		URI:       "file:///producer-proof-target.txt",
		MediaType: explorer.MediaTypeText, Content: "target",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := corpus.PutLatentLinkCells(ctx, []explorer.LatentLinkCell{{
		Row:             []byte("cell-a:" + string(source.Document.ID)),
		ColumnFamily:    []byte("link"),
		ColumnQualifier: []byte(target.Document.ID),
		Timestamp:       1, Value: []byte("0.91"),
	}}); err != nil {
		t.Fatal(err)
	}
	projection, err := explorer.DefaultLatentLinkAssertionProjection()
	if err != nil {
		t.Fatal(err)
	}
	result, err := corpus.BoundedNeighborhood(
		ctx, explorer.BoundedNeighborhoodRequest{
			NodeIDs: []shoal.ID{source.Document.ID},
			Depth:   1, Fanout: 8, MaxNodes: 8,
			EdgeTypes: []string{string(projection.Predicate)},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Neighborhood.Assertions) != 1 {
		t.Fatalf("assertions = %d", len(result.Neighborhood.Assertions))
	}
	assertion := result.Neighborhood.Assertions[0]
	producer, assertionNode, edge, ok, err :=
		explorer.ProducerGraphElementsForAssertion(assertion)
	if err != nil || !ok {
		t.Fatalf("canonical producer projection = %v, %v", ok, err)
	}
	raw := map[shoal.ID]graph.Node{
		producer.ID: producer, assertionNode.ID: assertionNode,
	}
	if !producerDerivationEdgeMatches(edge, raw, assertion) {
		t.Fatal("canonical producer projection was rejected")
	}
	forged := producer
	forged.Properties = cloneMetadata(forged.Properties)
	forged.Properties["forged"] = "true"
	raw[producer.ID] = forged
	if producerDerivationEdgeMatches(edge, raw, assertion) {
		t.Fatal("forged producer node was accepted")
	}
	raw[producer.ID] = producer
	forgedAssertion := assertionNode
	forgedAssertion.Properties = cloneMetadata(forgedAssertion.Properties)
	forgedAssertion.Properties["forged"] = "true"
	raw[assertionNode.ID] = forgedAssertion
	if producerDerivationEdgeMatches(edge, raw, assertion) {
		t.Fatal("forged derived assertion node was accepted")
	}
	raw[assertionNode.ID] = assertionNode
	forgedEdge := edge
	forgedEdge.Properties = cloneMetadata(forgedEdge.Properties)
	forgedEdge.Properties["forged"] = "true"
	if producerDerivationEdgeMatches(forgedEdge, raw, assertion) {
		t.Fatal("forged produced edge was accepted")
	}
}
