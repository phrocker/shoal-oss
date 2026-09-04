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

package explorer

import (
	"context"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestLatentLinkAssertionsReachBoundedGraphReadPath(t *testing.T) {
	ctx := context.Background()
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	source := ingestLatentReadPathDocument(t, corpus, "source")
	target := ingestLatentReadPathDocument(t, corpus, "target")
	cell := latentReadPathCell(source.Document.ID, target.Document.ID, 42, false)
	if err := corpus.PutLatentLinkCells(ctx, []LatentLinkCell{cell}); err != nil {
		t.Fatal(err)
	}

	got, err := corpus.BoundedNeighborhood(ctx, BoundedNeighborhoodRequest{
		NodeIDs: []shoal.ID{source.Document.ID}, Depth: 1, Fanout: 8, MaxNodes: 8,
		EdgeTypes: []string{latentReadPathEdgeType(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Neighborhood.Edges) != 1 || len(got.Neighborhood.Assertions) != 1 {
		t.Fatalf("derived graph read = %+v, want one edge and one assertion", got)
	}
	assertion := got.Neighborhood.Assertions[0]
	if assertion.Origin() != ontology.AssertionDerived {
		t.Fatalf("assertion origin = %q, want derived", assertion.Origin())
	}
	derivation, ok := assertion.Evidence()[0].Derivation()
	if !ok {
		t.Fatal("derived assertion evidence lost derivation")
	}
	if derivation.SourceEndpoint() != source.Document.ID ||
		derivation.TargetEndpoint() != target.Document.ID ||
		derivation.IteratorName() != LatentLinkDefaultIteratorName {
		t.Fatalf("derivation = %+v, want source/target/iterator provenance", derivation)
	}
	if got.Neighborhood.Edges[0].Properties[latentAssertionEdgePropertyOrigin] != "derived" {
		t.Fatalf("edge properties = %+v, want derived origin", got.Neighborhood.Edges[0].Properties)
	}
}

func TestLatentLinkGraphReadSkipsTombstonedCells(t *testing.T) {
	ctx := context.Background()
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	source := ingestLatentReadPathDocument(t, corpus, "deleted-source")
	target := ingestLatentReadPathDocument(t, corpus, "deleted-target")
	live := latentReadPathCell(source.Document.ID, target.Document.ID, 10, false)
	deleted := latentReadPathCell(source.Document.ID, target.Document.ID, 20, true)
	if err := corpus.PutLatentLinkCells(ctx, []LatentLinkCell{live, deleted}); err != nil {
		t.Fatal(err)
	}

	got, err := corpus.BoundedNeighborhood(ctx, BoundedNeighborhoodRequest{
		NodeIDs: []shoal.ID{source.Document.ID}, Depth: 1, Fanout: 8, MaxNodes: 8,
		EdgeTypes: []string{latentReadPathEdgeType(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Neighborhood.Edges) != 0 || len(got.Neighborhood.Assertions) != 0 {
		t.Fatalf("tombstoned latent link was returned: %+v", got.Neighborhood)
	}
}

func TestLatentLinkGraphReadErrorsAtExplicitCap(t *testing.T) {
	ctx := context.Background()
	corpus, err := OpenWithOptions(t.TempDir(), Options{
		MaxLatentDerivedAssertionsPerGraphRead: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	source := ingestLatentReadPathDocument(t, corpus, "cap-source")
	first := ingestLatentReadPathDocument(t, corpus, "cap-first")
	second := ingestLatentReadPathDocument(t, corpus, "cap-second")
	if err := corpus.PutLatentLinkCells(ctx, []LatentLinkCell{
		latentReadPathCell(source.Document.ID, first.Document.ID, 10, false),
		latentReadPathCell(source.Document.ID, second.Document.ID, 11, false),
	}); err != nil {
		t.Fatal(err)
	}

	_, err = corpus.BoundedNeighborhood(ctx, BoundedNeighborhoodRequest{
		NodeIDs: []shoal.ID{source.Document.ID}, Depth: 1, Fanout: 8, MaxNodes: 8,
		EdgeTypes: []string{latentReadPathEdgeType(t)},
	})
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("cap error = %v, want unavailable", err)
	}
}

func ingestLatentReadPathDocument(
	t *testing.T,
	corpus *Explorer,
	name string,
) IngestResult {
	t.Helper()
	result, err := corpus.Ingest(context.Background(), Source{
		URI:       "file:///" + name + ".txt",
		Title:     name,
		MediaType: MediaTypeText,
		Content:   name,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func latentReadPathCell(
	source, target shoal.ID,
	timestamp int64,
	deleted bool,
) LatentLinkCell {
	return LatentLinkCell{
		Row:             []byte("cell-a:" + string(source)),
		ColumnFamily:    []byte("link"),
		ColumnQualifier: []byte(target),
		Timestamp:       timestamp,
		Deleted:         deleted,
		Value:           []byte("0.91"),
	}
}

func latentReadPathEdgeType(t *testing.T) string {
	t.Helper()
	projection, err := DefaultLatentLinkAssertionProjection()
	if err != nil {
		t.Fatal(err)
	}
	return string(projection.Predicate)
}
