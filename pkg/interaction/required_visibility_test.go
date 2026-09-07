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

package interaction_test

import (
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestRequiredVisibilityCanOnlyNarrowRecordedOutput(t *testing.T) {
	session := interaction.Session{
		ID: "session", RecordedAt: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC),
		Operation:          interaction.OperationChat,
		RequiredVisibility: []string{"tenant", "ops"},
		SeedNodeIDs:        []shoal.ID{"source"},
		CitedNodeIDs:       []shoal.ID{"source"},
	}
	subgraph, err := session.Subgraph(func(id shoal.ID) ([]string, error) {
		if id != "source" {
			t.Fatalf("unexpected source ID %q", id)
		}
		return []string{"secret", "ops"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ops", "secret", "tenant"}
	if len(subgraph.Visibility) != len(want) {
		t.Fatalf("visibility = %v, want %v", subgraph.Visibility, want)
	}
	for index := range want {
		if subgraph.Visibility[index] != want[index] {
			t.Fatalf("visibility = %v, want %v", subgraph.Visibility, want)
		}
	}
	canonical, err := session.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	canonical.RequiredVisibility[0] = "mutated"
	repeated, err := session.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if repeated.RequiredVisibility[0] != "ops" {
		t.Fatal("canonical session leaked required visibility mutation")
	}
}

func TestCompleteEvidenceRetainsEdgesAndJoinsTheirVisibility(t *testing.T) {
	reference := interaction.EvidenceReference{
		AnchorID: "anchor", Kind: interaction.EvidenceGraph,
		NodeIDs: []shoal.ID{"node-b", "node-a"},
		EdgeIDs: []shoal.ID{"edge"},
		Assertions: []interaction.AssertionReference{{
			AssertionID: "assertion", EdgeID: "edge",
			Origin: ontology.AssertionInferred,
		}},
	}
	session := interaction.Session{
		ID: "session", RecordedAt: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC),
		Operation:    interaction.OperationChat,
		SeedNodeIDs:  []shoal.ID{"node-a", "node-b"},
		SeedEvidence: []interaction.EvidenceReference{reference},
	}
	if _, err := session.Subgraph(func(shoal.ID) ([]string, error) {
		return []string{"node"}, nil
	}); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("missing edge resolver error = %v", err)
	}
	subgraph, err := session.SubgraphWithEvidence(
		func(shoal.ID) ([]string, error) {
			return []string{"node"}, nil
		},
		func(id shoal.ID) ([]string, error) {
			if id != "edge" {
				t.Fatalf("unexpected edge ID %q", id)
			}
			return []string{"edge-secret"}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(subgraph.TouchedEdgeIDs) != 1 ||
		subgraph.TouchedEdgeIDs[0] != "edge" {
		t.Fatalf("touched edges = %v", subgraph.TouchedEdgeIDs)
	}
	want := []string{"edge-secret", "node"}
	for index := range want {
		if subgraph.Visibility[index] != want[index] {
			t.Fatalf("visibility = %v, want %v", subgraph.Visibility, want)
		}
	}
	canonical, err := session.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	canonical.SeedEvidence[0].NodeIDs[0] = "mutated"
	repeated, err := session.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if repeated.SeedEvidence[0].NodeIDs[0] != "node-a" {
		t.Fatal("canonical session leaked complete evidence mutation")
	}
}

func TestDocumentEvidenceRequiresExactCitationNodes(t *testing.T) {
	citation := document.Citation{
		DocumentID: "document", RevisionID: "revision",
		SectionID: "section", SpanID: "span",
		Range: document.SourceRange{
			Start: document.SourcePosition{Offset: 1},
			End:   document.SourcePosition{Offset: 2},
		},
	}
	reference := interaction.EvidenceReference{
		AnchorID: "anchor", Kind: interaction.EvidenceDocument,
		Citation: citation,
		NodeIDs:  []shoal.ID{"document", "section"},
	}
	if _, err := reference.Canonical(); !shoal.IsErrorCode(
		err, shoal.ErrorInvalidArgument,
	) {
		t.Fatalf("incomplete citation reference error = %v", err)
	}
}
