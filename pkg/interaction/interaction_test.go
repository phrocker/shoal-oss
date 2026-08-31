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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestConjoinIsSortedUniqueUnion(t *testing.T) {
	labels, err := interaction.Conjoin(
		[]string{"secret", "ops"}, []string{"ops", "incident"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := interaction.Expression(labels); got != "incident&ops&secret" {
		t.Fatalf("conjunction = %q", got)
	}
	empty, err := interaction.Conjoin(nil, []string{})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty conjunction = %v", empty)
	}
}

func TestParseVisibilityRoundTripsAndRejectsBadLabels(t *testing.T) {
	labels, err := interaction.ParseVisibility(" secret & ops ")
	if err != nil {
		t.Fatal(err)
	}
	if got := interaction.Expression(labels); got != "ops&secret" {
		t.Fatalf("parsed = %q", got)
	}
	for _, bad := range []string{"a&", "a b", "a|b", "a&&b", "a(b)"} {
		if _, err := interaction.ParseVisibility(bad); err == nil {
			t.Fatalf("visibility %q was accepted", bad)
		}
	}
	if _, err := interaction.ParseVisibility(""); err != nil {
		t.Fatalf("empty visibility rejected: %v", err)
	}
	if _, err := interaction.ParseVisibility(
		strings.Repeat("x", interaction.MaxVisibilityLabelSz+1),
	); err == nil {
		t.Fatal("oversized label accepted")
	}
}

func TestSubgraphDistinguishesRetrievedFromCited(t *testing.T) {
	session := interaction.Session{
		ID:         "session-1",
		RecordedAt: time.Unix(1700000000, 0).UTC(),
		Turns: []interaction.Turn{{
			Index:    0,
			Decision: "retrieve",
			ToolCall: &interaction.ToolCall{
				Kind:             "retrieve",
				RetrievedNodeIDs: []shoal.ID{"span-a", "span-b"},
			},
		}},
		CitedNodeIDs: []shoal.ID{"span-a"},
	}
	sub, err := session.Subgraph(func(id shoal.ID) ([]string, error) {
		switch id {
		case "span-a":
			return []string{"ops"}, nil
		case "span-b":
			return []string{"secret"}, nil
		}
		return nil, errors.New("unknown node")
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := interaction.Expression(sub.Visibility); got != "ops&secret" {
		t.Fatalf("session visibility = %q, want ops&secret", got)
	}
	kinds := map[string]int{}
	for _, node := range sub.Nodes {
		kinds[node.Kind]++
	}
	if kinds[interaction.KindSession] != 1 ||
		kinds[interaction.KindTurn] != 1 ||
		kinds[interaction.KindToolCall] != 1 {
		t.Fatalf("node kinds = %v", kinds)
	}
	retrieved, cited := map[shoal.ID]bool{}, map[shoal.ID]bool{}
	for _, edge := range sub.Edges {
		switch edge.Type {
		case interaction.EdgeRetrieved:
			retrieved[edge.To] = true
		case interaction.EdgeCited:
			cited[edge.To] = true
		}
	}
	if !retrieved["span-a"] || !retrieved["span-b"] {
		t.Fatalf("retrieved edges = %v", retrieved)
	}
	if !cited["span-a"] || cited["span-b"] {
		t.Fatalf("cited edges = %v", cited)
	}
}

// TestSubgraphFailsClosedWhenVisibilityCannotBeResolved pins that an
// unresolvable node produces an error, never a silently public node.
func TestSubgraphFailsClosedWhenVisibilityCannotBeResolved(t *testing.T) {
	session := interaction.Session{
		ID:           "session-2",
		RecordedAt:   time.Unix(1700000000, 0).UTC(),
		CitedNodeIDs: []shoal.ID{"span-missing"},
	}
	if _, err := session.Subgraph(func(shoal.ID) ([]string, error) {
		return nil, errors.New("unknown node")
	}); err == nil {
		t.Fatal("subgraph resolved an unknown node")
	}
	if _, err := session.Subgraph(nil); err == nil {
		t.Fatal("subgraph accepted a nil resolver")
	}
}

func TestSessionValidateRejectsMalformedSessions(t *testing.T) {
	valid := interaction.Session{
		ID:         "session-3",
		RecordedAt: time.Unix(1700000000, 0).UTC(),
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	missingID := valid
	missingID.ID = ""
	if err := missingID.Validate(); err == nil {
		t.Fatal("empty session ID accepted")
	}
	missingTime := valid
	missingTime.RecordedAt = time.Time{}
	if err := missingTime.Validate(); err == nil {
		t.Fatal("zero recorded time accepted")
	}
	badSeed := valid
	badSeed.SeedNodeIDs = []shoal.ID{""}
	if err := badSeed.Validate(); err == nil {
		t.Fatal("empty seed node ID accepted")
	}
	tooMany := valid
	tooMany.Turns = make([]interaction.Turn, interaction.MaxTurns+1)
	if err := tooMany.Validate(); err == nil {
		t.Fatal("unbounded turn list accepted")
	}
}

func TestKindAndEdgeNamespaceDetection(t *testing.T) {
	for _, kind := range []string{
		interaction.KindSession, interaction.KindTurn,
		interaction.KindToolCall, interaction.KindTombstone,
	} {
		if !interaction.IsInteractionKind(kind) {
			t.Fatalf("%q is not detected as an interaction kind", kind)
		}
	}
	for _, kind := range []string{
		"document", "section", "span",
		"code.source", "code.syntax", "code.symbol", "code.external",
	} {
		if interaction.IsInteractionKind(kind) {
			t.Fatalf("content kind %q is detected as an interaction kind", kind)
		}
	}
	if !interaction.IsInteractionEdgeType(interaction.EdgeCited) ||
		interaction.IsInteractionEdgeType("contains") {
		t.Fatal("edge namespace detection is wrong")
	}
}

func TestTombstoneNodeCarriesAuditFields(t *testing.T) {
	tombstone := interaction.Tombstone{
		SessionID:  "session-4",
		DeletedAt:  time.Unix(1700000123, 0).UTC(),
		NodeCount:  3,
		EdgeCount:  4,
		Visibility: []string{"ops", "secret"},
	}
	node, err := tombstone.Node()
	if err != nil {
		t.Fatal(err)
	}
	if node.Kind != interaction.KindTombstone {
		t.Fatalf("kind = %q", node.Kind)
	}
	if node.ID != interaction.TombstoneID("session-4") {
		t.Fatalf("id = %q", node.ID)
	}
	if node.Properties[interaction.PropertySessionID] != "session-4" ||
		node.Properties[interaction.PropertyNodeCount] != "3" ||
		node.Properties[interaction.PropertyEdgeCount] != "4" ||
		node.Properties[interaction.PropertyVisibility] != "ops&secret" ||
		node.Properties[interaction.PropertyDeletedAt] == "" {
		t.Fatalf("tombstone properties = %v", node.Properties)
	}
	if _, err := (interaction.Tombstone{SessionID: "x"}).Node(); err == nil {
		t.Fatal("tombstone without a deletion time accepted")
	}
}

// TestSessionIDVariesWithCaptureInstant pins that two identical questions
// asked at different times get distinct durable records rather than colliding.
func TestSessionIDVariesWithCaptureInstant(t *testing.T) {
	first := interaction.SessionID("transcript-1", time.Unix(1700000000, 0).UTC())
	second := interaction.SessionID("transcript-1", time.Unix(1700000001, 0).UTC())
	if first == second {
		t.Fatal("session IDs collide across capture instants")
	}
	if first != interaction.SessionID(
		"transcript-1", time.Unix(1700000000, 0).UTC(),
	) {
		t.Fatal("session ID is not deterministic")
	}
	if !strings.HasPrefix(string(first), interaction.KindPrefix) {
		t.Fatalf("session ID %q is outside the reserved namespace", first)
	}
}
