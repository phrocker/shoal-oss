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

package explorer_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

var interactionEvidenceSequence atomic.Uint64

func TestInteractionPersistsCompleteEdgeEvidenceAndVisibility(t *testing.T) {
	path := filepath.Join(
		"testdata",
		fmt.Sprintf(
			"interaction-evidence-%d-%d",
			os.Getpid(), interactionEvidenceSequence.Add(1)),
	)
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	corpus, err := explorer.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := corpus.Close(); err != nil {
			t.Errorf("close corpus: %v", err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("remove corpus: %v", err)
		}
	})
	left := ingestVisible(
		t, corpus, "memory://left", "# Left\n\nleft evidence\n", "")
	right := ingestVisible(
		t, corpus, "memory://right", "# Right\n\nright evidence\n", "")
	edge := graph.Edge{
		ID: "source-edge", From: left[0], To: right[0],
		Type: "related", Weight: 1,
		Properties: shoal.Metadata{
			interaction.PropertyVisibility: "edge-secret",
		},
	}
	if err := corpus.Connect(context.Background(), edge); err != nil {
		t.Fatal(err)
	}
	reference := interaction.EvidenceReference{
		AnchorID: "graph-anchor", Kind: interaction.EvidenceGraph,
		NodeIDs: []shoal.ID{left[0], right[0]}, EdgeIDs: []shoal.ID{edge.ID},
		Assertions: []interaction.AssertionReference{{
			AssertionID: "assertion", EdgeID: edge.ID,
			Origin: ontology.AssertionInferred,
		}},
	}
	session := interaction.Session{
		ID:           "complete-evidence-session",
		RecordedAt:   time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC),
		Operation:    interaction.OperationChat,
		SeedNodeIDs:  []shoal.ID{left[0], right[0]},
		SeedEvidence: []interaction.EvidenceReference{reference},
	}
	if err := corpus.RecordInteraction(
		context.Background(), session); err != nil {
		t.Fatal(err)
	}
	summaries, err := corpus.Interactions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 ||
		summaries[0].Visibility != "edge-secret" {
		t.Fatalf("interaction summaries = %+v", summaries)
	}
	hydrated, err := corpus.Interaction(
		context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(hydrated.SeedEvidence) != 1 ||
		len(hydrated.TouchedEdgeIDs()) != 1 ||
		hydrated.TouchedEdgeIDs()[0] != edge.ID ||
		hydrated.SeedEvidence[0].Assertions[0].Origin !=
			ontology.AssertionInferred {
		t.Fatalf("hydrated evidence = %+v", hydrated.SeedEvidence)
	}
	hydrated.SeedEvidence[0].EdgeIDs[0] = "mutated"
	repeated, err := corpus.Interaction(
		context.Background(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if repeated.SeedEvidence[0].EdgeIDs[0] != edge.ID {
		t.Fatal("persisted evidence leaked caller mutation")
	}
}

func TestInteractionIdenticalRetryReturnsOriginalAcrossRestart(t *testing.T) {
	path := filepath.Join(
		"testdata",
		fmt.Sprintf(
			"interaction-retry-%d-%d",
			os.Getpid(), interactionEvidenceSequence.Add(1)),
	)
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
	corpus, err := explorer.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	sourceIDs := ingestVisible(
		t, corpus, "memory://retry", "# Retry\n\nretry evidence\n", "")
	snapshot, err := corpus.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	firstAt := snapshot.AsOf.Add(time.Second)
	session := interaction.Session{
		ID: "retry-session", Operation: interaction.OperationChat,
		SnapshotID: "snapshot", SnapshotAsOf: snapshot.AsOf,
		AuthorizationFingerprint: "auth",
		AuthorizationExpiresAt:   snapshot.AsOf.Add(time.Hour),
		SeedNodeIDs:              sourceIDs, ResultID: "result",
	}
	recorder, err := interaction.NewRecorder(context.Background(), corpus)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.SetClock(func() time.Time { return firstAt }); err != nil {
		t.Fatal(err)
	}
	first, err := recorder.Record(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.SetClock(func() time.Time {
		return firstAt.Add(time.Minute)
	}); err != nil {
		t.Fatal(err)
	}
	retried, err := recorder.Record(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}
	if !retried.RecordedAt.Equal(first.RecordedAt) {
		t.Fatalf("retry time = %v, want %v", retried.RecordedAt, first.RecordedAt)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}
	corpus, err = explorer.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := corpus.Close(); err != nil {
			t.Errorf("close corpus: %v", err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("remove corpus: %v", err)
		}
	})
	recorder, err = interaction.NewRecorder(context.Background(), corpus)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.SetClock(func() time.Time {
		return firstAt.Add(2 * time.Minute)
	}); err != nil {
		t.Fatal(err)
	}
	reopened, err := recorder.Record(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}
	if !reopened.RecordedAt.Equal(first.RecordedAt) {
		t.Fatalf("reopened retry time = %v, want %v",
			reopened.RecordedAt, first.RecordedAt)
	}
	divergent := session
	divergent.ResultID = "different-result"
	if _, err := recorder.Record(
		context.Background(), divergent,
	); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("divergent retry error = %v", err)
	}
	if _, err := corpus.DeleteInteraction(
		context.Background(), session.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.Record(
		context.Background(), session,
	); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("tombstoned retry error = %v", err)
	}
}
