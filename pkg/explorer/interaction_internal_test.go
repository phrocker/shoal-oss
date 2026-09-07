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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestInteractionResultCancellationAfterCommitIsMarked(t *testing.T) {
	corpus, session := internalInteractionFixture(t, "cancel")
	ctx, cancel := context.WithCancel(context.Background())
	write := corpus.writeRecord
	corpus.interactionRecordWriter = func(
		row []byte, kind byte, value any,
	) error {
		if err := write(row, kind, value); err != nil {
			return err
		}
		cancel()
		return nil
	}
	recorded, err := corpus.RecordInteractionResult(ctx, session)
	if !IsCommittedInteraction(err) {
		t.Fatalf("post-commit cancellation error = %v", err)
	}
	if recorded.ID != "" {
		t.Fatalf("committed failure returned success-shaped session: %+v", recorded)
	}
	if _, err := corpus.Interaction(
		context.Background(), session.ID); err != nil {
		t.Fatalf("canceled committed interaction is not durable: %v", err)
	}
}

func TestInteractionResultWinsConcurrentDeletion(t *testing.T) {
	corpus, session := internalInteractionFixture(t, "delete-race")
	write := corpus.writeRecord
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	corpus.interactionRecordWriter = func(
		row []byte, kind byte, value any,
	) error {
		if err := write(row, kind, value); err != nil {
			return err
		}
		once.Do(func() { close(entered) })
		<-release
		return nil
	}
	type result struct {
		session interaction.Session
		err     error
	}
	recorded := make(chan result, 1)
	go func() {
		value, err := corpus.RecordInteractionResult(
			context.Background(), session)
		recorded <- result{session: value, err: err}
	}()
	<-entered
	deleted := make(chan error, 1)
	go func() {
		_, err := corpus.DeleteInteraction(
			context.Background(), session.ID)
		deleted <- err
	}()
	time.Sleep(10 * time.Millisecond)
	close(release)
	got := <-recorded
	if got.err != nil || got.session.ID != session.ID {
		t.Fatalf("record result = %+v, %v", got.session, got.err)
	}
	if err := <-deleted; err != nil {
		t.Fatalf("concurrent deletion error = %v", err)
	}
}

func TestInteractionResultWinsConcurrentVisibilityChange(t *testing.T) {
	corpus, session := internalInteractionFixture(t, "visibility-race")
	write := corpus.writeRecord
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	corpus.interactionRecordWriter = func(
		row []byte, kind byte, value any,
	) error {
		if err := write(row, kind, value); err != nil {
			return err
		}
		once.Do(func() { close(entered) })
		<-release
		return nil
	}
	type result struct {
		session interaction.Session
		err     error
	}
	recorded := make(chan result, 1)
	go func() {
		value, err := corpus.RecordInteractionResult(
			context.Background(), session)
		recorded <- result{session: value, err: err}
	}()
	<-entered
	replaced := make(chan error, 1)
	go func() {
		_, err := corpus.Ingest(context.Background(), Source{
			URI: "file:///visibility-race.txt", MediaType: MediaTypeText,
			Content: "replacement source",
			Metadata: shoal.Metadata{
				interaction.PropertyVisibility: "new-secret",
			},
		})
		replaced <- err
	}()
	time.Sleep(10 * time.Millisecond)
	close(release)
	got := <-recorded
	if got.err != nil || got.session.ID != session.ID {
		t.Fatalf("record result = %+v, %v", got.session, got.err)
	}
	if err := <-replaced; err != nil {
		t.Fatalf("concurrent source replacement error = %v", err)
	}
}

func internalInteractionFixture(
	t *testing.T, name string,
) (*Explorer, interaction.Session) {
	t.Helper()
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := corpus.Close(); err != nil {
			t.Errorf("close corpus: %v", err)
		}
	})
	receipt, err := corpus.Ingest(context.Background(), Source{
		URI: "file:///" + name + ".txt", MediaType: MediaTypeText,
		Content: "source evidence",
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := corpus.Document(
		context.Background(), receipt.Document.ID, receipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := corpus.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	recordedAt := snapshot.AsOf.Add(time.Second)
	return corpus, interaction.Session{
		ID: shoal.ID(name + "-session"), RecordedAt: recordedAt,
		Operation:  interaction.OperationChat,
		SnapshotID: "snapshot", SnapshotAsOf: snapshot.AsOf,
		AuthorizationFingerprint: "auth",
		AuthorizationExpiresAt:   recordedAt.Add(time.Hour),
		SeedNodeIDs:              []shoal.ID{view.Root.Spans[0].ID},
	}
}

func TestInteractionWriteResolvesCommittedIndeterminateOutcome(t *testing.T) {
	ctx := context.Background()
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	receipt, err := corpus.Ingest(ctx, Source{
		URI:       "file:///source.txt",
		MediaType: MediaTypeText,
		Content:   "durable source",
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := corpus.Document(ctx, receipt.Document.ID, receipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	spanID := view.Root.Spans[0].ID
	session := interaction.Session{
		ID:                       "session-indeterminate-committed",
		RecordedAt:               time.Unix(1700000000, 0).UTC(),
		SnapshotID:               "snapshot",
		SnapshotAsOf:             time.Unix(1699999990, 0).UTC(),
		AuthorizationFingerprint: "auth-sha256:test",
		AuthorizationExpiresAt:   time.Unix(1700003600, 0).UTC(),
		SeedNodeIDs:              []shoal.ID{spanID},
	}
	write := corpus.writeRecord
	corpus.interactionRecordWriter = func(
		row []byte, kind byte, value any,
	) error {
		if err := write(row, kind, value); err != nil {
			return err
		}
		return MarkIndeterminateCommit(errors.New("post-commit failure"))
	}
	if err := corpus.RecordInteraction(ctx, session); err != nil {
		t.Fatalf("committed indeterminate write was not reconciled: %v", err)
	}
	if _, err := corpus.Interaction(ctx, session.ID); err != nil {
		t.Fatalf("reconciled interaction is not hydrated: %v", err)
	}
}

func TestInteractionWritePreservesUnresolvedIndeterminateOutcome(t *testing.T) {
	ctx := context.Background()
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	defer corpus.Close()
	receipt, err := corpus.Ingest(ctx, Source{
		URI:       "file:///source.txt",
		MediaType: MediaTypeText,
		Content:   "durable source",
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := corpus.Document(ctx, receipt.Document.ID, receipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:                       "session-indeterminate-absent",
		RecordedAt:               time.Unix(1700000000, 0).UTC(),
		SnapshotID:               "snapshot",
		SnapshotAsOf:             time.Unix(1699999990, 0).UTC(),
		AuthorizationFingerprint: "auth-sha256:test",
		AuthorizationExpiresAt:   time.Unix(1700003600, 0).UTC(),
		SeedNodeIDs:              []shoal.ID{view.Root.Spans[0].ID},
	}
	corpus.interactionRecordWriter = func(
		[]byte, byte, any,
	) error {
		return MarkIndeterminateCommit(errors.New("unknown commit"))
	}
	err = corpus.RecordInteraction(ctx, session)
	if !IsIndeterminateCommit(err) {
		t.Fatalf("unresolved write error = %v", err)
	}
	if _, err := corpus.Interaction(ctx, session.ID); !shoal.IsErrorCode(
		err, shoal.ErrorNotFound,
	) {
		t.Fatalf("uncommitted interaction became visible: %v", err)
	}
}

func TestInteractionLoadRejectsConflictingLiveVersions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	corpus, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:         interaction.DerivedID("session", "conflicting-live"),
		RecordedAt: time.Unix(1700000000, 0).UTC(),
		Operation:  interaction.OperationRetrieval,
	}
	if err := corpus.RecordInteraction(ctx, session); err != nil {
		t.Fatal(err)
	}
	conflicting := *corpus.interactions[session.ID]
	conflicting.Session.StopReason = "different"
	if err := validatePersistedInteraction(conflicting); err != nil {
		t.Fatal(err)
	}
	if err := corpus.writeRecord(
		interactionRecordRow(session.ID),
		embeddedRecordInteraction,
		conflicting,
	); err != nil {
		t.Fatal(err)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(dir); err == nil {
		_ = reopened.Close()
		t.Fatal("conflicting durable live interaction versions were accepted")
	}
}

func TestFoldRetryAdoptsCommittedRecord(t *testing.T) {
	ctx := context.Background()
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	session := interaction.Session{
		ID:         interaction.DerivedID("session", "fold-retry"),
		RecordedAt: time.Unix(1700000000, 0).UTC(),
		Operation:  interaction.OperationRetrieval,
	}
	if err := corpus.RecordInteraction(ctx, session); err != nil {
		t.Fatal(err)
	}
	summaryDigest := interaction.Digest("fold retry")
	fold := interaction.Fold{
		Members:       []interaction.FoldMember{{SessionID: session.ID}},
		SummaryDigest: summaryDigest,
		FoldedAt:      time.Unix(1700000100, 0).UTC(),
	}
	subgraph, err := fold.Subgraph(func(shoal.ID) ([]string, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := fold.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	record := persistedFold{
		FoldID: subgraph.ID, Members: canonical.Members,
		SummaryDigest: canonical.SummaryDigest,
		Nodes:         subgraph.Nodes, Edges: subgraph.Edges,
		Visibility: interaction.Expression(subgraph.Visibility),
		FoldedAt:   fold.FoldedAt,
	}
	if err := corpus.writeRecord(
		foldRecordRow(record.FoldID), embeddedRecordFold, record,
	); err != nil {
		t.Fatal(err)
	}
	result, err := corpus.FoldInteractions(ctx, FoldRequest{
		SessionIDs: []shoal.ID{session.ID}, SummaryDigest: summaryDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Created || !result.FoldedAt.Equal(record.FoldedAt) {
		t.Fatalf("reconciled fold result = %+v", result)
	}
}

func TestFoldVisibilityTracksPersistedEdgeAndOutputRestrictions(t *testing.T) {
	corpus := &Explorer{
		graphInitialized: true,
		graphNodes: map[shoal.ID]graph.Node{
			"source": {
				ID: "source", Kind: "source",
				Properties: shoal.Metadata{
					interaction.PropertyVisibility: "source",
				},
			},
		},
		graphEdges: map[shoal.ID]graph.Edge{
			"evidence-edge": {
				ID: "evidence-edge", From: "left", To: "right", Type: "links",
				Properties: shoal.Metadata{
					interaction.PropertyVisibility: "edge",
				},
			},
		},
	}
	record := &persistedFold{
		Nodes: []graph.Node{{ID: "fold", Kind: interaction.KindFold}},
		Edges: []graph.Edge{{
			ID: "retrieved", From: "fold", To: "source",
			Type: interaction.EdgeRetrieved,
		}},
		SourceEdgeIDs:      []shoal.ID{"evidence-edge"},
		RequiredVisibility: []string{"output"},
		Visibility:         "edge&output&source",
	}
	current, err := corpus.currentFoldVisibilityLocked(record)
	if err != nil {
		t.Fatal(err)
	}
	if current != record.Visibility {
		t.Fatalf("current fold visibility = %q, want %q",
			current, record.Visibility)
	}
	edge := corpus.graphEdges["evidence-edge"]
	edge.Properties[interaction.PropertyVisibility] = "edge&tight"
	corpus.graphEdges["evidence-edge"] = edge
	current, err = corpus.currentFoldVisibilityLocked(record)
	if err != nil {
		t.Fatal(err)
	}
	if visibilityCovered(record.Visibility, current) {
		t.Fatalf("tightened edge remained covered: stored=%q current=%q",
			record.Visibility, current)
	}
}

func TestInteractionVisibilityTracksPersistedEdgeAndOutputRestrictions(
	t *testing.T,
) {
	corpus := &Explorer{
		graphInitialized: true,
		graphNodes: map[shoal.ID]graph.Node{
			"source": {
				ID: "source", Kind: "source",
				Properties: shoal.Metadata{
					interaction.PropertyVisibility: "source",
				},
			},
		},
		graphEdges: map[shoal.ID]graph.Edge{
			"evidence-edge": {
				ID: "evidence-edge", From: "left", To: "right", Type: "links",
				Properties: shoal.Metadata{
					interaction.PropertyVisibility: "edge",
				},
			},
		},
	}
	record := &persistedInteraction{
		SessionID: "session",
		Session: interaction.Session{
			ID:                 "session",
			RequiredVisibility: []string{"output"},
			CitedEvidence: []interaction.EvidenceReference{{
				AnchorID: "anchor", Kind: interaction.EvidenceGraph,
				NodeIDs: []shoal.ID{"source"},
				EdgeIDs: []shoal.ID{"evidence-edge"},
			}},
		},
		Nodes: []graph.Node{{ID: "session", Kind: interaction.KindSession}},
		Edges: []graph.Edge{{
			ID: "cited", From: "session", To: "source",
			Type: interaction.EdgeCited,
		}},
		Visibility: "edge&output&source",
	}
	corpus.interactions = map[shoal.ID]*persistedInteraction{
		record.SessionID: record,
	}
	corpus.interactionOrder = []shoal.ID{record.SessionID}
	current, err := corpus.currentInteractionVisibilityLocked(record)
	if err != nil {
		t.Fatal(err)
	}
	if current != record.Visibility {
		t.Fatalf("current interaction visibility = %q, want %q",
			current, record.Visibility)
	}
	if records, err := corpus.InteractionRecords(
		context.Background(),
	); err != nil || len(records) != 1 {
		t.Fatalf("initial interaction records = %d, %v", len(records), err)
	}
	if _, err := corpus.InteractionRecord(
		context.Background(), record.SessionID,
	); err != nil {
		t.Fatalf("initial interaction point read = %v", err)
	}
	edge := corpus.graphEdges["evidence-edge"]
	edge.Properties[interaction.PropertyVisibility] = "edge&tight"
	corpus.graphEdges["evidence-edge"] = edge
	current, err = corpus.currentInteractionVisibilityLocked(record)
	if err != nil {
		t.Fatal(err)
	}
	if visibilityCovered(record.Visibility, current) {
		t.Fatalf("tightened edge remained covered: stored=%q current=%q",
			record.Visibility, current)
	}
	if records, err := corpus.InteractionRecords(
		context.Background(),
	); err != nil || len(records) != 0 {
		t.Fatalf("tightened interaction records = %d, %v", len(records), err)
	}
	if _, err := corpus.InteractionRecord(
		context.Background(), record.SessionID,
	); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("tightened interaction point read = %v", err)
	}
}

func TestDeleteRetryAdoptsCommittedTombstone(t *testing.T) {
	ctx := context.Background()
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	session := interaction.Session{
		ID:         interaction.DerivedID("session", "delete-retry"),
		RecordedAt: time.Unix(1700000000, 0).UTC(),
		Operation:  interaction.OperationRetrieval,
	}
	if err := corpus.RecordInteraction(ctx, session); err != nil {
		t.Fatal(err)
	}
	existing := corpus.interactions[session.ID]
	deletedAt := time.Unix(1700000200, 0).UTC()
	tombstone := interaction.Tombstone{
		SessionID: session.ID, DeletedAt: deletedAt,
		NodeCount: len(existing.Nodes), EdgeCount: len(existing.Edges),
	}
	node, err := tombstone.Node()
	if err != nil {
		t.Fatal(err)
	}
	record := persistedInteraction{
		SessionID: session.ID, Operation: existing.Operation,
		Nodes: []graph.Node{node}, RecordedAt: existing.RecordedAt,
		Deleted: true, DeletedAt: deletedAt,
	}
	if err := validatePersistedInteraction(record); err != nil {
		t.Fatal(err)
	}
	if err := corpus.writeRecord(
		interactionRecordRow(session.ID),
		embeddedRecordInteraction,
		record,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := corpus.DeleteInteraction(
		ctx, session.ID,
	); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("reconciled delete error = %v", err)
	}
	if got := corpus.interactions[session.ID]; !got.Deleted ||
		!got.DeletedAt.Equal(deletedAt) {
		t.Fatalf("committed tombstone was not adopted: %+v", got)
	}
}

func TestFoldLoadRejectsConflictingLiveVersions(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	corpus, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:         interaction.DerivedID("session", "fold-live-conflict"),
		RecordedAt: time.Unix(1700000000, 0).UTC(),
		Operation:  interaction.OperationRetrieval,
	}
	if err := corpus.RecordInteraction(ctx, session); err != nil {
		t.Fatal(err)
	}
	result, err := corpus.FoldInteractions(ctx, FoldRequest{
		SessionIDs:    []shoal.ID{session.ID},
		SummaryDigest: interaction.Digest("conflicting fold"),
	})
	if err != nil {
		t.Fatal(err)
	}
	conflicting := *corpus.folds[result.FoldID]
	conflicting.FoldedAt = conflicting.FoldedAt.Add(time.Second)
	if err := validatePersistedFold(conflicting); err != nil {
		t.Fatal(err)
	}
	if err := corpus.writeRecord(
		foldRecordRow(result.FoldID), embeddedRecordFold, conflicting,
	); err != nil {
		t.Fatal(err)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(dir); err == nil {
		_ = reopened.Close()
		t.Fatal("conflicting durable live fold versions were accepted")
	}
}

func TestExactReadbackChecksOnlyCurrentVersion(t *testing.T) {
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	older := persistedInteractionSink{
		CheckedAt: time.Unix(1700000000, 0).UTC(),
	}
	expected, err := encodeEmbeddedRecord(
		embeddedRecordInteractionSink, older)
	if err != nil {
		t.Fatal(err)
	}
	if err := corpus.writeRecord(
		interactionSinkRow, embeddedRecordInteractionSink, older,
	); err != nil {
		t.Fatal(err)
	}
	newer := persistedInteractionSink{
		CheckedAt: older.CheckedAt.Add(time.Second),
	}
	if err := corpus.writeRecord(
		interactionSinkRow, embeddedRecordInteractionSink, newer,
	); err != nil {
		t.Fatal(err)
	}
	committed, err := corpus.hasExactRecord(interactionSinkRow, expected)
	if err != nil {
		t.Fatal(err)
	}
	if committed {
		t.Fatal("historical matching value masked the current durable value")
	}
}

func TestConditionalInteractionCreateKeepsOneWinner(t *testing.T) {
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	recordedAt := time.Unix(1700000000, 0).UTC()
	build := func(stopReason string) persistedInteraction {
		session := interaction.Session{
			ID:         interaction.DerivedID("session", "cas-winner"),
			RecordedAt: recordedAt, Operation: interaction.OperationRetrieval,
			StopReason: stopReason,
		}
		subgraph, err := session.Subgraph(
			func(shoal.ID) ([]string, error) { return nil, nil })
		if err != nil {
			t.Fatal(err)
		}
		return persistedInteraction{
			SessionID: session.ID, Session: session,
			Operation: session.Operation, Nodes: subgraph.Nodes,
			Edges: subgraph.Edges, RecordedAt: recordedAt,
		}
	}
	first := build("first")
	accepted, err := corpus.createInteractionRecord(
		interactionRecordRow(first.SessionID),
		embeddedRecordInteraction,
		first,
	)
	if err != nil || !accepted {
		t.Fatalf("first conditional create = %t, %v", accepted, err)
	}
	second := build("second")
	accepted, err = corpus.createInteractionRecord(
		interactionRecordRow(second.SessionID),
		embeddedRecordInteraction,
		second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if accepted {
		t.Fatal("conflicting conditional create was accepted")
	}
	stored, found, err := corpus.lookupPersistedInteraction(first.SessionID)
	if err != nil || !found {
		t.Fatalf("lookup winner = %t, %v", found, err)
	}
	if stored.Session.StopReason != first.Session.StopReason {
		t.Fatalf("durable winner = %+v", stored.Session)
	}
}
