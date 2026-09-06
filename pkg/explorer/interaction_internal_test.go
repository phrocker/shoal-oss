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
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

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
	write := corpus.interactionRecordWriter
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
