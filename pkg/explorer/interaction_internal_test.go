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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type stagedCancellationContext struct {
	context.Context
	mu          sync.Mutex
	calls       int
	cancelAfter int
}

func (c *stagedCancellationContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls >= c.cancelAfter {
		return context.Canceled
	}
	return nil
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
		ID:                       "interaction.session_indeterminate-committed",
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
		ID:                       "interaction.session_indeterminate-absent",
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

func TestInteractionResultMarksCancellationAfterDurableCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	defer corpus.Close()
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
	session := interaction.Session{
		ID:         interaction.DerivedID("session", "cancel-after-commit"),
		RecordedAt: time.Unix(1700000000, 0).UTC(),
		Operation:  interaction.OperationToolCall,
	}
	accepted, err := corpus.RecordInteractionResult(ctx, session)
	if !IsCommittedInteraction(err) ||
		!shoal.IsErrorCode(err, shoal.ErrorCanceled) {
		t.Fatalf("post-commit cancellation error = %v", err)
	}
	if accepted.ID != session.ID {
		t.Fatalf("accepted session = %+v", accepted)
	}
	if _, err := corpus.Interaction(
		context.Background(), session.ID); err != nil {
		t.Fatalf("committed session is unavailable: %v", err)
	}
}

func TestInteractionResultDoesNotPerformPostCommitPublicRead(t *testing.T) {
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	ctx := &stagedCancellationContext{
		Context: context.Background(), cancelAfter: 3,
	}
	session := interaction.Session{
		ID:         interaction.DerivedID("session", "single-lock-result"),
		RecordedAt: time.Unix(1700000000, 0).UTC(),
		Operation:  interaction.OperationToolCall,
	}
	accepted, err := corpus.RecordInteractionResult(ctx, session)
	if err != nil {
		t.Fatalf("atomic result performed a post-commit read: %v", err)
	}
	if accepted.ID != session.ID {
		t.Fatalf("accepted session = %+v", accepted)
	}
	ctx.mu.Lock()
	calls := ctx.calls
	ctx.mu.Unlock()
	if calls != 2 {
		t.Fatalf("context checks = %d, want admission and post-commit only", calls)
	}
}

func TestInteractionResultIsAtomicAgainstConcurrentDeletion(t *testing.T) {
	ctx := context.Background()
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	session := interaction.Session{
		ID:         interaction.DerivedID("session", "delete-race"),
		RecordedAt: time.Unix(1700000000, 0).UTC(),
		Operation:  interaction.OperationToolCall,
	}
	written := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	write := corpus.writeRecord
	corpus.interactionRecordWriter = func(
		row []byte, kind byte, value any,
	) error {
		if err := write(row, kind, value); err != nil {
			return err
		}
		if record, ok := value.(persistedInteraction); ok &&
			!record.Deleted {
			once.Do(func() {
				close(written)
				<-release
			})
		}
		return nil
	}
	type result struct {
		session interaction.Session
		err     error
	}
	recorded := make(chan result, 1)
	go func() {
		accepted, recordErr := corpus.RecordInteractionResult(ctx, session)
		recorded <- result{session: accepted, err: recordErr}
	}()
	<-written
	deleteStarted := make(chan struct{})
	deleted := make(chan error, 1)
	go func() {
		close(deleteStarted)
		_, deleteErr := corpus.DeleteInteraction(ctx, session.ID)
		deleted <- deleteErr
	}()
	<-deleteStarted
	time.Sleep(20 * time.Millisecond)
	close(release)
	recordResult := <-recorded
	if recordResult.err != nil || recordResult.session.ID != session.ID {
		t.Fatalf("atomic record result = %+v, %v",
			recordResult.session, recordResult.err)
	}
	if err := <-deleted; err != nil {
		t.Fatalf("concurrent deletion failed: %v", err)
	}
}

func TestInteractionResultDoesNotRereadAfterVisibilityTightening(t *testing.T) {
	ctx := context.Background()
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	source := Source{
		URI: "file:///visibility-race.txt", MediaType: MediaTypeText,
		Content: "stable source",
	}
	receipt, err := corpus.Ingest(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	view, err := corpus.Document(
		ctx, receipt.Document.ID, receipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:          interaction.DerivedID("session", "visibility-race"),
		RecordedAt:  time.Unix(1700000000, 0).UTC(),
		Operation:   interaction.OperationRetrieval,
		SeedNodeIDs: []shoal.ID{view.Root.Spans[0].ID},
	}
	written := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	write := corpus.writeRecord
	corpus.interactionRecordWriter = func(
		row []byte, kind byte, value any,
	) error {
		if err := write(row, kind, value); err != nil {
			return err
		}
		if record, ok := value.(persistedInteraction); ok &&
			!record.Deleted {
			once.Do(func() {
				close(written)
				<-release
			})
		}
		return nil
	}
	type result struct {
		session interaction.Session
		err     error
	}
	recorded := make(chan result, 1)
	go func() {
		accepted, recordErr := corpus.RecordInteractionResult(ctx, session)
		recorded <- result{session: accepted, err: recordErr}
	}()
	<-written
	ingestStarted := make(chan struct{})
	ingested := make(chan error, 1)
	go func() {
		close(ingestStarted)
		source.Metadata = shoal.Metadata{
			interaction.PropertyVisibility: "restricted",
		}
		_, ingestErr := corpus.Ingest(ctx, source)
		ingested <- ingestErr
	}()
	<-ingestStarted
	time.Sleep(20 * time.Millisecond)
	close(release)
	recordResult := <-recorded
	if recordResult.err != nil || recordResult.session.ID != session.ID {
		t.Fatalf("atomic record result = %+v, %v",
			recordResult.session, recordResult.err)
	}
	if err := <-ingested; err != nil {
		t.Fatalf("visibility tightening failed: %v", err)
	}
	if _, err := corpus.InteractionRecord(
		ctx, session.ID); err == nil {
		t.Fatal("tightened source left the interaction readable")
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

func TestInteractionRetryCancellationRemainsCommitted(t *testing.T) {
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	session := interaction.Session{
		ID:         interaction.DerivedID("session", "retry-cancellation"),
		RecordedAt: time.Unix(1700000000, 0).UTC(),
		Operation:  interaction.OperationToolCall,
	}
	first, err := corpus.RecordInteractionResult(context.Background(), session)
	if err != nil {
		t.Fatal(err)
	}
	// The exact record is already durable, so the retry observes it and only
	// then sees the cancellation.
	ctx := &stagedCancellationContext{
		Context: context.Background(), cancelAfter: 2,
	}
	retried, err := corpus.RecordInteractionResult(ctx, session)
	if err == nil {
		t.Fatal("cancellation after durable reconciliation was not reported")
	}
	if !IsCommittedInteraction(err) {
		t.Fatalf("durable retry cancellation reported as rollback: %v", err)
	}
	if !reflect.DeepEqual(retried, first) {
		t.Fatalf("reconciled session = %+v, want %+v", retried, first)
	}
}

// TestSnapshotObjectDigestSeparatesOpaqueBytes pins that the snapshot binding
// digest distinguishes graph objects whose opaque IDs, endpoints, or metadata
// differ only in bytes that a JSON encoding would fold onto U+FFFD.
func TestSnapshotObjectDigestSeparatesOpaqueBytes(t *testing.T) {
	pinnedEdge := graph.Edge{
		ID: "edge", From: "from", To: shoal.ID([]byte{0xFF}), Type: "cites",
	}
	mutatedEdge := pinnedEdge
	mutatedEdge.To = shoal.ID([]byte{0xFE})
	pinned, err := snapshotObjectDigest(pinnedEdge)
	if err != nil {
		t.Fatalf("digest pinned edge: %v", err)
	}
	mutated, err := snapshotObjectDigest(mutatedEdge)
	if err != nil {
		t.Fatalf("digest mutated edge: %v", err)
	}
	if pinned == mutated {
		t.Fatal("mutated edge endpoint kept its pinned snapshot digest")
	}
	pinnedNode := graph.Node{
		ID:         "node",
		Kind:       "entity",
		Properties: shoal.Metadata{"p": string([]byte{0xFF, 'A'})},
	}
	mutatedNode := graph.Node{
		ID:         "node",
		Kind:       "entity",
		Properties: shoal.Metadata{"p": string([]byte{0x80, 'A'})},
	}
	pinned, err = snapshotObjectDigest(pinnedNode)
	if err != nil {
		t.Fatalf("digest pinned node: %v", err)
	}
	mutated, err = snapshotObjectDigest(mutatedNode)
	if err != nil {
		t.Fatalf("digest mutated node: %v", err)
	}
	if pinned == mutated {
		t.Fatal("mutated node metadata kept its pinned snapshot digest")
	}
	if _, err := snapshotObjectDigest("unsupported"); err == nil {
		t.Fatal("expected an error for an unknown snapshot object")
	}
}
