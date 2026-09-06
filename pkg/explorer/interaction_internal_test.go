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

	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestInteractionResultCancellationAfterCommitIsMarked(t *testing.T) {
	corpus, session := internalInteractionFixture(t, "cancel")
	ctx, cancel := context.WithCancel(context.Background())
	write := corpus.interactionRecordWriter
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
	write := corpus.interactionRecordWriter
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
	write := corpus.interactionRecordWriter
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
