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

// Relabel narrowing (declassification) audit, #630.

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/devbackfill"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func (f *fixture) declassificationClient(
	t testing.TB,
	store authorized.PolicyStore,
	requireReason bool,
) *authorized.Client {
	t.Helper()
	return f.declassificationClientOver(t, f.base, store, requireReason)
}

func (f *fixture) declassificationClientOver(
	t testing.TB,
	base explorer.Client,
	store authorized.PolicyStore,
	requireReason bool,
) *authorized.Client {
	t.Helper()
	selector, err := authorized.NewStaticPolicySelector(f.sourceA, f.policyA)
	if err != nil {
		t.Fatal(err)
	}
	client, err := authorized.NewClient(authorized.Config{
		Base:                          base,
		VectorScorer:                  trustedVectorScorer(f.base),
		InteractionWriter:             trustedInteractionWriter(f.base),
		InteractionReader:             trustedInteractionReader(f.base),
		SnapshotValidator:             trustedSnapshotValidator(f.base),
		DerivedAssertionReader:        trustedDerivedAssertionReader(f.base),
		FoldStore:                     trustedFoldStore(f.base),
		Resolver:                      f.authority.Resolver(),
		PolicySelector:                selector,
		PolicyStore:                   store,
		GenerationReader:              f.reader,
		Clock:                         f.clock.Now,
		RequireDeclassificationReason: requireReason,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func declassifications(
	t *testing.T,
	store authorized.PolicyStore,
) []authorized.DeclassificationRecord {
	t.Helper()
	records, err := store.Declassifications(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func withReason(t *testing.T, ctx context.Context, reason string) context.Context {
	t.Helper()
	ctx, err := authorized.WithDeclassificationReason(ctx, reason)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

// TestDeclassificationAuditsLabelRemoval pins the audit an ingest that drops
// a label leaves: one intent and one commit record of one attempt, carrying
// the principal, request, document, revisions and both rules.
func TestDeclassificationAuditsLabelRemoval(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		f := newFixture(t)
		client := f.declassificationClient(t, store, false)
		const uri = "file:///declassify/removal.txt"
		first, err := client.Ingest(f.labelIngester(t, "owner", "x", "secret"),
			labelledSource(uri, "labelled x and secret", "x&secret"))
		if err != nil {
			t.Fatal(err)
		}
		if records := declassifications(t, store); len(records) != 0 {
			t.Fatalf("a first ingest was audited as a declassification: %+v", records)
		}
		ctx := withReason(t, f.labelIngester(t, "declassifier", "x", "secret"),
			"ticket:SEC-1")
		second, err := client.Ingest(ctx,
			labelledSource(uri, "now only x", "x"))
		if err != nil {
			t.Fatal(err)
		}
		records := declassifications(t, store)
		if len(records) != 2 {
			t.Fatalf("declassification records = %d, want intent and commit: %+v",
				len(records), records)
		}
		intent, commit := records[0], records[1]
		if intent.Phase != authorized.DeclassificationIntent ||
			commit.Phase != authorized.DeclassificationPreCommit {
			t.Fatalf("phases = %q, %q", intent.Phase, commit.Phase)
		}
		if intent.AttemptID == "" || intent.AttemptID != commit.AttemptID {
			t.Fatalf("attempt IDs = %q, %q, want one shared attempt",
				intent.AttemptID, commit.AttemptID)
		}
		for _, record := range records {
			if record.Subject != "declassifier" ||
				record.Actor != "declassifier-actor" ||
				record.RequestID != "declassifier-request" ||
				record.SourceURI != uri ||
				record.ReasonCode != "ticket:SEC-1" ||
				!record.OccurredAt.Equal(f.clock.Now()) {
				t.Fatalf("%s record attribution = %+v", record.Phase, record)
			}
			if got := record.RemovedLabels(); !reflect.DeepEqual(got, []string{"secret"}) {
				t.Fatalf("%s removed labels = %v, want [secret]", record.Phase, got)
			}
			if len(record.PriorRules) != 1 ||
				record.PriorRules[0].String() == record.Rule.String() {
				t.Fatalf("%s rules = %v -> %v", record.Phase, record.PriorRules, record.Rule)
			}
		}
		if commit.DocumentID != second.Document.ID ||
			commit.RevisionID != second.Revision.ID ||
			commit.PriorRevisionID != first.Revision.ID {
			t.Fatalf("commit identities = %+v, want document %s revision %s prior %s",
				commit, second.Document.ID, second.Revision.ID, first.Revision.ID)
		}
		decision, err := f.authority.Resolver().Resolve(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fingerprint, err := auth.AuthorizationFingerprint(decision)
		if err != nil {
			t.Fatal(err)
		}
		if commit.AuthorizationFingerprint != fingerprint {
			t.Fatal("commit record does not carry the principal's fingerprint")
		}
	})
}

// TestDeclassificationAuditsLabelSwap pins that replacing one label with
// another is a narrowing: holders of the new label who lack the old one gain
// access.
func TestDeclassificationAuditsLabelSwap(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		f := newFixture(t)
		client := f.declassificationClient(t, store, false)
		const uri = "file:///declassify/swap.txt"
		if _, err := client.Ingest(f.labelIngester(t, "owner", "x"),
			labelledSource(uri, "labelled x", "x")); err != nil {
			t.Fatal(err)
		}
		if _, err := client.Ingest(f.labelIngester(t, "both", "x", "Secret"),
			labelledSource(uri, "labelled Secret", "Secret")); err != nil {
			t.Fatal(err)
		}
		records := declassifications(t, store)
		if len(records) != 2 {
			t.Fatalf("swap records = %d, want 2", len(records))
		}
		if got := records[1].RemovedLabels(); !reflect.DeepEqual(got, []string{"x"}) {
			t.Fatalf("removed = %v, want [x]", got)
		}
		// Case is never folded: secret is a different label from Secret,
		// so this is a narrowing too.
		if _, err := client.Ingest(f.labelIngester(t, "case", "Secret", "secret"),
			labelledSource(uri, "labelled secret", "secret")); err != nil {
			t.Fatal(err)
		}
		records = declassifications(t, store)
		if len(records) != 4 {
			t.Fatalf("case swap records = %d, want 4", len(records))
		}
		if got := records[3].RemovedLabels(); !reflect.DeepEqual(got, []string{"Secret"}) {
			t.Fatalf("removed = %v, want [Secret]", got)
		}
	})
}

// TestDeclassificationFullyUnlabels pins that dropping every label, the
// plainest declassification, is audited.
func TestDeclassificationFullyUnlabels(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		f := newFixture(t)
		client := f.declassificationClient(t, store, false)
		const uri = "file:///declassify/unlabel.txt"
		if _, err := client.Ingest(f.labelIngester(t, "owner", "a", "b"),
			labelledSource(uri, "labelled a b", "a&b")); err != nil {
			t.Fatal(err)
		}
		second, err := client.Ingest(f.labelIngester(t, "owner", "a", "b"),
			labelledSource(uri, "public now", ""))
		if err != nil {
			t.Fatal(err)
		}
		records := declassifications(t, store)
		if len(records) != 2 {
			t.Fatalf("records = %d, want 2", len(records))
		}
		if got := records[1].RemovedLabels(); !reflect.DeepEqual(got, []string{"a", "b"}) {
			t.Fatalf("removed = %v, want [a b]", got)
		}
		if _, err := client.Document(f.labelIngester(t, "public-reader"),
			second.Document.ID, ""); err != nil {
			t.Fatalf("declassified document unreadable to a source holder: %v", err)
		}
	})
}

// TestDeclassificationIgnoresOrdinaryAndWideningIngests pins that a first
// ingest, a content change under the same labels, an unchanged re-ingest, and
// adding labels are never audited.
func TestDeclassificationIgnoresOrdinaryAndWideningIngests(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		f := newFixture(t)
		client := f.declassificationClient(t, store, true)
		const uri = "file:///declassify/ordinary.txt"
		owner := f.labelIngester(t, "owner", "x", "y")
		for _, step := range []explorer.Source{
			labelledSource(uri, "unlabelled", ""),
			labelledSource(uri, "unlabelled, edited", ""),
			labelledSource(uri, "unlabelled, edited", ""),
			labelledSource(uri, "x", "x"),
			labelledSource(uri, "x, edited", "x"),
			labelledSource(uri, "x and y", "y&x"),
			labelledSource(uri, "x and y, reordered", "x&y&x"),
		} {
			// The reason policy is on and no reason is given, so any of
			// these being treated as a narrowing would also fail it.
			if _, err := client.Ingest(owner, step); err != nil {
				t.Fatalf("ingest %q: %v", step.Content, err)
			}
		}
		if records := declassifications(t, store); len(records) != 0 {
			t.Fatalf("ordinary or widening ingests were audited: %+v", records)
		}
		// Another document, unrelated to this one, is not a narrowing of it.
		if _, err := client.Ingest(owner,
			labelledSource("file:///declassify/other.txt", "other", "")); err != nil {
			t.Fatal(err)
		}
		if records := declassifications(t, store); len(records) != 0 {
			t.Fatalf("an unrelated first ingest was audited: %+v", records)
		}
	})
}

// TestDeclassificationReasonPolicy pins the optional operator policy: with
// it on, a narrowing without a reason is refused before anything is written,
// and one with a reason proceeds and records it.
func TestDeclassificationReasonPolicy(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		f := newFixture(t)
		client := f.declassificationClient(t, store, true)
		const uri = "file:///declassify/reason.txt"
		first, err := client.Ingest(f.labelIngester(t, "owner", "secret"),
			labelledSource(uri, "secret", "secret"))
		if err != nil {
			t.Fatal(err)
		}
		before := baseDocumentCount(t, f)
		holder := f.labelIngester(t, "holder", "secret")
		if _, err := client.Ingest(holder, labelledSource(uri, "public", "")); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
			t.Fatalf("narrowing without a reason = %v, want invalid argument", err)
		}
		if records := declassifications(t, store); len(records) != 0 {
			t.Fatalf("a refused narrowing was recorded: %+v", records)
		}
		summaries, err := client.Documents(f.labelIngester(t, "lister", "secret"))
		if err != nil {
			t.Fatal(err)
		}
		if len(summaries) != 1 || summaries[0].Revision.ID != first.Revision.ID ||
			baseDocumentCount(t, f) != before {
			t.Fatalf("a refused narrowing changed the document: %+v", summaries)
		}
		if _, err := client.Document(f.labelIngester(t, "reader"), first.Document.ID, ""); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			t.Fatalf("refused narrowing exposed the document: %v", err)
		}
		// A principal without the label is refused as before, and learns
		// nothing about the reason policy.
		if _, err := client.Ingest(f.labelIngester(t, "outsider"),
			labelledSource(uri, "public", "")); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			t.Fatalf("outsider narrowing = %v, want not found", err)
		}
		if _, err := client.Ingest(withReason(t, holder, "legal-hold-release"),
			labelledSource(uri, "public", "")); err != nil {
			t.Fatalf("narrowing with a reason: %v", err)
		}
		records := declassifications(t, store)
		if len(records) != 2 || records[0].ReasonCode != "legal-hold-release" ||
			records[1].ReasonCode != "legal-hold-release" {
			t.Fatalf("records = %+v, want the reason on both", records)
		}
	})
}

func TestDeclassificationReasonValidation(t *testing.T) {
	for _, reason := range []string{"", "has space", "semi;colon", string(make([]byte, authorized.MaxDeclassificationReasonBytes+1))} {
		if _, err := authorized.WithDeclassificationReason(context.Background(), reason); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
			t.Fatalf("reason %q = %v, want invalid argument", reason, err)
		}
	}
}

// failingDeclassificationStore fails RecordDeclassification on the listed
// call numbers (1-based) and delegates everything else.
type failingDeclassificationStore struct {
	authorized.PolicyStore
	mu    sync.Mutex
	calls int
	fail  map[int]bool
}

func (s *failingDeclassificationStore) RecordDeclassification(
	ctx context.Context,
	record authorized.DeclassificationRecord,
) error {
	s.mu.Lock()
	s.calls++
	fail := s.fail[s.calls]
	s.mu.Unlock()
	if fail {
		return shoal.NewError(shoal.ErrorUnavailable, "audit sink down")
	}
	return s.PolicyStore.RecordDeclassification(ctx, record)
}

// TestDeclassificationIntentFailureBlocksTheRelabel pins audit-before-commit
// at the source claim: if the intent cannot be recorded, nothing is written.
func TestDeclassificationIntentFailureBlocksTheRelabel(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, inner authorized.PolicyStore) {
		f := newFixture(t)
		store := &failingDeclassificationStore{
			PolicyStore: inner, fail: map[int]bool{1: true}}
		client := f.declassificationClient(t, store, false)
		const uri = "file:///declassify/intent-failure.txt"
		first, err := client.Ingest(f.labelIngester(t, "owner", "secret"),
			labelledSource(uri, "secret", "secret"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Ingest(f.labelIngester(t, "owner", "secret"),
			labelledSource(uri, "public", "")); err == nil {
			t.Fatal("a narrowing whose audit failed succeeded")
		}
		// A narrowing re-ingest adds a revision, not a document, so the
		// base's current revision is what must be unchanged.
		summaries, err := f.base.Documents(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(summaries) != 1 || summaries[0].Revision.ID != first.Revision.ID {
			t.Fatalf("a narrowing whose intent audit failed reached the base: %+v", summaries)
		}
		claim, ok, err := inner.SourceClaim(context.Background(), uri)
		if err != nil || !ok || claim.Pending {
			t.Fatalf("claim after a failed intent audit = %+v, %v, %v", claim, ok, err)
		}
		if _, err := client.Document(f.labelIngester(t, "reader"), first.Document.ID, ""); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			t.Fatalf("failed narrowing exposed the document: %v", err)
		}
		if records := declassifications(t, inner); len(records) != 0 {
			t.Fatalf("records = %+v", records)
		}
	})
}

// TestDeclassificationCommitFailureBlocksTheRegistration pins
// audit-before-commit at the visibility commit, and that the commit check is
// independent of the claim: after a failed attempt has already swapped the
// claim to the narrow rule, a retry is still audited against the current
// registration before it is let in.
func TestDeclassificationCommitFailureBlocksTheRegistration(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, inner authorized.PolicyStore) {
		f := newFixture(t)
		store := &failingDeclassificationStore{
			PolicyStore: inner, fail: map[int]bool{2: true}}
		client := f.declassificationClient(t, store, false)
		const uri = "file:///declassify/commit-failure.txt"
		first, err := client.Ingest(f.labelIngester(t, "owner", "secret"),
			labelledSource(uri, "secret", "secret"))
		if err != nil {
			t.Fatal(err)
		}
		public := labelledSource(uri, "public", "")
		if _, err := client.Ingest(f.labelIngester(t, "owner", "secret"), public); err == nil {
			t.Fatal("a narrowing whose commit audit failed succeeded")
		}
		current, ok, err := inner.CurrentRevision(context.Background(), first.Document.ID)
		if err != nil || !ok || current.RevisionID != first.Revision.ID {
			t.Fatalf("current registration after a failed commit audit = %+v, %v, %v",
				current, ok, err)
		}
		reader := f.labelIngester(t, "reader")
		records := declassifications(t, inner)
		if len(records) != 1 || records[0].Phase != authorized.DeclassificationIntent {
			t.Fatalf("records after a failed commit audit = %+v", records)
		}

		// The claim now holds the narrow rule, so the claim-time check sees
		// no narrowing on a retry. The commit-time check still does, against
		// the current registration.
		second, err := client.Ingest(f.labelIngester(t, "retrier", "secret"), public)
		if err != nil {
			t.Fatalf("retry: %v", err)
		}
		records = declassifications(t, inner)
		if len(records) != 2 {
			t.Fatalf("records after the retry = %+v", records)
		}
		retry := records[1]
		if retry.Phase != authorized.DeclassificationPreCommit ||
			retry.Subject != "retrier" ||
			retry.AttemptID == records[0].AttemptID ||
			retry.DocumentID != second.Document.ID ||
			retry.RevisionID != second.Revision.ID ||
			retry.PriorRevisionID != first.Revision.ID ||
			!reflect.DeepEqual(retry.RemovedLabels(), []string{"secret"}) {
			t.Fatalf("retry record = %+v", retry)
		}
		if _, err := client.Document(reader, second.Document.ID, ""); err != nil {
			t.Fatalf("committed narrowing: %v", err)
		}
	})
}

// TestDeclassificationAuditsLegacyUnclaimedDocument covers a document
// registered without a source claim (the development backfill): its current
// registration is the replaced rule.
func TestDeclassificationAuditsLegacyUnclaimedDocument(t *testing.T) {
	f := newFixture(t)
	const uri = "file:///declassify/legacy.txt"
	if _, err := f.base.Ingest(context.Background(),
		labelledSource(uri, "labelled before the catalog", "secret")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.clientA.BackfillExistingDocumentsForDevelopment(
		f.labelIngester(t, "operator", "secret"), devbackfill.NewCapability()); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := f.store.SourceClaim(context.Background(), uri); err != nil || ok {
		t.Fatalf("backfill created a claim (%v): the legacy path is not exercised", err)
	}
	if _, err := f.clientA.Ingest(f.labelIngester(t, "owner", "secret"),
		labelledSource(uri, "public", "")); err != nil {
		t.Fatal(err)
	}
	records := declassifications(t, f.store)
	if len(records) != 2 || records[0].Phase != authorized.DeclassificationIntent ||
		records[0].DocumentID == "" || records[0].PriorRevisionID == "" ||
		!reflect.DeepEqual(records[0].RemovedLabels(), []string{"secret"}) {
		t.Fatalf("legacy records = %+v", records)
	}
}

// TestDeclassificationRecordsSurviveReopen pins durability.
func TestDeclassificationRecordsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := authorized.OpenDurablePolicyStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	f := newFixture(t)
	client := f.declassificationClient(t, store, false)
	const uri = "file:///declassify/durable.txt"
	if _, err := client.Ingest(f.labelIngester(t, "owner", "secret"),
		labelledSource(uri, "secret", "secret")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Ingest(withReason(t, f.labelIngester(t, "owner", "secret"), "r"),
		labelledSource(uri, "public", "")); err != nil {
		t.Fatal(err)
	}
	want := declassifications(t, store)
	if len(want) != 2 {
		t.Fatalf("records = %d", len(want))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := authorized.OpenDurablePolicyStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got := declassifications(t, reopened)
	if len(got) != len(want) {
		t.Fatalf("reopened records = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index].AttemptID != want[index].AttemptID ||
			got[index].Phase != want[index].Phase ||
			got[index].RevisionID != want[index].RevisionID ||
			got[index].ReasonCode != want[index].ReasonCode ||
			got[index].AuthorizationFingerprint != want[index].AuthorizationFingerprint ||
			!got[index].OccurredAt.Equal(want[index].OccurredAt) ||
			got[index].Rule.String() != want[index].Rule.String() ||
			!reflect.DeepEqual(got[index].RemovedLabels(), want[index].RemovedLabels()) {
			t.Fatalf("record %d after reopen = %+v, want %+v", index, got[index], want[index])
		}
	}
}

// TestDeclassificationStoreRefusesNonNarrowingRecords pins that a store
// cannot be handed a record that describes no narrowing.
func TestDeclassificationStoreRefusesNonNarrowingRecords(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		policy, err := auth.NewPolicy(auth.PolicyConfig{AuthorizationDomain: []byte("domain-one"), SourceID: []byte("source-a"), GrantPolicyID: []byte("policy-a"), Epoch: 1})
		if err != nil {
			t.Fatal(err)
		}
		rule, err := authorized.NewAccessRule(policy)
		if err != nil {
			t.Fatal(err)
		}
		labelled, err := authorized.LabelRule(policy, []string{"secret"})
		if err != nil {
			t.Fatal(err)
		}
		record := authorized.DeclassificationRecord{
			AttemptID: "a", Phase: authorized.DeclassificationIntent,
			OccurredAt: time.Date(2026, time.August, 26, 18, 0, 0, 0, time.UTC), SourceURI: "file:///x",
			PriorRules: []authorized.AccessRule{rule}, Rule: labelled,
		}
		if err := store.RecordDeclassification(context.Background(), record); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
			t.Fatalf("widening record = %v, want invalid argument", err)
		}
		record.PriorRules, record.Rule = []authorized.AccessRule{labelled}, rule
		if err := store.RecordDeclassification(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	})
}

// TestDeclassificationAuditsPendingClaimRetry covers an interrupted
// narrowing: the base committed but reported an indeterminate outcome, so the
// claim is pending with the narrow Rule and the replaced PreviousRule. A
// retry must be recognised as a narrowing at claim time from PreviousRule, so
// the reason policy refuses it before the base is touched again.
func TestDeclassificationAuditsPendingClaimRetry(t *testing.T) {
	withPolicyStores(t, func(t *testing.T, store authorized.PolicyStore) {
		f := newFixture(t)
		const uri = "file:///declassify/pending.txt"
		client := f.declassificationClient(t, store, true)
		if _, err := client.Ingest(f.labelIngester(t, "owner", "secret"),
			labelledSource(uri, "secret", "secret")); err != nil {
			t.Fatal(err)
		}
		indeterminate := &ingestOverrideClient{
			Client: f.base,
			ingest: func(ctx context.Context, source explorer.Source) (explorer.IngestResult, error) {
				if _, err := f.base.Ingest(ctx, source); err != nil {
					return explorer.IngestResult{}, err
				}
				return explorer.IngestResult{}, explorer.MarkIndeterminateCommit(
					shoal.NewError(shoal.ErrorUnavailable, "indeterminate"))
			},
		}
		flaky := f.declassificationClientOver(t, indeterminate, store, true)
		public := labelledSource(uri, "public", "")
		holder := f.labelIngester(t, "holder", "secret")
		if _, err := flaky.Ingest(withReason(t, holder, "first"), public); !explorer.IsIndeterminateCommit(err) {
			t.Fatalf("interrupted narrowing = %v", err)
		}
		claim, ok, err := store.SourceClaim(context.Background(), uri)
		if err != nil || !ok || !claim.Pending || claim.PreviousRule == nil {
			t.Fatalf("claim = %+v %v %v, want pending with a previous rule", claim, ok, err)
		}
		if records := declassifications(t, store); len(records) != 1 ||
			records[0].Phase != authorized.DeclassificationIntent {
			t.Fatalf("records after the interrupted narrowing = %+v", records)
		}
		// Without a reason the retry is refused at claim time.
		if _, err := client.Ingest(holder, public); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
			t.Fatalf("retry without a reason = %v, want invalid argument", err)
		}
		if records := declassifications(t, store); len(records) != 1 {
			t.Fatalf("a refused retry was recorded: %+v", records)
		}
		if _, err := client.Ingest(withReason(t, holder, "retry"), public); err != nil {
			t.Fatalf("retry with a reason: %v", err)
		}
		records := declassifications(t, store)
		if len(records) != 3 || records[1].Phase != authorized.DeclassificationIntent ||
			records[1].ReasonCode != "retry" ||
			!reflect.DeepEqual(records[1].RemovedLabels(), []string{"secret"}) ||
			records[2].Phase != authorized.DeclassificationPreCommit ||
			records[2].AttemptID != records[1].AttemptID {
			t.Fatalf("records after the retry = %+v", records)
		}
	})
}

// TestDeclassificationReconciliationIsRecordedNotRefused covers the state a
// failed commit leaves behind: the claim holds the narrow rule while the
// registration keeps the labelled one. The claim is the mutation authority,
// so a claim holder may complete it (as before #630), even one outside the
// labelled rule. With the reason policy on, that completion must not be
// refused, since a refusal would tell such a principal the registration
// carries labels; the narrowing was decided, reason-checked and recorded at
// the earlier intent. The completion is recorded, attributed to whoever
// made it current.
func TestDeclassificationReconciliationIsRecordedNotRefused(t *testing.T) {
	f := newFixture(t)
	inner := authorized.NewMemoryPolicyStore()
	store := &failingDeclassificationStore{
		PolicyStore: inner, fail: map[int]bool{2: true}}
	client := f.declassificationClient(t, store, true)
	const uri = "file:///declassify/reconcile.txt"
	first, err := client.Ingest(f.labelIngester(t, "owner", "secret"),
		labelledSource(uri, "secret", "secret"))
	if err != nil {
		t.Fatal(err)
	}
	public := labelledSource(uri, "public", "")
	if _, err := client.Ingest(withReason(t, f.labelIngester(t, "owner", "secret"), "r1"), public); err == nil {
		t.Fatal("commit audit failure was ignored")
	}
	current, ok, err := inner.CurrentRevision(context.Background(), first.Document.ID)
	if err != nil || !ok || current.RevisionID != first.Revision.ID {
		t.Fatalf("failed commit audit registered the narrowing: %+v %v %v", current, ok, err)
	}
	reconciled, err := client.Ingest(f.labelIngester(t, "nobody"), public)
	if err != nil {
		t.Fatalf("claim holder reconciliation without a reason = %v", err)
	}
	records := declassifications(t, inner)
	if len(records) != 2 ||
		records[0].Phase != authorized.DeclassificationIntent ||
		records[0].Subject != "owner" || records[0].ReasonCode != "r1" ||
		records[1].Phase != authorized.DeclassificationPreCommit ||
		records[1].Subject != "nobody" || records[1].ReasonCode != "" ||
		records[1].AttemptID == records[0].AttemptID ||
		records[1].RevisionID != reconciled.Revision.ID ||
		records[1].PriorRevisionID != first.Revision.ID {
		t.Fatalf("records = %+v", records)
	}
}

// TestDeclassificationRecordsAreAppendOnly pins that a second record for one
// (attempt, phase) is refused rather than shadowing the first on disk.
func TestDeclassificationRecordsAreAppendOnly(t *testing.T) {
	dir := t.TempDir()
	durableStore, err := authorized.OpenDurablePolicyStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	durableDirs := map[*authorized.DurablePolicyStore]string{durableStore: dir}
	for _, store := range []authorized.PolicyStore{
		authorized.NewMemoryPolicyStore(), durableStore,
	} {
		policy, err := auth.NewPolicy(auth.PolicyConfig{AuthorizationDomain: []byte("domain-one"), SourceID: []byte("source-a"), GrantPolicyID: []byte("policy-a"), Epoch: 1})
		if err != nil {
			t.Fatal(err)
		}
		rule, err := authorized.NewAccessRule(policy)
		if err != nil {
			t.Fatal(err)
		}
		labelled, err := authorized.LabelRule(policy, []string{"secret"})
		if err != nil {
			t.Fatal(err)
		}
		record := authorized.DeclassificationRecord{
			AttemptID: "a", Phase: authorized.DeclassificationIntent,
			OccurredAt: time.Date(2026, time.August, 26, 18, 0, 0, 0, time.UTC),
			SourceURI:  "file:///x", ReasonCode: "first",
			PriorRules: []authorized.AccessRule{labelled}, Rule: rule,
		}
		if err := store.RecordDeclassification(context.Background(), record); err != nil {
			t.Fatal(err)
		}
		record.ReasonCode = "overwrite"
		if err := store.RecordDeclassification(context.Background(), record); !shoal.IsErrorCode(err, shoal.ErrorConflict) {
			t.Fatalf("duplicate record = %v, want conflict", err)
		}
		records := declassifications(t, store)
		if len(records) != 1 || records[0].ReasonCode != "first" {
			t.Fatalf("records = %+v", records)
		}
		durable, ok := store.(*authorized.DurablePolicyStore)
		if !ok {
			continue
		}
		// The refused duplicate must not have reached the disk either,
		// where it would shadow the original on the next open.
		dir := durableDirs[durable]
		if err := durable.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := authorized.OpenDurablePolicyStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = reopened.Close() })
		records = declassifications(t, reopened)
		if len(records) != 1 || records[0].ReasonCode != "first" {
			t.Fatalf("records after reopen = %+v", records)
		}
	}
}

// TestDeclassificationDivergedCatalogIsAnIntent covers a URI with no source
// claim and no base document whose catalog still holds a current labelled
// registration (the catalog and base diverged, for example a base restored
// from backup). The claim-time check has nothing to compare with, so the
// pre-commit check is the narrowing's first check: it must authorize the
// principal under the registration it replaces and apply the reason policy,
// not treat it as a reconciliation.
func TestDeclassificationDivergedCatalogIsAnIntent(t *testing.T) {
	f := newFixture(t)
	const uri = "file:///declassify/diverged.txt"
	labelled, err := f.base.Ingest(context.Background(),
		labelledSource(uri, "labelled before the catalog", "secret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.clientA.BackfillExistingDocumentsForDevelopment(
		f.labelIngester(t, "operator", "secret"), devbackfill.NewCapability()); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := f.store.SourceClaim(context.Background(), uri); err != nil || ok {
		t.Fatalf("backfill created a claim (%v)", err)
	}
	// A fresh base that does not know the document: the catalog diverges.
	restored, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	client := f.declassificationClientOver(t, restored, f.store, true)
	public := labelledSource(uri, "public", "")
	registered := func() shoal.ID {
		t.Helper()
		current, ok, err := f.store.CurrentRevision(context.Background(), labelled.Document.ID)
		if err != nil || !ok {
			t.Fatalf("current registration: %v %v", ok, err)
		}
		return current.RevisionID
	}

	if _, err := client.Ingest(f.labelIngester(t, "outsider"), public); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
		t.Fatalf("outsider replacing a diverged labelled registration = %v, want not found", err)
	}
	// The outsider's refused attempt still left a committed claim under the
	// narrow rule (the claim commits on the base write, #665). That claim
	// passed no reason check, so it must not turn the holder's narrowing
	// into a reason-free reconciliation.
	if claim, ok, err := f.store.SourceClaim(context.Background(), uri); err != nil || !ok || claim.Pending {
		t.Fatalf("claim after the outsider = %+v %v %v", claim, ok, err)
	}
	if _, err := client.Ingest(f.labelIngester(t, "holder", "secret"), public); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("holder without a reason = %v, want invalid argument", err)
	}
	if got := registered(); got != labelled.Revision.ID {
		t.Fatalf("a refused narrowing replaced the registration: %s", got)
	}
	if records := declassifications(t, f.store); len(records) != 0 {
		t.Fatalf("refused narrowings were recorded: %+v", records)
	}
	result, err := client.Ingest(withReason(t, f.labelIngester(t, "holder", "secret"), "restore"), public)
	if err != nil {
		t.Fatalf("holder with a reason: %v", err)
	}
	records := declassifications(t, f.store)
	if len(records) != 1 || records[0].Phase != authorized.DeclassificationPreCommit ||
		records[0].Subject != "holder" || records[0].ReasonCode != "restore" ||
		records[0].RevisionID != result.Revision.ID ||
		records[0].PriorRevisionID != labelled.Revision.ID ||
		!reflect.DeepEqual(records[0].RemovedLabels(), []string{"secret"}) {
		t.Fatalf("records = %+v", records)
	}
}

// TestDeclassificationReconciliationNeedsAMatchingIntent pins that only an
// intent for the same URI, the same new rule and the current registration's
// rule makes a pre-commit narrowing a reason-free reconciliation. An intent
// that differs in either rule must not lift the reason policy in the
// diverged-catalog state.
func TestDeclassificationReconciliationNeedsAMatchingIntent(t *testing.T) {
	policy, err := auth.NewPolicy(auth.PolicyConfig{
		AuthorizationDomain: []byte("domain-one"), SourceID: []byte("source-a"),
		GrantPolicyID: []byte("policy-a"), Epoch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	rule := func(labels ...string) authorized.AccessRule {
		t.Helper()
		built, err := authorized.LabelRule(policy, labels)
		if err != nil {
			t.Fatal(err)
		}
		return built
	}
	const uri = "file:///declassify/unmatched-intent.txt"
	for _, seed := range []struct {
		name        string
		prior, next authorized.AccessRule
		reconciles  bool
	}{
		{"other prior rule", rule("x"), rule(), false},
		{"other new rule", rule("secret", "y"), rule("y"), false},
		{"other new rule, same prior", rule("secret"), rule("y"), false},
		{"matching", rule("secret"), rule(), true},
	} {
		t.Run(seed.name, func(t *testing.T) {
			f := newFixture(t)
			if err := f.store.RecordDeclassification(context.Background(),
				authorized.DeclassificationRecord{
					AttemptID: "seeded", Phase: authorized.DeclassificationIntent,
					OccurredAt: f.clock.Now(), SourceURI: uri,
					PriorRules: []authorized.AccessRule{seed.prior}, Rule: seed.next,
				}); err != nil {
				t.Fatal(err)
			}
			if _, err := f.base.Ingest(context.Background(),
				labelledSource(uri, "labelled before the catalog", "secret")); err != nil {
				t.Fatal(err)
			}
			if _, err := f.clientA.BackfillExistingDocumentsForDevelopment(
				f.labelIngester(t, "operator", "secret"), devbackfill.NewCapability()); err != nil {
				t.Fatal(err)
			}
			restored, err := explorer.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restored.Close() })
			client := f.declassificationClientOver(t, restored, f.store, true)
			_, err = client.Ingest(f.labelIngester(t, "holder", "secret"),
				labelledSource(uri, "public", ""))
			if seed.reconciles {
				if err != nil {
					t.Fatalf("narrowing covered by a matching intent = %v, want reconciliation", err)
				}
				return
			}
			if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
				t.Fatalf("narrowing with no matching intent and no reason = %v, want invalid argument", err)
			}
		})
	}
}
