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

// Package authorizedtest builds a real Explorer corpus behind a real
// authorized client, with sessions and folds recorded under output-policy
// labels by one principal and read by another. The label-withholding tests of
// every surface that serves interaction records (#567, #568) run against the
// same fixture, so each of them exercises the one shared filter in the
// authorized client rather than a fake of it.
package authorizedtest

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// SecretLabels is the output-policy label set the labelled records carry.
// Neither label may appear anywhere in a response to a reader that did not
// record them.
var SecretLabels = []string{"secret", "project-x"}

// Principal subjects. Both hold identical grants, so the reader is authorized
// on every touched source and sees every record; only the recorder identity
// differs.
const (
	RecorderSubject = "label-recorder"
	ReaderSubject   = "label-reader"
)

var (
	Domain   = []byte("label-domain")
	SourceID = []byte("label-source")
	PolicyID = []byte("label-policy")
)

// Operations is the grant both principals hold.
var Operations = []auth.Operation{
	auth.OperationIngest, auth.OperationList, auth.OperationRead,
	auth.OperationConnect, auth.OperationNeighborhood,
	auth.OperationRetrieve, auth.OperationValidate,
	auth.OperationTeamOverviewRead,
}

type generationReader struct{}

func (generationReader) CurrentPolicyGeneration(
	ctx context.Context, domain []byte,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !bytes.Equal(domain, Domain) {
		return 0, nil
	}
	return 1, nil
}

// Fixture is a corpus, its authorized client, and one ingested source span
// every recorded session touches.
type Fixture struct {
	Corpus    *explorer.Explorer
	Client    *authorized.Client
	Authority *auth.Authority
	NodeID    shoal.ID
	Snapshot  explorer.Snapshot
	expires   time.Time
	sequence  int
}

// New opens a fresh corpus and ingests one document under the shared grant.
func New(t testing.TB) *Fixture {
	t.Helper()
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = corpus.Close() })
	authority := auth.NewAuthority()
	selector, err := authorized.NewStaticPolicySelector(SourceID, PolicyID)
	if err != nil {
		t.Fatal(err)
	}
	client, err := authorized.NewClient(authorized.Config{
		Base: corpus, Resolver: authority.Resolver(), PolicySelector: selector,
		InteractionWriter: corpus, InteractionReader: corpus,
		SnapshotValidator: corpus, FoldStore: corpus,
		PolicyStore:      authorized.NewMemoryPolicyStore(),
		GenerationReader: generationReader{}, Clock: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	f := &Fixture{
		Corpus: corpus, Client: client, Authority: authority,
		expires: time.Now().Add(time.Hour).Truncate(time.Second),
	}
	ctx := f.Context(t, RecorderSubject)
	receipt, err := client.Ingest(ctx, explorer.Source{
		URI:       "file:///label-withholding.md",
		MediaType: explorer.MediaTypeMarkdown,
		Content:   "# Labels\n\nEvidence every labelled session touches.\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := client.Document(ctx, receipt.Document.ID, receipt.Revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	f.NodeID = firstSpan(view.Root)
	if f.NodeID == "" {
		t.Fatal("ingested document has no span")
	}
	f.Snapshot, err = client.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.EnsureInteractionSink(ctx); err != nil {
		t.Fatal(err)
	}
	return f
}

func firstSpan(section explorer.SectionView) shoal.ID {
	if len(section.Spans) > 0 {
		return section.Spans[0].ID
	}
	for _, child := range section.Children {
		if id := firstSpan(child); id != "" {
			return id
		}
	}
	return ""
}

// Decision is subject's decision. Every call returns a new request ID but the
// same authorization projection, so the fingerprint is stable per subject.
func (f *Fixture) Decision(t testing.TB, subject string) auth.Decision {
	t.Helper()
	f.sequence++
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: shoal.ID(subject), Actor: shoal.ID(subject + "-actor"),
		AuthorizationDomain:   Domain,
		AllowedOperations:     append([]auth.Operation(nil), Operations...),
		PermittedSourceIDs:    [][]byte{SourceID},
		PermittedPolicyIDs:    [][]byte{PolicyID},
		PolicyGeneration:      1,
		AuthenticationExpires: f.expires,
		RequestID: shoal.ID(subject + "-request-" +
			strconv.Itoa(f.sequence)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return decision
}

// Context binds subject's decision.
func (f *Fixture) Context(t testing.TB, subject string) context.Context {
	t.Helper()
	ctx, err := f.Authority.Binder().Bind(
		context.Background(), f.Decision(t, subject))
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

// Record writes one session as subject that touches the fixture span, under
// the given output-policy labels (none for an unlabelled session). The trusted
// sink stamps its own RecordedAt.
func (f *Fixture) Record(
	t testing.TB, subject string, id shoal.ID, labels []string,
) interaction.Session {
	t.Helper()
	decision := f.Decision(t, subject)
	ctx, err := f.Authority.Binder().Bind(context.Background(), decision)
	if err != nil {
		t.Fatal(err)
	}
	if len(labels) > 0 {
		ctx, err = interaction.WithRequiredVisibility(ctx, labels)
		if err != nil {
			t.Fatal(err)
		}
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		t.Fatal(err)
	}
	session := interaction.Session{
		ID:                       id,
		RecordedAt:               f.Snapshot.AsOf.Add(time.Second).UTC(),
		Operation:                interaction.OperationRetrieval,
		SnapshotID:               shoal.ID(f.Snapshot.ID),
		SnapshotAsOf:             f.Snapshot.AsOf,
		AuthorizationFingerprint: shoal.ID(fingerprint.String()),
		AuthorizationExpiresAt:   decision.AuthenticationExpires(),
		SeedNodeIDs:              []shoal.ID{f.NodeID},
		// The chat path stamps the workspace output policy on the session
		// itself as well as on the context (webapi chat_service.go), so a
		// labelled record carries the expression in both places.
		RequiredVisibility: append([]string(nil), labels...),
	}
	persisted, err := f.Client.RecordInteractionResult(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	return persisted
}

// Fold folds sessions as subject.
func (f *Fixture) Fold(
	t testing.TB, subject string, sessions ...shoal.ID,
) explorer.FoldResult {
	t.Helper()
	result, err := f.Client.FoldInteractions(
		f.Context(t, subject), explorer.FoldRequest{SessionIDs: sessions})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// Leaks reports which of the secret labels body contains.
func Leaks(body []byte) []string {
	var leaked []string
	for _, label := range SecretLabels {
		if bytes.Contains(body, []byte(label)) {
			leaked = append(leaked, label)
		}
	}
	return leaked
}
