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

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized/authorizedtest"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	labelledSession shoal.ID = "interaction.session_labelled"
	plainSession    shoal.ID = "interaction.session_plain"
	readerSession   shoal.ID = "interaction.session_reader_own"
)

// asPlainRecord renames a labelled record to the unlabelled one it is
// compared with. The two sessions are recorded with identical inputs and
// differ only in ID, labels, and the time the trusted sink stamped, so after
// renaming and restamping, any remaining difference is a label or a shape
// marker left by withholding.
func asPlainRecord(
	record, plain explorer.InteractionRecord,
) explorer.InteractionRecord {
	record.Summary.SessionID = plain.Summary.SessionID
	record.Session.ID = plain.Session.ID
	record.Summary.RecordedAt = plain.Summary.RecordedAt
	record.Session.RecordedAt = plain.Session.RecordedAt
	return record
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

// assertSameAsUnlabelled is the #566 check: a withheld value must be
// deep-equal to the value of a record that never had labels, and must encode
// to the same bytes, because nil-versus-empty differences survive one of the
// two comparisons but not the other.
func assertSameAsUnlabelled(t *testing.T, name string, withheld, plain any) {
	t.Helper()
	if !reflect.DeepEqual(withheld, plain) {
		t.Fatalf("%s: withheld value differs from an unlabelled one:\nwithheld=%#v\nplain=   %#v",
			name, withheld, plain)
	}
	withheldBytes, plainBytes := mustJSON(t, withheld), mustJSON(t, plain)
	if string(withheldBytes) != string(plainBytes) {
		t.Fatalf("%s: withheld bytes differ from an unlabelled record's:\nwithheld=%s\nplain=   %s",
			name, withheldBytes, plainBytes)
	}
	if leaked := authorizedtest.Leaks(withheldBytes); len(leaked) != 0 {
		t.Fatalf("%s leaked %v: %s", name, leaked, withheldBytes)
	}
}

func TestInteractionLabelsAreWithheldFromReadersWhoDidNotRecordThem(t *testing.T) {
	f := authorizedtest.New(t)
	f.Record(t, authorizedtest.RecorderSubject, labelledSession,
		authorizedtest.SecretLabels)
	f.Record(t, authorizedtest.RecorderSubject, plainSession, nil)
	reader := f.Context(t, authorizedtest.ReaderSubject)
	recorder := f.Context(t, authorizedtest.RecorderSubject)
	// Every withheld value is compared with the unlabelled session as its own
	// recorder reads it, which passes through no withholding and so is
	// exactly the shape a never-labelled record has.

	// Point read.
	labelled, err := f.Client.InteractionRecord(reader, labelledSession)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := f.Client.InteractionRecord(recorder, plainSession)
	if err != nil {
		t.Fatal(err)
	}
	assertSameAsUnlabelled(t, "point record", asPlainRecord(labelled, plain), plain)

	// Typed point read.
	session, err := f.Client.Interaction(reader, labelledSession)
	if err != nil {
		t.Fatal(err)
	}
	plainTyped, err := f.Client.Interaction(recorder, plainSession)
	if err != nil {
		t.Fatal(err)
	}
	session.ID = plainSession
	session.RecordedAt = plainTyped.RecordedAt
	assertSameAsUnlabelled(t, "typed session", session, plainTyped)

	// Bulk and paged reads.
	records, err := f.Client.InteractionRecords(reader)
	if err != nil {
		t.Fatal(err)
	}
	recorderRecords, err := f.Client.InteractionRecords(recorder)
	if err != nil {
		t.Fatal(err)
	}
	byID := map[shoal.ID]explorer.InteractionRecord{}
	for _, record := range records {
		byID[record.Summary.SessionID] = record
	}
	if len(byID) != 2 {
		t.Fatalf("reader lost records: %+v", records)
	}
	var plainListed explorer.InteractionRecord
	for _, record := range recorderRecords {
		if record.Summary.SessionID == plainSession {
			plainListed = record
		}
	}
	assertSameAsUnlabelled(t, "listed record",
		asPlainRecord(byID[labelledSession], plainListed), plainListed)
	page, err := f.Client.InteractionRecordsPage(reader, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if leaked := authorizedtest.Leaks(mustJSON(t, page)); len(leaked) != 0 {
		t.Fatalf("record page leaked %v", leaked)
	}
	summaries, err := f.Client.Interactions(reader)
	if err != nil {
		t.Fatal(err)
	}
	if leaked := authorizedtest.Leaks(mustJSON(t, summaries)); len(leaked) != 0 {
		t.Fatalf("summaries leaked %v", leaked)
	}

	// The subgraph's derived nodes and edges carry the expression as a
	// property; it is withheld there too.
	subgraph, err := f.Client.InteractionSubgraph(reader, labelledSession)
	if err != nil {
		t.Fatal(err)
	}
	plainSubgraph, err := f.Client.InteractionSubgraph(recorder, plainSession)
	if err != nil {
		t.Fatal(err)
	}
	if leaked := authorizedtest.Leaks(mustJSON(t, subgraph)); len(leaked) != 0 {
		t.Fatalf("subgraph leaked %v", leaked)
	}
	assertSamePropertyKeys(t, subgraph, plainSubgraph)

	// The recorder still sees every label, on every read.
	own, err := f.Client.InteractionRecord(recorder, labelledSession)
	if err != nil {
		t.Fatal(err)
	}
	if own.Summary.Visibility != "project-x&secret" ||
		!reflect.DeepEqual(own.Session.RequiredVisibility,
			[]string{"project-x", "secret"}) {
		t.Fatalf("recorder lost its labels: %+v / %+v",
			own.Summary.Visibility, own.Session.RequiredVisibility)
	}
	ownRecords, err := f.Client.InteractionRecords(recorder)
	if err != nil {
		t.Fatal(err)
	}
	if len(authorizedtest.Leaks(mustJSON(t, ownRecords))) != 2 {
		t.Fatalf("recorder list lost its labels: %+v", ownRecords)
	}
	ownSubgraph, err := f.Client.InteractionSubgraph(recorder, labelledSession)
	if err != nil {
		t.Fatal(err)
	}
	if len(authorizedtest.Leaks(mustJSON(t, ownSubgraph))) != 2 {
		t.Fatalf("recorder subgraph lost its labels")
	}

	// Storage is unchanged.
	stored, err := f.Corpus.InteractionRecord(t.Context(), labelledSession)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Summary.Visibility != "project-x&secret" {
		t.Fatalf("stored visibility changed: %q", stored.Summary.Visibility)
	}
}

// assertSamePropertyKeys compares the derived nodes and edges of a withheld
// subgraph with an unlabelled session's by property key set. Their values
// carry session-derived IDs, which differ between the two sessions.
func assertSamePropertyKeys(
	t *testing.T, withheld, plain explorer.Neighborhood,
) {
	t.Helper()
	keys := func(metadata shoal.Metadata) string {
		var names []string
		for key := range metadata {
			names = append(names, key)
		}
		sort.Strings(names)
		return strings.Join(names, ",")
	}
	if len(withheld.Nodes) != len(plain.Nodes) ||
		len(withheld.Edges) != len(plain.Edges) {
		t.Fatalf("subgraph shapes differ: %d/%d nodes, %d/%d edges",
			len(withheld.Nodes), len(plain.Nodes),
			len(withheld.Edges), len(plain.Edges))
	}
	plainNodes := map[string]string{}
	for _, node := range plain.Nodes {
		plainNodes[node.Kind] = keys(node.Properties)
	}
	for _, node := range withheld.Nodes {
		if keys(node.Properties) != plainNodes[node.Kind] {
			t.Fatalf("%s node keys = %s, unlabelled = %s",
				node.Kind, keys(node.Properties), plainNodes[node.Kind])
		}
	}
	plainEdges := map[string]string{}
	for _, edge := range plain.Edges {
		plainEdges[edge.Type] = keys(edge.Properties)
	}
	for _, edge := range withheld.Edges {
		if keys(edge.Properties) != plainEdges[edge.Type] {
			t.Fatalf("%s edge keys = %s, unlabelled = %s",
				edge.Type, keys(edge.Properties), plainEdges[edge.Type])
		}
	}
}

func TestFoldLabelsAreWithheldFromReadersWhoDidNotRecordThem(t *testing.T) {
	f := authorizedtest.New(t)
	f.Record(t, authorizedtest.RecorderSubject, labelledSession,
		authorizedtest.SecretLabels)
	f.Record(t, authorizedtest.RecorderSubject, plainSession, nil)
	labelledFold := f.Fold(t, authorizedtest.RecorderSubject, labelledSession)
	plainFold := f.Fold(t, authorizedtest.RecorderSubject, plainSession)
	if labelledFold.Visibility != "project-x&secret" {
		t.Fatalf("recorder's own fold result lost its labels: %+v", labelledFold)
	}
	reader := f.Context(t, authorizedtest.ReaderSubject)
	recorder := f.Context(t, authorizedtest.RecorderSubject)

	// Fold list.
	folds, err := f.Client.Folds(reader)
	if err != nil {
		t.Fatal(err)
	}
	listed := map[shoal.ID]explorer.FoldSummary{}
	for _, fold := range folds {
		listed[fold.FoldID] = fold
	}
	if len(listed) != 2 {
		t.Fatalf("reader lost folds: %+v", folds)
	}
	recorderFolds, err := f.Client.Folds(recorder)
	if err != nil {
		t.Fatal(err)
	}
	var plainSummary explorer.FoldSummary
	for _, fold := range recorderFolds {
		if fold.FoldID == plainFold.FoldID {
			plainSummary = fold
		}
	}
	withheldSummary := listed[labelledFold.FoldID]
	withheldSummary.FoldID = plainSummary.FoldID
	withheldSummary.FoldedAt = plainSummary.FoldedAt
	assertSameAsUnlabelled(t, "fold summary", withheldSummary, plainSummary)
	page, err := f.Client.FoldsPage(reader, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if leaked := authorizedtest.Leaks(mustJSON(t, page)); len(leaked) != 0 {
		t.Fatalf("fold page leaked %v", leaked)
	}

	// Unfold.
	unfolded, err := f.Client.RehydrateFold(reader, labelledFold.FoldID)
	if err != nil {
		t.Fatal(err)
	}
	plainUnfolded, err := f.Client.RehydrateFold(recorder, plainFold.FoldID)
	if err != nil {
		t.Fatal(err)
	}
	unfolded.FoldedAt = plainUnfolded.FoldedAt
	for index := range unfolded.Members {
		unfolded.Members[index].SessionID = plainSession
	}
	assertSameAsUnlabelled(t, "unfolded fold", unfolded, plainUnfolded)

	// Folding again as the reader replays the same fold, whose result is a
	// read of the fold's visibility like any other.
	replayed, err := f.Client.FoldInteractions(reader, explorer.FoldRequest{
		SessionIDs: []shoal.ID{labelledSession},
	})
	if err != nil {
		t.Fatal(err)
	}
	plainReplayed, err := f.Client.FoldInteractions(recorder, explorer.FoldRequest{
		SessionIDs: []shoal.ID{plainSession},
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Created || replayed.FoldID != labelledFold.FoldID {
		t.Fatalf("fold replay = %+v", replayed)
	}
	replayed.FoldID = plainReplayed.FoldID
	replayed.FoldedAt = plainReplayed.FoldedAt
	assertSameAsUnlabelled(t, "fold result", replayed, plainReplayed)

	// The recorder still sees them.
	ownFolds, err := f.Client.Folds(recorder)
	if err != nil {
		t.Fatal(err)
	}
	if len(authorizedtest.Leaks(mustJSON(t, ownFolds))) != 2 {
		t.Fatalf("recorder fold list lost its labels: %+v", ownFolds)
	}
	ownUnfolded, err := f.Client.RehydrateFold(recorder, labelledFold.FoldID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ownUnfolded.Members[0].Visibility,
		[]string{"project-x", "secret"}) {
		t.Fatalf("recorder unfold lost its labels: %+v", ownUnfolded)
	}
}

// A fold spans several recorders. Its own conjoined expression is shown only
// to a reader who recorded every member; each member's label set is shown only
// to that member's recorder.
func TestMultiRecorderFoldShowsEachReaderOnlyItsOwnMemberLabels(t *testing.T) {
	f := authorizedtest.New(t)
	f.Record(t, authorizedtest.RecorderSubject, labelledSession,
		authorizedtest.SecretLabels)
	f.Record(t, authorizedtest.ReaderSubject, readerSession,
		[]string{"reader-own"})
	fold := f.Fold(t, authorizedtest.RecorderSubject,
		labelledSession, readerSession)
	if fold.Visibility != "" {
		t.Fatalf("folder that recorded only one member saw the fold's labels: %+v", fold)
	}

	memberLabels := func(subject string) (string, map[shoal.ID][]string) {
		t.Helper()
		ctx := f.Context(t, subject)
		folds, err := f.Client.Folds(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(folds) != 1 {
			t.Fatalf("%s folds = %+v", subject, folds)
		}
		unfolded, err := f.Client.RehydrateFold(ctx, fold.FoldID)
		if err != nil {
			t.Fatal(err)
		}
		labels := map[shoal.ID][]string{}
		for _, member := range unfolded.Members {
			labels[member.SessionID] = member.Visibility
		}
		return folds[0].Visibility, labels
	}

	foldLabel, labels := memberLabels(authorizedtest.ReaderSubject)
	if foldLabel != "" || labels[labelledSession] != nil ||
		!reflect.DeepEqual(labels[readerSession], []string{"reader-own"}) {
		t.Fatalf("reader fold view = %q / %+v", foldLabel, labels)
	}
	foldLabel, labels = memberLabels(authorizedtest.RecorderSubject)
	if foldLabel != "" || labels[readerSession] != nil ||
		!reflect.DeepEqual(labels[labelledSession], []string{"project-x", "secret"}) {
		t.Fatalf("recorder fold view = %q / %+v", foldLabel, labels)
	}
	// A third principal recorded neither member.
	third := f.Context(t, "label-third")
	unfolded, err := f.Client.RehydrateFold(third, fold.FoldID)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range unfolded.Members {
		if member.Visibility != nil {
			t.Fatalf("third principal saw member labels: %+v", unfolded)
		}
	}
}
