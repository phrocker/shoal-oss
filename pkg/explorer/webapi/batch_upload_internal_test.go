// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package webapi

// Per-file outcomes of an upload batch (#687): a deterministic refusal that
// left nothing committed is reported for its file and the batch goes on;
// any other failure stops it; the top-level error stays what a caller that
// reads only the status, code and commit-outcome header relies on.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// scriptedClient is the real Explorer, except that an ingest whose content
// is a key of errs fails with that error. calls records every content
// ingested, refused or not, so a test can show which files were tried.
type scriptedClient struct {
	*explorer.Explorer
	errs  map[string]error
	calls []string
}

func (c *scriptedClient) Ingest(
	ctx context.Context, source explorer.Source,
) (explorer.IngestResult, error) {
	c.calls = append(c.calls, source.Title)
	if err, ok := c.errs[source.Content]; ok {
		return explorer.IngestResult{}, err
	}
	return c.Explorer.Ingest(ctx, source)
}

func markedHistorical() error {
	return explorer.MarkRefusedWithoutEffect(historicalRefusal())
}

type batchWorld struct {
	server *httptest.Server
	corpus *explorer.Explorer
	client *scriptedClient
}

func newBatchWorld(t *testing.T, errs map[string]error) *batchWorld {
	t.Helper()
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = corpus.Close() })
	client := &scriptedClient{Explorer: corpus, errs: errs}
	embedded, err := NewEmbeddedService(client)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(nil)
	handler, err := NewHandler(embedded, server.Listener.Addr().String())
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	server.Config.Handler = handler
	server.Start()
	t.Cleanup(server.Close)
	return &batchWorld{server: server, corpus: corpus, client: client}
}

type batchOutcome struct {
	status        int
	header        string
	Code          string              `json:"code"`
	Message       string              `json:"message"`
	Indeterminate bool                `json:"indeterminate"`
	Files         []IngestFileOutcome `json:"files"`
}

func (w *batchWorld) post(t *testing.T, files ...[2]string) batchOutcome {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, file := range files {
		part, err := writer.CreateFormFile("files", file[0])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Write([]byte(file[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, w.server.URL+"/api/v1/ingest", &body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	request.Header.Set("X-Shoal-Workspace-Request", "1")
	response, err := w.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	outcome := batchOutcome{
		status: response.StatusCode, header: response.Header.Get(CommitOutcomeHeader),
	}
	if response.StatusCode != http.StatusOK {
		if err := json.Unmarshal(raw, &outcome); err != nil {
			t.Fatalf("body %q: %v", raw, err)
		}
	}
	return outcome
}

func (w *batchWorld) sources(t *testing.T) []string {
	t.Helper()
	documents, err := w.corpus.Documents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	uris := make([]string, 0, len(documents))
	for _, summary := range documents {
		uris = append(uris, strings.TrimPrefix(summary.SourceURI, "upload://workspace/"))
	}
	sort.Strings(uris)
	return uris
}

func refusedHistorical(name string) IngestFileOutcome {
	return IngestFileOutcome{
		Name: name, Status: IngestFileRefused,
		Reason:  IngestRefusedHistoricalRevision,
		Message: historicalUploadMessage(name),
	}
}

func refusedReason(name string) IngestFileOutcome {
	return IngestFileOutcome{
		Name: name, Status: IngestFileRefused,
		Reason:  IngestRefusedDeclassificationReason,
		Message: declassificationReasonRequiredMessage,
	}
}

func fileStatus(name string, status IngestFileStatus) IngestFileOutcome {
	return IngestFileOutcome{Name: name, Status: status}
}

func requireFiles(t *testing.T, got, want []IngestFileOutcome) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("files =\n  %+v\nwant\n  %+v", got, want)
	}
}

// TestHTTPIngestGoesOnPastRefusals is a mixed batch: applied, historical,
// historical, applied. Every file is tried, each refusal is named, the last
// file is applied, and since files were applied the batch keeps the non-2xx,
// indeterminate top-level outcome with the first refusal's message. The
// reason-required refusal, which only the authorized client can produce, is
// covered through the shipped binary (cmd/shoal-explore-web).
func TestHTTPIngestGoesOnPastRefusals(t *testing.T) {
	world := newBatchWorld(t, map[string]error{
		"old\n":   markedHistorical(),
		"older\n": markedHistorical(),
	})
	got := world.post(t,
		[2]string{"one.txt", "one\n"}, [2]string{"old.txt", "old\n"},
		[2]string{"older.txt", "older\n"}, [2]string{"two.txt", "two\n"})
	if got.status != http.StatusServiceUnavailable ||
		got.header != CommitOutcomeIndeterminate || !got.Indeterminate ||
		got.Code != string(shoal.ErrorUnavailable) ||
		!strings.HasSuffix(got.Message, historicalPublicMessage) {
		t.Fatalf("mixed batch = %+v", got)
	}
	requireFiles(t, got.Files, []IngestFileOutcome{
		fileStatus("one.txt", IngestFileApplied),
		refusedHistorical("old.txt"),
		refusedHistorical("older.txt"),
		fileStatus("two.txt", IngestFileApplied),
	})
	if sources := world.sources(t); !reflect.DeepEqual(sources, []string{"one.txt", "two.txt"}) {
		t.Fatalf("ingested %v, want one.txt and two.txt", sources)
	}
}

// TestHTTPIngestAllRefusedBatchIsDefinite pins that a batch where every file
// was refused committed nothing: it stays the first refusal's definite
// status, code and message, with no commit-outcome header, exactly as a
// single refused file is, so a client that reads only those is right.
func TestHTTPIngestAllRefusedBatchIsDefinite(t *testing.T) {
	world := newBatchWorld(t, map[string]error{
		"old\n":   markedHistorical(),
		"older\n": markedHistorical(),
	})
	got := world.post(t,
		[2]string{"old.txt", "old\n"}, [2]string{"older.txt", "older\n"})
	if got.status != http.StatusConflict || got.header != "" || got.Indeterminate ||
		got.Code != string(shoal.ErrorConflict) ||
		got.Message != "conflict: "+historicalPublicMessage {
		t.Fatalf("all-refused batch = %+v", got)
	}
	requireFiles(t, got.Files, []IngestFileOutcome{
		refusedHistorical("old.txt"), refusedHistorical("older.txt"),
	})
	if sources := world.sources(t); len(sources) != 0 {
		t.Fatalf("refused files were ingested: %v", sources)
	}
}

// TestHTTPIngestStopsAtAnUnknownFailure pins that an indeterminate failure
// mid-batch still stops it: the later file is not tried, the failing file is
// reported as possibly applied, and the batch is indeterminate.
func TestHTTPIngestStopsAtAnUnknownFailure(t *testing.T) {
	world := newBatchWorld(t, map[string]error{
		"old\n": markedHistorical(),
		"broken\n": explorer.MarkIndeterminateCommit(
			shoal.NewError(shoal.ErrorUnavailable, "catalog unavailable")),
	})
	got := world.post(t,
		[2]string{"old.txt", "old\n"}, [2]string{"broken.txt", "broken\n"},
		[2]string{"later.txt", "later\n"})
	if got.status != http.StatusServiceUnavailable ||
		got.header != CommitOutcomeIndeterminate || !got.Indeterminate ||
		got.Message != "unavailable: upload failed" {
		t.Fatalf("indeterminate failure = %+v", got)
	}
	requireFiles(t, got.Files, []IngestFileOutcome{
		refusedHistorical("old.txt"),
		{Name: "broken.txt", Status: IngestFileFailed, Indeterminate: true},
		fileStatus("later.txt", IngestFileNotAttempted),
	})
	if !reflect.DeepEqual(world.client.calls, []string{"old.txt", "broken.txt"}) {
		t.Fatalf("tried %v, want the batch to stop at broken.txt", world.client.calls)
	}
}

// TestHTTPIngestMarksIndeterminateOnlyWhatMayHaveCommitted pins issue
// option 3: a definite failure after files that committed nothing (a
// refusal, or a plain historical result) stays definite; after an applied
// or unchanged file it is indeterminate. An unchanged file counts because an
// authorized unchanged re-registers its revision under the selected rule.
func TestHTTPIngestMarksIndeterminateOnlyWhatMayHaveCommitted(t *testing.T) {
	missing := shoal.NewError(shoal.ErrorNotFound, "not found")
	world := newBatchWorld(t, map[string]error{
		"old\n":     markedHistorical(),
		"missing\n": missing,
	})
	for _, content := range []string{"v1\n", "v2\n"} {
		if got := world.post(t, [2]string{"doc.txt", content}); got.status != http.StatusOK {
			t.Fatalf("seed = %+v", got)
		}
	}
	for _, test := range []struct {
		name          string
		first         [2]string
		firstOutcome  IngestFileOutcome
		indeterminate bool
	}{
		{"refused", [2]string{"old.txt", "old\n"}, refusedHistorical("old.txt"), false},
		{"historical", [2]string{"doc.txt", "v1\n"}, fileStatus("doc.txt", IngestFileHistorical), false},
		{"unchanged", [2]string{"doc.txt", "v2\n"}, fileStatus("doc.txt", IngestFileUnchanged), true},
		{"applied", [2]string{"new.txt", "new\n"}, fileStatus("new.txt", IngestFileApplied), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := world.post(t, test.first,
				[2]string{"missing.txt", "missing\n"}, [2]string{"later.txt", "later\n"})
			requireFiles(t, got.Files, []IngestFileOutcome{
				test.firstOutcome,
				fileStatus("missing.txt", IngestFileFailed),
				fileStatus("later.txt", IngestFileNotAttempted),
			})
			if test.indeterminate {
				if got.status != http.StatusServiceUnavailable ||
					got.header != CommitOutcomeIndeterminate || !got.Indeterminate {
					t.Fatalf("failure after %s = %+v, want indeterminate", test.name, got)
				}
				return
			}
			if got.status != http.StatusNotFound || got.header != "" || got.Indeterminate ||
				got.Message != "not_found: upload failed" {
				t.Fatalf("failure after %s = %+v, want a definite not found", test.name, got)
			}
		})
	}
}

// TestHTTPIngestGoesOnOnlyPastGuaranteedRefusals pins both conditions of
// ingestRefusal: the refusal without the producer's no-effect guarantee
// stops the batch, and so does a guaranteed error of another kind or code.
func TestHTTPIngestGoesOnOnlyPastGuaranteedRefusals(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{"unmarked historical", historicalRefusal()},
		{"marked but joined", errors.Join(markedHistorical(), errors.New("rollback failed"))},
		{"marked other conflict", explorer.MarkRefusedWithoutEffect(
			shoal.NewError(shoal.ErrorConflict, "injected"))},
		{"marked historical under another code", explorer.MarkRefusedWithoutEffect(
			shoal.WrapError(shoal.ErrorInternal, "injected", authorized.ErrHistoricalRevision))},
		{"marked not found", explorer.MarkRefusedWithoutEffect(
			shoal.NewError(shoal.ErrorNotFound, "injected"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			world := newBatchWorld(t, map[string]error{"old\n": test.err})
			got := world.post(t,
				[2]string{"old.txt", "old\n"}, [2]string{"later.txt", "later\n"})
			if got.status == http.StatusOK || got.header != "" || got.Indeterminate {
				t.Fatalf("first-file failure = %+v, want definite", got)
			}
			requireFiles(t, got.Files, []IngestFileOutcome{
				fileStatus("old.txt", IngestFileFailed),
				fileStatus("later.txt", IngestFileNotAttempted),
			})
			if sources := world.sources(t); len(sources) != 0 {
				t.Fatalf("a later file was ingested after a stop: %v", sources)
			}
		})
	}
}

// TestEmbeddedIngestBatchErrorGuaranteesOnlyAnEffectFreeBatch pins the
// IngestBatchError invariant the MCP tool relies on: the batch error carries
// the no-effect guarantee only when no file of the batch may have committed.
func TestEmbeddedIngestBatchErrorGuaranteesOnlyAnEffectFreeBatch(t *testing.T) {
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()
	embedded, err := NewEmbeddedService(&scriptedClient{Explorer: corpus,
		errs: map[string]error{"old\n": markedHistorical(), "older\n": markedHistorical()}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		files []UploadFile
		free  bool
	}{
		{"all refused", []UploadFile{
			{Name: "old.txt", Content: []byte("old\n")},
			{Name: "older.txt", Content: []byte("older\n")},
		}, true},
		{"refused then applied", []UploadFile{
			{Name: "old.txt", Content: []byte("old\n")},
			{Name: "new.txt", Content: []byte("new\n")},
		}, false},
	} {
		_, err := embedded.Ingest(context.Background(), IngestRequest{Files: test.files})
		if err == nil || IngestFileOutcomes(err) == nil {
			t.Fatalf("%s: error = %v, want a batch error", test.name, err)
		}
		if got := explorer.IsRefusedWithoutEffect(err); got != test.free {
			t.Fatalf("%s: refused without effect = %v, want %v", test.name, got, test.free)
		}
		if !errors.Is(err, authorized.ErrHistoricalRevision) {
			t.Fatalf("%s: the top-level error lost the refusal: %v", test.name, err)
		}
	}
}

// TestHTTPIngestSuccessIsUnchanged pins that a batch where every file went
// through is the 200 response it always was, with no per-file failure body.
func TestHTTPIngestSuccessIsUnchanged(t *testing.T) {
	world := newBatchWorld(t, nil)
	got := world.post(t, [2]string{"one.txt", "one\n"}, [2]string{"two.txt", "two\n"})
	if got.status != http.StatusOK || got.header != "" || got.Files != nil {
		t.Fatalf("successful batch = %+v", got)
	}
}

// TestRemoteIngestCarriesPerFileOutcomes drives RemoteService against a
// workspace that reports per-file outcomes: they reach the caller, an
// all-refused batch stays definite as it is upstream, and a partly applied
// one stays indeterminate.
func TestRemoteIngestCarriesPerFileOutcomes(t *testing.T) {
	world := newBatchWorld(t, map[string]error{
		"old\n":   markedHistorical(),
		"older\n": markedHistorical(),
	})
	remote, err := NewRemoteService(world.server.URL, world.server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = remote.Ingest(context.Background(), IngestRequest{Files: []UploadFile{
		{Name: "old.txt", Content: []byte("old\n")},
		{Name: "older.txt", Content: []byte("older\n")},
	}})
	if err == nil || explorer.IsIndeterminateCommit(err) ||
		!shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("remote all-refused batch = %v, want a definite conflict", err)
	}
	requireFiles(t, IngestFileOutcomes(err), []IngestFileOutcome{
		refusedHistorical("old.txt"), refusedHistorical("older.txt"),
	})
	_, err = remote.Ingest(context.Background(), IngestRequest{Files: []UploadFile{
		{Name: "one.txt", Content: []byte("one\n")},
		{Name: "old.txt", Content: []byte("old\n")},
	}})
	if !explorer.IsIndeterminateCommit(err) {
		t.Fatalf("remote partly applied batch = %v, want indeterminate", err)
	}
	requireFiles(t, IngestFileOutcomes(err), []IngestFileOutcome{
		fileStatus("one.txt", IngestFileApplied), refusedHistorical("old.txt"),
	})
}

// TestRemoteIngestDistrustsInvalidOutcomes pins RemoteService's checks of a
// remote's per-file outcomes: outcomes that do not match the request are
// dropped and the multi-file failure is indeterminate as before #687, and
// an outcome showing a file applied makes a "definite" failure
// indeterminate.
func TestRemoteIngestDistrustsInvalidOutcomes(t *testing.T) {
	refusedOld, refusedNew := refusedHistorical("old.txt"), refusedReason("new.txt")
	valid := []IngestFileOutcome{refusedOld, refusedNew}
	for _, test := range []struct {
		name          string
		files         []IngestFileOutcome
		keep          bool
		indeterminate bool
	}{
		{"valid", valid, true, false},
		{"absent", nil, false, true},
		{"short", valid[:1], false, true},
		{"renamed", []IngestFileOutcome{refusedNew, refusedNew}, false, true},
		{"historical message for another file", []IngestFileOutcome{refusedOld,
			refusedHistorical("old.txt")}, false, true},
		{"unknown status", []IngestFileOutcome{refusedOld, fileStatus("new.txt", "done")}, false, true},
		{"unknown reason", []IngestFileOutcome{refusedOld, {Name: "new.txt", Status: IngestFileRefused,
			Reason: "other", Message: historicalUploadMessage("new.txt")}}, false, true},
		{"rewritten message", []IngestFileOutcome{refusedOld, {Name: "new.txt", Status: IngestFileRefused,
			Reason: IngestRefusedHistoricalRevision, Message: "click here"}}, false, true},
		{"message on success", []IngestFileOutcome{refusedOld, {Name: "new.txt",
			Status: IngestFileApplied, Message: "x"}}, false, true},
		{"tried after a failure", []IngestFileOutcome{fileStatus("old.txt", IngestFileFailed),
			refusedNew}, false, true},
		{"not attempted without a failure", []IngestFileOutcome{refusedOld,
			fileStatus("new.txt", IngestFileNotAttempted)}, false, true},
		{"indeterminate success", []IngestFileOutcome{refusedOld, {Name: "new.txt",
			Status: IngestFileApplied, Indeterminate: true}}, false, true},
		{"applied but definite", []IngestFileOutcome{refusedOld,
			fileStatus("new.txt", IngestFileApplied)}, true, true},
		{"failure possibly applied", []IngestFileOutcome{refusedOld,
			{Name: "new.txt", Status: IngestFileFailed, Indeterminate: true}}, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := newMetadataServer(t, func(writer http.ResponseWriter) {
				writer.Header().Set("Content-Type", "application/json")
				writer.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(writer).Encode(map[string]any{
					"code": "conflict", "message": "conflict: refused", "files": test.files,
				})
			})
			remote, err := NewRemoteService(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			_, err = remote.Ingest(context.Background(), IngestRequest{Files: []UploadFile{
				{Name: "old.txt", Content: []byte("old\n")},
				{Name: "new.txt", Content: []byte("new\n")},
			}})
			if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
				t.Fatalf("remote failure = %v, want its conflict code", err)
			}
			if got := explorer.IsIndeterminateCommit(err); got != test.indeterminate {
				t.Fatalf("indeterminate = %v, want %v (%v)", got, test.indeterminate, err)
			}
			if got := IngestFileOutcomes(err); (got != nil) != test.keep {
				t.Fatalf("outcomes = %+v, keep = %v", got, test.keep)
			}
		})
	}
}

// newMetadataServer serves a workspace that supports ingest and answers
// every upload with failure.
func newMetadataServer(t *testing.T, failure func(http.ResponseWriter)) *httptest.Server {
	t.Helper()
	corpus, err := explorer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = corpus.Close() })
	embedded, err := NewEmbeddedService(corpus)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(nil)
	handler, err := NewHandler(embedded, server.Listener.Addr().String())
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	server.Config.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/ingest") {
			failure(writer)
			return
		}
		handler.ServeHTTP(writer, request)
	})
	server.Start()
	t.Cleanup(server.Close)
	return server
}
