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

package webapi_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized/authorizedtest"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const labelSubjectHeader = "X-Test-Label-Subject"

const (
	httpLabelledSession shoal.ID = "interaction.session_http_labelled"
	httpPlainSession    shoal.ID = "interaction.session_http_plain"
)

func labelID(id shoal.ID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

type labelHTTP struct {
	t       *testing.T
	handler *webapi.Handler
}

func newLabelHTTP(t *testing.T, f *authorizedtest.Fixture) labelHTTP {
	t.Helper()
	embedded, err := webapi.NewEmbeddedService(f.Client)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := webapi.NewAuthenticatedHandler(
		embedded,
		webapi.AuthenticatorFunc(func(request *http.Request) (auth.Decision, error) {
			return f.Decision(t, request.Header.Get(labelSubjectHeader)), nil
		}),
		f.Authority.Binder(), "example.test",
	)
	if err != nil {
		t.Fatal(err)
	}
	provenance, err := webapi.NewInteractionService(f.Client)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.SetInteractionProvider(provenance); err != nil {
		t.Fatal(err)
	}
	return labelHTTP{t: t, handler: handler}
}

// do returns the exact response bytes the handler wrote.
func (h labelHTTP) do(subject, method, path string, body any) []byte {
	h.t.Helper()
	var reader *bytes.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			h.t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	} else {
		reader = bytes.NewReader(nil)
	}
	request := httptest.NewRequest(method, path, reader)
	request.Host = "example.test"
	request.Header.Set(labelSubjectHeader, subject)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	h.handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK && response.Code != http.StatusCreated {
		h.t.Fatalf("%s %s as %s = %d: %s",
			method, path, subject, response.Code, response.Body.String())
	}
	return response.Body.Bytes()
}

// field returns the raw JSON of one top-level field of an object.
func field(t *testing.T, raw []byte, name string) string {
	t.Helper()
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	return string(object[name])
}

// renamed rewrites the identity and timestamp fields that necessarily differ
// between two stored records, leaving every other byte, including any label
// or shape marker, untouched.
func renamed(raw []byte, pairs ...string) []byte {
	result := raw
	for index := 0; index+1 < len(pairs); index += 2 {
		result = bytes.ReplaceAll(result, []byte(pairs[index]), []byte(pairs[index+1]))
	}
	return result
}

func assertWireUnlabelled(t *testing.T, name string, withheld, plain []byte) {
	t.Helper()
	if !bytes.Equal(withheld, plain) {
		t.Fatalf("%s: withheld wire differs from an unlabelled record's:\nwithheld=%s\nplain=   %s",
			name, withheld, plain)
	}
}

func assertNoLeak(t *testing.T, name string, body []byte) {
	t.Helper()
	if leaked := authorizedtest.Leaks(body); len(leaked) != 0 {
		t.Fatalf("%s leaked %v to a reader that did not record them: %s",
			name, leaked, body)
	}
}

func assertRecorderSees(t *testing.T, name string, body []byte) {
	t.Helper()
	if leaked := authorizedtest.Leaks(body); len(leaked) != len(authorizedtest.SecretLabels) {
		t.Fatalf("%s: recorder no longer sees its own labels: %s", name, body)
	}
}

// TestProvenanceHTTPWithholdsLabelsFromNonRecorders drives list, inspect,
// fold and unfold over HTTP through the real authorized client and corpus.
// A reader that did not record a labelled session receives byte-for-byte the
// response an unlabelled session produces; the recorder still sees its labels.
func TestProvenanceHTTPWithholdsLabelsFromNonRecorders(t *testing.T) {
	f := authorizedtest.New(t)
	f.Record(t, authorizedtest.RecorderSubject, httpLabelledSession,
		authorizedtest.SecretLabels)
	f.Record(t, authorizedtest.RecorderSubject, httpPlainSession, nil)
	labelledFold := f.Fold(t, authorizedtest.RecorderSubject, httpLabelledSession)
	plainFold := f.Fold(t, authorizedtest.RecorderSubject, httpPlainSession)
	h := newLabelHTTP(t, f)
	reader, recorder := authorizedtest.ReaderSubject, authorizedtest.RecorderSubject

	// Every withheld response is compared with the unlabelled record's
	// response to its own recorder, which passes through no withholding and
	// so is exactly the wire shape of a never-labelled record.

	// List.
	listed := h.do(reader, http.MethodGet, "/api/v1/provenance", nil)
	assertNoLeak(t, "provenance list", listed)
	recorderListed := h.do(recorder, http.MethodGet, "/api/v1/provenance", nil)
	assertRecorderSees(t, "provenance list", recorderListed)
	listItems := func(body []byte) map[string][]byte {
		var page struct {
			Interactions []json.RawMessage `json:"interactions"`
			Folds        []json.RawMessage `json:"folds"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatal(err)
		}
		items := map[string][]byte{}
		for _, raw := range append(page.Interactions, page.Folds...) {
			id := field(t, raw, "session_id")
			if id == "" {
				id = field(t, raw, "fold_id")
			}
			items[id] = raw
		}
		return items
	}
	items, recorderItems := listItems(listed), listItems(recorderListed)
	quoted := func(id shoal.ID) string { return `"` + labelID(id) + `"` }
	labelledItem := items[quoted(httpLabelledSession)]
	plainItem := recorderItems[quoted(httpPlainSession)]
	if labelledItem == nil || plainItem == nil {
		t.Fatalf("reader list lost records: %s", listed)
	}
	assertWireUnlabelled(t, "listed interaction",
		renamed(labelledItem,
			labelID(httpLabelledSession), labelID(httpPlainSession),
			field(t, labelledItem, "recorded_at"), field(t, plainItem, "recorded_at")),
		plainItem)
	labelledFoldItem := items[quoted(labelledFold.FoldID)]
	plainFoldItem := recorderItems[quoted(plainFold.FoldID)]
	if labelledFoldItem == nil || plainFoldItem == nil {
		t.Fatalf("reader list lost folds: %s", listed)
	}
	assertWireUnlabelled(t, "listed fold",
		renamed(labelledFoldItem,
			labelID(labelledFold.FoldID), labelID(plainFold.FoldID),
			field(t, labelledFoldItem, "folded_at"), field(t, plainFoldItem, "folded_at")),
		plainFoldItem)

	// Inspect.
	inspected := h.do(reader, http.MethodGet,
		"/api/v1/provenance/"+labelID(httpLabelledSession), nil)
	plainInspected := h.do(recorder, http.MethodGet,
		"/api/v1/provenance/"+labelID(httpPlainSession), nil)
	assertNoLeak(t, "provenance inspect", inspected)
	assertWireUnlabelled(t, "inspected interaction",
		renamed(inspected,
			labelID(httpLabelledSession), labelID(httpPlainSession),
			field(t, inspected, "recorded_at"), field(t, plainInspected, "recorded_at")),
		plainInspected)
	assertRecorderSees(t, "provenance inspect", h.do(recorder, http.MethodGet,
		"/api/v1/provenance/"+labelID(httpLabelledSession), nil))

	// Unfold.
	unfold := func(subject string, id shoal.ID) []byte {
		return h.do(subject, http.MethodPost, "/api/v1/provenance/unfold",
			webapi.ProvenanceUnfoldRequest{FoldID: labelID(id)})
	}
	unfolded := unfold(reader, labelledFold.FoldID)
	plainUnfolded := unfold(recorder, plainFold.FoldID)
	assertNoLeak(t, "provenance unfold", unfolded)
	assertWireUnlabelled(t, "unfolded fold",
		renamed(unfolded,
			labelID(labelledFold.FoldID), labelID(plainFold.FoldID),
			labelID(httpLabelledSession), labelID(httpPlainSession),
			field(t, unfolded, "folded_at"), field(t, plainUnfolded, "folded_at")),
		plainUnfolded)
	assertRecorderSees(t, "provenance unfold", unfold(recorder, labelledFold.FoldID))

	// Fold (an idempotent replay returns the existing fold's visibility).
	fold := func(subject string, id shoal.ID) []byte {
		return h.do(subject, http.MethodPost, "/api/v1/provenance/fold",
			webapi.ProvenanceFoldRequest{SessionIDs: []string{labelID(id)}})
	}
	folded := fold(reader, httpLabelledSession)
	plainFolded := fold(recorder, httpPlainSession)
	assertNoLeak(t, "provenance fold", folded)
	assertWireUnlabelled(t, "fold result",
		renamed(folded,
			labelID(labelledFold.FoldID), labelID(plainFold.FoldID),
			field(t, folded, "folded_at"), field(t, plainFolded, "folded_at")),
		plainFolded)
	assertRecorderSees(t, "provenance fold", fold(recorder, httpLabelledSession))
}

// TestProvenanceHTTPMultiRecorderFoldShowsOnlyOwnMemberLabels: a fold over
// sessions from two recorders shows neither its conjoined label nor the other
// recorder's member label to either of them.
func TestProvenanceHTTPMultiRecorderFoldShowsOnlyOwnMemberLabels(t *testing.T) {
	f := authorizedtest.New(t)
	f.Record(t, authorizedtest.RecorderSubject, httpLabelledSession,
		authorizedtest.SecretLabels)
	const readerOwn shoal.ID = "interaction.session_http_reader_own"
	f.Record(t, authorizedtest.ReaderSubject, readerOwn, []string{"reader-own"})
	fold := f.Fold(t, authorizedtest.RecorderSubject, httpLabelledSession, readerOwn)
	h := newLabelHTTP(t, f)

	listed := h.do(authorizedtest.ReaderSubject, http.MethodGet,
		"/api/v1/provenance", nil)
	assertNoLeak(t, "multi-recorder list", listed)
	if !bytes.Contains(listed, []byte(`"output_visibility":"reader-own"`)) {
		t.Fatalf("reader lost its own session's label: %s", listed)
	}
	unfolded := h.do(authorizedtest.ReaderSubject, http.MethodPost,
		"/api/v1/provenance/unfold",
		webapi.ProvenanceUnfoldRequest{FoldID: labelID(fold.FoldID)})
	assertNoLeak(t, "multi-recorder unfold", unfolded)
	if !bytes.Contains(unfolded, []byte(`"output_visibility":["reader-own"]`)) ||
		field(t, unfolded, "output_visibility") != `""` {
		t.Fatalf("reader multi-recorder unfold = %s", unfolded)
	}
	ownUnfolded := h.do(authorizedtest.RecorderSubject, http.MethodPost,
		"/api/v1/provenance/unfold",
		webapi.ProvenanceUnfoldRequest{FoldID: labelID(fold.FoldID)})
	assertRecorderSees(t, "multi-recorder unfold", ownUnfolded)
	if bytes.Contains(ownUnfolded, []byte("reader-own")) ||
		field(t, ownUnfolded, "output_visibility") != `""` {
		t.Fatalf("recorder multi-recorder unfold = %s", ownUnfolded)
	}
}
