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

package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized/authorizedtest"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	mcpLabelledSession shoal.ID = "interaction.session_mcp_labelled"
	mcpPlainSession    shoal.ID = "interaction.session_mcp_plain"
)

func mcpLabelID(id shoal.ID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

// mcpLabelServer is a real MCP server whose provenance tools are backed by the
// real authorized client, acting as subject.
func mcpLabelServer(
	t *testing.T, f *authorizedtest.Fixture, subject string,
) *Server {
	t.Helper()
	service, err := webapi.NewEmbeddedService(f.Client)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := webapi.NewInteractionService(f.Client)
	if err != nil {
		t.Fatal(err)
	}
	tools, err := NewInteractionTools(provider)
	if err != nil {
		t.Fatal(err)
	}
	template := f.Decision(t, subject)
	server, err := newRecordedTestServer(t, Config{
		Service: service, Authority: f.Authority,
		Decisions: DecisionProviderFunc(func(context.Context) (auth.Decision, error) {
			return template, nil
		}),
		Snapshots: f.Client, OptionalTools: tools,
		requestIDFactory: sequentialRequestIDs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	makeReady(t, server)
	return server
}

// mcpLabelCall returns the tool's structured content bytes.
func mcpLabelCall(t *testing.T, server *Server, name, arguments string) []byte {
	t.Helper()
	response := callToolRequest(t, server, name, arguments)
	result := decodeToolResult(t, response)
	if response.Error != nil || result.IsError {
		t.Fatalf("%s failed: %+v / %s", name, response.Error, result.StructuredContent)
	}
	return result.StructuredContent
}

func mcpField(t *testing.T, raw []byte, name string) string {
	t.Helper()
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	return string(object[name])
}

func mcpRenamed(raw []byte, pairs ...string) []byte {
	for index := 0; index+1 < len(pairs); index += 2 {
		raw = bytes.ReplaceAll(raw, []byte(pairs[index]), []byte(pairs[index+1]))
	}
	return raw
}

// TestProvenanceToolsWithholdLabelsFromNonRecorders: the MCP provenance list,
// inspect and unfold tools serve a reader that did not record a labelled
// session exactly the bytes an unlabelled session produces, and still serve
// the recorder its labels.
func TestProvenanceToolsWithholdLabelsFromNonRecorders(t *testing.T) {
	f := authorizedtest.New(t)
	f.Record(t, authorizedtest.RecorderSubject, mcpLabelledSession,
		authorizedtest.SecretLabels)
	f.Record(t, authorizedtest.RecorderSubject, mcpPlainSession, nil)
	labelledFold := f.Fold(t, authorizedtest.RecorderSubject, mcpLabelledSession)
	plainFold := f.Fold(t, authorizedtest.RecorderSubject, mcpPlainSession)
	reader := mcpLabelServer(t, f, authorizedtest.ReaderSubject)
	recorder := mcpLabelServer(t, f, authorizedtest.RecorderSubject)
	noLeak := func(name string, body []byte) {
		t.Helper()
		if leaked := authorizedtest.Leaks(body); len(leaked) != 0 {
			t.Fatalf("%s leaked %v: %s", name, leaked, body)
		}
	}
	recorderSees := func(name string, body []byte) {
		t.Helper()
		if len(authorizedtest.Leaks(body)) != len(authorizedtest.SecretLabels) {
			t.Fatalf("%s: recorder lost its labels: %s", name, body)
		}
	}
	same := func(name string, withheld, plain []byte) {
		t.Helper()
		if !bytes.Equal(withheld, plain) {
			t.Fatalf("%s: withheld differs from unlabelled:\nwithheld=%s\nplain=   %s",
				name, withheld, plain)
		}
	}

	// List.
	listed := mcpLabelCall(t, reader, ToolProvenanceList, `{}`)
	noLeak("list", listed)
	recorderSees("list", mcpLabelCall(t, recorder, ToolProvenanceList, `{}`))
	var page webapi.ProvenanceListResponse
	if err := json.Unmarshal(listed, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Interactions) != 2 || len(page.Folds) != 2 {
		t.Fatalf("reader list lost records: %s", listed)
	}

	// Inspect.
	inspect := func(server *Server, id shoal.ID) []byte {
		return mcpLabelCall(t, server, ToolProvenanceInspect,
			`{"session_id":"`+mcpLabelID(id)+`"}`)
	}
	inspected := inspect(reader, mcpLabelledSession)
	// The unlabelled record as its own recorder reads it passes through no
	// withholding, so it is exactly the never-labelled wire shape.
	plainInspected := inspect(recorder, mcpPlainSession)
	noLeak("inspect", inspected)
	same("inspect", mcpRenamed(inspected,
		mcpLabelID(mcpLabelledSession), mcpLabelID(mcpPlainSession),
		mcpField(t, inspected, "recorded_at"), mcpField(t, plainInspected, "recorded_at"),
	), plainInspected)
	recorderSees("inspect", inspect(recorder, mcpLabelledSession))

	// Unfold.
	unfold := func(server *Server, id shoal.ID) []byte {
		return mcpLabelCall(t, server, ToolProvenanceUnfold,
			`{"fold_id":"`+mcpLabelID(id)+`"}`)
	}
	unfolded := unfold(reader, labelledFold.FoldID)
	plainUnfolded := unfold(recorder, plainFold.FoldID)
	noLeak("unfold", unfolded)
	same("unfold", mcpRenamed(unfolded,
		mcpLabelID(labelledFold.FoldID), mcpLabelID(plainFold.FoldID),
		mcpLabelID(mcpLabelledSession), mcpLabelID(mcpPlainSession),
		mcpField(t, unfolded, "folded_at"), mcpField(t, plainUnfolded, "folded_at"),
	), plainUnfolded)
	recorderSees("unfold", unfold(recorder, labelledFold.FoldID))
}

// TestProvenanceToolsMultiRecorderFoldShowsOnlyOwnMemberLabels: through MCP,
// a reader who recorded one member of a two-recorder fold sees neither the
// fold's conjoined label nor the other member's.
func TestProvenanceToolsMultiRecorderFoldShowsOnlyOwnMemberLabels(t *testing.T) {
	f := authorizedtest.New(t)
	f.Record(t, authorizedtest.RecorderSubject, mcpLabelledSession,
		authorizedtest.SecretLabels)
	const readerOwn shoal.ID = "interaction.session_mcp_reader_own"
	f.Record(t, authorizedtest.ReaderSubject, readerOwn, []string{"reader-own"})
	fold := f.Fold(t, authorizedtest.RecorderSubject, mcpLabelledSession, readerOwn)
	reader := mcpLabelServer(t, f, authorizedtest.ReaderSubject)
	listed := mcpLabelCall(t, reader, ToolProvenanceList, `{}`)
	unfolded := mcpLabelCall(t, reader, ToolProvenanceUnfold,
		`{"fold_id":"`+mcpLabelID(fold.FoldID)+`"}`)
	for name, body := range map[string][]byte{"list": listed, "unfold": unfolded} {
		if leaked := authorizedtest.Leaks(body); len(leaked) != 0 {
			t.Fatalf("multi-recorder %s leaked %v: %s", name, leaked, body)
		}
		if !bytes.Contains(body, []byte("reader-own")) {
			t.Fatalf("multi-recorder %s lost the reader's own label: %s", name, body)
		}
	}
}
