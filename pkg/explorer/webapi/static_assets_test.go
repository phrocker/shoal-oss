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

package webapi_test

import (
	"os"
	"strings"
	"testing"
)

func TestStaticWorkspaceGatesVectorModeFailClosed(t *testing.T) {
	html := readStaticAsset(t, "static/index.html")
	app := readStaticAsset(t, "static/app.js")

	if !strings.Contains(html, `id="mode-vector"`) ||
		!strings.Contains(html, `id="vector-mode-status"`) {
		t.Fatal("vector mode control or explanatory status is missing")
	}
	for _, want := range []string{
		"vector_retrieval: false",
		"state.capabilities[name] === true",
		"checkbox.checked = false",
		"did not advertise vector support",
		"#modes input:checked:not(:disabled)",
	} {
		if !strings.Contains(app, want) {
			t.Fatalf("app.js missing vector gating marker %q", want)
		}
	}
}

func TestStaticWorkspaceDoesNotShowRawSourceURIsByDefault(t *testing.T) {
	app := readStaticAsset(t, "static/app.js")

	for _, want := range []string{
		"function sourceLabel",
		"sourceLabel(sourceURI, documentID)",
		`summary.textContent = "Full source URI"`,
		"code.textContent = sourceURI",
	} {
		if !strings.Contains(app, want) {
			t.Fatalf("app.js missing source-label marker %q", want)
		}
	}
	if strings.Contains(app, "item.source_uri || item.document.id") {
		t.Fatal("document cards still render the raw source URI as the default label")
	}
}

func TestStaticWorkspaceResponsiveHeaderWrapsBeforeTabletWidths(t *testing.T) {
	style := readStaticAsset(t, "static/style.css")

	for _, want := range []string{
		"header{min-height:58px;display:flex",
		"flex-wrap:wrap",
		"form{display:flex",
		"flex:1 1 36rem",
		"@media(max-width:1100px)",
		"form{flex-basis:100%;max-width:none}",
	} {
		if !strings.Contains(style, want) {
			t.Fatalf("style.css missing responsive layout marker %q", want)
		}
	}
}

func readStaticAsset(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
