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
	"os/exec"
	"strings"
	"testing"
)

func TestStaticWorkspaceSettingsLensControls(t *testing.T) {
	index := readStaticAsset(t, "static/index.html")
	script := readStaticAsset(t, "static/workspace-settings.js")
	style := readStaticAsset(t, "static/style.css")

	for _, marker := range []string{
		`id="workspace-settings-id"`,
		`id="workspace-settings-new"`,
		`id="workspace-settings-lens"`,
		`id="workspace-settings-refresh"`,
		`id="workspace-settings-apply"`,
		`id="workspace-settings-status"`,
	} {
		if !strings.Contains(index, marker) {
			t.Fatalf("workspace settings markup missing %q", marker)
		}
	}
	settingsScript := strings.Index(
		index, `<script src="/assets/workspace-settings.js"></script>`)
	appScript := strings.Index(index, `<script src="/assets/app.js"></script>`)
	if settingsScript < 0 || appScript < 0 || settingsScript > appScript {
		t.Fatal("workspace settings must establish request binding before app.js")
	}
	for _, marker := range []string{
		`Shoal-Workspace-ID`,
		`sessionStorage`,
		`/settings/lens`,
		`expected_revision`,
		`selected_ontology`,
		`crypto.getRandomValues`,
		`window.ShoalWorkspaceSettings`,
		`response.status === 404`,
	} {
		if !strings.Contains(script, marker) {
			t.Fatalf("workspace settings script missing %q", marker)
		}
	}
	for _, forbidden := range []string{
		"localStorage",
		".innerHTML",
		"document.write",
		"eval(",
	} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("workspace settings script must not use %q", forbidden)
		}
	}
	if !strings.Contains(style, ".workspace-settings-form") {
		t.Fatal("workspace settings controls are not styled")
	}
}

func TestStaticWorkspaceSettingsBindsOnlyApplicableRequests(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("node is required for executable static UI checks in CI")
		}
		t.Skip("node is not available for executable static UI checks")
	}
	script := `
const assert = require("assert");
const fs = require("fs");
const vm = require("vm");
const calls = [];
const storage = new Map([["shoal.workspace.id", "d29ya3NwYWNl"]]);
const window = {
  location: {
    href: "https://example.test/",
    origin: "https://example.test",
    reload() {},
  },
  crypto: {getRandomValues(bytes) { bytes.fill(7); return bytes; }},
  fetch: async (input, init) => {
    calls.push({input, init: init || {}});
    return {ok: true, status: 200, statusText: "OK"};
  },
  addEventListener() {},
  setTimeout(callback) { callback(); },
};
const context = {
  window,
  sessionStorage: {
    getItem(key) { return storage.get(key) || null; },
    setItem(key, value) { storage.set(key, value); },
    removeItem(key) { storage.delete(key); },
  },
  document: {},
  Headers,
  Request,
  URL,
  Uint8Array,
  atob,
  Object,
  console,
};
vm.createContext(context);
vm.runInContext(fs.readFileSync("static/workspace-settings.js", "utf8"), context);
(async () => {
  assert.strictEqual(
    window.ShoalWorkspaceSettings.selectedWorkspaceID(),
    "d29ya3NwYWNl",
  );
  await window.fetch("/api/v1/retrieve", {
    method: "POST",
    headers: {"content-type": "application/json"},
  });
  assert.strictEqual(
    calls[0].init.headers.get("Shoal-Workspace-ID"),
    "d29ya3NwYWNl",
  );
  await window.fetch(
    "/api/v1/workspaces/d29ya3NwYWNl/settings/lens",
    {headers: {"accept": "application/json"}},
  );
  assert.strictEqual(
    new Headers(calls[1].init.headers).has("Shoal-Workspace-ID"),
    false,
  );
  await window.fetch("https://other.test/api/v1/retrieve");
  assert.strictEqual(
    new Headers(calls[2].init.headers).has("Shoal-Workspace-ID"),
    false,
  );
  await window.fetch("/mcp", {method: "POST"});
  assert.strictEqual(
    calls[3].init.headers.get("Shoal-Workspace-ID"),
    "d29ya3NwYWNl",
  );
  await window.fetch("/api/v1/auth-config");
  assert.strictEqual(
    new Headers(calls[4].init.headers).has("Shoal-Workspace-ID"),
    false,
  );
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
`
	command := exec.Command("node", "-e", script)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("node workspace settings check failed: %v\n%s", err, output)
	}
}

func TestStaticWorkspaceSettingsLoadsLensAfterNewWorkspaceNotFound(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("node is required for executable static UI checks in CI")
		}
		t.Skip("node is not available for executable static UI checks")
	}
	script := `
const assert = require("assert");
const fs = require("fs");
const vm = require("vm");
const calls = [];
let ready;
function element() {
  return {
    value: "",
    disabled: false,
    className: "",
    textContent: "",
    options: [],
    addEventListener() {},
    replaceChildren() { this.options = []; },
    append(value) { this.options.push(value); },
  };
}
const elements = new Map([
  "workspace-settings-form",
  "workspace-settings-id",
  "workspace-settings-new",
  "workspace-settings-clear",
  "workspace-settings-lens",
  "workspace-settings-refresh",
  "workspace-settings-apply",
  "workspace-settings-status",
].map((id) => [id, element()]));
const window = {
  location: {
    href: "https://example.test/",
    origin: "https://example.test",
    reload() {},
  },
  crypto: {getRandomValues(bytes) { bytes.fill(7); return bytes; }},
  fetch: async (input, init) => {
    const url = new URL(String(input), "https://example.test/");
    calls.push(url.pathname);
    if (url.pathname === "/api/v1/identity") {
      return {ok: false, status: 404, statusText: "Not Found"};
    }
    if (url.pathname.endsWith("/settings/lens")) {
      return {
        ok: true,
        status: 200,
        statusText: "OK",
        async json() {
          return {
            workspace_id: "d29ya3NwYWNl",
            settings_revision: 0,
            active: {known: false, reading: "unresolved"},
            choices: [],
          };
        },
      };
    }
    return {ok: true, status: 200, statusText: "OK"};
  },
  addEventListener(name, callback) {
    if (name === "DOMContentLoaded") ready = callback;
  },
  setTimeout(callback) { callback(); },
};
const context = {
  window,
  sessionStorage: {
    getItem() { return "d29ya3NwYWNl"; },
    setItem() {},
    removeItem() {},
  },
  document: {
    getElementById(id) { return elements.get(id); },
    createElement() { return element(); },
  },
  Headers,
  Request,
  URL,
  Uint8Array,
  atob,
  Object,
  console,
};
vm.createContext(context);
vm.runInContext(fs.readFileSync("static/workspace-settings.js", "utf8"), context);
ready();
(async () => {
  await window.fetch("/api/v1/identity");
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepStrictEqual(calls, [
    "/api/v1/identity",
    "/api/v1/workspaces/d29ya3NwYWNl/settings/lens",
  ]);
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
`
	command := exec.Command("node", "-e", script)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("node new-workspace lens check failed: %v\n%s", err, output)
	}
}
