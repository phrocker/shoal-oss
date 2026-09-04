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
	"testing"
)

func TestStaticWorkspaceOntologyPanelStates(t *testing.T) {
	runOntologyUITest(t, `
const none = await runOntologyScenario({
  configured: false,
  identity: {known: false, reading: "unresolved"},
  schema: null,
  version: null,
  concepts: [],
  relationships: [],
  properties: [],
  limits: {},
});
assert.strictEqual(none.ids["ontology-status"].className, "empty-state");
assert.match(none.ids["ontology-status"].textContent, /No active ontology is configured/);
assert.match(none.ids["ontology-status"].textContent, /not an empty ontology/);
assert.strictEqual(none.ids["ontology-details"].children.length, 0);

const empty = await runOntologyScenario({
  configured: true,
  identity: {known: true, schema_id: "schema-empty", version_id: "version-empty", reading: "same_version"},
  schema: {id: "schema-empty", key: "empty", name: "Empty"},
  version: {id: "version-empty", version: "v1", created_at: "2026-09-04T00:00:00Z"},
  concepts: [],
  relationships: [],
  properties: [],
  limits: {},
});
assert.strictEqual(empty.ids["ontology-status"].className, "muted");
assert.match(empty.ids["ontology-status"].textContent, /0 concept/);
assert.match(renderedText(empty.ids["ontology-details"]), /Active identity/);
assert.match(renderedText(empty.ids["ontology-details"]), /no relationships/);
assert.notStrictEqual(empty.ids["ontology-status"].textContent, none.ids["ontology-status"].textContent);

const denied = await runOntologyScenario(null, {
  ontologyError: {
    status: 401,
    statusText: "Unauthorized",
    body: {code: "unauthorized", message: "operation describe ontology is not authorized"},
  },
});
assert.strictEqual(denied.ids["ontology-status"].className, "denied");
assert.match(denied.ids["ontology-status"].textContent, /Access denied/);
assert.match(denied.ids["ontology-status"].textContent, /not an empty result/);
assert.strictEqual(denied.ids["ontology-details"].children.length, 0);
`)
}

func TestStaticWorkspaceOntologyRendersDeclaredJoins(t *testing.T) {
	runOntologyUITest(t, `
const rich = await runOntologyScenario({
  configured: true,
  identity: {known: true, schema_id: "schema-rich", version_id: "version-rich", reading: "same_version"},
  schema: {id: "schema-rich", key: "workspace", name: "Workspace"},
  version: {id: "version-rich", version: "v1", created_at: "2026-09-04T00:00:00Z"},
  concepts: [
    {id: "concept-person", key: "person", name: "Person", properties: ["property-name"]},
    {id: "concept-org", key: "organization", name: "Organization", properties: ["property-name"]},
  ],
  relationships: [{
    id: "relationship-member-of",
    key: "member_of",
    name: "Member of",
    directed: true,
    from_concepts: ["concept-person"],
    to_concepts: ["concept-org"],
    properties: ["property-role"],
  }],
  properties: [
    {id: "property-name", key: "name", name: "Name", value_type: "string", constraints: [
      {kind: "required"},
      {kind: "allowed_values", allowed_values: [
        {type: "string", value: "Alice"},
        {type: "string", value: "Bob"},
      ]},
    ]},
    {id: "property-role", key: "role", name: "Role", value_type: "string", constraints: [
      {kind: "pattern", pattern: "^[A-Za-z ]+$"},
    ]},
  ],
  limits: {},
});
const text = renderedText(rich.ids["ontology-details"]);
assert.match(text, /Active identity/);
assert.match(text, /Declared joins/);
assert.match(text, /Person → Organization/);
assert.match(text, /Directed relationship/);
assert.match(text, /Role/);
assert.match(text, /required/);
assert.match(text, /allowed_values: string Alice, string Bob/);
`)
}

func runOntologyUITest(t *testing.T, assertions string) {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("node is required for executable ontology UI checks in CI")
		}
		t.Skip("node is not available for executable static UI checks")
	}
	command := exec.Command("node", "-e", ontologyHarnessScript(assertions))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("node ontology UI check failed: %v\n%s", err, output)
	}
}

func ontologyHarnessScript(assertions string) string {
	return `
const assert = require("assert");
const fs = require("fs");
const vm = require("vm");
const app = fs.readFileSync("static/app.js", "utf8");

class ClassList {
  constructor(element) { this.element = element; }
  add(name) { this.set(name, true); }
  remove(name) { this.set(name, false); }
  toggle(name, force) { this.set(name, force === undefined ? !this.has(name) : force); }
  has(name) { return this.element.className.split(/\s+/).includes(name); }
  set(name, enabled) {
    const values = new Set(this.element.className.split(/\s+/).filter(Boolean));
    if (enabled) values.add(name); else values.delete(name);
    this.element.className = [...values].join(" ");
  }
}

class Element {
  constructor(tag, id = "") {
    this.tagName = tag.toUpperCase();
    this.id = id;
    this.children = [];
    this.dataset = {};
    this.attributes = {};
    this.className = "";
    this.classList = new ClassList(this);
    this.textContent = "";
    this.hidden = false;
    this.disabled = false;
    this.checked = false;
    this.value = "";
    this.type = "";
    this.title = "";
    this.onclick = null;
    this.onsubmit = null;
    this.onkeydown = null;
  }
  append(...nodes) {
    for (const node of nodes.flat()) {
      if (node && node.tagName === "FRAGMENT") this.children.push(...node.children);
      else this.children.push(node);
    }
  }
  replaceChildren(...nodes) { this.children = []; this.append(...nodes); }
  setAttribute(name, value) { this.attributes[name] = String(value); }
  getAttribute(name) { return this.attributes[name]; }
  querySelector(selector) { return descendants(this).find((node) => matches(node, selector)) || null; }
  querySelectorAll(selector) { return descendants(this).filter((node) => matches(node, selector)); }
  getBoundingClientRect() { return {width: 0, height: 0, left: 0, top: 0}; }
  getContext() { return canvasContext; }
}

const canvasContext = {
  setTransform() {}, clearRect() {}, beginPath() {}, moveTo() {}, lineTo() {},
  stroke() {}, fillText() {}, arc() {}, fill() {},
};

function descendants(element) {
  const out = [];
  for (const child of element.children || []) {
    if (!child) continue;
    out.push(child, ...descendants(child));
  }
  return out;
}

function renderedText(element) {
  return [element, ...descendants(element)]
    .map((node) => (node && node.textContent) || "")
    .join(" ");
}

function matches(element, selector) {
  if (selector === ".doc") return element.className.split(/\s+/).includes("doc");
  if (selector === ".doc-card") return element.className.split(/\s+/).includes("doc-card");
  if (selector === "[data-section]") return Boolean(element.dataset.section);
  if (selector === "button") return element.tagName === "BUTTON";
  return false;
}

function response(value, options = {}) {
  const headers = options.headers || {};
  return {
    ok: options.ok !== undefined ? options.ok : true,
    status: options.status !== undefined
      ? options.status
      : (options.ok === false ? 400 : 200),
    statusText: options.statusText || "OK",
    headers: {get(name) {
      const found = headers[String(name).toLowerCase()];
      return found === undefined ? null : found;
    }},
    json: async () => value,
  };
}

class TestFormData {
  constructor() { this.parts = []; }
  append(name, file, filename) { this.parts.push({name, file, filename}); }
}

function makeDocument() {
  const ids = {};
  const pathFromLabel = new Element("label");
  const pathToLabel = new Element("label");
  const panels = {};
  const tabs = {};
  const inputs = [];
  const document = {
    getElementById(id) { return ids[id]; },
    createElement(tag) { return new Element(tag); },
    createDocumentFragment() { return new Element("fragment"); },
    querySelectorAll(selector) {
      if (selector === ".tab") return Object.values(tabs);
      if (selector === ".tab,.panel") return [...Object.values(tabs), ...Object.values(panels)];
      if (selector === "[for='path-from'],[for='path-to']") return [pathFromLabel, pathToLabel];
      if (selector === "#modes input:checked:not(:disabled)") {
        return inputs.filter((input) => input.checked && !input.disabled && !ids.modes.disabled);
      }
      if (selector === ".doc-card") return descendants(ids.documents).filter((node) => matches(node, selector));
      return [];
    },
    querySelector(selector) {
      const panel = selector.match(/^\[data-panel="(.+)"\]$/);
      if (panel) return tabs[panel[1]];
      return null;
    },
  };
  for (const id of [
    "query", "search", "search-button", "modes", "mode-vector", "mode-vector-control",
    "vector-mode-status", "evidence", "evidence-status", "evidence-results", "expand",
    "continue-expansion", "path-from", "path-to", "find-path", "graph-status",
    "upload-section", "upload", "upload-drop", "upload-files", "upload-button",
    "upload-status", "upload-results", "documents", "documents-status", "more",
    "graph-nodes", "graph-edges", "canvas", "selection", "hierarchy",
    "hierarchy-status", "snapshot", "identity", "identity-badge", "ontology",
    "ontology-status", "ontology-details", "login",
  ]) ids[id] = new Element(id === "canvas" ? "canvas" : "div", id);
  for (const id of [
    "documents-status", "hierarchy-status", "evidence-status", "vector-mode-status",
    "upload-status", "identity", "ontology-status",
  ]) ids[id].setAttribute("role", "status");
  ids.search = new Element("form", "search");
  ids.upload = new Element("form", "upload");
  ids["upload-files"] = new Element("input", "upload-files");
  ids["upload-button"] = new Element("button", "upload-button");
  ids["upload-drop"] = new Element("label", "upload-drop");
  ids["search-button"] = new Element("button", "search-button");
  ids.modes = new Element("fieldset", "modes");
  for (const value of ["lexical", "tree", "graph", "vector"]) {
    const input = value === "vector" ? ids["mode-vector"] : new Element("input");
    input.value = value;
    input.checked = value !== "vector";
    inputs.push(input);
  }
  for (const name of ["evidence", "graph", "ontology"]) panels[name] = ids[name];
  for (const name of ["evidence", "graph", "ontology"]) {
    tabs[name] = new Element("button");
    tabs[name].dataset.panel = name;
  }
  return {document, ids};
}

async function runOntologyScenario(ontology, options = {}) {
  const dom = makeDocument();
  const snapshot = {id: "snapshot123456", as_of: "2026-09-04T00:00:00Z", frontier: 1};
  const ctx = {
    document: dom.document,
    FormData: TestFormData,
    ResizeObserver: class { observe() {} },
    devicePixelRatio: 1,
    URL,
    crypto: require("crypto").webcrypto,
    TextEncoder,
    fetch: async (url) => {
      if (url === "/api/v1/auth-config") return response({configured: false});
      if (url === "/api/v1/identity") return response({
        authenticated: true,
        subject: "development-principal@localhost",
        operations: ["retrieve"],
      });
      if (url === "/api/v1/ontology") {
        if (options.ontologyError) {
          const failure = options.ontologyError;
          return response(failure.body || {}, {
            ok: false,
            status: failure.status,
            statusText: failure.statusText || "Error",
            headers: failure.wwwAuthenticate ? {"www-authenticate": "Bearer"} : {},
          });
        }
        return response(ontology);
      }
      if (url === "/api/v1/meta") return response({capabilities: {documents: true}});
      if (url.endsWith("/documents")) return response({snapshot, documents: [], next_cursor: ""});
      throw new Error("unexpected fetch " + url);
    },
  };
  vm.createContext(ctx);
  vm.runInContext(app, ctx);
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  return {ctx, ids: dom.ids};
}

(async () => {
` + assertions + `
})().catch((error) => { console.error(error.stack || error); process.exit(1); });
`
}
