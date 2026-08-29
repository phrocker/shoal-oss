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

func TestStaticWorkspaceVectorModeBehavior(t *testing.T) {
	runNodeUITest(t, `
const unavailable = await runScenario({
  documents: true, document: true, retrieve: true, neighborhood: true, path: true,
});
assert.strictEqual(unavailable.ids["mode-vector"].disabled, true);
assert.strictEqual(unavailable.ids["mode-vector"].checked, false);
assert.match(unavailable.ids["vector-mode-status"].textContent, /did not advertise vector support/);
unavailable.ids["mode-vector"].checked = true;
unavailable.ids.query.value = "knowledge graph";
await unavailable.ids.search.onsubmit({preventDefault() {}});
assert.deepStrictEqual(unavailable.retrieveBody.query.modes, ["lexical", "tree", "graph"]);

const advertised = await runScenario({
  documents: true, document: true, retrieve: true, neighborhood: true, path: true,
  vector_retrieval: true,
});
assert.strictEqual(advertised.ids["mode-vector"].disabled, false);
assert.strictEqual(advertised.ids["vector-mode-status"].hidden, true);
advertised.ids["mode-vector"].checked = true;
advertised.ids.query.value = "knowledge graph";
await advertised.ids.search.onsubmit({preventDefault() {}});
assert.ok(advertised.retrieveBody.query.modes.includes("vector"));

const aliasOnly = await runScenario({
  documents: true, document: true, retrieve: true, neighborhood: true, path: true,
  vector: true, retrieve_vector: true,
});
assert.strictEqual(aliasOnly.ids["mode-vector"].disabled, true);
`)
}

func TestStaticWorkspaceSourceLabelsCollapseHostPaths(t *testing.T) {
	runNodeUITest(t, `
const fileURI = "file:///C:/Dev/shoal-web/docs/explorer-demo-guide.md";
const unixURI = "file:///home/build/shoal/docs/guide.md";
const webURI = "https://example.test/private/tree/guide.md?token=secret";
const scenario = await runScenario(
  {documents: true, document: true, retrieve: true, neighborhood: true, path: true},
  [{document: {id: "doc1", title: "Demo"}, revision: {id: "rev1"}, source_uri: fileURI}],
);
const card = scenario.ids.documents.children[0];
const button = card.children[0];
const details = card.children[1];
assert.strictEqual(button.children[1].textContent, "explorer-demo-guide.md");
assert.strictEqual(button.title, "explorer-demo-guide.md");
assert.ok(!button.children[1].textContent.includes("C:/Dev"));
assert.ok(!button.title.includes("C:/Dev"));
assert.strictEqual(details.children[1].textContent, fileURI);
assert.strictEqual(scenario.ctx.sourceLabel(unixURI, "doc"), "guide.md");
assert.strictEqual(scenario.ctx.sourceLabel(webURI, "doc"), "example.test / guide.md");
`)
}

func TestStaticWorkspaceResponsiveHeaderWrapsBeforeTabletWidths(t *testing.T) {
	html := readStaticAsset(t, "static/index.html")
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
	for _, noisy := range []string{
		`id="documents" aria-live`,
		`id="hierarchy" class="tree" aria-live`,
		`id="evidence" class="panel active" role="tabpanel" aria-labelledby="tab-evidence" aria-live`,
	} {
		if strings.Contains(html, noisy) {
			t.Fatalf("interactive content container should not be a broad live region: %q", noisy)
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

func runNodeUITest(t *testing.T, assertions string) {
	t.Helper()
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node is not available for executable static UI checks")
	}
	script := uiHarnessScript(assertions)
	command := exec.Command("node", "-e", script)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("node UI check failed: %v\n%s", err, output)
	}
}

func uiHarnessScript(assertions string) string {
	return `
const assert = require("assert");
const fs = require("fs");
const vm = require("vm");
const app = fs.readFileSync("static/app.js", "utf8") + "\nthis.sourceLabel = sourceLabel;";

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
  setAttribute(name, value) { this.attributes[name] = String(value); if (name === "aria-pressed") this.ariaPressed = String(value); }
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

function matches(element, selector) {
  if (selector === ".doc") return element.className.split(/\s+/).includes("doc");
  if (selector === ".doc-card") return element.className.split(/\s+/).includes("doc-card");
  if (selector === "[data-section]") return Boolean(element.dataset.section);
  if (selector === "button") return element.tagName === "BUTTON";
  return false;
}

function response(value) {
  return {ok: true, statusText: "OK", json: async () => value};
}

function makeDocument(capabilities, documents) {
  const ids = {};
  const pathFromLabel = new Element("label");
  const pathToLabel = new Element("label");
  pathFromLabel.attributes.for = "path-from";
  pathToLabel.attributes.for = "path-to";
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
    "documents", "documents-status", "more", "graph-nodes", "graph-edges", "canvas",
    "selection", "hierarchy", "hierarchy-status", "snapshot",
  ]) ids[id] = new Element(id === "canvas" ? "canvas" : "div", id);
  for (const id of ["documents-status", "hierarchy-status", "evidence-status", "vector-mode-status"]) {
    ids[id].setAttribute("role", "status");
  }
  ids.query.value = "";
  ids.search = new Element("form", "search");
  ids["search-button"] = new Element("button", "search-button");
  ids.modes = new Element("fieldset", "modes");
  for (const value of ["lexical", "tree", "graph", "vector"]) {
    const input = value === "vector" ? ids["mode-vector"] : new Element("input");
    input.value = value;
    input.checked = value !== "vector";
    inputs.push(input);
  }
  ids["mode-vector-control"] = new Element("label", "mode-vector-control");
  ids["vector-mode-status"] = new Element("span", "vector-mode-status");
  for (const name of ["evidence", "graph"]) panels[name] = new Element("div");
  for (const name of ["evidence", "graph"]) {
    tabs[name] = new Element("button");
    tabs[name].dataset.panel = name;
  }
  return {document, ids};
}

async function runScenario(capabilities, documents = []) {
  const dom = makeDocument(capabilities, documents);
  let retrieveBody = null;
  const snapshot = {id: "snapshot123456", as_of: "2026-08-29T00:00:00Z", frontier: 1};
  const ctx = {
    document: dom.document,
    ResizeObserver: class { constructor(callback) { this.callback = callback; } observe() {} },
    devicePixelRatio: 1,
    URL,
    fetch: async (url, options = {}) => {
      if (url === "/api/v1/meta") return response({capabilities});
      if (url.endsWith("/documents")) return response({snapshot, documents, next_cursor: ""});
      if (url.endsWith("/retrieve")) {
        retrieveBody = JSON.parse(options.body);
        return response({snapshot, retrieval: {results: []}});
      }
      throw new Error("unexpected fetch " + url);
    },
  };
  vm.createContext(ctx);
  vm.runInContext(app, ctx);
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  return {ctx, ids: dom.ids, get retrieveBody() { return retrieveBody; }};
}

(async () => {
` + assertions + `
})().catch((error) => { console.error(error.stack || error); process.exit(1); });
`
}
