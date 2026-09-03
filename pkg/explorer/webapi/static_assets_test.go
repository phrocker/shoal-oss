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

func TestStaticWorkspaceAvoidsMarkupInjectionAPIs(t *testing.T) {
	app := readStaticAsset(t, "static/app.js")
	for _, forbidden := range []string{"innerHTML", "insertAdjacentHTML", "eval("} {
		if strings.Contains(app, forbidden) {
			t.Fatalf("app.js must not use %s with untrusted workspace data", forbidden)
		}
	}
}

func TestStaticWorkspaceUploadBehavior(t *testing.T) {
	runNodeUITest(t, `
const hidden = await runScenario({documents: true, document: true});
assert.strictEqual(hidden.ids["upload-section"].hidden, true);

const uploadedDocs = [{
  document: {id: "doc-upload", title: "guide.md"},
  revision: {id: "rev-upload"},
  source_uri: "upload://workspace/guide.md",
  source_media_type: "text/markdown",
}];
const uploaded = await runScenario(
  {documents: true, document: true, ingest: true},
  [],
  {
    documentSnapshot: {id: "uploadsnapshot", as_of: "2026-08-29T01:00:00Z", frontier: 2},
    documentResponses: [[], uploadedDocs],
    ingestResponse: {
      snapshot: {id: "uploadsnapshot", as_of: "2026-08-29T01:00:00Z", frontier: 2},
      files: [
        {name: "guide.md", disposition: "applied", media_type: "text/markdown", span_count: 1},
        {name: "main.go", disposition: "unchanged", media_type: "text/x-source-code", span_count: 1},
      ],
    },
  },
);
uploaded.ids["upload-files"].files = [{name: "guide.md"}, {name: "main.go"}];
await uploaded.ids.upload.onsubmit({preventDefault() {}});
assert.strictEqual(uploaded.uploadRequest.url, "/api/v1/ingest");
assert.strictEqual(uploaded.uploadRequest.options.method, "POST");
assert.strictEqual(uploaded.uploadRequest.options.headers["x-shoal-workspace-request"], "1");
assert.deepStrictEqual(uploaded.uploadRequest.options.body.parts.map((part) => part.filename), ["guide.md", "main.go"]);
assert.match(uploaded.ids["upload-status"].textContent, /Uploaded 2 file/);
assert.match(uploaded.ids["upload-results"].children[0].textContent, /guide\.md: applied/);
assert.match(uploaded.ids["upload-results"].children[1].textContent, /main\.go: unchanged/);
assert.strictEqual(uploaded.ids.documents.children.length, 1);
assert.match(uploaded.ids.snapshot.textContent, /uploadsnap/);

const failed = await runScenario(
  {documents: true, document: true, ingest: true},
  [],
  {ingestError: {statusText: "Bad Request", body: {message: "upload failed"}}},
);
failed.ids["upload-files"].files = [{name: "bad.exe"}];
await failed.ids.upload.onsubmit({preventDefault() {}});
assert.match(failed.ids["upload-status"].textContent, /upload failed/);
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

func TestStaticWorkspaceIdentityIsSurfaced(t *testing.T) {
	runNodeUITest(t, `
const scenario = await runScenario({documents: true, document: true, retrieve: true});
const badge = scenario.ids["identity-badge"];
assert.strictEqual(badge.hidden, false);
assert.match(badge.textContent, /development-principal@localhost/);
const panel = renderedText(scenario.ids.identity);
assert.match(panel, /You are/);
assert.match(panel, /development-principal@localhost/);
assert.match(panel, /Shared governed workspace/);
assert.match(panel, /retrieve/);
assert.match(panel, /neighborhood/);
assert.match(panel, /no client-side identity switch/);

const anon = await runScenario(
  {documents: true}, [], {identity: {authenticated: false, operations: []}});
assert.strictEqual(anon.ids["identity-badge"].hidden, true);
assert.match(renderedText(anon.ids.identity), /without an established identity/);

const broken = await runScenario(
  {documents: true}, [],
  {identityError: {statusText: "Service Unavailable", body: {code: "unavailable", message: "identity down"}}});
assert.strictEqual(broken.ids["identity-badge"].hidden, true);
assert.strictEqual(broken.ids.identity.className, "identity identity-unknown");
assert.match(renderedText(broken.ids.identity), /Identity is unavailable/);
`)
}

func TestStaticWorkspaceDistinguishesDeniedFromEmpty(t *testing.T) {
	runNodeUITest(t, `
const scenario = await runScenario({documents: true, retrieve: true});
assert.strictEqual(scenario.ctx.isDenied({code: "unauthorized"}), true);
assert.strictEqual(scenario.ctx.isDenied({status: 401}), true);
assert.strictEqual(scenario.ctx.isDenied({code: "not_found", status: 404}), false);
assert.strictEqual(scenario.ctx.isDenied({code: "internal", status: 500}), false);
assert.strictEqual(scenario.ctx.isDenied(null), false);

const identity = {authenticated: true, subject: "alice@localhost", operations: ["retrieve"]};
const denied = scenario.ctx.deniedText("run this retrieval", identity);
const empty = scenario.ctx.emptyRetrievalText(identity);
assert.notStrictEqual(denied, empty);
assert.match(denied, /Access denied/);
assert.match(denied, /not an empty result/);
assert.match(denied, /alice@localhost/);
assert.match(empty, /No evidence matched/);
assert.match(empty, /alice@localhost/);
`)
}

func TestStaticWorkspaceRetrievalDeniedStateRendersDistinctly(t *testing.T) {
	runNodeUITest(t, `
const denied = await runScenario(
  {documents: true, retrieve: true}, [],
  {retrieveError: {statusText: "Unauthorized", body: {code: "unauthorized", message: "operation retrieve is not authorized"}}});
denied.ids.query.value = "classified";
await denied.ids.search.onsubmit({preventDefault() {}});
assert.strictEqual(denied.ids["evidence-status"].className, "denied");
assert.match(denied.ids["evidence-status"].textContent, /Access denied/);
assert.strictEqual(denied.ids["evidence-results"].children.length, 0);

const empty = await runScenario({documents: true, retrieve: true});
empty.ids.query.value = "nothing here";
await empty.ids.search.onsubmit({preventDefault() {}});
assert.strictEqual(empty.ids["evidence-status"].className, "empty-state");
assert.match(empty.ids["evidence-status"].textContent, /No evidence matched/);
assert.notStrictEqual(
  empty.ids["evidence-status"].className, denied.ids["evidence-status"].className);

const failed = await runScenario(
  {documents: true, retrieve: true}, [],
  {retrieveError: {statusText: "Internal", body: {code: "internal", message: "boom"}}});
failed.ids.query.value = "boom";
await failed.ids.search.onsubmit({preventDefault() {}});
assert.strictEqual(failed.ids["evidence-status"].className, "error");
assert.match(failed.ids["evidence-status"].textContent, /boom/);
`)
}

func TestStaticWorkspaceEmptyDocumentsNamesTheIdentity(t *testing.T) {
	runNodeUITest(t, `
const scenario = await runScenario({documents: true, document: true});
assert.strictEqual(scenario.ids["documents-status"].className, "empty-state");
assert.match(scenario.ids["documents-status"].textContent, /No documents are visible/);
assert.match(scenario.ids["documents-status"].textContent, /development-principal@localhost/);
assert.match(scenario.ids["documents-status"].textContent, /No documents were withheld/);
`)
}

func TestStaticWorkspaceSuppressionClauseDistinguishesThreeCounts(t *testing.T) {
	runNodeUITest(t, `
const scenario = await runScenario({documents: true, retrieve: true});
const identity = {authenticated: true, subject: "alice@localhost", operations: ["retrieve"]};

const none = scenario.ctx.suppressionClause(0, identity);
assert.match(none, /No documents were withheld/);
assert.match(none, /alice@localhost/);
assert.doesNotMatch(none, /[1-9]/);

const one = scenario.ctx.suppressionClause(1, identity);
assert.match(one, /1 document is withheld/);
assert.match(one, /not a confirmation that nothing exists/);
assert.match(one, /alice@localhost/);

const many = scenario.ctx.suppressionClause(3, identity);
assert.match(many, /3 documents are withheld/);

// The three states must never render identically.
assert.notStrictEqual(none, one);
assert.notStrictEqual(one, many);
`)
}

func TestStaticWorkspaceRetrievalReportsWithheldContext(t *testing.T) {
	runNodeUITest(t, `
// State 1: results, nothing withheld.
const clean = await runScenario(
  {documents: true, retrieve: true}, [],
  {retrievalResults: [{id: "span-1", score: 0.9, evidence: []}], retrievalSuppressed: 0});
clean.ids.query.value = "alpha";
await clean.ids.search.onsubmit({preventDefault() {}});
assert.strictEqual(clean.ids["evidence-status"].className, "muted");
assert.match(clean.ids["evidence-status"].textContent, /Showing 1 evidence result/);
assert.match(clean.ids["evidence-status"].textContent, /No documents were withheld/);

// State 2: results, some withheld.
const some = await runScenario(
  {documents: true, retrieve: true}, [],
  {retrievalResults: [{id: "span-1", score: 0.9, evidence: []}], retrievalSuppressed: 2});
some.ids.query.value = "alpha";
await some.ids.search.onsubmit({preventDefault() {}});
assert.strictEqual(some.ids["evidence-status"].className, "withheld");
assert.match(some.ids["evidence-status"].textContent, /Showing 1 evidence result/);
assert.match(some.ids["evidence-status"].textContent, /2 documents are withheld/);
assert.strictEqual(some.ids["evidence-results"].children.length, 1);

// State 3: no results, yet withheld — the case that used to read as flat empty.
const only = await runScenario(
  {documents: true, retrieve: true}, [],
  {retrievalResults: [], retrievalSuppressed: 4});
only.ids.query.value = "alpha";
await only.ids.search.onsubmit({preventDefault() {}});
assert.strictEqual(only.ids["evidence-status"].className, "withheld");
assert.match(only.ids["evidence-status"].textContent, /No evidence matched/);
assert.match(only.ids["evidence-status"].textContent, /4 documents are withheld/);
assert.strictEqual(only.ids["evidence-results"].children.length, 0);

// The withheld state must be visually distinct from both a denial and a plain
// empty result.
assert.notStrictEqual(only.ids["evidence-status"].className, "denied");
assert.notStrictEqual(only.ids["evidence-status"].className, "empty-state");
`)
}

func TestStaticWorkspaceDocumentsReportWithheldContext(t *testing.T) {
	runNodeUITest(t, `
// No visible documents but some withheld.
const onlyWithheld = await runScenario(
  {documents: true, document: true}, [], {documentsSuppressed: 5});
assert.strictEqual(onlyWithheld.ids["documents-status"].className, "withheld");
assert.match(onlyWithheld.ids["documents-status"].textContent, /No documents are visible/);
assert.match(onlyWithheld.ids["documents-status"].textContent, /5 documents are withheld/);

// Visible documents plus some withheld.
const visibleDocs = [{
  document: {id: "doc-1", title: "Visible"},
  revision: {id: "rev-1"},
  source_uri: "file:///visible.txt",
}];
const mixed = await runScenario(
  {documents: true, document: true}, [],
  {documentResponses: [visibleDocs], documentsSuppressed: 1});
assert.strictEqual(mixed.ids["documents-status"].className, "withheld");
assert.match(mixed.ids["documents-status"].textContent, /Showing 1 document/);
assert.match(mixed.ids["documents-status"].textContent, /1 document is withheld/);
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
		// Asymmetric on purpose: a developer laptop without Node may skip these
		// executable UI checks, but CI must never let them silently vanish into
		// a green run. GitHub Actions always sets CI, so there a missing node is
		// a hard failure rather than a skip.
		if os.Getenv("CI") != "" {
			t.Fatal("node is required for executable static UI checks in CI")
		}
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
const app = fs.readFileSync("static/app.js", "utf8") +
  "\nthis.sourceLabel = sourceLabel;" +
  "\nthis.isDenied = isDenied;" +
  "\nthis.deniedText = deniedText;" +
  "\nthis.emptyRetrievalText = emptyRetrievalText;" +
  "\nthis.emptyDocumentsText = emptyDocumentsText;" +
  "\nthis.suppressionClause = suppressionClause;";

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
  return {
    ok: options.ok !== undefined ? options.ok : true,
    statusText: options.statusText || "OK",
    json: async () => value,
  };
}

class TestFormData {
  constructor() { this.parts = []; }
  append(name, file, filename) { this.parts.push({name, file, filename}); }
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
    "upload-section", "upload", "upload-drop", "upload-files", "upload-button",
    "upload-status", "upload-results",
    "documents", "documents-status", "more", "graph-nodes", "graph-edges", "canvas",
    "selection", "hierarchy", "hierarchy-status", "snapshot", "identity", "identity-badge",
  ]) ids[id] = new Element(id === "canvas" ? "canvas" : "div", id);
  for (const id of ["documents-status", "hierarchy-status", "evidence-status", "vector-mode-status", "upload-status", "identity"]) {
    ids[id].setAttribute("role", "status");
  }
  ids.query.value = "";
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
  ids["mode-vector-control"] = new Element("label", "mode-vector-control");
  ids["vector-mode-status"] = new Element("span", "vector-mode-status");
  for (const name of ["evidence", "graph"]) panels[name] = new Element("div");
  for (const name of ["evidence", "graph"]) {
    tabs[name] = new Element("button");
    tabs[name].dataset.panel = name;
  }
  return {document, ids};
}

async function runScenario(capabilities, documents = [], scenarioOptions = {}) {
  const dom = makeDocument(capabilities, documents);
  let retrieveBody = null;
  let uploadRequest = null;
  let documentCalls = 0;
  const snapshot = {id: "snapshot123456", as_of: "2026-08-29T00:00:00Z", frontier: 1};
  const ctx = {
    document: dom.document,
    FormData: TestFormData,
    ResizeObserver: class { constructor(callback) { this.callback = callback; } observe() {} },
    devicePixelRatio: 1,
    URL,
    fetch: async (url, requestOptions = {}) => {
      if (url === "/api/v1/meta") return response({capabilities});
      if (url === "/api/v1/identity") {
        if (scenarioOptions.identityError) {
          return response(scenarioOptions.identityError.body || {}, {
            ok: false,
            statusText: scenarioOptions.identityError.statusText || "Error",
          });
        }
        return response("identity" in scenarioOptions ? scenarioOptions.identity : {
          authenticated: true,
          subject: "development-principal@localhost",
          actor: "shoal-explore-web-dev-auth",
          authorization_domain: "shoal-explore-web",
          operations: ["ingest", "list", "read", "connect", "neighborhood", "retrieve"],
          policy_generation: 1,
          audit_purpose: "localhost development workspace",
          request_id: "dev-request-abc123",
        });
      }
      if (url.endsWith("/documents")) {
        const sequence = scenarioOptions.documentResponses || [documents];
        const docs = sequence[Math.min(documentCalls, sequence.length - 1)];
        const documentSnapshot = scenarioOptions.documentSnapshot || snapshot;
        documentCalls++;
        return response({
          snapshot: documentSnapshot, documents: docs, next_cursor: "",
          suppressed: scenarioOptions.documentsSuppressed || 0,
        });
      }
      if (url === "/api/v1/ingest") {
        uploadRequest = {url, options: requestOptions};
        if (scenarioOptions.ingestError) {
          return response(scenarioOptions.ingestError.body, {
            ok: false,
            statusText: scenarioOptions.ingestError.statusText,
          });
        }
        return response(scenarioOptions.ingestResponse);
      }
      if (url.endsWith("/retrieve")) {
        retrieveBody = JSON.parse(requestOptions.body);
        if (scenarioOptions.retrieveError) {
          return response(scenarioOptions.retrieveError.body || {}, {
            ok: false,
            statusText: scenarioOptions.retrieveError.statusText || "Error",
          });
        }
        return response({
          snapshot,
          retrieval: {results: scenarioOptions.retrievalResults || []},
          suppressed: scenarioOptions.retrievalSuppressed || 0,
        });
      }
      throw new Error("unexpected fetch " + url);
    },
  };
  vm.createContext(ctx);
  vm.runInContext(app, ctx);
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  return {
    ctx, ids: dom.ids,
    get retrieveBody() { return retrieveBody; },
    get uploadRequest() { return uploadRequest; },
  };
}

(async () => {
` + assertions + `
})().catch((error) => { console.error(error.stack || error); process.exit(1); });
`
}
