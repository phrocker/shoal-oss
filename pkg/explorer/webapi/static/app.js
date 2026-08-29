const state = {
  snapshot: {},
  cursor: "",
  document: null,
  currentDocumentID: "",
  selectedSectionID: "",
  nodes: new Map(),
  edges: new Map(),
  graphCursors: new Map(),
  sourceURIs: new Map(),
  selected: null,
  capabilities: {
    documents: false,
    document: false,
    retrieve: false,
    neighborhood: false,
    path: false,
    vector_retrieval: false,
    ingest: false,
  },
  uploadLimits: {
    max_upload_files: 0,
    max_upload_file_bytes: 0,
    max_upload_total_bytes: 0,
  },
};
const $ = (id) => document.getElementById(id);
let documentGeneration = 0;
let searchGeneration = 0;
let documentsGeneration = 0;
let graphGeneration = 0;
let documentsLoading = false;
let documentsRefreshQueued = false;
let uploadLoading = false;

async function api(path, body) {
  const response = await fetch(`/api/v1/${path}`, {
    method: "POST",
    headers: {"content-type": "application/json"},
    body: JSON.stringify(body),
  });
  const value = await response.json();
  if (!response.ok) throw new Error(value.message || response.statusText);
  return value;
}

async function loadMeta() {
  const response = await fetch("/api/v1/meta", {headers: {"accept": "application/json"}});
  const value = await response.json();
  if (!response.ok) throw new Error(value.message || response.statusText);
  state.capabilities = {...state.capabilities, ...(value.capabilities || {})};
  state.uploadLimits = {
    max_upload_files: value.max_upload_files || 0,
    max_upload_file_bytes: value.max_upload_file_bytes || 0,
    max_upload_total_bytes: value.max_upload_total_bytes || 0,
  };
  applyCapabilities();
}

function capability(name) {
  return state.capabilities[name] === true;
}

function vectorRetrievalCapability() {
  return capability("vector_retrieval");
}

function applyCapabilities() {
  const canRetrieve = capability("retrieve");
  $("query").disabled = !canRetrieve;
  $("search-button").disabled = !canRetrieve;
  $("modes").disabled = !canRetrieve;
  applyVectorCapability(canRetrieve);

  const evidenceStatus = $("evidence-status");
  if (!canRetrieve) {
    evidenceStatus.dataset.capabilityPlaceholder = "retrieve";
    setStatus(evidenceStatus, "Retrieval is unavailable from this Explorer service.");
    $("evidence-results").replaceChildren();
  } else if (evidenceStatus.dataset.capabilityPlaceholder === "retrieve") {
    delete evidenceStatus.dataset.capabilityPlaceholder;
    setStatus(evidenceStatus, "Run a retrieval query to inspect exact evidence and explanations.");
  }

  const canNeighborhood = capability("neighborhood");
  $("expand").hidden = !canNeighborhood;
  $("expand").disabled = !canNeighborhood || !state.selected;
  updateContinueButton();

  const canPath = capability("path");
  document.querySelectorAll("[for='path-from'],[for='path-to']").forEach((label) => {
    label.hidden = !canPath;
  });
  $("path-from").hidden = !canPath;
  $("path-to").hidden = !canPath;
  $("find-path").hidden = !canPath;
  $("find-path").disabled = !canPath;
  const graphStatus = $("graph-status");
  if (!canNeighborhood && !canPath) {
    graphStatus.dataset.capabilityPlaceholder = "graph";
    graphStatus.textContent =
      "Graph expansion and path finding are unavailable from this Explorer service.";
  } else if (graphStatus.dataset.capabilityPlaceholder === "graph") {
    delete graphStatus.dataset.capabilityPlaceholder;
    graphStatus.textContent = "No graph expansion yet.";
  }

  if (!capability("documents")) {
    setStatus($("documents-status"), "Document listing is unavailable from this Explorer service.");
    $("documents").replaceChildren();
    $("more").hidden = true;
  }
  applyIngestCapability();
}

function applyIngestCapability() {
  const canIngest = capability("ingest");
  $("upload-section").hidden = !canIngest;
  $("upload-files").disabled = !canIngest || uploadLoading;
  $("upload-button").disabled = !canIngest || uploadLoading;
  if (!canIngest) {
    $("upload-results").replaceChildren();
    setStatus($("upload-status"), "Uploads are unavailable from this Explorer service.");
  } else if (!$("upload-status").textContent) {
    setStatus($("upload-status"), uploadLimitText());
  }
}

function uploadLimitText() {
  const files = state.uploadLimits.max_upload_files || "bounded";
  const bytes = state.uploadLimits.max_upload_file_bytes || 0;
  const size = bytes ? `${Math.floor(bytes / 1024)} KiB` : "bounded";
  return `Upload up to ${files} file(s), ${size} each.`;
}

function applyVectorCapability(canRetrieve) {
  const checkbox = $("mode-vector");
  const control = $("mode-vector-control");
  const status = $("vector-mode-status");
  const canVector = vectorRetrievalCapability();
  checkbox.disabled = !canRetrieve || !canVector;
  control.classList.toggle("unavailable", !canVector);
  control.setAttribute("aria-disabled", String(!canVector));
  if (!canVector) {
    checkbox.checked = false;
    status.hidden = false;
    status.textContent =
      "Vector retrieval is disabled because this Explorer service did not advertise vector support.";
  } else {
    status.hidden = true;
    status.textContent = "";
  }
}

function setMessage(element, message, className = "muted") {
  const paragraph = document.createElement("p");
  paragraph.className = className;
  paragraph.textContent = message;
  element.replaceChildren(paragraph);
}

function setStatus(element, message, className = "muted") {
  element.className = className;
  element.textContent = message;
}

function showError(element, error) {
  if (element.getAttribute && element.getAttribute("role") === "status") {
    setStatus(element, error.message || String(error), "error");
    return;
  }
  setMessage(element, error.message || String(error), "error");
}

function pin(snapshot) {
  state.snapshot = snapshot;
  $("snapshot").textContent =
    `snapshot ${String(snapshot.id || "").slice(0, 10)} · frontier ${snapshot.frontier}`;
}

async function loadDocuments(reset = true) {
  if (!capability("documents")) return;
  if (documentsLoading) {
    if (reset) {
      documentsRefreshQueued = true;
      documentsGeneration++;
    }
    return;
  }
  const generation = ++documentsGeneration;
  documentsLoading = true;
  $("more").disabled = true;
  $("documents").setAttribute("aria-busy", "true");
  if (reset) {
    state.cursor = "";
    setStatus($("documents-status"), "Loading documents…");
  }
  try {
    const response = await api("documents", {
      snapshot: state.snapshot,
      page: {limit: 25, cursor: state.cursor},
    });
    if (generation !== documentsGeneration) return;
    pin(response.snapshot);
    if (reset) $("documents").replaceChildren();
    const documents = response.documents || [];
    if (documents.length === 0 && reset) {
      setStatus($("documents-status"), "No documents have been ingested yet.", "empty-state");
    } else {
      setStatus($("documents-status"), `Showing ${$("documents").children.length + documents.length} document(s).`);
    }
    const fragment = document.createDocumentFragment();
    for (const item of documents) fragment.append(createDocumentCard(item));
    $("documents").append(fragment);
    state.cursor = response.next_cursor || "";
    $("more").hidden = !state.cursor;
  } catch (error) {
    if (generation === documentsGeneration) showError($("documents-status"), error);
  } finally {
    documentsLoading = false;
    $("documents").setAttribute("aria-busy", "false");
    $("more").disabled = false;
    if (documentsRefreshQueued) {
      documentsRefreshQueued = false;
      loadDocuments(true);
    }
  }
}

async function uploadFiles(fileList) {
  if (!capability("ingest") || uploadLoading) return;
  const files = [...(fileList || [])];
  if (files.length === 0) {
    setStatus($("upload-status"), "Choose at least one file to upload.", "empty-state");
    return;
  }
  uploadLoading = true;
  applyIngestCapability();
  $("upload-results").replaceChildren();
  setStatus($("upload-status"), `Uploading ${files.length} file(s)…`);
  try {
    const body = new FormData();
    for (const file of files) body.append("files", file, file.name);
    const response = await fetch("/api/v1/ingest", {
      method: "POST",
      headers: {"accept": "application/json", "x-shoal-workspace-request": "1"},
      body,
    });
    const value = await response.json();
    if (!response.ok) throw new Error(value.message || response.statusText);
    clearSnapshotDependentViews();
    pin(value.snapshot);
    renderUploadResults(value.files || []);
    await loadDocuments(true);
  } catch (error) {
    showError($("upload-status"), error);
    clearSnapshotDependentViews();
    clearPinnedSnapshot();
    await loadDocuments(true);
  } finally {
    uploadLoading = false;
    applyIngestCapability();
    $("upload-files").value = "";
  }
}

function clearPinnedSnapshot() {
  state.snapshot = {};
  $("snapshot").textContent = "snapshot latest";
}

function clearSnapshotDependentViews() {
  documentGeneration++;
  searchGeneration++;
  graphGeneration++;
  state.document = null;
  state.currentDocumentID = "";
  state.selectedSectionID = "";
  state.selected = null;
  state.nodes.clear();
  state.edges.clear();
  state.graphCursors.clear();
  positions.clear();
  $("hierarchy").replaceChildren();
  setStatus($("hierarchy-status"), "Select a document.");
  $("evidence-results").replaceChildren();
  setStatus($("evidence-status"), "Run a retrieval query to inspect exact evidence and explanations.");
  $("graph-status").textContent = "No graph expansion yet.";
  $("selection").className = "muted";
  setMessage($("selection"), "Select evidence, a hierarchy section, or a graph node.");
  renderGraphList();
  updateContinueButton();
  draw();
}

function renderUploadResults(files) {
  const results = $("upload-results");
  results.replaceChildren();
  if (files.length === 0) {
    setStatus($("upload-status"), "No files were ingested.", "empty-state");
    return;
  }
  setStatus($("upload-status"), `Uploaded ${files.length} file(s).`);
  for (const file of files) {
    const item = document.createElement("li");
    item.textContent = `${file.name}: ${file.disposition} (${file.media_type}, ` +
      `${file.span_count} span(s))`;
    results.append(item);
  }
}

$("upload").onsubmit = async (event) => {
  event.preventDefault();
  await uploadFiles($("upload-files").files);
};

$("upload-files").onchange = () => {
  const count = $("upload-files").files ? $("upload-files").files.length : 0;
  setStatus(
    $("upload-status"),
    count ? `${count} file(s) ready to upload.` : uploadLimitText(),
  );
};

$("upload-drop").ondragover = (event) => {
  event.preventDefault();
  if (capability("ingest")) $("upload-drop").classList.add("dragging");
};

$("upload-drop").ondragleave = () => {
  $("upload-drop").classList.remove("dragging");
};

$("upload-drop").ondrop = async (event) => {
  event.preventDefault();
  $("upload-drop").classList.remove("dragging");
  await uploadFiles(event.dataTransfer && event.dataTransfer.files);
};

function createDocumentCard(item) {
  const documentValue = item.document || {};
  const title = documentValue.title || "(untitled document)";
  const documentID = documentValue.id || "";
  const sourceURI = item.source_uri || "";
  const sourceMediaType = item.source_media_type || "";
  if (documentID) state.sourceURIs.set(documentID, sourceURI);

  const card = document.createElement("div");
  card.className = "doc-card";
  card.dataset.document = documentID;

  const button = document.createElement("button");
  button.className = "doc";
  button.type = "button";
  button.disabled = !capability("document");
  button.title = sourceLabel(sourceURI, documentID);
  button.onclick = () => loadDocument(documentID, item.revision && item.revision.id);

  const titleElement = document.createElement("strong");
  titleElement.textContent = title;
  const sourceElement = document.createElement("small");
  sourceElement.className = "source-label";
  sourceElement.textContent = sourceLabel(sourceURI, documentID);
  button.append(titleElement, sourceElement);
  if (sourceMediaType) {
    const media = document.createElement("span");
    media.className = "media-badge";
    media.textContent = sourceMediaType;
    button.append(media);
  }
  card.append(button);

  if (sourceURI) {
    const details = document.createElement("details");
    details.className = "source-details";
    const summary = document.createElement("summary");
    summary.textContent = "Full source URI";
    const code = document.createElement("code");
    code.textContent = sourceURI;
    details.append(summary, code);
    card.append(details);
  }
  updateDocumentCardSelection(card);
  return card;
}

function sourceLabel(uri, fallback = "") {
  const value = String(uri || "").trim();
  if (!value) return fallback || "source unavailable";
  try {
    const parsed = new URL(value);
    if (parsed.protocol === "file:") return basename(parsed.pathname) || "local file";
    if (parsed.protocol === "http:" || parsed.protocol === "https:") {
      const leaf = basename(parsed.pathname);
      return leaf ? `${parsed.hostname} / ${leaf}` : parsed.hostname;
    }
  } catch (_) {
    // Fall through to generic path handling.
  }
  return basename(value) || value;
}

function basename(value) {
  const withoutQuery = String(value || "").split(/[?#]/, 1)[0];
  const normalized = withoutQuery.replace(/\\/g, "/").replace(/\/+$/, "");
  const name = normalized.slice(normalized.lastIndexOf("/") + 1);
  try {
    return decodeURIComponent(name);
  } catch (_) {
    return name;
  }
}

async function loadDocument(documentID, revisionID) {
  if (!capability("document")) return;
  const generation = ++documentGeneration;
  state.currentDocumentID = documentID;
  state.document = null;
  state.selectedSectionID = "";
  updateDocumentCardSelection();
  $("hierarchy").replaceChildren();
  $("hierarchy").setAttribute("aria-busy", "true");
  setStatus($("hierarchy-status"), "Loading document hierarchy…");
  try {
    const response = await api("document", {
      snapshot: state.snapshot,
      document_id: documentID,
      revision_id: revisionID,
    });
    if (generation !== documentGeneration) return;
    pin(response.snapshot);
    state.document = response.document;
    state.sourceURIs.set(response.document.document.id, response.document.source_uri || "");
    renderHierarchy(response.document.root);
    mergeGraph({nodes: [nodeFromDocument(response.document.document)], edges: []});
    draw();
  } catch (error) {
    if (generation === documentGeneration) showError($("hierarchy-status"), error);
  } finally {
    if (generation === documentGeneration) $("hierarchy").setAttribute("aria-busy", "false");
  }
}

function updateDocumentCardSelection(card) {
  const cards = card ? [card] : [...document.querySelectorAll(".doc-card")];
  for (const candidate of cards) {
    const selected = candidate.dataset.document === state.currentDocumentID;
    candidate.classList.toggle("selected", selected);
    const button = candidate.querySelector(".doc");
    if (button) button.setAttribute("aria-current", selected ? "true" : "false");
  }
}

function renderHierarchy(root) {
  const hierarchy = $("hierarchy");
  if (!root) {
    hierarchy.replaceChildren();
    setStatus($("hierarchy-status"), "This document has no hierarchy.", "empty-state");
    return;
  }
  setStatus($("hierarchy-status"), "Document hierarchy loaded.");
  hierarchy.replaceChildren(sectionList(root));
}

function sectionList(view) {
  const list = document.createElement("ul");
  const item = document.createElement("li");
  const button = document.createElement("button");
  button.className = "section-node";
  button.type = "button";
  button.dataset.section = view.section.id;
  button.textContent = view.section.heading || "(untitled)";
  button.setAttribute("aria-pressed", String(view.section.id === state.selectedSectionID));
  button.onclick = () => selectSection(view.section.id);
  item.append(button);
  for (const span of view.spans || []) {
    const snippet = document.createElement("div");
    snippet.className = "citation span-snippet";
    snippet.textContent = span.text || "";
    item.append(snippet);
  }
  for (const child of view.children || []) item.append(sectionList(child));
  list.append(item);
  return list;
}

function selectSection(id) {
  if (!state.document || !state.document.root) return;
  const find = (view) =>
    view.section.id === id ? view : (view.children || []).map(find).find(Boolean);
  const view = find(state.document.root);
  if (!view) return;
  state.selectedSectionID = id;
  updateSectionSelection();
  showSelection([
    ["Section", view.section.heading || "(untitled)"],
    ["ID", id],
    ["Range", `${view.section.range.start.offset}–${view.section.range.end.offset}`],
  ]);
  if (capability("neighborhood")) expandIDs([id]);
}

function updateSectionSelection() {
  $("hierarchy").querySelectorAll("[data-section]").forEach((element) => {
    element.setAttribute(
      "aria-pressed",
      String(element.dataset.section === state.selectedSectionID),
    );
  });
}

function nodeFromDocument(documentValue) {
  return {
    id: documentValue.id,
    kind: "document",
    labels: [documentValue.title],
    properties: documentValue.metadata || {},
  };
}

$("search").onsubmit = async (event) => {
  event.preventDefault();
  if (!capability("retrieve")) return;
  const generation = ++searchGeneration;
  setStatus($("evidence-status"), "Searching evidence…");
  $("evidence-results").replaceChildren();
  try {
    const response = await api("retrieve", {
      snapshot: state.snapshot,
      query: {
        text: $("query").value,
        top_k: 20,
        modes: [...document.querySelectorAll("#modes input:checked:not(:disabled)")].map(
          (input) => input.value,
        ),
        explain: true,
        as_of: state.snapshot.as_of,
      },
    });
    if (generation !== searchGeneration) return;
    pin(response.snapshot);
    renderEvidence(response.retrieval);
  } catch (error) {
    if (generation === searchGeneration) showError($("evidence-status"), error);
  }
};

function renderEvidence(response) {
  const results = response.results || [];
  if (results.length === 0) {
    $("evidence-results").replaceChildren();
    setStatus($("evidence-status"), "No evidence matched.", "empty-state");
    draw();
    return;
  }
  setStatus($("evidence-status"), `Showing ${results.length} evidence result(s).`);
  const fragment = document.createDocumentFragment();
  results.forEach((result) => {
    const element = document.createElement("article");
    element.className = "result";

    const header = document.createElement("div");
    header.className = "result-header";
    const title = document.createElement("b");
    title.className = "result-title";
    title.textContent = result.id;
    title.title = result.id;
    const score = document.createElement("span");
    score.className = "score";
    score.textContent = Number(result.score).toFixed(3);
    header.append(title, score);
    element.append(header);

    (result.evidence || []).forEach((item) => {
      const quote = document.createElement("button");
      quote.className = "evidence-quote";
      quote.type = "button";
      quote.textContent = item.quote || "";
      quote.onclick = () => selectEvidence(result, item);
      const citation = document.createElement("div");
      citation.className = "citation";
      citation.textContent = `${item.citation.document_id} / ${item.citation.revision_id} / bytes ` +
        `${item.citation.range.start.offset}–${item.citation.range.end.offset}`;
      element.append(quote, citation);
      mergeGraph(item.path || {});
    });

    if (result.explanation) {
      const explanation = document.createElement("pre");
      explanation.className = "explanation";
      explanation.textContent = `${result.explanation.summary || ""}\n` +
        JSON.stringify(result.explanation.scores || {}, null, 2);
      element.append(explanation);
    }
    fragment.append(element);
  });
  $("evidence-results").replaceChildren(fragment);
  draw();
}

function selectEvidence(result, evidence) {
  const source = state.sourceURIs.get(evidence.citation.document_id) || "";
  showSelection([
    ["Result", result.id],
    ["Quote", evidence.quote],
    ["Document", sourceLabel(source, evidence.citation.document_id)],
    ["Section", evidence.citation.section_id],
    ["Span", evidence.citation.span_id],
  ]);
  loadDocument(evidence.citation.document_id, evidence.citation.revision_id);
}

function showSelection(entries) {
  const container = document.createElement("div");
  container.className = "kv";
  for (const [key, value] of entries) {
    const label = document.createElement("b");
    label.textContent = key;
    const text = document.createElement("span");
    text.textContent = value == null ? "" : String(value);
    text.title = text.textContent;
    container.append(label, text);
  }
  $("selection").classList.remove("muted");
  $("selection").replaceChildren(container);
}

function mergeGraph(graph) {
  const previousNodeCount = state.nodes.size;
  for (const node of graph.nodes || []) state.nodes.set(node.id, node);
  for (const edge of graph.edges || []) state.edges.set(edge.id, edge);
  if (state.nodes.size !== previousNodeCount) positions.clear();
  renderGraphList();
}

async function expandIDs(ids, cursor = "") {
  if (!capability("neighborhood")) return;
  const generation = graphGeneration;
  $("graph-status").textContent = cursor ? "Loading more neighbors…" : "Expanding graph…";
  try {
    const response = await api("neighborhood", {
      snapshot: state.snapshot,
      node_ids: ids,
      depth: 1,
      fanout: 18,
      max_nodes: 120,
      cursor,
    });
    if (generation !== graphGeneration) return;
    pin(response.snapshot);
    mergeGraph(response.neighborhood);
    if (ids.length === 1) {
      if (response.next_cursor) state.graphCursors.set(ids[0], response.next_cursor);
      else state.graphCursors.delete(ids[0]);
      updateContinueButton();
    }
    $("graph-status").textContent = response.truncated
      ? `Bounded result truncated at snapshot frontier ${response.snapshot.frontier}.`
      : `Complete requested expansion at snapshot frontier ${response.snapshot.frontier}.`;
    activate("graph");
    draw();
  } catch (error) {
    if (generation !== graphGeneration) return;
    $("graph-status").textContent = `Graph expansion failed: ${error.message || String(error)}`;
    showError($("selection"), error);
  }
}

$("expand").onclick = () => state.selected && expandIDs([state.selected]);
$("continue-expansion").onclick = () => {
  const cursor = state.graphCursors.get(state.selected);
  if (cursor) expandIDs([state.selected], cursor);
};
$("more").onclick = () => loadDocuments(false);
$("find-path").onclick = async () => {
  if (!capability("path")) return;
  const generation = graphGeneration;
  $("graph-status").textContent = "Finding directed path…";
  try {
    const response = await api("path", {
      snapshot: state.snapshot,
      from: $("path-from").value,
      to: $("path-to").value,
      max_depth: 4,
      fanout: 25,
    });
    if (generation !== graphGeneration) return;
    pin(response.snapshot);
    mergeGraph(response.path);
    $("graph-status").textContent =
      `Directed path at snapshot frontier ${response.snapshot.frontier}.`;
    draw();
  } catch (error) {
    if (generation !== graphGeneration) return;
    $("graph-status").textContent = `Path finding failed: ${error.message || String(error)}`;
    showError($("selection"), error);
  }
};

document.querySelectorAll(".tab").forEach((tab) => {
  tab.onclick = () => activate(tab.dataset.panel);
  tab.onkeydown = (event) => {
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    const tabs = [...document.querySelectorAll(".tab")];
    const offset = event.key === "ArrowRight" ? 1 : -1;
    const target = tabs[(tabs.indexOf(tab) + offset + tabs.length) % tabs.length];
    activate(target.dataset.panel);
    target.focus();
  };
});

function activate(id) {
  document.querySelectorAll(".tab,.panel").forEach((element) => {
    element.classList.remove("active");
  });
  document.querySelectorAll(".tab").forEach((tab) => {
    const selected = tab.dataset.panel === id;
    tab.setAttribute("aria-selected", String(selected));
    tab.tabIndex = selected ? 0 : -1;
  });
  document.querySelector(`[data-panel="${id}"]`).classList.add("active");
  $(id).classList.add("active");
  if (id === "graph") draw();
}

function renderGraphList() {
  const nodesElement = $("graph-nodes");
  const edgesElement = $("graph-edges");
  nodesElement.replaceChildren();
  edgesElement.replaceChildren();
  if (state.nodes.size === 0) {
    setMessage(nodesElement, "No graph nodes yet.", "empty-state");
  } else {
    for (const node of state.nodes.values()) {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = nodeName(node, node.id);
      button.title = button.textContent;
      button.setAttribute("aria-label", `${button.textContent}, node ${node.id}`);
      button.setAttribute("aria-pressed", String(node.id === state.selected));
      button.onclick = () => selectNode(node.id);
      nodesElement.append(button);
    }
  }
  if (state.edges.size === 0) {
    const item = document.createElement("li");
    item.className = "muted";
    item.textContent = "No graph edges yet.";
    edgesElement.append(item);
  } else {
    for (const edge of state.edges.values()) {
      const item = document.createElement("li");
      const from = state.nodes.get(edge.from);
      const to = state.nodes.get(edge.to);
      item.textContent = `${nodeName(from, edge.from)} → ${nodeName(to, edge.to)} (${edge.type})`;
      item.setAttribute(
        "aria-label",
        `${nodeName(from, edge.from)} ${edge.from} to ${nodeName(to, edge.to)} ${edge.to}, ${edge.type}`,
      );
      edgesElement.append(item);
    }
  }
}

function nodeName(node, fallback) {
  return node ? ((node.labels && node.labels[0]) || node.kind || node.id) : fallback;
}

function selectNode(id) {
  state.selected = id;
  $("expand").disabled = !id || !capability("neighborhood");
  updateContinueButton();
  const node = state.nodes.get(id);
  if (node) {
    showSelection([
      ["Node", id],
      ["Kind", node.kind],
      ["Labels", (node.labels || []).join(", ")],
      ["Properties", JSON.stringify(node.properties || {})],
    ]);
    $("path-from").value = id;
  } else {
    $("selection").className = "muted";
    setMessage($("selection"), "Select evidence, a hierarchy section, or a graph node.");
  }

  renderGraphList();
  draw();
}

function updateContinueButton() {
  $("continue-expansion").hidden =
    !capability("neighborhood") ||
    !state.selected ||
    !state.graphCursors.has(state.selected);
}

function shortLabel(value) {
  const text = String(value || "");
  return text.length > 42 ? `${text.slice(0, 20)}…${text.slice(-18)}` : text;
}

const canvas = $("canvas");
const context = canvas.getContext("2d");
const positions = new Map();

function draw() {
  const rectangle = canvas.getBoundingClientRect();
  if (rectangle.width === 0 || rectangle.height === 0) return;
  const ratio = devicePixelRatio || 1;
  canvas.width = rectangle.width * ratio;
  canvas.height = rectangle.height * ratio;
  context.setTransform(ratio, 0, 0, ratio, 0, 0);
  const nodes = [...state.nodes.values()];
  const centerX = rectangle.width / 2;
  const centerY = rectangle.height / 2;
  const radius = Math.min(centerX, centerY) * 0.7;
  nodes.forEach((node, index) => {
    if (!positions.has(node.id)) {
      const angle = 2 * Math.PI * index / Math.max(nodes.length, 1);
      positions.set(node.id, {
        x: centerX + Math.cos(angle) * radius,
        y: centerY + Math.sin(angle) * radius,
      });
    }
  });
  context.clearRect(0, 0, rectangle.width, rectangle.height);
  if (nodes.length === 0) return;
  context.strokeStyle = "#536a8d";
  for (const edge of state.edges.values()) {
    const from = positions.get(edge.from);
    const to = positions.get(edge.to);
    if (!from || !to) continue;
    context.beginPath();
    context.moveTo(from.x, from.y);
    context.lineTo(to.x, to.y);
    context.stroke();
    context.fillStyle = "#8fa3bf";
    context.fillText(shortLabel(edge.type), (from.x + to.x) / 2, (from.y + to.y) / 2);
  }
  for (const node of nodes) {
    const position = positions.get(node.id);
    context.beginPath();
    context.arc(position.x, position.y, node.id === state.selected ? 10 : 7, 0, Math.PI * 2);
    context.fillStyle = node.id === state.selected ? "#ffd166" : "#55c2ff";
    context.fill();
    context.fillStyle = "#e5edf8";
    context.fillText(shortLabel(nodeName(node, node.id)), position.x + 11, position.y + 4);
  }
}

canvas.onclick = (event) => {
  const rectangle = canvas.getBoundingClientRect();
  const x = event.clientX - rectangle.left;
  const y = event.clientY - rectangle.top;
  let hit = null;
  let distance = 18;
  for (const [id, position] of positions) {
    const candidate = Math.hypot(position.x - x, position.y - y);
    if (candidate < distance) {
      hit = id;
      distance = candidate;
    }
  }
  selectNode(hit);
};

new ResizeObserver(() => {
  positions.clear();
  draw();
}).observe(canvas);
renderGraphList();
applyCapabilities();
loadMeta()
  .then(() => loadDocuments())
  .catch((error) => showError($("documents"), error));
