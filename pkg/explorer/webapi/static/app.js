const state = {
  snapshot: {},
  cursor: "",
  document: null,
  nodes: new Map(),
  edges: new Map(),
  selected: null,
};
const $ = (id) => document.getElementById(id);

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

function escapeHTML(value) {
  return String(value ?? "").replace(
    /[&<>"']/g,
    (character) => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[character]),
  );
}

function pin(snapshot) {
  state.snapshot = snapshot;
  $("snapshot").textContent =
    `snapshot ${snapshot.id.slice(0, 10)} · frontier ${snapshot.frontier}`;
}

async function loadDocuments(reset = true) {
  try {
    if (reset) {
      state.cursor = "";
      $("documents").innerHTML = "";
    }
    const response = await api("documents", {
      snapshot: state.snapshot,
      page: {limit: 25, cursor: state.cursor},
    });
    pin(response.snapshot);
    for (const item of response.documents) {
      const element = document.createElement("button");
      element.className = "doc";
      element.innerHTML =
        `<strong>${escapeHTML(item.document.title)}</strong><br>` +
        `<small>${escapeHTML(item.source_uri || item.document.id)}</small>`;
      element.onclick = () => loadDocument(item.document.id, item.revision.id);
      $("documents").append(element);
    }
    state.cursor = response.next_cursor || "";
    $("more").hidden = !state.cursor;
  } catch (error) {
    showError($("documents"), error);
  }
}

async function loadDocument(documentID, revisionID) {
  try {
    const response = await api("document", {
      snapshot: state.snapshot,
      document_id: documentID,
      revision_id: revisionID,
    });
    pin(response.snapshot);
    state.document = response.document;
    $("hierarchy").classList.remove("muted");
    $("hierarchy").innerHTML = sectionHTML(response.document.root);
    $("hierarchy").querySelectorAll("[data-section]").forEach((element) => {
      element.onclick = () => selectSection(element.dataset.section);
    });
    mergeGraph({nodes: [nodeFromDocument(response.document.document)], edges: []});
    draw();
  } catch (error) {
    showError($("hierarchy"), error);
  }
}

function sectionHTML(view) {
  return `<ul><li><button class="section-node" data-section="${escapeHTML(view.section.id)}">` +
    `${escapeHTML(view.section.heading || "(untitled)")}</button>` +
    view.spans.map((span) => `<div class="citation">${escapeHTML(span.text)}</div>`).join("") +
    view.children.map(sectionHTML).join("") +
    "</li></ul>";
}

function selectSection(id) {
  const find = (view) =>
    view.section.id === id ? view : view.children.map(find).find(Boolean);
  const view = find(state.document.root);
  $("selection").innerHTML = `<div class="kv"><b>Section</b>` +
    `<span>${escapeHTML(view.section.heading)}</span><b>ID</b>` +
    `<span>${escapeHTML(id)}</span><b>Range</b>` +
    `<span>${view.section.range.start.offset}–${view.section.range.end.offset}</span></div>`;
  expandIDs([id]);
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
  try {
    const response = await api("retrieve", {
      snapshot: state.snapshot,
      query: {
        text: $("query").value,
        top_k: 20,
        modes: ["lexical", "tree", "graph"],
        explain: true,
        as_of: state.snapshot.as_of,
      },
    });
    pin(response.snapshot);
    renderEvidence(response.retrieval);
  } catch (error) {
    showError($("evidence"), error);
  }
};

function renderEvidence(response) {
  const results = response.results || [];
  $("evidence").innerHTML = results.length ? "" : "<p class=muted>No evidence matched.</p>";
  for (const result of results) {
    const element = document.createElement("article");
    element.className = "result";
    const evidence = (result.evidence || []).map((item, index) =>
      `<blockquote data-evidence="${index}">${escapeHTML(item.quote)}</blockquote>` +
      `<div class="citation">${escapeHTML(item.citation.document_id)} / ` +
      `${escapeHTML(item.citation.revision_id)} / bytes ` +
      `${item.citation.range.start.offset}–${item.citation.range.end.offset}</div>`,
    ).join("");
    const explanation = result.explanation
      ? `<pre class="explanation">${escapeHTML(result.explanation.summary)}\n` +
        `${escapeHTML(JSON.stringify(result.explanation.scores, null, 2))}</pre>`
      : "";
    element.innerHTML = `<b>${escapeHTML(result.id)}</b> ` +
      `<span class=score>${Number(result.score).toFixed(3)}</span>${evidence}${explanation}`;
    element.querySelectorAll("blockquote").forEach((quote, index) => {
      quote.onclick = () => selectEvidence(result, result.evidence[index]);
    });
    $("evidence").append(element);
    for (const item of result.evidence || []) mergeGraph(item.path || {});
  }
  draw();
}

function selectEvidence(result, evidence) {
  $("selection").innerHTML = `<div class=kv><b>Result</b><span>${escapeHTML(result.id)}</span>` +
    `<b>Quote</b><span>${escapeHTML(evidence.quote)}</span>` +
    `<b>Document</b><span>${escapeHTML(evidence.citation.document_id)}</span>` +
    `<b>Section</b><span>${escapeHTML(evidence.citation.section_id)}</span>` +
    `<b>Span</b><span>${escapeHTML(evidence.citation.span_id)}</span></div>`;
  loadDocument(evidence.citation.document_id, evidence.citation.revision_id);
}

function mergeGraph(graph) {
  const previousNodeCount = state.nodes.size;
  for (const node of graph.nodes || []) state.nodes.set(node.id, node);
  for (const edge of graph.edges || []) state.edges.set(edge.id, edge);
  if (state.nodes.size !== previousNodeCount) positions.clear();
  renderGraphList();
}

async function expandIDs(ids) {
  try {
    const response = await api("neighborhood", {
      snapshot: state.snapshot,
      node_ids: ids,
      depth: 1,
      fanout: 18,
      max_nodes: 120,
    });
    pin(response.snapshot);
    mergeGraph(response.neighborhood);
    activate("graph");
    draw();
  } catch (error) {
    showError($("selection"), error);
  }
}

$("expand").onclick = () => state.selected && expandIDs([state.selected]);
$("more").onclick = () => loadDocuments(false);
$("find-path").onclick = async () => {
  try {
    const response = await api("path", {
      snapshot: state.snapshot,
      from: $("path-from").value,
      to: $("path-to").value,
      max_depth: 4,
      fanout: 25,
    });
    pin(response.snapshot);
    mergeGraph(response.path);
    draw();
  } catch (error) {
    showError($("selection"), error);
  }
};

document.querySelectorAll(".tab").forEach((tab) => {
  tab.onclick = () => activate(tab.dataset.panel);
});

function activate(id) {
  document.querySelectorAll(".tab,.panel").forEach((element) => {
    element.classList.remove("active");
  });
  document.querySelector(`[data-panel="${id}"]`).classList.add("active");
  $(id).classList.add("active");
  if (id === "graph") draw();
}

function showError(element, error) {
  element.innerHTML = `<p class=error>${escapeHTML(error.message)}</p>`;
}

function renderGraphList() {
  $("graph-nodes").innerHTML = "";
  $("graph-edges").innerHTML = "";
  for (const node of state.nodes.values()) {
    const button = document.createElement("button");
    button.textContent = (node.labels && node.labels[0]) || node.kind || node.id;
    button.setAttribute("aria-pressed", String(node.id === state.selected));
    button.onclick = () => selectNode(node.id);
    $("graph-nodes").append(button);
  }
  for (const edge of state.edges.values()) {
    const item = document.createElement("li");
    const from = state.nodes.get(edge.from);
    const to = state.nodes.get(edge.to);
    item.textContent = `${nodeName(from, edge.from)} → ${nodeName(to, edge.to)} (${edge.type})`;
    $("graph-edges").append(item);
  }
}

function nodeName(node, fallback) {
  return node ? ((node.labels && node.labels[0]) || node.kind || node.id) : fallback;
}

function selectNode(id) {
  state.selected = id;
  $("expand").disabled = !id;
  const node = state.nodes.get(id);
  if (node) {
    $("selection").innerHTML = `<div class=kv><b>Node</b><span>${escapeHTML(id)}</span>` +
      `<b>Kind</b><span>${escapeHTML(node.kind)}</span><b>Labels</b>` +
      `<span>${escapeHTML((node.labels || []).join(", "))}</span><b>Properties</b>` +
      `<span>${escapeHTML(JSON.stringify(node.properties || {}))}</span></div>`;
    $("path-from").value = id;
  }
  renderGraphList();
  draw();
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
    context.fillText(edge.type, (from.x + to.x) / 2, (from.y + to.y) / 2);
  }
  for (const node of nodes) {
    const position = positions.get(node.id);
    context.beginPath();
    context.arc(position.x, position.y, node.id === state.selected ? 10 : 7, 0, Math.PI * 2);
    context.fillStyle = node.id === state.selected ? "#ffd166" : "#55c2ff";
    context.fill();
    context.fillStyle = "#e5edf8";
    context.fillText(
      (node.labels && node.labels[0]) || node.kind || node.id,
      position.x + 11,
      position.y + 4,
    );
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
loadDocuments();
