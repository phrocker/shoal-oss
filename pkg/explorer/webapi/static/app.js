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
  identity: null,
  accessToken: null,
  auth: {configured: false},
  ontology: null,
  reauthRequired: false,
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
let ontologyGeneration = 0;

async function api(path, body) {
  const response = await fetch(`/api/v1/${path}`, {
    method: "POST",
    headers: {"content-type": "application/json", ...authHeaders()},
    body: JSON.stringify(body),
  });
  const value = await response.json();
  if (!response.ok) throw apiError(value, response);
  return value;
}

// apiError preserves the server's structured error envelope so callers can
// tell an authorization denial apart from any other failure. The workspace
// must never render a denial as if it were a generic error or an empty result.
function apiError(value, response) {
  const error = new Error((value && value.message) || (response && response.statusText) || "request failed");
  error.code = (value && value.code) || "";
  error.status = response ? response.status : 0;
  error.reauthenticate = challengedForBearer(response);
  return error;
}

// isDenied reports whether an error is an explicit authorization denial the
// server signalled, as opposed to a match that simply returned nothing. Only
// the unambiguous unauthorized signal counts: a 404 is deliberately
// indistinguishable at the server between "hidden from you" and "absent", so
// it is never claimed here as a denial.
function isDenied(error) {
  if (!error) return false;
  // A re-authentication signal is a token problem, never an authorization
  // denial. It must not read as "access denied", which would misdescribe the
  // governance state, so it is excluded here before the unauthorized test.
  if (needsReauthentication(error)) return false;
  return error.code === "unauthorized" || error.status === 401;
}

// authHeaders adds the bearer token to an API request only when one has been
// acquired in memory. With -dev-auth no token exists, so the header is absent
// and the request is byte-identical to the local development path.
function authHeaders() {
  return state.accessToken ? {authorization: `Bearer ${state.accessToken}`} : {};
}

// challengedForBearer reports whether the transport answered with the standard
// bearer challenge. The central gate sets WWW-Authenticate: Bearer only when
// authentication itself failed (no or expired token); an authorization denial
// from the service is also HTTP 401 but carries no such header. This header is
// therefore the only reliable way to tell "sign in again" apart from "you are
// authenticated but not permitted": the UI never infers it from status alone.
function challengedForBearer(response) {
  if (!response || !response.headers || typeof response.headers.get !== "function") {
    return false;
  }
  const header = response.headers.get("WWW-Authenticate") || "";
  return header.toLowerCase().includes("bearer");
}

// needsReauthentication reports whether an error means the caller must obtain a
// fresh token, as opposed to being denied. It requires both a 401 status and
// the bearer challenge, so a governance 401 (no challenge) is never treated as
// a token expiry and never triggers a login redirect.
function needsReauthentication(error) {
  return Boolean(error) && error.status === 401 && error.reauthenticate === true;
}

// reauthenticationText states plainly that a token expiry is not an
// authorization decision, so re-authentication never reads as a denial and the
// governance wording elsewhere is not undermined.
function reauthenticationText() {
  return "Your sign-in has expired or is not established. This is not an " +
    "authorization denial — sign in again to continue. Your access to the " +
    "workspace is unchanged.";
}

// handleReauthentication surfaces the sign-in affordance again after a token
// expiry. It never makes a security decision: the server remains the only
// enforcement point. It is a no-op when no login is configured (the -dev-auth
// path) or when the shell has no login control (the test harness).
function handleReauthentication() {
  state.reauthRequired = true;
  if (!state.auth || !state.auth.configured) return;
  const button = document.getElementById("login");
  if (!button) return;
  button.hidden = false;
  button.textContent = "Sign in again";
}

function trimTrailingSlash(value) {
  return String(value || "").replace(/\/+$/, "");
}

// base64UrlEncode encodes bytes as unpadded base64url (RFC 4648 §5), the
// encoding PKCE requires for the code verifier and challenge. It avoids btoa so
// it is pure and testable without a browser global.
function base64UrlEncode(bytes) {
  const alphabet =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  let out = "";
  for (let index = 0; index < bytes.length; index += 3) {
    const b0 = bytes[index];
    const b1 = index + 1 < bytes.length ? bytes[index + 1] : 0;
    const b2 = index + 2 < bytes.length ? bytes[index + 2] : 0;
    out += alphabet[b0 >> 2];
    out += alphabet[((b0 & 3) << 4) | (b1 >> 4)];
    if (index + 1 < bytes.length) out += alphabet[((b1 & 15) << 2) | (b2 >> 6)];
    if (index + 2 < bytes.length) out += alphabet[b2 & 63];
  }
  return out;
}

// randomUrlToken returns a cryptographically random base64url string for the
// PKCE verifier, the CSRF state and the OIDC nonce.
function randomUrlToken(byteLength) {
  const bytes = new Uint8Array(byteLength);
  crypto.getRandomValues(bytes);
  return base64UrlEncode(bytes);
}

// PKCE_METHOD is S256 and is the only challenge method this client emits. The
// plain method is deliberately absent: downgrading to plain would let anyone
// who observes the redirect replay the authorization code. Nothing here ever
// produces or accepts "plain".
const PKCE_METHOD = "S256";

// pkceChallengeFromVerifier derives the S256 challenge: base64url(SHA-256(v)).
async function pkceChallengeFromVerifier(verifier) {
  const data = new TextEncoder().encode(verifier);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return base64UrlEncode(new Uint8Array(digest));
}

function encodeForm(pairs) {
  return pairs
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
    .join("&");
}

// buildAuthorizeUrl composes the Authorization Code + PKCE request. The
// challenge method is fixed to S256 by construction.
function buildAuthorizeUrl(config, params) {
  const authorize = `${trimTrailingSlash(config.authority)}/oauth2/v2.0/authorize`;
  const query = encodeForm([
    ["client_id", config.client_id],
    ["response_type", "code"],
    ["redirect_uri", params.redirectUri],
    ["response_mode", "query"],
    ["scope", config.scope],
    ["state", params.state],
    ["nonce", params.nonce],
    ["code_challenge", params.codeChallenge],
    ["code_challenge_method", PKCE_METHOD],
  ]);
  return `${authorize}?${query}`;
}

// verifyCallbackState is the CSRF defence on the redirect. It accepts a
// callback only when the returned state exactly matches the value generated and
// stored before redirecting. A missing stored value never matches, so a forged
// callback with no prior login attempt is refused.
function verifyCallbackState(returnedState, expectedState) {
  return Boolean(expectedState) && returnedState === expectedState;
}

// callbackParams parses a redirect query string into a flat map without relying
// on URLSearchParams, so it is pure and testable in any environment.
function callbackParams(search) {
  const params = {};
  const query = String(search || "").replace(/^\?/, "");
  if (!query) return params;
  for (const pair of query.split("&")) {
    if (!pair) continue;
    const eq = pair.indexOf("=");
    const rawKey = eq < 0 ? pair : pair.slice(0, eq);
    const rawValue = eq < 0 ? "" : pair.slice(eq + 1);
    params[decodeURIComponent(rawKey)] =
      decodeURIComponent(rawValue.replace(/\+/g, " "));
  }
  return params;
}

// exchangeAuthorizationCode verifies state and then swaps the code for an
// access token. State is checked BEFORE the code is presented, so a mismatched
// or forged callback never reaches the network. deps.fetch is injected so the
// exchange is exercisable without a browser. The returned token is handed back
// to the caller and never written to storage.
async function exchangeAuthorizationCode(config, callback, stored, deps) {
  if (!verifyCallbackState(callback.state, stored && stored.state)) {
    throw new Error("login state did not match; the callback was rejected");
  }
  const endpoint = `${trimTrailingSlash(config.authority)}/oauth2/v2.0/token`;
  const body = encodeForm([
    ["client_id", config.client_id],
    ["grant_type", "authorization_code"],
    ["code", callback.code],
    ["redirect_uri", stored.redirectUri],
    ["code_verifier", stored.verifier],
    ["scope", config.scope],
  ]);
  const response = await deps.fetch(endpoint, {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      "accept": "application/json",
    },
    body,
  });
  const value = await response.json();
  if (!response.ok) {
    throw new Error(
      (value && (value.error_description || value.error)) || "token request failed");
  }
  return value.access_token;
}

const LOGIN_FLOW_KEY = "shoal-login-flow";

function browserEnvironment() {
  return typeof window !== "undefined" &&
    typeof window.location !== "undefined" &&
    typeof window.sessionStorage !== "undefined";
}

function redirectUri() {
  return `${window.location.origin}/`;
}

function readLoginFlow() {
  try {
    return JSON.parse(window.sessionStorage.getItem(LOGIN_FLOW_KEY) || "null");
  } catch (_) {
    return null;
  }
}

function clearLoginFlow() {
  try {
    window.sessionStorage.removeItem(LOGIN_FLOW_KEY);
  } catch (_) {
    // Storage may be unavailable; the flow is single-use regardless.
  }
}

function clearCallbackFromUrl() {
  if (typeof window.history === "undefined" || !window.history.replaceState) return;
  window.history.replaceState({}, document.title, window.location.pathname);
}

// beginLogin starts the interactive Authorization Code + PKCE flow. The
// verifier, state and nonce are single-use login-flow artifacts — not the
// credential — and must survive the full-page redirect, so they live briefly in
// sessionStorage and are cleared the instant the callback is consumed. The
// access token itself is never written to any storage (see
// completeLoginFromRedirect); it is held in memory only.
async function beginLogin() {
  if (!browserEnvironment() || !state.auth || !state.auth.configured) return;
  const verifier = randomUrlToken(48);
  const codeChallenge = await pkceChallengeFromVerifier(verifier);
  const flow = {
    verifier,
    state: randomUrlToken(24),
    nonce: randomUrlToken(24),
    redirectUri: redirectUri(),
  };
  window.sessionStorage.setItem(LOGIN_FLOW_KEY, JSON.stringify(flow));
  window.location.assign(buildAuthorizeUrl(state.auth, {
    redirectUri: flow.redirectUri,
    state: flow.state,
    nonce: flow.nonce,
    codeChallenge,
  }));
}

// completeLoginFromRedirect finishes a login when the page was loaded as an
// OIDC redirect callback. It consumes and clears the stored flow and strips the
// code from the address bar before doing anything else, then exchanges the code
// for a token that is kept in memory only.
async function completeLoginFromRedirect(config) {
  if (!browserEnvironment()) return false;
  const params = callbackParams(window.location.search);
  if (!params.code && !params.error) return false;
  const stored = readLoginFlow();
  clearLoginFlow();
  clearCallbackFromUrl();
  if (params.error) {
    throw new Error(params.error_description || params.error);
  }
  const token = await exchangeAuthorizationCode(config, params, stored, {
    fetch: (url, options) => fetch(url, options),
  });
  if (!token) throw new Error("token endpoint returned no access token");
  state.accessToken = token;
  state.reauthRequired = false;
  return true;
}

async function loadAuthConfig() {
  try {
    const response = await fetch("/api/v1/auth-config", {
      headers: {"accept": "application/json"},
    });
    if (!response.ok) return {configured: false};
    return await response.json();
  } catch (_) {
    return {configured: false};
  }
}

// configureLoginUI reveals the sign-in control only when a login is configured
// and the caller holds no token yet. With -dev-auth the config reports
// unconfigured, so no login UI renders at all and the local path is unchanged.
function configureLoginUI(config) {
  const button = document.getElementById("login");
  if (!button) return;
  if (!config || !config.configured) {
    button.hidden = true;
    return;
  }
  button.onclick = () => { beginLogin(); };
  button.hidden = Boolean(state.accessToken);
  button.textContent = "Sign in";
}

function renderLoginError(error) {
  const container = document.getElementById("identity");
  if (!container) return;
  setMessage(
    container,
    `Sign-in could not be completed: ${error && error.message ? error.message : String(error)}.`,
    "error",
  );
}

function identitySubjectLabel(identity) {
  if (identity && identity.authenticated && identity.subject) return identity.subject;
  return "the current identity";
}

// deniedText describes an explicit denial and states plainly that it is a
// denial and not an empty result, so the two never read the same.
function deniedText(action, identity) {
  return `Access denied — ${identitySubjectLabel(identity)} is not authorized to ` +
    `${action}. This is a denial, not an empty result.`;
}

// emptyRetrievalText describes a genuine empty match while making the
// identity scope explicit, because the server filters unauthorized content
// silently. Whether context was withheld is stated separately by
// suppressionClause, so this text no longer claims nothing was withheld.
function emptyRetrievalText(identity) {
  return `No evidence matched for ${identitySubjectLabel(identity)}. ` +
    `Results are scoped to this identity; content you are not authorized to see ` +
    `is never included here.`;
}

function emptyDocumentsText(identity) {
  return `No documents are visible to ${identitySubjectLabel(identity)}. ` +
    `The corpus is shared and filtered per identity, so this may mean nothing ` +
    `was ingested or that nothing here is authorized for you.`;
}

// suppressionClause states, in plain language, whether authorization withheld
// context from this identity. It reveals only a count: the server discloses how
// many documents were withheld and never what they are. The zero case is stated
// explicitly so the three states — results with nothing withheld, results with
// some withheld, and nothing-but-withheld — never read the same. The wording is
// careful not to overclaim: a positive count is withheld context, never a claim
// that nothing exists, and it is scoped to authorization withholding, which is
// the only thing the server counts.
function suppressionClause(count, identity) {
  const withheld = Number(count) || 0;
  const who = identitySubjectLabel(identity);
  if (withheld <= 0) {
    return ` No documents were withheld from ${who} by authorization.`;
  }
  const noun = withheld === 1 ? "document" : "documents";
  const verb = withheld === 1 ? "is" : "are";
  return ` ${withheld} ${noun} ${verb} withheld from ${who} by authorization and ` +
    `not shown. This counts withheld context and is not a confirmation that ` +
    `nothing exists; nothing about the withheld ${noun} is disclosed.`;
}

async function loadMeta() {
  const response = await fetch("/api/v1/meta", {headers: {"accept": "application/json", ...authHeaders()}});
  const value = await response.json();
  if (!response.ok) throw apiError(value, response);
  state.capabilities = {...state.capabilities, ...(value.capabilities || {})};
  state.uploadLimits = {
    max_upload_files: value.max_upload_files || 0,
    max_upload_file_bytes: value.max_upload_file_bytes || 0,
    max_upload_total_bytes: value.max_upload_total_bytes || 0,
  };
  applyCapabilities();
}

async function loadIdentity() {
  try {
    const response = await fetch("/api/v1/identity", {headers: {"accept": "application/json", ...authHeaders()}});
    const value = await response.json();
    if (!response.ok) throw apiError(value, response);
    state.identity = value;
    renderIdentity(value);
  } catch (error) {
    state.identity = null;
    renderIdentityError(error);
  }
}

function renderIdentity(identity) {
  const container = $("identity");
  const badge = $("identity-badge");
  container.className = "identity";
  if (!identity || !identity.authenticated) {
    container.classList.add("identity-anon");
    setMessage(
      container,
      "This workspace is serving requests without an established identity. " +
        "Results are not attributed to any principal.",
    );
    badge.hidden = true;
    badge.textContent = "";
    return;
  }

  badge.hidden = false;
  badge.textContent = `▲ ${identity.subject}`;
  badge.title = `Signed in as ${identity.subject}. Shared governed workspace — ` +
    "every result is scoped to this identity.";

  const fragment = document.createDocumentFragment();

  const who = document.createElement("div");
  who.className = "identity-who";
  const label = document.createElement("span");
  label.className = "identity-label";
  label.textContent = "You are";
  const subject = document.createElement("strong");
  subject.className = "identity-subject";
  subject.textContent = identity.subject;
  subject.title = identity.subject;
  who.append(label, subject);
  fragment.append(who);

  const shared = document.createElement("p");
  shared.className = "identity-shared";
  shared.textContent =
    "Shared governed workspace. Every result below is retrieved as this " +
    "identity, so what you see is exactly what you are authorized to see.";
  fragment.append(shared);

  fragment.append(identityChips("Authorized operations", identity.operations || []));

  const details = document.createElement("dl");
  details.className = "identity-facts";
  appendFact(details, "Actor", identity.actor);
  appendFact(details, "Domain", identity.authorization_domain);
  appendFact(details, "Policy gen", identity.policy_generation
    ? String(identity.policy_generation) : "");
  appendFact(details, "Session ends", formatExpiry(identity.authentication_expires));
  appendFact(details, "Purpose", identity.audit_purpose);
  appendFact(details, "Request", identity.request_id);
  if (details.children.length > 0) fragment.append(details);

  const note = document.createElement("p");
  note.className = "identity-note muted";
  note.textContent =
    "Identity is assigned by the server, not chosen here. This build " +
    "authenticates every request as one development principal over a loopback " +
    "listener, so there is no client-side identity switch.";
  fragment.append(note);

  container.replaceChildren(fragment);
}

function identityChips(legendText, values) {
  const group = document.createElement("div");
  group.className = "identity-chips";
  group.setAttribute("role", "group");
  group.setAttribute("aria-label", legendText);
  const legend = document.createElement("span");
  legend.className = "identity-label";
  legend.textContent = legendText;
  group.append(legend);
  if (values.length === 0) {
    const none = document.createElement("span");
    none.className = "chip chip-empty";
    none.textContent = "none";
    group.append(none);
    return group;
  }
  for (const value of values) {
    const chip = document.createElement("span");
    chip.className = "chip";
    chip.textContent = value;
    group.append(chip);
  }
  return group;
}

function appendFact(list, term, value) {
  if (!value) return;
  const dt = document.createElement("dt");
  dt.textContent = term;
  const dd = document.createElement("dd");
  dd.textContent = value;
  dd.title = value;
  list.append(dt, dd);
}

function formatExpiry(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleTimeString([], {hour: "2-digit", minute: "2-digit"});
}

function renderIdentityError(error) {
  const container = $("identity");
  if (needsReauthentication(error)) {
    handleReauthentication();
    container.className = "identity identity-unknown";
    setMessage(container, reauthenticationText(), "reauth");
    const badge = $("identity-badge");
    badge.hidden = true;
    badge.textContent = "";
    return;
  }
  container.className = "identity identity-unknown";
  setMessage(
    container,
    `Identity is unavailable: ${error && error.message ? error.message : String(error)}. ` +
      "Result sets below cannot be attributed to a principal.",
    "error",
  );
  const badge = $("identity-badge");
  badge.hidden = true;
  badge.textContent = "";
}

async function loadOntology() {
  const status = $("ontology-status");
  if (!status) return;
  const generation = ++ontologyGeneration;
  setStatus(status, "Loading active ontology…");
  try {
    const response = await fetch("/api/v1/ontology", {
      headers: {"accept": "application/json", ...authHeaders()},
    });
    const value = await response.json();
    if (!response.ok) throw apiError(value, response);
    if (generation !== ontologyGeneration) return;
    state.ontology = value;
    renderOntology(value);
  } catch (error) {
    if (generation !== ontologyGeneration) return;
    state.ontology = null;
    renderOntologyError(error);
  }
}

function renderOntology(ontology) {
  const status = $("ontology-status");
  const details = $("ontology-details");
  if (!status || !details) return;
  details.replaceChildren();
  if (!ontology || !ontology.configured) {
    setStatus(
      status,
      "No active ontology is configured. This is an explicit unconfigured state, " +
        "not an empty ontology and not a loading failure.",
      "empty-state",
    );
    return;
  }

  const concepts = ontology.concepts || [];
  const relationships = ontology.relationships || [];
  const properties = ontology.properties || [];
  setStatus(
    status,
    `Active ontology ${shortLabel(ontology.identity && ontology.identity.schema_id)} / ` +
      `${shortLabel(ontology.identity && ontology.identity.version_id)}: ` +
      `${concepts.length} concept(s), ${relationships.length} relationship(s), ` +
      `${properties.length} property definition(s).`,
    "muted",
  );

  const conceptMap = new Map(concepts.map((concept) => [concept.id, concept]));
  const propertyMap = new Map(properties.map((property) => [property.id, property]));
  const fragment = document.createDocumentFragment();
  fragment.append(ontologyIdentityFacts(ontology));
  fragment.append(ontologySection(
    "Declared joins",
    relationships,
    "This active ontology declares no relationships. It is configured, but it has no joins yet.",
    (relationship) => relationshipCard(relationship, conceptMap, propertyMap),
  ));
  fragment.append(ontologySection(
    "Concepts",
    concepts,
    "This active ontology has no concepts.",
    (concept) => conceptCard(concept, propertyMap),
  ));
  fragment.append(ontologySection(
    "Properties and constraints",
    properties,
    "This active ontology has no properties or constraints.",
    propertyCard,
  ));
  details.replaceChildren(fragment);
}

function ontologyIdentityFacts(ontology) {
  const section = document.createElement("section");
  section.className = "ontology-identity";
  const heading = document.createElement("h3");
  heading.textContent = "Active identity";
  const facts = document.createElement("dl");
  facts.className = "identity-facts";
  appendFact(facts, "Schema ID", ontology.identity && ontology.identity.schema_id);
  appendFact(facts, "Version ID", ontology.identity && ontology.identity.version_id);
  appendFact(facts, "Reading", ontology.identity && ontology.identity.reading);
  appendFact(facts, "Schema", ontology.schema && ontology.schema.name);
  appendFact(facts, "Version", ontology.version && ontology.version.version);
  appendFact(facts, "Created", ontology.version && ontology.version.created_at);
  section.append(heading, facts);
  return section;
}

function ontologySection(title, values, emptyText, renderItem) {
  const section = document.createElement("section");
  section.className = "ontology-section";
  const heading = document.createElement("h3");
  heading.textContent = title;
  section.append(heading);
  if (values.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = emptyText;
    section.append(empty);
    return section;
  }
  const list = document.createElement("div");
  list.className = "ontology-list";
  for (const value of values) list.append(renderItem(value));
  section.append(list);
  return section;
}

function relationshipCard(relationship, conceptMap, propertyMap) {
  const card = ontologyCard(relationship);
  const join = document.createElement("p");
  join.className = "ontology-join";
  const arrow = relationship.directed ? "→" : "↔";
  join.textContent = `${conceptNames(relationship.from_concepts, conceptMap)} ` +
    `${arrow} ${conceptNames(relationship.to_concepts, conceptMap)}`;
  card.append(join);
  const direction = document.createElement("p");
  direction.className = "muted";
  direction.textContent = relationship.directed
    ? "Directed relationship: source concepts join to target concepts."
    : "Undirected relationship: either endpoint set may be read from the other.";
  card.append(direction);
  appendPropertyChips(card, relationship.properties || [], propertyMap);
  return card;
}

function conceptCard(concept, propertyMap) {
  const card = ontologyCard(concept);
  appendPropertyChips(card, concept.properties || [], propertyMap);
  return card;
}

function propertyCard(property) {
  const card = ontologyCard(property);
  const type = document.createElement("p");
  type.className = "muted";
  type.textContent = `Value type: ${property.value_type || "unknown"}`;
  card.append(type);
  const constraints = property.constraints || [];
  if (constraints.length === 0) {
    const none = document.createElement("p");
    none.className = "muted";
    none.textContent = "No constraints.";
    card.append(none);
    return card;
  }
  const list = document.createElement("ul");
  list.className = "ontology-constraints";
  for (const constraint of constraints) {
    const item = document.createElement("li");
    item.textContent = constraintText(constraint);
    list.append(item);
  }
  card.append(list);
  return card;
}

function ontologyCard(value) {
  const card = document.createElement("article");
  card.className = "ontology-card";
  const heading = document.createElement("h4");
  heading.textContent = value.name || value.key || shortLabel(value.id);
  heading.title = value.id || "";
  card.append(heading);
  if (value.description) {
    const description = document.createElement("p");
    description.className = "muted";
    description.textContent = value.description;
    card.append(description);
  }
  const id = document.createElement("code");
  id.textContent = value.id || "";
  id.title = value.id || "";
  card.append(id);
  return card;
}

function appendPropertyChips(card, propertyIDs, propertyMap) {
  const group = document.createElement("div");
  group.className = "identity-chips";
  const label = document.createElement("span");
  label.className = "identity-label";
  label.textContent = "Properties";
  group.append(label);
  if (propertyIDs.length === 0) {
    const none = document.createElement("span");
    none.className = "chip chip-empty";
    none.textContent = "none";
    group.append(none);
  } else {
    for (const id of propertyIDs) {
      const chip = document.createElement("span");
      chip.className = "chip";
      const property = propertyMap.get(id);
      chip.textContent = property ? property.name : shortLabel(id);
      chip.title = id;
      group.append(chip);
    }
  }
  card.append(group);
}

function conceptNames(ids, conceptMap) {
  const names = (ids || []).map((id) => {
    const concept = conceptMap.get(id);
    return concept ? concept.name : shortLabel(id);
  });
  return names.length === 0 ? "(none)" : names.join(", ");
}

function constraintText(constraint) {
  const kind = constraint.kind || "constraint";
  if (Object.prototype.hasOwnProperty.call(constraint, "count")) {
    return `${kind}: ${constraint.count}`;
  }
  if (constraint.value) {
    return `${kind}: ${formatOntologyValue(constraint.value)}`;
  }
  if (constraint.pattern) {
    return `${kind}: ${constraint.pattern}`;
  }
  if ((constraint.allowed_values || []).length > 0) {
    return `${kind}: ${(constraint.allowed_values || []).map(formatOntologyValue).join(", ")}`;
  }
  return kind;
}

function formatOntologyValue(value) {
  if (!value) return "";
  return `${value.type || "value"} ${String(value.value)}`;
}

function renderOntologyError(error) {
  const status = $("ontology-status");
  const details = $("ontology-details");
  if (!status || !details) return;
  details.replaceChildren();
  if (needsReauthentication(error)) {
    handleReauthentication();
    setStatus(status, reauthenticationText(), "reauth");
    return;
  }
  if (isDenied(error)) {
    setStatus(status, deniedText("describe the active ontology", state.identity), "denied");
    return;
  }
  setStatus(
    status,
    `Ontology is unavailable: ${error && error.message ? error.message : String(error)}.`,
    "error",
  );
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

// showActionError renders an explicit authorization denial distinctly from any
// other failure. A denied action gets the "denied" treatment and reports true;
// every other error falls back to the generic error rendering.
function showActionError(element, error, action) {
  if (needsReauthentication(error)) {
    handleReauthentication();
    if (element.getAttribute && element.getAttribute("role") === "status") {
      setStatus(element, reauthenticationText(), "reauth");
    } else {
      setMessage(element, reauthenticationText(), "reauth");
    }
    return true;
  }
  if (isDenied(error)) {
    if (element.getAttribute && element.getAttribute("role") === "status") {
      setStatus(element, deniedText(action, state.identity), "denied");
    } else {
      setMessage(element, deniedText(action, state.identity), "denied");
    }
    return true;
  }
  showError(element, error);
  return false;
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
    const suppressed = response.suppressed || 0;
    if (documents.length === 0 && reset) {
      setStatus(
        $("documents-status"),
        emptyDocumentsText(state.identity) + suppressionClause(suppressed, state.identity),
        suppressed > 0 ? "withheld" : "empty-state",
      );
    } else {
      const total = $("documents").children.length + documents.length;
      setStatus(
        $("documents-status"),
        `Showing ${total} document(s).` + suppressionClause(suppressed, state.identity),
        suppressed > 0 ? "withheld" : "muted",
      );
    }
    const fragment = document.createDocumentFragment();
    for (const item of documents) fragment.append(createDocumentCard(item));
    $("documents").append(fragment);
    state.cursor = response.next_cursor || "";
    $("more").hidden = !state.cursor;
  } catch (error) {
    if (generation === documentsGeneration) {
      if (isDenied(error)) $("documents").replaceChildren();
      showActionError($("documents-status"), error, "list documents");
    }
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
      headers: {"accept": "application/json", "x-shoal-workspace-request": "1", ...authHeaders()},
      body,
    });
    const value = await response.json();
    if (!response.ok) throw apiError(value, response);
    clearSnapshotDependentViews();
    pin(value.snapshot);
    renderUploadResults(value.files || []);
    await loadDocuments(true);
  } catch (error) {
    showActionError($("upload-status"), error, "upload files");
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
    if (generation === documentGeneration) showActionError($("hierarchy-status"), error, "open this document");
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
    renderEvidence(response.retrieval, response.suppressed || 0);
  } catch (error) {
    if (generation === searchGeneration) {
      $("evidence-results").replaceChildren();
      showActionError($("evidence-status"), error, "run this retrieval");
    }
  }
};

function renderEvidence(response, suppressed = 0) {
  const results = response.results || [];
  const withheld = Number(suppressed) || 0;
  if (results.length === 0) {
    $("evidence-results").replaceChildren();
    setStatus(
      $("evidence-status"),
      emptyRetrievalText(state.identity) + suppressionClause(withheld, state.identity),
      withheld > 0 ? "withheld" : "empty-state",
    );
    draw();
    return;
  }
  setStatus(
    $("evidence-status"),
    `Showing ${results.length} evidence result(s).` +
      suppressionClause(withheld, state.identity),
    withheld > 0 ? "withheld" : "muted",
  );
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
    if (needsReauthentication(error)) {
      handleReauthentication();
      $("graph-status").textContent = reauthenticationText();
    } else if (isDenied(error)) {
      $("graph-status").textContent = deniedText("expand this neighborhood", state.identity);
    } else {
      $("graph-status").textContent = `Graph expansion failed: ${error.message || String(error)}`;
    }
    showActionError($("selection"), error, "expand this neighborhood");
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
    if (needsReauthentication(error)) {
      handleReauthentication();
      $("graph-status").textContent = reauthenticationText();
    } else if (isDenied(error)) {
      $("graph-status").textContent = deniedText("find a path", state.identity);
    } else {
      $("graph-status").textContent = `Path finding failed: ${error.message || String(error)}`;
    }
    showActionError($("selection"), error, "find a path");
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

// bootstrap resolves any login before the first identity/data calls so those
// calls carry a bearer token when one is configured. With -dev-auth the config
// reports unconfigured, no login UI renders, and the sequence is the same as
// before: identity, then metadata, then documents.
async function bootstrap() {
  const config = await loadAuthConfig();
  state.auth = config;
  configureLoginUI(config);
  if (config.configured) {
    try {
      await completeLoginFromRedirect(config);
    } catch (error) {
      renderLoginError(error);
    }
  }
  loadIdentity();
  loadOntology();
  loadMeta()
    .then(() => loadDocuments())
    .catch((error) => showError($("documents"), error));
}
bootstrap();
