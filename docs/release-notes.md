# Release notes

## Unreleased

### The policy catalog lets a derived edge win an application edge's ID (#732)

Before #699 an authorized `Connect` accepted any edge ID, and a document's
root `contains` edge ID can be derived from its URI. So a stored application
edge can hold the intrinsic edge ID of a document ingested later. #724 let the
intrinsic edge win in the base graph, but the policy catalog still refused to
register the document's revision: every authorized ingest of that URI
returned `conflict: authorization policy catalog conflict`, and the document,
which the base had committed, stayed withheld from every reader. It failed
closed, but no supported operation fixed it. Extraction and materialization
were refused the same way over an application edge holding one of their edge
IDs.

**Behaviour change.** The catalog now follows the base graph:

- **Intrinsic edges.** A revision whose intrinsic edge ID an application
  registration holds is registered, whether that registration has other
  content or the intrinsic edge's very content. The intrinsic edge
  wins its ID: `Edge` and `Edges` resolve it, so it is served under the
  document's rule alone. The application registration stays stored but
  governs nothing, so the application edge is withheld from every reader,
  even if the base showed it. It is reported by the new operator diagnostic
  `ApplicationEdgeCollisions(ctx)` on `MemoryPolicyStore` and
  `DurablePolicyStore`, which matches `Explorer.ApplicationEdgeCollisions`
  for intrinsic edges, except that it also lists an application registration
  identical to the intrinsic edge, which the base graph does not count as a
  collision. Like the base's, it is not on `PolicyStore` and no
  `authorized.Client` method exposes it. Only an exact document `contains`
  edge (`explorer.IsDocumentContainsEdge`) wins: the client reads a
  revision's intrinsic edges from the base graph, so anything else could be
  the application edge itself, and that revision is still refused.
- **An application `contains` edge between a document's own nodes**, which
  the base graph shows and the client therefore reads as intrinsic, is no
  longer a reason to refuse the document's next revision, unless it is an
  exact document `contains` edge (which wins, as above). It is left out of
  the revision's intrinsic edges and stays governed by its application
  registration (its rule and both endpoints' rules), never by the document's
  rule alone.
- **Extracted and materialized edges.** This is a deliberate precedence
  change. An extracted or materialized registration replaces an application
  registration of other content or rule under its ID, and consumes a
  leftover reservation there, instead of refusing the extraction or
  materialization. That includes an extracted edge of the same content under
  another rule, and any materialized edge, which were refused before.
  The application registration's binding (#695) is dropped with it: another
  rule's binding must not govern the derived edge, and a materialized edge
  carries none. With other content, the base still holds and reports the
  application edge. It never matches the derived registration, so it stays
  withheld even if the base shows it again. With the same content, the base
  shows one edge, which is the extractor's or materializer's own canonical
  assertion and is served under its registration. Only an extracted edge
  matching an application registration exactly, under the same rule, keeps
  the binding, as before: that is the same assertion under the same policy
  (usually an extracted edge persisted before registration kinds existed),
  and keeping it can only narrow.
- Whichever way the derived edge won, `Connect` can never take the ID back:
  the reservation is refused while the derived registration holds it.

**Format.** Nothing new is persisted. Which application registrations are
withheld follows from the current revisions on every open, and a replaced
registration is an ordinary edge row. So there is no migration and no format
floor change.

**Rollback.** A binary from before this release opens the catalog unchanged
and applies its own rules to it. One since #724 prefers the application
registration again over a withheld collision, so the intrinsic edge no
longer matches it and is withheld, and the next ingest of the document is
refused as before: fail closed. The application edge stays withheld there
too, because that binary's base graph withholds it. A binary from before
#724 lets the application edge replace the intrinsic edge in its graph and
serves it under its application registration, as it did before this
release. Every binary since #721 includes #724, and binaries from before
#721 refuse a catalog once it holds the namespace key, which it does from
the first time it needs a namespace. Binaries from before #695
refuse it from the first read-write open since (the format floor). A
derived registration that replaced an application one is an ordinary edge
row to every binary.

**Residuals.**

- No supported API retires a stored application edge yet, in the base or the
  catalog. It is no longer needed to make a squatted document usable, but the
  withheld edge keeps its ID, and an application edge whose ID an intrinsic
  edge holds becomes visible again, under its own registration, if a later
  revision of that document no longer has that intrinsic edge.
- An application `contains` edge that is not an exact document `contains`
  edge but is read as intrinsic, and has no catalog registration (written
  through the unwrapped `Explorer.Connect`), is still registered as
  intrinsic, as before this release.

### Edge and upload namespaces are keyed by the catalog (#721)

The application edge namespace (`app-edge_<digest>/<local>`, #699) and the
upload namespace (`upload://workspace/u-<digest>/<name>`, #712) named an
identity by an unkeyed SHA-256 of its components. Every component is known or
guessable, so anyone who could read an edge ID or a document's `source_uri`
could group edges and uploads by author, and confirm a guessed author by
recomputing the digest.

**Behaviour change.** The digest is now HMAC-SHA256 under a random 256-bit
key that belongs to the policy catalog, still truncated to 32 hex digits. It
can be compared for equality but not recomputed from an identity, and two
catalogs give one identity unrelated namespaces.

- **Key.** A memory catalog draws its key on first use and keeps it for its
  life. A durable catalog opened read-write draws one the first time it
  needs a namespace, not at open, and stores it in a reserved catalog row
  next to the format floor before it uses it. So a host that opens an empty
  or unmounted catalog directory and then refuses to serve it leaves no key
  that a backup copied in later would contradict. A failed write makes the
  store fail closed until it is reopened. The key is
  never rotated, and it never appears in an error, log, listing or metric.
  `OpenExistingDurablePolicyStore` (the offline listings) never writes it.
  Over a catalog with no key, such a store reports the namespace routes
  unavailable.
- **`PolicyStore`.** The interface gains `IdentityNamespaceMAC(ctx, message)`,
  which returns a MAC under the key, never the key itself. Wrappers that embed
  a `PolicyStore` pick it up unchanged.
- **API change.** `authorized.ApplicationEdgeLocalID` now takes the context
  `Connect` passes the edge selector: `ApplicationEdgeLocalID(ctx, decision,
  id)`. With any other context it reports false.

**Migration.** Nothing is rewritten. A document's ID is derived from its
source URI, and an edge's ID is what its writer chose. So what was written
before this release keeps its unkeyed digest, which stays linkable as before.
Each identity's own unkeyed namespace is an alias of its keyed one, and never
of anyone else's:

- **Uploads.** Step 1 of the upload route (#712) tries the caller's keyed URI
  of the name first, then its unkeyed one, and goes to whichever has something
  stored. An ingest at the caller's unkeyed URI is accepted as its own when
  something is stored there. If nothing is, it is refused with the same
  `InvalidArgument` as an ineligible target. New documents only go to the
  keyed namespace.
- **Edges.** `Connect` accepts an ID in the caller's unkeyed namespace only
  when an edge is registered under it, which is a retry. It refuses any other
  such ID with the same `InvalidArgument` as an ID outside the namespace. New
  edges use `ApplicationEdgeID`, which returns the keyed ID.
  `ApplicationEdgeLocalID` recognises both.

Edge reservations left by a `Connect` from before this release whose base
write was indeterminate can no longer be completed under their unkeyed ID.

**Rollback.** The key row is under the source-claim prefix, like the format
floor, with another record kind. Every durable binary from before this
release, including those since the floor (#695), therefore refuses a catalog
that holds the key with `stored policy source claim record is corrupt`. That
error does not mean the catalog is corrupt. Because of this refusal, an older
binary never computes unkeyed namespaces over a catalog whose identities
already have keyed ones, and no identity ends up with two namespaces. To roll
back, restore the catalog from a backup taken before the key was written.
Documents and edges written to the corpus after that backup are then
unregistered, so reads deny them, as with any catalog restore.

Rolling forward again draws a **new** key, so every identity gets new keyed
namespaces. Keyed edge IDs and upload URIs held outside the catalog are then
another identity's namespace to the server: application caches, stored
`ApplicationEdgeID` or `OwnUploadSourceURI` results, a client's own
bookkeeping. `Connect` refuses those IDs, and ingests at those URIs are
refused unless eligible as another uploader's documents. Re-derive them with
`ApplicationEdgeID` and `OwnUploadSourceURI`. The same applies to restoring
any backup taken before the key was written.

### Uploads by name live in the uploader's namespace (#712)

The browser upload route and the MCP `shoal.ingest` tool store a file as
`upload://workspace/<escaped filename>`. Before this release that URI was the
same for every principal. Ingesting at a URI whose source claim the caller
cannot read is refused with `not_found` (writing over a hidden document is not
acceptable), while a name never uploaded succeeds. So a reader could test
whether a hidden document existed under a guessed filename, for example
`merger-plan.md`, on the memory and durable catalogs alike.

**Behaviour change.** `upload://workspace/<name>` is now a request to upload
`<name>`, which `authorized.Client.Ingest` routes, under the mutation lock and
before anything is written, to the first of:

1. the caller's own URI of the name, `upload://workspace/u-<digest of the
   caller's identity>/<name>`, when anything is stored there;
2. the name's un-namespaced URI (every upload before this release), when it is
   eligible: something is recorded for it and the caller is authorized to
   **ingest** under every rule recorded for it (its claim's rule, a pending
   claim's previous rule and its current registration);
3. the one other identity's URI of the name, stored in the base, that is
   eligible, when there is exactly one;
4. the caller's own URI of the name. When two or more other identities'
   documents of the name are eligible, the name is ambiguous and none of them
   is written.

Steps 2 and 3 skip every URI that is not eligible, exactly as if it were
absent. So the outcome of uploading a name never depends on another
identity's document that the caller is not authorized to ingest under: a
reader uploading a hidden document's name gets what it gets for a name never
uploaded, its own new document, and the hidden one is untouched. (It does
depend on the caller's own identity's document of the name; see below.) The
identity is the one the application edge namespace hashes (#699), under a
different version string. It includes the client and acting application, so
one person uploading through the browser and through an MCP client has two
namespaces, and two documents of a name uploaded both ways.

**Name notices.** An upload that leaves other eligible documents of its name
unchanged says so, so a fork is never silent. That happens when the caller
already has its own document of the name (step 1) or the name was ambiguous
(step 4), and when it went to the pre-#712 document while other eligible ones
exist (step 2). The upload route's and MCP tool's per-file result then carries
`name_notice`: `route` (`own` or `legacy`), `source_uri` (the document
written), `other_documents` (how many were left unchanged), `ambiguous`, and a
public `message`, which the workspace UI shows. Only eligible documents are
counted, so the notice discloses nothing the caller could not have written
itself. For example, if a labelled document and a reader's public document
share a name, another holder of the label re-uploading the name to declassify
the labelled one gets its own document, and the notice says two documents of
the name were not changed. The labelled document keeps its labels and no
declassification is recorded. `name_notice` is sent only then, so a
`webapi.RemoteService` client built before this release, which refuses unknown
fields, fails only on such a reply.

**Reaching a specific document.** A library caller can ingest at a document's
source URI: its own (`Client.OwnUploadSourceURI(ctx, name)`), or another
uploader's namespaced URI when it is eligible.
`Client.IngestUpload(ctx, source)` is `Ingest` that also returns the route
(`authorized.UploadRoute`). Over HTTP and MCP there is no per-file target yet.
There, a name reaches another uploader's document only by steps 2 and 3, so
an ambiguity is resolved by whoever holds the colliding documents (relabelling
or re-uploading under another name) or through a library host.

What stays the same:

- **Shared documents.** Principals authorized under a document still share it
  by name. A re-upload, a relabel by any holder of its labels (#630, #680),
  the reason policy and its ordering (#664, #685), the historical refusal
  (#674) and per-file batch outcomes (#687, #689) all act on that document.
- **Uploads stored before this release** keep their un-namespaced URIs, read
  exactly as before, and stay re-uploadable and relabelable by name by
  everyone authorized to ingest under them. Nothing is rewritten on upgrade.
  Uploads from catalogs older than source claims (a registration and no
  claim) are covered the same way, by their registration's rule.

What changes:

- New uploads get namespaced source URIs (`source_uri` in document listings
  and the declassification listing). Titles and the `source_label` metadata
  are still the filename.
- Two principals who cannot ingest under each other's document of a name now
  get two documents, where the second used to be refused with `not_found`.
- **Your own document comes first.** If someone else relabels your own
  document of a name to a rule you are not authorized under, your uploads of
  that name are refused with `not_found`, as an ingest over any document you
  cannot read is. That lasts until a holder relabels it back or you get the
  labels back. You can upload under another name meanwhile. The route does
  not skip your own document in that case, because the upload would then
  silently write another uploader's document of the name, or land on your
  own URI anyway. This was already so before this release, at the shared URI.
- Library callers: an ingest at a namespaced URI outside the caller's own
  namespace that is not eligible is `InvalidArgument`. It is the same whether
  a document the caller is outside is stored there or nothing is. Any other
  URI under `upload://workspace/` that is not a bare name is `InvalidArgument`
  before anything is read. The policy selector still sees the source as
  requested.

Not changed, and documented as trusted ingest only: source URIs outside
`upload://workspace/` (`file://`, `https://`, a host's own schemes) are stored
as given, and ingesting one over a hidden document is still `not_found`. No
HTTP or MCP route lets a caller choose such a URI; they come from operator
configuration, `cmd/shoal-explore` (the unwrapped corpus) and library hosts.
A host that ingests caller-chosen URIs of its own must namespace them itself.

**Residuals.**

- The namespace binds the identity, not its current authority, as #699's
  does. A narrowed session of the same identity (a token with fewer label
  claims, say) routes to the same namespace, so it can still tell whether
  that identity's broader sessions uploaded a name it cannot read. Unlike
  `Connect`, this is reachable over HTTP and MCP, but only about the caller's
  own identity's uploads.
- A namespaced source URI shows readers of a document an unkeyed digest of
  its first uploader's identity. They can tell which readable uploads came
  from the same identity, and confirm a guess of who that is by recomputing
  the digest. The same holds for #699's application edge IDs. Keying both
  digests is tracked on #721.

### `Connect` edges to extracted and materialized nodes stay in the graph (#724)

`Connect` accepted an application edge whose endpoint was an extracted
ontology entity or a materialized graph node, and the edge showed up right
away. The next graph rebuild then dropped it: any ingest, extraction,
interaction, fold, materialization or restart. The base graph rebuilt
application edges before it placed the extracted and materialized nodes, so
the edge's endpoint was "missing". The edge record stayed on disk and its
policy-catalog registration stayed in the catalog, but no read served it.
Edges between documents, sections and spans were not affected. The bug dates
from the first extraction release (#311).

Now:

- The rebuild places every node before any application edge. Edges that were
  hidden come back after the upgrade, with no migration step.
- What a reappearing edge shows is still decided by its catalog registration
  and, since #695, its endpoints' revisions. The label migration and an
  authorized relabel narrow registrations from the catalog, not from the
  graph, so they covered hidden edges too. An edge to an entity of a document
  labelled in the meantime comes back closed to readers without the label.
- An edge ID is written once. The unwrapped `Explorer.Connect` used to check
  only the graph, so an edge the graph did not show could be overwritten with
  different content. It now checks the stored record too and returns
  Conflict. The authorized `Connect` was already refused by the catalog
  reservation and never reached the base.
- When an application edge and a derived edge (intrinsic, extracted,
  materialized or derived assertion) hold the same ID with different
  content, the derived edge wins. Before, an application edge silently
  replaced an intrinsic edge with its ID. The application edge stays stored
  but is withheld from the graph. The rebuild never fails over it, and the
  ingest, extraction or materialization that derived the edge is never
  refused over it. Identical content is one edge. Collisions show up in two
  places:
  - `Explorer.ApplicationEdgeCollisions(ctx)`, an operator diagnostic that
    the authorized client does not expose;
  - a warning log line, `explorer: application edges withheld ...`, with
    the count and the first 16 IDs, written each time the set changes.

  New collisions can't be made through `authorized.Client`, whose
  application edge IDs live in the writer's namespace (#699). `Connect`
  refuses an ID that any stored application edge or shown edge already
  holds, but an unwrapped `Explorer.Connect` can still take the ID of a
  derived edge that doesn't exist yet. That edge is withheld and reported
  once the derived edge appears. Old state can also hold one. Before #699 an authorized `Connect` accepted any ID, and a document's
  root `contains` edge ID can be derived from its URI, so an application edge
  could take the intrinsic ID of a document ingested later. Through the
  authorized client the policy catalog gives the derived edge the ID too
  (#732, below). Such a legacy pre-#699 application ID is not in
  any writer's namespace, so it appears verbatim in the collision log line,
  usually as a document's intrinsic edge ID.

**If an operator finds a collision:**

- **The derived edge is served and the application edge is withheld.** The
  application edge's ID can't be reused. Connect the edge again under a new
  local ID: `ApplicationEdgeID(ctx, "<new local id>")`, or any free ID on the
  unwrapped explorer.
- **Through `authorized.Client`** the same holds since #732: see below.

**An edge overwritten while it was hidden.** A base edge rewritten by an
unwrapped `Explorer.Connect` while the bug hid it no longer matches its
catalog registration. The authorized client treats it like any base edge
that disagrees with its registration: it fails closed and serves neither the
base content nor the registered content. Neighborhood and bounded
neighborhood reads leave the edge out and serve every other edge as usual.
An interaction whose graph evidence uses the edge fails verification as not
found. Remedy: the ID can't be reused, so connect the intended edge again
under a new local ID (`ApplicationEdgeID(ctx, "<new local id>")`). The stale
edge stays withheld. The unwrapped explorer serves the base content because
it has no catalog. This only arises on a deployment that called the
unwrapped `Explorer.Connect` directly on a corpus also served through
`authorized.Client`.

### `Connect` edge IDs live in the writer's namespace (#699)

`authorized.Client.Connect` returned Conflict for any edge ID already in the
corpus, whatever its rule. Intrinsic `contains` edge IDs are derived from a
document's URI and self-edges are allowed, so a reader with one readable node
could test whether a hidden document existed at a guessed URI, or whether a
hidden application edge ID existed. Each probe that succeeded also created an
edge.

**API change.** `Connect` now accepts only an edge ID in the caller's own
namespace, which `Client.ApplicationEdgeID(ctx, localID)` returns:
`app-edge_<digest of the caller's identity>/<localID>`. Any other ID is
`InvalidArgument: graph edge ID is outside the caller's application edge
namespace`, before an endpoint or the policy catalog is read. So:

- the refusal depends on the caller's identity and the ID it sent, never on
  what the corpus holds;
- no server-derived ID (document, section, span, intrinsic edge, extraction,
  derivation, interaction, materialized) can be in the namespace;
- another identity's namespace differs by a 128-bit digest, so an accepted ID
  can only collide with an edge connected under the same identity. A
  writer's identical retry still succeeds, and a different edge under its
  own ID still conflicts.

The identity is the authorization domain, subject, actor, client ID,
on-behalf-of chain and service role and ceiling. It leaves out the request,
the grants and the policy generation, so a retry under new grants lands in
the same namespace.

**Caveat.** The namespace binds the identity, not its current authority. A
narrowed session of the same identity (workspace narrowing, a down-scoped
token, a revoked label) shares the namespace of that identity's broader
sessions, so it can still tell whether one of their local edge IDs exists:
Conflict for a different edge under that ID. This is library-only today
(`Connect` has no HTTP or MCP route).

`Connect` no longer discloses a hidden document by URI. Neither does an
upload by name since #712 (above).

Migrating callers:

- call `ApplicationEdgeID` and connect under the result. Reads (for example
  `Neighborhood`) report the namespaced ID;
- a trusted `EdgePolicySelector` that selects by edge ID gets the namespaced
  ID. `authorized.ApplicationEdgeLocalID(ctx, decision, id)`, with the context
  `Connect` passed the selector, returns the local ID (#721);
- application edges connected before this release keep their IDs and stay
  readable as before. A retry of one now gets `InvalidArgument` instead of
  succeeding;
- the conformance harness (`internal/explorerconformance`) maps fixture edge
  IDs through `ApplicationEdgeID` when the client under test provides it.

The unwrapped `explorer.Client` is unchanged: it has no authorization and
accepts any edge ID.

### Interactions, folds and edges stay bound to the revision they came from (#695)

An interaction (and every fold of it) and an application edge (`Connect`)
were authorized on read against their inputs' current rules only. After an
authorized relabel that removed a label, the labelled revision stayed closed
but everything derived from it opened. Now:

- A session records, when it is written, the revisions of every document it
  touched or cited; every read also requires each of those revisions' rules
  (and, for a revision that is no longer current, the document's current
  rule). Folds follow their members.
- An application edge records the revision each endpoint belonged to when it
  was connected, and its effective rule includes those revisions' rules.
- Outputs recorded before this release carry no such record:
  - A session is held to every revision of its documents up to the one that
    was current at the session's snapshot (by the corpus's publication
    sequence, which a wall-clock step cannot reorder), plus the current one.
  - An application edge is held to every revision its endpoints' documents
    ever had, plus the current one. So an edge connected before this release
    to a document that was ever labelled stays closed to a reader without
    that label.
  - Edges extracted or materialized before registration kinds existed are
    not affected.

**Policy catalog format: back up before upgrading.** The first time this
release opens a policy catalog read-write (`shoal-explore-web`,
`shoal-mcp`), it writes a format-floor row, `sourceclaim/\xffformat-floor`
with record kind 12, and logs that it did. Older `shoal-explore-web` and
`shoal-mcp` binaries, and their `-list-*` offline listings, then refuse to
open the catalog with `internal: stored policy source claim record is
corrupt`. The catalog is **not** corrupt: that refusal is deliberate, because
an older binary would otherwise ignore the new bindings and reopen what they
close. Back up the policy directory (`-policy-dir`, or the policy catalog
under `-state-dir`) before upgrading. To roll back, restore that backup; do
not delete the row. The offline listings of this release do not write the
row. The corpus volume does not need to be restored with it: a document
ingested after the backup has no registration in the restored catalog, so it
is withheld from every reader (fail closed) until it is ingested again.
Restore the corpus volume from the same point if you want the two to agree.

### Copy-down through edges, interactions and extraction is not a declassification (#663)

#663 asked whether labelled content written into weaker-labelled material
through an operation that names its inputs should be audited like a relabel
(#630). It should not: none of these writes serves its content to a new
reader when it is made.

- An application edge (`Connect`) is stored under the edge policy, which never
  carries a label, but every read also requires both endpoints' current rules
  and, since #695, the rule of the revision each endpoint was connected to.
- An interaction's output restriction may omit its evidence's labels (an MCP
  tool call records only the workspace output policy), but every read also
  requires every touched node's and edge's current rule and, since #695, the
  rule of every revision it was derived from.
- Extracted entities and relations are registered under their document's own
  rule and follow it through a relabel.

So these writes record no declassification, and
`-require-declassification-reason` does not refuse them. New tests pin, for
each operation and both policy stores, that no content of the output is
served to a reader without the label, although its stored rule is weaker.
This is a claim about content, not existence: see #698, #699 and #700
below. The binding is
documented in the deploy guide and in `pkg/explorer/authorized`. Nothing
changes in behaviour, wire formats or the policy catalog.

Found while checking this and filed:

- #695: interactions and application edges follow their inputs' current
  rule, not the rule of the revision they were derived from. So a relabel
  that removes a label opens what was derived from the labelled revision,
  while that revision itself stays closed. Fixed in this release (above).
- #696: a fleet action's `Output` is served to readers whose evidence refs
  are withheld.
- #698, #699, #700: existence and count oracles. A bounded neighborhood's
  scan limit reveals the exact count of hidden edges and interactions on a
  visible node. A `Connect` ID colliding with a hidden edge reveals that
  edge, and through intrinsic edge IDs, a hidden document by URI. `Connect`
  no longer discloses either (#699, above), and neither does an upload by
  name (#712, above). A shared
  extracted entity first owned by a later-labelled document reveals the
  entity key.
- #694: heuristic detection of copy-down that names no input (session
  correlation, content similarity), as operator signals that never block a
  write.

### An upload batch reports each file, and refusals no longer stop it (#687)

A workspace upload stopped at the first file that failed, and the workspace UI
then marked every file of the batch failed, including the ones it had
applied. After #674, re-uploading a directory after switching branches back
showed one refused file per attempt.

- Two per-file refusals no longer stop the batch: a file whose content matches
  an earlier revision, and a relabel that needs a declassification reason. The
  authorized client marks each as leaving nothing committed
  (`explorer.IsRefusedWithoutEffect`), only after the claim has rolled back
  with nothing registered. Any other failure still stops the batch, and the
  files after it are not tried.
- A batch that did not fully succeed is still an error, never a `200`. Its
  body keeps `code`, `message`, `indeterminate` and the commit-outcome header,
  and adds `files`. That is one entry per file, with a `status`: `applied`,
  `unchanged`, `historical`, `refused` with a `reason`, `failed`, or
  `not_attempted`.
- A batch is indeterminate only when a file's own outcome is unknown, which is
  when the failure that stopped it was indeterminate. Before, every failure
  after the first file was. A batch whose files were applied or unchanged
  beside refusals now keeps the refusal's definite status (for example a
  `409`) and is marked partial: `Shoal-Commit-Outcome: partial` (a new header
  value) and `"partial": true`. Retrying it converges, because applied files
  come back unchanged. A batch where every file was refused has no header.
- The workspace UI lists each file's own outcome, with the refusal's reason,
  and counts refused files apart. It says when files of a failed batch took
  effect and refreshes the document list.
- The MCP `shoal.ingest` tool returns the same `files` in its error. It
  reports a batch whose every file was tried (and has a known outcome) with
  its definite code, partial or not. A batch stopped by a failure stays
  `indeterminate`. `webapi.RemoteService` passes an upstream's `files` on
  after checking them against the request and keeps the upstream's verified
  definite or partial outcome. With an upstream built before #687 (no
  `files`), a multi-file failure stays indeterminate, over MCP too.
- The authorized client conceals the no-effect marker on any error it did not
  mark itself, including one from its base.
- `explorer.Ingest` marks an error after its durable document write (a failed
  graph rebuild) indeterminate. Graph materialization and extraction do the
  same after their records are durable. Before, such an error was definite
  although the revision was published.
- A snapshot read that fails after every file of an upload went through is
  reported with the files' outcomes, as partial when any was applied or
  unchanged. Before, it was a bare `503` with no commit-outcome header.
- Every error an authorized ingest returns after its base may have written is
  now indeterminate: a failed read, audit, registration or guard after an
  applied write, a commit-time reason refusal, and a failed re-registration.
  Before, some of these were returned as definite errors. With the batch rule
  above, a batch stopped by one of them would otherwise have been reported as
  definite.

### Re-ingesting an earlier revision says so (#674)

Re-ingesting content byte-identical to an earlier revision of a document used
to report success while the document kept serving its current revision. An
operator undoing a relabel (secret, then public, then the secret content
again) was told it worked.

- `explorer.Ingest` reports a new disposition, `historical`, when the content
  matches a non-current revision. It is still a success and never makes the
  old revision current again. There is no restore operation; change the
  content by any byte to make it current.
- The authorized client refuses that case with a `conflict` error wrapping
  `authorized.ErrHistoricalRevision`, and changes nothing.
- The browser upload route returns it naming the file (a 409 when it is the
  first file of the batch; see the next item otherwise): `upload file
  "<name>" matches an earlier revision of its document, so the document was
  left unchanged; change the content to upload it`. This is common when a
  directory is re-uploaded after switching branches back.
- A multi-file upload that fails on any file after the first now reports an
  indeterminate commit outcome, as the remote client already did, because
  earlier files of the batch may have been applied (an earlier file that was
  unchanged wrote nothing). #687 replaces this with a definite, partial
  outcome, and lets the batch go on past the two refusals below; see its
  entry above. Like every indeterminate write it is a 503
  `unavailable` with the `Shoal-Commit-Outcome: indeterminate` header and
  `"indeterminate": true`, and it keeps the file's message. This covers the
  historical refusal and the missing-reason 400. A failure on the first file
  keeps its own status. Those two refusals are deterministic: retrying the
  same batch returns the same response. Change the named file's content, or
  supply a reason, before retrying.

### The offline catalog listings refuse a running workspace (#686)

`-list-declassifications` and `-list-untranslatable-labels` checked only that
the policy catalog's WAL was empty. A workspace that was idle passed that
check and could then write before the listing opened the catalog. The listing
would replay those writes and, on exit, flush them and truncate the WAL the
workspace was still appending to.

- Every process that opens the policy catalog (`shoal-explore-web`,
  `shoal-mcp` and the listings) now holds an exclusive lock on
  `.shoal-policy-catalog.lock` in the policy directory while the catalog is
  open. A second opener is refused with "is already open". That includes a
  second workspace started on the same directory.
- The listings try the lock without waiting and refuse a running workspace,
  idle or not. With the lock held they check the catalog again before opening
  it. They write nothing, not even the lock file: they refuse a catalog
  without one. **After upgrading, start and cleanly stop the workspace once
  before running a listing.**
- The kernel releases the lock when its process exits, however it exits. A
  lock file left behind by a crash does not block the next start.
- The lock file is created mode `0600` by the workspace's user. A listing run
  as a different (non-root) user gets a permission error, not "already open".
  Run the listings as the workspace's user. Any lock failure other than
  another holder, such as a filesystem without lock support, is reported as
  itself.
- **Rolling back reopens #686.** A workspace older than this release ignores
  the lock. If you roll back after a newer release has created the lock file,
  a current listing will take the lock and open the catalog under the running
  old workspace. Stop the workspace before running a listing.

### Declassification reason codes and an offline listing (#664)

Every relabel that removes visibility labels has been audited since #630. You
could not send a reason with one, though, and nothing shipped could read the
records.

- The browser upload route, the MCP `shoal.ingest` tool and the remote web
  API client take an optional `declassification_reason`. It is a multipart
  form field on the route, with an optional input in the workspace upload
  form, and a tool argument on MCP. The reason is recorded
  on the audit. A malformed reason refuses the batch with a 400 /
  `invalid_argument` before anything is ingested.
- `-require-declassification-reason` (chart:
  `explorer.declassification.requireReason`) refuses such a relabel unless
  it carries a reason. It is off by default.
- `shoal-explore-web -list-declassifications` prints the records, with the
  labels each one removed, and exits. It is offline only, like
  `-list-untranslatable-labels`. No online read of the records exists.
- Retention is not decided. The records are append-only and loaded on every
  start. `docs/shoal-explore-web-deploy.md` gives the expected volume, and the
  decision is open on #664.

### The LLM gateway's listener serves TLS (#424)

`shoal-llm-gateway` refused to send a prompt to a remote provider over
plaintext but accepted the same prompt over plaintext on any address. The
listener now follows the rule the process applies to the hops it makes: TLS
anywhere, plaintext on loopback only.

- `-tls-cert-file` and `-tls-key-file` serve TLS, both or neither. A missing
  file, a malformed certificate or a key that is not the certificate's is
  refused at startup. The pair is re-read on every handshake, so a rotated
  cert-manager Secret is served without a restart; a pair caught halfway
  through a rotation keeps the previous one serving.
- A plaintext listener on a non-loopback address is refused at startup unless
  `-allow-plaintext-listener` acknowledges a mesh sidecar that terminates mTLS.
  It opens the listener only, not the provider hop.
- The chart serves TLS by default from `llmGateway.tls.secretName`, a
  `kubernetes.io/tls` Secret, and names the port `https`. `tls.enabled: false`
  renders the acknowledgement instead. A non-ClusterIP Service with a plaintext
  listener is refused unless `llmGateway.service.allowPlaintext: true`
  acknowledges a terminating load balancer.

**Upgrade action required for every chart deployment of the LLM gateway.** An
existing values file stops rendering with `llmGateway.tls.secretName is
required`. Either name a TLS Secret and move callers to `https://`, or set
`llmGateway.tls.enabled: false` behind an mTLS mesh. A gateway run outside the
chart with a non-loopback `-listen` and neither TLS nor the acknowledgement now
exits at startup. See "Upgrading" in `docs/llm-gateway-deploy.md`.

### A non-sub subject claim has its own identity namespace (#546)

`-oidc-subject-claim X` (any claim but `sub`) used to name principals
`oidc:<iss>#<value>`, the same namespace as `sub`-derived principals. After
a switch from `sub` to `X`, or from one claim to another, the old identities
were still inside the namespace in force, so the approval service's
namespace rule (#553) could not refuse them. A human's old registrations
then counted as current identities under the new scheme, which is the same
self-approval shape #553 closed for `oidcid:`. Now:

- A non-default subject claim names principals `oidc:<iss>#<tag>#<value>`,
  where the tag is 16 hex digits of a digest of the claim name. Its actor,
  client and delegation values take the same prefix. Its requests are
  stamped with its scheme.
- The `sub` namespace `oidc:<iss>#` is flat (legacy Entra excepted). A `sub`, actor, client or
  delegation value containing `#` is refused on both branches, and an
  identity with a `#` after the issuer's is foreign under `sub`. So
  `oidc:<iss>#<sub>` and `oidc:<iss>#<tag>#<value>` cannot be confused.
- The default `sub` scheme and legacy Entra mode keep the same recorded
  scheme digest and start without any flag. Every identity without a `#`
  is unchanged.

**Behaviour change on the default `sub` scheme: `#` is refused.** A token
whose `sub`, or whose `-oidc-actor-claim`, `-oidc-client-id-claim` or
`-oidc-delegation-claim` value, contains `#` is now refused with `401`, on
the workspace branch and on the approver branch. The scheme digest does not
change, so **nothing at startup warns about this**. If the client-ID or
actor claim you configured routinely carries `#`, every user is locked out.
Before upgrading, check that none of these claims can contain `#`. If one
can, configure a different claim. The first refusal for each claim is
logged at WARN, naming the claim but never the value, and the log repeats
at most once a minute per claim. `shoal-demo-seed` likewise refuses an
`oidc_subject_id` containing `#`, and an issuer containing one.

**Upgrade action required for deployments that set `-oidc-subject-claim`.**
The scheme digest covers the identity format, so such a deployment's
recorded scheme no longer matches, and the server refuses to start. The
refusal explains this and prints the digest to pass as
`-oidc-identity-scheme-migrate`. After the migration, identities minted
before the upgrade are refused wherever they are involved in an approval
until #526's adoption route lands, and requests pending at the upgrade can
only expire. See "Upgrading a deployment that sets `-oidc-subject-claim`" in
`docs/shoal-explore-web-deploy.md`.

### Free-form visibility labels are enforced (#570)

`metadata["shoal.visibility"]` labels used to be stored on a document's nodes
and nowhere else, so every holder of the document's source could read it. They
are now enforced on every read path by translating each label into a
structured policy on the document's source and conjoining it into the
document's access rule. This ships as three changes that must be released
together:

- **Ingest** (#585). A labelled ingest is registered under the source policy
  and one policy per label; the ingester must hold every label it writes.
- **Grants** (#590). Labels are granted only by the operator: the
  `shoal.label-grants/v1` file (`-oidc-label-grants-file`), `-dev-auth-labels`
  for local development, and `shoal-mcp -identity-labels`. A label no grant
  names is visible to nobody.
- **Migration** (this change). On every start, before serving,
  `shoal-explore-web` and `shoal-mcp` narrow the catalog rule of each document
  labelled under an earlier release, including its historical revisions,
  extracted entities and relations, and source claim. A document whose labels
  cannot be translated becomes readable by nobody and is listed by
  `shoal-explore-web -list-untranslatable-labels`. It runs on every start, so
  documents labelled by an older binary after a rollback are closed on the
  next start; any error refuses to start. A document whose corpus revision
  is newer than its registered one (a failed registration) is narrowed by
  its registered labels and reported for a retried ingest, never locked.

**Upgrade action required.** Write the label grant file before starting the
new release, then review the untranslatable list and rebuild lexicon bundles.
The steps are in "Upgrading: labelled documents are tightened at startup" in
`docs/shoal-explore-web-deploy.md`.

This differs from earlier tightenings such as #544, which applied at the next
registration and left stored records resolving. Labelled documents already in
the corpus would stay readable under that model, so this one rewrites stored
authorization at startup instead.
