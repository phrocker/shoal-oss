# Release notes

## Unreleased

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
