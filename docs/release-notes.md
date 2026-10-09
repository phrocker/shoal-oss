# Release notes

## Unreleased

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
  next start; any error refuses to start.

**Upgrade action required.** Write the label grant file before starting the
new release, then review the untranslatable list and rebuild lexicon bundles.
The steps are in "Upgrading: labelled documents are tightened at startup" in
`docs/shoal-explore-web-deploy.md`.

This differs from earlier tightenings such as #544, which applied at the next
registration and left stored records resolving. Labelled documents already in
the corpus would stay readable under that model, so this one rewrites stored
authorization at startup instead.
