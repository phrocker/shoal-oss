# shoal-explore-web deployment

Single-instance Shoal Explorer workspace: one binary, one data directory, no
external datastore. See [`docs/shoal-explore-web-deploy.md`](../../docs/shoal-explore-web-deploy.md)
for the full guide (image, configuration seam, the unsafe-configuration guard,
the persistence/#284 caveat, and Azure options).

## Local, one command

```console
docker compose -f deploy/shoal-explore-web/docker-compose.yml up --build
```

Runs the loopback-gated development authenticator (`-dev-auth`) over host
networking. On Linux, open <http://127.0.0.1:8098>. On Docker Desktop, "host" is
the Docker VM — see the guide.

Files:

- `docker-compose.yml` — the local one-command profile (host networking, single
  state-root volume `explorer-state` mounted at `/var/lib/shoal`).
- `.env.example` — copy to `.env` to override `SHOAL_EXPLORE_LISTEN`.

## Volume layout: one state root

The volume is the **state root** `/var/lib/shoal`, not the corpus directory. All
persistent state lives under it:

- `/var/lib/shoal/corpus` — the document corpus (`-data`).
- `/var/lib/shoal/corpus-policy` — the authorization catalog once the durable
  policy store (#284 / PR #288) lands. It is derived as
  `filepath.Clean(-data)+"-policy"`, a **sibling** of the corpus (the engine
  treats every `-data` subdirectory as a table, so it cannot nest), and so falls
  under the same single mount.

Mounting the corpus directory alone would preserve documents but drop every
authorization registration on restart. Mounting the state root keeps both,
robustly, under either the current sibling layout or a future
`<state-root>/corpus` + `<state-root>/policy` layout.

The image (`Dockerfile.shoal-explore-web`, at the repo root) is the same artifact
a shared instance would run; only the flags differ. The shared path has open
prerequisites (a real authenticator, host-authority config, durable policy
store) documented in the guide.
