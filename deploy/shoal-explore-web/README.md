# shoal-explore-web deployment

Single-instance Shoal Explorer workspace: one binary, one state directory, no
external datastore. See [`docs/shoal-explore-web-deploy.md`](../../docs/shoal-explore-web-deploy.md)
for the full guide (image, configuration seam, the unsafe-configuration guard,
persistence and the split-brain guard, and Azure options).

## Local, one command

```console
docker compose -f deploy/shoal-explore-web/docker-compose.yml up --build
```

Runs the loopback-gated development authenticator (`-dev-auth`) over host
networking. On Linux, open <http://127.0.0.1:8098>. On Docker Desktop, "host" is
the Docker VM — see the guide.

Files:

- `docker-compose.yml` — the local one-command profile (host networking,
  `-state-dir /var/lib/shoal`, single state-root volume `explorer-state`).
- `.env.example` — copy to `.env` to override `SHOAL_EXPLORE_LISTEN`.

## Volume layout: one state root

Configure `-state-dir /var/lib/shoal` and mount that one directory. **Persist
`/var/lib/shoal` and both the corpus and the authorization catalog survive a
restart:**

- `/var/lib/shoal/corpus` — the document corpus.
- `/var/lib/shoal/policy` — the durable authorization catalog (#284 / PR #288). It
  is a **sibling** of the corpus, never inside it (the engine treats every corpus
  subdirectory as a table), and falls under the same single mount.

Mounting the corpus directory alone would drop every authorization registration
on restart; the command's split-brain guard then refuses to start (in production
mode) rather than serving an under-authorized corpus. `-data` and `-policy-dir`
remain available for legacy/custom layouts — see the guide.

The image (`Dockerfile.shoal-explore-web`, at the repo root) is the same artifact
a shared instance would run; only the flags differ. The shared path still has open
prerequisites (a real authenticator, host-authority config) documented in the
guide.
