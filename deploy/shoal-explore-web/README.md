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

- `docker-compose.yml` — the local one-command profile (host networking, named
  volume `explorer-data`).
- `.env.example` — copy to `.env` to override `SHOAL_EXPLORE_LISTEN`.

The image (`Dockerfile.shoal-explore-web`, at the repo root) is the same artifact
a shared instance would run; only the flags differ. The shared path has open
prerequisites (a real authenticator, host-authority config, durable policy
store) documented in the guide.
