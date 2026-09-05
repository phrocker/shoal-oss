# Shoal MCP stdio server

`shoal-mcp` exposes an authorized embedded Explorer corpus as a
newline-delimited JSON-RPC MCP server on standard input and standard output.
It does not require a live Accumulo cluster or any network service.

```bash
go run ./cmd/shoal-mcp \
  -state-dir .shoal/mcp \
  -dev-auth
```

An MCP client should launch the command as a subprocess and exchange one
JSON-RPC message per line. Standard output is reserved exclusively for protocol
messages. Flag help, startup failures, storage failures, and other diagnostics
are written to standard error.

This shell smoke test performs the MCP initialization handshake and lists the
tools against a local embedded workspace:

```bash
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"smoke","version":"1"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' |
  go run ./cmd/shoal-mcp -state-dir .shoal/mcp-smoke -dev-auth
```

## Storage and policy configuration

The recommended `-state-dir` layout stores the embedded corpus in `corpus/`
and its durable authorization catalog in `policy/` under one root. Persist both
directories together.

| Flag | Environment | Purpose |
|---|---|---|
| `-state-dir` | `SHOAL_MCP_STATE_DIR` | Recommended state root containing `corpus/` and `policy/` |
| `-data` | `SHOAL_MCP_DATA` | Corpus directory when `-state-dir` is unset; defaults to `.shoal/explorer` |
| `-policy-dir` | `SHOAL_MCP_POLICY_DIR` | Explicit durable policy directory |

If an existing corpus contains documents but the selected policy catalog has
no registrations, startup is refused. Restore the matching policy directory or
ingest through an authorized workspace; the command does not silently grant an
unregistered corpus to its process identity.

## Process identity

`-dev-auth` (or `SHOAL_MCP_DEV_AUTH=true`) selects the fixed local development
process identity. Its domain, source, and policy defaults match the local
Explorer workspace defaults, allowing the commands to use the same persisted
corpus and policy catalog.

For an explicitly configured process identity, omit `-dev-auth` and provide:

| Flag | Environment |
|---|---|
| `-identity-subject` | `SHOAL_MCP_IDENTITY_SUBJECT` |
| `-identity-actor` | `SHOAL_MCP_IDENTITY_ACTOR` |
| `-identity-client-id` | `SHOAL_MCP_IDENTITY_CLIENT_ID` |
| `-identity-domain` | `SHOAL_MCP_IDENTITY_DOMAIN` |
| `-identity-source` | `SHOAL_MCP_IDENTITY_SOURCE` |
| `-identity-policy` | `SHOAL_MCP_IDENTITY_POLICY` |
| `-identity-operations` | `SHOAL_MCP_IDENTITY_OPERATIONS` |
| `-identity-generation` | `SHOAL_MCP_IDENTITY_GENERATION` |
| `-identity-lifetime` | `SHOAL_MCP_IDENTITY_LIFETIME` |
| `-identity-audit-purpose` | `SHOAL_MCP_IDENTITY_AUDIT_PURPOSE` |

Subject, actor, domain, source, and policy are required. The default explicit
operation set is read-only:
`list,read,neighborhood,retrieve,validation`. Generation defaults to `1`, and
each decision lifetime defaults to `15m`.

The launcher configuration is the trusted identity source in stdio v1. The MCP
server mints and binds a fresh authorization decision with a new request ID for
every `tools/call`, but every caller connected to the same process uses the same
configured identity. Stdio cannot independently authenticate remote callers.
A future HTTP transport is required for independently authenticated per-call
remote callers.

## Tool surface and recording

The command constructs a real embedded `Explorer`, wraps it with the authorized
client, and serves it through `webapi.EmbeddedService`. Optional tools are
advertised only when that service implements their public capability
interfaces; the embedded service currently supplies ingestion, extraction,
recomputation, and the authorized changes feed in addition to the read tools.

Tool-call recording is not implemented. The server does not claim to record or
persist MCP invocations.
