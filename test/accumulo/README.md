# Exact Accumulo 4 local conformance harness

This opt-in harness starts a disposable ZooKeeper, HDFS, and Accumulo cluster
for local conformance work. Accumulo is built from the same exact source
revision as Shoal's vendored Thrift IDLs:

- Accumulo `4.0.0-SNAPSHOT`
- revision `1a716b2c1bb5762ead4b46d2bc4f53e13873b314`
- Hadoop `3.4.2`
- ZooKeeper `3.9.5`
- Thrift `0.17.0` (in the pinned Accumulo build)

The image build verifies SHA-512 hashes for both the Accumulo source archive
and Hadoop binary archive. It requires network access to Maven Central, Apache
archives, and GitHub while building.

## Commands

```bash
make test-accumulo-static  # config and command tests; Docker is not required
make accumulo-up           # clean old state, build, start, and wait
make accumulo-smoke        # run the Java client smoke against a running cluster
make accumulo-down         # remove containers, orphans, and named volumes
make test-accumulo         # full lifecycle with guaranteed cleanup
make conformance-replay    # deterministic per-gate JSON from replay fixtures
make conformance-live      # live JSON; invokes this Docker harness for client
```

`make test-accumulo` first removes stale project resources, starts the exact
cluster, polls with an unmodified Accumulo 4 Java client until the metadata
table is readable and a tablet server is registered, then:

1. creates a deterministic temporary table;
2. writes one mutation with `BatchWriter`;
3. explicitly flushes the writer and table;
4. scans and verifies the exact key/value;
5. deletes the table in a Java `finally` block.

The lifecycle runner prints Compose logs on failure and executes `down
--volumes --remove-orphans` on success, failure, Ctrl+C, and termination.
Container ports bind only to loopback. Set
`SHOAL_ACCUMULO_READY_TIMEOUT=<seconds>` to change the 300-second readiness
deadline.

## Docker availability

The static target always runs without Docker. Live targets require the Docker
CLI, Compose v2, and a reachable daemon. If any are unavailable, the runner
prints:

```text
SKIP (needs-docker): ...; live Accumulo 4 test was not run
```

and exits with status 2. This is an explicit unexecuted live test, not a
passing result.

## Machine-readable replacement verdicts

`cmd/shoal-conformance` emits schema-versioned JSON for the `tserver`,
`scanserver`, `compactor`, `promotion`, and `client` gates. Output order is
stable and includes the Shoal commit, exact Accumulo version and revision,
GOOS/GOARCH, execution mode, every evidence selector and replay command, the
checked-in fixture path, and its SHA-256 digest.

Replay mode executes the commands in
`test/conformance/fixtures/<gate>.json`. Those fixtures bind each verdict to
named tests instead of treating a package-wide success as evidence. Run:

```bash
go build -o bin/shoal-conformance ./cmd/shoal-conformance
bin/shoal-conformance -mode replay -output verdict.json
```

Live mode reuses `python test/accumulo/harness.py test` for the client gate.
The current Docker harness does not yet install Shoal processes as live
tserver, scanserver, or compactor replacements, so those live role gates are
reported as `unsupported`, never `pass`. Promotion is likewise unsupported
until a live destination adapter exists.

### Release-gate semantics

Each selected gate is `required` and has exactly one state:

- `pass`: every referenced replay command passed, or the live command ran
  successfully against the pinned Accumulo build;
- `fail`: evidence was malformed or a required command ran and failed;
- `unsupported`: the environment or live adapter cannot execute the gate;
- `skipped`: the gate was omitted with `-required`.

Exit status is `0` only when every required gate passes, `1` when any required
gate fails, and `2` when no required gate failed but at least one is
unsupported or skipped. Consequently Docker absence and unwired live roles
cannot authorize a release. To evaluate a slice independently, pass a
comma-separated list such as `-required scanserver,client`; all gates remain
present in the JSON so consumers do not confuse omission with success.

## Debugging and overrides

The Compose project is always `shoal-accumulo4-test`, so cleanup is isolated:

```bash
docker compose --project-name shoal-accumulo4-test \
  --file test/accumulo/docker-compose.yml ps
docker compose --project-name shoal-accumulo4-test \
  --file test/accumulo/docker-compose.yml logs --no-color
```

The Accumulo image is deliberately not environment-overridable: a successful
run must exercise the repository's pinned source revision. Run
`make test-accumulo-static` after any harness configuration change.
