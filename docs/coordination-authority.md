<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Coordination authority and quorum model

Status: **accepted architecture direction**, 2026-08-19. Implementation is
tracked by [#128](https://github.com/phrocker/shoal-oss/issues/128).

## 1. Decision

Shoal uses **one coordination authority for each logical table at any point in
time**. It does not mirror ownership into two quorum systems and it does not
add a second consensus service beside an Accumulo deployment.

| Deployment domain | Coordination authority | Durable state |
| --- | --- | --- |
| Embedded, one process | OS/PVC exclusion plus a persisted manifest generation | WAL, immutable files, and manifests on the configured backend |
| Shoal-only Kubernetes pods | Kubernetes `coordination.k8s.io/Lease` objects for membership and election | CAS-protected manifests plus WAL and immutable files on PVC/object storage |
| Accumulo-connected Shoal | Accumulo ZooKeeper ServiceLocks and manager/coordinator authority | Accumulo metadata, FATE state, WALs, and RFiles |

Kubernetes already has a consensus system behind its API. A Shoal-only
Kubernetes deployment uses that existing service at low frequency for leases;
it does not deploy an application-visible etcd or Raft cluster. When Shoal is
connected to Accumulo, it uses the ZooKeeper ensemble Accumulo already
requires. Shoal never creates a parallel Kubernetes lease that can authorize
the same Accumulo-hosted table.

A dedicated etcd cluster is justified only if Shoal later supports a
multi-node, multi-writer standalone deployment outside both Kubernetes and
Accumulo. That is not a current requirement. A custom embedded Raft
implementation is explicitly out of scope.

## 2. Authority invariants

These invariants are release gates, not implementation preferences:

1. **One writer domain.** A logical table cannot be writable under local,
   Kubernetes, and Accumulo authority simultaneously.
2. **Immutable proof.** Every assignment, mutation, compaction job, promotion
   step, and completion carries an authority token that can be rejected after
   lease or session loss.
3. **Monotonic epochs.** Recovery cannot reuse an epoch previously allowed to
   mutate state. Reconnection establishes a new epoch; it never revives work
   issued under an expired one.
4. **Fail closed.** If a process cannot prove current authority, it withdraws
   readiness, rejects new mutations, and cannot publish asynchronous results.
5. **Bounded quorum state.** Coordination stores contain membership, leases,
   service descriptors, and small fencing records. WALs, table manifests,
   files, scan state, and workflow payloads remain in their authoritative data
   stores.
6. **Authority-preserving delegation.** ZooKeeper presence does not make Shoal
   the Accumulo manager. Placement, metadata mutation, imports, and online
   compaction completion still flow through manager/coordinator/FATE APIs.
7. **No clock-only fencing.** Lease timeouts detect loss, but an API-assigned
   identity and a CAS-protected generation fence writes. Wall-clock time alone
   never proves ownership.

## 3. Common vocabulary

Implementations should share a small coordination vocabulary without forcing
all backends to pretend they have identical consistency primitives:

```go
type AuthorityToken struct {
    Domain     string
    Resource   string
    Owner      string
    LeaseID    string
    Generation uint64
    Attempt    string
}

type Coordinator interface {
    Acquire(context.Context, string) (Lease, error)
    Watch(context.Context, string) (<-chan Event, error)
    Members(context.Context, string) ([]Member, error)
}

type Lease interface {
    Token() AuthorityToken
    Renew(context.Context) error
    Release(context.Context) error
    Lost() <-chan struct{}
}
```

This is a design shape, not a committed public Go API. Backend-specific proof
must remain available:

- an embedded token includes the persisted manifest generation and lock-file
  identity;
- a Kubernetes token includes the Lease UID/resource version observed during
  acquisition and the CAS-protected manifest generation. Kubernetes resource
  versions are opaque identities, not integers to compare;
- an Accumulo token includes the exact ServiceLock identity and sequence plus
  the manager-issued assignment or coordinator-issued job attempt.

The `Attempt` component prevents a delayed completion from applying to a later
operation that happens to run under the same process lease. This matches the
tablet-hosting attempt fence in
[`tserver-hosting-lifecycle.md`](./tserver-hosting-lifecycle.md).

## 4. State placement

Quorum availability and durable-data availability are different concerns.
Shoal must not make a coordination service carry data-plane load.

| State | Correct home |
| --- | --- |
| Process membership and endpoint descriptors | ZooKeeper ServiceLock or Kubernetes Lease-associated discovery |
| Leader/active-writer lease | Current domain's coordinator |
| Authority epoch and terminal handoff record | CAS-protected durable manifest, with the active lease identity |
| Local mutations and recovery | WAL plus manifest/file generations |
| Accumulo tablet placement and files | Accumulo manager, metadata, and FATE |
| Online compaction lifecycle | Accumulo coordinator/manager protocol |
| Promotion payload | Immutable export files, checksums, and manager FATE operation identity |
| Scan sessions and query intermediates | Owning process with bounded expiry; never ZooKeeper or Kubernetes Lease data |

## 5. Failure behavior

### 5.1 Embedded process

The process acquires exclusive OS/PVC ownership before opening a writable
engine. Every durable manifest replacement increments a generation with atomic
replacement or backend compare-and-swap. A second process cannot open the same
writer domain merely because it can read the files.

### 5.2 Shoal-only Kubernetes

Use the standard Kubernetes leader-election/Lease protocol. A pod that cannot
renew before the configured deadline:

1. withdraws readiness;
2. stops admitting mutations and authority-sensitive background work;
3. prevents completions carrying the lost token from publishing;
4. drains or aborts in-flight work within a bounded period; and
5. requires a fresh Lease observation and a new durable generation before it
   can become writable again.

An API-server partition is therefore an authority failure even if the pod can
still reach its PVC or object store. The durable manifest CAS protects against
an old pod racing a replacement after connectivity returns.

### 5.3 Accumulo-connected processes

Production Accumulo should retain its normal odd-sized ZooKeeper ensemble
(typically three or five members with failure-domain separation). A
single-member ensemble is acceptable only for local development.

ZooKeeper session expiration is an immediate fence. A Shoal tserver drops its
hosted claims, a compactor cannot commit an old result, and a reconnected
process receives a new ServiceLock generation. The exact lock generation,
manager identity, and per-operation attempt travel with work; seeing the same
tablet or job identifier later is not sufficient authority.

ZooKeeper is not used as a replacement for Accumulo metadata or FATE. In
particular, promotion never writes metadata or ZooKeeper directly, and an
online compactor never commits file references without the authoritative
manager/coordinator boundary.

## 6. Local-to-Accumulo authority handoff

Promotion is a transfer of authority, not data copying followed by eventual
guesswork. The durable state machine is:

```text
LOCAL_WRITABLE
    -> LOCAL_FROZEN
    -> IMPORT_SUBMITTED
    -> IMPORT_VERIFIED
    -> ACCUMULO_WRITABLE
    -> LOCAL_RETIRED
```

The handoff proceeds as follows:

1. Fence the local writer with its current lease and manifest generation.
2. Stop admitting local writes and durably record `LOCAL_FROZEN`.
3. Flush/checkpoint an immutable generation and verify its checksums.
4. Stage the export and submit the manager-authoritative FATE import.
5. Persist the destination FATE identity before interpreting ambiguous client
   results.
6. Verify the destination through Accumulo's authoritative state.
7. Durably record `ACCUMULO_WRITABLE`, revoke local authority, and only then
   enable distributed writes.
8. Retain an auditable lineage record tying local generation, export
   checksums, destination table, FATE identity, and terminal authority epoch.

Before import submission, a failed attempt may return to `LOCAL_WRITABLE` only
under a successful CAS proving the same local authority still exists. After an
ambiguous submission, local writes remain frozen until the FATE result is
reconciled. Guessing that the import failed could authorize both domains.

No step grants Kubernetes and Accumulo concurrent write authority. Ongoing
fan-in is a new immutable transfer from a new source generation; it is not a
long-lived second writer.

## 7. Implementation order

The dependency order is maintained in issue #128:

1. **Architecture contract** — this document and links from existing designs.
2. **Common vocabulary and embedded fencing** — authority token, conformance
   suite, local lock, and durable manifest generation.
3. **Accumulo adapter** — complete #67 ServiceLock integration and carry its
   tokens through manager/coordinator work.
4. **Kubernetes adapter** — Lease membership/election and CAS manifest fences
   for Shoal-only pods.
5. **Promotion handoff** — extend #70 with the durable state machine and
   manager FATE reconciliation above.
6. **Operations and release gates** — extend #71 and #74 with diagnostics,
   partitions, session loss, rolling upgrades, and handoff crash tests.
7. **Dependent data planes** — unblock #69 and #72 only after their authority
   prerequisites pass.

Each phase must ship as independently tested slices. A coordinator abstraction
alone does not satisfy a phase; the loss, restart, partition, and stale-token
tests are part of the deliverable.

## 8. Rejected alternatives

### Mirror every Accumulo lock into Kubernetes

Rejected because two independently available control planes can disagree.
There is no atomic transaction spanning ZooKeeper and the Kubernetes API, so a
network partition can make both sides believe they own the table.

### Store table manifests in Lease objects

Rejected because Leases are small, high-level coordination records. Using the
Kubernetes API as a table metadata database couples data-plane progress to API
availability and creates unbounded control-plane load.

### Run an embedded Raft group in every Shoal deployment

Rejected because it adds membership changes, snapshots, backup, upgrades,
quorum-loss recovery, and split-brain handling where Kubernetes or Accumulo
already supplies a supported coordination service.

### Let promotion accept writes on both sides and reconcile later

Rejected because timestamp, delete, visibility, and derived-index semantics do
not provide a general conflict-free merge. Explicit freeze and fenced cutover
are required.
