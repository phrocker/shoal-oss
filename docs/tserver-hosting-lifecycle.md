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
# Tablet hosting lifecycle and fencing

Status: **partial**. The fenced lifecycle state machine
(`internal/tserver`) has landed, and so has the ZooKeeper registration
that makes it visible to a manager: the tablet-server ServiceLock, the
service descriptors the manager reads, and the manager-lock observation
that supplies live manager authority — see [§6](#6-registering-with-zookeeper).
The manager RPC surface and tablet data loading are not wired to it yet
— see [§7](#7-what-is-not-here-yet). Tracking issue: #67.

## 1. Goal

Let an unmodified Accumulo manager assign, monitor, migrate, and unassign
tablets hosted by a Shoal process, without ever letting a tablet end up
hosted in two places at once.

`internal/tserver.Host` is the piece that holds that invariant. It tracks
which tablets this process claims and refuses any transition it cannot
prove is current.

## 2. The manager is the only authority

Nothing in `internal/tserver` decides what to host. `Host` has no policy,
no balancer, and no way to give itself a tablet: `Assign` and `Unassign`
exist only to apply what the manager asked for, and the manager's
decisions are always applied when they can be applied safely.

The one thing `Host` will refuse a manager is a request it can prove came
from a **superseded** manager — see the manager fence in
[§4](#4-the-fence). That is not a competing authority; it is how the live
manager's authority is protected from a predecessor that has not noticed
it lost the lock.

## 3. States

The happy path is one line — assigned, loaded, drained, released:

```
              Assign      LoadComplete   Unassign(graceful)  UnloadComplete
 UNASSIGNED ──────────► LOADING ──────► HOSTED ───────────► UNLOADING ──────────► UNASSIGNED
```

| State | Meaning | Routable |
| --- | --- | --- |
| `UNASSIGNED` | not tracked here (the zero value, so an unknown extent reports as this) | no |
| `LOADING` | assigned by the manager, being brought online | no |
| `HOSTED` | online and serving | **yes** |
| `UNLOADING` | draining before release; still claimed, so not re-assignable | no |

Every transition, including the ones that leave the happy path:

| From | Call | To |
| --- | --- | --- |
| `UNASSIGNED` | `Assign` | `LOADING` |
| `LOADING` | `LoadComplete` | `HOSTED` |
| `LOADING` | `LoadFailed` | `UNASSIGNED` |
| `LOADING` | `Unassign(UnloadGraceful)` | `UNLOADING` |
| `HOSTED` | `Unassign(UnloadGraceful)` | `UNLOADING` |
| `UNLOADING` | `UnloadComplete` | `UNASSIGNED` |
| any assigned state | `Unassign(UnloadImmediate)` | `UNASSIGNED` |
| any assigned state | `LoseLock` (all tablets at once) | `UNASSIGNED` |

`Hosted()` returns only `HOSTED` tablets — the set the manager may route
to. `Tablets()` returns every tracked tablet with its state, so a slow
handoff can be told apart from a stuck one.

Two paths deliberately do **not** exist. A tablet cannot go from
`LOADING` straight to `HOSTED` after the manager unassigned it: the
unassignment moves it to `UNLOADING`, `LoadComplete` then fails with
`ErrWrongState`, and the loader releases it with `UnloadComplete` rather
than publishing a tablet the manager has already placed elsewhere. And a
tablet cannot be re-assigned while it is `UNLOADING`, because this
process has not let go of it yet.

Each trip through this table is one *attempt*, identified by the handle
`Assign` returns. A tablet that reaches `UNASSIGNED` and is assigned here
again starts a new attempt, and the completions of the old one no longer
apply to it — see §4.

## 4. The fence

Every manager-directed transition carries a `Fence` of two ServiceLock
identities. A `LockID` mirrors the ephemeral ZooKeeper node a lock holder
creates — `zlock#<uuid>#<sequence>` — and the sequence is the generation
counter: ZooKeeper hands out strictly increasing sequence numbers, so a
lock that was lost and re-acquired always carries a higher one.

**Server lock** — the tablet-server lock the manager believed this
process held. It must equal the lock held right now. An older generation
was minted against a view of the cluster that no longer exists; a
generation we do not hold cannot be verified at all. Both are refused.

**Manager lock** — the lock held by the manager that issued the request.
The live manager lock is observed externally from ZooKeeper, and requests
are accepted only when this lock exactly matches that authoritative live
manager identity. A delayed predecessor RPC is therefore stale as soon as
the live-manager observation moves on, even before the successor has sent
this host any request. If live-manager knowledge is temporarily cleared,
requests fail closed until a manager lock is observed again — but the
highest manager epoch already seen is retained, so re-observation cannot
roll authority backward to an older epoch.

Local completions (`LoadComplete`, `LoadFailed`, `UnloadComplete`) carry
an `Attempt` instead: an opaque handle minted by `Assign`, naming one
assignment of one tablet under one server lock. They report the outcome
of work this process started, and what matters is whether that work
still belongs to us.

The server lock alone is not fine-grained enough to decide that. The
manager may unassign a tablet and assign it here again without the lock
ever changing — a migration that is rolled back, or a reassignment after
a forced unload — so a completion left over from the first assignment
would find the extent, the lock and even the state all matching a second
time. `LoadFailed` would release a tablet that is still loading, and
`LoadComplete` would publish one whose data is not loaded yet. The
attempt id is minted per assignment and never reused, so a completion
can only ever apply to the assignment it belongs to; anything else is
`ErrStaleAttempt`.

A graceful `Unassign` hands the same attempt back, so the drain it starts
can be finished with `UnloadComplete`, and a load that was unassigned
mid-flight can give the tablet back with the handle it already holds.
When there is nothing to finish — the tablet was not tracked, or
`UnloadImmediate` released it outright — the returned attempt is invalid.

### Fail closed, everywhere

Every refusal leaves hosting state exactly as it was, so a non-nil error
from any transition means nothing moved:

| Situation | Result |
| --- | --- |
| no lock held | `ErrNoLock` |
| stale or unheld server lock | `ErrStaleServerLock` |
| superseded or missing manager lock | `ErrStaleManagerLock` |
| tablet already assigned here | `ErrAlreadyAssigned` |
| extent overlaps an assigned tablet | `ErrOverlapping` |
| malformed extent | `ErrInvalidExtent` |
| completion for a tablet this host does not track | `ErrNotAssigned` |
| completion for an assignment that has already ended | `ErrStaleAttempt` |
| completion for a tablet in another state | `ErrWrongState` |
| unload mode this host does not implement | `ErrInvalidUnloadMode` |

`AdoptLock` fails closed the same way, leaving the lock held — and so the
tablets tracked under it — untouched:

| Situation | Result |
| --- | --- |
| lock identity that could not name a `zlock` node | `ErrInvalidLock` |
| lock no newer than one already used | `ErrLockNotNewer` |
| tablets still tracked from an earlier generation | `ErrTabletsAssigned` |

`ErrNotAssigned`, `ErrStaleAttempt` and `ErrWrongState` are deliberately
distinct: the first says the host never had the tablet (or has already
released it), the second says it has the extent but as a later assignment
that this completion knows nothing about, and the third says it has the
right assignment but the completion arrived for the wrong phase. Only the
third is worth retrying.

`ErrNotAssigned` and `ErrStaleAttempt` both count as `RejectedStale`: only
`Assign` mints attempts, so a handle that finds nothing tracked named a
tablet this host really had, and its assignment ended before the
completion arrived.

Overlap is what catches stale split metadata. A parent extent arriving
after its children are assigned — or a child arriving after its parent —
would host the same rows twice, so it is refused even though the extent
itself is not a duplicate.

Overlap is checked against the two neighbouring tablets of the extent's
place in a per-table range index, not against every tablet the host has.
Tracked extents in a table never overlap each other, so those two are the
only ones that can reach it — which keeps loading N tablets at
N log N comparisons instead of N².

The manager-facing `Unassign` is the one deliberate exception, but only
for an extent that is absent *and* has no overlapping coverage still
tracked here. In that case the requested end state already holds:
unassigning a tablet this host does not have succeeds without changing
anything. A stale split/merge parent or child is not treated as
idempotent, because returning success while another extent still covers
those rows would let the manager place overlapping coverage elsewhere.
The fence is still checked first, so a superseded manager cannot
unassign anything.

A row bound is *absent* when it is nil or empty; the two are the same
bound. That matches `cclient.KeyExtent` and Accumulo's Java `KeyExtent`,
where a null and an empty `Text` collapse on the wire. If the two were
kept distinct, one tablet could arrive under two identities — assigned
with a nil bound and unassigned with an empty one — and `Unassign`
would report success without ever releasing it.

## 5. Lock loss and restart

`LoseLock` drops **every** tracked tablet at once and returns them so the
caller can close them. Once the lock is gone the manager is free to place
those tablets elsewhere, so the host stops claiming them immediately
rather than waiting to be told. Any work still in flight then fails
closed, because its server lock no longer matches.

The notification is fenced to its own generation: `LoseLock` takes the
`LockID` that was lost and does nothing unless that is still the lock
held. A delayed or duplicated watcher callback for a generation that has
already been replaced would otherwise tear down its successor and drop
tablets the manager believes are being served.

`AdoptLock` refuses a generation that is not strictly newer than any this
host has already used — including ones released by `LoseLock`, whose
sequence is remembered for exactly this reason — and refuses to run at
all while tablets are still assigned. A restarted or reconnected process
therefore always starts from an empty hosted set under a fresh
generation, which is what makes "process restart and ServiceLock loss do
not leave a tablet multiply hosted" hold.

A `LockID` is only usable as fencing authority if it could name a real
`zlock#<uuid>#<sequence>` node: the UUID must parse and the sequence must
fit the signed 32-bit counter Accumulo reads with `Integer.parseInt`.
The same checks live in `internal/zk`'s node parser.

## 6. Registering with ZooKeeper

A fenced state machine nobody can see is not a tablet server. Three
pieces turn it into one, and none of them decide anything: they publish
presence, and they read authority.

### The lock node

`ServiceLock` implements Accumulo's protocol as written, because the
manager and every Java client already implement the other half of it. It
creates an ephemeral sequential `zlock#<uuid>#<sequence>` node under
`<instance>/tservers/<group>/<address>` with the `ZooUtil.PUBLIC` ACL,
creating the resource-group path when it is the first server to use it,
the way `ServiceLockSupport.createNonHaServiceLockPath` does. The holder
is the lowest sequence that survives `validateAndSort`; a candidate that
is not the holder queues behind the holder's **lowest** node rather than
its immediate predecessor, so it wakes when that process leaves rather
than when it tidies up a duplicate of its own.

Duplicates are real: a create whose response was lost still created a
node, and a retry creates another. Both carry this process's UUID, so
the first is kept and the rest are deleted — otherwise one process would
hold several places in line and leave a node behind when it released.

A cancelled acquisition deletes whatever it created. An abandoned
ephemeral node keeps its place in the queue until the session ends, which
is a range nobody hosts for no reason.

### What the node says

The payload is `ServiceLockData` in the exact Gson wire form the manager
reads — a `descriptors` array of `{uuid, service, address, group}` — and a
tablet server publishes the same five services a Java one does: `CLIENT`,
`TABLET_INGEST`, `TABLET_MANAGEMENT`, `TABLET_SCAN`, `TSERV`.

The advertisement is refused rather than published when it would mislead
the manager: no descriptors, an address that is empty, unbound, or not
`host:port`, an unknown service, or two descriptors for one service —
which Accumulo's `EnumMap` would silently collapse to whichever arrived
last. A claim the manager cannot act on is worse than staying invisible,
because it draws work to a process that cannot do it.

That cuts both ways: a process should advertise what it can serve, not
what it intends to serve. `TabletServerServices()` names the full Java
set, but the caller chooses the subset, and until the Thrift endpoints
behind a service exist, advertising it routes work into a black hole.

### Holding it, and letting go

`Maintain` watches the node and returns when the generation ends.
Anything it cannot prove is treated as a loss: a watch that cannot be
established, a session that expired, a watch the client tore down. That
mirrors `LockWatcher.unableToMonitorLockNode`, where the Java tablet
server halts — a lock this process cannot monitor is one it cannot prove
it still holds, and hosting on an unprovable lock is exactly what
[§4](#4-the-fence) exists to stop.

An optional verify interval re-reads the directory on a timer. It catches
what a watch cannot: a watch dropped without an event, and a lock
directory deleted and recreated, which restarts ZooKeeper's sequence
counter and lets a later arrival take a lower number than the one held
here. The node is still there; it is simply no longer the holder.

`Participate` is the only place ZooKeeper reality meets the fence. It
acquires, hands the acquired generation to `AdoptLock`, watches it, and
on the way out calls `LoseLock` with **that** generation before deleting
the node. The order matters: deleting the node first would tell the
manager these tablets may be placed elsewhere while this process still
claimed them. If the host refuses the generation — one that is not newer
than a generation it already used — the lock is given back rather than
held, so the manager can place the work on a server that will take it.

### Reading the manager's lock

`Host` refuses every manager-directed transition until it has been told
which manager is live, and [§2](#2-the-manager-is-the-only-authority) is
why: authority is read from ZooKeeper, never inferred from the requests
that arrive. `WatchManagerLock` supplies it by reading the manager's own
ServiceLock directory and taking the lowest node — the same rule
`internal/zk` applies to resolve the manager's address, so the standby
managers queued behind it are not mistaken for authority.

Two failures that look alike are kept apart. A directory that cannot be
read leaves the previous observation in place, because failing to reach
ZooKeeper is not evidence the manager changed and withdrawing authority
would refuse the live manager's assignments for nothing. A directory that
is readable and holds no lock is evidence, and clears it. An observation
the host refuses — an epoch older than one it has already seen — is
dropped, so authority never moves backwards.

## 7. What is not here yet

This is the lifecycle core only. Still to land for #67:

- the manager-facing Thrift surface that turns RPCs into `Assign` /
  `Unassign` calls and reports `TabletServerStatus`
- loading tablet metadata, file and log references, table properties,
  constraints, and iterator configuration behind `StateLoading`
- advertising `TSERV` and the `TABLET_*` services for real, which is safe
  only once the Thrift endpoints behind them exist
- the process wiring that owns the ZooKeeper session, chooses the
  advertised address and resource group, and restarts participation after
  a lock loss
- publishing the `Metrics()` counters through the observability endpoints
- end-to-end tests against a live manager, including migration to and
  from a Java tserver and rolling mixed-fleet replacement

## 8. Metrics

`Host.Metrics()` snapshots the operational surface #67 asks for:
`Loading` / `Hosted` / `Unloading` gauges, and counters for
`Assignments`, `Loads`, `LoadFailures`, `Unloads`, `ForcedUnloads`,
`RejectedStale`, `RejectedDuplicate`, `LockLosses`, and
`DroppedOnLockLoss`. The two rejection counters are the ones to alert on:
a healthy cluster refuses almost nothing, so a rising `RejectedStale`
means transitions are naming a superseded caller — assignments racing
lock churn, or completions arriving after their assignment ended — and a
rising `RejectedDuplicate` means the manager is working from stale tablet
metadata.
