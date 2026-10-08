# ATPL policy files

`pkg/atpl` compiles reviewable, versioned policy files to fleet registry
registrations, and `shoalctl policy` reconciles them with a live registry. This
is the first slice of #452. Design context: `docs/gateways.md`, "What comes from
ATPL".

The format is derived from the Agent Trust Policy Language
(`github.com/SentriusLLC/atpl`, Apache-2.0). It keeps the name and the idea,
policy reviewed as a diff and reproduced from source, but not ATPL's schema.
Every file carries that credit in its required `origin` field.

## Format

A policy is a directory of `*.atpl.json` files, read non-recursively. Each file
is one JSON object:

```json
{
  "atpl": "shoal.atpl/v1",
  "origin": "Derived from the Agent Trust Policy Language, github.com/SentriusLLC/atpl (Apache-2.0)",
  "executors": [
    {"ref": "remote-exec", "max_effects": ["external", "reads-corpus"], "min_effects": []}
  ],
  "agents": [
    {
      "id": "planner",
      "authorization_domain": "domain",
      "scopes": [{"source_id": "source-a", "policy_id": "policy"}],
      "executor_ref": "remote-exec",
      "lease_ttl": "12h",
      "capabilities": [
        {"name": "search", "actions": [
          {"name": "query", "effects": ["reads-corpus"],
           "input_schema": {"type": "object"}, "output_schema": {"type": "object"}}
        ]}
      ]
    }
  ]
}
```

- **Agents** compile one-to-one to `fleet.Spec` (`pkg/explorer/fleet/model.go`).
  IDs, the authorization domain and scope identities are UTF-8 strings in the
  file and the registry's opaque bytes unchanged on the wire.
- **Effects** use the registry's wire spellings: `reads-corpus`,
  `egresses-content`, `external`. Omitted means the empty set.
- **`inherit: true`** on an action copies the same-named action of the parent's
  same-named capability, schemas and effects exactly. It is refused if the
  parent has no such action, or if the action also declares its own fields.
- **`lease_ttl`** is a Go duration in (0, 23h55m]. Compile adds it to one shared
  compile time, so a child and its parent compare TTLs exactly as the registry
  compares absolute leases. Leases are absolute and at most 24h ahead
  (`Spec.canonical`), which is why the file holds a TTL. The 5 minutes below
  the registry's bound (`SkewMargin`) absorb a client clock running ahead of
  the server's: apply computes the lease on the client, the registry checks it
  on the server.
- **Executors** assert what the host binds a reference to. Ceilings and floors
  are host Go objects with no read API (`executorCeiling`, `executorFloor`,
  `cmd/shoal-explore-web/fleet.go`), so the file declares them and compile
  checks every action against them. The server stays authoritative and refuses
  on its own binding. `min_effects` must be within `max_effects`.

Strictness, in `Decode`: UTF-8 only; `\u` escapes of unpaired UTF-16
surrogates refused (they decode to U+FFFD, so two spellings would read as one);
one top-level object; duplicate keys refused at any depth, schemas included;
trailing data refused; the version gate before any other field; every unknown
field refused. Fields ATPL defines that this version does not compile are
refused by name with the reason:

| Field | Refusal |
| --- | --- |
| `approval` | requires a later ATPL version (#451) |
| `obligations` | not declared in policy: admission obligations are computed per request; deferred |
| `attestation`, `runtime` | requires a later ATPL version (#446) |
| `trust_score` | not part of ATPL in Shoal; trust is a typed decision |
| `behavior` | not part of ATPL in Shoal; fixed thresholds are not gates |

Bounds: at most 4096 agents and 1024 executors per policy; at most 4097 files
(one per agent plus executors, the layout export writes, so a later export
diffs agent by agent); at most 4 MiB per file and 256 MiB per policy directory.
4 MiB is what any one agent the registry accepts can need: the registry bounds
an agent at `fleet.MaxDescriptorBytes` of its own JSON, where byte fields are
base64, and here those fields are strings that escape to at most 6 bytes per
byte. Schemas are written compact (`Encode`), so indentation does not grow
them.

## Validation

Compile refuses anything Register would refuse, before apply, with the file and
path: `agents.atpl.json: agents[id=searcher].lease_ttl: 13h0m0s outlasts parent
agents[id=planner]'s lease_ttl 12h0m0s`. It calls the registry's own validators
through thin wrappers (`pkg/explorer/fleet/policy_export.go`) one action at a
time, so a refusal names the action, then once over the whole spec. Delegation
conditions (`service.go`, the delegated-agent check) are re-composed per item:
same domain, scopes a subset, every action present in the parent with identical
schemas and no wider effects, lease no later. A parity test runs every refusal
fixture through `fleet.Service.Register` as well.

A parent must be declared in the policy or, under `plan` and `apply`, live in
the registry. Offline `compile` refuses an undeclared parent. A live parent is
checked the way Register's `activeChain` checks it: compile walks the live
registrations to the root, refusing an ancestor that is not visible, revoked or
expired, a link that no longer narrows its parent, and a chain that would reach
`fleet.MaxDelegationDepth` (64 registrations) counting live ancestors and the
policy's own.

Compile accepts documents built in code as well as decoded ones, so it refuses
every string that is not UTF-8 itself: encoding/json would otherwise replace
the invalid bytes and two different policies would share a digest.

## Identity

`Policy.Digest` is `atpl:policy:v1:<sha256>` over the Go `encoding/json` form of
the version, executors and every registration without its absolute lease but
with its TTL, as `pkg/decision` identities are computed. The same files give the
same digest at any time and in any file, list or key order. `policy compile`
prints those exact bytes.

## Commands

```
shoalctl policy compile DIR
shoalctl policy plan DIR -endpoint URL -token-file F
shoalctl policy apply DIR -endpoint URL -token-file F -plan-digest D
shoalctl policy export -out DIR -endpoint URL -token-file F [-executors manifest.atpl.json]
```

`compile` is offline: it prints the policy digest and canonical JSON, or exits
non-zero with the refusal.

`plan` reads the registry through the list route (`POST
/api/v1/fleet/agents/resolve`, `pkg/explorer/webapi/fleet_registry.go`), 16 per
page, compiles against it and prints the registry, then one line per agent:
`+` create, `~` narrow, `*` executor change, `=` unchanged, `!` refused, `?`
unmanaged, with field-level `+`/`-`/`~` lines beneath. An executor change is an
update the registry accepts like a narrowing, but it changes what runs the
agent's actions, so it is marked apart. It exits non-zero if anything is
refused. Refusals are: widening a live agent (the registry only narrows,
`service.go`), moving it to another parent, and a write that would leave a
delegation link exceeding its parent at any point during or after apply,
including a live child the plan does not rewrite that would stop resolving.
Leases are not compared; heartbeats maintain them, and apply does not renew
them.

The plan digest (`atpl:plan:v2:`) binds the policy digest, the normalized
endpoint (lower-case scheme and host, default port and trailing slash dropped),
each managed agent's kind and `ContentDigest` of its live registration, and the
IDs of unmanaged agents, and the write order below, two-step writes included.
`ContentDigest` covers parent, domain, scopes, executor
and capabilities, and leaves out generation, subject, actor, lease and update
time, which heartbeats move. A heartbeat between plan and apply therefore does
not invalidate a reviewed plan, unless it changes the write order; a plan
reviewed against one registry does not apply to another. The write order
depends on leases, so on time and heartbeats: a plan reviewed long before apply
can order differently by then, and apply refuses it rather than write an order
nobody reviewed.

`apply` recomputes the plan and refuses unless its digest equals
`-plan-digest` and nothing is refused. It then registers with reason code
`atpl-apply` and the policy digest as reason detail, in an order where every
intermediate state is one the registry accepts and resolves
(`pkg/atpl/order.go`). Creates come last, parents first. Updates are ordered
for each child updated together with its parent:

1. the child's new registration, lease included, fits the parent's live one:
   the child first;
2. else the child's live registration fits the parent's new one: the parent
   first;
3. else only the lease prevents (1), because a narrowed child always fits its
   parent's content: the child is written twice, first with its lease clamped
   to the parent's live lease, then, after the parent, with its full lease.
   Plan output lists the apply order and marks both steps;
4. otherwise the child is refused.

The resulting sequence is then replayed against the live state, checking every
link each write touches, both to its parent and to its children; a plan
whose replay leaves any link exceeding its parent, even between two writes, is
refused. Chains of any depth are ordered the same way. Each write expects the
generation apply read, or for a second step the one the first step wrote. If a heartbeat moves it before the
write lands, the registry's compare-and-swap refuses; apply re-reads the agent
through the resolve route and retries once if its `ContentDigest` is unchanged,
and stops otherwise. The registration key hashes the policy digest, agent,
expected generation and lease. Apply stops at the first refusal and is not
atomic across agents.

`export` writes `executors.atpl.json` and one file per live agent, named
`agent.<id>.atpl.json` for plain lower-case IDs and by hash otherwise. Actions
are written in full, schemas compact. Each TTL is the lease remaining at one
export time, rounded to the second and clamped to the policy's maximum and to
the parent's exported TTL: export reflects what the live fleet holds at that
moment, not the TTL each agent was first registered with. The clamp absorbs
rounding and also a real inversion, where a parent's own heartbeat left it with
less time than its child: the child then exports with the parent's TTL, shorter
than what it holds live, and the export still compiles. A lease with under a
second remaining, measured before rounding, is refused. Without `-executors`, each executor's `max_effects` is the union of
its actions' effects and `min_effects` is empty, and the command warns that this
is not what the host binds. A live ID, domain, scope or executor reference that
is not UTF-8 is refused, naming the field. Export refuses a directory that
already holds policy files, read with `os.ReadDir` so a directory name with glob
metacharacters cannot defeat the check, and refuses before writing anything if
the files together would exceed the 256 MiB a policy directory may hold.

Endpoints must be `https`, or `http` on loopback, with no credentials, query or
fragment; a bare trailing `?` or `#` is refused too. Requests go to the
normalized endpoint, built from scheme, host, port and path. Redirects are not
followed.

## Limits

- The registry lists only active registrations. A revoked or expired agent is
  invisible to plan, which shows its ID as a create, and the registry then
  refuses it: a revoked or expired ID cannot be registered again.
- `internal/explorerfleet`'s durable lifecycle recorder does not persist reason
  code or detail, so in a hosted deployment the policy digest reaches
  `fleet.Lifecycle` but not the durable interaction record.

## Deferred

- Approval rules (#451) and attestation requirements (#446): refused by name
  until they compile.
- Admission obligations. They are computed per request at admission, not
  declared per agent; how a policy would constrain them is undecided.
- YAML. The repository has no YAML library, and every strict decoder here is
  JSON.
- Atomic multi-agent apply, revoking unmanaged agents, renewing leases.
- A server read API for executor ceilings, so export need not derive them.
- Whether the ATPL repository is archived with a pointer here
  (`docs/gateways.md`, open decisions).
