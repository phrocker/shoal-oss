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
- **`lease_ttl`** is a Go duration in (0, 24h]. Compile adds it to one shared
  compile time, so a child and its parent compare TTLs exactly as the registry
  compares absolute leases. Leases are absolute and at most 24h ahead
  (`Spec.canonical`), which is why the file holds a TTL.
- **Executors** assert what the host binds a reference to. Ceilings and floors
  are host Go objects with no read API (`executorCeiling`, `executorFloor`,
  `cmd/shoal-explore-web/fleet.go`), so the file declares them and compile
  checks every action against them. The server stays authoritative and refuses
  on its own binding. `min_effects` must be within `max_effects`.

Strictness, in `Decode`: at most 256 files of at most `fleet.MaxDescriptorBytes`
each; UTF-8 only; one top-level object; duplicate keys refused at any depth,
schemas included; trailing data refused; the version gate before any other
field; every unknown field refused. Fields ATPL defines that this version does
not compile are refused by name with the reason:

| Field | Refusal |
| --- | --- |
| `approval` | requires a later ATPL version (#451) |
| `obligations` | requires a later ATPL version (#452) |
| `attestation`, `runtime` | requires a later ATPL version (#446) |
| `trust_score` | not part of ATPL in Shoal; trust is a typed decision |
| `behavior` | not part of ATPL in Shoal; fixed thresholds are not gates |

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
the registry. Offline `compile` refuses an undeclared parent.

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
page, compiles against it and prints one line per agent: `+` create, `~` narrow,
`=` unchanged, `!` refused, `?` unmanaged, with field-level `+`/`-`/`~` lines
beneath, then a plan digest over the policy digest and each agent's kind and
live generation. It exits non-zero if anything is refused. Refusals are:
widening a live agent (the registry only narrows, `service.go`), moving it to
another parent, and a write that would leave a delegation link exceeding its
parent, including a live child the plan does not rewrite that would stop
resolving. Leases are not compared; heartbeats maintain them, and apply does
not renew them.

`apply` recomputes the plan and refuses unless its digest equals
`-plan-digest` and nothing is refused. It then registers each create or narrow,
parents first, with `expected_generation` set to the live generation the plan
saw, reason code `atpl-apply` and the policy digest as reason detail. The
registration key hashes the policy digest, agent, expected generation and lease.
It stops at the first refusal; a write the registry moved since the read fails
its compare-and-swap rather than overwriting. Apply is not atomic across
agents.

`export` writes `executors.atpl.json` and one file per live agent, named
`agent.<id>.atpl.json` for plain lower-case IDs and by hash otherwise. Actions
are written in full. Each TTL is the live lease minus the last write, rounded to
the second, so it carries no client-server clock skew. Without `-executors`,
each executor's `max_effects` is the union of its actions' effects and
`min_effects` is empty, and the command warns that this is not what the host
binds. A live ID, domain or scope that is not UTF-8 is refused, naming the field.
Export, then compile, yields the registrations that are live.

Endpoints must be `https`, or `http` on loopback. Redirects are not followed.

## Limits

- The registry lists only active registrations. A revoked or expired agent is
  invisible to plan, which shows its ID as a create, and the registry then
  refuses it: a revoked or expired ID cannot be registered again.
- The lease is computed from the client's clock. A client ahead of the server
  can exceed the 24h bound with a 24h TTL.
- `internal/explorerfleet`'s durable lifecycle recorder does not persist reason
  code or detail, so in a hosted deployment the policy digest reaches
  `fleet.Lifecycle` but not the durable interaction record.

## Deferred

- Approval rules (#451), attestation requirements (#446) and admission
  obligations: refused by name until they compile.
- YAML. The repository has no YAML library, and every strict decoder here is
  JSON.
- Atomic multi-agent apply, revoking unmanaged agents, renewing leases.
- A server read API for executor ceilings, so export need not derive them.
- Whether the ATPL repository is archived with a pointer here
  (`docs/gateways.md`, open decisions).
