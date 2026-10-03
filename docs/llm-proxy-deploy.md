# Deploying the Shoal LLM proxy

`shoal-llm-proxy` is the enforcement plane: an OpenAI-compatible endpoint a
caller points at instead of the real provider. For each request it asks the
Explorer for admission, applies whatever obligations come back, forwards or
refuses, and reports the outcome. The value is that it needs no cooperation from
the caller — an agent framework, an IDE plugin, a shell script with `curl`:
anything that speaks the API is governed by changing one base URL.

- [Why this is a separate document](#why-this-is-a-separate-document)
- [Why it is a separate process](#why-it-is-a-separate-process)
- [The flag contract](#the-flag-contract)
- [Kubernetes: the Helm chart](#kubernetes-the-helm-chart)
- [The render-time refusals](#the-render-time-refusals)
- [Orchestrator probes: the health surface](#orchestrator-probes-the-health-surface)
- [Stateless, and not a singleton](#stateless-and-not-a-singleton)
- [Secrets: two credentials, both required](#secrets-two-credentials-both-required)
- [Upgrading](#upgrading)
- [What the chart cannot express yet](#what-the-chart-cannot-express-yet)

## Why this is a separate document

`docs/shoal-explore-web-deploy.md` is organised around one binary with one state
root served by one replica, and nearly every deployment decision in it follows
from that. The proxy has the opposite properties — no state root, several
replicas, a rolling update that is allowed to surge — and a different threat
model. Folding it into that document would make its title untrue and would bury
the one contrast that matters most about the two planes.

The two documents cross-reference instead. Read that one for the Explorer the
proxy asks; read this one for the proxy.

## Why it is a separate process

It must not be inside `shoal-explore-web`. The Explorer holds the policy store
and the corpus; the proxy handles untrusted prompt content from arbitrary
callers and speaks to third-party endpoints. Putting them in one process puts
prompt injection in the same address space as the decision plane.

That is also why the admission seam is an HTTP API rather than a Go interface —
see `docs/admission-seam.md`. The proxy is a different binary and, under the
execution boundary, an out-of-process actor whose effects Shoal declares but
does not contain.

One consequence shapes everything below. The proxy is the first place in Shoal
where a denial means the work does not happen at all; everywhere else,
enforcement withholds from a response that is still produced. So the proxy must
fail closed, and a proxy that fails closed on everything is not a crash — it is
a pod that passes every probe and refuses every request. From outside, that is
indistinguishable from a total outage of whatever is configured to go through
it. Almost every guard in the chart exists to move one way of producing that
from a production incident to `helm template`.

## The flag contract

| Flag | Purpose |
| --- | --- |
| `-listen` | OpenAI-compatible listen address, for example `0.0.0.0:8100`. |
| `-health-address` | Separate probe listener serving `GET /healthz` and `GET /readyz`. |
| `-allowed-host` | Comma-separated exact-match external authorities — the same gate as the Explorer's. |
| `-admission-url` | Base URL of the Explorer's authenticated API. |
| `-admission-token-env` | Environment variable holding the bearer token the proxy presents to the Explorer. |
| `-upstream-base-url` | The real OpenAI-compatible provider. |
| `-upstream-api-key-env` | Environment variable holding the upstream credential. |
| `-agent-id` | Registered descriptor this proxy admits against. |
| `-agent-generation` | `int64`, positive. |
| `-capability` | Registered capability name. |
| `-action` | Registered action name. |
| `-source-id` | Scope source, canonical unpadded base64url. |
| `-policy-id` | Scope policy, canonical unpadded base64url. |
| `-lease` | Admission lease duration. Capped at `MaxActionClaimTTL`, five minutes. |
| `-request-timeout` | Upstream request timeout. |

Every one of these is a container argument on the pod spec rather than a
ConfigMap entry, for the reason the Explorer's are: a changed argument changes
the pod template and rolls the pod on its own. A ConfigMap would need a checksum
annotation to get the same effect, and without one an edited `-admission-url`
would leave the running proxy asking the old decision plane while the chart
claimed the new one.

## Kubernetes: the Helm chart

`deploy/helm/shoal` renders the proxy as a **Deployment** with a Service and a
PodDisruptionBudget. The `llmProxy` values block is off by default and
independent of the chart's `mode`, which selects a storage topology the proxy
does not depend on.

```console
$ cp deploy/helm/shoal/values-llm-proxy.yaml my-llm-proxy-values.yaml
$ helm upgrade --install shoal deploy/helm/shoal -f my-llm-proxy-values.yaml \
    --set llmProxy.image.repository=ghcr.io/YOUR_ORG/shoal-llm-proxy \
    --set llmProxy.image.tag=TAG
```

The profile **does not install as shipped**, on the same principle as
`values-explorer.yaml`. Every value it leaves empty is one the chart refuses to
render without. `helm template` names the first missing one, so working through
the errors in order fills the profile.

Both planes compose from one values file. `llmProxy.admission.url` may equally
name an Explorer installed separately, in another namespace, or in another
cluster — the proxy needs one to ask, not one in the same release:

```console
$ helm upgrade --install shoal deploy/helm/shoal \
    -f my-explorer-values.yaml -f my-llm-proxy-values.yaml
```

Before changing the chart, run its checks:

```console
$ deploy/helm/validate-chart.sh
```

They render every profile, schema-check the output, assert that each refusal
below still refuses and each valid configuration still renders, and assert that
the proxy's two listeners are wired end to end. A guard that silently stops
firing is the failure they exist to catch, and the checks themselves are
mutation-tested: each guard is removed in turn and the case that must then fail
is confirmed to fail.

## The render-time refusals

| Setting | Why the chart will not render without it |
| --- | --- |
| `llmProxy.admission.url` | A proxy that cannot ask must deny. With no decision plane every request behind it is refused while the pod stays healthy. It must also be an absolute `http://` or `https://` URL: a bare host is a transport error on every admission. |
| a plaintext `admission.url` to a non-loopback host | Over `http://` the bearer token crosses the network in the clear, and so does the verdict — anything on the path can rewrite a deny into an allow, which removes the enforcement plane while everything still looks healthy. `admission.allowPlaintext: true` accepts it where a mesh already authenticates the hop. |
| `llmProxy.admission.tokenEnv`, `credentialSecretName`, `credentialSecretKey` | The Explorer's API is authenticated in every deployment this chart can render, so an admission request with no bearer token is a 401 — every time. The proxy then denies every call. |
| `llmProxy.admission.lease` above `5m` | `pkg/explorer/fleet` caps an admission lease at `MaxActionClaimTTL` and **refuses** a request outside the bound rather than shortening it. A larger value is not a longer lease; it is every call denied as an invalid argument, which the caller cannot act on and an operator cannot tell apart from a policy denial. |
| `llmProxy.upstream.baseURL` | An admitted request has nowhere to go, so the caller sees a failure on exactly the calls policy allowed — and the admission is spent on work that never happened. Must also be absolute. |
| a remote upstream with no `credentialSecretName` | The credential is read at request time and nothing projects it into the pod, so every admitted call is rejected by the provider after admission has been spent on it. A loopback provider needs none. |
| `llmProxy.upstream.requestTimeout` above `admission.lease` | A call that outlives its lease is performed under a token that can no longer be reported against. The fleet record calls that *abandoned* rather than resolved: whether the effect happened is unknown and stays unknown, which breaks the loop the report exists to close. |
| `llmProxy.allowedHosts` | An empty allow-list answers every request `421 Misdirected Request` before admission is even asked. See the Explorer document's host-authority section: the gate is the same, and the resolved listen address of a pod is an authority no real client sends. |
| `llmProxy.identity.*` | Admission resolves the agent, capability, action and scope together. An incomplete identity is not a startup failure — it is a proxy that comes up healthy and is refused on every request, with a denial that carries no reason by design. There is nothing in the pod log to read. `agentGeneration` must be a positive `int64`; `sourceID` and `policyID` must be canonical unpadded base64url. |
| `llmProxy.healthPort` equal to `containerPort` | Two listeners cannot share a port, and which one loses is decided by bind order. If the health listener wins the proxy serves nothing; if the traffic listener wins every probe answers `421` and the pod never becomes ready. |
| either port below `1024` | The container runs as uid 65532 with all capabilities dropped. The bind fails and the pod restarts forever. |
| `llmProxy.podDisruptionBudget.maxUnavailable: 0` | The Explorer uses `0` because it is a singleton over one state root. The proxy is stateless and replaceable, so a `0` budget protects nothing and instead makes `kubectl drain` and cluster-autoscaler consolidation block forever on a pod that could safely have moved. |
| any value still containing `REPLACE_ME` | A placeholder is not configuration. The dangerous case is not the one that fails: an identity of literally `REPLACE_ME` renders a proxy refused on every call while the chart reports success. |

A value written and left blank counts as missing, and so does one that is only
whitespace. A key with nothing after it in YAML is `nil`, not `""`, and `nil`
stringifies to `"<nil>"` — non-blank. A required-value check that did not
normalise first would read an absent value as configured and render the pod with
an empty flag, which is exactly the healthy-and-denying proxy these guards
exist for. Every value is therefore normalised before it is tested, and the
`-allowed-host` argument is built from the same template the guard counts, so
the chart cannot pass its own check and then render `" llm.example.test"` — an
authority matched exactly, with a space in it, matching nothing.

## Orchestrator probes: the health surface

The host-authority gate runs before routing and before anything else. That makes
the traffic port unusable as a probe target: a kubelet addresses a pod by its
runtime-assigned IP, which no static `-allowed-host` list can name, so every
probe there answers `421` and the pod never becomes ready.

`-health-address` opens a second listener for exactly this, and behaves as the
Explorer's does:

| Route | Meaning |
| --- | --- |
| `GET /healthz` | The process is up. Stays `200` throughout a drain. |
| `GET /readyz` | The proxy is serving. `503` before it serves and from the moment shutdown begins. |

The split is what makes a rollout safe. Readiness drops **before** the listener
stops accepting, so the endpoints controller removes the pod from the Service
while it is still finishing in-flight requests. Liveness does not drop, because
a pod shedding traffic on purpose has not failed and restarting it would throw
away the graceful close.

The readiness probe is deliberately short — `periodSeconds: 3`,
`failureThreshold: 1`. A failing readiness probe is the only thing that takes
the pod out of the Service, so that interval *is* the window between the drain
starting and traffic stopping; a slower probe spends it answering requests the
proxy has already decided to stop serving.

There is no startup probe, unlike the Explorer. The Explorer needs one because
opening a large corpus and policy catalog takes time. The proxy opens nothing,
so a startup probe would only postpone the first readiness check.

The Service publishes the traffic port only. The probe surface exists for the
kubelet, which reaches the pod directly; publishing it would make an
unauthenticated endpoint routable in-cluster for no reason, and it is the one
endpoint on the pod that answers without passing through admission.

`validate-chart.sh` asserts all of this end to end rather than in parts — the
probes address the health port, `-health-address` is passed and names the port
the probes and the container agree on, `-listen` names the port the Service
targets, the Service does not publish the health port, and its selector
actually matches the pod's labels. Asserting only that the probes *name* a
health port once passed a chart whose probes addressed a port nothing was
listening on.

## Stateless, and not a singleton

A **Deployment**, where the Explorer is a StatefulSet, and that single
difference is the whole difference between the two planes.

The Explorer needs its rolling update to stop the old pod before starting the
new one, because two processes over one `ReadWriteOnce` state root is a
split-brain it has no protocol for — and `explorer.replicas` above 1 is refused
for the same reason. The proxy has no state root at all: the decision lives in
the Explorer, the admission token travels in the request it was granted for, and
nothing is retained between calls. So replicas are independent askers of one
decision plane, surging is correct, and there is no volume, no ordinal identity
and no governing headless Service to need a StatefulSet for.

The default is two replicas, and raising it is the expected direction. The
rollout is `maxUnavailable: 0`, `maxSurge: 1`: no old pod is removed until a
surge pod has passed readiness, because a gap in capacity here is an outage for
everything behind the proxy rather than a slow read. Replicas carry a soft
anti-affinity across nodes so a single node loss is not the whole plane —
*soft*, because a cluster with fewer nodes than replicas must still schedule,
and a Pending pod is as unavailable as a denied one.

Supplying `llmProxy.affinity` replaces that default outright rather than merging
with it. Merging two affinity trees would produce a scheduling constraint
neither the chart nor the operator wrote.

The pod has no writable path and no `emptyDir` to give one back. The proxy holds
no state root and must not spool payloads: prompt and completion content is
exactly what it is governing the egress of, and a scratch directory is where
that content would end up on a node's disk.

## Secrets: two credentials, both required

The chart projects both from pre-existing Secrets. Neither is created for you.

```console
$ kubectl create secret generic shoal-admission-token --from-literal=token="$EXPLORER_BEARER"
$ kubectl create secret generic shoal-upstream-key --from-literal=api-key="$PROVIDER_KEY"
```

```yaml
llmProxy:
  admission:
    credentialSecretName: shoal-admission-token
    credentialSecretKey: token
  upstream:
    credentialSecretName: shoal-upstream-key
    credentialSecretKey: api-key
```

The admission credential is unconditional, not conditional on a Secret being
named — the chart requires one. The upstream credential is required for any
non-loopback provider, exactly as the Explorer's chat provider is.

## Upgrading

Two things to know before an upgrade.

**`identity.agentGeneration` pins a descriptor generation.** Re-registering the
descriptor bumps it, and this value has to be bumped with it in the same change.
A stale generation is not a startup failure: the proxy comes up, passes its
probes, and is refused on every admission request.

**Changing any setting rolls the pods, by design.** Every setting is an
argument, so there is no case where the chart reports a new posture while a
running process is on the old one. `maxUnavailable: 0` means the rollout keeps
every replica serving while it happens.

## What the chart cannot express yet

Gaps in the flag contract, recorded rather than worked around.

- **No metrics or observability listener.** There is no `-metrics-address`, so
  there is nothing to scrape and nothing for the chart to annotate with
  `prometheus.io/port` — which the storage tier and read fleet both have. Issue
  #390 requires that an operator be able to tell an infrastructural denial (the
  decision plane unreachable) from a policy denial. Without a metrics surface
  that distinction exists only in logs the proxy is also forbidden from filling
  with payload content.
- **The admission token can only come from an environment variable.**
  `-admission-token-env` has no file-path counterpart, so a projected
  ServiceAccount token — which rotates, and is the Kubernetes-native way to
  authenticate pod to service — cannot be used. The credential has to be a
  static Secret, and rotating it requires a pod roll.
- **No TLS flags for the proxy's own listener.** `writeTier`, `readFleet`,
  `tserver` and `compactor` all take `tls.enabled`/`secretName`. The proxy
  cannot, so it is plaintext behind an ingress or a mesh. The Explorer has the
  same gap, which is also why reaching it over `http://` needs the explicit
  acknowledgement above.
- **No drain or shutdown-timeout flag.** `readFleet` has `-quiesce-delay` and
  `-drain-timeout`. The chart sets `terminationGracePeriodSeconds` for the
  proxy, but the proxy has no flag bounding its own drain, so the two cannot be
  made to agree — the grace period is a guess at what the binary will do.
- **No report timeout.** `-request-timeout` bounds the upstream call. The report
  that follows it has no bound of its own, and an admission that is never
  reported is the outstanding case `docs/admission-seam.md` describes.
- **`-agent-id` is not documented as base64url**, though `AdmissionRequest.AgentID`
  is a `shoal.ID` like the scope identities, which are. The chart validates the
  encoding of `-source-id` and `-policy-id` because the contract says they are
  base64url, and does not validate `-agent-id` because it does not. If the wire
  encoding is in fact the same, the contract should say so and the chart should
  check it the same way.
