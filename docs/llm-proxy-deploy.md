# Deploying the Shoal LLM proxy

`shoal-llm-proxy` is the enforcement plane: an OpenAI-compatible endpoint a
caller points at instead of the real provider. For each request it asks the
Explorer for admission, applies whatever obligations it can satisfy, forwards or
refuses, and reports the outcome. The value is that it needs no cooperation from
the caller — an agent framework, an IDE plugin, a shell script with `curl`:
anything that speaks the API is governed by changing one base URL.

**A withhold obligation is refused on this surface, not applied.** It used to be
described — and implemented — as stripping the withheld references and
forwarding the rest. That enforced nothing: `shoal_references` is a flat list of
IDs, the material lives in `messages[].content` as free text, and nothing
connects the two, so the proxy could not identify the bytes it had been told to
withhold. Removing the label did not change what the model received either.
Withholding is therefore as strong as a denial here, which is tracked as
[#426](https://github.com/phrocker/shoal-oss/issues/426) rather than papered
over. Read anything below about obligations with that in mind.

- [Why this is a separate document](#why-this-is-a-separate-document)
- [Why it is a separate process](#why-it-is-a-separate-process)
- [The flag contract](#the-flag-contract)
- [Kubernetes: the Helm chart](#kubernetes-the-helm-chart)
- [The render-time refusals](#the-render-time-refusals)
- [Orchestrator probes: the health surface](#orchestrator-probes-the-health-surface)
- [Stateless, and not a singleton](#stateless-and-not-a-singleton)
- [Secrets: two credentials, both required](#secrets-two-credentials-both-required)
- [A rotating credential: the projected ServiceAccount token](#a-rotating-credential-the-projected-serviceaccount-token)
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
| `-allow-plaintext-admission` | Accept a remote `http://` `-admission-url`. Off by default, and the admission hop only. |
| `-admission-token-env` | Environment variable holding the bearer token the proxy presents to the Explorer. Mutually exclusive with the file form. |
| `-admission-token-file` | File holding that token instead, read per request. The only form a rotating credential has. |
| `-upstream-base-url` | The real OpenAI-compatible provider, including the version segment it documents. |
| `-upstream-api-key-env` | Environment variable holding the upstream credential. Mutually exclusive with the file form. |
| `-model` | Model names this deployment expects, comma-separated. Optional; see below. |
| `-upstream-api-key-file` | File holding that credential instead, read per request. |
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

### Credentials, and what "read per request" does and does not buy

Both credentials are read at the moment they are used rather than captured at
startup. That keeps the value out of the proxy's own state, where a crash dump
would carry it.

It does **not** make an environment variable rotatable, and an earlier version
of this guide implied that it did. A process environment is fixed once the
container starts: updating the Secret behind a `secretKeyRef` leaves every
running proxy on the old value until the pod is replaced, so re-reading
`os.Getenv` per request re-reads the same string forever. For the variable forms,
per-request reading buys the crash-dump property and nothing else.

Rotation needs a source that can change underneath a running process, which is
what the `-file` forms are for:

| Source | Changes under a running pod? |
| --- | --- |
| `secretKeyRef` into an environment variable | No. Requires replacing the pod. |
| A Secret mounted as a volume | Yes. The kubelet updates the file in place. |
| A projected ServiceAccount token | Yes, and it is rewritten on rotation by the kubelet. |
| A CSI driver's volume (SPIFFE, Vault Agent, secrets-store-csi) | Per that driver. |

Each credential takes exactly one form. The binary refuses an explicitly chosen
`-admission-token-env` together with `-admission-token-file` — and the same pair
for the upstream key — rather than ranking them, because picking one silently
makes the effective credential invisible: an operator who adds a file while a
stale variable is still in the manifest cannot tell from the configuration which
one is being presented, and the symptom of the wrong answer is an authentication
failure that names neither. The variable flags ship with non-empty defaults, so
"both set" means the default was left in place rather than that two were chosen
deliberately — which is why the default is what gets compared, and why the file
form works without also blanking the variable.

### The plaintext acknowledgement covers one hop

`-allow-plaintext-admission` accepts a remote `http://` decision plane. It
exists for a mesh that already supplies the transport authentication the scheme
would, and it makes accepting that an explicit act: over plaintext the bearer
token crosses the network in the clear and so does the verdict, and anything on
the path can rewrite a deny into an allow — which removes the enforcement plane
while leaving every sign that it is running.

## Declared models, and why the list exists

`-model` (`llmProxy.models`) names the models this deployment expects. It is
optional, and an empty list is a working configuration.

The reason it exists is not routing — `-model` restricts nothing, and an
unlisted model is still forwarded. It is the declaration. The proxy sends Shoal
a description of what a call would do and never the payload, and `model` is a
caller-controlled free-text field: a prompt or a secret fits in it exactly as
well as `gpt-4o` does. So the declaration reports a **named** model as itself
and anything else as `other`, and the names come from the operator rather than
from the request.

The trade is explicit. With no list, no caller text can reach the plane through
this field, and the plane also cannot write policy about which model was used —
every call reports `other`. Naming models buys that granularity back for the
ones you name. Either way the caller's own `model` value goes upstream
unchanged, because rewriting it would make the proxy the reason an unmodified
client gets a different answer.

A plane that wants to refuse unfamiliar models can deny on `other`. Deciding
that here would make the proxy a model gate, which #390 lists as a non-goal.

## No plaintext acknowledgement for the provider hop

There is deliberately no counterpart for `-upstream-base-url`. That request
carries the prompt itself and the operator's provider credential, and a mesh
authenticating the hop to the Explorer says nothing about the hop to a third
party. A remote `http://` upstream is refused by the chart at render and by the
binary at startup, with no way to accept it.

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
| `llmProxy.admission.tokenEnv`, `credentialSecretName`, `credentialSecretKey` | The Explorer's API is authenticated in every deployment this chart can render, so an admission request with no bearer token is a 401 — every time. The proxy then denies every call. Required **in the environment-variable form**; the file form requires other things instead, below. |
| a non-boolean `admission.allowPlaintext` | It is rendered verbatim into `-allow-plaintext-admission`, and Go's boolean flag parser refuses anything it cannot read as one. YAML words like `yes` and `on` are strings here, not booleans, and the pod exits at startup on a flag error. |
| a remote `http://` `upstream.baseURL` | No acknowledgement accepts it, unlike the admission hop: this request carries the prompt and the provider credential. The binary refuses it at startup too, so rendering it is a pod that never serves. |
| `llmProxy.terminationGracePeriodSeconds` not exceeding `admission.lease` by 5s | The proxy drains for the lease on SIGTERM, because the lease is the bound on an admitted call's whole lifetime including the report that closes it. This value is the kubelet's budget for the same window, so a shorter one means SIGKILL arrives mid-drain and kills the calls whose egress has already happened and whose grant has already been spent — an unreported grant produced by every rolling update, on a schedule, while the pod terminates cleanly as far as the kubelet is concerned. The margin is required because the grace period is whole seconds while a lease need not be, and because the process still has to exit after the drain returns. |
| `llmProxy.identity.agentID` that is not canonical unpadded base64url | The workspace decodes this field and the binary refuses an undecodable value at startup. The guard reaches encoding mistakes and **not** the class of wrong values: a `shoal.ID` is opaque and variable-length, so there is no width to validate, and a readable name that happens to decode is indistinguishable here from a registered ID. Of fifteen plausible names, eleven decode cleanly to garbage (`gateway` to five bytes, `my-agent` to six) and only four fail on length (`llm-proxy`, `proxy`, `agent`, each `4n+1`). The eleven are refused by the plane on every request instead, as a descriptor that does not exist. |
| `agentGeneration`, a port, `tokenExpirationSeconds` or `terminationGracePeriodSeconds` that is not a whole number as written | Each is rendered verbatim, and converting before testing is lossy in exactly the cases worth refusing: `int64` of `1.5` is a positive `1`, so a guard that only checked positivity passed it and then rendered `-agent-generation=1.5`, which exits the pod on a flag error. A leading zero is worse than a refusal because it works — `010` is parsed as octal `8`, a generation nothing was registered under. |
| `llmProxy.admission.lease` above `5m` | `pkg/explorer/fleet` caps an admission lease at `MaxActionClaimTTL` and **refuses** a request outside the bound rather than shortening it. A larger value is not a longer lease; it is every call denied as an invalid argument, which the caller cannot act on and an operator cannot tell apart from a policy denial. |
| `llmProxy.upstream.baseURL` | An admitted request has nowhere to go, so the caller sees a failure on exactly the calls policy allowed — and the admission is spent on work that never happened. Must also be absolute. |
| a remote upstream with no credential at all | The credential is read at request time and nothing puts one in the pod, so every admitted call is rejected by the provider after admission has been spent on it — policy allowed work that then did not happen. A Secret or an `apiKeyFile` with `apiKeyFileSource: volume` satisfies it; a loopback provider needs neither. |
| `llmProxy.upstream.requestTimeout` above `admission.lease` | A call that outlives its lease is performed under a token that can no longer be reported against. The fleet record calls that *abandoned* rather than resolved: whether the effect happened is unknown and stays unknown, which breaks the loop the report exists to close. |
| `llmProxy.allowedHosts` | An empty allow-list answers every request `421 Misdirected Request` before admission is even asked. See the Explorer document's host-authority section: the gate is the same, and the resolved listen address of a pod is an authority no real client sends. |
| `llmProxy.identity.*` | Admission resolves the agent, capability, action and scope together. An incomplete identity is not a startup failure — it is a proxy that comes up healthy and is refused on every request, with a denial that carries no reason by design. There is nothing in the pod log to read. `agentGeneration` must be a positive `int64`; `sourceID` and `policyID` must be canonical unpadded base64url. |
| `llmProxy.healthPort` equal to `containerPort` | Two listeners cannot share a port, and which one loses is decided by bind order. If the health listener wins the proxy serves nothing; if the traffic listener wins every probe answers `421` and the pod never becomes ready. |
| either port below `1024` | The container runs as uid 65532 with all capabilities dropped. The bind fails and the pod restarts forever. |
| `llmProxy.podDisruptionBudget.maxUnavailable: 0` | The Explorer uses `0` because it is a singleton over one state root. The proxy is stateless and replaceable, so a `0` budget protects nothing and instead makes `kubectl drain` and cluster-autoscaler consolidation block forever on a pod that could safely have moved. |
| a `tokenFile` or `apiKeyFile` that is relative, or names a file at `/` | The path is both the flag and the mount the chart derives from it. A relative path resolves against a working directory nothing in the pod spec guarantees, so every read fails while the pod stays healthy; a mount at `/` replaces the container's root filesystem and the binary with it. |
| a `tokenFileSource` or `apiKeyFileSource` the chart does not render | The flag would name a path with no volume mounted there, which is an unreadable credential on every request — a pod that passes every probe and governs nothing. |
| `admission.credentialSecretName` with `tokenFileSource: projected` or `volume` | Neither source reads a Secret, so it is a credential the operator believes is being presented and the pod never opens. This is the ambiguity the binary refuses two token flags over, arriving through the chart instead. |
| `admission.tokenAudience` with a source that issues no token | The audience names the verifier a projected token is minted for. With any other source it records a binding that exists nowhere in the deployment. |
| `tokenFileSource: projected` with no `serviceAccountName` | A projected token's subject is the pod's ServiceAccount, and left implicit that is the namespace's `default` — an account every other pod in the namespace can also mint a token for, so authorizing the proxy authorizes the namespace. Naming `default` explicitly is accepted, and is the acknowledgement. |
| `tokenFileSource: projected` with no `tokenAudience` | A token projected with no audience is issued for the cluster's own API server, which the Explorer is not. Every admission request is then a 401. |
| `tokenExpirationSeconds` below `600` | The API server's floor for a token projection. Below it the pod spec is invalid, so the Deployment is accepted and no pod is ever created from it — a rollout that never completes and no pod log at all. |
| an empty or self-naming `tokenVolume`/`apiKeyVolume` | A volume with a name and no source is not a pod spec the API server accepts; a second name renders a duplicate key, so the volume is named something the mount does not reference and the kubelet never mounts the credential. |
| both credential files in one directory | Each is mounted as its own volume at its own file's directory, and a pod cannot mount two volumes at one path. |
| an explicitly chosen `tokenEnv` beside a `tokenFile`, or `apiKeyEnv` beside an `apiKeyFile` | The binary refuses that pair at startup rather than ranking them, so rendering it is a pod that never serves. The variable at its shipped default is not a second choice — that is how the file form stays reachable. |
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

The credential volumes do not change that. Each is mounted `readOnly`, the root
filesystem stays read-only, and the only thing in them is the credential the
kubelet writes from outside the container. `validate-chart.sh` asserts that a
credential mount never arrives alongside a writable root filesystem.

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

In this form the admission credential is unconditional — the chart requires a
Secret. The upstream credential is required for any non-loopback provider,
exactly as the Explorer's chat provider is, and is satisfied either by a Secret
or by an operator-supplied volume (below).

Neither of these rotates. Both are environment variables, and a container's
environment is fixed after start: changing either Secret leaves every running
proxy on the old value until the pod is replaced. Where that is not acceptable,
use a file.

## A rotating credential: the projected ServiceAccount token

A projected ServiceAccount token is a file the kubelet rewrites in place when it
rotates it. It never updates an environment variable, so with only the variable
form it could not be used at all — the admission credential had to be a
long-lived static Secret, which is the thing projected tokens exist to avoid.

### What the chart needs from you

```console
$ kubectl create serviceaccount shoal-llm-proxy
```

That is all: the account is not created by the chart, like the Secrets, and no
RBAC is needed for the token itself. The Explorer must be configured to accept
it — its OIDC authenticator has to trust the cluster as an issuer and include
the audience below in `explorer.auth.oidc.audiences` — and the descriptor the
proxy admits against has to be registered to the identity the token carries,
`system:serviceaccount:<namespace>:shoal-llm-proxy`.

### The values file, complete

```yaml
llmProxy:
  enabled: true
  image:
    repository: ghcr.io/YOUR_ORG/shoal-llm-proxy
    tag: TAG
  replicas: 2
  allowedHosts: [llm.internal.example.test]
  # The token's subject is this account. Required for the projected form, and
  # not created by the chart.
  serviceAccountName: shoal-llm-proxy
  admission:
    url: https://shoal-explorer.shoal.svc.cluster.local:8098
    # The file form. No credentialSecretName, and naming one here is refused:
    # it would be a credential the pod never opens.
    tokenFile: /var/run/secrets/shoal/token
    tokenFileSource: projected
    # Must be an audience the Explorer's authenticator accepts. Empty is not a
    # default — a token projected with no audience is issued for the cluster's
    # own API server, which the Explorer is not, so every admission request
    # would be a 401.
    tokenAudience: shoal
    # 600 is the API server's floor. 3600 is a reasonable lifetime; see below
    # for why no value above the floor can make the proxy read a stale token.
    tokenExpirationSeconds: 3600
    lease: 60s
  upstream:
    baseURL: https://api.example.test/v1
    credentialSecretName: shoal-upstream-key
    credentialSecretKey: api-key
    requestTimeout: 30s
  # Must exceed the lease by at least 5s, because the proxy drains for the
  # lease. The default is 75 against the default 60s lease.
  terminationGracePeriodSeconds: 75
  identity:
    agentID: Z292ZXJuZWQtcHJveHk
    agentGeneration: 1
    capability: chat.completions
    action: complete
    sourceID: c291cmNl
    policyID: cG9saWN5
```

What that renders, in the part that matters:

```yaml
spec:
  template:
    spec:
      serviceAccountName: shoal-llm-proxy
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        fsGroup: 65532            # not optional; see below
      containers:
        - name: shoal-llm-proxy
          args:
            - -admission-token-file=/var/run/secrets/shoal/token
            # and no -admission-token-env at all
          volumeMounts:
            - name: admission-token
              mountPath: /var/run/secrets/shoal     # the file's directory
              readOnly: true
      volumes:
        - name: admission-token
          projected:
            defaultMode: 0440     # not 0400; see below
            sources:
              - serviceAccountToken:
                  path: token     # the file's base name
                  audience: shoal
                  expirationSeconds: 3600
```

The mount is **derived from the path in the flag**: `mountPath` is its directory
and the projected `path` is its base name, so there is no second value to keep
in step with `-admission-token-file`. That is why the path has to be absolute
and has to name a file inside a directory other than `/` — a volume mounted at
`/` would replace the container's root filesystem, taking the binary with it.

### fsGroup and 0440 are one decision

The kubelet writes projected and Secret volumes owned by root, and this
container runs as uid 65532 with every capability dropped. At `0400` with no
`fsGroup`, the one process that needs the credential cannot open it — and the
failure is not a crash. The proxy starts, passes both probes, and fails every
admission request on an unreadable file, which from outside looks exactly like a
policy problem. `fsGroup: 65532` makes the mount group-owned by the group the
process runs as, and `0440` is then readable and writable by nobody.
`validate-chart.sh` asserts the pair rather than either half.

### Why the per-request read sees a fresh token

The proxy reads the file on every request, and that is what makes rotation work:
a value captured at startup would be the one that has since expired.

Two properties make it correct rather than racy.

**The rewrite is atomic.** The kubelet projects these volumes through an atomic
writer — it writes the new content into a fresh timestamped directory and then
swaps a `..data` symlink — so a reader opening the path gets either the whole
old token or the whole new one, never a half-written file.

**The rewrite happens well before the expiry.** The kubelet refreshes a
projected token when it is 80% of the way through its lifetime, so the margin
between the rewrite and the expiry is a fifth of `expirationSeconds`.

That margin is why `600` is the floor twice over. The API server rejects a
projection below 600 seconds as an invalid pod spec — which is the failure that
does not look like one: the Deployment is accepted and no pod is ever created
from it, so there is no pod log to read. And 600 is also where the margin, two
minutes, stays comfortably clear of the kubelet's own re-projection cadence of
about a minute.

So **no accepted value above the floor can make this proxy read a stale token**:
a larger `expirationSeconds` only widens the margin. The one way the proxy can
present an expired token is the kubelet failing to refresh at all — a node-level
problem — and the result is a 401 on every admission request, which denies every
call. Fail-closed, which is the only direction this component is allowed to
fail.

Longer than a day is accepted and not advised. The kubelet re-projects at 24
hours regardless, so extra validity buys no fewer rotations; it only widens the
window in which a leaked token is still accepted.

### The other two sources

`tokenFileSource` also takes:

**`secret`** — `admission.credentialSecretName` mounted as the token file
instead of projected into the environment. Worth having on its own rather than a
worse version of the variable form: the kubelet updates a mounted Secret's
contents in place, so rotating the Secret's value takes effect without rolling
the pod, which the variable form cannot do at all.

```yaml
admission:
  tokenFile: /var/run/secrets/shoal/token
  tokenFileSource: secret
  credentialSecretName: shoal-admission-token
  credentialSecretKey: token
```

**`volume`** — `admission.tokenVolume`, supplied verbatim, for a CSI driver the
chart does not model (SPIFFE, Vault Agent, secrets-store-csi). Give the volume
*source* only; the chart owns the name, so the volume and its mount cannot name
different things. Set an explicit mode and item path, or the credential lands
wherever the driver's defaults put it:

```yaml
admission:
  tokenFile: /var/run/spiffe/token
  tokenFileSource: volume
  tokenVolume:
    csi:
      driver: csi.spiffe.io
      readOnly: true
```

### The upstream credential takes a file too

`-upstream-api-key-file`, with the same reasoning and a smaller set of sources:

```yaml
upstream:
  baseURL: https://api.example.test/v1
  apiKeyFile: /etc/shoal/upstream/api-key
  apiKeyFileSource: secret        # or volume
  credentialSecretName: shoal-upstream-key
  credentialSecretKey: api-key
```

There is no `projected` source here, because a projected ServiceAccount token is
a cluster-issued credential for a cluster verifier and a third-party provider is
neither. Where a provider does accept one — workload identity federation — the
`volume` source expresses that projection verbatim, audience and expiry
included, which is strictly more than a second copy of those two keys could say.

The two credential files must sit in **different directories**. Each is mounted
as its own volume at its own file's directory, and a pod cannot mount two
volumes at one path: the API server refuses the pod spec, so the Deployment is
created and no pod is ever made from it.

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

**The lease is also the drain window, so it is also the rollout's worst case.**
On SIGTERM the proxy drains for `-lease`, because the lease is the bound on an
admitted call's entire lifetime including the report that closes it — a
ten-second drain abandoned in-flight calls whose egress had already happened and
whose grant had already been spent, which made every rolling update a producer
of unreported grants. `terminationGracePeriodSeconds` must exceed the lease by
at least five seconds for the same reason, and the chart refuses the pair when
it does not.

The cost is explicit rather than hidden. With `maxUnavailable: 0` and
`maxSurge: 1` the controller will not exceed `replicas + 1` pods, so it waits
for a terminating pod to go away before surging the next one: a rollout
serializes on termination. At a five-minute lease and ten replicas that is a
large number if every pod takes its whole window.

In practice it does not, and the distinction matters when choosing a lease.
`Shutdown` returns as soon as the last connection closes, so the drain window is
a **ceiling, not a delay** — a pod with nothing in flight exits immediately, and
a pod with a call in flight waits for that call, which `-request-timeout` already
bounds. So the expected termination time is governed by the request timeout, and
the lease only sets the worst case the kubelet must be prepared to allow. The
knob for faster rollouts is therefore `upstream.requestTimeout`; the lease
follows it, because the chart requires the lease to exceed it by the report
window.

The chart deliberately does **not** cap the lease below the fleet's own
`MaxActionClaimTTL` to make rollouts faster. The lease is a correctness bound —
how long a grant stays reportable, and therefore the longest call this proxy
will admit — and capping it for a deployment-convenience reason would silently
shorten what callers can do, which is the kind of coupling that produces a
denial nobody can explain.

## What the chart cannot express yet

Gaps in the flag contract, recorded rather than worked around.

- **No metrics or observability listener.** There is no `-metrics-address`, so
  there is nothing to scrape and nothing for the chart to annotate with
  `prometheus.io/port` — which the storage tier and read fleet both have. Issue
  #390 requires that an operator be able to tell an infrastructural denial (the
  decision plane unreachable) from a policy denial. Without a metrics surface
  that distinction exists only in logs the proxy is also forbidden from filling
  with payload content.
- **No TLS flags for the proxy's own listener.** `writeTier`, `readFleet`,
  `tserver` and `compactor` all take `tls.enabled`/`secretName`. The proxy
  cannot, so it is plaintext behind an ingress or a mesh. The Explorer has the
  same gap, which is also why reaching it over `http://` needs the explicit
  acknowledgement above.
- **No quiesce delay.** `readFleet` has `-quiesce-delay`; the proxy does not.
  Readiness drops and the listener stops accepting in the same breath, so
  requests arriving in the few seconds before the endpoints controller removes
  the pod are refused at the connection rather than answered. Nothing is
  admitted in that window, so no grant is stranded by it — the caller sees a
  connection error instead of a 503, which is worse to read and not worse to
  recover from. The drain itself is no longer unbounded or guessed at: it is
  `-lease`, and `terminationGracePeriodSeconds` is required to exceed it.
- **No report timeout.** `-request-timeout` bounds the upstream call. The report
  that follows it has no bound of its own, and an admission that is never
  reported is the outstanding case `docs/admission-seam.md` describes.
- **A withhold obligation is refused rather than applied**, because the proxy
  cannot identify which message content carries a withheld reference. Withholding
  is therefore as strong as a denial on this surface, which leaves one of #390's
  acceptance criteria unmet in substance. Tracked as
  [#426](https://github.com/phrocker/shoal-oss/issues/426).
- **No ServiceAccount is created for the projected token form.** The chart
  references `llmProxy.serviceAccountName` and does not create the account or
  any RBAC, on the same principle as the Secrets. An operator wiring the
  Kubernetes-native credential still has one object to create by hand.
- **The chart cannot check that the Explorer accepts the token it projects.**
  `tokenAudience` has to match an audience the Explorer's authenticator accepts
  and the cluster has to be a trusted issuer there, and neither is visible from
  the proxy's own values — the Explorer may be in another cluster entirely. A
  mismatch is a 401 on every admission request, with a healthy pod.
