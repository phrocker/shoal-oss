#!/usr/bin/env bash
#
# Chart checks that run locally and in CI.
#
# Three things are verified, and the third is the one that matters. Rendering
# and schema-validating the chart proves it produces well-formed Kubernetes
# objects. But the explorer and llm-proxy templates exist to make a
# misconfiguration fail at render rather than at runtime in a pod log, and a
# guard that stops firing is invisible: the chart renders, installs, and
# produces a workspace that denies everything or answers nothing — or a proxy
# that passes every probe and refuses every call. So each guard is asserted to
# refuse, and each valid configuration is asserted to render.
#
# These assertions are themselves mutation-tested: each guard is removed in
# turn and the case that must then fail is confirmed to fail. A check that
# cannot fail is worse than no check, and adding one here without that
# confirmation is how two chart guards have shipped broken before.
#
#   deploy/helm/validate-chart.sh
#
# Requires helm, python3 with PyYAML, and optionally kubeconform for schema
# validation. Each missing tool is reported rather than quietly skipped, so a
# run that checked less than it looks like says so.
set -euo pipefail

chart="$(cd "$(dirname "${BASH_SOURCE[0]}")/shoal" && pwd)"
failures=0

note() { printf '%s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; failures=$((failures + 1)); }

# A valid explorer configuration, as the preset supplies it. The refusal cases
# below each break exactly one thing in it, so a refusal can only be caused by
# the value under test.
valid_explorer=(
  --set explorer.enabled=true
  --set explorer.auth.mode=oidc
  --set explorer.auth.oidc.issuer=https://issuer.example.test/
  --set 'explorer.auth.oidc.audiences={shoal}'
  --set explorer.auth.oidc.authorizationClaim=groups
  --set 'explorer.auth.oidc.readerValues={readers}'
  --set 'explorer.allowedHosts={shoal.example.test}'
)

# The shipped profile leaves every required value empty on purpose, so it does
# not install as-is. Filling it is what an operator does; this is that.
explorer_base=(-f "$chart/values-explorer.yaml" "${valid_explorer[@]}")

# A valid LLM proxy configuration, on the same principle: every refusal case
# below breaks exactly one thing in it.
#
# The scope identities are real base64url ("source" and "policy"), because the
# chart checks the encoding — a stand-in like "src" would make the valid case
# fail for a reason that has nothing to do with the case under test.
valid_llm_proxy=(
  --set llmProxy.enabled=true
  --set 'llmProxy.allowedHosts={llm.example.test}'
  --set llmProxy.admission.url=https://shoal.example.test
  --set llmProxy.admission.credentialSecretName=shoal-admission-token
  --set llmProxy.upstream.baseURL=https://api.example.test/v1
  --set llmProxy.upstream.credentialSecretName=shoal-upstream-key
  --set llmProxy.identity.agentID=Z292ZXJuZWQtcHJveHk
  --set llmProxy.identity.capability=chat.completions
  --set llmProxy.identity.action=complete
  --set llmProxy.identity.sourceID=c291cmNl
  --set llmProxy.identity.policyID=cG9saWN5
)
llm_proxy_base=(-f "$chart/values-llm-proxy.yaml" "${valid_llm_proxy[@]}")

renders() {
  local description="$1"; shift
  if ! helm template shoal "$chart" "$@" >/dev/null 2>&1; then
    fail "should render but was refused: $description"
    helm template shoal "$chart" "$@" 2>&1 | grep -oE 'execution error.*' | head -1 | sed 's/^/      /'
  fi
}

refuses() {
  local description="$1"; shift
  if helm template shoal "$chart" "$@" >/dev/null 2>&1; then
    fail "should be refused but rendered: $description"
  fi
}

note "== tools =="
command -v helm >/dev/null 2>&1 || { printf 'FAIL  helm is required\n'; exit 1; }
if ! python3 -c 'import yaml' 2>/dev/null; then
  fail "python3 with PyYAML is required: the rendered-object checks parse Helm output"
  note "  install it with: python3 -m pip install pyyaml"
fi

note "== lint =="
for values in values.yaml values-single.yaml values-distributed.yaml values-accumulo.yaml; do
  helm lint "$chart" -f "$chart/$values" >/dev/null || fail "helm lint $values"
done
# The explorer profile is linted with the overrides that make it valid.
# Linting it as shipped lints nothing: its required values are empty on
# purpose, so the templates refuse, and `helm lint` reports a template failure
# as INFO and still exits 0 — a check that cannot fail and would also break
# outright under a helm version that treats it as an error.
helm lint "$chart" "${explorer_base[@]}" >/dev/null || fail "helm lint values-explorer.yaml"
helm lint "$chart" "${llm_proxy_base[@]}" >/dev/null || fail "helm lint values-llm-proxy.yaml"

note "== every profile renders =="
for values in values.yaml values-single.yaml values-distributed.yaml values-accumulo.yaml; do
  renders "$values" -f "$chart/$values"
done
renders "values-explorer.yaml, filled in" "${explorer_base[@]}"
renders "storage tier plus explorer" -f "$chart/values.yaml" "${valid_explorer[@]}"
refuses "values-explorer.yaml as shipped" -f "$chart/values-explorer.yaml"
renders "values-llm-proxy.yaml, filled in" "${llm_proxy_base[@]}"
renders "storage tier plus llm proxy" -f "$chart/values.yaml" "${valid_llm_proxy[@]}"
renders "explorer plus llm proxy" "${explorer_base[@]}" "${valid_llm_proxy[@]}"
refuses "values-llm-proxy.yaml as shipped" -f "$chart/values-llm-proxy.yaml"

note "== schema =="
if command -v kubeconform >/dev/null 2>&1; then
  for values in values.yaml values-single.yaml values-distributed.yaml values-accumulo.yaml; do
    helm template shoal "$chart" -f "$chart/$values" |
      kubeconform -strict -summary - >/dev/null || fail "kubeconform $values"
  done
  helm template shoal "$chart" "${explorer_base[@]}" |
    kubeconform -strict -summary - >/dev/null || fail "kubeconform values-explorer.yaml"
  helm template shoal "$chart" -f "$chart/values.yaml" "${valid_explorer[@]}" |
    kubeconform -strict -summary - >/dev/null || fail "kubeconform storage tier plus explorer"
  helm template shoal "$chart" "${llm_proxy_base[@]}" |
    kubeconform -strict -summary - >/dev/null || fail "kubeconform values-llm-proxy.yaml"
  helm template shoal "$chart" "${explorer_base[@]}" "${valid_llm_proxy[@]}" |
    kubeconform -strict -summary - >/dev/null || fail "kubeconform explorer plus llm proxy"
else
  note "  skipped: kubeconform is not on PATH"
fi

note "== guards refuse =="
refuses "authenticator unset"           "${explorer_base[@]}" --set explorer.auth.mode=
refuses "dev-auth requested"            "${explorer_base[@]}" --set explorer.auth.mode=dev-unsafe
refuses "no OIDC issuer"                "${explorer_base[@]}" --set explorer.auth.oidc.issuer=
refuses "no OIDC audience"              "${explorer_base[@]}" --set explorer.auth.oidc.audiences=null
refuses "no authorization claim"        "${explorer_base[@]}" --set explorer.auth.oidc.authorizationClaim=
refuses "claim mapped to no values"     "${explorer_base[@]}" --set explorer.auth.oidc.readerValues=null,explorer.auth.oidc.contributorValues=null,explorer.auth.oidc.fleetValues=null
refuses "no allowed hosts"              "${explorer_base[@]}" --set explorer.allowedHosts=null
refuses "more than one replica"         "${explorer_base[@]}" --set explorer.replicas=2
refuses "mosaic budget with no window"  "${explorer_base[@]}" --set explorer.disclosure.mosaic.maxDomains=3,explorer.disclosure.mosaic.window=
refuses "remote chat with no credential" "${explorer_base[@]}" --set explorer.chat.provider=openai-compatible,explorer.chat.model=m,explorer.chat.baseURL=https://api.example.test/v1
refuses "chat provider with no model"   "${explorer_base[@]}" --set explorer.chat.provider=ollama,explorer.chat.baseURL=http://localhost:11434
refuses "voyage with no credential"     "${explorer_base[@]}" --set explorer.embedding.provider=voyage,explorer.embedding.model=v3
refuses "ask executor not allowlisted"  "${explorer_base[@]}" --set explorer.fleet.askExecutorRef=ask,explorer.chat.provider=ollama,explorer.chat.model=m,explorer.chat.baseURL=http://localhost:11434
refuses "ask executor with no chat"     "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={ask}',explorer.fleet.askExecutorRef=ask
refuses "health port collides"          "${explorer_base[@]}" --set explorer.healthPort=8098
# Present-but-blank is not configured. The workspace drops empty entries and
# then reports the setting missing, so these render a pod that exits at startup.
refuses "blank issuer"                  "${explorer_base[@]}" --set explorer.auth.oidc.issuer=" "
refuses "blank audience only"           "${explorer_base[@]}" --set 'explorer.auth.oidc.audiences={ }'
refuses "blank authorization claim"     "${explorer_base[@]}" --set explorer.auth.oidc.authorizationClaim=" "
refuses "blank role value only"         "${explorer_base[@]}" --set 'explorer.auth.oidc.readerValues={ }'
refuses "blank allowed host only"       "${explorer_base[@]}" --set 'explorer.allowedHosts={ }'
refuses "placeholder issuer"            "${explorer_base[@]}" --set explorer.auth.oidc.issuer=https://REPLACE_ME/
refuses "placeholder allowed host"      "${explorer_base[@]}" --set 'explorer.allowedHosts={REPLACE_ME.example.test}'
# A placeholder in a role mapping is worse than a non-working deployment: it is
# a working one that authorizes the literal claim "REPLACE_ME", denying every
# real caller while the chart reports success.
refuses "placeholder reader value"      "${explorer_base[@]}" --set 'explorer.auth.oidc.readerValues={REPLACE_ME}'
refuses "placeholder contributor value" "${explorer_base[@]}" --set 'explorer.auth.oidc.contributorValues={REPLACE_ME}'
refuses "placeholder fleet value"       "${explorer_base[@]}" --set 'explorer.auth.oidc.fleetValues={REPLACE_ME}'

note "== valid configurations still render =="
renders "loopback chat needs no credential" "${explorer_base[@]}" --set explorer.chat.provider=ollama,explorer.chat.model=llama3,explorer.chat.baseURL=http://localhost:11434
renders "remote chat with a credential"     "${explorer_base[@]}" --set explorer.chat.provider=openai-compatible,explorer.chat.model=m,explorer.chat.baseURL=https://api.example.test/v1,explorer.chat.credentialSecretName=chat
renders "mosaic budget enabled"             "${explorer_base[@]}" --set explorer.disclosure.mosaic.maxDomains=3
renders "withholding concealed"             "${explorer_base[@]}" --set explorer.disclosure.concealWithholding=true
renders "ask executor wired"                "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={ask}',explorer.fleet.askExecutorRef=ask,explorer.chat.provider=ollama,explorer.chat.model=m,explorer.chat.baseURL=http://localhost:11434
renders "lexical embedding"                 "${explorer_base[@]}" --set explorer.embedding.provider=lexical,explorer.embedding.dimensions=256
renders "scaled to zero"                    "${explorer_base[@]}" --set explorer.replicas=0
renders "values around the blanks are trimmed" "${explorer_base[@]}" --set 'explorer.allowedHosts={ shoal.example.test , }'

note "== llm proxy guards refuse =="
# The proxy's failure mode is not a crash. It is required to fail closed, so
# nearly every misconfiguration below renders a pod that passes every probe and
# denies every call — which from outside is a total outage of whatever is
# configured to go through it. That is what these move to render time.
refuses "no admission URL"              "${llm_proxy_base[@]}" --set llmProxy.admission.url=
refuses "blank admission URL"           "${llm_proxy_base[@]}" --set llmProxy.admission.url=" "
refuses "admission URL with no scheme"  "${llm_proxy_base[@]}" --set llmProxy.admission.url=shoal.example.test
refuses "plaintext remote admission"    "${llm_proxy_base[@]}" --set llmProxy.admission.url=http://shoal.example.test
refuses "no admission token env"        "${llm_proxy_base[@]}" --set llmProxy.admission.tokenEnv=
refuses "blank admission token env"     "${llm_proxy_base[@]}" --set llmProxy.admission.tokenEnv=" "
refuses "no admission credential"       "${llm_proxy_base[@]}" --set llmProxy.admission.credentialSecretName=
refuses "blank admission credential"    "${llm_proxy_base[@]}" --set llmProxy.admission.credentialSecretName=" "
refuses "no admission credential key"   "${llm_proxy_base[@]}" --set llmProxy.admission.credentialSecretKey=
# MaxActionClaimTTL is five minutes and the admission surface refuses a request
# outside it, so a longer lease is not a longer lease — it is every call denied.
refuses "lease above MaxActionClaimTTL" "${llm_proxy_base[@]}" --set llmProxy.admission.lease=10m
refuses "lease at 5m plus a second"     "${llm_proxy_base[@]}" --set llmProxy.admission.lease=5m1s
refuses "zero lease"                    "${llm_proxy_base[@]}" --set llmProxy.admission.lease=0s
# The duration parser, from both sides. Each of these is a value whose leading
# characters do parse — "30sec" scans as 30s, "1.5m" as 5m — so dropping the
# parser's refusal makes them render rather than trip a neighbouring guard.
# That is what makes them attribute to the parser and not to something else.
refuses "unparseable lease"             "${llm_proxy_base[@]}" --set llmProxy.admission.lease=30sec
refuses "fractional lease"              "${llm_proxy_base[@]}" --set llmProxy.admission.lease=1.5m
refuses "unparseable request timeout"   "${llm_proxy_base[@]}" --set llmProxy.upstream.requestTimeout=30sec
refuses "unitless lease"                "${llm_proxy_base[@]}" --set llmProxy.admission.lease=30
refuses "no upstream base URL"          "${llm_proxy_base[@]}" --set llmProxy.upstream.baseURL=
refuses "blank upstream base URL"       "${llm_proxy_base[@]}" --set llmProxy.upstream.baseURL=" "
refuses "upstream URL with no scheme"   "${llm_proxy_base[@]}" --set llmProxy.upstream.baseURL=api.example.test/v1
refuses "no upstream api key env"       "${llm_proxy_base[@]}" --set llmProxy.upstream.apiKeyEnv=
refuses "remote upstream, no credential" "${llm_proxy_base[@]}" --set llmProxy.upstream.credentialSecretName=
refuses "upstream credential, no key"   "${llm_proxy_base[@]}" --set llmProxy.upstream.credentialSecretKey=
# A call that outlives its lease is performed under a token that can no longer
# be reported against: the admission is abandoned, not resolved.
refuses "timeout outlives the lease"    "${llm_proxy_base[@]}" --set llmProxy.upstream.requestTimeout=90s
refuses "unbounded upstream request"    "${llm_proxy_base[@]}" --set llmProxy.upstream.requestTimeout=0s
refuses "no allowed hosts"              "${llm_proxy_base[@]}" --set llmProxy.allowedHosts=null
refuses "blank allowed host only"       "${llm_proxy_base[@]}" --set 'llmProxy.allowedHosts={ }'
refuses "no agent id"                   "${llm_proxy_base[@]}" --set llmProxy.identity.agentID=
refuses "blank agent id"                "${llm_proxy_base[@]}" --set llmProxy.identity.agentID=" "
refuses "no capability"                 "${llm_proxy_base[@]}" --set llmProxy.identity.capability=
refuses "no action"                     "${llm_proxy_base[@]}" --set llmProxy.identity.action=
refuses "no source id"                  "${llm_proxy_base[@]}" --set llmProxy.identity.sourceID=
refuses "no policy id"                  "${llm_proxy_base[@]}" --set llmProxy.identity.policyID=
refuses "zero agent generation"         "${llm_proxy_base[@]}" --set llmProxy.identity.agentGeneration=0
refuses "negative agent generation"     "${llm_proxy_base[@]}" --set llmProxy.identity.agentGeneration=-1
refuses "source id is not base64url"    "${llm_proxy_base[@]}" --set 'llmProxy.identity.sourceID=source/one'
refuses "padded policy id"              "${llm_proxy_base[@]}" --set 'llmProxy.identity.policyID=cG9saWN5=='
refuses "truncated source id"           "${llm_proxy_base[@]}" --set llmProxy.identity.sourceID=c291cmNlZ
refuses "placeholder admission URL"     "${llm_proxy_base[@]}" --set llmProxy.admission.url=https://REPLACE_ME/
refuses "placeholder allowed host"      "${llm_proxy_base[@]}" --set 'llmProxy.allowedHosts={REPLACE_ME.example.test}'
# A placeholder identity is the dangerous case: the proxy comes up, passes every
# probe, and is refused by the plane on every request while the chart reports
# success.
refuses "placeholder capability"        "${llm_proxy_base[@]}" --set llmProxy.identity.capability=REPLACE_ME
refuses "placeholder upstream URL"      "${llm_proxy_base[@]}" --set llmProxy.upstream.baseURL=https://REPLACE_ME/v1
refuses "health port collides"          "${llm_proxy_base[@]}" --set llmProxy.healthPort=8100
refuses "privileged listen port"        "${llm_proxy_base[@]}" --set llmProxy.containerPort=80
refuses "privileged health port"        "${llm_proxy_base[@]}" --set llmProxy.healthPort=81
refuses "port out of range"             "${llm_proxy_base[@]}" --set llmProxy.servicePort=70000
refuses "negative replicas"             "${llm_proxy_base[@]}" --set llmProxy.replicas=-1
refuses "fractional replicas"           "${llm_proxy_base[@]}" --set llmProxy.replicas=1.5
refuses "null replicas"                 "${llm_proxy_base[@]}" --set llmProxy.replicas=null
# The mirror image of the explorer's rule. A budget that refuses every eviction
# protects a singleton; this is not one, so it only wedges drains.
refuses "singleton disruption budget"   "${llm_proxy_base[@]}" --set llmProxy.podDisruptionBudget.maxUnavailable=0
# A key written with nothing after it is nil, not "". `toString nil` is the
# string "<nil>", which is non-blank — so a required-value check that does not
# normalize first reads an absent value as configured and renders the pod with
# an empty flag. These were observed rendering before `default ""` went in
# ahead of every `trim`, which is the whole reason they are pinned here.
refuses "null admission URL"            "${llm_proxy_base[@]}" --set llmProxy.admission.url=null
refuses "null admission token env"      "${llm_proxy_base[@]}" --set llmProxy.admission.tokenEnv=null
refuses "null admission credential"     "${llm_proxy_base[@]}" --set llmProxy.admission.credentialSecretName=null
refuses "null lease"                    "${llm_proxy_base[@]}" --set llmProxy.admission.lease=null
refuses "null upstream base URL"        "${llm_proxy_base[@]}" --set llmProxy.upstream.baseURL=null
refuses "null upstream api key env"     "${llm_proxy_base[@]}" --set llmProxy.upstream.apiKeyEnv=null
refuses "null request timeout"          "${llm_proxy_base[@]}" --set llmProxy.upstream.requestTimeout=null
refuses "null agent id"                 "${llm_proxy_base[@]}" --set llmProxy.identity.agentID=null
refuses "null capability"               "${llm_proxy_base[@]}" --set llmProxy.identity.capability=null
refuses "null action"                   "${llm_proxy_base[@]}" --set llmProxy.identity.action=null
refuses "null source id"                "${llm_proxy_base[@]}" --set llmProxy.identity.sourceID=null
refuses "null agent generation"         "${llm_proxy_base[@]}" --set llmProxy.identity.agentGeneration=null

note "== llm proxy valid configurations still render =="
# The proxy is stateless and must not be a singleton: the explorer refuses
# replicas above 1 and the proxy must not, so scaling out is asserted to render
# rather than merely left unforbidden.
renders "scaled out to ten"                 "${llm_proxy_base[@]}" --set llmProxy.replicas=10
renders "scaled to zero"                    "${llm_proxy_base[@]}" --set llmProxy.replicas=0
renders "loopback upstream needs no credential" "${llm_proxy_base[@]}" --set llmProxy.upstream.credentialSecretName=,llmProxy.upstream.baseURL=http://localhost:11434/v1
renders "plaintext admission acknowledged"  "${llm_proxy_base[@]}" --set llmProxy.admission.url=http://shoal-explorer:8098,llmProxy.admission.allowPlaintext=true
renders "loopback admission needs no acknowledgement" "${llm_proxy_base[@]}" --set llmProxy.admission.url=http://127.0.0.1:8098
# The fleet's claim ceiling is the upper bound, and the report window is the
# lower one. A lease equal to the timeout is refused, not accepted: the binary
# refuses that pair at startup, so rendering it would produce a pod that never
# serves — which is the failure these guards exist to move to render time.
renders "lease at the ceiling with room to report" "${llm_proxy_base[@]}" --set llmProxy.admission.lease=5m,llmProxy.upstream.requestTimeout=4m
refuses "lease equal to the timeout"        "${llm_proxy_base[@]}" --set llmProxy.admission.lease=5m,llmProxy.upstream.requestTimeout=5m
refuses "lease inside the report window"    "${llm_proxy_base[@]}" --set llmProxy.admission.lease=35s,llmProxy.upstream.requestTimeout=31s
renders "compound durations"                "${llm_proxy_base[@]}" --set llmProxy.admission.lease=2m30s,llmProxy.upstream.requestTimeout=1m30s
refuses "timeout equal to the lease"        "${llm_proxy_base[@]}" --set llmProxy.upstream.requestTimeout=60s
renders "millisecond timeout"               "${llm_proxy_base[@]}" --set llmProxy.upstream.requestTimeout=500ms
renders "disruption budget disabled"        "${llm_proxy_base[@]}" --set llmProxy.podDisruptionBudget.enabled=false,llmProxy.podDisruptionBudget.maxUnavailable=0
renders "operator-supplied affinity"        "${llm_proxy_base[@]}" --set 'llmProxy.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].key=kubernetes.io/os' --set 'llmProxy.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].operator=In' --set 'llmProxy.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].values={linux}'
renders "several allowed hosts, trimmed"    "${llm_proxy_base[@]}" --set 'llmProxy.allowedHosts={ llm.example.test , llm-internal.example.test }'

note "== the probe surface is wired end to end =="
# Three separate things have to agree, and the pod never becomes ready if any
# one of them drifts:
#
#   the probes address the health port, not the workspace port, which refuses
#   any Host an allow-list cannot name and so answers 421 to a kubelet;
#   -health-address is actually passed, or nothing listens there;
#   the port it names is the port the probes and the container port declare.
#
# Checking only the first would pass a chart whose probes point at a port no
# process is bound to.
if ! helm template shoal "$chart" "${explorer_base[@]}" | python3 -c '
import sys, yaml

problems = []
for document in yaml.safe_load_all(sys.stdin):
    if not document or document.get("kind") != "StatefulSet":
        continue
    container = document["spec"]["template"]["spec"]["containers"][0]

    probes = ("readinessProbe", "livenessProbe", "startupProbe")
    for probe in probes:
        if probe not in container:
            problems.append(f"{probe} is missing")
            continue
        port = container[probe]["httpGet"]["port"]
        if port != "health":
            problems.append(f"{probe} addresses {port} rather than the health port")

    declared = {port["name"]: port["containerPort"] for port in container["ports"]}
    if "health" not in declared:
        problems.append("the container declares no health port")

    served = [
        argument.split(":")[-1]
        for argument in container["args"]
        if argument.startswith("-health-address=")
    ]
    if not served:
        problems.append(
            "-health-address is not passed, so nothing listens on the port the "
            "probes address and the pod never becomes ready"
        )
    elif "health" in declared and int(served[0]) != int(declared["health"]):
        problems.append(
            f"-health-address serves {served[0]} but the health port is "
            f"{declared['health']}"
        )

for problem in problems:
    print(problem)
raise SystemExit(1 if problems else 0)
'; then
  fail "the explorer probe surface is not wired end to end (see above)"
fi

note "== the llm proxy is wired end to end =="
# Same end-to-end rule as the explorer's check above, plus the second listener
# the proxy has: the one carrying traffic.
#
# The trap this is written against is real and was hit on the explorer: a check
# that asserted only that the probes *name* the health port passed a chart whose
# probes addressed a port nothing was listening on. So every link is asserted
# rather than any one of them:
#
#   the probes address the health port, not the OpenAI-compatible port, which
#   answers 421 to a kubelet addressing the pod by IP;
#   -health-address is passed, and names the port the probes address and the
#   container declares;
#   -listen names the port the container declares as http and the Service
#   targets, or the Service routes to a port nothing serves;
#   the Service does not publish the health port, which is unauthenticated;
#   the Service's selector actually matches the pod template's labels, or the
#   Service has no endpoints at all and nothing above matters.
#
# It renders with padding around the configured values on purpose. The host
# gate matches an authority exactly, so " llm.example.test" matches nothing —
# a 421 to every request, produced by a space in a YAML list. Trimming in the
# guard but not in the argument would let the chart pass its own check and
# render exactly that, so the arguments are asserted to come out trimmed.
if ! helm template shoal "$chart" "${llm_proxy_base[@]}" \
  --set 'llmProxy.allowedHosts={ llm.example.test ,, llm-internal.example.test }' \
  --set 'llmProxy.admission.url= https://shoal.example.test ' \
  --set 'llmProxy.identity.capability= chat.completions ' | python3 -c '
import sys, yaml

problems = []
deployment = None
services = []
for document in yaml.safe_load_all(sys.stdin):
    if not document:
        continue
    component = document.get("metadata", {}).get("labels", {}).get(
        "app.kubernetes.io/component")
    if component != "llm-proxy":
        continue
    if document["kind"] == "Deployment":
        deployment = document
    elif document["kind"] == "Service":
        services.append(document)

if deployment is None:
    print("no llm-proxy Deployment was rendered")
    raise SystemExit(1)

# A Deployment, not a StatefulSet: the proxy is stateless, and this is the one
# structural difference from the explorer.
template = deployment["spec"]["template"]
container = template["spec"]["containers"][0]
declared = {port["name"]: port["containerPort"] for port in container["ports"]}

for name in ("http", "health"):
    if name not in declared:
        problems.append(f"the container declares no {name} port")

if "startupProbe" in container:
    problems.append(
        "a startupProbe only delays the first readiness check: the proxy opens "
        "no corpus"
    )
for probe in ("readinessProbe", "livenessProbe"):
    if probe not in container:
        problems.append(f"{probe} is missing")
        continue
    port = container[probe]["httpGet"]["port"]
    if port != "health":
        problems.append(
            f"{probe} addresses {port} rather than the health port, which "
            "answers 421 to a kubelet addressing the pod by its IP"
        )


expected_hosts = "llm.example.test,llm-internal.example.test"
for argument in container["args"]:
    flag, _, value = argument.partition("=")
    if value != value.strip():
        problems.append(
            f"{flag} is rendered as {argument!r}, with whitespace the host gate "
            "and the flag parser both match literally"
        )
    if flag == "-allowed-host" and value != expected_hosts:
        problems.append(
            f"-allowed-host is {value!r}, not {expected_hosts!r}: blank entries "
            "and padding must be dropped, or an authority matched exactly can "
            "never match"
        )

for name in ("env",):
    for entry in container.get(name, []):
        if entry["name"] != entry["name"].strip():
            problems.append(f"env name {entry['name']!r} carries whitespace")


def served(flag):
    values = [
        argument.split("=", 1)[1].rsplit(":", 1)[-1]
        for argument in container["args"]
        if argument.startswith(flag + "=")
    ]
    return values[0] if values else None


for flag, name in (("-health-address", "health"), ("-listen", "http")):
    port = served(flag)
    if port is None:
        problems.append(
            f"{flag} is not passed, so nothing listens on the {name} port"
        )
    elif name in declared and int(port) != int(declared[name]):
        problems.append(
            f"{flag} serves {port} but the {name} port is {declared[name]}"
        )

if not services:
    problems.append("no llm-proxy Service was rendered")
for service in services:
    for port in service["spec"]["ports"]:
        target = port.get("targetPort")
        if target != "http":
            problems.append(
                "Service port %s targets %s rather than the http port"
                % (port["port"], target)
            )
        if "health" in declared and port["port"] == declared["health"]:
            problems.append(
                "the Service publishes the health port, which is "
                "unauthenticated and exists only for the kubelet"
            )
    selector = service["spec"]["selector"]
    labels = template["metadata"]["labels"]
    missing = {
        key: value for key, value in selector.items() if labels.get(key) != value
    }
    if missing:
        problems.append(
            f"the Service selector {missing} does not match the pod labels, so "
            "it has no endpoints"
        )

for problem in problems:
    print(problem)
raise SystemExit(1 if problems else 0)
'; then
  fail "the llm proxy is not wired end to end (see above)"
fi

note "== every rendered name fits the 63-character limit =="
# Kubernetes rejects a name longer than 63 characters, and the explorer's
# headless Service name is the longest the chart derives. A long release name is
# the case that finds it: truncating the finished name is not enough, because
# the suffix is appended after the truncation. Helm caps a release name at 53,
# so this is the worst case an install can actually present.
#
# Both planes are rendered together so every name the chart derives — including
# the proxy's, which hangs off its own bounded stem — is measured in one pass.
long_release="shoal-production-authorized-plane-euw1-cluster-prime"
if ! helm template "$long_release" "$chart" "${explorer_base[@]}" "${valid_llm_proxy[@]}" | python3 -c '
import sys, yaml

problems = []
for document in yaml.safe_load_all(sys.stdin):
    if not document:
        continue
    kind = document.get("kind", "object")
    name = document.get("metadata", {}).get("name", "")
    if len(name) > 63:
        problems.append("%s name is %d characters: %s" % (kind, len(name), name))
    if kind == "StatefulSet":
        service = document["spec"]["serviceName"]
        if len(service) > 63:
            problems.append("serviceName is %d characters: %s" % (len(service), service))

for problem in problems:
    print(problem)
raise SystemExit(1 if problems else 0)
'; then
  fail "a long release name produces a name Kubernetes will reject (see above)"
fi

if [ "$failures" -ne 0 ]; then
  note ""
  note "$failures check(s) failed"
  exit 1
fi
note ""
note "all chart checks passed"
