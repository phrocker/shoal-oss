#!/usr/bin/env bash
#
# Chart checks that run locally and in CI.
#
# Three things are verified, and the third is the one that matters. Rendering
# and schema-validating the chart proves it produces well-formed Kubernetes
# objects. But the explorer templates exist to make a misconfiguration fail at
# render rather than at runtime in a pod log, and a guard that stops firing is
# invisible: the chart renders, installs, and produces a workspace that denies
# everything or answers nothing. So each guard is asserted to refuse, and each
# valid configuration is asserted to render.
#
#   deploy/helm/validate-chart.sh
#
# Requires helm. Uses kubeconform for schema validation when it is on PATH and
# says so when it is not, rather than quietly checking less.
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

note "== lint =="
for values in values.yaml values-single.yaml values-distributed.yaml values-accumulo.yaml values-explorer.yaml; do
  helm lint "$chart" -f "$chart/$values" >/dev/null || fail "helm lint $values"
done

note "== every profile renders =="
for values in values.yaml values-single.yaml values-distributed.yaml values-accumulo.yaml; do
  renders "$values" -f "$chart/$values"
done
renders "values-explorer.yaml" -f "$chart/values-explorer.yaml"
renders "storage tier plus explorer" -f "$chart/values.yaml" "${valid_explorer[@]}"

note "== schema =="
if command -v kubeconform >/dev/null 2>&1; then
  for values in values.yaml values-single.yaml values-distributed.yaml values-accumulo.yaml; do
    helm template shoal "$chart" -f "$chart/$values" |
      kubeconform -strict -summary - >/dev/null || fail "kubeconform $values"
  done
  helm template shoal "$chart" -f "$chart/values-explorer.yaml" |
    kubeconform -strict -summary - >/dev/null || fail "kubeconform values-explorer.yaml"
  helm template shoal "$chart" -f "$chart/values.yaml" "${valid_explorer[@]}" |
    kubeconform -strict -summary - >/dev/null || fail "kubeconform storage tier plus explorer"
else
  note "  skipped: kubeconform is not on PATH"
fi

note "== guards refuse =="
explorer_base=(-f "$chart/values-explorer.yaml")
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

note "== valid configurations still render =="
renders "loopback chat needs no credential" "${explorer_base[@]}" --set explorer.chat.provider=ollama,explorer.chat.model=llama3,explorer.chat.baseURL=http://localhost:11434
renders "remote chat with a credential"     "${explorer_base[@]}" --set explorer.chat.provider=openai-compatible,explorer.chat.model=m,explorer.chat.baseURL=https://api.example.test/v1,explorer.chat.credentialSecretName=chat
renders "mosaic budget enabled"             "${explorer_base[@]}" --set explorer.disclosure.mosaic.maxDomains=3
renders "withholding concealed"             "${explorer_base[@]}" --set explorer.disclosure.concealWithholding=true
renders "ask executor wired"                "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={ask}',explorer.fleet.askExecutorRef=ask,explorer.chat.provider=ollama,explorer.chat.model=m,explorer.chat.baseURL=http://localhost:11434
renders "lexical embedding"                 "${explorer_base[@]}" --set explorer.embedding.provider=lexical,explorer.embedding.dimensions=256
renders "scaled to zero"                    "${explorer_base[@]}" --set explorer.replicas=0

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
if ! helm template shoal "$chart" -f "$chart/values-explorer.yaml" | python3 -c '
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

if [ "$failures" -ne 0 ]; then
  note ""
  note "$failures check(s) failed"
  exit 1
fi
note ""
note "all chart checks passed"
