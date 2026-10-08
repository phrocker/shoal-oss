#!/usr/bin/env bash
#
# Chart checks that run locally and in CI.
#
# Three things are verified, and the third is the one that matters. Rendering
# and schema-validating the chart proves it produces well-formed Kubernetes
# objects. But the explorer and llm-gateway templates exist to make a
# misconfiguration fail at render rather than at runtime in a pod log, and a
# guard that stops firing is invisible: the chart renders, installs, and
# produces a workspace that denies everything or answers nothing — or a gateway
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

# A valid LLM gateway configuration, on the same principle: every refusal case
# below breaks exactly one thing in it.
#
# The scope identities are real base64url ("source" and "policy"), because the
# chart checks the encoding — a stand-in like "src" would make the valid case
# fail for a reason that has nothing to do with the case under test.
valid_llm_gateway=(
  --set llmGateway.enabled=true
  --set 'llmGateway.allowedHosts={llm.example.test}'
  --set llmGateway.admission.url=https://shoal.example.test
  --set llmGateway.admission.credentialSecretName=shoal-admission-token
  --set llmGateway.upstream.baseURL=https://api.example.test/v1
  --set llmGateway.upstream.credentialSecretName=shoal-upstream-key
  --set llmGateway.identity.agentID=Z292ZXJuZWQtcHJveHk
  --set llmGateway.identity.capability=chat.completions
  --set llmGateway.identity.action=complete
  --set llmGateway.identity.sourceID=c291cmNl
  --set llmGateway.identity.policyID=cG9saWN5
)
llm_gateway_base=(-f "$chart/values-llm-gateway.yaml" "${valid_llm_gateway[@]}")

# The same configuration with the admission token coming from a file instead of
# an environment variable, as a projected ServiceAccount token.
#
# It is a separate fixture rather than one more --set on the base, because the
# two forms require different things and a fixture that carried both could not
# express either. The admission Secret is cleared on purpose: naming one here
# is refused, since nothing in the projected form reads it, and leaving it set
# would make every case below pass or fail for that reason instead of its own.
valid_token_file=(
  --set llmGateway.admission.credentialSecretName=
  --set llmGateway.admission.tokenFile=/var/run/secrets/shoal/token
  --set llmGateway.admission.tokenAudience=shoal
  --set llmGateway.serviceAccountName=shoal-llm-gateway
)
token_file_base=("${llm_gateway_base[@]}" "${valid_token_file[@]}")

renders() {
  local description="$1"; shift
  if ! helm template shoal "$chart" "$@" >/dev/null 2>&1; then
    fail "should render but was refused: $description"
    helm template shoal "$chart" "$@" 2>&1 | grep -oE 'execution error.*|Error: .*' | head -1 | sed 's/^/      /' || true
  fi
}

refuses() {
  local description="$1"; shift
  if helm template shoal "$chart" "$@" >/dev/null 2>&1; then
    fail "should be refused but rendered: $description"
  fi
}

# A refusal is only evidence of a guard when it is *that* guard's refusal.
#
# Several values below are refused by something downstream as well: an unknown
# token file source also trips the audience rule, a non-numeric token lifetime
# also casts to 0 and trips the floor, and an empty operator volume renders YAML
# Helm itself rejects. A case that only asserted "did not render" would keep
# passing with the guard it is named for deleted — which is the defect this
# script exists to prevent, arriving through the assertion instead of the
# template. These pin the sentence.
# A pattern that must appear in the rendered output. "renders" only proves a
# values file was not refused, which cannot see whether what the API server and
# the binary actually receive says what the operator asked for — and a list
# joined into one argument is exactly where a blank element disappears quietly
# or arrives as an empty name.
assert_renders() {
  local description="$1" pattern="$2"; shift 2
  local rendered
  if ! rendered=$(helm template shoal "$chart" "$@" 2>&1); then
    fail "should render but was refused: $description"
    return
  fi
  if ! printf '%s\n' "$rendered" | grep -qE -- "$pattern"; then
    fail "the rendered output does not match /$pattern/: $description"
  fi
}

# The mirror, for a flag that must not appear unless an operator asked for it.
# "renders" cannot see this and neither can assert_renders: a key that widened
# an effect ceiling by default would render, validate, install, and pass every
# positive assertion in this file. The absence is the property.
assert_absent() {
  local description="$1" pattern="$2"; shift 2
  local rendered
  if ! rendered=$(helm template shoal "$chart" "$@" 2>&1); then
    fail "should render but was refused: $description"
    return
  fi
  if printf '%s\n' "$rendered" | grep -qE -- "$pattern"; then
    fail "the rendered output matches /$pattern/ and must not: $description"
    printf '%s\n' "$rendered" | grep -E -- "$pattern" | head -3 | sed 's/^/      /'
  fi
}

assert_absent_or_refused() {
  local description="$1" pattern="$2"; shift 2
  local rendered
  if ! rendered=$(helm template shoal "$chart" "$@" 2>&1); then
    return 0
  fi
  if printf '%s\n' "$rendered" | grep -qE -- "$pattern"; then
    fail "the rendered output matches /$pattern/ and must not: $description"
    printf '%s\n' "$rendered" | grep -E -- "$pattern" | head -3 | sed 's/^/      /'
  fi
}

refuses_citing() {
  local expected="$1" description="$2"; shift 2
  local output
  if output="$(helm template shoal "$chart" "$@" 2>&1)"; then
    fail "should be refused but rendered: $description"
  elif ! printf '%s' "$output" | grep -qF -- "$expected"; then
    fail "refused, but not by the guard under test: $description"
    # Helm's own YAML errors carry no "execution error", and under pipefail a
    # grep that finds nothing would end the whole run here, silently skipping
    # every check after it. So any error line is shown, and none is not fatal.
    printf '%s' "$output" | grep -oE 'execution error.*|Error: .*' | head -1 |
      cut -c1-200 | sed 's/^/      /' || true
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
helm lint "$chart" "${llm_gateway_base[@]}" >/dev/null || fail "helm lint values-llm-gateway.yaml"

note "== every profile renders =="
for values in values.yaml values-single.yaml values-distributed.yaml values-accumulo.yaml; do
  renders "$values" -f "$chart/$values"
done
renders "values-explorer.yaml, filled in" "${explorer_base[@]}"
renders "storage tier plus explorer" -f "$chart/values.yaml" "${valid_explorer[@]}"
refuses "values-explorer.yaml as shipped" -f "$chart/values-explorer.yaml"
renders "values-llm-gateway.yaml, filled in" "${llm_gateway_base[@]}"
renders "storage tier plus llm gateway" -f "$chart/values.yaml" "${valid_llm_gateway[@]}"
renders "explorer plus llm gateway" "${explorer_base[@]}" "${valid_llm_gateway[@]}"
refuses "values-llm-gateway.yaml as shipped" -f "$chart/values-llm-gateway.yaml"

note "== the storage profiles are unchanged by the enforcement plane =="
# The standing guarantee for this chart: work on the explorer and the gateway
# changes nothing for an operator who has not enabled them. Both are off by
# default and independent of `mode`, so each storage profile must render
# byte-identically to the baseline — not merely "still render", which is what
# the section above checks and what a stray always-rendered key would pass.
#
# The baseline is origin/main. A clone without it reports a skip rather than
# quietly checking nothing, as the schema section does for kubeconform.
baseline="${SHOAL_CHART_BASELINE:-origin/main}"
# From the repository root, because an `archive` pathspec is resolved against
# the working directory and the chart is three levels down.
repository="$(git -C "$chart" rev-parse --show-toplevel 2>/dev/null || true)"
if [ -n "$repository" ] &&
  git -C "$repository" rev-parse --verify --quiet "$baseline" >/dev/null 2>&1; then
  reference="$(mktemp -d)"
  trap 'rm -rf "$reference"' EXIT
  if git -C "$repository" archive "$baseline" deploy/helm/shoal | tar -x -C "$reference"; then
    for values in values.yaml values-single.yaml values-distributed.yaml values-accumulo.yaml; do
      if ! diff -u \
        <(helm template shoal "$reference/deploy/helm/shoal" -f "$reference/deploy/helm/shoal/$values" 2>&1) \
        <(helm template shoal "$chart" -f "$chart/$values" 2>&1) > "$reference/diff"; then
        fail "$values no longer renders byte-identically to $baseline: the enforcement plane is off by default and must change nothing for anyone who has not enabled it"
        head -20 "$reference/diff" | sed 's/^/      /'
      fi
    done
    # The explorer and gateway profiles may change bytes — quoting a scalar
    # does — but never meaning: every object must parse to exactly what the
    # baseline's did. This is what lets a template outside the storage
    # profiles be quoted without a reviewer diffing manifests by eye. The
    # explorer case names a storage class and a chat credential, so the
    # optional fields it quotes are rendered too.
    reference_chart="$reference/deploy/helm/shoal"
    explorer_parsed=(--set explorer.storageClassName=fast --set explorer.chat.provider=openai-compatible,explorer.chat.model=m,explorer.chat.baseURL=https://api.example.test/v1,explorer.chat.credentialSecretName=chat --set explorer.embedding.provider=voyage,explorer.embedding.model=v3,explorer.embedding.credentialSecretName=voyage)
    for profile in explorer llm-gateway; do
      if [ "$profile" = explorer ]; then
        overrides=("${valid_explorer[@]}" "${explorer_parsed[@]}")
      else
        overrides=("${valid_llm_gateway[@]}")
      fi
      # Both must render, or two identical error messages would compare equal.
      if ! helm template shoal "$reference_chart" -f "$reference_chart/values-$profile.yaml" "${overrides[@]}" > "$reference/before" 2>&1 ||
        ! helm template shoal "$chart" -f "$chart/values-$profile.yaml" "${overrides[@]}" > "$reference/after" 2>&1; then
        fail "values-$profile.yaml, filled in, does not render on both $baseline and this tree, so it cannot be compared"
      elif ! python3 - "$reference/before" "$reference/after" <<'PARSED'
import sys, yaml
before, after = (list(yaml.safe_load_all(open(path))) for path in sys.argv[1:])
raise SystemExit(0 if before == after else 1)
PARSED
      then
        fail "values-$profile.yaml, filled in, no longer parses to the same objects as $baseline: quoting may change bytes, never meaning"
      fi
    done
  else
    fail "could not export the chart at $baseline to compare against"
  fi
else
  note "  skipped: $baseline is not in this clone (set SHOAL_CHART_BASELINE)"
fi

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
  helm template shoal "$chart" "${llm_gateway_base[@]}" |
    kubeconform -strict -summary - >/dev/null || fail "kubeconform values-llm-gateway.yaml"
  helm template shoal "$chart" "${explorer_base[@]}" "${valid_llm_gateway[@]}" |
    kubeconform -strict -summary - >/dev/null || fail "kubeconform explorer plus llm gateway"
  # The file form renders a volume, a mount and an fsGroup the env form does
  # not, so it is a different object shape and needs its own schema pass.
  helm template shoal "$chart" "${token_file_base[@]}" |
    kubeconform -strict -summary - >/dev/null || fail "kubeconform llm gateway with a projected token"
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

note "== a reference cannot carry a character that changes what the argument means =="
# Every executor reference reaches the container inside one argument, and three
# of the four settings are comma-joined into it. Two characters therefore cannot
# appear in a reference without changing what the argument says.
#
# A newline was the serious one. The container arguments were unquoted YAML
# scalars, so a newline in any list entry ended the scalar and the remainder
# became a new sequence entry — an arbitrary argument of the explorer
# container. The arguments are quoted now and the characters are refused where
# they are written, which are two halves of one fix: quoting stops the
# injection, and the guard stops the quoted remainder from silently matching no
# executor while the pod starts and serves.
refuses_citing "holds a control or line-separator character" "a newline in an allowlisted reference" "${explorer_base[@]}" --set-string 'explorer.fleet.executorRefs[0]=deploy
            - -conceal-withholding=false'
refuses_citing "holds a control or line-separator character" "a newline in an external reference" "${explorer_base[@]}" --set-string 'explorer.fleet.executorRefs[0]=deploy' --set-string 'explorer.fleet.externalExecutorRefs[0]=deploy
            - -conceal-withholding=false'
refuses_citing "holds a control or line-separator character" "a newline in a transmitting reference" "${explorer_base[@]}" --set-string 'explorer.fleet.executorRefs[0]=notify' --set-string 'explorer.fleet.externalEgressExecutorRefs[0]=notify
            - -conceal-withholding=false'
refuses_citing "holds a control or line-separator character" "a newline in the ask reference" "${explorer_base[@]}" --set-string 'explorer.fleet.executorRefs[0]=ask' --set explorer.chat.provider=ollama,explorer.chat.model=m,explorer.chat.baseURL=http://localhost:11434 --set-string 'explorer.fleet.askExecutorRef=ask
            - -conceal-withholding=false'
# A comma is the separator. One entry of "a,b" reaches the workspace as two
# references, so a single element silently becomes two bindings — and the
# collision guards compare the unsplit string, which is how an entry of "a,b"
# passes a check against an askExecutorRef of "b". The Go side refuses that at
# startup, so this is defence in depth rather than the only line.
# Not every line break is ASCII. go-yaml breaks lines on U+0085, U+2028 and
# U+2029 as well as on LF, and RE2's [[:cntrl:]] is ASCII-only -- so the guard
# these three cover used to pass them. The arguments are quoted, so none of
# them injects anything; what they do is reach the workspace inside the
# reference, match no registered executor, and bind nothing while the pod
# starts and serves normally. That is the silent failure the guard exists to
# refuse, which is why it is refused for the same reason a newline is.
refuses_citing "holds a control or line-separator character" "U+0085 in an allowlisted reference" "${explorer_base[@]}" --set-string "explorer.fleet.executorRefs[0]=deploy"$''"- -conceal-withholding=false"
refuses_citing "holds a control or line-separator character" "U+2028 in an allowlisted reference" "${explorer_base[@]}" --set-string "explorer.fleet.executorRefs[0]=deploy"$' '"- -conceal-withholding=false"
refuses_citing "holds a control or line-separator character" "U+2029 in an external reference" "${explorer_base[@]}" --set-string 'explorer.fleet.executorRefs[0]=deploy' --set-string "explorer.fleet.externalExecutorRefs[0]=deploy"$' '"- -conceal-withholding=false"
# And a character that is non-ASCII without being a line break must still
# render, or the widened class would be refusing ordinary values.
assert_renders "a non-ASCII reference still renders" '^ +- "-fleet-executor-refs=d.ploy"$' "${explorer_base[@]}" --set-string "explorer.fleet.executorRefs[0]=d"$'é'"ploy"

refuses_citing "holds a comma" "a comma inside one allowlisted entry" "${explorer_base[@]}" --set-string 'explorer.fleet.executorRefs[0]=deploy\,restart'
refuses_citing "holds a comma" "a comma inside one external entry" "${explorer_base[@]}" --set-string 'explorer.fleet.executorRefs[0]=deploy' --set-string 'explorer.fleet.externalExecutorRefs[0]=deploy\,restart'
# And the injection itself, asserted as an absence rather than as a refusal.
# This is the assertion that would fail if the guard above were deleted AND the
# arguments were unquoted again, which is the state the chart was in: the guard
# alone makes it unreachable, so without this the quoting could be reverted
# with every check still passing.
# The payload has to be in the value for this to mean anything: an earlier
# version of this check passed a reference of "deploy" and so asserted the
# absence of an argument nothing had tried to inject. It cannot use
# refuses_citing, because the point is the state with the guard gone — so it
# asserts the absence under a value the guard currently refuses, which is why
# it is written as an absence and not as a render.
assert_absent_or_refused "a newline cannot become a container argument" "^ +- -conceal-withholding=false$" "${explorer_base[@]}" --set-string 'explorer.fleet.executorRefs[0]=deploy
            - -conceal-withholding=false'
# Every explorer argument is a quoted scalar, which is what makes that true for
# values this file does not enumerate. Pinned positively so the property is
# asserted rather than assumed from the refusals above.
assert_renders "explorer arguments are quoted scalars" '^ +- "-allowed-host=shoal\.example\.test"$' "${explorer_base[@]}"

note "== the external-effect executor bindings =="
# explorer.fleet.externalExecutorRefs and externalEgressExecutorRefs are the
# only keys in this chart that raise an executor's effect ceiling to admit work
# whose consequences land outside Shoal. Everything else in the enforcement
# plane is fail-closed by omission; this is the one place an operator says "I
# accept that Shoal will not perform this and will not undo it".
#
# So the first thing asserted is the absence, not a refusal: nothing an
# operator leaves unset may produce that ceiling, and no positive assertion in
# this file can see that.
#
# The refusals after it each cover a values file that reads like the opt-in
# while not being one, or being one twice over. Every one is pinned to its own
# sentence rather than asserted as "did not render": a blank entry is also
# refused by the allow-list guard downstream of it, so that case would keep
# passing with the guard it is named for deleted — and the other three are
# cheap to pin once the helper is being used anyway.
assert_absent "no configuration produces an external ceiling by omission" "fleet-external" "${explorer_base[@]}"
assert_absent "nor does allowlisting a reference without naming it" "fleet-external" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={deploy}'
refuses_citing "is not in explorer.fleet.executorRefs" "an external reference that is not allowlisted" "${explorer_base[@]}" --set 'explorer.fleet.externalExecutorRefs={deploy}'
refuses_citing "is not in explorer.fleet.executorRefs" "a transmitting external reference that is not allowlisted" "${explorer_base[@]}" --set 'explorer.fleet.externalEgressExecutorRefs={notify}'
# Present but blank binds nothing, and the pod starts and serves anyway — so
# the operator learns about it when the gateway descriptor is refused.
refuses_citing "contains a blank entry" "a blank external reference only" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={deploy}' --set 'explorer.fleet.externalExecutorRefs={ }'
refuses_citing "contains a blank entry" "a blank transmitting reference only" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={notify}' --set 'explorer.fleet.externalEgressExecutorRefs={ }'
# The grounded-reasoning executor's floor equals its ceiling and excludes
# external mutation. One reference cannot carry both bindings, and the loser is
# decided by startup order.
refuses_citing "is also explorer.fleet.askExecutorRef" "the ask reference also bound for external mutation" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={ask}',explorer.fleet.askExecutorRef=ask,explorer.chat.provider=ollama,explorer.chat.model=m,explorer.chat.baseURL=http://localhost:11434 --set 'explorer.fleet.externalExecutorRefs={ask}'
refuses_citing "is also explorer.fleet.askExecutorRef" "the ask reference also bound for transmitting external work" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={ask}',explorer.fleet.askExecutorRef=ask,explorer.chat.provider=ollama,explorer.chat.model=m,explorer.chat.baseURL=http://localhost:11434 --set 'explorer.fleet.externalEgressExecutorRefs={ask}'
# Naming one reference in both lists is two different ceilings for it. The wider
# one winning would mean egress authority is acquired by listing it twice.
refuses_citing "already names" "one reference in both effect lists" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={deploy}' --set 'explorer.fleet.externalExecutorRefs={deploy}' --set 'explorer.fleet.externalEgressExecutorRefs={deploy}'

# And the configurations that must still render. The flag value is asserted,
# not just the absence of a refusal: a list joined into one argument is where a
# dropped or mistyped element disappears quietly, and the whole point of the
# pair of keys is that the two ceilings reach the process as different flags.
assert_renders "an external gateway reference is bound" "\-fleet-external-executor-refs=deploy\"$" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={deploy}' --set 'explorer.fleet.externalExecutorRefs={deploy}'
assert_renders "a transmitting external gateway is bound" "\-fleet-external-egress-executor-refs=notify\"$" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={notify}' --set 'explorer.fleet.externalEgressExecutorRefs={notify}'
assert_absent "a mutating reference does not acquire the egress flag" "fleet-external-egress-executor-refs" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={deploy}' --set 'explorer.fleet.externalExecutorRefs={deploy}'
assert_renders "several external references join into one argument" "\-fleet-external-executor-refs=deploy,restart\"$" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={deploy,restart}' --set 'explorer.fleet.externalExecutorRefs={deploy,restart}'
# Distinct references in the two lists are not a collision: a deployment can
# have one gateway that only mutates and another that also transmits.
assert_renders "both ceilings on separate references" "\-fleet-external-egress-executor-refs=notify\"$" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={deploy,notify}' --set 'explorer.fleet.externalExecutorRefs={deploy}' --set 'explorer.fleet.externalEgressExecutorRefs={notify}'
# The reasoning executor and a gateway coexist on separate references, which is
# the configuration #391 needs: Shoal answers from the corpus in process and
# dispatches the external work.
assert_renders "the ask executor beside a gateway" "\-fleet-external-executor-refs=deploy\"$" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={ask,deploy}',explorer.fleet.askExecutorRef=ask,explorer.chat.provider=ollama,explorer.chat.model=m,explorer.chat.baseURL=http://localhost:11434 --set 'explorer.fleet.externalExecutorRefs={deploy}'
assert_renders "and the ask binding survives it" "\-fleet-ask-executor-ref=ask\"$" "${explorer_base[@]}" --set 'explorer.fleet.executorRefs={ask,deploy}',explorer.fleet.askExecutorRef=ask,explorer.chat.provider=ollama,explorer.chat.model=m,explorer.chat.baseURL=http://localhost:11434 --set 'explorer.fleet.externalExecutorRefs={deploy}'

note "== a gateway value cannot carry a character that changes what the pod is given =="
# Every string the gateway pod is rendered from is a quoted scalar now, and a
# control character in one is refused where it is written. Before the quoting, a
# newline in identity.capability appended an argument of the operator's choosing
# — -allow-plaintext-admission=true among them.
assert_renders "gateway arguments are quoted scalars" '^ +- "-capability=chat\.completions"$' "${llm_gateway_base[@]}"
refuses_citing "holds a control character" "a newline in the capability" "${llm_gateway_base[@]}" --set-string 'llmGateway.identity.capability=chat
            - -allow-plaintext-admission=true'
refuses_citing "holds a control character" "a newline in the admission token variable" "${llm_gateway_base[@]}" --set-string 'llmGateway.admission.tokenEnv=TOKEN
            - name: OTHER'
refuses_citing "holds a control character" "a newline in an allowed host" "${llm_gateway_base[@]}" --set-string 'llmGateway.allowedHosts[0]=llm.example.test
            - -allow-plaintext-admission=true'
refuses_citing "holds a control character" "a newline in a model" "${llm_gateway_base[@]}" --set-string 'llmGateway.models[0]=gpt-4o
            - -allow-plaintext-admission=true'
refuses_citing "holds a control character" "a newline in the image tag" "${llm_gateway_base[@]}" --set-string 'llmGateway.image.tag=v1
          securityContext: {}'
refuses_citing "holds a comma" "a comma inside one allowed host" "${llm_gateway_base[@]}" --set-string 'llmGateway.allowedHosts[0]=a.example.test\,b.example.test'
refuses_citing "holds a comma" "a comma inside one model" "${llm_gateway_base[@]}" --set-string 'llmGateway.models[0]=gpt-4o\,claude-opus-5'
# Structural, so a newly added field or argument cannot be left out of it: in
# the rendered gateway Deployment no container argument is a bare scalar, and
# every value-shaped field is double-quoted unless it is one of the template's
# own constants.
gateway_scalars_quoted() {
  local description="$1"; shift
  local rendered stray
  if ! rendered=$(helm template shoal "$chart" "$@" -s templates/llm-gateway-deployment.yaml 2>&1); then
    fail "should render but was refused: $description"
    return
  fi
  stray=$(printf '%s\n' "$rendered" | grep -E '^ +- -' || true)
  stray+=$'\n'$(printf '%s\n' "$rendered" |
    grep -E '^ +(- )?(name|key|secretName|mountPath|audience|path|image|imagePullPolicy|serviceAccountName|type): [^"]' |
    grep -vE ': (shoal-llm-gateway|RollingUpdate|RuntimeDefault|http|health|admission-token|upstream-api-key|/readyz|/healthz)$' || true)
  stray=$(printf '%s\n' "$stray" | sed '/^$/d')
  if [ -n "$stray" ]; then
    fail "an unquoted value in the gateway Deployment: $description"
    printf '%s\n' "$stray" | head -3 | sed 's/^/      /'
  fi
}
gateway_scalars_quoted "env-form credentials" "${llm_gateway_base[@]}"

# Each class of rendered value is asserted quoted, so reverting the quoting of
# any one of them fails here rather than only behind the guard.
both_files=("${llm_gateway_base[@]}" "${valid_token_file[@]}" --set llmGateway.upstream.apiKeyFile=/etc/shoal/upstream/api-key --set llmGateway.upstream.apiKeyFileSource=secret)
assert_renders "the env variable name is quoted" '^ +- name: "SHOAL_ADMISSION_TOKEN"$' "${llm_gateway_base[@]}"
assert_renders "the Secret reference is quoted" '^ +name: "shoal-admission-token"$' "${llm_gateway_base[@]}"
assert_renders "the Secret key is quoted" '^ +key: "token"$' "${llm_gateway_base[@]}"
assert_renders "the image is quoted" '^ +image: "[^"]+"$' "${llm_gateway_base[@]}"
assert_renders "the pull policy is quoted" '^ +imagePullPolicy: "IfNotPresent"$' "${llm_gateway_base[@]}"
assert_renders "the Service type is quoted" '^ +type: "ClusterIP"$' "${llm_gateway_base[@]}"
assert_renders "the ServiceAccount is quoted" '^ +serviceAccountName: "shoal-llm-gateway"$' "${both_files[@]}"
assert_renders "a mount path is quoted" '^ +mountPath: "/var/run/secrets/shoal"$' "${both_files[@]}"
assert_renders "a projected token audience is quoted" '^ +audience: "shoal"$' "${both_files[@]}"
assert_renders "a projected token path is quoted" '^ +path: "token"$' "${both_files[@]}"
assert_renders "a credential volume's Secret is quoted" '^ +secretName: "shoal-upstream-key"$' "${both_files[@]}"
assert_renders "a credential volume's item key is quoted" '^ +- key: "api-key"$' "${both_files[@]}"
gateway_scalars_quoted "file-form credentials" "${both_files[@]}"
# The credential volumes are built by a helper the arguments do not pass
# through, so its inputs carry the payload too.
refuses_citing "holds a control character" "a newline in a credential volume's Secret key" "${both_files[@]}" --set-string 'llmGateway.upstream.credentialSecretKey=other-key
                path: decoy
              - key: api-key'
refuses_citing "holds a control character" "a newline in the projected token audience" "${both_files[@]}" --set-string 'llmGateway.admission.tokenAudience=shoal
              - secret:
                  name: injected-secret'
# YAML breaks lines on NEL and the Unicode line and paragraph separators as
# well, and [[:cntrl:]] is ASCII only.
refuses_citing "holds a control character" "a NEL in the capability" "${llm_gateway_base[@]}" --set-string "llmGateway.identity.capability=chat$(printf '\u0085')- -allow-plaintext-admission=true"
refuses_citing "holds a control character" "a line separator in an allowed host" "${llm_gateway_base[@]}" --set-string "llmGateway.allowedHosts[0]=llm.example.test$(printf '\u2028')x"
refuses_citing "holds a control character" "a paragraph separator in a model" "${llm_gateway_base[@]}" --set-string "llmGateway.models[0]=gpt-4o$(printf '\u2029')x"
# The contrast: a non-ASCII character that is not a line break still renders,
# so widening the class did not start refusing ordinary values.
assert_renders "a non-ASCII model name still renders" '\-model=modèle-é"$' "${llm_gateway_base[@]}" --set-string 'llmGateway.models[0]=modèle-é'
renders "a trailing newline the template trims is not a control character in the value" "${llm_gateway_base[@]}" --set-string 'llmGateway.identity.action=complete
'

note "== a name override cannot carry YAML of its own =="
# Both overrides are rendered unquoted into names and labels on every object.
# A line break in either used to end the scalar and inject the remainder:
# fullnameOverride "x<newline>  namespace: kube-system" moved the gateway's
# objects into kube-system.
refuses_citing "fullnameOverride" "a newline in fullnameOverride" "${llm_gateway_base[@]}" --set-string 'fullnameOverride=x
  namespace: kube-system'
refuses_citing "nameOverride" "a newline in nameOverride" -f "$chart/values.yaml" --set-string 'nameOverride=x
  namespace: kube-system'
refuses_citing "fullnameOverride" "a NEL in fullnameOverride" -f "$chart/values.yaml" --set-string "fullnameOverride=x$(printf '\u0085')y"
refuses_citing "fullnameOverride" "a full name that cannot be a Service name" -f "$chart/values.yaml" --set-string fullnameOverride=9shoal
refuses_citing "nameOverride" "an upper-case name override" -f "$chart/values.yaml" --set-string nameOverride=Shoal
refuses_citing "fullnameOverride" "a full name over 63 characters" -f "$chart/values.yaml" --set-string fullnameOverride=$(printf 'a%.0s' $(seq 1 64))
refuses_citing "nameOverride" "a non-string name override" -f "$chart/values.yaml" --set nameOverride=7
# The contrast: ordinary overrides still render, and an empty one is unset.
assert_renders "a valid fullnameOverride names the gateway" '^  name: platform-llm-gateway$' "${llm_gateway_base[@]}" --set-string fullnameOverride=platform
assert_renders "a valid nameOverride names the release's objects" 'app.kubernetes.io/name: "lake"' -f "$chart/values.yaml" --set-string nameOverride=lake
assert_renders "beside a full name, a label-valid name override still renders" 'app.kubernetes.io/name: "Shoal.App_v2"' -f "$chart/values.yaml" --set-string fullnameOverride=shoal --set-string nameOverride=Shoal.App_v2
refuses_citing "nameOverride" "beside a full name, a newline in the name override" -f "$chart/values.yaml" --set-string fullnameOverride=shoal --set-string 'nameOverride=x
  namespace: kube-system'
# A label must be a string: a name override that YAML would read as a number
# or boolean stays one once quoted.
assert_renders "a numeric-looking name override stays a string label" 'app.kubernetes.io/name: "1.5"' -f "$chart/values.yaml" --set-string fullnameOverride=shoal --set-string nameOverride=1.5
renders "an empty fullnameOverride is unset" -f "$chart/values.yaml" --set-string fullnameOverride=
renders "a 63-character fullnameOverride still renders" -f "$chart/values.yaml" --set-string fullnameOverride=$(printf 'a%.0s' $(seq 1 63))

note "== no chart value can carry a line break (#468) =="
# Most of the storage tier's values are rendered as bare YAML scalars, and so
# were several of the explorer's. A newline in any of them ended the scalar and
# the remainder became YAML of the operator's choosing: explorer.service.type
# "ClusterIP<newline>  externalIPs: [6.6.6.6]" gave the Service an external IP.
# The storage profiles must stay byte-identical, so their templates cannot be
# quoted; validate.yaml instead walks every value and refuses the character,
# naming the key.
#
# First the values the issue found injecting, each in a configuration that
# renders it, so the refusal is the one standing between the payload and a
# manifest. Each is pinned to the walk's own sentence for that key: several are
# also shape-checked, and a refusal from the shape check would keep this
# passing with the walk gone.
line_break_cases=(
  "storage image.pullPolicy IfNotPresent"
  "storage writeTier.storageSize 50Gi"
  "storage writeTier.dataDir /var/lib/shoal"
  "storage writeTier.quiesceDelay 10s"
  "storage objectStorage.credentialsSecretName shoal-object-storage-credentials"
  "write-tls writeTier.tls.secretName shoal-tls"
  "distributed image.pullPolicy IfNotPresent"
  "distributed objectStorage.credentialsSecretName shoal-object-storage-credentials"
  "read-tls readFleet.tls.secretName shoal-tls"
  "accumulo tserver.group default"
  "accumulo tserver.drainTimeout 30s"
  "accumulo tserver.walStorageSize 20Gi"
  "accumulo tserver.credentialsSecretName shoal-accumulo-write-credentials"
  "accumulo-tls tserver.tls.secretName shoal-tls"
  "accumulo compactor.group shoal_default"
  "accumulo compactor.hdfsNamenode hdfs://namenode:8020"
  "accumulo compactor.shutdownTimeout 30s"
  "accumulo compactor.stateStorageSize 5Gi"
  "accumulo compactor.credentialsSecretName shoal-accumulo-write-credentials"
  "accumulo-tls compactor.tls.secretName shoal-tls"
  "explorer explorer.storageSize 20Gi"
  "explorer explorer.storageClassName fast"
  "explorer explorer.stateDir /var/lib/shoal"
  "explorer explorer.image.pullPolicy IfNotPresent"
  "explorer explorer.service.type ClusterIP"
)
for line_break_case in "${line_break_cases[@]}"; do
  read -r profile key value <<<"$line_break_case"
  case "$profile" in
    storage) base=(-f "$chart/values.yaml") ;;
    write-tls) base=(-f "$chart/values.yaml" --set writeTier.tls.enabled=true) ;;
    distributed) base=(-f "$chart/values-distributed.yaml") ;;
    read-tls) base=(-f "$chart/values-distributed.yaml" --set readFleet.tls.enabled=true) ;;
    accumulo) base=(-f "$chart/values-accumulo.yaml") ;;
    accumulo-tls) base=(-f "$chart/values-accumulo.yaml" --set tserver.tls.enabled=true --set compactor.tls.enabled=true --set-string tserver.tls.secretName=shoal-tls --set-string compactor.tls.secretName=shoal-tls) ;;
    explorer) base=("${explorer_base[@]}") ;;
  esac
  # The valid value renders in that configuration, so the refusals below can
  # only be about the character.
  renders "$key=$value ($profile)" "${base[@]}" --set-string "$key=$value"
  refuses_citing "$key holds \"\\n\"" "a newline in $key ($profile)" "${base[@]}" --set-string "$key=$value
  injected: true"
  refuses_citing "$key holds \"\\u0085\"" "a NEL in $key ($profile)" "${base[@]}" --set-string "$key=${value}$(printf '\u0085')injected: true"
  refuses_citing "$key holds \"\\u2028\"" "a line separator in $key ($profile)" "${base[@]}" --set-string "$key=${value}$(printf '\u2028')injected: true"
done
# The injections themselves, asserted as absences under the payload, in the
# manner of the explorer arguments above: these are what the walk prevents, and
# they would fail if it and the explorer's quoting were both reverted.
assert_absent_or_refused "a newline in the explorer Service type cannot add externalIPs" "externalIPs" "${explorer_base[@]}" --set-string 'explorer.service.type=ClusterIP
  externalIPs: [6.6.6.6]'
assert_absent_or_refused "a newline in the write tier's storage request cannot name a storage class" "storageClassName: attacker" -f "$chart/values.yaml" --set-string 'writeTier.storageSize=1Gi
        storageClassName: attacker'
assert_absent_or_refused "a newline in a Secret name cannot add a volume" "name: injected" -f "$chart/values.yaml" --set-string 'objectStorage.credentialsSecretName=creds
        - name: injected'
# Every explorer scalar a value reaches is quoted, so the property holds for a
# field this file does not list. Structural, in the manner of the gateway's:
# no bare container argument, and every value-shaped field double-quoted unless
# it is one of the template's own constants. The configuration names a storage
# class and a credential, so the optional fields are rendered and inspected.
explorer_scalars_quoted() {
  local rendered stray
  if ! rendered=$(helm template shoal "$chart" "$@" -s templates/explorer-statefulset.yaml -s templates/explorer-service.yaml 2>&1); then
    fail "should render but was refused: the explorer's quoted scalars"
    return
  fi
  stray=$(printf '%s\n' "$rendered" | grep -E '^ +- -' || true)
  stray+=$'\n'$(printf '%s\n' "$rendered" |
    grep -E '^ +(- )?(name|key|secretName|mountPath|image|imagePullPolicy|type|storage|storageClassName): [^"]' |
    grep -vE ': (shoal-explore-web|shoal-explorer|shoal-explorer-headless|state|tmp|http|health|/tmp|RollingUpdate|RuntimeDefault)$' || true)
  stray=$(printf '%s\n' "$stray" | sed '/^$/d')
  if [ -n "$stray" ]; then
    fail "an unquoted value in the explorer's StatefulSet or Service"
    printf '%s\n' "$stray" | head -3 | sed 's/^/      /'
  fi
}
explorer_scalars_quoted "${explorer_base[@]}" --set explorer.storageClassName=fast --set explorer.chat.provider=openai-compatible,explorer.chat.model=m,explorer.chat.baseURL=https://api.example.test/v1,explorer.chat.credentialSecretName=chat --set explorer.embedding.provider=voyage,explorer.embedding.model=v3,explorer.embedding.credentialSecretName=voyage
# The contrasts. A non-ASCII character that is not a line break still renders,
# in a value rendered bare and in one the explorer now quotes.
assert_renders "a non-ASCII data directory still renders" '^ +mountPath: /var/lib/données$' -f "$chart/values.yaml" --set-string 'writeTier.dataDir=/var/lib/données'
assert_renders "a non-ASCII explorer state root still renders" '^ +mountPath: "/var/lib/données"$' "${explorer_base[@]}" --set-string 'explorer.stateDir=/var/lib/données'
# An annotation may span lines with LF: toYaml keeps that inside its own block
# scalar. (Its other line breaks, and any in a key, are refused; see the toYaml
# section below.)
assert_renders "a multi-line Service annotation still renders, as one value" '^    note: \|-$' "${explorer_base[@]}" --set-string 'explorer.service.annotations.note=first
second'
# The service-account key file is the one value that is multi-line by design.
# LF, CRLF and tab stay inside its block scalar; a bare CR or a NEL is a line
# break YAML sees and `indent` does not, so it would leave the block.
key_file="$(mktemp)"
printf 'objectStorage:\n  gcsKeyJson: "{\\r\\n\\t\\"type\\": \\"service_account\\"\\n}\\n"\n' > "$key_file"
assert_renders "a multi-line key file stays inside its block scalar" '^    	"type": "service_account"' -f "$chart/values.yaml" -f "$key_file"
printf 'objectStorage:\n  gcsKeyJson: "{}\\rkind: Injected"\n' > "$key_file"
refuses_citing 'objectStorage.gcsKeyJson holds "\r"' "a bare CR in the key file" -f "$chart/values.yaml" -f "$key_file"
printf 'objectStorage:\n  gcsKeyJson: "{}\\Nkind: Injected"\n' > "$key_file"
refuses_citing 'objectStorage.gcsKeyJson holds "\u0085"' "a NEL in the key file" -f "$chart/values.yaml" -f "$key_file"
rm -f "$key_file"
# The walk covers values the chart's values.yaml does not list, so a key added
# to a template before its default is covered from the start.
refuses_citing 'explorer.extra holds "\n"' "a newline in a key values.yaml does not list" -f "$chart/values.yaml" --set-string 'explorer.extra=x
y'
# And the refusal names the character, never the value: the walk reaches the
# accumulo password and the key file, and refusals are printed to CI logs.
password_refusal=$(helm template shoal "$chart" -f "$chart/values.yaml" --set-string 'readFleet.accumuloPassword=hunter2-secret
' 2>&1 || true)
if printf '%s' "$password_refusal" | grep -qF 'hunter2-secret'; then
  fail "a refused password is printed in the refusal"
fi
if ! printf '%s' "$password_refusal" | grep -qF 'readFleet.accumuloPassword holds'; then
  fail "a trailing newline in the accumulo password is not refused by the walk"
fi

# Then every value in values.yaml, enumerated rather than listed, so a key added
# there later is fuzzed without anyone adding a case for it. Each leaf (an empty
# list's first element, an empty map's new key) gets a newline payload and a NEL
# payload on the default values, where every optional component is off — so no
# component's own guard can be what refuses it — and the refusal has to be the
# walk's own sentence for that key (or, for mode, the mode guard's), since a
# shape check names the key too and must not stand in for the walk. A non-ASCII
# value that is not a line break must render for every string-valued leaf
# except those held to a shape.
if ! python3 - "$chart" <<'FUZZ'
import concurrent.futures, subprocess, sys, yaml

chart = sys.argv[1]
values = yaml.safe_load(open(chart + "/values.yaml"))
# Mirrors $lineBreakMultiline in validate.yaml: values under these may hold LF
# (and CRLF and tab), and nothing else of the class. They are still walked.
multiline = ("objectStorage.gcsKeyJson", "explorer.service.annotations",
             "llmGateway.service.annotations", "llmGateway.admission.tokenVolume",
             "llmGateway.upstream.apiKeyVolume")
# Held to a shape (or, for mode, an enumeration) on the default values, so a
# non-ASCII value there is refused for that reason and not this one.
shaped = {"mode", "image.pullPolicy", "objectStorage.credentialsSecretName",
          "writeTier.storageSize", "writeTier.quiesceDelay"}

leaves = []
def walk(path, value):
    if isinstance(value, dict):
        if not value:
            leaves.append((path + ".fuzz", None))
        for key, child in value.items():
            walk(f"{path}.{key}" if path else key, child)
    elif isinstance(value, list):
        if not value:
            leaves.append((path + "[0]", None))
        for index, child in enumerate(value):
            walk(f"{path}[{index}]", child)
    else:
        leaves.append((path, value))
walk("", values)

def helm(path, payload):
    return subprocess.run(
        ["helm", "template", "shoal", chart, "-f", chart + "/values.yaml",
         "--set-string", f"{path}={payload}"],
        capture_output=True, text=True)

# The walk's own sentence, so a shape check that also names the key cannot
# stand in for it. mode is the exception: its guard runs first and names it.
def expected(path):
    return "mode must be one of" if path == "mode" else f"{path} holds "

def check(leaf):
    path, default = leaf
    problems = []
    payloads = [("a NEL", "x\u0085injected: true")]
    if not any(path == m or path.startswith(m + ".") for m in multiline):
        payloads.append(("a newline", "x\n  injected: true"))
    for label, payload in payloads:
        result = helm(path, payload)
        if result.returncode == 0:
            problems.append(f"{label} in {path} renders")
        elif expected(path) not in result.stderr:
            problems.append(f"{label} in {path} is refused, but not by the walk: "
                            + result.stderr.strip().splitlines()[0][:160])
    if isinstance(default, str) and path not in shaped:
        result = helm(path, "x\u00e9y")
        if result.returncode != 0:
            problems.append(f"a non-ASCII value in {path} is refused: "
                            + result.stderr.strip().splitlines()[0][:160])
    return problems

with concurrent.futures.ThreadPoolExecutor(8) as pool:
    problems = [p for found in pool.map(check, leaves) for p in found]
for problem in problems:
    print("      " + problem)
if len(leaves) < 150:
    print(f"      only {len(leaves)} leaves enumerated: the walk over values.yaml is broken")
    raise SystemExit(1)
raise SystemExit(1 if problems else 0)
FUZZ
then
  fail "a value in values.yaml can carry a line break into the manifests, or a valid one is refused (see above)"
fi

note "== unquoted values are held to their shape =="
# The storage templates cannot be quoted, and a bare scalar is cut short by
# " #" or turned into a map by ": " without any line break; in the accumulo
# templates' flow mappings a comma ends it. Values with a fixed form are held
# to it, which refuses nothing the API server or the binary would accept.
refuses_citing "writeTier.storageSize must be a Kubernetes quantity" "a storage request that is not a quantity" -f "$chart/values.yaml" --set-string 'writeTier.storageSize=50Gi #'
refuses_citing "explorer.storageSize must be a Kubernetes quantity" "an explorer storage request that is not a quantity" "${explorer_base[@]}" --set-string 'explorer.storageSize=fast: 20Gi'
refuses_citing "tserver.walStorageSize must be a Kubernetes quantity" "a WAL storage request that is not a quantity" -f "$chart/values-accumulo.yaml" --set-string 'tserver.walStorageSize=20Gi}'
refuses_citing "compactor.stateStorageSize must be a Kubernetes quantity" "a compactor storage request that is not a quantity" -f "$chart/values-accumulo.yaml" --set-string 'compactor.stateStorageSize=5Gi}'
refuses_citing "writeTier.quiesceDelay must be a Go duration" "a bare number as the quiesce delay" -f "$chart/values.yaml" --set-string writeTier.quiesceDelay=10
refuses_citing "readFleet.drainTimeout must be a Go duration" "a read-fleet drain timeout with no unit" -f "$chart/values-distributed.yaml" --set-string readFleet.drainTimeout=30
refuses_citing "tserver.drainTimeout must be a Go duration" "a tserver drain timeout that is not a duration" -f "$chart/values-accumulo.yaml" --set-string 'tserver.drainTimeout=30 seconds'
refuses_citing "compactor.shutdownTimeout must be a Go duration" "a compactor timeout that is not a duration" -f "$chart/values-accumulo.yaml" --set-string 'compactor.shutdownTimeout=30s #'
refuses_citing "explorer.auth.oidc.clockSkew must be a Go duration" "a clock skew that is not a duration" "${explorer_base[@]}" --set-string explorer.auth.oidc.clockSkew=1minute
refuses_citing "explorer.disclosure.mosaic.window must be a Go duration" "a mosaic window that is not a duration" "${explorer_base[@]}" --set explorer.disclosure.mosaic.maxDomains=3 --set-string explorer.disclosure.mosaic.window=hourly
# The flow-mapping injection a line break is not needed for: a comma ends the
# Secret name inside {name: ..., key: ...} and the rest names another key.
refuses_citing "tserver.credentialsSecretName must be a Secret name" "a comma in the tserver's Secret name" -f "$chart/values-accumulo.yaml" --set-string 'tserver.credentialsSecretName=creds\, key: other'
refuses_citing "compactor.credentialsSecretName must be a Secret name" "a brace in the compactor's Secret name" -f "$chart/values-accumulo.yaml" --set-string 'compactor.credentialsSecretName=creds}'
refuses_citing "compactor.tls.secretName must be a Secret name" "a brace in the compactor's TLS Secret" -f "$chart/values-accumulo.yaml" --set compactor.tls.enabled=true --set-string 'compactor.tls.secretName=tls}}'
refuses_citing "objectStorage.credentialsSecretName must be a Secret name" "an upper-case Secret name" -f "$chart/values.yaml" --set-string objectStorage.credentialsSecretName=Shoal
refuses_citing "writeTier.tls.secretName must be a Secret name" "a write-tier TLS Secret that is not a name" -f "$chart/values.yaml" --set writeTier.tls.enabled=true --set-string 'writeTier.tls.secretName=tls #'
refuses_citing "readFleet.tls.secretName must be a Secret name" "a read-fleet TLS Secret that is not a name" -f "$chart/values-distributed.yaml" --set readFleet.tls.enabled=true --set-string 'readFleet.tls.secretName=tls: x'
refuses_citing "explorer.chat.credentialSecretName must be a Secret name" "a chat credential Secret that is not a name" "${explorer_base[@]}" --set explorer.chat.provider=openai-compatible,explorer.chat.model=m,explorer.chat.baseURL=https://api.example.test/v1 --set-string 'explorer.chat.credentialSecretName=Chat Key'
refuses_citing "image.pullPolicy must be one of Always, IfNotPresent or Never" "an unknown pull policy" -f "$chart/values.yaml" --set-string image.pullPolicy=Sometimes
refuses_citing "explorer.image.pullPolicy must be one of Always, IfNotPresent or Never" "an unknown explorer pull policy" "${explorer_base[@]}" --set-string explorer.image.pullPolicy=always
refuses_citing "explorer.service.type must be one of ClusterIP, NodePort, LoadBalancer or ExternalName" "an unknown Service type" "${explorer_base[@]}" --set-string explorer.service.type=Internal
# The contrasts: every form those fields legitimately take still renders.
renders "decimal, binary-SI and exponent quantities" -f "$chart/values-accumulo.yaml" --set-string writeTier.storageSize=1.5Gi,tserver.walStorageSize=500M,compactor.stateStorageSize=5e9
renders "an integer storage request" -f "$chart/values.yaml" --set writeTier.storageSize=53687091200
renders "compound and fractional durations" -f "$chart/values-accumulo.yaml" --set-string tserver.drainTimeout=1m30s,compactor.shutdownTimeout=1.5s
renders "zero and sub-second durations" -f "$chart/values-distributed.yaml" --set-string readFleet.quiesceDelay=0,readFleet.drainTimeout=250ms,readFleet.readinessInterval=1h
renders "dotted Secret names" -f "$chart/values-accumulo.yaml" --set-string tserver.credentialsSecretName=shoal.accumulo-write,compactor.credentialsSecretName=shoal.accumulo-write
renders "every pull policy" -f "$chart/values.yaml" --set-string image.pullPolicy=Never
renders "an empty pull policy is left to the API server's default" -f "$chart/values.yaml" --set-string image.pullPolicy=
renders "every Service type" "${explorer_base[@]}" --set-string explorer.service.type=LoadBalancer
# Checked only where rendered: a disabled component's leftovers do not stop an
# install that never uses them.
renders "a disabled component's shapes are not checked" -f "$chart/values.yaml" --set-string tserver.drainTimeout=later,compactor.stateStorageSize=big,explorer.service.type=Internal

note "== llm gateway guards refuse =="
# The gateway's failure mode is not a crash. It is required to fail closed, so
# nearly every misconfiguration below renders a pod that passes every probe and
# denies every call — which from outside is a total outage of whatever is
# configured to go through it. That is what these move to render time.
refuses_citing "llmProxy has been renamed to llmGateway" \
  "the pre-rename llmProxy key still enabled" -f "$chart/values.yaml" --set llmProxy.enabled=true
refuses_citing "llmProxy has been renamed to llmGateway" \
  "settings left under llmProxy beside an enabled llmGateway" "${llm_gateway_base[@]}" \
  --set llmProxy.upstream.requestTimeout=30s
refuses_citing "llmProxy has been renamed to llmGateway" \
  "a scalar llmProxy" -f "$chart/values.yaml" --set llmProxy=true
renders "a null llmProxy is ignored"     "${llm_gateway_base[@]}" --set llmProxy=null
renders "an empty llmProxy is ignored"   "${llm_gateway_base[@]}" --set-json 'llmProxy={}'
renders "a disabled llmProxy is ignored" "${llm_gateway_base[@]}" --set llmProxy.enabled=false
renders "an old values dump with llmProxy disabled is ignored" "${llm_gateway_base[@]}" \
  --set llmProxy.enabled=false,llmProxy.replicas=2,llmProxy.upstream.requestTimeout=30s
refuses_citing "llmProxy has been renamed to llmGateway" \
  "a string enabled under llmProxy is not a disabled one" -f "$chart/values.yaml" \
  --set-string llmProxy.enabled=false
refuses "no admission URL"              "${llm_gateway_base[@]}" --set llmGateway.admission.url=
refuses "blank admission URL"           "${llm_gateway_base[@]}" --set llmGateway.admission.url=" "
refuses "admission URL with no scheme"  "${llm_gateway_base[@]}" --set llmGateway.admission.url=shoal.example.test
refuses "plaintext remote admission"    "${llm_gateway_base[@]}" --set llmGateway.admission.url=http://shoal.example.test
refuses "no admission token env"        "${llm_gateway_base[@]}" --set llmGateway.admission.tokenEnv=
refuses "blank admission token env"     "${llm_gateway_base[@]}" --set llmGateway.admission.tokenEnv=" "
refuses "no admission credential"       "${llm_gateway_base[@]}" --set llmGateway.admission.credentialSecretName=
refuses "blank admission credential"    "${llm_gateway_base[@]}" --set llmGateway.admission.credentialSecretName=" "
refuses "no admission credential key"   "${llm_gateway_base[@]}" --set llmGateway.admission.credentialSecretKey=
# MaxActionClaimTTL is five minutes and the admission surface refuses a request
# outside it, so a longer lease is not a longer lease — it is every call denied.
refuses "lease above MaxActionClaimTTL" "${llm_gateway_base[@]}" --set llmGateway.admission.lease=10m
refuses "lease at 5m plus a second"     "${llm_gateway_base[@]}" --set llmGateway.admission.lease=5m1s
refuses "zero lease"                    "${llm_gateway_base[@]}" --set llmGateway.admission.lease=0s
# The duration parser, from both sides. Each of these is a value whose leading
# characters do parse — "30sec" scans as 30s, "1.5m" as 5m — so dropping the
# parser's refusal makes them render rather than trip a neighbouring guard.
# That is what makes them attribute to the parser and not to something else.
refuses "unparseable lease"             "${llm_gateway_base[@]}" --set llmGateway.admission.lease=30sec
refuses "fractional lease"              "${llm_gateway_base[@]}" --set llmGateway.admission.lease=1.5m
refuses "unparseable request timeout"   "${llm_gateway_base[@]}" --set llmGateway.upstream.requestTimeout=30sec
refuses "unitless lease"                "${llm_gateway_base[@]}" --set llmGateway.admission.lease=30
refuses "no upstream base URL"          "${llm_gateway_base[@]}" --set llmGateway.upstream.baseURL=
refuses "blank upstream base URL"       "${llm_gateway_base[@]}" --set llmGateway.upstream.baseURL=" "
refuses "upstream URL with no scheme"   "${llm_gateway_base[@]}" --set llmGateway.upstream.baseURL=api.example.test/v1
refuses "no upstream api key env"       "${llm_gateway_base[@]}" --set llmGateway.upstream.apiKeyEnv=
refuses "remote upstream, no credential" "${llm_gateway_base[@]}" --set llmGateway.upstream.credentialSecretName=
refuses "upstream credential, no key"   "${llm_gateway_base[@]}" --set llmGateway.upstream.credentialSecretKey=
# A call that outlives its lease is performed under a token that can no longer
# be reported against: the admission is abandoned, not resolved.
refuses "timeout outlives the lease"    "${llm_gateway_base[@]}" --set llmGateway.upstream.requestTimeout=90s
refuses "unbounded upstream request"    "${llm_gateway_base[@]}" --set llmGateway.upstream.requestTimeout=0s
refuses "no allowed hosts"              "${llm_gateway_base[@]}" --set llmGateway.allowedHosts=null
refuses "blank allowed host only"       "${llm_gateway_base[@]}" --set 'llmGateway.allowedHosts={ }'
refuses "no agent id"                   "${llm_gateway_base[@]}" --set llmGateway.identity.agentID=
refuses "blank agent id"                "${llm_gateway_base[@]}" --set llmGateway.identity.agentID=" "
refuses "no capability"                 "${llm_gateway_base[@]}" --set llmGateway.identity.capability=
refuses "no action"                     "${llm_gateway_base[@]}" --set llmGateway.identity.action=
refuses "no source id"                  "${llm_gateway_base[@]}" --set llmGateway.identity.sourceID=
refuses "no policy id"                  "${llm_gateway_base[@]}" --set llmGateway.identity.policyID=
refuses "zero agent generation"         "${llm_gateway_base[@]}" --set llmGateway.identity.agentGeneration=0
refuses "negative agent generation"     "${llm_gateway_base[@]}" --set llmGateway.identity.agentGeneration=-1
refuses "source id is not base64url"    "${llm_gateway_base[@]}" --set 'llmGateway.identity.sourceID=source/one'
refuses "padded policy id"              "${llm_gateway_base[@]}" --set 'llmGateway.identity.policyID=cG9saWN5=='
refuses "truncated source id"           "${llm_gateway_base[@]}" --set llmGateway.identity.sourceID=c291cmNlZ
refuses "placeholder admission URL"     "${llm_gateway_base[@]}" --set llmGateway.admission.url=https://REPLACE_ME/
refuses "placeholder allowed host"      "${llm_gateway_base[@]}" --set 'llmGateway.allowedHosts={REPLACE_ME.example.test}'
# A placeholder identity is the dangerous case: the gateway comes up, passes every
# probe, and is refused by the plane on every request while the chart reports
# success.
refuses "placeholder capability"        "${llm_gateway_base[@]}" --set llmGateway.identity.capability=REPLACE_ME
refuses "placeholder upstream URL"      "${llm_gateway_base[@]}" --set llmGateway.upstream.baseURL=https://REPLACE_ME/v1
refuses "health port collides"          "${llm_gateway_base[@]}" --set llmGateway.healthPort=8100
refuses "privileged listen port"        "${llm_gateway_base[@]}" --set llmGateway.containerPort=80
refuses "privileged health port"        "${llm_gateway_base[@]}" --set llmGateway.healthPort=81
refuses "port out of range"             "${llm_gateway_base[@]}" --set llmGateway.servicePort=70000
refuses "negative replicas"             "${llm_gateway_base[@]}" --set llmGateway.replicas=-1
refuses "fractional replicas"           "${llm_gateway_base[@]}" --set llmGateway.replicas=1.5
refuses "null replicas"                 "${llm_gateway_base[@]}" --set llmGateway.replicas=null
# The mirror image of the explorer's rule. A budget that refuses every eviction
# protects a singleton; this is not one, so it only wedges drains.
refuses "singleton disruption budget"   "${llm_gateway_base[@]}" --set llmGateway.podDisruptionBudget.maxUnavailable=0
# A key written with nothing after it is nil, not "". `toString nil` is the
# string "<nil>", which is non-blank — so a required-value check that does not
# normalize first reads an absent value as configured and renders the pod with
# an empty flag. These were observed rendering before `default ""` went in
# ahead of every `trim`, which is the whole reason they are pinned here.
refuses "null admission URL"            "${llm_gateway_base[@]}" --set llmGateway.admission.url=null
refuses "null admission token env"      "${llm_gateway_base[@]}" --set llmGateway.admission.tokenEnv=null
refuses "null admission credential"     "${llm_gateway_base[@]}" --set llmGateway.admission.credentialSecretName=null
refuses "null lease"                    "${llm_gateway_base[@]}" --set llmGateway.admission.lease=null
refuses "null upstream base URL"        "${llm_gateway_base[@]}" --set llmGateway.upstream.baseURL=null
refuses "null upstream api key env"     "${llm_gateway_base[@]}" --set llmGateway.upstream.apiKeyEnv=null
refuses "null request timeout"          "${llm_gateway_base[@]}" --set llmGateway.upstream.requestTimeout=null
refuses "null agent id"                 "${llm_gateway_base[@]}" --set llmGateway.identity.agentID=null
refuses "null capability"               "${llm_gateway_base[@]}" --set llmGateway.identity.capability=null
refuses "null action"                   "${llm_gateway_base[@]}" --set llmGateway.identity.action=null
refuses "null source id"                "${llm_gateway_base[@]}" --set llmGateway.identity.sourceID=null
refuses "null agent generation"         "${llm_gateway_base[@]}" --set llmGateway.identity.agentGeneration=null

note "== llm gateway valid configurations still render =="
# The gateway is stateless and must not be a singleton: the explorer refuses
# replicas above 1 and the gateway must not, so scaling out is asserted to render
# rather than merely left unforbidden.
renders "scaled out to ten"                 "${llm_gateway_base[@]}" --set llmGateway.replicas=10
renders "scaled to zero"                    "${llm_gateway_base[@]}" --set llmGateway.replicas=0
renders "loopback upstream needs no credential" "${llm_gateway_base[@]}" --set llmGateway.upstream.credentialSecretName=,llmGateway.upstream.baseURL=http://localhost:11434/v1
renders "plaintext admission acknowledged"  "${llm_gateway_base[@]}" --set llmGateway.admission.url=http://shoal-explorer:8098,llmGateway.admission.allowPlaintext=true
renders "loopback admission needs no acknowledgement" "${llm_gateway_base[@]}" --set llmGateway.admission.url=http://127.0.0.1:8098
# The fleet's claim ceiling is the upper bound, and the report window is the
# lower one. A lease equal to the timeout is refused, not accepted: the binary
# refuses that pair at startup, so rendering it would produce a pod that never
# serves — which is the failure these guards exist to move to render time.
# The grace period moves with the lease, because the gateway drains for the lease:
# at the 5m ceiling the shipped 75s grace period is refused, and that refusal is
# asserted below rather than worked around here.
renders "lease at the ceiling with room to report" "${llm_gateway_base[@]}" --set llmGateway.admission.lease=5m,llmGateway.upstream.requestTimeout=4m,llmGateway.terminationGracePeriodSeconds=305
refuses "lease equal to the timeout"        "${llm_gateway_base[@]}" --set llmGateway.admission.lease=5m,llmGateway.upstream.requestTimeout=5m
refuses "lease inside the report window"    "${llm_gateway_base[@]}" --set llmGateway.admission.lease=35s,llmGateway.upstream.requestTimeout=31s
renders "compound durations"                "${llm_gateway_base[@]}" --set llmGateway.admission.lease=2m30s,llmGateway.upstream.requestTimeout=1m30s,llmGateway.terminationGracePeriodSeconds=155
refuses "timeout equal to the lease"        "${llm_gateway_base[@]}" --set llmGateway.upstream.requestTimeout=60s
renders "millisecond timeout"               "${llm_gateway_base[@]}" --set llmGateway.upstream.requestTimeout=500ms
renders "disruption budget disabled"        "${llm_gateway_base[@]}" --set llmGateway.podDisruptionBudget.enabled=false,llmGateway.podDisruptionBudget.maxUnavailable=0
renders "operator-supplied affinity"        "${llm_gateway_base[@]}" --set 'llmGateway.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].key=kubernetes.io/os' --set 'llmGateway.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].operator=In' --set 'llmGateway.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].values={linux}'
renders "several allowed hosts, trimmed"    "${llm_gateway_base[@]}" --set 'llmGateway.allowedHosts={ llm.example.test , llm-internal.example.test }'

note "== the grace period covers the drain =="
# The gateway drains for the lease on SIGTERM, because the lease is the bound on
# an admitted call's whole lifetime including its report. terminationGracePeriod
# is the kubelet's budget for that window, so a grace period inside it is a
# SIGKILL mid-drain: the calls killed are exactly the ones whose egress has
# already happened and whose grant has already been spent, and every rolling
# update produces them on a schedule while the pod terminates cleanly.
#
# The shipped values did this — 45 against a 60s lease — which is why the
# default moved and why both sides of the boundary are pinned here.
refuses "grace period below the lease"      "${llm_gateway_base[@]}" --set llmGateway.terminationGracePeriodSeconds=30
refuses "grace period equal to the lease"   "${llm_gateway_base[@]}" --set llmGateway.terminationGracePeriodSeconds=60
# One second apart, on either side of the required 5s margin. A guard that only
# refused a grace period *below* the lease would pass the first of these, and
# the pod would be killed while the drain still had five seconds of work.
refuses "grace period one second inside the margin" "${llm_gateway_base[@]}" --set llmGateway.terminationGracePeriodSeconds=64
renders "grace period exactly the margin above the lease" "${llm_gateway_base[@]}" --set llmGateway.terminationGracePeriodSeconds=65
refuses "the shipped grace period against a ceiling lease" "${llm_gateway_base[@]}" --set llmGateway.admission.lease=5m,llmGateway.upstream.requestTimeout=4m
renders "a ceiling lease with the grace period raised" "${llm_gateway_base[@]}" --set llmGateway.admission.lease=5m,llmGateway.upstream.requestTimeout=4m,llmGateway.terminationGracePeriodSeconds=305
refuses "zero grace period"                 "${llm_gateway_base[@]}" --set llmGateway.terminationGracePeriodSeconds=0
# Cited, because a value that is not a number casts to 0 and is then refused by
# the drain guard above as well.
refuses_citing "must be a whole number of seconds" "null grace period" "${llm_gateway_base[@]}" --set llmGateway.terminationGracePeriodSeconds=null
refuses_citing "must be a whole number of seconds" "negative grace period" "${llm_gateway_base[@]}" --set llmGateway.terminationGracePeriodSeconds=-1
# A float that is comfortably above the lease, via --set-json for the reason the
# token lifetime needs it: this is the only shape the whole-number guard alone
# catches, because it casts to 75 and clears the drain guard.
refuses_citing "must be a whole number of seconds" "fractional grace period" "${llm_gateway_base[@]}" --set-json 'llmGateway.terminationGracePeriodSeconds=75.5'

note "== the admission token file form =="
# The file form exists because a rotating credential is a file: the kubelet
# rewrites a projected ServiceAccount token in place and never updates an
# environment variable. Every refusal here is either a pod the binary rejects at
# startup, a pod spec the API server rejects, or — the common case and the
# dangerous one — a pod that comes up, passes every probe, and fails every
# admission request on a credential it cannot read or that nothing accepts.
renders "a projected ServiceAccount token"  "${token_file_base[@]}"
# The binary compares -admission-token-env against its own flag default rather
# than against emptiness, so the default being present is not a second choice.
# If it were, the file form would be unreachable without also blanking the env
# key, and this case is what proves the chart kept that property.
renders "the file form with tokenEnv at its default" "${token_file_base[@]}" --set llmGateway.admission.tokenEnv=SHOAL_ADMISSION_TOKEN
renders "the file form with tokenEnv blanked" "${token_file_base[@]}" --set llmGateway.admission.tokenEnv=
refuses "both token forms chosen explicitly" "${token_file_base[@]}" --set llmGateway.admission.tokenEnv=OTHER_VAR
# Blank and absent are the env form, not a broken file form — so these render
# against the base's Secret rather than being refused. Without them, reading
# `tokenFile` as configured-when-whitespace would be invisible.
renders "a blank token file is the env form" "${llm_gateway_base[@]}" --set 'llmGateway.admission.tokenFile= '
renders "a null token file is the env form"  "${llm_gateway_base[@]}" --set llmGateway.admission.tokenFile=null
refuses "a relative token file"             "${token_file_base[@]}" --set llmGateway.admission.tokenFile=secrets/token
refuses "a token file at the filesystem root" "${token_file_base[@]}" --set llmGateway.admission.tokenFile=/token
# Cited, not merely refused: an unrecognised source leaves the audience set with
# no projection to bind it to, so the audience guard refuses these too and a
# bare `refuses` would pass with the source guard deleted.
refuses_citing "tokenFileSource must be projected" "an unknown token file source" "${token_file_base[@]}" --set llmGateway.admission.tokenFileSource=serviceaccount
refuses_citing "tokenFileSource must be projected" "a null token file source" "${token_file_base[@]}" --set llmGateway.admission.tokenFileSource=null
refuses_citing "tokenFileSource must be projected" "a blank token file source" "${token_file_base[@]}" --set 'llmGateway.admission.tokenFileSource= '
# A Secret named where nothing reads it is the invisible credential the binary
# refuses two token flags over: the operator believes one thing is presented and
# another is, and the symptom names neither.
refuses "a projected token with an admission Secret" "${token_file_base[@]}" --set llmGateway.admission.credentialSecretName=shoal-admission-token
refuses "an operator volume with an admission Secret" "${token_file_base[@]}" --set llmGateway.admission.tokenFileSource=volume,llmGateway.admission.tokenAudience=,llmGateway.admission.credentialSecretName=shoal-admission-token --set 'llmGateway.admission.tokenVolume.csi.driver=csi.spiffe.io'
# Without an audience the projected token is issued for the cluster's own API
# server, which the explorer is not: a 401 on every admission request.
refuses "a projected token with no audience" "${token_file_base[@]}" --set llmGateway.admission.tokenAudience=
refuses "a projected token with a blank audience" "${token_file_base[@]}" --set 'llmGateway.admission.tokenAudience= '
refuses "a projected token with a placeholder audience" "${token_file_base[@]}" --set llmGateway.admission.tokenAudience=REPLACE_ME
refuses "a placeholder token file"          "${token_file_base[@]}" --set llmGateway.admission.tokenFile=/var/run/REPLACE_ME/token
# A projected token's subject is the pod's ServiceAccount. Left implicit it is
# the namespace's `default`, so authorizing the gateway authorizes every pod in
# the namespace.
refuses "a projected token with no ServiceAccount" "${token_file_base[@]}" --set llmGateway.serviceAccountName=
refuses "a projected token with a blank ServiceAccount" "${token_file_base[@]}" --set 'llmGateway.serviceAccountName= '
renders "default named explicitly as the ServiceAccount" "${token_file_base[@]}" --set llmGateway.serviceAccountName=default
# 600 is the API server's floor for a token projection. Below it the Deployment
# is accepted and no pod is ever created from it, which is a rollout that never
# completes and no pod log to read at all.
refuses "a token lifetime below the API server floor" "${token_file_base[@]}" --set llmGateway.admission.tokenExpirationSeconds=599
refuses "a zero token lifetime"             "${token_file_base[@]}" --set llmGateway.admission.tokenExpirationSeconds=0
refuses "a negative token lifetime"         "${token_file_base[@]}" --set llmGateway.admission.tokenExpirationSeconds=-1
# Cited for the reason above, from the other side: anything non-numeric casts to
# 0, so the floor guard refuses all of these as well.
refuses_citing "must be a whole number of seconds" "a null token lifetime" "${token_file_base[@]}" --set llmGateway.admission.tokenExpirationSeconds=null
refuses_citing "must be a whole number of seconds" "an unparseable token lifetime" "${token_file_base[@]}" --set llmGateway.admission.tokenExpirationSeconds=1h
# --set-json, because --set cannot express a float: it parses what it can as an
# integer and keeps the rest as a string, and a string casts to 0 and lands on
# the floor guard. A real fractional value is the one shape only the
# whole-number guard can catch — it casts to a perfectly acceptable 600 — and
# a values file can hold one, so the fixture has to be able to say it.
refuses_citing "must be a whole number of seconds" "a fractional token lifetime" "${token_file_base[@]}" --set-json 'llmGateway.admission.tokenExpirationSeconds=600.5'
renders "a token lifetime at the floor"     "${token_file_base[@]}" --set llmGateway.admission.tokenExpirationSeconds=600
renders "a day-long token lifetime"         "${token_file_base[@]}" --set llmGateway.admission.tokenExpirationSeconds=86400
# The file form with a loopback upstream is the one configuration where nothing
# at all goes into the environment. `env:` with no entries under it is YAML
# null, which the API server rejects as not a list, so the key has to be omitted
# rather than emptied — and this is the only case that renders it.
loopback_token_file=(
  "${token_file_base[@]}"
  --set llmGateway.upstream.credentialSecretName=
  --set llmGateway.upstream.baseURL=http://localhost:11434/v1
)
renders "a token file with nothing in the environment" "${loopback_token_file[@]}"

# The Secret-as-a-file source. Worth having rather than redundant with the env
# form: the kubelet updates a mounted Secret's contents in place and the gateway
# re-reads per request, so rotating the value takes effect without rolling the
# pod — which an environment variable cannot do at all.
secret_file=(
  "${llm_gateway_base[@]}"
  --set llmGateway.admission.tokenFile=/etc/shoal/admission/token
  --set llmGateway.admission.tokenFileSource=secret
)
renders "the admission Secret mounted as a file" "${secret_file[@]}"
refuses "a Secret file source with no Secret" "${secret_file[@]}" --set llmGateway.admission.credentialSecretName=
refuses "a Secret file source with no key"  "${secret_file[@]}" --set llmGateway.admission.credentialSecretKey=
# An audience names the verifier a projected token is minted for. With this
# source nothing issues a token, so a value here records a binding that exists
# nowhere in the deployment.
refuses "a Secret file source with an audience" "${secret_file[@]}" --set llmGateway.admission.tokenAudience=shoal
# The lifetime is only consulted for a projection, so a bad one here must not
# refuse: a guard that fired anyway would be refusing a configuration that works.
renders "a Secret file source ignores the token lifetime" "${secret_file[@]}" --set llmGateway.admission.tokenExpirationSeconds=1

# The operator-supplied volume: the seam for a CSI driver the chart does not
# model. It can check only the two things that make the volume unusable rather
# than merely unknown.
operator_volume=(
  "${llm_gateway_base[@]}"
  --set llmGateway.admission.credentialSecretName=
  --set llmGateway.admission.tokenFile=/var/run/spiffe/token
  --set llmGateway.admission.tokenFileSource=volume
)
renders "an operator-supplied CSI token volume" "${operator_volume[@]}" --set 'llmGateway.admission.tokenVolume.csi.driver=csi.spiffe.io' --set 'llmGateway.admission.tokenVolume.csi.readOnly=true'
# Cited: with the guard gone the chart renders a volume whose source is an empty
# flow mapping, which Helm refuses while parsing its own output — so the case
# would pass for a reason that has nothing to do with the guard.
refuses_citing "tokenVolume is required" "a volume source with no volume" "${operator_volume[@]}"
refuses "a volume source that names itself" "${operator_volume[@]}" --set 'llmGateway.admission.tokenVolume.name=my-token' --set 'llmGateway.admission.tokenVolume.csi.driver=csi.spiffe.io'
refuses "a volume source with an audience"  "${operator_volume[@]}" --set llmGateway.admission.tokenAudience=shoal --set 'llmGateway.admission.tokenVolume.csi.driver=csi.spiffe.io'

# The env form's own guards still own the env form, which is what makes
# relaxing them for the file form safe. Each of these is already asserted above
# against llm_gateway_base; the point here is that the file form does not silently
# relax them for everyone.
renders "the env form still needs no volume" "${llm_gateway_base[@]}"
refuses "the env form still needs a Secret"  "${llm_gateway_base[@]}" --set llmGateway.admission.credentialSecretName=
refuses "the env form still needs a variable" "${llm_gateway_base[@]}" --set llmGateway.admission.tokenEnv=

note "== the transport acknowledgement, and the one hop that has none =="
# The acknowledgement is a flag now, because it was a values key that reached
# nothing: the binary refuses a remote http:// admission URL without it, so the
# documented mesh deployment rendered cleanly and produced CrashLoopBackOff.
renders "plaintext admission acknowledged, as a flag" "${llm_gateway_base[@]}" --set llmGateway.admission.url=http://shoal-explorer:8098,llmGateway.admission.allowPlaintext=true
# It is rendered verbatim into a boolean flag, so a YAML word that is not a
# boolean is a pod that exits on a flag error.
refuses "a non-boolean plaintext acknowledgement" "${llm_gateway_base[@]}" --set llmGateway.admission.url=http://shoal-explorer:8098 --set-string llmGateway.admission.allowPlaintext=yes
# And the upstream hop has no acknowledgement at all: it carries the prompt and
# the provider credential, so a mesh authenticating the hop to the explorer says
# nothing about it. allowPlaintext must not open this one.
refuses "a plaintext remote upstream" "${llm_gateway_base[@]}" --set llmGateway.upstream.baseURL=http://api.example.test/v1
refuses_citing "is plaintext to a remote provider" "a plaintext remote upstream even with the admission acknowledgement" "${llm_gateway_base[@]}" --set llmGateway.upstream.baseURL=http://api.example.test/v1,llmGateway.admission.allowPlaintext=true
renders "a loopback provider over http"     "${llm_gateway_base[@]}" --set llmGateway.upstream.baseURL=http://localhost:11434/v1,llmGateway.upstream.credentialSecretName=

note "== the declared model list =="
# Empty is valid: every model is then reported as "other", which is what keeps
# caller text out of the declaration without configuration.
renders "no models named"        "${llm_gateway_base[@]}"
renders "one model named"        "${llm_gateway_base[@]}" --set 'llmGateway.models={gpt-4o}'
renders "several models named"   "${llm_gateway_base[@]}" --set 'llmGateway.models={gpt-4o,claude-opus-5}'
# Blank entries are dropped rather than rendered, since the list is joined into
# one argument and a comma pair is an empty model name to the binary.
assert_renders "a blank model entry is dropped" "\-model=gpt-4o\"$" "${llm_gateway_base[@]}" --set 'llmGateway.models={gpt-4o, }'
assert_renders "models render as one joined argument" "\-model=gpt-4o,claude-opus-5\"$" "${llm_gateway_base[@]}" --set 'llmGateway.models={gpt-4o,claude-opus-5}'
assert_renders "no models renders an empty flag" "\-model=\"$" "${llm_gateway_base[@]}"

note "== rollout and disruption values are validated as written =="
# These reach the API server verbatim, and their only guard cast to int first —
# which is where the value escapes. int 1.5 is 1 and int -1 is -1, so a guard
# asking "did it cast to zero" approves both: the chart reports success and the
# API server rejects the object at install or upgrade time.
refuses_citing "non-negative whole number" "a fractional rollout surge" "${llm_gateway_base[@]}" --set-json llmGateway.strategy.maxSurge=1.5
refuses_citing "non-negative whole number" "a negative rollout surge" "${llm_gateway_base[@]}" --set llmGateway.strategy.maxSurge=-1
refuses_citing "non-negative whole number" "a fractional rollout maxUnavailable" "${llm_gateway_base[@]}" --set-json llmGateway.strategy.maxUnavailable=1.5
refuses_citing "non-negative whole number" "a negative rollout maxUnavailable" "${llm_gateway_base[@]}" --set llmGateway.strategy.maxUnavailable=-1
refuses_citing "non-negative whole number" "a fractional disruption budget" "${llm_gateway_base[@]}" --set-json llmGateway.podDisruptionBudget.maxUnavailable=1.5
refuses_citing "non-negative whole number" "a negative disruption budget" "${llm_gateway_base[@]}" --set llmGateway.podDisruptionBudget.maxUnavailable=-1
refuses_citing "non-negative whole number" "a nonsense rollout value" "${llm_gateway_base[@]}" --set-string llmGateway.strategy.maxSurge=lots
# Both at zero is accepted by the API server and then never progresses: nothing
# may be added and nothing taken down, so every upgrade hangs with no event
# saying why. A values file that zeroes the surge "to be careful" produces it.
refuses_citing "cannot both be 0" "a rollout that cannot start" "${llm_gateway_base[@]}" --set llmGateway.strategy.maxSurge=0,llmGateway.strategy.maxUnavailable=0
# Percentages are an IntOrString and must still render, as must the shipped
# pair — a guard that refuses the chart's own defaults is the first thing to
# get wrong here, and did: Helm's "default" treats 0 as empty.
renders "percentage rollout values"   "${llm_gateway_base[@]}" --set-string llmGateway.strategy.maxSurge=25%,llmGateway.strategy.maxUnavailable=0%
renders "a percentage disruption budget" "${llm_gateway_base[@]}" --set-string llmGateway.podDisruptionBudget.maxUnavailable=50%
renders "the shipped rollout pair"    "${llm_gateway_base[@]}" --set llmGateway.strategy.maxSurge=1,llmGateway.strategy.maxUnavailable=0
renders "a larger surge"              "${llm_gateway_base[@]}" --set llmGateway.strategy.maxSurge=3,llmGateway.strategy.maxUnavailable=1
# Zero is judged on what was written, so "0%" is zero and "50%" is not. The
# pre-existing guard cast to int first, and int of "50%" is 0 — so a valid
# percentage budget was refused as if it blocked every eviction, which is the
# same cast-before-validating flaw one line further on.
refuses_citing "must not be 0" "a disruption budget of 0%" "${llm_gateway_base[@]}" --set-string llmGateway.podDisruptionBudget.maxUnavailable=0%
refuses_citing "cannot both be 0" "a rollout zeroed as percentages" "${llm_gateway_base[@]}" --set-string llmGateway.strategy.maxSurge=0%,llmGateway.strategy.maxUnavailable=0%

note "== a credential path names a file, not a directory =="
# A trailing slash passes an absolute-path test and is not a file. The chart
# derives the mount from the directory and the projected item from the base, so
# "/var/run/secrets/shoal/" renders that exact flag while the credential lands
# at "/var/run/secrets/shoal/shoal" — the gateway then opens a directory on every
# request, and the pod passes both probes while denying every call.
refuses_citing "must name a file" "a token path with a trailing slash" "${token_file_base[@]}" --set 'llmGateway.admission.tokenFile=/var/run/secrets/shoal/'
# The matching upstream case lives with the upstream key fixture further down:
# these arrays are ordinary shell arrays, so using one above its definition
# expands to nothing and renders a chart with the gateway disabled — which is a
# check that cannot fail, in a script rather than in Go this time.
#
# The same path without the slash still renders, or the guard refuses the
# feature.
renders "a token path naming a file"  "${token_file_base[@]}" --set 'llmGateway.admission.tokenFile=/var/run/secrets/shoal/token'
assert_renders "and the flag matches the mount" "admission-token-file=/var/run/secrets/shoal/token\"$" "${token_file_base[@]}"

note "== no Kubernetes API credential in the prompt-processing pod =="
# This pod needs no API access: it speaks HTTP to the workspace and HTTP to the
# provider, and touches the API server nowhere. The automatic mount put a token
# for its ServiceAccount on the filesystem anyway, so a compromise of the one
# process in this chart that parses arbitrary caller input inherited whatever
# RBAC the account carries — the opposite of the reason the gateway is a separate
# process from the workspace at all.
assert_renders "the automatic token mount is off in the env form" "automountServiceAccountToken: false" "${llm_gateway_base[@]}"
assert_renders "the automatic token mount is off in the file form" "automountServiceAccountToken: false" "${token_file_base[@]}"
assert_renders "the automatic token mount is off with an operator volume" "automountServiceAccountToken: false" "${operator_volume[@]}" --set 'llmGateway.admission.tokenVolume.secret.secretName=shoal-admission-token' --set 'llmGateway.admission.tokenVolume.secret.defaultMode=288' --set 'llmGateway.admission.tokenVolume.secret.items[0].key=token' --set 'llmGateway.admission.tokenVolume.secret.items[0].path=token'
# The pairing that looks like it should conflict and does not: suppressing the
# automatic mount leaves an explicitly declared serviceAccountToken projection
# alone, and the kubelet still mints it. Asserted rather than assumed, because
# if it were wrong the whole projected-token form would be dead on arrival and
# every other check here would still pass.
assert_renders "an explicit projection survives it" "serviceAccountToken:" "${token_file_base[@]}"
assert_renders "and keeps its audience" "audience: \"shoal\"" "${token_file_base[@]}"

note "== both URLs are parsed, not prefix-matched =="
# These guards tested hasPrefix "http://" and hasPrefix "http://localhost",
# which disagreed with the binary in both directions. Every case below is one
# the prefix form got wrong, and each produced a pod the chart had approved.
#
# Scheme present, host absent. The prefix matched, the binary refused it for
# having no host, and the pod went into CrashLoopBackOff.
refuses_citing "with a host" "an admission URL with a scheme and no host" "${llm_gateway_base[@]}" --set llmGateway.admission.url=https://
refuses_citing "with a host" "an upstream URL with a scheme and no host" "${llm_gateway_base[@]}" --set llmGateway.upstream.baseURL=https://
# A host that merely begins with "localhost". The prefix form read this as
# loopback and exempted a remote plaintext hop from the rule entirely — the
# direction that matters, since it waved through what the rule exists to stop.
refuses_citing "is plaintext to a non-loopback decision plane" "an admission host that only starts with localhost" "${llm_gateway_base[@]}" --set llmGateway.admission.url=http://localhost.example:8098
refuses_citing "is plaintext to a remote provider" "an upstream host that only starts with localhost" "${llm_gateway_base[@]}" --set llmGateway.upstream.baseURL=http://localhost.example:11434/v1,llmGateway.upstream.credentialSecretName=
# A scheme-relative URL has a host and no scheme, and is not absolute.
refuses_citing "with a host" "a scheme-relative admission URL" "${llm_gateway_base[@]}" --set llmGateway.admission.url=//shoal-explorer:8098
refuses_citing "with a host" "a non-HTTP scheme" "${llm_gateway_base[@]}" --set llmGateway.admission.url=ftp://shoal-explorer:8098
# And the loopback forms the binary accepts must all still render, or parsing
# has traded one disagreement for another. isLoopback there is EqualFold on
# "localhost" plus net.IP.IsLoopback, so the whole 127.0.0.0/8 block counts and
# case does not.
renders "a loopback admission URL in upper case" "${llm_gateway_base[@]}" --set llmGateway.admission.url=HTTP://LOCALHOST:8098
renders "an admission URL elsewhere in 127.0.0.0/8" "${llm_gateway_base[@]}" --set llmGateway.admission.url=http://127.5.5.5:8098
renders "an IPv6 loopback admission URL" "${llm_gateway_base[@]}" --set 'llmGateway.admission.url=http://[::1]:8098'
renders "an IPv6 loopback upstream" "${llm_gateway_base[@]}" --set 'llmGateway.upstream.baseURL=http://[::1]:11434/v1' --set llmGateway.upstream.credentialSecretName=
renders "an https URL with a port and a path" "${llm_gateway_base[@]}" --set llmGateway.admission.url=https://shoal.example.test:8443/base
# Loopback is matched as an address, not as a "127." prefix. The prefix form
# classified the DNS name 127.example.com as loopback and exempted it from the
# plaintext rule, which the binary then refuses at startup — the same
# chart-approves-what-the-binary-refuses failure the parsing was meant to end.
refuses_citing "is plaintext to a non-loopback decision plane" "a DNS name beginning with 127." "${llm_gateway_base[@]}" --set llmGateway.admission.url=http://127.example.com:8098
refuses_citing "is plaintext to a remote provider" "an upstream DNS name beginning with 127." "${llm_gateway_base[@]}" --set llmGateway.upstream.baseURL=http://127.example.com:11434/v1,llmGateway.upstream.credentialSecretName=
# Each octet is bounded, because net.ParseIP refuses this and a loose \d{1,3}
# would approve it.
refuses_citing "is plaintext to a non-loopback decision plane" "an octet above 255" "${llm_gateway_base[@]}" --set llmGateway.admission.url=http://127.0.0.256:8098
# Go refuses the short form too, so a full dotted quad is required.
refuses_citing "is plaintext to a non-loopback decision plane" "a short-form loopback address" "${llm_gateway_base[@]}" --set llmGateway.admission.url=http://127.1:8098
# And the whole block still counts, as net.IP.IsLoopback has it.
renders "the lowest address in 127.0.0.0/8" "${llm_gateway_base[@]}" --set llmGateway.admission.url=http://127.0.0.0:8098
renders "the expanded IPv6 loopback spelling" "${llm_gateway_base[@]}" --set 'llmGateway.admission.url=http://[0:0:0:0:0:0:0:1]:8098'

note "== values rendered verbatim are validated as written =="
# Each of these is written into an argument or a port declaration unchanged, so
# a guard that converts before testing passes a value the flag parser or the API
# server then rejects. int64 of 1.5 is a positive 1; int of 8100.5 is 8100.
refuses "a fractional agent generation"     "${llm_gateway_base[@]}" --set llmGateway.identity.agentGeneration=1.5
# --set-string, because --set normalises 010 to 10 and the fixture would then be
# unable to express the condition it is named for. Written as a string it
# renders as 010, which Go parses as octal 8 — a generation nothing registered.
refuses "an agent generation with a leading zero" "${llm_gateway_base[@]}" --set-string llmGateway.identity.agentGeneration=010
refuses "an agent generation past the int64 maximum" "${llm_gateway_base[@]}" --set llmGateway.identity.agentGeneration=9223372036854775808
refuses "a twenty-digit agent generation"   "${llm_gateway_base[@]}" --set llmGateway.identity.agentGeneration=99999999999999999999
renders "the largest int64 agent generation" "${llm_gateway_base[@]}" --set llmGateway.identity.agentGeneration=9223372036854775807
# Cited: a non-numeric port casts to 0 and the range guard refuses it too, so
# only a float can attribute to the whole-number guard — and only --set-json can
# express one.
refuses_citing "llmGateway.containerPort must be a whole number" "a fractional container port" "${llm_gateway_base[@]}" --set-json 'llmGateway.containerPort=8100.5'
refuses_citing "llmGateway.servicePort must be a whole number" "a fractional service port" "${llm_gateway_base[@]}" --set-json 'llmGateway.servicePort=8100.5'

note "== the agent id is an encoding, not a name =="
# The workspace decodes this field and the binary refuses an undecodable value
# at startup. The guard reaches encoding mistakes and not the class: a readable
# name that happens to decode is indistinguishable from a registered ID here.
refuses "an agent id that is not base64url" "${llm_gateway_base[@]}" --set 'llmGateway.identity.agentID=governed/gateway'
refuses "a padded agent id"                 "${llm_gateway_base[@]}" --set 'llmGateway.identity.agentID=Z292ZXJuZWQtcHJveHk='
refuses "a truncated agent id"              "${llm_gateway_base[@]}" --set llmGateway.identity.agentID=Z292ZXJuZWQtcHJveHkBC
# The documented limit, pinned so nobody later claims the guard catches it: a
# display name of exactly the right shape decodes to garbage and renders.
renders "a readable agent id that happens to decode" "${llm_gateway_base[@]}" --set llmGateway.identity.agentID=gateway

note "== the upstream credential file form =="
# The same reasoning as the admission token, applied to the provider key: an
# environment variable cannot be rotated under a running pod, because a
# container's environment is fixed after start. A Secret mounted as a volume is
# updated in place and the per-request read picks it up.
upstream_key_file=(
  "${llm_gateway_base[@]}"
  --set llmGateway.upstream.apiKeyFile=/etc/shoal/upstream/api-key
  --set llmGateway.upstream.apiKeyFileSource=secret
)
renders "the provider key mounted as a file" "${upstream_key_file[@]}"
renders "the upstream file form with apiKeyEnv at its default" "${upstream_key_file[@]}" --set llmGateway.upstream.apiKeyEnv=SHOAL_UPSTREAM_API_KEY
# Blanking the variable is the other way to say "use the file", and it has to
# render: the binary only refuses a *changed* variable name beside a file.
renders "the upstream file form with apiKeyEnv blanked" "${upstream_key_file[@]}" --set llmGateway.upstream.apiKeyEnv=
refuses "both upstream key forms chosen explicitly" "${upstream_key_file[@]}" --set llmGateway.upstream.apiKeyEnv=OTHER_VAR
# A trailing slash is the token path's problem too; see that section. The chart
# derives the mount from the directory and the item from the base, so the flag
# would name a directory while the credential landed inside it.
refuses_citing "must name a file" "a key path with a trailing slash" "${upstream_key_file[@]}" --set 'llmGateway.upstream.apiKeyFile=/var/run/secrets/upstream/'
assert_renders "the key flag matches its mount" "upstream-api-key-file=/etc/shoal/upstream/api-key\"$" "${upstream_key_file[@]}"
refuses "a relative upstream key file"      "${upstream_key_file[@]}" --set llmGateway.upstream.apiKeyFile=upstream/api-key
refuses "an upstream key file at the filesystem root" "${upstream_key_file[@]}" --set llmGateway.upstream.apiKeyFile=/api-key
# Cited: with the source guard gone the chart renders an empty volume source,
# which Helm refuses while parsing its own output.
refuses_citing "apiKeyFileSource must be secret or volume" "an unknown upstream key file source" "${upstream_key_file[@]}" --set llmGateway.upstream.apiKeyFileSource=projected
# Against a loopback provider, so the remote-credential rule above cannot be
# what refuses it: the Secret source with no Secret is its own failure.
refuses "a Secret source for the provider key with no Secret" "${upstream_key_file[@]}" --set llmGateway.upstream.baseURL=http://localhost:11434/v1,llmGateway.upstream.credentialSecretName=
refuses "a Secret source for the provider key with no key" "${upstream_key_file[@]}" --set llmGateway.upstream.credentialSecretKey=
upstream_key_volume=(
  "${llm_gateway_base[@]}"
  --set llmGateway.upstream.credentialSecretName=
  --set llmGateway.upstream.apiKeyFile=/etc/shoal/upstream/api-key
  --set llmGateway.upstream.apiKeyFileSource=volume
)
# The relaxation that makes the volume source worth having: a remote provider
# with no Secret anywhere must render, or a CSI-delivered credential is
# unexpressible and the guard is a guard against the feature.
renders "a remote provider whose key comes from a CSI volume" "${upstream_key_volume[@]}" --set 'llmGateway.upstream.apiKeyVolume.csi.driver=secrets-store.csi.k8s.io' --set 'llmGateway.upstream.apiKeyVolume.csi.readOnly=true'
refuses_citing "apiKeyVolume is required" "an upstream volume source with no volume" "${upstream_key_volume[@]}"
refuses "an upstream volume source that names itself" "${upstream_key_volume[@]}" --set 'llmGateway.upstream.apiKeyVolume.name=my-key' --set 'llmGateway.upstream.apiKeyVolume.csi.driver=secrets-store.csi.k8s.io'
# Both credentials from files is the configuration the mount paths collide in.
both_files=(
  "${token_file_base[@]}"
  --set llmGateway.upstream.apiKeyFile=/etc/shoal/upstream/api-key
  --set llmGateway.upstream.apiKeyFileSource=secret
)
renders "both credentials from files in separate directories" "${both_files[@]}"
refuses "both credentials from files in one directory" "${both_files[@]}" --set llmGateway.upstream.apiKeyFile=/var/run/secrets/shoal/api-key
# Including the case where they are the same file, which is the same refusal
# and the one an operator reaches by copying the path.
refuses "both credentials from the same file" "${both_files[@]}" --set llmGateway.upstream.apiKeyFile=/var/run/secrets/shoal/token

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

note "== the llm gateway is wired end to end =="
# Same end-to-end rule as the explorer's check above, plus the second listener
# the gateway has: the one carrying traffic.
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
if ! helm template shoal "$chart" "${llm_gateway_base[@]}" \
  --set 'llmGateway.allowedHosts={ llm.example.test ,, llm-internal.example.test }' \
  --set 'llmGateway.admission.url= https://shoal.example.test ' \
  --set 'llmGateway.identity.capability= chat.completions ' | python3 -c '
import sys, yaml

problems = []
deployment = None
services = []
for document in yaml.safe_load_all(sys.stdin):
    if not document:
        continue
    component = document.get("metadata", {}).get("labels", {}).get(
        "app.kubernetes.io/component")
    if component != "llm-gateway":
        continue
    if document["kind"] == "Deployment":
        deployment = document
    elif document["kind"] == "Service":
        services.append(document)

if deployment is None:
    print("no llm-gateway Deployment was rendered")
    raise SystemExit(1)

# A Deployment, not a StatefulSet: the gateway is stateless, and this is the one
# structural difference from the explorer.
template = deployment["spec"]["template"]
container = template["spec"]["containers"][0]
declared = {port["name"]: port["containerPort"] for port in container["ports"]}

for name in ("http", "health"):
    if name not in declared:
        problems.append(f"the container declares no {name} port")

if "startupProbe" in container:
    problems.append(
        "a startupProbe only delays the first readiness check: the gateway opens "
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
    problems.append("no llm-gateway Service was rendered")
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
  fail "the llm gateway is not wired end to end (see above)"
fi

note "== each credential reaches the process in exactly one form =="
# No `refuses` case can see any of this. A guard can only refuse a values file;
# it cannot tell whether the pod that *did* render presents the credential the
# operator configured, and every failure below produces a pod that starts,
# passes both probes, and fails on the credential at request time:
#
#   both forms of one credential rendered at once, which the binary refuses at
#   startup, or neither rendered at all;
#   a file form rendered with the Secret still projected into the environment,
#   so which credential is in use depends on the binary precedence rather than
#   on the manifest;
#   a mount path that is not the directory of the path in the flag, or a
#   projected file name that is not its base name — either way the process
#   opens a path nothing put a credential at;
#   two credentials mounted at one path, which the API server refuses;
#   a file mode the container cannot read. This is the one that cost the most
#   thought: the kubelet writes projected and Secret volumes owned by root, the
#   container runs as uid 65532 with every capability dropped, and 0400 with no
#   fsGroup is a credential the only process that needs it cannot open. The
#   symptom is a 401 on every call, which looks exactly like a policy problem;
#   an `env:` key with nothing under it, which is YAML null and not a list;
#   -allow-plaintext-admission not reaching the process, which is the defect
#   that made the documented mesh deployment CrashLoopBackOff.
#
# Every form is checked, because "exactly one" is a claim about all of them.
credential_wiring='
import os, sys, yaml

problems = []
deployment = None
for document in yaml.safe_load_all(sys.stdin):
    if not document:
        continue
    labels = document.get("metadata", {}).get("labels", {})
    if document.get("kind") == "Deployment" and labels.get(
            "app.kubernetes.io/component") == "llm-gateway":
        deployment = document

if deployment is None:
    print("no llm-gateway Deployment was rendered")
    raise SystemExit(1)

pod = deployment["spec"]["template"]["spec"]
container = pod["containers"][0]
security = pod.get("securityContext", {})
fsgroup = security.get("fsGroup")
run_as_group = security.get("runAsGroup")
arguments = dict(
    argument.split("=", 1) for argument in container["args"] if "=" in argument)
if "env" in container and not container["env"]:
    problems.append(
        "env: is rendered with nothing under it, which is YAML null rather than "
        "an empty list, and the API server rejects the pod spec"
    )
environment = {entry["name"]: entry for entry in container.get("env") or []}
mounts = {mount["name"]: mount for mount in container.get("volumeMounts") or []}
volumes = {volume["name"]: volume for volume in pod.get("volumes") or []}

# The acknowledgement has to reach the process, or it acknowledges nothing: the
# binary refuses a remote plaintext admission URL without it, which is the
# CrashLoopBackOff a values key that reached nothing used to produce.
acknowledged = arguments.get("-allow-plaintext-admission")
if acknowledged not in ("true", "false"):
    problems.append(
        f"-allow-plaintext-admission is {acknowledged!r}: it is a boolean flag, "
        "and anything else is a flag error at startup"
    )
plane = arguments.get("-admission-url", "")
host = plane.split("//", 1)[-1].split("/", 1)[0].split(":")[0]
if plane.startswith("http://") and host not in ("127.0.0.1", "localhost", "[") and (
        acknowledged != "true"):
    problems.append(
        f"-admission-url is {plane} and -allow-plaintext-admission is "
        f"{acknowledged}: the binary refuses a remote plaintext decision plane "
        "without the acknowledgement, so this pod exits at startup"
    )

# volume name, file flag, variable flag, what the pod does without the
# credential, and whether the variable form must be projected from a Secret.
# The admission token must be: the workspace has no unauthenticated mode. The
# provider credential need not: a loopback model server takes none.
credentials = [
    ("admission-token", "-admission-token-file", "-admission-token-env",
     "denies every call", True),
    ("upstream-api-key", "-upstream-api-key-file", "-upstream-api-key-env",
     "fails every admitted call after the admission is spent", False),
]
expected_volumes = set()
expected_variables = set()
for name, file_flag, env_flag, consequence, must_project in credentials:
    from_file = arguments.get(file_flag)
    from_env = arguments.get(env_flag)
    if from_file and from_env:
        problems.append(
            f"{file_flag} and {env_flag} are both rendered: the binary refuses "
            "that pair at startup rather than ranking them, so this pod never "
            "serves"
        )
    if not from_file and not from_env:
        problems.append(f"neither {file_flag} nor {env_flag} is rendered")
    if not from_file:
        if from_env:
            expected_variables.add(from_env)
            if must_project and from_env not in environment:
                problems.append(
                    f"{from_env} is named by {env_flag} and projected from "
                    f"nothing, so the gateway {consequence}"
                )
        if name in volumes or name in mounts:
            problems.append(
                f"the {env_flag} form rendered a {name} volume nothing reads")
        continue

    expected_volumes.add(name)
    mount = mounts.get(name)
    if mount is None:
        problems.append(
            f"{file_flag}={from_file} is rendered with no volumeMount, so the "
            f"path does not exist in the container and the gateway {consequence}"
        )
    else:
        mount_path = mount["mountPath"]
        if mount_path != os.path.dirname(from_file):
            problems.append(
                f"{name} is mounted at {mount_path} but {file_flag} names "
                f"{from_file}, so the process opens a path nothing mounts"
            )
        if not mount.get("readOnly"):
            problems.append(f"the {name} mount is writable")

    volume = volumes.get(name)
    if volume is None:
        problems.append(f"the {name} mount references a volume that is not declared")
        continue
    source = {key: value for key, value in volume.items() if key != "name"}
    if len(source) != 1:
        problems.append(f"the {name} volume has {len(source)} sources, not one")
    kind = next(iter(source), None)
    body = source.get(kind, {})

    # The mode belongs to the chart for the sources it renders. An
    # operator-supplied volume is theirs, and the chart can see nothing inside
    # a CSI driver, so it is not checked here.
    mode = body.get("defaultMode")
    if kind in ("projected", "secret") and mode is None:
        problems.append(
            f"the {name} volume sets no defaultMode, so it takes 0644 and says "
            "nothing about who can read the credential"
        )
    elif mode is not None:
        if mode & 0o222:
            problems.append(f"the {name} file is writable (mode {mode:04o})")
        if not mode & 0o044:
            problems.append(
                f"the {name} file is mode {mode:04o}, which only its owner can "
                "read. The kubelet writes it owned by root and the container "
                "runs as uid 65532, so the one process that needs the "
                f"credential cannot open it: the gateway starts, passes every "
                f"probe, and {consequence}"
            )
        elif not mode & 0o004 and fsgroup != run_as_group:
            problems.append(
                f"the {name} file is group-readable (mode {mode:04o}) but "
                f"fsGroup is {fsgroup} and the container runs as group "
                f"{run_as_group}, so the group that can read it is not the "
                "group the process is in"
            )

    # The file name inside the volume has to be the base name in the flag, from
    # whichever side the volume builds it. Checking only the projected form left
    # the Secret form free to mount the credential under another name — the
    # process then opens a path the volume never creates.
    if kind == "secret":
        items = body.get("items") or []
        names = [item.get("path") for item in items]
        if names != [os.path.basename(from_file)]:
            problems.append(
                f"the {name} Secret is mounted as {names} but the flag names "
                f"{os.path.basename(from_file)!r}, so the process opens a path "
                "the volume does not create"
            )
        for item in items:
            if not str(item.get("key", "")).strip():
                problems.append(f"the {name} Secret item names no key")
    if kind == "projected":
        projections = [
            entry["serviceAccountToken"]
            for entry in body.get("sources", [])
            if "serviceAccountToken" in entry
        ]
        if len(projections) != 1:
            problems.append(
                f"{len(projections)} token projections in {name}, not one")
        for projection in projections:
            projected_name = projection.get("path")
            if projected_name != os.path.basename(from_file):
                problems.append(
                    f"the token is projected as {projected_name!r} but the flag "
                    f"names {os.path.basename(from_file)!r}"
                )
            if not str(projection.get("audience", "")).strip():
                problems.append(
                    "the token is projected with no audience, so it is issued "
                    "for the cluster API server and the explorer rejects every "
                    "request"
                )
            expiry = projection.get("expirationSeconds")
            if not isinstance(expiry, int) or expiry < 600:
                problems.append(
                    f"expirationSeconds is {expiry!r}: below the API server "
                    "floor of 600 the pod spec is invalid and no pod is ever "
                    "created"
                )
        if not str(pod.get("serviceAccountName", "")).strip():
            problems.append(
                "a projected token is rendered with no serviceAccountName, so "
                "its subject is the namespace default account, which every "
                "other pod in the namespace can also obtain a token for"
            )

# A Secret projected into a variable no flag names is the invisible credential
# the mutual exclusion exists to prevent: the operator sees two sources in the
# manifest and the binary picks one.
from_secrets = {
    name for name, entry in environment.items()
    if "secretKeyRef" in entry.get("valueFrom", {})
}
unread = sorted(from_secrets - expected_variables)
if unread:
    problems.append(
        f"a Secret is projected into {unread} that no flag names, so the "
        "credential the process uses is not the one the manifest shows"
    )
paths = [mount["mountPath"] for mount in container.get("volumeMounts") or []]
if len(paths) != len(set(paths)):
    problems.append(
        f"two volumes are mounted at one path ({paths}), which the API server "
        "refuses: the Deployment is created and no pod ever is"
    )
unused = sorted(set(volumes) - expected_volumes)
if unused:
    problems.append(f"volumes nothing reads: {unused}")
if not expected_volumes and "fsGroup" in security:
    problems.append("an fsGroup is set with no volume to own")
if not container["securityContext"].get("readOnlyRootFilesystem"):
    problems.append("a credential mount came at the cost of a writable root filesystem")

for problem in problems:
    print(problem)
raise SystemExit(1 if problems else 0)
'
wired() {
  local description="$1"; shift
  if ! helm template shoal "$chart" "$@" | python3 -c "$credential_wiring"; then
    fail "the credentials are not wired end to end: $description (see above)"
  fi
}
wired "a projected admission token"           "${token_file_base[@]}"
wired "the admission Secret as a file"        "${secret_file[@]}"
wired "a token file with nothing in the environment" "${loopback_token_file[@]}"
# The operator volume is checked against the same rules as the chart's own
# sources, with an explicit item and mode, because that is what makes the path
# the flag names deterministic. A volume source that leaves either out puts the
# credential wherever the Secret's keys happen to fall, which is the operator's
# business and not something the chart can see — so the fixture says it the way
# the guide tells an operator to.
wired "an operator-supplied admission volume" "${operator_volume[@]}" --set 'llmGateway.admission.tokenVolume.secret.secretName=shoal-admission-token' --set 'llmGateway.admission.tokenVolume.secret.defaultMode=288' --set 'llmGateway.admission.tokenVolume.secret.items[0].key=token' --set 'llmGateway.admission.tokenVolume.secret.items[0].path=token'
wired "the provider key as a file"            "${upstream_key_file[@]}"
wired "both credentials from files"           "${both_files[@]}"
wired "both credentials from the environment" "${llm_gateway_base[@]}"
wired "a loopback provider that needs no credential" "${llm_gateway_base[@]}" --set llmGateway.upstream.baseURL=http://localhost:11434/v1,llmGateway.upstream.credentialSecretName=
wired "an acknowledged plaintext decision plane" "${llm_gateway_base[@]}" --set llmGateway.admission.url=http://shoal-explorer:8098,llmGateway.admission.allowPlaintext=true

note "== a map rendered through toYaml cannot carry YAML of its own (#468) =="
# toYaml was assumed to make whatever it renders safe, and it does not, in two
# ways. A map key holding a newline is emitted as a block that nindent indents
# line by line, so the key ends early: nodeSelector key
# "k<newline>w<newline>    hostNetwork: true<newline>    junk: |" put
# hostNetwork: true in the pod spec. And a value holding LF is emitted as a
# `|-` block with U+2028 and U+2029 written raw inside it; YAML reads those as
# line breaks and nindent does not indent after them, so annotation
# "v<LF>w<U+2028>namespace: kube-system" moved the Service into kube-system.
#
# So every toYaml'd map, for both components, gets each line break as a key and
# as a value, in a configuration that renders that map. Keys are refused every
# character of the class. Values are refused it too, except that the
# annotations and the operator volumes, which legitimately span lines, keep LF,
# CRLF and tab: those stay inside the block, and a multi-line value with them
# must render and parse back to exactly itself with nothing injected. Any
# render, refused or not, is parsed and must contain no injected key.
join_args() { printf '%s\x1f' "$@"; }
if ! SHOAL_EXPLORER="$(join_args "${explorer_base[@]}")" \
  SHOAL_GATEWAY="$(join_args "${llm_gateway_base[@]}")" \
  SHOAL_TOKEN_VOLUME="$(join_args "${operator_volume[@]}" --set llmGateway.admission.tokenVolume.csi.driver=csi.spiffe.io)" \
  SHOAL_STORAGE="$(join_args -f "$chart/values.yaml")" \
  SHOAL_DISTRIBUTED="$(join_args -f "$chart/values-distributed.yaml")" \
  SHOAL_KEY_VOLUME="$(join_args "${upstream_key_volume[@]}" --set llmGateway.upstream.apiKeyVolume.csi.driver=secrets-store.csi.k8s.io)" \
  python3 - "$chart" <<'TOYAML'
import concurrent.futures, json, os, subprocess, sys, tempfile, yaml

chart = sys.argv[1]
bases = {name: [a for a in os.environ["SHOAL_" + name].split("\x1f") if a]
         for name in ("EXPLORER", "GATEWAY", "TOKEN_VOLUME", "KEY_VOLUME", "STORAGE", "DISTRIBUTED")}

# (base, path of the map, whether its values may span lines). A path ending in
# [0] is a one-element list holding the map.
targets = [
    ("EXPLORER", "explorer.service.annotations", True),
    ("EXPLORER", "explorer.nodeSelector", False),
    ("EXPLORER", "explorer.tolerations[0]", False),
    ("EXPLORER", "explorer.affinity", False),
    ("EXPLORER", "explorer.resources.limits", False),
    ("GATEWAY", "llmGateway.service.annotations", True),
    ("GATEWAY", "llmGateway.nodeSelector", False),
    ("GATEWAY", "llmGateway.tolerations[0]", False),
    ("GATEWAY", "llmGateway.affinity", False),
    ("GATEWAY", "llmGateway.resources.limits", False),
    ("STORAGE", "writeTier.resources.limits", False),
    ("DISTRIBUTED", "readFleet.resources.limits", False),
    ("TOKEN_VOLUME", "llmGateway.admission.tokenVolume.csi.volumeAttributes", True),
    ("KEY_VOLUME", "llmGateway.upstream.apiKeyVolume.csi.volumeAttributes", True),
]
breaks = {"LF": "\n", "CR": "\r", "NEL": "\u0085", "LS": " ", "PS": " "}
injected = {"hostNetwork", "namespace", "injected"}

def overlay(path, mapping):
    root = {}
    node = root
    parts = path.split(".")
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    last = parts[-1]
    if last.endswith("[0]"):
        node[last[:-3]] = [mapping]
    else:
        node[last] = mapping
    return root

def render(base, values):
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump(values, handle)
    try:
        return subprocess.run(["helm", "template", "shoal", chart, *bases[base], "-f", handle.name],
                              capture_output=True, text=True)
    finally:
        os.unlink(handle.name)

def injected_keys(text):
    found = set()
    def walk(node):
        if isinstance(node, dict):
            for key, child in node.items():
                if key in injected:
                    found.add(key)
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)
    for document in yaml.safe_load_all(text):
        walk(document)
    return found

def strings(text):
    found = set()
    def walk(node):
        if isinstance(node, dict):
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)
        elif isinstance(node, str):
            found.add(node)
    for document in yaml.safe_load_all(text):
        walk(document)
    return found

def cases():
    for base, path, multiline in targets:
        for name, char in breaks.items():
            key = f"k{char}hostNetwork: true"
            yield (base, path, f"{name} in a key", {key: "x"}, f"{path} has a key")
            value = f"v\nw{char}namespace: kube-system" if char != "\n" else "v\n  injected: true"
            allowed = multiline and char == "\n"
            yield (base, path, f"{name} in a value", {"a": value}, None if allowed else f"{path}.a holds")
        yield (base, path, "the issue's nodeSelector key",
               {"k\nw\n    hostNetwork: true\n    junk: |": "x"}, f"{path} has a key")
        if multiline:
            yield (base, path, "LF, CRLF and tab in a value",
                   {"a": "{\r\n\t\"line\": 1\n}\n"}, None)

def check(case):
    base, path, label, mapping, refusal = case
    result = render(base, overlay(path, mapping))
    problems = []
    if result.returncode == 0:
        found = injected_keys(result.stdout)
        if found:
            problems.append(f"{label} under {path} injects {sorted(found)}")
        if refusal:
            problems.append(f"{label} under {path} renders and must be refused")
        else:
            # The value must reach the object as exactly itself.
            if mapping["a"] not in strings(result.stdout):
                problems.append(f"{label} under {path} renders, but not as the value given")
    elif refusal is None:
        problems.append(f"{label} under {path} is refused and must render: "
                        + result.stderr.strip().splitlines()[0][:160])
    elif refusal not in result.stderr:
        problems.append(f"{label} under {path} is refused, but not by the walk: "
                        + result.stderr.strip().splitlines()[0][:160])
    return problems

all_cases = list(cases())
with concurrent.futures.ThreadPoolExecutor(8) as pool:
    problems = [p for found in pool.map(check, all_cases) for p in found]
for problem in problems:
    print("      " + problem)
raise SystemExit(1 if problems or len(all_cases) < 100 else 0)
TOYAML
then
  fail "a toYaml'd map can carry a line break into the manifests, or a valid multi-line value is refused (see above)"
fi
# The list above is only as good as its coverage of the templates, so every
# toYaml in them is found, resolved to the values path it renders, and given a
# line break in a key under that path — on the default values, where the walk
# alone stands between the payload and the manifest. A site this cannot
# resolve fails, so a new toYaml cannot be added without being covered here.
# (The templates' other loops over caller-supplied values are the gateway's
# model and allowed-host lists, which hold strings and are walked as values.)
#
# The scan reads template actions, not lines: each {{ ... }} is joined across
# the lines it spans before it is matched, so a toYaml whose argument is on the
# next line, or inside a multi-line include (dict ...), is still found. `.` is
# resolved through a stack of with/range/if/define ... end blocks, so a `with`
# that has already closed does not lend its value to a later `toYaml .`;
# variables ($explorer := .Values.explorer) are followed; and a helper's `.x`
# is resolved through every include of that helper that passes "x". Anything
# else it cannot resolve is a failure, not a skip.
toyaml_scanner=$(cat <<'SCAN'
import json, os, re, sys

def scan(chart):
    templates = os.path.join(chart, "templates")
    comment = re.compile(r"\{\{-?\s*/\*.*?\*/\s*-?\}\}", re.S)
    action = re.compile(r"\{\{-?(.*?)-?\}\}", re.S)
    sites, includes = [], []
    for name in sorted(os.listdir(templates)):
        text = open(os.path.join(templates, name)).read()
        # Comments are blanked to the same length so line numbers survive.
        text = comment.sub(lambda m: re.sub(r"[^\n]", " ", m.group(0)), text)
        stack, variables = [], {}
        def resolve(expression):
            expression = expression.strip("()")
            if expression.startswith(".Values."):
                return expression[len(".Values."):]
            match = re.match(r"\$(\w+)((?:\.\w+)*)$", expression)
            if match and variables.get(match.group(1)):
                return variables[match.group(1)] + match.group(2)
            if expression == ".":
                # The innermost block that rebinds dot; at the top level, or
                # under a range, dot is nothing a values path names.
                for kind, value in reversed(stack):
                    if kind == "with":
                        return value
                    if kind in ("range", "define"):
                        return None
                return None
            match = re.match(r"\.(\w+)$", expression)
            if match:
                for kind, value in reversed(stack):
                    if kind == "define":
                        return ("helper", value, match.group(1))
            return None
        for found in action.finditer(text):
            body = " ".join(found.group(1).split())
            line = text.count("\n", 0, found.start()) + 1
            site = f"{name}:{line}"
            head = body.split(" ", 1)[0] if body else ""
            if head in ("with", "range", "if", "define", "block"):
                argument = body.split(" ", 1)[1] if " " in body else ""
                if head == "with":
                    stack.append(("with", resolve(argument.split(" ")[0])))
                elif head == "define":
                    stack.append(("define", argument.strip('"')))
                else:
                    stack.append((head, None))
            elif head == "end":
                if stack:
                    stack.pop()
            assignment = re.match(r"\$(\w+) :?= (\S+)$", body)
            if assignment:
                variables[assignment.group(1)] = resolve(assignment.group(2))
            for call in re.finditer(r'include "([\w.]+)" \(dict (.*?)\)(?: \||$)', body):
                for key, value in re.findall(r'"(\w+)" (\S+)', call.group(2)):
                    includes.append((call.group(1), key, resolve(value), site))
            arguments = re.findall(r"toYaml (\S+)", body) + re.findall(r"(\S+) \| toYaml\b", body)
            for argument in arguments:
                sites.append((site, argument, resolve(argument)))
    resolved, unresolved = {}, []
    for site, argument, target in sites:
        if isinstance(target, tuple):
            _, helper, key = target
            paths = [path for name, k, path, _ in includes if name == helper and k == key]
            if not paths or None in paths:
                unresolved.append(f"{site}: toYaml {argument}")
                continue
        elif target is None:
            unresolved.append(f"{site}: toYaml {argument}")
            continue
        else:
            paths = [target]
        for path in paths:
            resolved.setdefault(path, []).append(site)
    return resolved, unresolved

if __name__ == "__main__":
    resolved, unresolved = scan(sys.argv[1])
    print(json.dumps({"resolved": resolved, "unresolved": unresolved}))
SCAN
)
# The scanner is tested before it is trusted: a copy of the chart gains a
# toYaml split across lines, one inside a multi-line include (dict ...), one
# under a `with` that is still open, and a `toYaml .` after a `with` has
# closed. The first three must resolve to their paths and the last must be
# reported as unresolvable rather than borrowing the closed block's value.
scanner_probe="$(mktemp -d)"
cp -R "$chart" "$scanner_probe/shoal"
cat > "$scanner_probe/shoal/templates/zz-scanner-probe.yaml" <<'PROBE'
{{- if false }}
metadata:
  labels:
    {{- toYaml
          .Values.zzSplit | nindent 4 }}
  {{- include "shoal.zzProbe" (dict
        "rendered" (toYaml .Values.zzInclude)
        "other" 1) }}
  {{- with .Values.zzOpen }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.zzClosed }}{{ end }}
  more:
    {{- toYaml . | nindent 4 }}
{{- end }}
PROBE
if ! python3 -c "$toyaml_scanner" "$scanner_probe/shoal" | python3 -c '
import json, sys
report = json.load(sys.stdin)
resolved, unresolved = report["resolved"], report["unresolved"]
problems = []
for path in ("zzSplit", "zzInclude", "zzOpen"):
    if not any(site.startswith("zz-scanner-probe.yaml:") for site in resolved.get(path, [])):
        problems.append(f"the scanner did not resolve the probe site for {path}")
if "zzClosed" in resolved:
    problems.append("the scanner resolved `toYaml .` to a with block that had already closed")
if not any(entry.startswith("zz-scanner-probe.yaml:15:") for entry in unresolved):
    problems.append("the scanner did not report `toYaml .` outside any with block as unresolvable: " + repr(unresolved))
for problem in problems:
    print("      " + problem)
raise SystemExit(1 if problems else 0)
'; then
  fail "the toYaml site scanner misses or misresolves a site (see above)"
fi
rm -rf "$scanner_probe"

# The scan's report goes through a file: the check below is read from stdin.
scanner_report="$(mktemp)"
python3 -c "$toyaml_scanner" "$chart" > "$scanner_report" || printf '{"resolved": {}, "unresolved": ["the scanner itself failed"]}' > "$scanner_report"
if ! python3 - "$chart" "$scanner_report" <<'SITES'
import json, os, subprocess, sys, tempfile, yaml

chart = sys.argv[1]
report = json.load(open(sys.argv[2]))
sources, problems = report["resolved"], []
for entry in report["unresolved"]:
    problems.append(f"{entry}: cannot tell which value this renders; teach the scanner, or the walk cannot be shown to cover it")
if len(sources) < 12:
    problems.append(f"only {len(sources)} toYaml sources found: the scan is broken")
defaults = yaml.safe_load(open(os.path.join(chart, "values.yaml")))

def default_at(path):
    node = defaults
    for part in path.split("."):
        node = node.get(part) if isinstance(node, dict) else None
    return node

for path, sites in sorted(sources.items()):
    if path.split(".")[0] in ("global", "llmProxy"):
        problems.append(f"{path} ({', '.join(sites)}) is rendered but not walked")
        continue
    key = "k\nw\n    hostNetwork: true\n    junk: |"
    parts = path.split(".")
    overlay = node = {}
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    if isinstance(default_at(path), list):
        node[parts[-1]] = [{key: "x"}]
        expected = f"{path}[0] has a key"
    else:
        node[parts[-1]] = {key: "x"}
        expected = f"{path} has a key"
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump(overlay, handle)
    result = subprocess.run(["helm", "template", "shoal", chart, "-f", os.path.join(chart, "values.yaml"),
                             "-f", handle.name], capture_output=True, text=True)
    os.unlink(handle.name)
    if result.returncode == 0 or expected not in result.stderr:
        problems.append(f"a line break in a key under {path} ({', '.join(sites)}) is not refused by the walk")

for problem in problems:
    print("      " + problem)
raise SystemExit(1 if problems else 0)
SITES
then
  fail "a toYaml site in the templates renders a values path the walk does not guard (see above)"
fi
rm -f "$scanner_report"

note "== the guide's worked example still installs =="
# The example in docs/llm-gateway-deploy.md is copied by operators verbatim, and a
# values file in prose is the first thing to rot when a key is renamed or a new
# refusal lands. So it is extracted from the document and rendered, rather than
# trusted.
#
# It is the projected-token example specifically, because that is the one with
# keys the chart refuses to render without — an audience, a ServiceAccount, no
# admission Secret — and the one where a stale document produces a pod that
# passes every probe and denies every call.
guide="$chart/../../../docs/llm-gateway-deploy.md"
if [ -f "$guide" ]; then
  example="$(mktemp)"
  python3 - "$guide" > "$example" <<'EXTRACT'
import re, sys

document = open(sys.argv[1]).read()
heading = "### The values file, complete"
if heading not in document:
    raise SystemExit("the guide no longer has a complete values example")
block = re.search(r"```yaml\n(.*?)```", document[document.index(heading):], re.S)
if not block:
    raise SystemExit("the example after that heading is not a yaml block")
sys.stdout.write(block.group(1))
EXTRACT
  if [ -s "$example" ]; then
    renders "the worked example from docs/llm-gateway-deploy.md" -f "$chart/values-llm-gateway.yaml" -f "$example"
    wired "the worked example from docs/llm-gateway-deploy.md" -f "$chart/values-llm-gateway.yaml" -f "$example"
  else
    fail "could not extract the worked example from docs/llm-gateway-deploy.md"
  fi
  rm -f "$example"
else
  fail "docs/llm-gateway-deploy.md is missing: the worked example cannot be checked"
fi

note "== every rendered name fits the 63-character limit =="
# Kubernetes rejects a name longer than 63 characters, and the explorer's
# headless Service name is the longest the chart derives. A long release name is
# the case that finds it: truncating the finished name is not enough, because
# the suffix is appended after the truncation. Helm caps a release name at 53,
# so this is the worst case an install can actually present.
#
# Both planes are rendered together so every name the chart derives — including
# the gateway's, which hangs off its own bounded stem — is measured in one pass.
long_release="shoal-production-authorized-plane-euw1-cluster-prime"
if ! helm template "$long_release" "$chart" "${explorer_base[@]}" "${valid_llm_gateway[@]}" | python3 -c '
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
