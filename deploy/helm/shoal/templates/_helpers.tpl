{{- define "shoal.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "shoal.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "shoal.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "shoal.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "shoal.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: shoal-platform
{{- end -}}

{{- define "shoal.writeTierEnabled" -}}
{{- if kindIs "bool" .Values.writeTier.enabled -}}
{{- .Values.writeTier.enabled -}}
{{- else -}}
{{- or (eq .Values.mode "single") (eq .Values.mode "distributed") -}}
{{- end -}}
{{- end -}}

{{- define "shoal.readFleetEnabled" -}}
{{- if kindIs "bool" .Values.readFleet.enabled -}}
{{- .Values.readFleet.enabled -}}
{{- else -}}
{{- or (eq .Values.mode "distributed") (eq .Values.mode "accumulo") -}}
{{- end -}}
{{- end -}}

{{- define "shoal.tserverEnabled" -}}
{{- if kindIs "bool" .Values.tserver.enabled -}}
{{- .Values.tserver.enabled -}}
{{- else -}}
{{- eq .Values.mode "accumulo" -}}
{{- end -}}
{{- end -}}

{{- define "shoal.compactorEnabled" -}}
{{- if kindIs "bool" .Values.compactor.enabled -}}
{{- .Values.compactor.enabled -}}
{{- else -}}
{{- eq .Values.mode "accumulo" -}}
{{- end -}}
{{- end -}}

{{- define "shoal.explorerEnabled" -}}
{{- .Values.explorer.enabled -}}
{{- end -}}

{{- /*
Explorer names are built from one bounded stem so that every name derived from
it fits Kubernetes' 63-character limit.

Truncating the finished name is not enough: the headless Service name is the
longest of them, and appending "-headless" to an already-63-character name
produces 72. The stem reserves room for the longest suffix instead, so a long
release name shortens the stem rather than overflowing the name.

  stem            45
  -explorer        9  -> 54
  -explorer-headless 18 -> 63
*/ -}}
{{- define "shoal.explorerStem" -}}
{{- include "shoal.fullname" . | trunc 45 | trimSuffix "-" -}}
{{- end -}}

{{- define "shoal.explorerName" -}}
{{- printf "%s-explorer" (include "shoal.explorerStem" .) -}}
{{- end -}}

{{- define "shoal.explorerHeadlessName" -}}
{{- printf "%s-explorer-headless" (include "shoal.explorerStem" .) -}}
{{- end -}}

{{- define "shoal.explorerSelectorLabels" -}}
app.kubernetes.io/name: shoal-explore-web
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: explorer
{{- end -}}

{{- define "shoal.llmProxyEnabled" -}}
{{- .Values.llmProxy.enabled -}}
{{- end -}}

{{- /*
The proxy's names come from a bounded stem for the same reason the explorer's
do: Kubernetes rejects a name over 63 characters, and truncating the finished
name does not help, because the suffix is appended after the truncation. The
stem reserves room for the longest suffix instead, so a long release name
shortens the stem rather than overflowing the name.

  stem         53
  -llm-proxy   10  -> 63
*/ -}}
{{- define "shoal.llmProxyStem" -}}
{{- include "shoal.fullname" . | trunc 53 | trimSuffix "-" -}}
{{- end -}}

{{- define "shoal.llmProxyName" -}}
{{- printf "%s-llm-proxy" (include "shoal.llmProxyStem" .) -}}
{{- end -}}

{{- /*
A selector distinct from the explorer's and from the storage tier's. The two
planes are separate processes deliberately, and a selector that could match
either would let the Service carrying arbitrary prompt traffic land on the pod
holding the policy store.
*/ -}}
{{- define "shoal.llmProxySelectorLabels" -}}
app.kubernetes.io/name: shoal-llm-proxy
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: llm-proxy
{{- end -}}

{{- /*
The proxy's host allow-list, normalized and comma-joined for -allowed-host.

One definition rather than two, because the guard in validate.yaml and the
argument in the Deployment have to agree about what an entry is. A blank entry
is not a configured authority — the gate drops it and then reports the setting
as missing — so if the guard trimmed and the argument did not, the chart would
pass its own check and then render `-allowed-host=` with a stray comma or a
space inside an authority that is matched exactly. An authority with a space in
it matches nothing, which is the 421 the guard exists to prevent, reached
through the one path the guard was not looking at.

Emits the empty string when nothing survives, which is what the guard tests.
*/ -}}
{{- define "shoal.llmProxyAllowedHosts" -}}
{{- $hosts := list -}}
{{- range (default (list) .Values.llmProxy.allowedHosts) -}}
{{- if trim (default "" .) -}}
{{- $hosts = append $hosts (trim .) -}}
{{- end -}}
{{- end -}}
{{- join "," $hosts -}}
{{- end -}}

{{- /*
One credential volume, for either of the proxy's two -file credentials.

The two break differently — an unreadable admission token denies every call,
an unreadable upstream key fails calls policy already allowed — but the
mechanism that delivers them is identical, and two copies of it would be two
places for the mode, the path and the source to drift apart.

Takes a dict: name, path, source, audience, expirationSeconds, secretName,
secretKey, volume.

The file name inside the volume is derived from the path the flag carries, so
the projection and the flag cannot name different files. The directory half of
that same path is the mountPath at the call site.

defaultMode is 0440 and never 0400, which is the mistake worth spelling out.
The kubelet writes projected and Secret volumes owned by root; the proxy
container runs as uid 65532 with every capability dropped. At 0400 the one
process that needs the credential cannot open it, and the failure is not a
crash — the pod starts, passes both probes, and fails on the credential at
every request. 0440 is readable exactly because the pod declares fsGroup 65532
alongside it; neither half works without the other.
*/ -}}
{{- define "shoal.llmProxyCredentialVolume" -}}
- name: {{ .name }}
  {{- if eq .source "projected" }}
  projected:
    defaultMode: 0440
    sources:
      - serviceAccountToken:
          path: {{ base .path }}
          audience: {{ .audience }}
          expirationSeconds: {{ .expirationSeconds }}
  {{- else if eq .source "secret" }}
  secret:
    secretName: {{ .secretName }}
    defaultMode: 0440
    items:
      - key: {{ .secretKey }}
        path: {{ base .path }}
  {{- else }}
  {{- toYaml .volume | nindent 2 }}
  {{- end }}
{{- end -}}

{{- /*
Converts a Go duration literal to milliseconds so the chart can compare two of
them, failing on anything Go itself would not parse.

Helm has no duration type and no duration arithmetic, and the two durations the
proxy takes are not independent: an upstream request timeout above the
admission lease produces a call that outlives the permission it was granted
under. Comparing them needs a number, and reading one out of "90s" is the only
way to get it.

Compound literals are accepted ("1m30s") because Go accepts them, and a guard
that refused a duration the binary would take is a false refusal. The
concatenation check is what makes that safe: it rejects anything with
characters the unit scan did not consume, so "30sec", "5 s" and "1e3s" fail
rather than silently parsing as their leading prefix. Fractional and unitless
forms ("1.5m", "0") are refused deliberately — a bare 0 means "no timeout" to
Go, which for this component is an upstream call that cannot outlive the lease
because it cannot end.

Takes a dict of name and value; emits the total in milliseconds.
*/ -}}
{{- define "shoal.durationMillis" -}}
{{- $name := .name -}}
{{- $value := trim (toString .value) -}}
{{- $parts := regexFindAll "[0-9]+(ms|h|m|s)" $value -1 -}}
{{- if or (not $parts) (ne (join "" $parts) $value) -}}
{{- fail (printf "%s must be a Go duration built from whole numbers and the units ms, s, m or h — for example 30s, 2m or 1m30s (got %q). Anything else is not a duration the binary can parse, so the pod exits at startup with a flag error" $name $value) -}}
{{- end -}}
{{- $factors := dict "ms" 1 "s" 1000 "m" 60000 "h" 3600000 -}}
{{- $total := int64 0 -}}
{{- range $parts -}}
{{- $unit := regexFind "(ms|h|m|s)$" . -}}
{{- $total = add $total (mul (int64 (regexFind "^[0-9]+" .)) (index $factors $unit)) -}}
{{- end -}}
{{- $total -}}
{{- end -}}
