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
{{- /* Quoted only when overridden: an override such as "1.5" or "True" is
       otherwise read as a number or boolean, which a label cannot be, and the
       chart's own name renders exactly as it always has. */}}
app.kubernetes.io/name: {{ if .Values.nameOverride }}{{ include "shoal.name" . | quote }}{{ else }}{{ include "shoal.name" . }}{{ end }}
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

{{- define "shoal.llmGatewayEnabled" -}}
{{- .Values.llmGateway.enabled -}}
{{- end -}}

{{- /*
The gateway's names come from a bounded stem for the same reason the explorer's
do: Kubernetes rejects a name over 63 characters, and truncating the finished
name does not help, because the suffix is appended after the truncation. The
stem reserves room for the longest suffix instead, so a long release name
shortens the stem rather than overflowing the name.

  stem         51
  -llm-gateway 12  -> 63
*/ -}}
{{- define "shoal.llmGatewayStem" -}}
{{- include "shoal.fullname" . | trunc 51 | trimSuffix "-" -}}
{{- end -}}

{{- define "shoal.llmGatewayName" -}}
{{- printf "%s-llm-gateway" (include "shoal.llmGatewayStem" .) -}}
{{- end -}}

{{- /*
A selector distinct from the explorer's and from the storage tier's. The two
planes are separate processes deliberately, and a selector that could match
either would let the Service carrying arbitrary prompt traffic land on the pod
holding the policy store.
*/ -}}
{{- define "shoal.llmGatewaySelectorLabels" -}}
app.kubernetes.io/name: shoal-llm-gateway
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: llm-gateway
{{- end -}}

{{- /*
The gateway's host allow-list, normalized and comma-joined for -allowed-host.

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
{{- /*
The models this deployment names, trimmed the way the allow-list is and for the
same reason: the value is joined into one argument, so a blank element would
render a comma pair the binary reads as an empty model name.

Empty is a working configuration, not a missing one. With no models named the
declaration reports every model as "other", which keeps caller-controlled text
out of what the workspace receives — the field is free-form, so a prompt fits
in it as readily as a model name. Naming models here buys policy granularity
and nothing else; it does not restrict which models may be called.
*/ -}}
{{- define "shoal.llmGatewayModels" -}}
{{- $models := list -}}
{{- range (default (list) .Values.llmGateway.models) -}}
{{- if trim (default "" .) -}}
{{- $models = append $models (trim .) -}}
{{- end -}}
{{- end -}}
{{- join "," $models -}}
{{- end -}}

{{- define "shoal.llmGatewayAllowedHosts" -}}
{{- $hosts := list -}}
{{- range (default (list) .Values.llmGateway.allowedHosts) -}}
{{- if trim (default "" .) -}}
{{- $hosts = append $hosts (trim .) -}}
{{- end -}}
{{- end -}}
{{- join "," $hosts -}}
{{- end -}}

{{- /*
One credential volume, for either of the gateway's two -file credentials.

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
The kubelet writes projected and Secret volumes owned by root; the gateway
container runs as uid 65532 with every capability dropped. At 0400 the one
process that needs the credential cannot open it, and the failure is not a
crash — the pod starts, passes both probes, and fails on the credential at
every request. 0440 is readable exactly because the pod declares fsGroup 65532
alongside it; neither half works without the other.
*/ -}}
{{- define "shoal.llmGatewayCredentialVolume" -}}
- name: {{ .name | quote }}
  {{- if eq .source "projected" }}
  projected:
    defaultMode: 0440
    sources:
      - serviceAccountToken:
          path: {{ base .path | quote }}
          audience: {{ .audience | quote }}
          expirationSeconds: {{ .expirationSeconds }}
  {{- else if eq .source "secret" }}
  secret:
    secretName: {{ .secretName | quote }}
    defaultMode: 0440
    items:
      - key: {{ .secretKey | quote }}
        path: {{ base .path | quote }}
  {{- else }}
  {{- toYaml .volume | nindent 2 }}
  {{- end }}
{{- end -}}

{{- /*
Converts a Go duration literal to milliseconds so the chart can compare two of
them, failing on anything Go itself would not parse.

Helm has no duration type and no duration arithmetic, and the two durations the
gateway takes are not independent: an upstream request timeout above the
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

{{- /*
The gateway's transport rule, as the binary spells it (absoluteURL and isLoopback
in cmd/shoal-llm-gateway/admission.go): an absolute http(s) URL with a host, and
http permitted only to loopback.

These were scheme-prefix tests, which disagreed with the binary in both
directions. "https://" has the prefix and no host, so the chart passed it and
the binary refused it for having no host — a pod in CrashLoopBackOff from a
values file the chart approved. And "http://localhost.example" has the
"http://localhost" prefix without being loopback at all, so the chart treated a
remote plaintext host as exempt while the binary refused it. Parsing is the only
way to get both right, and urlParse gives the two fields the rule needs.

The hostname is lowered because the binary compares it with EqualFold, so
"HTTP://LOCALHOST" is loopback there and must be here. Go's url.Parse already
lowers the scheme but not the host.
*/ -}}
{{- define "shoal.urlIsAbsolute" -}}
{{- $parsed := urlParse . -}}
{{- if and (has $parsed.scheme (list "http" "https")) $parsed.hostname -}}
true
{{- end -}}
{{- end -}}

{{- define "shoal.urlIsPlaintext" -}}
{{- if eq (urlParse .).scheme "http" -}}
true
{{- end -}}
{{- end -}}

{{- /* 127.0.0.0/8, matched as an address rather than as a prefix.
       net.IP.IsLoopback accepts any address in that block, so "127.0.0.1"
       alone is too narrow — but "127." as a prefix is too broad in the
       direction that matters: "127.example.com" is a DNS name the binary
       refuses, and a prefix test approved it, which is the same
       chart-renders-what-the-binary-refuses failure the parsing was meant to
       end.

       Each octet is bounded, because net.ParseIP refuses 127.0.0.256 and a
       looser \d{1,3} would approve it. Go also refuses a short form like
       "127.1", so a full dotted quad is required here too.

       IPv6 loopback is matched as "::1" and its fully expanded spelling. Other
       spellings (0::1, ::0001) are loopback to net.ParseIP and are refused
       here, which is a false refusal rather than an approved-but-broken pod —
       the safe direction for a guard to be wrong in, and stated so nobody
       reads it as an oversight. */ -}}
{{- define "shoal.urlIsLoopback" -}}
{{- $host := lower (urlParse .).hostname -}}
{{- $octet := "(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])" -}}
{{- $v4 := printf "^127\\.%s\\.%s\\.%s$" $octet $octet $octet -}}
{{- if or (eq $host "localhost") (eq $host "::1")
          (eq $host "0:0:0:0:0:0:0:1") (regexMatch $v4 $host) -}}
true
{{- end -}}
{{- end -}}

{{- /*
A rollout or disruption value, validated as written.

These reach the API server verbatim, and the only guard they had cast to int
first — which is exactly where the value escapes. int 1.5 is 1 and int -1 is
-1, so a guard testing "did it cast to zero" approves both, the chart reports
success, and the API server rejects the object at install or upgrade time.
Casting before validating cannot see the thing it is validating.

Kubernetes takes an IntOrString here: a non-negative whole number, or a
percentage. Both forms are accepted and nothing else is, which is narrower than
int and wider than a bare integer.
*/ -}}
{{- define "shoal.requireIntOrPercent" -}}
{{- $name := .name -}}
{{- /* No "default" here: Helm's default treats 0 as empty, so
       `default "" 0` is "" and this guard refused the chart's own shipped
       maxUnavailable: 0. The value is stringified directly, and a missing key
       fails the match like any other non-value. */ -}}
{{- $raw := trim (toString .value) -}}
{{- if not (or (regexMatch "^(0|[1-9][0-9]*)$" $raw) (regexMatch "^(0|[1-9][0-9]*)%$" $raw)) -}}
{{- fail (printf "%s must be a non-negative whole number or a percentage (got %q): it is rendered into the object verbatim, so the API server rejects the %s at install or upgrade time while the chart reports success. A fractional or negative value is not an IntOrString, and a guard that casts to int before testing cannot tell — int of 1.5 is 1 and int of -1 is -1" $name $raw (default "object" .kind)) -}}
{{- end -}}
{{- end -}}

{{- /*
Whether an IntOrString means zero, judged on what was written.

"0" and "0%" both mean zero; "50%" does not. The guard that needed this cast
with int first, and int of "50%" is 0 — so a perfectly valid percentage
disruption budget was refused as if it were zero, with a message about
blocking evictions that had nothing to do with it. Same flaw as the one
requireIntOrPercent exists for, in the guard that was already there.
*/ -}}
{{- define "shoal.isZeroIntOrPercent" -}}
{{- $raw := trim (toString .) -}}
{{- if or (eq $raw "0") (eq $raw "0%") -}}
true
{{- end -}}
{{- end -}}

{{- /*
Refuse a line break anywhere in a values subtree, naming the key that holds it.

Takes a dict of path (the dotted key, for the message), value, and two lists of
paths: free, whose subtrees are not walked at all, and trimmed, whose strings
are checked after trimming because the templates trim them before use. Recurses
through maps and lists; every string leaf is checked. Every other scalar kind
(number, boolean, null) renders through Go formatting and cannot carry one.

The class is [\p{Cc}\p{Zl}\p{Zp}], in a raw string so the template does not
unescape it: the ASCII and C1 controls, which take in LF, CR and NEL, plus the
Unicode line and paragraph separators. go-yaml breaks lines on all of those,
and RE2's [[:cntrl:]] is ASCII only, so a narrower class lets NEL through.

objectStorage.gcsKeyJson is the one value in the chart that is multi-line by
design: a service-account key file, rendered as the body of a `|` block scalar
through `indent 4`. indent splits on LF only, so LF (and CRLF, and tab) stay
inside the block. A CR alone, a NEL or a separator is a line break that YAML
sees and indent does not, and the text after it lands at column zero — outside
the block scalar and outside the Secret. So that value keeps LF, CRLF and tab
and is refused everything else.
*/ -}}
{{- define "shoal.refuseLineBreaks" -}}
{{- $context := . -}}
{{- if not (has .path .free) -}}
{{- if kindIs "map" .value -}}
{{- range $key, $child := .value -}}
{{- include "shoal.refuseLineBreaks" (dict "path" (printf "%s.%s" $context.path $key) "value" $child "free" $context.free "trimmed" $context.trimmed) -}}
{{- end -}}
{{- else if kindIs "slice" .value -}}
{{- range $index, $child := .value -}}
{{- include "shoal.refuseLineBreaks" (dict "path" (printf "%s[%d]" $context.path $index) "value" $child "free" $context.free "trimmed" $context.trimmed) -}}
{{- end -}}
{{- else if kindIs "string" .value -}}
{{- $text := .value -}}
{{- range $prefix := .trimmed -}}
{{- if or (eq $context.path $prefix) (hasPrefix (printf "%s." $prefix) $context.path) (hasPrefix (printf "%s[" $prefix) $context.path) -}}
{{- $text = trim $text -}}
{{- end -}}
{{- end -}}
{{- if eq .path "objectStorage.gcsKeyJson" -}}
{{- $text = $text | replace "\r\n" "" | replace "\n" "" | replace "\t" "" -}}
{{- end -}}
{{- /* The message names the character and not the value: the walk reaches
       passwords and key files, and a refusal is printed to CI logs. */ -}}
{{- $found := regexFind `[\p{Cc}\p{Zl}\p{Zp}]` $text -}}
{{- if $found -}}
{{- fail (printf "%s holds %q, a line break or other control character (NEL, U+2028 and U+2029 included): the chart renders it into the manifests as a YAML scalar, and a line break there ends the scalar, so the rest of the value becomes YAML of its own — another field, another container argument, another key. No value this chart renders may carry one. A value read with --set-file keeps the file's trailing newline, so strip it" .path $found) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- /*
A value with a fixed textual shape, validated as written: a Kubernetes quantity,
a Go duration, a Secret name, an enumerated field. Takes a dict of name, value,
pattern, what (the shape, for the message) and why. An empty value is left to
whatever already owns that case.

Each of these is rendered unquoted by a template the storage profiles also
render, and those profiles must stay byte-identical, so quoting is not
available; the value is held to a shape that contains no YAML syntax instead.
That matters beyond line breaks in the accumulo templates, where the value sits
inside a flow mapping and a comma or a brace ends it on the same line: a Secret
name of "creds, key: other" names a different key.
*/ -}}
{{- define "shoal.requireShape" -}}
{{- $raw := toString (default "" .value) -}}
{{- if and $raw (not (regexMatch .pattern $raw)) -}}
{{- fail (printf "%s must be %s (got %q): %s" .name .what $raw .why) -}}
{{- end -}}
{{- end -}}
