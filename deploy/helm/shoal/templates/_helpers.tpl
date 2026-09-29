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
