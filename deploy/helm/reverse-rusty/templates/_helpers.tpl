{{/* Base name + release-qualified fullname (standard Helm idiom). */}}
{{- define "reverse-rusty.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "reverse-rusty.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "reverse-rusty.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "reverse-rusty.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "reverse-rusty.selectorLabels" -}}
app.kubernetes.io/name: {{ include "reverse-rusty.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Fully-qualified image ref; tag defaults to the chart appVersion. */}}
{{- define "reverse-rusty.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Per-component object names. The base is truncated to 51 (= 63 − len("-coordinator"))
BEFORE the suffix so every name stays within the 63-char DNS-label limit AND the
distinguishing suffix is never itself truncated away (which a trunc-after-suffix would
do at the boundary, collapsing shard/control/coordinator onto the same name).
*/}}
{{- define "reverse-rusty.shardName" -}}{{ printf "%s-shard" (include "reverse-rusty.fullname" . | trunc 51 | trimSuffix "-") }}{{- end -}}
{{- define "reverse-rusty.controlName" -}}{{ printf "%s-control" (include "reverse-rusty.fullname" . | trunc 51 | trimSuffix "-") }}{{- end -}}
{{- define "reverse-rusty.coordinatorName" -}}{{ printf "%s-coordinator" (include "reverse-rusty.fullname" . | trunc 51 | trimSuffix "-") }}{{- end -}}

{{/* http vs https for mesh URLs — must match the served transport (ADR-082). */}}
{{- define "reverse-rusty.scheme" -}}
{{- if .Values.tls.enabled -}}https{{- else -}}http{{- end -}}
{{- end -}}

{{/* Stable DNS FQDN for shard ordinal $i (headless service per StatefulSet). */}}
{{- define "reverse-rusty.shardFqdn" -}}
{{- $i := .index -}}
{{- printf "%s-%d.%s.%s.svc.%s" (include "reverse-rusty.shardName" .root) $i (include "reverse-rusty.shardName" .root) .root.Release.Namespace .root.Values.clusterDomain -}}
{{- end -}}

{{/* Stable DNS FQDN for control ordinal $i. */}}
{{- define "reverse-rusty.controlFqdn" -}}
{{- $i := .index -}}
{{- printf "%s-%d.%s.%s.svc.%s" (include "reverse-rusty.controlName" .root) $i (include "reverse-rusty.controlName" .root) .root.Release.Namespace .root.Values.clusterDomain -}}
{{- end -}}

{{/*
Probe settings (ADR-211): the chart's defaults, under whatever the values set.

The defaults are written here as well as in values.yaml because `helm upgrade --reuse-values`
renders with the values a release was installed with, and a release installed before these
settings existed has no `probes` map at all. Without the defaults here such an upgrade would
fail to render, or render a coordinator with no probes. deploy/check-probes.sh renders the
chart both ways and fails if the two disagree.
*/}}
{{- define "reverse-rusty.coordinatorProbes" -}}
{{- $defaults := dict
    "liveness" (dict "path" "/_health/live" "periodSeconds" 10 "timeoutSeconds" 3 "failureThreshold" 6 "terminationGracePeriodSeconds" 30)
    "readiness" (dict "path" "/_health/ready" "periodSeconds" 10 "timeoutSeconds" 3 "failureThreshold" 3)
    "startup" (dict "periodSeconds" 5 "timeoutSeconds" 3 "failureThreshold" 60 "terminationGracePeriodSeconds" 30) -}}
{{- $set := deepCopy (((.Values.coordinator | default dict).probes) | default dict) -}}
{{- mustMergeOverwrite $defaults $set | toYaml -}}
{{- end -}}

{{- define "reverse-rusty.shardProbes" -}}
{{- $defaults := dict "timeoutSeconds" 3 "startup" (dict "periodSeconds" 10 "failureThreshold" 60) -}}
{{- $set := deepCopy (((.Values.shard | default dict).probes) | default dict) -}}
{{- mustMergeOverwrite $defaults $set | toYaml -}}
{{- end -}}

{{- define "reverse-rusty.controlProbes" -}}
{{- $defaults := dict "timeoutSeconds" 3 -}}
{{- $set := deepCopy (((.Values.controlPlane | default dict).probes) | default dict) -}}
{{- mustMergeOverwrite $defaults $set | toYaml -}}
{{- end -}}

