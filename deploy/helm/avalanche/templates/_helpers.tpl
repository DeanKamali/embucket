{{/* Chart boilerplate helpers. */}}
{{- define "avalanche.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "avalanche.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if eq .Release.Name .Chart.Name -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "avalanche.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "avalanche.selectorLabels" -}}
app.kubernetes.io/name: {{ include "avalanche.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "avalanche.labels" -}}
{{ include "avalanche.selectorLabels" . }}
app.kubernetes.io/part-of: avalanche
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}