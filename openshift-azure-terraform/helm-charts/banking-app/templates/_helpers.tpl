{{- define "banking-app.namespace" -}}
{{- default .Release.Namespace .Values.namespaceOverride -}}
{{- end -}}

{{- define "banking-app.meshLabels" -}}
{{- if .Values.global.mesh.enabled }}
version: v1
mesh-scope: {{ .Values.global.mesh.scope | quote }}
sidecar.istio.io/inject: "true"
{{- end }}
{{- end -}}
