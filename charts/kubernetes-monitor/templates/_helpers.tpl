{{/*
Expand the name of the chart.
*/}}
{{- define "kubernetes-monitor.name" -}}
{{- .Values.nameOverride | default .Chart.Name | trunc 63 | lower | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "kubernetes-monitor.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | lower | trimSuffix "-" }}
{{- else }}
{{- $name := .Values.nameOverride | default .Chart.Name }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | lower | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | lower | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "kubernetes-monitor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "kubernetes-monitor.labels" -}}
helm.sh/chart: {{ include "kubernetes-monitor.chart" . }}
{{ include "kubernetes-monitor.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "kubernetes-monitor.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kubernetes-monitor.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "kubernetes-monitor.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- .Values.serviceAccount.name | default (include "kubernetes-monitor.fullname" .) -}}
{{- else -}}
{{- required "If you aren't creating a service account, a valid .Values.serviceAccount.name is required!" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{/*
Create the name of the kubernetes monitor role to use
*/}}
{{- define "kubernetes-monitor.kubernetesMonitorRoleName" -}}
{{- printf "%s-role" (include "kubernetes-monitor.serviceAccountName" .) }}
{{- end }}

{{/*
Create the name of the kubernetes monitor role binding to use
*/}}
{{- define "kubernetes-monitor.kubernetesMonitorRoleBindingName" -}}
{{- printf "%s-binding" (include "kubernetes-monitor.serviceAccountName" .) }}
{{- end }}

{{/*
RBAC rules for the kubernetes monitor (Role and ClusterRole).
Verbs are hardcoded to get/watch/list; apiGroups and resources are taken from
.Values.monitor.rules so users can scope access without granting extra verbs.
*/}}
{{- define "kubernetes-monitor.rules" -}}
{{- range $i, $rule := .Values.monitor.rules -}}
{{- if $i }}
{{ end -}}
- verbs:
    - get
    - watch
    - list
  apiGroups:
    {{- toYaml $rule.apiGroups | nindent 4 }}
  resources:
    {{- toYaml $rule.resources | nindent 4 }}
{{- end -}}
{{- end -}}

{{/*
The base image for the monitor, without any suffixes.
Defaults to the Chart Appversion.
*/}}
{{- define "kubernetes-monitor.image" -}}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.repository (.Values.image.tag | default (printf "v%s" .Chart.AppVersion)) }}
{{- end }}

{{/*
The complete image for the monitor, including any optional suffixes.
*/}}
{{- define "kubernetes-monitor.fullImage" -}}
{{- if .Values.image.tagSuffix }}
{{- printf "%s-%s" (include "kubernetes-monitor.image" .) .Values.image.tagSuffix }}
{{- else }}
{{- (include "kubernetes-monitor.image" .) }}
{{- end }}
{{- end }}

{{/*
The name of the secret to store the authentication information (bearer token/api key)
*/}}
{{- define "kubernetes-monitor.secrets.serverApiAuth" -}}
{{- printf "%s-server-api-auth" ( include "kubernetes-monitor.name" . ) }}
{{- end }}

{{/*
The name of the secret to store the authentication details
*/}}
{{- define "kubernetes-monitor.authentication.secretName" -}}
{{- printf "%s-authentication" ( include "kubernetes-monitor.fullname" . ) }}
{{- end }}

{{/*
The prefix of the secret to store the authentication details
*/}}
{{- define "kubernetes-monitor.authentication.secretPrefix" -}}
{{- printf "%s" ( include "kubernetes-monitor.fullname" . ) }}
{{- end }}

{{/*
The name of the secret to store the CA data
*/}}
{{- define "kubernetes-monitor.customCa.secretName" -}}
{{- printf "%s-custom-ca" ( include "kubernetes-monitor.fullname" . ) }}
{{- end }}

{{/*
Hook annotations
*/}}
{{- define "kubernetes-monitor.registration.hookAnnotations" -}}
"helm.sh/hook": "pre-upgrade,pre-install"
"helm.sh/hook-delete-policy": "before-hook-creation,hook-succeeded"
{{- end }}

{{/*
The name of the registration hook pod
*/}}
{{- define "kubernetes-monitor.registration.podName" -}}
{{- printf "%s-registration" ( include "kubernetes-monitor.fullname" . ) }}
{{- end }}

{{/*
The name of the service account to use for registration
*/}}
{{- define "kubernetes-monitor.registration.serviceAccountName" -}}
{{- if .Values.registration.serviceAccount.create -}}
{{- .Values.registration.serviceAccount.name | default (printf "%s-registration" (include "kubernetes-monitor.fullname" .)) -}}
{{- else -}}
{{- required "If you aren't creating a service account, a valid .Values.registration.serviceAccount.name is required!" .Values.registration.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{/*
The name of the secret to use for temporary registration credentials
*/}}
{{- define "kubernetes-monitor.registration.secretName" -}}
{{- if .Values.registration.serverAccessTokenSecretName }}
{{- .Values.registration.serverAccessTokenSecretName }}
{{- else }}
{{- printf "%s-registration" ( include "kubernetes-monitor.fullname" . ) }}
{{- end }}
{{- end }}

{{/*
The key in the secret that contains the server access token
*/}}
{{- define "kubernetes-monitor.registration.secretKey" -}}
{{- .Values.registration.serverAccessTokenSecretKey | default "SERVER_ACCESS_TOKEN" }}
{{- end }}

{{/*
Create the name of the registration role to use
*/}}
{{- define "kubernetes-monitor.registration.roleName" -}}
{{- printf "%s-role" (include "kubernetes-monitor.registration.serviceAccountName" .) }}
{{- end }}

Create the name of the registration role binding to use
*/}}
{{- define "kubernetes-monitor.registration.roleBindingName" -}}
{{- printf "%s-binding" (include "kubernetes-monitor.registration.serviceAccountName" .) }}
{{- end }}

{{/*
The server API url - the global is used unless overridden by the value in values.yaml
*/}}
{{- define "kubernetes-monitor.serverApiUrl" -}}
{{- if .Values.registration.serverApiUrl }}
{{- .Values.registration.serverApiUrl }}
{{- else if .Values.global.serverApiUrl }}
{{- .Values.global.serverApiUrl }}
{{- end }}
{{- end }}

{{/*
The server CA certificate - the global is used unless overridden by the value in values.yaml
*/}}
{{- define "kubernetes-monitor.serverCertificate.certificate" -}}
{{- if .Values.registration.serverCertificate }}
{{- .Values.registration.serverCertificate }}
{{- else if .Values.global.serverCertificate }}
{{- .Values.global.serverCertificate }}
{{- end }}
{{- end }}

{{/*
The name of the secret to store the certificate data of the Octopus Server API
*/}}
{{- define "kubernetes-monitor.serverCertificate.secretName" -}}
{{- if .Values.registration.serverCertificateSecretName }}
{{- .Values.registration.serverCertificateSecretName }}
{{- else if .Values.global.serverCertificateSecretName }}
{{- .Values.global.serverCertificateSecretName }}
{{- else if (include "kubernetes-monitor.serverCertificate.certificate" .) }}
{{- printf "%s-server-certificate" ( include "kubernetes-monitor.fullname" . ) }}
{{- end }}
{{- end }}