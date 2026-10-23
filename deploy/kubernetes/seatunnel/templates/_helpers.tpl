#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

{{- define "seatunnel.fullname" -}}
{{- default (printf "%s-seatunnel" .Release.Name) .Values.fullnameOverride | trunc 55 | trimSuffix "-" -}}
{{- end -}}

{{- define "seatunnel.selectorLabels" -}}
app.kubernetes.io/name: seatunnel
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/part-of: seatunnel
{{- end -}}

{{- define "seatunnel.common.labels" -}}
{{ include "seatunnel.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
{{- end -}}

{{- define "seatunnel.image" -}}
{{- $reference := .Values.image.registry -}}
{{- $registry := trimSuffix "/" (.Values.global.imageRegistry | default "") -}}
{{- $first := first (splitList "/" $reference) -}}
{{- $qualified := and (contains "/" $reference) (or (contains "." $first) (contains ":" $first) (eq $first "localhost")) -}}
{{- if and $registry (not $qualified) -}}
{{- $reference = printf "%s/%s" $registry $reference -}}
{{- end -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" $reference .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" $reference (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end -}}

{{- define "seatunnel.configMapName" -}}
{{- if .Values.configMap.create -}}
{{- include "seatunnel.fullname" . }}-configs
{{- else -}}
{{- required "seatunnel.configMap.existingConfigMapName is required when configMap.create=false" .Values.configMap.existingConfigMapName -}}
{{- end -}}
{{- end -}}

{{- define "seatunnel.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "seatunnel.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "seatunnel.masterReplicas" -}}
{{- $replicas := ternary 1 2 (eq .Values.mode "standalone") -}}
{{- if ne (.Values.master.replicas | toString) "<nil>" -}}
{{- $replicas = int .Values.master.replicas -}}
{{- end -}}
{{- if and (eq .Values.mode "standalone") (ne (int $replicas) 1) -}}
{{- fail "seatunnel.master.replicas must be 1 in standalone mode" -}}
{{- end -}}
{{- $replicas -}}
{{- end -}}

{{/* Render the same final files into ConfigMap data and rollout checksum. */}}
{{- define "seatunnel.configData" -}}
{{- $files := dict -}}
{{- range $path, $_ := .Files.Glob "conf/*" -}}
{{- $name := base $path -}}
{{- $content := tpl ($.Files.Get $path) $ -}}
{{- if eq $name "seatunnel.yaml" -}}
{{- $config := fromYaml $content -}}
{{- $engine := mergeOverwrite $config.seatunnel.engine (deepCopy $.Values.engineConfig) -}}
{{- $_ := set $engine.http "port" (int $.Values.service.port) -}}
{{- $_ := set $engine.http "enable-http" true -}}
{{- $_ := set $engine.http "enable-dynamic-port" false -}}
{{- $content = toYaml $config -}}
{{- end -}}
{{- $_ := set $files $name $content -}}
{{- end -}}
{{- range $name, $content := .Values.configMap.data -}}
{{- $_ := set $files $name (tpl $content $) -}}
{{- end -}}
{{- toYaml $files -}}
{{- end -}}
