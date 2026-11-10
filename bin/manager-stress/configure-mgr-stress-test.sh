#! /usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
default_client_config="${script_dir}/../../conf/accumulo-testing.properties"
default_namespace="mgrstress_$(date +%Y%m%d_%H%M%S)_${RANDOM}"
default_host=$(hostname -f 2>/dev/null || hostname)

prompt_default() {
  local label=$1
  local default=$2
  local answer
  printf '%s [%s]: ' "$label" "$default" >&2
  IFS= read -r answer
  REPLY=${answer:-$default}
}

prompt_optional() {
  local label=$1
  local default_description=$2
  printf '%s [%s]: ' "$label" "$default_description" >&2
  IFS= read -r REPLY
}

shell_quote() {
  local value=$1
  value=${value//\'/\'\\\'\'}
  printf "'%s'" "$value"
}

printf '%s\n' \
  'Enter the ManagerStress settings. Press Enter to accept the displayed defaults.' \
  'Client passwords should be stored in the selected client properties file.' >&2

prompt_default 'ManagerStress duration' '5m'
manager_duration=$REPLY
prompt_default 'ManagerStress client JVMs' '2'
manager_clients=$REPLY
prompt_default 'ManagerStress table count' '4'
manager_tables=$REPLY
prompt_default 'Namespace (generated names must not already exist)' "$default_namespace"
manager_namespace=$REPLY
prompt_default 'Compactor resource group (shared with StressCompactor)' 'default'
compactor_resource_group=$REPLY
prompt_optional 'Table prefix (blank generates one)' 'generated automatically'
manager_table_prefix=$REPLY
prompt_default 'Shared HDFS directory for bulk imports' 'hdfs:///tmp/accumulo-managerstress'
manager_hdfs_dir=$REPLY
prompt_default 'Create-table weight' '1'
manager_create_weight=$REPLY
prompt_default 'Delete-table weight' '1'
manager_delete_weight=$REPLY
prompt_default 'Split weight' '1'
manager_split_weight=$REPLY
prompt_default 'Merge weight' '1'
manager_merge_weight=$REPLY
prompt_default 'Tablet-availability weight' '1'
manager_availability_weight=$REPLY
prompt_default 'Compaction weight' '1'
manager_compact_weight=$REPLY
prompt_default 'Bulk-import weight' '1'
manager_bulk_import_weight=$REPLY
prompt_optional 'ManagerStress random seed (blank uses a random seed)' 'random per run'
manager_seed=$REPLY
prompt_optional 'ManagerStress JAVA_OPTS (blank uses JVM defaults)' 'empty'
manager_java_opts=$REPLY

printf '\n%s\n' 'Enter the StressCompactor settings.' >&2
prompt_default 'StressCompactor JVM duration' '5m'
compactor_duration=$REPLY
prompt_default 'Compactors per JVM' '1'
compactors_per_jvm=$REPLY
prompt_default 'Compactor callback host' "$default_host"
compactor_host=$REPLY
prompt_default 'Compactor callback port range' '0'
compactor_port_range=$REPLY
prompt_default 'Compaction success weight' '1'
compactor_success_weight=$REPLY
prompt_default 'Compaction failure weight' '1'
compactor_failure_weight=$REPLY
prompt_default 'Compaction cancellation weight' '1'
compactor_cancellation_weight=$REPLY
prompt_optional 'StressCompactor random seed (blank uses a random seed)' 'random per run'
compactor_seed=$REPLY
prompt_optional 'StressCompactor JAVA_OPTS (blank uses JVM defaults)' 'empty'
compactor_java_opts=$REPLY

printf '\n%s\n' 'Enter shared Accumulo client settings.' >&2
prompt_default 'Accumulo client config file' "$default_client_config"
client_config=$REPLY
prompt_optional 'Accumulo user override (blank uses client config)' 'from client config'
client_user=$REPLY
prompt_optional 'Authorizations (blank uses none)' 'empty'
client_auths=$REPLY
prompt_default 'Enable distributed tracing? (yes/no)' 'no'
trace_choice=${REPLY,,}
case "$trace_choice" in
  yes | y) enable_trace=true ;;
  no | n) enable_trace=false ;;
  *)
    printf 'Please answer yes or no.\n' >&2
    exit 2
    ;;
esac

client_args=(--config-file "$client_config")
if [[ -n "$client_user" ]]; then
  client_args+=(--user "$client_user")
fi
if [[ -n "$client_auths" ]]; then
  client_args+=(--auths "$client_auths")
fi
if [[ "$enable_trace" == true ]]; then
  client_args+=(--trace)
fi
while true; do
  prompt_optional 'Additional client property override (key=value; blank to finish)' 'none'
  if [[ -z "$REPLY" ]]; then
    break
  fi
  client_args+=(-o "$REPLY")
done

manager_args=(
  --namespace "$manager_namespace"
  --compactor-resource-group "$compactor_resource_group"
  --duration "$manager_duration"
  --clients "$manager_clients"
  --tables "$manager_tables"
  --hdfs-dir "$manager_hdfs_dir"
  --create-weight "$manager_create_weight"
  --delete-weight "$manager_delete_weight"
  --split-weight "$manager_split_weight"
  --merge-weight "$manager_merge_weight"
  --availability-weight "$manager_availability_weight"
  --compact-weight "$manager_compact_weight"
  --bulk-import-weight "$manager_bulk_import_weight"
)
if [[ -n "$manager_table_prefix" ]]; then
  manager_args+=(--table-prefix "$manager_table_prefix")
fi
if [[ -n "$manager_seed" ]]; then
  manager_args+=(--seed "$manager_seed")
fi
manager_args+=("${client_args[@]}")

compactor_args=(
  --resource-group "$compactor_resource_group"
  --duration "$compactor_duration"
  --compactors-per-jvm "$compactors_per_jvm"
  --host "$compactor_host"
  --port-range "$compactor_port_range"
  --success-weight "$compactor_success_weight"
  --failure-weight "$compactor_failure_weight"
  --cancellation-weight "$compactor_cancellation_weight"
)
if [[ -n "$compactor_seed" ]]; then
  compactor_args+=(--seed "$compactor_seed")
fi
compactor_args+=("${client_args[@]}")

print_command() {
  local label=$1
  local java_opts=$2
  local script=$3
  local argument
  local index=0
  shift 3
  local -a arguments=("$@")

  printf '# %s\n' "$label"
  if [[ -n "$java_opts" ]]; then
    printf 'JAVA_OPTS=%s ' "$(shell_quote "$java_opts")"
  fi
  printf '%q' "$script"
  while ((index < ${#arguments[@]})); do
    argument=${arguments[index]}
    printf ' \\\n  %q' "$argument"
    if [[ "$argument" == --trace ]]; then
      index=$((index + 1))
      continue
    fi
    if ((index + 1 >= ${#arguments[@]})); then
      printf 'Option %s is missing its value.\n' "$argument" >&2
      return 2
    fi
    printf ' %q' "${arguments[index + 1]}"
    index=$((index + 2))
  done
  printf '\n\n'
}

print_command 'ManagerStress' "$manager_java_opts" "${script_dir}/managerstress.sh" \
  "${manager_args[@]}"
print_command 'StressCompactor' "$compactor_java_opts" "${script_dir}/stresscompactor.sh" \
  "${compactor_args[@]}"
