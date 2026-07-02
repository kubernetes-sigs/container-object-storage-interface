#!/usr/bin/env bash
# Copyright 2024 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Source this file to acquire a Boskos GCE project in Prow.

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

BOSKOS_URL="${BOSKOS_URL:-http://boskos.test-pods.svc.cluster.local}"

acquire_project() {
  local project=""
  local project_type="gce-project"
  local boskos_response

  boskos_response=$(curl -X POST "${BOSKOS_URL}/acquire?type=${project_type}&state=free&dest=busy&owner=${JOB_NAME}")
  echo
  echo "DEBUG--Boskos Response: ${boskos_response}"
  echo
  if project=$(echo "${boskos_response}" | jq -r '.name'); then
    echo "Using GCP project: ${project}"
    PROJECT="${project}"
    export PROJECT
    heartbeat_project_forever &
    BOSKOS_HEARTBEAT_PID=$!
    export BOSKOS_HEARTBEAT_PID
  else
    echo "ERROR: failed to acquire GCP project. boskos response was: ${boskos_response}" >&2
    exit 1
  fi
}

release_project() {
  curl -X POST "${BOSKOS_URL}/release?name=${PROJECT}&owner=${JOB_NAME}&dest=dirty"
}

heartbeat_project() {
  curl -X POST "${BOSKOS_URL}/update?name=${PROJECT}&state=busy&owner=${JOB_NAME}" >/dev/null 2>&1
}

heartbeat_project_forever() {
  set +x
  local heartbeat_seconds=10
  while true; do
    heartbeat_project || true
    sleep "${heartbeat_seconds}"
  done
}

cleanup_boskos() {
  kill "${BOSKOS_HEARTBEAT_PID}" || true
  release_project
}
