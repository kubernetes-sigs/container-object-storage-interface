#!/usr/bin/env bash
# Copyright 2026 The Kubernetes Authors.
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

# Deploy cosi-driver-sample into the current kops cluster. Images are expected
# to be pushed to a registry reachable by the cluster before this script runs.

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
ROOT="${SCRIPT_DIR}/.."

SAMPLE_DRIVER_REPO="${SAMPLE_DRIVER_REPO:-https://github.com/kubernetes-sigs/cosi-driver-sample.git}"
SAMPLE_DRIVER_BRANCH="${SAMPLE_DRIVER_BRANCH:-main}"
SAMPLE_DRIVER_IMAGE="${SAMPLE_DRIVER_IMAGE:-cosi-driver-sample:latest}"
SIDECAR_TAG="${SIDECAR_TAG:-cosi-provisioner-sidecar:latest}"
DRIVER_NAMESPACE="${DRIVER_NAMESPACE:-cosi-driver-sample-system}"
SAMPLE_DRIVER_PATH="${SAMPLE_DRIVER_PATH:-${ROOT}/../cosi-driver-sample}"
CREDS_FILE="${CREDS_FILE:?CREDS_FILE must point to a values file produced by cosi-driver-sample/hack/setup-s3-backend.sh}"

if [ ! -d "${SAMPLE_DRIVER_PATH}" ]; then
  git clone --depth 1 --branch "${SAMPLE_DRIVER_BRANCH}" \
    "${SAMPLE_DRIVER_REPO}" "${SAMPLE_DRIVER_PATH}"
fi

make -C "${ROOT}" kustomize
KUSTOMIZE="${KUSTOMIZE:-${ROOT}/.cache/tools/kustomize}"

"${KUSTOMIZE}" build "${SAMPLE_DRIVER_PATH}/config/default" | kubectl apply -f -

kubectl -n "${DRIVER_NAMESPACE}" set image deployment/cosi-sample-driver \
  driver="${SAMPLE_DRIVER_IMAGE}" \
  objectstorage-provisioner-sidecar="${SIDECAR_TAG}"

read_value() {
  local key="$1"
  grep -E "^${key}:" "${CREDS_FILE}" | head -n1 | sed -E 's/^[^:]+:[[:space:]]*"?([^"]*)"?[[:space:]]*$/\1/'
}

S3_ENDPOINT="$(read_value s3Endpoint)"
S3_REGION="$(read_value s3Region)"
ADMIN_KEY="$(read_value adminAccessKeyId)"
ADMIN_SECRET="$(read_value adminSecretAccessKey)"
ACCESS_KEY="$(read_value accessKeyId)"
ACCESS_SECRET="$(read_value accessSecretKey)"

kubectl -n "${DRIVER_NAMESPACE}" create secret generic cosi-sample-s3-admin-secret \
  --from-literal=AWS_ENDPOINT_URL="${S3_ENDPOINT}" \
  --from-literal=AWS_REGION="${S3_REGION}" \
  --from-literal=AWS_ACCESS_KEY_ID="${ADMIN_KEY}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${ADMIN_SECRET}" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "${DRIVER_NAMESPACE}" create secret generic cosi-sample-s3-access-secret \
  --from-literal=AWS_ENDPOINT_URL="${S3_ENDPOINT}" \
  --from-literal=AWS_REGION="${S3_REGION}" \
  --from-literal=AWS_ACCESS_KEY_ID="${ACCESS_KEY}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${ACCESS_SECRET}" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "${DRIVER_NAMESPACE}" rollout status deployment/cosi-sample-driver --timeout=300s
