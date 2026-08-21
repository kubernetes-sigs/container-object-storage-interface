#!/usr/bin/env bash
# Deploy cosi-driver-sample into the current kind cluster, including the
# admin/access Secrets the BucketClass/BucketAccessClass reference.

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
ROOT="${SCRIPT_DIR}/.."

# shellcheck source=hack/kind-helpers.sh disable=SC1091
source "${SCRIPT_DIR}/kind-helpers.sh"

SAMPLE_DRIVER_REPO="${SAMPLE_DRIVER_REPO:-https://github.com/BlaineEXE/cosi-driver-sample.git}" # testonly
SAMPLE_DRIVER_BRANCH="${SAMPLE_DRIVER_BRANCH:-update-rook-version}" # testonly
SAMPLE_DRIVER_IMAGE="${SAMPLE_DRIVER_IMAGE:-cosi-driver-sample:latest}"
SIDECAR_TAG="${SIDECAR_TAG:-cosi-provisioner-sidecar:latest}"
DRIVER_NAMESPACE="${DRIVER_NAMESPACE:-cosi-driver-sample-system}"
SAMPLE_DRIVER_PATH="${SAMPLE_DRIVER_PATH:-${ROOT}/../cosi-driver-sample}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-cosi-e2e}"
CREDS_FILE="${CREDS_FILE:?CREDS_FILE must point to a values file produced by cosi-driver-sample/hack/setup-s3-backend.sh}"

if [ ! -d "${SAMPLE_DRIVER_PATH}" ]; then
  git clone --depth 1 --branch "${SAMPLE_DRIVER_BRANCH}" \
    "${SAMPLE_DRIVER_REPO}" "${SAMPLE_DRIVER_PATH}"
fi

make -C "${SAMPLE_DRIVER_PATH}" build SAMPLE_DRIVER_TAG="${SAMPLE_DRIVER_IMAGE}"
load_image_into_cluster "${SAMPLE_DRIVER_IMAGE}"

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
