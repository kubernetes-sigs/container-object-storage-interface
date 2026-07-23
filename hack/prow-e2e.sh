#!/usr/bin/env bash
# prow-e2e.sh - Run the COSI E2E suite end-to-end inside a Prow job.
#
# Flow:
#   1. bring up a kind cluster with loop-backed raw devices for Rook/Ceph
#   2. clone cosi-driver-sample and let it stand up its Rook/Ceph RGW backend
#   3. build + sideload + deploy the COSI controller
#   4. build + sideload the COSI sidecar
#   5. build + sideload + deploy the sample driver and S3 credentials
#   6. run the chainsaw e2e suite
set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
ROOT="${SCRIPT_DIR}/.."

# shellcheck source=hack/kind-helpers.sh disable=SC1091
source "${SCRIPT_DIR}/kind-helpers.sh"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-cosi-e2e}"
KIND_LOOP_DEVICE_DIR="${KIND_LOOP_DEVICE_DIR:-/tmp/cosi-kind-${KIND_CLUSTER_NAME}}"
CONTROLLER_TAG="${CONTROLLER_TAG:-cosi-controller:latest}"
SIDECAR_TAG="${SIDECAR_TAG:-cosi-provisioner-sidecar:latest}"
SAMPLE_DRIVER_IMAGE="${SAMPLE_DRIVER_IMAGE:-cosi-driver-sample:latest}"
SAMPLE_DRIVER_REPO="${SAMPLE_DRIVER_REPO:-https://github.com/shanduur/cosi-driver-sample.git}"
SAMPLE_DRIVER_BRANCH="${SAMPLE_DRIVER_BRANCH:-ci/loop-device-osds}"
SAMPLE_DRIVER_PATH="${SAMPLE_DRIVER_PATH:-${ROOT}/../cosi-driver-sample}"
CREDS_FILE="${CREDS_FILE:-${ROOT}/.cache/s3-credentials.yaml}"
export KIND_CLUSTER_NAME KIND_LOOP_DEVICE_DIR CONTROLLER_TAG SIDECAR_TAG SAMPLE_DRIVER_IMAGE SAMPLE_DRIVER_REPO SAMPLE_DRIVER_BRANCH SAMPLE_DRIVER_PATH CREDS_FILE

dump_debug() {
  local exit_code=$?
  set +o errexit
  set +o nounset
  set +o pipefail
  set +o xtrace

  echo "===== COSI E2E debug dump (exit=${exit_code}) ====="

  echo "===== Kubernetes nodes ====="
  kubectl get nodes -o wide || true

  echo "===== Rook/Ceph resources ====="
  kubectl -n rook-ceph get pods -o wide || true
  kubectl -n rook-ceph get cephcluster,cephobjectstore,cephobjectstoreuser -o wide || true
  kubectl -n rook-ceph get events --sort-by=.lastTimestamp || true

  echo "===== Rook/Ceph descriptions ====="
  kubectl -n rook-ceph describe cephcluster my-cluster || true
  kubectl -n rook-ceph describe cephobjectstore my-store || true
  kubectl -n rook-ceph describe pods -l app=rook-ceph-rgw || true

  echo "===== Rook/Ceph logs ====="
  kubectl -n rook-ceph logs deploy/rook-ceph-operator --tail=500 || true
  kubectl -n rook-ceph logs -l app=rook-ceph-rgw --all-containers --tail=300 || true
  kubectl -n rook-ceph logs -l app=rook-ceph-osd --all-containers --tail=300 || true
  kubectl -n rook-ceph logs -l app=rook-ceph-mon --all-containers --tail=300 || true

  echo "===== Rook/Ceph OSD prepare logs ====="
  for pod in $(kubectl -n rook-ceph get pods -o name 2>/dev/null | grep 'pod/rook-ceph-osd-prepare-' || true); do
    echo "===== ${pod} ====="
    kubectl -n rook-ceph logs "${pod}" --all-containers --tail=500 || true
  done

  echo "===== Rook/Ceph detect-version job logs ====="
  for pod in $(kubectl -n rook-ceph get pods -o name 2>/dev/null | grep 'pod/ceph-object-controller-detect-version' || true); do
    echo "===== ${pod} ====="
    kubectl -n rook-ceph logs "${pod}" --all-containers --tail=200 || true
  done

  echo "===== kind loop devices ====="
  for node in $(kind get nodes --name "${KIND_CLUSTER_NAME}" 2>/dev/null); do
    echo "===== ${node}: losetup ====="
    docker exec "${node}" losetup -a || true
    echo "===== ${node}: lsblk ====="
    docker exec "${node}" lsblk -f || true
    echo "===== ${node}: /dev/loop* ====="
    docker exec "${node}" sh -c 'ls -l /dev/loop* 2>/dev/null' || true
  done

  echo "===== end COSI E2E debug dump ====="
  exit "${exit_code}"
}

cleanup() {
  if [[ "${DELETE_KIND_CLUSTER:-true}" == "true" ]]; then
    kind delete cluster --name "${KIND_CLUSTER_NAME}" || true
  fi
  for disk in "${KIND_LOOP_DEVICE_DIR}"/ceph-osd-*.img; do
    [ -e "${disk}" ] || continue
    loop_device=$(run_as_root losetup -j "${disk}" | cut -d: -f1 || true)
    if [[ -n "${loop_device}" ]]; then
      run_as_root losetup -d "${loop_device}" || true
    fi
  done
}
trap dump_debug ERR
trap cleanup EXIT

"${SCRIPT_DIR}/setup-kind.sh"

if [ ! -d "${SAMPLE_DRIVER_PATH}" ]; then
  git clone --depth 1 --branch "${SAMPLE_DRIVER_BRANCH}" \
    "${SAMPLE_DRIVER_REPO}" \
    "${SAMPLE_DRIVER_PATH}"
fi
mkdir -p "$(dirname "${CREDS_FILE}")"
OUT_CREDS_FILE="${CREDS_FILE}" LOOP_DEVICE_OSDS=true \
  LOOP_DEVICE_BACKING_DIR="${KIND_LOOP_DEVICE_DIR}" \
  "${SAMPLE_DRIVER_PATH}/hack/setup-s3-backend.sh"

make -C "${ROOT}" build.controller
load_image_into_cluster "${CONTROLLER_TAG}"
make -C "${ROOT}" deploy

make -C "${ROOT}" build.sidecar
load_image_into_cluster "${SIDECAR_TAG}"

"${SCRIPT_DIR}/setup-sample-driver.sh"

make -C "${ROOT}" test-e2e
