#!/usr/bin/env bash
# prow-e2e.sh - Run the COSI E2E suite end-to-end inside a Prow job.
#
# Scope: this script targets the project's Prow CI environment only. It is
# not maintained as a portable local-dev entry point. Local contributors
# should follow docs/src/developing/core.md instead.
#
# Flow:
#   1. bring up a minikube cluster with extra raw disks for the S3 backend
#   2. clone the cosi-driver-sample repo and let it stand up its own S3
#      backend; capture the generated credentials values file
#   3. build + sideload + deploy the COSI controller
#   4. build + sideload the sidecar
#   5. build + sideload + deploy the sample driver, creating the admin
#      and access Secrets from the backend credentials
#   6. run the chainsaw e2e suite (purely functional - no deploys)
set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
ROOT="${SCRIPT_DIR}/.."

CONTROLLER_TAG="${CONTROLLER_TAG:-cosi-controller:latest}"
SIDECAR_TAG="${SIDECAR_TAG:-cosi-provisioner-sidecar:latest}"
SAMPLE_DRIVER_IMAGE="${SAMPLE_DRIVER_IMAGE:-cosi-driver-sample:latest}"
SAMPLE_DRIVER_REPO="${SAMPLE_DRIVER_REPO:-https://github.com/kubernetes-sigs/cosi-driver-sample.git}"
SAMPLE_DRIVER_BRANCH="${SAMPLE_DRIVER_BRANCH:-main}"
SAMPLE_DRIVER_PATH="${SAMPLE_DRIVER_PATH:-${ROOT}/../cosi-driver-sample}"
CREDS_FILE="${CREDS_FILE:-${ROOT}/.cache/s3-credentials.yaml}"
export CONTROLLER_TAG SIDECAR_TAG SAMPLE_DRIVER_IMAGE SAMPLE_DRIVER_REPO SAMPLE_DRIVER_BRANCH SAMPLE_DRIVER_PATH CREDS_FILE

# 0. install minikube (if not already present in the Prow image).
if ! command -v minikube &>/dev/null; then
  "${SCRIPT_DIR}/install-minikube.sh"
fi

# 1. cluster.
"${SCRIPT_DIR}/setup-minikube.sh"

# 2. clone sample driver repo + provision its S3 backend.
if [ ! -d "${SAMPLE_DRIVER_PATH}" ]; then
  git clone --depth 1 --branch "${SAMPLE_DRIVER_BRANCH}" \
    "${SAMPLE_DRIVER_REPO}" \
    "${SAMPLE_DRIVER_PATH}"
fi
mkdir -p "$(dirname "${CREDS_FILE}")"
OUT_CREDS_FILE="${CREDS_FILE}" "${SAMPLE_DRIVER_PATH}/hack/setup-s3-backend.sh"

# 3. controller: build, sideload, deploy.
make -C "${ROOT}" build.controller
minikube image load "${CONTROLLER_TAG}"
make -C "${ROOT}" deploy

# 4. sidecar: build, sideload.
make -C "${ROOT}" build.sidecar
minikube image load "${SIDECAR_TAG}"

# 5. sample driver: build, sideload, deploy (kustomize + image patch +
# admin/access Secrets).
"${SCRIPT_DIR}/setup-sample-driver.sh"

# 6. chainsaw e2e suite. The suite is purely functional and assumes all
# the above is already in place.
make -C "${ROOT}" test-e2e
