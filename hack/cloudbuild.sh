#!/usr/bin/env bash
set -o errexit
set -o nounset

# with nounset, these will fail if necessary vars are missing
echo "GIT_TAG: ${GIT_TAG}"
echo "PULL_BASE_REF: ${PULL_BASE_REF}"
echo "PLATFORM: ${PLATFORM}"

# debug the rest of the script in case of image/CI build issues
set -o xtrace

REPO="registry.k8s.io/k8s-staging-sig-storage"

CONTROLLER_IMAGE="${REPO}/objectstorage-controller"
SIDECAR_IMAGE="${REPO}/objectstorage-sidecar"

# args to 'make build'
export DOCKER="/buildx-entrypoint" # available in gcr.io/k8s-testimages/gcb-docker-gcloud image
export BUILD_ARGS="--push"
export PLATFORM
export SIDECAR_TAG="${SIDECAR_IMAGE}:${GIT_TAG}"
export CONTROLLER_TAG="${CONTROLLER_IMAGE}:${GIT_TAG}"

# build in parallel
make --jobs --output-sync build

# add latest tag to just-built images
gcloud container images add-tag "${CONTROLLER_TAG}" "${CONTROLLER_IMAGE}:latest"
gcloud container images add-tag "${SIDECAR_TAG}" "${SIDECAR_IMAGE}:latest"

# PULL_BASE_REF is 'controller/TAG' for a controller release
if [[ "${PULL_BASE_REF}" == controller/* ]]; then
  echo " ! ! ! this is a tagged controller release ! ! !"
  TAG="${PULL_BASE_REF#controller/*}"
  gcloud container images add-tag "${CONTROLLER_TAG}" "${CONTROLLER_IMAGE}:${TAG}"
fi

# PULL_BASE_REF is 'sidecar/TAG' for a controller release
if [[ "${PULL_BASE_REF}" == sidecar/* ]]; then
  echo " ! ! ! this is a tagged sidecar release ! ! !"
  TAG="${PULL_BASE_REF#sidecar/*}"
  gcloud container images add-tag "${SIDECAR_TAG}" "${SIDECAR_IMAGE}:${TAG}"
fi

# else, PULL_BASE_REF is a branch name (e.g., master, release-0.2) or a tag (e.g., client/v0.2.0, proto/v0.2.0)
