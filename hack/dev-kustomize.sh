#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o xtrace

#
# generate a kustomization file for local development use
# use the Makefile's CONTROLLER_TAG as the image used for dev deployment
#

# store generated file(s) in cache dir not checked into git
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT="$SCRIPT_DIR"/..
CACHE_DIR="$ROOT"/.cache
mkdir -p "$CACHE_DIR"

# copy root kustomization.yaml file to cache dir, and...
# replace './' in root kustomization.yaml with '../' for usage from cache dir
DEV_KUSTOMIZE_FILE="$CACHE_DIR"/kustomization.yaml
sed -e 's|\./|../|g' "$ROOT"/kustomization.yaml > "$DEV_KUSTOMIZE_FILE"

# process Makefile's CONTROLLER_TAG into name and tag components
#   e.g., CONTROLLER_TAG="localhost:5000/cosi-controller:latest"
NEW_NAME="${CONTROLLER_TAG%:*}" # e.g., "localhost:5000/cosi-controller"
NEW_TAG="${CONTROLLER_TAG##*:}" # e.g., "latest"

# replace the default controller image with one for local dev, and force
# imagePullPolicy: IfNotPresent so a sideloaded/locally-built image is
# preferred over a registry pull (kubelet defaults to Always for :latest).
cat <<EOF >> "$DEV_KUSTOMIZE_FILE"

images:
  - name: gcr.io/k8s-staging-sig-storage/objectstorage-controller
    newName: "$NEW_NAME"
    newTag: "$NEW_TAG"

patches:
  - target:
      kind: Deployment
      name: controller
    patch: |-
      - op: add
        path: /spec/template/spec/containers/0/imagePullPolicy
        value: IfNotPresent
EOF
