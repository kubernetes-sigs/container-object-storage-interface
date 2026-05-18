#!/usr/bin/env bash
# prow-e2e.sh - run the COSI E2E suite end-to-end against a fresh minikube.
#
# The flow matches the local dev workflow documented in
# docs/src/developing/core.md so that CI and contributors exercise the same
# targets:
#   1. start a minikube cluster (with extra disks for Rook OSDs)
#   2. deploy Rook/Ceph and generate S3 credentials
#   3. build + sideload + deploy the COSI controller
#   4. build + sideload the sample driver + sidecar, then run the chainsaw
#      suite (which deploys the sample driver and exercises the API)
#
# Images are sideloaded into the minikube node via 'minikube image load' - no
# container registry is required. The Makefile derives a content-aware
# IMAGE_TAG from the git state, so each run uses a fresh tag and the load is
# never a no-op.
set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
ROOT="${SCRIPT_DIR}/.."

make -C "${ROOT}" cluster
make -C "${ROOT}" deploy-rook
make -C "${ROOT}" deploy
make -C "${ROOT}" test-e2e
