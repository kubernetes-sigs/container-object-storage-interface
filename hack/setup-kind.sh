#!/usr/bin/env bash
# setup-kind.sh - Bring up a kind cluster for the COSI E2E CI.
#
# Cluster shape and node prep follow rook/rook PR 17822: a single-node kind
# cluster that bind-mounts the host's /dev, /var/lib/rook and /run/udev into
# the node so raw block devices, LVM, and the udev database created on the
# host runner are directly visible to the Rook/Ceph pods inside the node.
#
# Loop-backed block devices for OSDs are created on the runner host by this
# script; HostToContainer propagation on /dev makes them appear inside the
# node automatically (no more per-worker mknod dance).
#
# Not maintained as a local-dev entry point; local contributors should
# follow docs/src/developing/core.md.

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

KIND_VERSION="${KIND_VERSION:-v0.32.0}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-cosi-e2e}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.36.1}"
KIND_CONFIG="${KIND_CONFIG:-${SCRIPT_DIR}/kind/config.yaml}"
KIND_LOOP_DEVICES="${KIND_LOOP_DEVICES:-3}"
KIND_LOOP_DEVICE_SIZE="${KIND_LOOP_DEVICE_SIZE:-10G}"
KIND_LOOP_DEVICE_DIR="${KIND_LOOP_DEVICE_DIR:-/tmp/cosi-kind-${KIND_CLUSTER_NAME}}"

# shellcheck source=hack/kind-helpers.sh disable=SC1091
source "${SCRIPT_DIR}/kind-helpers.sh"

if ! command -v kind >/dev/null 2>&1; then
  GOBIN="${GOBIN:-$(go env GOPATH)/bin}"
  export GOBIN
  export PATH="${GOBIN}:${PATH}"
  go install "sigs.k8s.io/kind@${KIND_VERSION}"
fi

# /var/lib/rook is bind-mounted into the node by kind config; make sure it
# exists on the host before the cluster comes up so the mount does not fail.
run_as_root mkdir -p /var/lib/rook /run/udev
ensure_shared_mount /dev
ensure_shared_mount /var/lib/rook
ensure_shared_mount /run/udev

mkdir -p "${KIND_LOOP_DEVICE_DIR}"
for i in $(seq 1 "${KIND_LOOP_DEVICES}"); do
  disk="${KIND_LOOP_DEVICE_DIR}/ceph-osd-${i}.img"
  if ! run_as_root losetup -j "${disk}" | grep -q "${disk}"; then
    truncate -s 0 "${disk}"
    truncate -s "${KIND_LOOP_DEVICE_SIZE}" "${disk}"
    run_as_root losetup -f --show "${disk}"
  fi
done

if ! kind get clusters | grep -qx "${KIND_CLUSTER_NAME}"; then
  kind create cluster \
    --name "${KIND_CLUSTER_NAME}" \
    --config "${KIND_CONFIG}" \
    --image "${KIND_NODE_IMAGE}" \
    --wait 300s
fi

prepare_kind_node
add_host_routes_to_cluster

kubectl cluster-info --context "kind-${KIND_CLUSTER_NAME}"
