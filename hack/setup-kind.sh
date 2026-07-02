#!/usr/bin/env bash
# setup-kind.sh - Bring up a kind cluster suitable for the COSI E2E suite.
# The cluster gets host loop-backed raw devices exposed into worker containers
# so Rook/Ceph can discover OSD devices without host KVM or cloud VMs.

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

KIND_VERSION="${KIND_VERSION:-v0.32.0}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-cosi-e2e}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.36.1}"
KIND_LOOP_DEVICES="${KIND_LOOP_DEVICES:-3}"
KIND_LOOP_DEVICE_SIZE="${KIND_LOOP_DEVICE_SIZE:-10G}"
KIND_LOOP_DEVICE_DIR="${KIND_LOOP_DEVICE_DIR:-/tmp/cosi-kind-${KIND_CLUSTER_NAME}}"
KIND_CONFIG="${KIND_CONFIG:-}"
KIND_WORKERS="${KIND_WORKERS:-2}"

if ! command -v kind >/dev/null 2>&1; then
  GOBIN="${GOBIN:-$(go env GOPATH)/bin}"
  export GOBIN
  export PATH="${GOBIN}:${PATH}"
  go install "sigs.k8s.io/kind@${KIND_VERSION}"
fi

mkdir -p "${KIND_LOOP_DEVICE_DIR}"
for i in $(seq 1 "${KIND_LOOP_DEVICES}"); do
  disk="${KIND_LOOP_DEVICE_DIR}/ceph-osd-${i}.img"
  if [ ! -f "${disk}" ]; then
    truncate -s "${KIND_LOOP_DEVICE_SIZE}" "${disk}"
  fi
  if ! losetup -j "${disk}" | grep -q "${disk}"; then
    losetup -f --show "${disk}"
  fi
done

if ! kind get clusters | grep -qx "${KIND_CLUSTER_NAME}"; then
  if [[ -z "${KIND_CONFIG}" ]]; then
    KIND_CONFIG="$(mktemp)"
    {
      cat <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
EOF
      for _ in $(seq 1 "${KIND_WORKERS}"); do
        cat <<EOF
- role: worker
  extraMounts:
  - hostPath: /dev
    containerPath: /dev
EOF
      done
    } >"${KIND_CONFIG}"
  fi
  kind create cluster --name "${KIND_CLUSTER_NAME}" --config "${KIND_CONFIG}" --image "${KIND_NODE_IMAGE}"
fi

for node in $(kind get nodes --name "${KIND_CLUSTER_NAME}" | grep worker); do
  for loop_device in $(losetup -j "${KIND_LOOP_DEVICE_DIR}"/ceph-osd-*.img | cut -d: -f1); do
    loop_number="${loop_device#/dev/loop}"
    docker exec "${node}" mknod "${loop_device}" b 7 "${loop_number}" 2>/dev/null || true
  done
  docker exec "${node}" losetup -a || true
done

kubectl cluster-info --context "kind-${KIND_CLUSTER_NAME}"
