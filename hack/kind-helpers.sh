#!/usr/bin/env bash
# kind-helpers.sh - shared helpers for the kind-based COSI E2E CI.
#
# Ported from rook/rook PR 17822 (tests/scripts/github-action-helper.sh):
# same techniques applied to a single-node kind cluster hosting the COSI
# controller, sidecar, sample driver and its Rook/Ceph RGW backend.
#
# Source this file; do not execute it. Each function is a leaf action.
#
# shellcheck shell=bash

ensure_kind_is_available() {
  # The kind-based CI helpers drive the cluster through the kind CLI; fail
  # clearly if it is absent.
  command -v kind >/dev/null 2>&1 || {
    echo "the 'kind' command is required but was not found" >&2
    exit 1
  }
}

load_image_into_cluster() {
  # Under kind, a locally built image lives only in the host docker daemon
  # and must be explicitly imported into each cluster node's containerd
  # before any pod can run it.
  #
  # Import through the node's own ctr rather than `kind load docker-image`:
  # kind's loader parses the node's containerd config, and the kind version
  # bundled in some environments cannot read the config version shipped by
  # current kindest/node images ("unknown containerd config version: 4").
  # Piping `docker save` into the node's ctr sidesteps that mismatch.
  ensure_kind_is_available
  local image="${1?image is required}"
  local cluster="${KIND_CLUSTER_NAME:-kind}"
  if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
    echo "load_image_into_cluster: no kind cluster '${cluster}'" >&2
    exit 1
  fi
  local node
  for node in $(kind get nodes --name "${cluster}"); do
    docker save "${image}" | docker exec -i "${node}" ctr --namespace=k8s.io images import -
  done
}

add_host_routes_to_cluster() {
  # kind runs the cluster inside a docker network, so route the Service and
  # pod CIDRs to the kind node so host-side tests (chainsaw's `kubectl exec`
  # into probe pods still goes through the API server, but any host-side S3
  # request would go via ClusterIP) can reach in-cluster services. Also
  # future-proofs against workflows that hit ClusterIPs directly from the
  # runner.
  ensure_kind_is_available
  local cluster="${KIND_CLUSTER_NAME:-kind}"
  local node ip
  node=$(kind get nodes --name "${cluster}" | grep control-plane | head -1)
  ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${node}")
  [ -n "${ip}" ] || {
    echo "ERROR: no kind node IP for routing" >&2
    return 1
  }
  # kind defaults: service subnet 10.96.0.0/16, pod subnet 10.244.0.0/16
  sudo ip route replace 10.96.0.0/16 via "${ip}"
  sudo ip route replace 10.244.0.0/16 via "${ip}"
  ip route show | grep -E '10\.(96|244)\.' || true # diagnostic print only
}

prepare_kind_node() {
  # Prepare each kind node for host-level operations Rook performs against
  # the node's host (remount /sys rw for krbd; install lvm2/cryptsetup for
  # LVM/encryption OSDs). Best-effort: a failure must not break raw-device
  # OSD jobs that need none of it.
  ensure_kind_is_available
  local cluster="${KIND_CLUSTER_NAME:-kind}"
  local node
  for node in $(kind get nodes --name "${cluster}"); do
    # kind mounts the node's /sys read-only, so CSI's `rbd map --device-type
    # krbd` fails with "rbd: sysfs write failed ... Read-only file system"
    # writing /sys/bus/rbd/add. The node is privileged, so remount /sys
    # read-write to let kernel RBD mapping work.
    docker exec "${node}" mount -o remount,rw /sys \
      || echo "WARNING: could not remount /sys rw in kind node ${node}" >&2
    # Rook provisions LVM- and encryption-backed OSDs by running lvm and
    # cryptsetup in the node's mount namespace; kindest/node images do not
    # ship those tools, so install them.
    for _ in 1 2 3; do
      if docker exec "${node}" sh -c "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y lvm2 cryptsetup"; then
        break
      fi
      echo "WARNING: could not install lvm2/cryptsetup into kind node ${node}; retrying" >&2
      sleep 5
    done
  done
}
