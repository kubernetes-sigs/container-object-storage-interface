#!/usr/bin/env bash
# Copyright 2020 The Kubernetes Authors.
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

# setup-rook.sh - Deploy Rook/Ceph with a single-node RGW to the local
# cluster and write S3 credentials to test/e2e/rook-credentials.yaml so that
# the Chainsaw E2E suite can consume them via --values.

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
ROOT="${SCRIPT_DIR}/.."
ROOK_DIR="${SCRIPT_DIR}/rook"

ROOK_VERSION="${ROOK_VERSION:-v1.19.4}"
ROOK_RAW_BASE="https://raw.githubusercontent.com/rook/rook/${ROOK_VERSION}/deploy/examples"

ROOK_NS="rook-ceph"

OBJECT_STORE="my-store"
OBJECT_USER="my-user"
USER_SECRET="rook-ceph-object-user-${OBJECT_STORE}-${OBJECT_USER}"

CRED_OUT="${ROOT}/test/e2e/rook-credentials.yaml"

# ---------------------------------------------------------------------------
# Step 1: Apply Rook CRDs, common RBAC, and operator (fetched from upstream
# release ${ROOK_VERSION}; override via ROOK_VERSION env var).
# ---------------------------------------------------------------------------
echo "==> Applying Rook CRDs (${ROOK_VERSION})"
kubectl apply --server-side -f "${ROOK_RAW_BASE}/crds.yaml"

echo "==> Applying Rook common resources (${ROOK_VERSION})"
kubectl apply -f "${ROOK_RAW_BASE}/common.yaml"

echo "==> Applying Ceph CSI operator (${ROOK_VERSION})"
kubectl apply --server-side -f "${ROOK_RAW_BASE}/csi-operator.yaml"

echo "==> Deploying Rook operator (${ROOK_VERSION})"
kubectl apply -f "${ROOK_RAW_BASE}/operator.yaml"

echo "==> Waiting for Rook operator to be available"
kubectl wait deployment rook-ceph-operator \
  -n "${ROOK_NS}" \
  --for=condition=Available \
  --timeout=120s

# ---------------------------------------------------------------------------
# Step 2: Create the CephCluster
# ---------------------------------------------------------------------------
echo "==> Creating CephCluster (single-node, OSDs on minikube extra disks)"
kubectl apply -f "${ROOK_DIR}/cluster-test.yaml"

echo "==> Waiting for CephCluster to reach Ready phase (up to 10 min)"
DEADLINE=$(( $(date +%s) + 600 ))
while true; do
  PHASE=$(kubectl get cephcluster my-cluster -n "${ROOK_NS}" \
    -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
  if [ "${PHASE}" = "Ready" ]; then
    echo "  CephCluster is Ready"
    break
  fi
  if [ "$(date +%s)" -gt "${DEADLINE}" ]; then
    echo "ERROR: CephCluster did not reach Ready within 10 minutes (phase=${PHASE})"
    kubectl describe cephcluster my-cluster -n "${ROOK_NS}" || true
    exit 1
  fi
  echo "  phase=${PHASE:-<pending>}, retrying in 15s..."
  sleep 15
done

# ---------------------------------------------------------------------------
# Step 3: Create the CephObjectStore and CephObjectStoreUser
# ---------------------------------------------------------------------------
echo "==> Creating CephObjectStore and CephObjectStoreUser"
kubectl apply -f "${ROOK_DIR}/object-test.yaml"

echo "==> Waiting for CephObjectStore RGW pods to be Ready (up to 5 min)"
DEADLINE=$(( $(date +%s) + 300 ))
while true; do
  if kubectl get pod -n "${ROOK_NS}" -l "app=rook-ceph-rgw,rgw=${OBJECT_STORE}" \
    -o jsonpath='{.items[*].status.containerStatuses[*].ready}' 2>/dev/null \
    | grep -qw true; then
    echo "  RGW pod ready"
    break
  fi
  if [ "$(date +%s)" -gt "${DEADLINE}" ]; then
    echo "ERROR: RGW pod did not become Ready within 5 minutes"
    kubectl get pods -n "${ROOK_NS}" -l "app=rook-ceph-rgw,rgw=${OBJECT_STORE}" || true
    exit 1
  fi
  echo "  waiting for RGW pod..."
  sleep 10
done

echo "==> Waiting for CephObjectStoreUser secret to appear (up to 2 min)"
DEADLINE=$(( $(date +%s) + 120 ))
while true; do
  if kubectl get secret "${USER_SECRET}" -n "${ROOK_NS}" >/dev/null 2>&1; then
    echo "  secret ${USER_SECRET} found"
    break
  fi
  if [ "$(date +%s)" -gt "${DEADLINE}" ]; then
    echo "ERROR: secret ${USER_SECRET} did not appear within 2 minutes"
    kubectl get cephobjectstoreuser "${OBJECT_USER}" -n "${ROOK_NS}" -o yaml || true
    exit 1
  fi
  echo "  waiting for secret..."
  sleep 5
done

# ---------------------------------------------------------------------------
# Step 4: Extract credentials and write rook-credentials.yaml
# ---------------------------------------------------------------------------
echo "==> Extracting S3 credentials from secret ${USER_SECRET}"

ACCESS_KEY=$(kubectl get secret "${USER_SECRET}" -n "${ROOK_NS}" \
  -o jsonpath='{.data.AccessKey}' | base64 -d)
SECRET_KEY=$(kubectl get secret "${USER_SECRET}" -n "${ROOK_NS}" \
  -o jsonpath='{.data.SecretKey}' | base64 -d)

# The RGW Service is created by Rook with the name rook-ceph-rgw-<store>.
ENDPOINT="http://rook-ceph-rgw-${OBJECT_STORE}.${ROOK_NS}.svc.cluster.local"

cat > "${CRED_OUT}" <<EOF
# Generated by hack/setup-rook.sh - DO NOT EDIT or commit.
# Re-run 'make deploy-rook' to refresh after cluster recreation.
s3Endpoint: "${ENDPOINT}"
# RGW accepts any region; AWS SDK v2 rejects empty strings, so use a placeholder.
s3Region: "us-east-1"
adminAccessKeyId: "${ACCESS_KEY}"
adminSecretAccessKey: "${SECRET_KEY}"
accessKeyId: "${ACCESS_KEY}"
accessSecretKey: "${SECRET_KEY}"
EOF

echo "==> Rook setup complete."
echo "    Credentials written to ${CRED_OUT}"
echo "    Endpoint : ${ENDPOINT}"
echo "    AccessKey: ${ACCESS_KEY}"
