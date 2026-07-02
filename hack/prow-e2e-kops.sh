#!/usr/bin/env bash
# Copyright 2026 The Kubernetes Authors.
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

# Run COSI E2E in Prow on a temporary kops/GCE cluster. This follows the
# kubetest2-kops pattern used by other Kubernetes CI jobs: the Prow pod only
# orchestrates; the test cluster and raw disks live on cloud VMs.

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "${REPO_ROOT}"
cd ../..
WORKSPACE=$(pwd)
cd "${REPO_ROOT}"

BINDIR="${WORKSPACE}/bin"
export PATH="${BINDIR}:${PATH}"
mkdir -p "${BINDIR}"
KUBETEST2_ARGS=()

cleanup() {
  if [[ "${DELETE_CLUSTER:-}" == "true" ]]; then
    kubetest2 kops "${KUBETEST2_ARGS[@]}" --down || echo "kubetest2 down failed"
  fi
  if [[ "${CLEANUP_BOSKOS:-}" == "true" ]]; then
    cleanup_boskos
  fi
}
trap cleanup EXIT

SCRIPT_NAME=$(basename "$0" .sh)
if [[ -z "${CLUSTER_NAME:-}" ]]; then
  CLUSTER_NAME="${SCRIPT_NAME}-${RANDOM}.k8s.local"
fi
echo "CLUSTER_NAME=${CLUSTER_NAME}"

WORKDIR="${WORKDIR:-${WORKSPACE}/clusters/${CLUSTER_NAME}}"
mkdir -p "${WORKDIR}"

# Ensure we have a GCP project. In Prow this comes from Boskos unless the job
# was invoked with GCP_PROJECT already set.
# shellcheck source=hack/boskos.sh
source "${REPO_ROOT}/hack/boskos.sh"
if [[ -z "${GCP_PROJECT:-}" ]]; then
  echo "GCP_PROJECT not set, acquiring project from boskos"
  acquire_project
  GCP_PROJECT="${PROJECT}"
  CLEANUP_BOSKOS="true"
fi
export GCP_PROJECT

gcloud config set project "${GCP_PROJECT}"
echo "GCP_PROJECT=${GCP_PROJECT}"

if [[ -z "${SSH_PRIVATE_KEY:-}" ]]; then
  SSH_PRIVATE_KEY="${WORKDIR}/google_compute_engine"
  gcloud compute --project="${GCP_PROJECT}" config-ssh --ssh-key-file="${SSH_PRIVATE_KEY}"
  export KUBE_SSH_USER="${KUBE_SSH_USER:-ubuntu}"
fi
export SSH_PRIVATE_KEY
echo "SSH_PRIVATE_KEY=${SSH_PRIVATE_KEY}"

KOPS_REPO_PATH="${KOPS_REPO_PATH:-}"
if [[ -z "${KOPS_REPO_PATH}" ]]; then
  if [[ -d "${WORKSPACE}/k8s.io/kops" ]]; then
    KOPS_REPO_PATH="${WORKSPACE}/k8s.io/kops"
  elif [[ -d "${WORKSPACE}/kops" ]]; then
    KOPS_REPO_PATH="${WORKSPACE}/kops"
  fi
fi
if [[ -n "${KOPS_REPO_PATH}" ]]; then
  make -C "${KOPS_REPO_PATH}" test-e2e-install GOBIN="${BINDIR}"
fi

if [[ -z "${K8S_VERSION:-}" ]]; then
  K8S_VERSION="$(curl -sL https://dl.k8s.io/release/stable.txt)"
fi
export K8S_VERSION

if [[ -z "${KOPS_BASE_URL:-}" ]]; then
  KOPS_BASE_URL="$(curl -s https://storage.googleapis.com/k8s-staging-kops/kops/releases/markers/master/latest-ci-updown-green.txt)"
fi
export KOPS_BASE_URL

KOPS_BIN="${BINDIR}/kops"
wget -qO "${KOPS_BIN}" "${KOPS_BASE_URL}/$(go env GOOS)/$(go env GOARCH)/kops"
chmod +x "${KOPS_BIN}"
export KOPS_FEATURE_FLAGS="${KOPS_FEATURE_FLAGS:-}"

CLOUD_PROVIDER="gce"
GCP_LOCATION="${GCP_LOCATION:-us-central1}"

if [[ -z "${KOPS_STATE_STORE:-}" ]]; then
  KOPS_STATE_STORE="gs://kops-state-${GCP_PROJECT}"
  gsutil ls -p "${GCP_PROJECT}" "${KOPS_STATE_STORE}" || gsutil mb -p "${GCP_PROJECT}" -l "${GCP_LOCATION}" "${KOPS_STATE_STORE}"
  gsutil ubla set off "${KOPS_STATE_STORE}"
  SA=$(gcloud config list --format 'value(core.account)')
  gsutil iam ch "serviceAccount:${SA}:admin" "${KOPS_STATE_STORE}"
fi
export KOPS_STATE_STORE
echo "KOPS_STATE_STORE=${KOPS_STATE_STORE}"

IMAGE_REPO="${IMAGE_REPO:-gcr.io/${GCP_PROJECT}}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)-$(date +%Y%m%dT%H%M%S)}"
CONTROLLER_TAG="${CONTROLLER_TAG:-${IMAGE_REPO}/cosi-controller:${IMAGE_TAG}}"
SIDECAR_TAG="${SIDECAR_TAG:-${IMAGE_REPO}/cosi-provisioner-sidecar:${IMAGE_TAG}}"
SAMPLE_DRIVER_IMAGE="${SAMPLE_DRIVER_IMAGE:-${IMAGE_REPO}/cosi-driver-sample:${IMAGE_TAG}}"
SAMPLE_DRIVER_REPO="${SAMPLE_DRIVER_REPO:-https://github.com/kubernetes-sigs/cosi-driver-sample.git}"
SAMPLE_DRIVER_BRANCH="${SAMPLE_DRIVER_BRANCH:-main}"
SAMPLE_DRIVER_PATH="${SAMPLE_DRIVER_PATH:-${REPO_ROOT}/../cosi-driver-sample}"
CREDS_FILE="${CREDS_FILE:-${REPO_ROOT}/.cache/s3-credentials.yaml}"
export CONTROLLER_TAG SIDECAR_TAG SAMPLE_DRIVER_IMAGE SAMPLE_DRIVER_REPO SAMPLE_DRIVER_BRANCH SAMPLE_DRIVER_PATH CREDS_FILE

gcloud auth configure-docker --quiet

make -C "${REPO_ROOT}" build.controller CONTROLLER_TAG="${CONTROLLER_TAG}"
docker push "${CONTROLLER_TAG}"

make -C "${REPO_ROOT}" build.sidecar SIDECAR_TAG="${SIDECAR_TAG}"
docker push "${SIDECAR_TAG}"

if [ ! -d "${SAMPLE_DRIVER_PATH}" ]; then
  git clone --depth 1 --branch "${SAMPLE_DRIVER_BRANCH}" \
    "${SAMPLE_DRIVER_REPO}" "${SAMPLE_DRIVER_PATH}"
fi
make -C "${SAMPLE_DRIVER_PATH}" build SAMPLE_DRIVER_TAG="${SAMPLE_DRIVER_IMAGE}"
docker push "${SAMPLE_DRIVER_IMAGE}"

ADMIN_ACCESS="${ADMIN_ACCESS:-0.0.0.0/0}"

create_args="--networking gce --node-count=${KOPS_NODE_COUNT:-1} --node-size=${KOPS_NODE_SIZE:-n2-standard-4}"
if [[ -n "${ZONES:-}" ]]; then
  create_args="${create_args} --zones=${ZONES}"
fi
create_args="${create_args} --gce-service-account=default"

KUBETEST2_ARGS=(
  -v=2
  --cloud-provider="${CLOUD_PROVIDER}"
  --cluster-name="${CLUSTER_NAME}"
  --kops-binary-path="${KOPS_BIN}"
  --admin-access="${ADMIN_ACCESS}"
  --gcp-project="${GCP_PROJECT}"
  --ssh-private-key="${SSH_PRIVATE_KEY}"
)
if [[ -n "${KOPS_FEATURE_FLAGS:-}" ]]; then
  KUBETEST2_ARGS+=(--env="KOPS_FEATURE_FLAGS=${KOPS_FEATURE_FLAGS}")
fi
if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  KUBETEST2_ARGS+=(--env="GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}")
fi

DELETE_CLUSTER="${DELETE_CLUSTER:-true}"

kubetest2 kops "${KUBETEST2_ARGS[@]}" \
  --up \
  --kubernetes-version="${K8S_VERSION}" \
  --create-args="${create_args}" \
  --control-plane-size="${KOPS_CONTROL_PLANE_SIZE:-n2-standard-2}" \
  --template-path="${KOPS_TEMPLATE:-}"

# Add an extra raw persistent disk to each node instance group for Rook/Ceph.
# kops applies this by creating a new instance template and rolling the nodes.
NODE_IGS=$("${KOPS_BIN}" get ig --name "${CLUSTER_NAME}" -o json | jq -r '.items[] | select(.spec.role == "Node") | .metadata.name')
for ig in ${NODE_IGS}; do
  "${KOPS_BIN}" get ig "${ig}" --name "${CLUSTER_NAME}" -o json >"${WORKDIR}/${ig}.json"
  python3 - "${WORKDIR}/${ig}.json" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as f:
    obj = json.load(f)
volumes = obj.setdefault("spec", {}).setdefault("volumes", [])
if not any(v.get("device") == "/dev/sdb" for v in volumes):
    volumes.append({
        "device": "/dev/sdb",
        "size": int(__import__("os").environ.get("KOPS_NODE_ADDITIONAL_VOLUME_SIZE", "20")),
        "type": __import__("os").environ.get("KOPS_NODE_ADDITIONAL_VOLUME_TYPE", "pd-standard"),
    })
with open(path, "w", encoding="utf-8") as f:
    json.dump(obj, f, indent=2)
PY
  "${KOPS_BIN}" replace -f "${WORKDIR}/${ig}.json" --name "${CLUSTER_NAME}"
done
"${KOPS_BIN}" update cluster "${CLUSTER_NAME}" --yes
"${KOPS_BIN}" rolling-update cluster "${CLUSTER_NAME}" --yes

kubectl wait --for=condition=Ready nodes --all --timeout=10m
mkdir -p "$(dirname "${CREDS_FILE}")"
OUT_CREDS_FILE="${CREDS_FILE}" "${SAMPLE_DRIVER_PATH}/hack/setup-s3-backend.sh"

make -C "${REPO_ROOT}" deploy CONTROLLER_TAG="${CONTROLLER_TAG}"
"${REPO_ROOT}/hack/setup-sample-driver-kops.sh"
make -C "${REPO_ROOT}" test-e2e

if [[ "${DELETE_CLUSTER}" == "true" ]]; then
  kubetest2 kops "${KUBETEST2_ARGS[@]}" --down
  DELETE_CLUSTER=false
fi
