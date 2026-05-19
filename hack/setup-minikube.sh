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

# setup-minikube.sh - Bring up a minikube cluster suitable for the COSI E2E
# suite in the project's Prow CI environment.
#
# Scope: not maintained for local-dev use. Local contributors are expected
# to provision their own cluster (any flavour, any driver) following the
# guidance in docs/src/developing/core.md.

set -o errexit
set -o nounset
set -o pipefail

MINIKUBE_CPUS="${MINIKUBE_CPUS:-4}"
MINIKUBE_MEMORY="${MINIKUBE_MEMORY:-6g}"
# Extra raw disks attached to the minikube VM, consumed by the S3 backend.
MINIKUBE_EXTRA_DISKS="${MINIKUBE_EXTRA_DISKS:-3}"
MINIKUBE_DRIVER="${MINIKUBE_DRIVER:-kvm2}"
MINIKUBE_CONTAINER_RUNTIME="${MINIKUBE_CONTAINER_RUNTIME:-containerd}"

minikube start \
  --cpus="${MINIKUBE_CPUS}" \
  --memory="${MINIKUBE_MEMORY}" \
  --extra-disks="${MINIKUBE_EXTRA_DISKS}" \
  --driver="${MINIKUBE_DRIVER}" \
  --container-runtime="${MINIKUBE_CONTAINER_RUNTIME}"
