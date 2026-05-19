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

# install-minikube.sh - Install minikube in the Prow CI environment.

set -o errexit
set -o nounset
set -o pipefail

ARCH="$(uname -m)"

case "${ARCH}" in
    x86_64) ARCH=amd64 ;;
    aarch64) ARCH=arm64 ;;
    *) echo "unsupported architecture: ${ARCH}" >&2; exit 1 ;;
esac

MINIKUBE_VERSION="${MINIKUBE_VERSION:-latest}"

curl --fail --remote-name --location "https://github.com/kubernetes/minikube/releases/${MINIKUBE_VERSION}/download/minikube-linux-${ARCH}"
install "minikube-linux-${ARCH}" /usr/local/bin/minikube && rm "minikube-linux-${ARCH}"
