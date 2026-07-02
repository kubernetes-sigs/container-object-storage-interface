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
x86_64)
    ARCH=amd64
    QEMU_BIN="qemu-system-x86_64"
    APT_QEMU="qemu-system-x86 qemu-utils"
    DNF_QEMU="qemu-system-x86 qemu-img"
    APK_QEMU="qemu-system-x86_64 qemu-img"
    ;;
aarch64)
    ARCH=arm64
    QEMU_BIN="qemu-system-aarch64"
    APT_QEMU="qemu-system-arm qemu-efi-aarch64 qemu-utils"
    DNF_QEMU="qemu-system-aarch64 qemu-img edk2-aarch64"
    APK_QEMU="qemu-system-aarch64 qemu-img"
    ;;
*)
    echo "unsupported architecture: ${ARCH}" >&2
    exit 1
    ;;
esac

COMMON_APT="dnsmasq iptables iproute2"
COMMON_DNF="dnsmasq iptables iproute"
COMMON_APK="dnsmasq iptables iproute2"

if command -v apt >/dev/null 2>&1; then
    apt update
    apt install -y $APT_QEMU $COMMON_APT
elif command -v dnf >/dev/null 2>&1; then
    dnf install -y $DNF_QEMU $COMMON_DNF
elif command -v yum >/dev/null 2>&1; then
    yum install -y $DNF_QEMU $COMMON_DNF
elif command -v apk >/dev/null 2>&1; then
    apk update
    apk add $APK_QEMU $COMMON_APK
else
    echo "Unsupported package manager. Need apt, dnf, yum, or apk." >&2
    exit 1
fi

echo
echo "Checking QEMU:"
if command -v "$QEMU_BIN" >/dev/null 2>&1; then
    "$QEMU_BIN" --version | head -1
else
    echo "Expected QEMU binary not found: $QEMU_BIN" >&2
    exit 1
fi

echo
if [ -e /dev/kvm ]; then
    echo "KVM acceleration available: /dev/kvm"
else
    echo "Warning: /dev/kvm not found. QEMU may be slow."
fi

MINIKUBE_VERSION="${MINIKUBE_VERSION:-latest}"

curl --fail --remote-name --location "https://github.com/kubernetes/minikube/releases/${MINIKUBE_VERSION}/download/minikube-linux-${ARCH}"
install "minikube-linux-${ARCH}" /usr/local/bin/minikube && rm "minikube-linux-${ARCH}"
