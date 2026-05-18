#!/usr/bin/env bash

set -eu

MINIKUBE="${1}"
MINIKUBE_VERSION="${2}"

# If it exists, do not redownload
if [ -f "${MINIKUBE}-${MINIKUBE_VERSION}" ]; then
	exit 0
fi

# Detect OS and architecture
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
Linux) OS_TYPE="linux" ;;
Darwin) OS_TYPE="darwin" ;;
*)
	echo "Unsupported OS: $OS"
	exit 1
	;;
esac

case "$ARCH" in
x86_64) ARCH_TYPE="amd64" ;;
arm64 | aarch64) ARCH_TYPE="arm64" ;;
*)
	echo "Unsupported architecture: $ARCH"
	exit 1
	;;
esac

URL="https://github.com/kubernetes/minikube/releases/download/${MINIKUBE_VERSION}/minikube-${OS_TYPE}-${ARCH_TYPE}"
echo "Downloading: $URL"

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

curl -sSfL "$URL" -o "${TMPDIR}/minikube"

chmod +x "${TMPDIR}/minikube"
mv "${TMPDIR}/minikube" "${MINIKUBE}-${MINIKUBE_VERSION}"
ln -sf "${MINIKUBE}-${MINIKUBE_VERSION}" "${MINIKUBE}"

echo "minikube ${MINIKUBE_VERSION} installed successfully!"
