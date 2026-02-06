#!/usr/bin/env bash
set -euo pipefail

BIN="$1"
VERSION="$2"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$ARCH" in
  x86_64) ARCH="x86_64" ;;
  arm64)  ARCH="aarch64" ;;
esac

TMPDIR="$(mktemp -d)"
TARBALL="shellcheck-${VERSION}.${OS}.${ARCH}.tar.xz"

curl -sSL \
  "https://github.com/koalaman/shellcheck/releases/download/${VERSION}/${TARBALL}" \
  -o "${TMPDIR}/${TARBALL}"

tar -xJ -C "${TMPDIR}" -f "${TMPDIR}/${TARBALL}"

install -m 0755 \
  "${TMPDIR}/shellcheck-${VERSION}/shellcheck" \
  "${BIN}-${VERSION}"

ln -sf "${BIN}-${VERSION}" "${BIN}"

rm -rf "${TMPDIR}"
