#!/usr/bin/env bash

set -eu

TOOLBIN="${1}"
GOLANGCI_LINT="${2}"
GOLANGCI_LINT_VERSION="${3}"

# If it exists, do not redownload
if [ -f "${GOLANGCI_LINT}-${GOLANGCI_LINT_VERSION}" ]; then
  exit 0
fi

INSTALLER="https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh"

curl -sSfL "${INSTALLER}" | sh -s -- -b "${TOOLBIN}" "${GOLANGCI_LINT_VERSION}"

mv "${TOOLBIN}/golangci-lint" "${GOLANGCI_LINT}-${GOLANGCI_LINT_VERSION}"
ln -sf "${GOLANGCI_LINT}-${GOLANGCI_LINT_VERSION}" "${GOLANGCI_LINT}"
