#!/usr/bin/env bash

set -eu

TOOLBIN="${1}"
SPELL_LINT="${2}"
SPELL_LINT_VERSION="${3}"

# If it exists, do not redownload
if [ -f "${SPELL_LINT}-${SPELL_LINT_VERSION}" ]; then
  exit 0
fi

INSTALLER="https://raw.githubusercontent.com/golangci/misspell/master/install-misspell.sh"

curl -sSfL "${INSTALLER}" | sh -s -- -b "${TOOLBIN}" "${SPELL_LINT_VERSION}"

mv "${TOOLBIN}/misspell" "${SPELL_LINT}-${SPELL_LINT_VERSION}"
ln -sf "${SPELL_LINT}-${SPELL_LINT_VERSION}" "${SPELL_LINT}"
