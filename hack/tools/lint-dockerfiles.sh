#!/usr/bin/env bash

set -eu

HADOLINT_VERSION=${1:-latest}

FILES=$(find . -path './vendor' -prune -o -name Dockerfile -print)

IFS=$'\n'

for file in $FILES; do
  echo "Linting: ${file}"
  docker run --rm -i ghcr.io/hadolint/hadolint:"${HADOLINT_VERSION}" hadolint --failure-threshold warning - < "${file}"
done

unset IFS
