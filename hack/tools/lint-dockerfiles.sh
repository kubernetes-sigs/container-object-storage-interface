#!/usr/bin/env bash
set -o errexit
set -o nounset

HADOLINT_VERSION=${1:-latest}

# Exclude vendor dependencies to avoid linting other Dockerfiles and prevent irrelevant warnings
FILES=$(find . -path './vendor' -prune -o -name Dockerfile -print)

for file in $FILES; do
  echo "Linting Dockerfile: ${file}"
  ${DOCKER:-docker} run --rm -i ghcr.io/hadolint/hadolint:"${HADOLINT_VERSION}" hadolint --failure-threshold warning - < "${file}"
done
