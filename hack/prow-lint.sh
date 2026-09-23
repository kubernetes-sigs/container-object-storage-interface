#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o xtrace

# The CI image ships whatever Go k/k master uses, and GOTOOLCHAIN=auto never downgrades to
# go.mod's toolchain. Use that toolchain like a local build would, still upgrading for tools
# that require a newer Go.
GOTOOLCHAIN="$(sed -n "s/^toolchain //p" go.mod)+auto"
export GOTOOLCHAIN

echo "GOMAXPROCS: $GOMAXPROCS" # debug prow CPU limit to ensure job not being throttled

GOLANGCI_LINT_RUN_OPTS=""
GOLANGCI_LINT_RUN_OPTS="$GOLANGCI_LINT_RUN_OPTS --verbose" # debug linter timing and mem usage
GOLANGCI_LINT_RUN_OPTS="$GOLANGCI_LINT_RUN_OPTS --concurrency=$GOMAXPROCS" # golangci-lint seems to do a bad job obeying GOMAXPROCS
export GOLANGCI_LINT_RUN_OPTS

make lint
