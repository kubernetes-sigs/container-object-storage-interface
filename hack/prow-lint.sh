#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o xtrace

echo "GOMAXPROCS: $GOMAXPROCS" # debug prow CPU limit to ensure job not being throttled

GOLANGCI_LINT_RUN_OPTS=""
GOLANGCI_LINT_RUN_OPTS="$GOLANGCI_LINT_RUN_OPTS --verbose" # debug linter timing and mem usage
GOLANGCI_LINT_RUN_OPTS="$GOLANGCI_LINT_RUN_OPTS --concurrency=$GOMAXPROCS" # golangci-lint seems to do a bad job obeying GOMAXPROCS
export GOLANGCI_LINT_RUN_OPTS

make lint
