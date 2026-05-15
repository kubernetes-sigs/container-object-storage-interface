#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o xtrace

echo "GOMAXPROCS: $GOMAXPROCS" # debug prow CPU limit to ensure job not being throttled

go version

make test
