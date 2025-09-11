#!/usr/bin/env bash

set -eu

jq ".[1]"

SHA=$(git rev-parse HEAD)
VERSION="Commit: <a target="_blank" href=\"https:\/\/github.com\/kubernetes-sigs\/container-object-storage-interface\/tree\/${SHA}\"><code>${SHA}<\/code><\/a>"
BRANCH=$(git rev-parse --abbrev-ref HEAD)

if [ -n "$BRANCH" ]; then
  VERSION="Branch: <a target="_blank" href=\"https:\/\/github.com\/kubernetes-sigs\/container-object-storage-interface\/tree\/${BRANCH}\"><code>${BRANCH}<\/code><\/a> ${VERSION}"
fi

sed "s/VERSION-PLACEHOLDER/${VERSION}/" theme/index-template.hbs > theme/index.hbs
