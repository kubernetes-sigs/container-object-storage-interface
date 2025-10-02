#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o xtrace

generate_cmd="make prebuild"
$generate_cmd

git="git --no-pager"

# if a developer is having trouble with any generator on their local system, git diff output
# can give a fallback mechanism for applying necessary changes
$git diff

# if there are generated changes, status output (with human-readable newlines) could be helpful
status_cmd="$git status --porcelain"
$status_cmd

{ set +o xtrace; } 2>/dev/null # helpful "pretty" output below is more legible without xtrace output

s="$($status_cmd)"
if [[ -n "$s" ]]; then
  cat <<EOF >/dev/stderr

==============================================================================

    Generated file changes are missing. Run '$generate_cmd'"

==============================================================================

EOF

  exit 1
fi
