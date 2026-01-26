#!/usr/bin/env bash
set -o errexit
set -o nounset
# set -o xtrace

function file_ends_with_newline() {
    [[ $(tail -c1 "$1" | wc -l) -gt 0 ]]
}

any_failure=false

for file in $(git ls-files -- . ':(exclude)vendor/*' ) ; do
  if ! file_ends_with_newline "$file" ; then
    echo "FAIL: $file -- no newline at end of file"
    any_failure=true
  fi
done

if $any_failure ; then
  exit 1
fi
