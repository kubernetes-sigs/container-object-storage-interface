#!/bin/bash

# Copyright 2018 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

CLIENT_ROOT=$(unset CDPATH && cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)

# because kubernetes/code-generator is used as a bash script instead of a go binary, it has to be
# downloaded. kubernetes/code-generator project recommends vendor dir for this
# this unfortunately requires downloading unnecessary other deps, but client has few
go mod vendor

source "${CLIENT_ROOT}/vendor/k8s.io/code-generator/kube_codegen.sh"

kube::codegen::gen_helpers \
    --boilerplate "${CLIENT_ROOT}/hack/boilerplate.go.txt" \
    "${CLIENT_ROOT}/apis"

kube::codegen::gen_client \
    --output-dir "${CLIENT_ROOT}" \
    --output-pkg "sigs.k8s.io/container-object-storage-interface-api/client" \
    --boilerplate "${CLIENT_ROOT}/hack/boilerplate.go.txt" \
    --with-watch \
    "${CLIENT_ROOT}/apis"

# an out-of date vendor dir can cause 'go vet' to fail, so delete it after we're done
rm -rf "$CLIENT_ROOT"/vendor/
