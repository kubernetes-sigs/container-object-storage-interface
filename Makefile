# Copyright 2020 The Kubernetes Authors.
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

.DEFAULT_GOAL := help
.SUFFIXES: # remove legacy builtin suffixes to allow easier make debugging
SHELL = /usr/bin/env bash

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

# If GOARCH is not set in the env, find it
GOARCH ?= $(shell go env GOARCH)

#
# ==== ARGS =====
#

# Container build tool compatible with `docker` API
DOCKER ?= docker

# Tool compatible with `kubectl` API
KUBECTL ?= kubectl

# Platform for 'build'
PLATFORM ?= linux/$(GOARCH)

# Additional args for 'build'
BUILD_ARGS ?=

# Image tag for controller image build
CONTROLLER_TAG ?= cosi-controller:latest

# Image tag for sidecar image build
SIDECAR_TAG ?= cosi-provisioner-sidecar:latest

##@ Development

.PHONY: generate
generate: crd-ref-docs controller/Dockerfile sidecar/Dockerfile ## Generate files
	$(MAKE) -C client crds
	$(MAKE) -C proto generate
	$(CRD_REF_DOCS) \
		--config=./docs/.crd-ref-docs.yaml \
		--source-path=./client/apis \
		--renderer=markdown \
		--output-path=./docs/src/api/
%/Dockerfile: hack/Dockerfile.in hack/gen-dockerfile.sh
	hack/gen-dockerfile.sh $* > "$@"

.PHONY: codegen
codegen: codegen.client codegen.proto ## Generate code
codegen.%: FORCE
	$(MAKE) -C $* codegen

.PHONY: fmt
fmt: fmt.client fmt.controller fmt.sidecar ## Format code
fmt.%: FORCE
	cd $* && go fmt ./...

.PHONY: vet
vet: vet.client vet.controller vet.sidecar ## Vet code
vet.%: FORCE
	cd $* && go vet ./...

.PHONY: test
test: .test.proto test.client test.controller test.sidecar ## Run all unit tests including vet and fmt
test.%: fmt.% vet.% FORCE
	cd $* && go test ./...
.PHONY: .test.proto
.test.proto: # gRPC proto has a special unit test
	$(MAKE) -C proto check

.PHONY: test-e2e
test-e2e: chainsaw # Run e2e tests against the K8s cluster specified in ~/.kube/config. It requires both controller and driver deployed. If you need to create a cluster beforehand, consider using 'cluster' and 'deploy' targets.
	$(CHAINSAW) test --values ./test/e2e/values.yaml

.PHONY: lint
lint: golangci-lint.client golangci-lint.controller golangci-lint.sidecar spell-lint ## Run all linters (suggest `make -k`)
golangci-lint.%: golangci-lint
	cd $* && $(GOLANGCI_LINT) run $(GOLANGCI_LINT_RUN_OPTS) --config $(CURDIR)/.golangci.yaml --new
spell-lint:
	git ls-files | grep -v -e CHANGELOG -e go.mod -e go.sum -e vendor | xargs $(SPELL_LINT) -i "Creater,creater,ect" -error -o stderr

.PHONY: lint-fix
lint-fix: golangci-lint-fix.client golangci-lint-fix.controller golangci-lint-fix.sidecar ## Run all linters and perform fixes where possible (suggest `make -k`)
golangci-lint-fix.%: golangci-lint
	cd $* && $(GOLANGCI_LINT) run $(GOLANGCI_LINT_RUN_OPTS) --config $(CURDIR)/.golangci.yaml --new --fix

##@ Build

.PHONY: all .gen
.gen: generate codegen # can be done in parallel with 'make -j'
.NOTPARALLEL: all # codegen must be finished before fmt/vet
all: .gen fmt vet build ## Build all container images, plus their prerequisites (faster with 'make -j')

.PHONY: build
build: build.controller build.sidecar ## Build container images without prerequisites

.PHONY: build.controller build.sidecar
build.controller: controller/Dockerfile ## Build only the controller container image
	$(DOCKER) build --file controller/Dockerfile --platform $(PLATFORM) $(BUILD_ARGS) --tag $(CONTROLLER_TAG) .
build.sidecar: sidecar/Dockerfile ## Build only the sidecar container image
	$(DOCKER) build --file sidecar/Dockerfile --platform $(PLATFORM) $(BUILD_ARGS) --tag $(SIDECAR_TAG) .

.PHONY: build-docs
build-docs: generate mdbook
	cd docs; $(MDBOOK) build

MDBOOK_PORT ?= 3000

.PHONY: serve-docs
serve-docs: generate mdbook build-docs
	cd docs; $(MDBOOK) serve --port $(MDBOOK_PORT)

.PHONY: clean
clean: ## Clean build environment
	$(MAKE) -C proto clean

.PHONY: clobber
clobber: ## Clean build environment and cached tools
	$(MAKE) -C proto clobber
	rm -rf $(TOOLBIN)
	rm -rf $(CURDIR)/.cache

##@ Deployment

.PHONY: cluster
cluster: kind ctlptl ## Create Kind cluster and local registry
	PATH=$(TOOLBIN):$(PATH) $(CTLPTL) apply -f ctlptl.yaml

.PHONY: cluster-reset
cluster-reset: kind ctlptl ## Delete Kind cluster
	PATH=$(TOOLBIN):$(PATH) $(CTLPTL) delete -f ctlptl.yaml

.PHONY: deploy
deploy: kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config
	$(KUSTOMIZE) build . | $(KUBECTL) apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config
	$(KUSTOMIZE) build . | $(KUBECTL) delete --ignore-not-found=true -f -

#
# ===== Tools =====
#

# Location to install dependencies to
TOOLBIN ?= $(CURDIR)/.cache/tools
$(TOOLBIN):
	mkdir -p $(TOOLBIN)

# Tool Binaries
CHAINSAW      ?= $(TOOLBIN)/chainsaw
CRD_REF_DOCS  ?= $(TOOLBIN)/crd-ref-docs
CTLPTL        ?= $(TOOLBIN)/ctlptl
GOLANGCI_LINT ?= $(TOOLBIN)/golangci-lint
KIND          ?= $(TOOLBIN)/kind
KUSTOMIZE     ?= $(TOOLBIN)/kustomize
MDBOOK        ?= $(TOOLBIN)/mdbook
SPELL_LINT    ?= $(TOOLBIN)/spell-lint

# Tool Versions
CHAINSAW_VERSION      ?= v0.2.12
CRD_REF_DOCS_VERSION  ?= v0.1.0
CTLPTL_VERSION        ?= v0.8.39
GOLANGCI_LINT_VERSION ?= v1.64.7
KIND_VERSION          ?= v0.27.0
KUSTOMIZE_VERSION     ?= v5.6.0
MDBOOK_VERSION        ?= v0.4.47
SPELL_LINT_VERSION    ?= v0.6.0

.PHONY: chainsaw
chainsaw: $(CHAINSAW)-$(CHAINSAW_VERSION)
$(CHAINSAW)-$(CHAINSAW_VERSION): $(TOOLBIN)
	$(call go-install-tool,$(CHAINSAW),github.com/kyverno/chainsaw,$(CHAINSAW_VERSION))

.PHONY: crd-ref-docs
crd-ref-docs: $(CRD_REF_DOCS)-$(CRD_REF_DOCS_VERSION)
$(CRD_REF_DOCS)-$(CRD_REF_DOCS_VERSION): $(TOOLBIN)
	$(call go-install-tool,$(CRD_REF_DOCS),github.com/elastic/crd-ref-docs,$(CRD_REF_DOCS_VERSION))

.PHONY: ctlptl
ctlptl: $(CTLPTL)-$(CTLPTL_VERSION)
$(CTLPTL)-$(CTLPTL_VERSION): $(TOOLBIN)
	$(call go-install-tool,$(CTLPTL),github.com/tilt-dev/ctlptl/cmd/ctlptl,$(CTLPTL_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT)-$(GOLANGCI_LINT_VERSION)
$(GOLANGCI_LINT)-$(GOLANGCI_LINT_VERSION): $(TOOLBIN)
	./hack/tools/install-golangci-lint.sh $(TOOLBIN) $(GOLANGCI_LINT) $(GOLANGCI_LINT_VERSION)

.PHONY: kind
kind: $(KIND)-$(KIND_VERSION)
$(KIND)-$(KIND_VERSION): $(TOOLBIN)
	$(call go-install-tool,$(KIND),sigs.k8s.io/kind,$(KIND_VERSION))

.PHONY: kustomize
kustomize: $(KUSTOMIZE)-$(KUSTOMIZE_VERSION)
$(KUSTOMIZE)-$(KUSTOMIZE_VERSION): $(TOOLBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: mdbook
mdbook: $(MDBOOK)-$(MDBOOK_VERSION)
$(MDBOOK)-$(MDBOOK_VERSION): $(TOOLBIN)
	./hack/tools/install-mdbook.sh $(MDBOOK) $(MDBOOK_VERSION)

.PHONY: spell-lint
spell-lint: $(SPELL_LINT)-$(SPELL_LINT_VERSION)
$(SPELL_LINT)-$(SPELL_LINT_VERSION): $(TOOLBIN)
	./hack/tools/install-misspell-lint.sh $(TOOLBIN) $(SPELL_LINT) $(SPELL_LINT_VERSION)

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(TOOLBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef

.PHONY: FORCE # use this to force phony behavior for targets with pattern rules
FORCE:
