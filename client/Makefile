# # Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: generate test

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: generate
generate: crds codegen ## Run all code generation/modification tools.

##@ Generators

.PHONY: crds
crds: ## Generate CustomResourceDefinitions.
	go run -v sigs.k8s.io/controller-tools/cmd/controller-gen crd paths="./apis/objectstorage/..."

.PHONY:
codegen: ## Generate deepcopy, client, informer, and lister implementations.
	./hack/update-codegen.sh
