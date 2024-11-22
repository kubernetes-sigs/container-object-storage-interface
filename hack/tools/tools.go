//go:build never
// +build never

package tools

import (
	_ "github.com/golangci/golangci-lint/cmd/golangci-lint"
	_ "github.com/kyverno/chainsaw"
	_ "github.com/tilt-dev/ctlptl/cmd/ctlptl"
	_ "sigs.k8s.io/kind"
	_ "sigs.k8s.io/kustomize/kustomize/v5"
)
