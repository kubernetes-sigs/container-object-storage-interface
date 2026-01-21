/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package test contains utilities for unit testing.
package test

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
)

// Dependencies contains bootstrapped COSI unit test dependencies.
type Dependencies struct {
	ContextWithLogger context.Context // context containing the test logger
	Client            client.Client   // test client containing bootstrapped objects
	Logger            logr.Logger     // the test logger, for convenience
}

// MustBootstrap bootstraps new COSI unit test dependencies, or panics if an error occurs.
func MustBootstrap(t *testing.T, initialClientObjects ...client.Object) *Dependencies {
	logger := testr.NewWithOptions(t, testr.Options{
		Verbosity: 1,
	})
	contextWithLogger := logr.NewContext(context.Background(), logger)

	scheme := runtime.NewScheme()
	err := cosiapi.AddToScheme(scheme)
	if err != nil {
		panic(err)
	}
	err = corev1.AddToScheme(scheme) // required for bucketaccess tests with secrets
	if err != nil {
		panic(err)
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(initialClientObjects...).
		WithStatusSubresource(
			&cosiapi.Bucket{},
			&cosiapi.BucketClaim{},
			&cosiapi.BucketAccess{},
		).
		Build()

	return &Dependencies{
		ContextWithLogger: contextWithLogger,
		Client:            client,
		Logger:            logger,
	}
}
