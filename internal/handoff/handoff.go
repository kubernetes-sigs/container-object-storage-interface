/*
Copyright 2025 The Kubernetes Authors.

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

// Package handoff defines logic needed for handing off control of resources between Controller and
// Sidecar.
package handoff

import (
	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
)

// BucketAccessManagedBySidecar returns true if a BucketAccess should be managed by the Sidecar.
// A false return value indicates that it should be managed by the Controller instead.
//
// In order for COSI Controller and any given Sidecar to work well together, they should avoid
// managing the same BucketAccess resource at the same time. Instances where a resource has no
// manager MUST be avoided without exception.
//
// Version skew between Controller and Sidecar should be assumed. In order for version skew issues
// to be minimized, avoid updating this logic unless it is absolutely critical. If updates are made,
// be sure to carefully consider all version skew cases below. Minimize dual-ownership scenarios,
// and avoid no-owner scenarios.
//
//  1. Sidecar version low, Controller version low
//  2. Sidecar version low, Controller version high
//  3. Sidecar version high, Controller version low
//  4. Sidecar version high, Controller version high
func BucketAccessManagedBySidecar(ba *cosiapi.BucketAccess) bool {
	// Allow a future-compatible mechanism by which the Controller can override the normal
	// BucketAccess management handoff logic in order to resolve a bug.
	// Instances where this is utilized should be infrequent -- ideally, never used.
	if _, ok := ba.Annotations[cosiapi.ControllerManagementOverrideAnnotation]; ok {
		return false
	}

	// During provisioning, there are several status fields that the Controller needs to set before
	// the Sidecar can provision an access. However, tying this function's logic to ALL of the
	// status items could make long-term Controller-Sidecar handoff logic fragile. More logic means
	// more risk of unmanaged resources and more difficulty reasoning about how changes will impact
	// ownership during version skew. Minimize risk by relying on a single determining status field.
	if ba.Status.DriverName == "" {
		return false
	}

	// During deletion, as long as the access was handed off to the Sidecar at some point, the
	// Sidecar must first clean up the backend bucket, then hand back final deletion to the
	// Controller by setting an annotation.
	if !ba.DeletionTimestamp.IsZero() {
		_, ok := ba.Annotations[cosiapi.SidecarCleanupFinishedAnnotation]
		return !ok // ok means sidecar is done cleaning up
	}

	return true
}
