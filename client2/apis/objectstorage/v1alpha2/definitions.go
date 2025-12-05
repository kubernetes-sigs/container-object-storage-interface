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

package v1alpha2

// Finalizers
const (
	// ProtectionFinalizer is applied to a COSI resource object to protect it from deletion while
	// COSI processes deletion of the object's intermediate and backend resources.
	ProtectionFinalizer = `objectstorage.k8s.io/protection`
)

// Annotations
const (
	// HasBucketAccessReferencesAnnotation : This annotation is applied by the COSI Controller to a
	// BucketClaim when a BucketAccess that references the BucketClaim is created. The annotation
	// remains for as long as any BucketAccess references the BucketClaim. Once all BucketAccesses
	// that reference the BucketClaim are deleted, the annotation is removed.
	HasBucketAccessReferencesAnnotation = `objectstorage.k8s.io/has-bucketaccess-references`

	// SidecarCleanupFinishedAnnotation : This annotation is applied by a COSI Sidecar to a managed
	// BucketAccess when the resources is being deleted. The Sidecar calls the Driver to perform
	// backend deletion actions and then hands off final deletion cleanup to the COSI Controller
	// by setting this annotation on the resource.
	SidecarCleanupFinishedAnnotation = `objectstorage.k8s.io/sidecar-cleanup-finished`

	// ControllerManagementOverrideAnnotation : This annotation can be applied to a resource by the
	// COSI Controller in order to reclaim management of the resource temporarily when it would
	// otherwise be managed by a COSI Sidecar. This is intended for scenarios where a bug in
	// provisioning needs to be rectified by a newer version of the COSI Controller. Once the bug is
	// resolved, the annotation should be removed to allow normal Sidecar handoff to occur.
	ControllerManagementOverrideAnnotation = `objectstorage.k8s.io/controller-management-override`
)

// Sidecar RPC definitions
const (
	// RpcEndpointDefault is the default RPC endpoint unix socket location.
	RpcEndpointDefault = "unix:///var/lib/cosi/cosi.sock"

	// RpcEndpointEnvVarName is the name of the environment variable that is expected to hold the
	// RPC endpoint unix socket location. If unspecified, RpcEndpointDefault should be used.
	RpcEndpointEnvVarName = "COSI_ENDPOINT"
)
