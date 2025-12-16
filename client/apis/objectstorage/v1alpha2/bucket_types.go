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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// BucketDeletionPolicy configures COSI's behavior when a Bucket resource is deleted.
// +enum
// +kubebuilder:validation:Enum:=Retain;Delete
type BucketDeletionPolicy string

const (
	// BucketDeletionPolicyRetain configures COSI to keep the Bucket object as well as the backend
	// bucket when a Bucket resource is deleted.
	BucketDeletionPolicyRetain BucketDeletionPolicy = "Retain"

	// BucketDeletionPolicyDelete configures COSI to delete the Bucket object as well as the backend
	// bucket when a Bucket resource is deleted.
	BucketDeletionPolicyDelete BucketDeletionPolicy = "Delete"
)

// BucketSpec defines the desired state of Bucket
// +kubebuilder:validation:XValidation:message="parameters map cannot be added or removed after creation",rule="has(oldSelf.parameters) == has(self.parameters)"
// +kubebuilder:validation:XValidation:message="protocols list cannot be added or removed after creation",rule="has(oldSelf.protocols) == has(self.protocols)"
// +kubebuilder:validation:XValidation:message="existingBucketID cannot be added or removed after creation",rule="has(oldSelf.existingBucketID) == has(self.existingBucketID)"
type BucketSpec struct {
	// driverName is the name of the driver that fulfills requests for this Bucket.
	// See driver documentation to determine the correct value to set.
	// Must be 63 characters or less, beginning and ending with an alphanumeric character
	// ([a-z0-9A-Z]) with dashes (-), dots (.), and alphanumerics between.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9]([a-zA-Z0-9\-\.]{0,61}[a-zA-Z0-9])?$`
	// +kubebuilder:validation:XValidation:message="driverName is immutable",rule="self == oldSelf"
	DriverName string `json:"driverName,omitempty"`

	// deletionPolicy determines whether a Bucket should be deleted when its bound BucketClaim is
	// deleted. This is mutable to allow Admins to change the policy after creation.
	// Possible values:
	//  - Retain: keep both the Bucket object and the backend bucket
	//  - Delete: delete both the Bucket object and the backend bucket
	// +required
	DeletionPolicy BucketDeletionPolicy `json:"deletionPolicy,omitempty"`

	// parameters is an opaque map of driver-specific configuration items passed to the driver that
	// fulfills requests for this Bucket.
	// See driver documentation to determine supported parameters and their effects.
	// A maximum of 512 parameters are allowed.
	// +optional
	// +kubebuilder:validation:MinProperties=1
	// +kubebuilder:validation:MaxProperties=512
	// +kubebuilder:validation:XValidation:message="parameters map is immutable",rule="self == oldSelf"
	Parameters map[string]string `json:"parameters,omitempty"`

	// protocols lists object store protocols that the provisioned Bucket must support.
	// If specified, COSI will verify that each item is advertised as supported by the driver.
	// See driver documentation to determine supported protocols.
	// Possible values: 'S3', 'Azure', 'GCS'.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	// +kubebuilder:validation:XValidation:message="protocols list is immutable",rule="self == oldSelf"
	Protocols []ObjectProtocol `json:"protocols,omitempty"`

	// bucketClaimRef references the BucketClaim that resulted in the creation of this Bucket.
	// For statically-provisioned buckets, set the namespace and name of the BucketClaim that is
	// allowed to bind to this Bucket; UID may be left unset if desired and will be updated by COSI.
	// +required
	BucketClaimRef BucketClaimReference `json:"bucketClaimRef,omitzero"`

	// existingBucketID is the unique identifier for an existing backend bucket known to the driver.
	// Use driver documentation to determine the correct value to set.
	// This field is used only for static Bucket provisioning.
	// This field will be empty when the Bucket is dynamically provisioned from a BucketClaim.
	// Must be at most 2048 characters and consist only of alphanumeric characters ([a-z0-9A-Z]),
	// dashes (-), dots (.), underscores (_), and forward slash (/).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9/._-]+$`
	// +kubebuilder:validation:XValidation:message="existingBucketID is immutable",rule="self == oldSelf"
	ExistingBucketID string `json:"existingBucketID,omitempty"`
}

// BucketClaimReference is a reference to a BucketClaim object.
// +kubebuilder:validation:XValidation:message="namespace cannot be removed once set",rule="!has(oldSelf.namespace) || has(self.namespace)"
// +kubebuilder:validation:XValidation:message="uid cannot be removed once set",rule="!has(oldSelf.uid) || has(self.uid)"
type BucketClaimReference struct {
	// name is the name of the BucketClaim being referenced.
	// Must be a valid Kubernetes resource name: at most 253 characters, consisting only of
	// lower-case alphanumeric characters, hyphens, and periods, starting and ending with an
	// alphanumeric character.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="name must be a valid resource name",rule="!format.dns1123Subdomain().validate(self).hasValue()"
	// +kubebuilder:validation:XValidation:message="name is immutable",rule="self == oldSelf"
	Name string `json:"name,omitempty"`

	// namespace is the namespace of the BucketClaim being referenced.
	// Must be a valid Kubernetes Namespace name: at most 63 characters, consisting only of
	// lower-case alphanumeric characters and hyphens, starting and ending with alphanumerics.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:XValidation:message="namespace must be a valid namespace name",rule="!format.dns1123Label().validate(self).hasValue()"
	// +kubebuilder:validation:XValidation:message="namespace is immutable",rule="self == oldSelf"
	Namespace string `json:"namespace,omitempty"`

	// uid is the UID of the BucketClaim being referenced.
	// Must be a valid Kubernetes UID: RFC 4122 form with lowercase hexadecimal characters
	// (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
	// +optional
	// +kubebuilder:validation:MinLength=36
	// +kubebuilder:validation:MaxLength=36
	// +kubebuilder:validation:Type=string
	// +kubebuilder:validation:Pattern=`^[0-9a-f]{8}-([0-9a-f]{4}\-){3}[0-9a-f]{12}$`
	// +kubebuilder:validation:XValidation:message="uid is immutable once set",rule="oldSelf == '' || self == oldSelf"
	UID types.UID `json:"uid,omitempty"`
}

// BucketStatus defines the observed state of Bucket.
// +kubebuilder:validation:XValidation:message="bucketID cannot be removed once set",rule="!has(oldSelf.bucketID) || has(self.bucketID)"
// +kubebuilder:validation:XValidation:message="protocols cannot be removed once set",rule="!has(oldSelf.protocols) || has(self.protocols)"
type BucketStatus struct {
	// readyToUse indicates that the bucket is ready for consumption by workloads.
	// +required
	ReadyToUse *bool `json:"readyToUse,omitempty"`

	// bucketID is the unique identifier for the backend bucket known to the driver.
	// Must be at most 2048 characters and consist only of alphanumeric characters ([a-z0-9A-Z]),
	// dashes (-), dots (.), underscores (_), and forward slash (/).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9/._-]+$`
	// +kubebuilder:validation:XValidation:message="boundBucketName is immutable once set",rule="self == oldSelf"
	BucketID string `json:"bucketID,omitempty"`

	// protocols is the set of protocols the Bucket reports to support. BucketAccesses can request
	// access to this Bucket using any of the protocols reported here.
	// Possible values: 'S3', 'Azure', 'GCS'.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	Protocols []ObjectProtocol `json:"protocols,omitempty"`

	// bucketInfo contains info about the bucket reported by the driver, rendered in the same
	// COSI_<PROTOCOL>_<KEY> format used for the BucketAccess Secret.
	// e.g., COSI_S3_ENDPOINT, COSI_AZURE_STORAGE_ACCOUNT.
	// This should not contain any sensitive information.
	// +optional
	// +kubebuilder:validation:MinProperties=1
	// +kubebuilder:validation:MaxProperties=128
	BucketInfo map[string]string `json:"bucketInfo,omitempty"`

	// error holds the most recent error message, with a timestamp.
	// This is cleared when provisioning is successful.
	// +optional
	Error *TimestampedError `json:"error,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:metadata:annotations="api-approved.kubernetes.io=unapproved, experimental v1alpha2 changes"

// Bucket is the Schema for the buckets API
type Bucket struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of Bucket
	// +required
	Spec BucketSpec `json:"spec,omitzero"`

	// status defines the observed state of Bucket
	// +optional
	Status BucketStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// BucketList contains a list of Bucket
type BucketList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Bucket `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Bucket{}, &BucketList{})
}
