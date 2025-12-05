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
// +kubebuilder:validation:XValidation:message="parameters map is immutable",rule="has(oldSelf.parameters) == has(self.parameters)"
// +kubebuilder:validation:XValidation:message="protocols list is immutable",rule="has(oldSelf.protocols) == has(self.protocols)"
// +kubebuilder:validation:XValidation:message="existingBucketID is immutable",rule="has(oldSelf.existingBucketID) == has(self.existingBucketID)"
type BucketSpec struct {
	// driverName is the name of the driver that fulfills requests for this Bucket.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:message="driverName is immutable",rule="self == oldSelf"
	DriverName string `json:"driverName"`

	// deletionPolicy determines whether a Bucket should be deleted when its bound BucketClaim is
	// deleted. This is mutable to allow Admins to change the policy after creation.
	// Possible values:
	//  - Retain: keep both the Bucket object and the backend bucket
	//  - Delete: delete both the Bucket object and the backend bucket
	// +required
	DeletionPolicy BucketDeletionPolicy `json:"deletionPolicy"`

	// parameters is an opaque map of driver-specific configuration items passed to the driver that
	// fulfills requests for this Bucket.
	// +optional
	// +kubebuilder:validation:XValidation:message="parameters map is immutable",rule="self == oldSelf"
	Parameters map[string]string `json:"parameters,omitempty"`

	// protocols lists object store protocols that the provisioned Bucket must support.
	// If specified, COSI will verify that each item is advertised as supported by the driver.
	// +optional
	// +listType=set
	// +kubebuilder:validation:XValidation:message="protocols list is immutable",rule="self == oldSelf"
	Protocols []ObjectProtocol `json:"protocols,omitempty"`

	// bucketClaim references the BucketClaim that resulted in the creation of this Bucket.
	// For statically-provisioned buckets, set the namespace and name of the BucketClaim that is
	// allowed to bind to this Bucket.
	// +required
	BucketClaimRef BucketClaimReference `json:"bucketClaim"`

	// existingBucketID is the unique identifier for an existing backend bucket known to the driver.
	// Use driver documentation to determine how to set this value.
	// This field is used only for Bucket static provisioning.
	// This field will be empty when the Bucket is dynamically provisioned from a BucketClaim.
	// +optional
	// +kubebuilder:validation:XValidation:message="existingBucketID is immutable",rule="self == oldSelf"
	ExistingBucketID string `json:"existingBucketID,omitempty"`
}

// BucketClaimReference is a reference to a BucketClaim object.
// +kubebuilder:validation:XValidation:message="namespace is immutable once set",rule="!has(oldSelf.namespace) || has(self.namespace)"
// +kubebuilder:validation:XValidation:message="uid is immutable once set",rule="!has(oldSelf.uid) || has(self.uid)"
type BucketClaimReference struct {
	// name is the name of the BucketClaim being referenced.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="name is immutable",rule="self == oldSelf"
	Name string `json:"name"`

	// namespace is the namespace of the BucketClaim being referenced.
	// If empty, the Kubernetes 'default' namespace is assumed.
	// namespace is immutable except to update '' to 'default'.
	// +optional
	// +kubebuilder:validation:MinLength=0
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="namespace is immutable",rule="(oldSelf == '' && self == 'default') || self == oldSelf"
	Namespace string `json:"namespace"`

	// uid is the UID of the BucketClaim being referenced.
	// +optional
	// +kubebuilder:validation:XValidation:message="uid is immutable once set",rule="oldSelf == '' || self == oldSelf"
	UID types.UID `json:"uid"`
}

// BucketStatus defines the observed state of Bucket.
// +kubebuilder:validation:XValidation:message="bucketID is immutable once set",rule="!has(oldSelf.bucketID) || has(self.bucketID)"
// +kubebuilder:validation:XValidation:message="protocols is immutable once set",rule="!has(oldSelf.protocols) || has(self.protocols)"
type BucketStatus struct {
	// readyToUse indicates that the bucket is ready for consumption by workloads.
	ReadyToUse bool `json:"readyToUse"`

	// bucketID is the unique identifier for the backend bucket known to the driver.
	// +optional
	// +kubebuilder:validation:XValidation:message="boundBucketName is immutable once set",rule="oldSelf == '' || self == oldSelf"
	BucketID string `json:"bucketID"`

	// protocols is the set of protocols the Bucket reports to support. BucketAccesses can request
	// access to this BucketClaim using any of the protocols reported here.
	// +optional
	// +listType=set
	Protocols []ObjectProtocol `json:"protocols"`

	// BucketInfo reported by the driver, rendered in the COSI_<PROTOCOL>_<KEY> format used for the
	// BucketAccess Secret. e.g., COSI_S3_ENDPOINT, COSI_AZURE_STORAGE_ACCOUNT.
	// This should not contain any sensitive information.
	// +optional
	BucketInfo map[string]string `json:"bucketInfo,omitempty"`

	// Error holds the most recent error message, with a timestamp.
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
	Spec BucketSpec `json:"spec"`

	// status defines the observed state of Bucket
	// +optional
	Status BucketStatus `json:"status,omitempty,omitzero"`
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
