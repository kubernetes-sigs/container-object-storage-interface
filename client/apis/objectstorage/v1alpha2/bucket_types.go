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
	// +kubebuilder:validation:XValidation:message="protocols list is immutable",rule="self == oldSelf"
	Protocols []ObjectProtocol `json:"protocols,omitempty"`

	// bucketClaim references the BucketClaim that resulted in the creation of this Bucket.
	// For statically-provisioned buckets, set the namespace and name of the BucketClaim that is
	// allowed to bind to this Bucket.
	// +required
	BucketClaimRef BucketClaimReference `json:"bucketClaim"`
}

// BucketClaimReference is a reference to a BucketClaim object.
type BucketClaimReference struct {
	// name is the name of the BucketClaim being referenced.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:message="driverName is immutable",rule="self == oldSelf"
	Name string `json:"name"`

	// namespace is the namespace of the BucketClaim being referenced.
	// If empty, the Kubernetes 'default' namespace is assumed.
	// namespace is immutable except to update '' to 'default'.
	// +optional
	// +kubebuilder:validation:MinLength=0
	// +kubebuilder:validation:XValidation:message="driverName is immutable",rule="(oldSelf == '' && self == 'default') || self == oldSelf"
	Namespace string `json:"namespace"`

	// uid is the UID of the BucketClaim being referenced.
	// Once set, the UID is immutable.
	// +optional
	// +kubebuilder:validation:XValidation:message="driverName is immutable",rule="oldSelf == '' || self == oldSelf"
	UID types.UID `json:"uid"`
}

// BucketStatus defines the observed state of Bucket.
type BucketStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:metadata:annotations="api-approved.kubernetes.io=https://github.com/kubernetes/enhancements/tree/master/keps/sig-storage/1979-object-storage-support"

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
