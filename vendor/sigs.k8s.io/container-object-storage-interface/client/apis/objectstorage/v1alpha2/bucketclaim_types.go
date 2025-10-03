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
)

// BucketClaimSpec defines the desired state of BucketClaim
// +kubebuilder:validation:ExactlyOneOf=bucketClassName;existingBucketName
type BucketClaimSpec struct {
	// bucketClassName selects the BucketClass for provisioning the BucketClaim.
	// This field is used only for BucketClaim dynamic provisioning.
	// If unspecified, existingBucketName must be specified for binding to an existing Bucket.
	// +optional
	// +kubebuilder:validation:XValidation:message="bucketClassName is immutable",rule="self == oldSelf"
	BucketClassName string `json:"bucketClassName,omitempty"`

	// protocols lists object storage protocols that the provisioned Bucket must support.
	// If specified, COSI will verify that each item is advertised as supported by the driver.
	// +optional
	// +kubebuilder:validation:XValidation:message="protocols list is immutable",rule="self == oldSelf"
	Protocols []ObjectProtocol `json:"protocols,omitempty"`

	// existingBucketName selects the name of an existing Bucket resource that this BucketClaim
	// should bind to.
	// This field is used only for BucketClaim static provisioning.
	// If unspecified, bucketClassName must be specified for dynamically provisioning a new bucket.
	// +optional
	// +kubebuilder:validation:XValidation:message="existingBucketName is immutable",rule="self == oldSelf"
	ExistingBucketName string `json:"existingBucketName,omitempty"`
}

// BucketClaimStatus defines the observed state of BucketClaim.
type BucketClaimStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the BucketClaim resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:metadata:annotations="api-approved.kubernetes.io=https://github.com/kubernetes/enhancements/tree/master/keps/sig-storage/1979-object-storage-support"

// BucketClaim is the Schema for the bucketclaims API
type BucketClaim struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of BucketClaim
	// +required
	Spec BucketClaimSpec `json:"spec"`

	// status defines the observed state of BucketClaim
	// +optional
	Status BucketClaimStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true
// +kubebuilder:metadata:annotations="api-approved.kubernetes.io=https://github.com/kubernetes/enhancements/tree/master/keps/sig-storage/1979-object-storage-support"

// BucketClaimList contains a list of BucketClaim
type BucketClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BucketClaim `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BucketClaim{}, &BucketClaimList{})
}
