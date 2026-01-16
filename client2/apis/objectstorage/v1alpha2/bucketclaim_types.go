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
// +kubebuilder:validation:XValidation:message="bucketClassName is immutable",rule="has(oldSelf.bucketClassName) == has(self.bucketClassName)"
// +kubebuilder:validation:XValidation:message="existingBucketName is immutable",rule="has(oldSelf.existingBucketName) == has(self.existingBucketName)"
// +kubebuilder:validation:XValidation:message="protocols list is immutable",rule="has(oldSelf.protocols) == has(self.protocols)"
type BucketClaimSpec struct {
	// bucketClassName selects the BucketClass for provisioning the BucketClaim.
	// This field is used only for BucketClaim dynamic provisioning.
	// If unspecified, existingBucketName must be specified for binding to an existing Bucket.
	// +optional
	// +kubebuilder:validation:MaxLength=253
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
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="existingBucketName is immutable",rule="self == oldSelf"
	ExistingBucketName string `json:"existingBucketName,omitempty"`
}

// BucketClaimStatus defines the observed state of BucketClaim.
// +kubebuilder:validation:XValidation:message="boundBucketName is immutable once set",rule="!has(oldSelf.boundBucketName) || has(self.boundBucketName)"
// +kubebuilder:validation:XValidation:message="protocols is immutable once set",rule="!has(oldSelf.protocols) || has(self.protocols)"
type BucketClaimStatus struct {
	// boundBucketName is the name of the Bucket this BucketClaim is bound to.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="boundBucketName is immutable once set",rule="oldSelf == '' || self == oldSelf"
	BoundBucketName string `json:"boundBucketName"`

	// readyToUse indicates that the bucket is ready for consumption by workloads.
	ReadyToUse bool `json:"readyToUse"`

	// protocols is the set of protocols the bound Bucket reports to support. BucketAccesses can
	// request access to this BucketClaim using any of the protocols reported here.
	// +optional
	// +listType=set
	Protocols []ObjectProtocol `json:"protocols"`

	// error holds the most recent error message, with a timestamp.
	// This is cleared when provisioning is successful.
	// +optional
	Error *TimestampedError `json:"error,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:metadata:annotations="api-approved.kubernetes.io=unapproved, experimental v1alpha2 changes"

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

// BucketClaimList contains a list of BucketClaim
type BucketClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BucketClaim `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BucketClaim{}, &BucketClaimList{})
}
