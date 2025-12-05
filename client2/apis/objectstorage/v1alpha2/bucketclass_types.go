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

// BucketClassSpec defines the BucketClass.
type BucketClassSpec struct {
	// driverName is the name of the driver that fulfills requests for this BucketClass.
	// +required
	// +kubebuilder:validation:MinLength=1
	DriverName string `json:"driverName"`

	// deletionPolicy determines whether a Bucket created through the BucketClass should be deleted
	// when its bound BucketClaim is deleted.
	// Possible values:
	//  - Retain: keep both the Bucket object and the backend bucket
	//  - Delete: delete both the Bucket object and the backend bucket
	// +required
	DeletionPolicy BucketDeletionPolicy `json:"deletionPolicy"`

	// parameters is an opaque map of driver-specific configuration items passed to the driver that
	// fulfills requests for this BucketClass.
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:metadata:annotations="api-approved.kubernetes.io=unapproved, experimental v1alpha2 changes"

// BucketClass defines a named "class" of object storage buckets.
// Different classes might map to different object storage protocols, quality-of-service levels,
// backup policies, or any other arbitrary configuration determined by storage administrators.
// The name of a BucketClass object is significant, and is how users can request a particular class.
type BucketClass struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the BucketClass. spec is entirely immutable.
	// +required
	// +kubebuilder:validation:XValidation:message="BucketClass spec is immutable",rule="self == oldSelf"
	Spec BucketClassSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// BucketClassList contains a list of BucketClass
type BucketClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BucketClass `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BucketClass{}, &BucketClassList{})
}
