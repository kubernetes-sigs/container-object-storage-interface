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

// BucketAccessClassSpec defines the desired state of BucketAccessClass
type BucketAccessClassSpec struct {
	// driverName is the name of the driver that fulfills requests for this BucketAccessClass.
	// See driver documentation to determine the correct value to set.
	// Must be 63 characters or less, beginning and ending with an alphanumeric character
	// ([a-z0-9A-Z]) with dashes (-), dots (.), and alphanumerics between.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9]([a-zA-Z0-9\-\.]{0,61}[a-zA-Z0-9])?$`
	DriverName string `json:"driverName,omitempty"`

	// authenticationType specifies which authentication mechanism is used bucket access.
	// See driver documentation to determine which values are supported.
	// Possible values:
	//  - Key: The driver should generate a protocol-appropriate access key that clients can use to
	//    authenticate to the backend object store.
	//  - ServiceAccount: The driver should configure the system such that Pods using the given
	//    ServiceAccount authenticate to the backend object store automatically.
	// +required
	AuthenticationType BucketAccessAuthenticationType `json:"authenticationType,omitempty"`

	// parameters is an opaque map of driver-specific configuration items passed to the driver that
	// fulfills requests for this BucketAccessClass.
	// See driver documentation to determine supported parameters and their effects.
	// A maximum of 512 parameters are allowed.
	// +optional
	// +kubebuilder:validation:MinProperties=1
	// +kubebuilder:validation:MaxProperties=512
	Parameters map[string]string `json:"parameters,omitempty"`

	// disallowedBucketAccessModes is a list of disallowed Read/Write access modes. A BucketAccess
	// using this class will not be allowed to request access to a BucketClaim with any access mode
	// listed here.
	// This is particularly useful for administrators to restrict access to a statically-provisioned
	// bucket that is managed outside the BucketAccess Namespace or Kubernetes cluster.
	// Possible values: 'ReadWrite', 'ReadOnly', 'WriteOnly'.
	// +optional
	// +listType=set
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	DisallowedBucketAccessModes []BucketAccessMode `json:"disallowedBucketAccessModes,omitempty"`

	// multiBucketAccess specifies whether a BucketAccess using this class can reference multiple
	// BucketClaims. When omitted, this means no opinion, and COSI will choose a reasonable default,
	// which is subject to change over time.
	// Possible values:
	//  - SingleBucket: (default) A BucketAccess may reference only a single BucketClaim.
	//  - MultipleBuckets: A BucketAccess may reference multiple (1 or more) BucketClaims.
	// +optional
	MultiBucketAccess MultiBucketAccess `json:"multiBucketAccess,omitempty"`
}

// MultiBucketAccess specifies whether a BucketAccess can reference multiple BucketClaims.
// +enum
// +kubebuilder:validation:Enum:=SingleBucket;MultipleBuckets
type MultiBucketAccess string

const (
	// MultiBucketAccessSingleBucket indicates that a BucketAccess can reference only a single
	// BucketClaim.
	MultiBucketAccessSingleBucket MultiBucketAccess = "SingleBucket"

	// MultiBucketAccessMultipleBuckets indicates that a BucketAccess can reference multiple
	// (1 or more) BucketClaims.
	MultiBucketAccessMultipleBuckets MultiBucketAccess = "MultipleBuckets"
)

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:metadata:annotations="api-approved.kubernetes.io=unapproved, experimental v1alpha2 changes"

// BucketAccessClass is the Schema for the bucketaccessclasses API
type BucketAccessClass struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of BucketAccessClass
	// +required
	// +kubebuilder:validation:XValidation:message="BucketAccessClass spec is immutable",rule="self == oldSelf"
	Spec BucketAccessClassSpec `json:"spec,omitzero"`
}

// +kubebuilder:object:root=true

// BucketAccessClassList contains a list of BucketAccessClass
type BucketAccessClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BucketAccessClass `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BucketAccessClass{}, &BucketAccessClassList{})
}

func (validate BucketAccessClassSpec) SupportsMultiBucketAccess() bool {
	if validate.MultiBucketAccess == MultiBucketAccessMultipleBuckets {
		return true
	}
	return false // default to single-bucket access
}
