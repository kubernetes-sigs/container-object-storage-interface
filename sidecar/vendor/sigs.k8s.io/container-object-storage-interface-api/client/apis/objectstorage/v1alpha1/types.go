/*
Copyright 2020 The Kubernetes Authors.

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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	SchemeBuilder.Register(&Bucket{}, &BucketList{})
	SchemeBuilder.Register(&BucketClaim{}, &BucketClaimList{})
	SchemeBuilder.Register(&BucketClass{}, &BucketClassList{})

	SchemeBuilder.Register(&BucketAccess{}, &BucketAccessList{})
	SchemeBuilder.Register(&BucketAccessClass{}, &BucketAccessClassList{})
}

type DeletionPolicy string

const (
	DeletionPolicyRetain DeletionPolicy = "Retain"
	DeletionPolicyDelete DeletionPolicy = "Delete"
)

type Protocol string

const (
	ProtocolS3    Protocol = "S3"
	ProtocolAzure Protocol = "Azure"
	ProtocolGCP   Protocol = "GCP"
)

type AuthenticationType string

const (
	AuthenticationTypeKey AuthenticationType = "Key"
	AuthenticationTypeIAM AuthenticationType = "IAM"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:storageversion
// +kubebuilder:subresource:status
type Bucket struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec BucketSpec `json:"spec,omitempty"`

	// +optional
	Status BucketStatus `json:"status,omitempty"`
}

type BucketSpec struct {
	// DriverName is the name of driver associated with this bucket
	DriverName string `json:"driverName"`

	// Name of the BucketClass specified in the BucketRequest
	BucketClassName string `json:"bucketClassName"`

	// Name of the BucketClaim that resulted in the creation of this Bucket
	// In case the Bucket object was created manually, then this should refer
	// to the BucketClaim with which this Bucket should be bound
	BucketClaim *corev1.ObjectReference `json:"bucketClaim"`

	// Protocols are the set of data APIs this bucket is expected to support.
	// The possible values for protocol are:
	// -  S3: Indicates Amazon S3 protocol
	// -  Azure: Indicates Microsoft Azure BlobStore protocol
	// -  GCS: Indicates Google Cloud Storage protocol
	Protocols []Protocol `json:"protocols"`

	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`

	// DeletionPolicy is used to specify how COSI should handle deletion of this
	// bucket. There are 2 possible values:
	//  - Retain: Indicates that the bucket should not be deleted from the OSP (default)
	//  - Delete: Indicates that the bucket should be deleted from the OSP
	//        once all the workloads accessing this bucket are done
	// +optional
	// +kubebuilder:default:=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy"`

	// ExistingBucketID is the unique id of the bucket in the OSP. This field should be
	// used to specify a bucket that has been created outside of COSI.
	// This field will be empty when the Bucket is dynamically provisioned by COSI.
	// +optional
	ExistingBucketID string `json:"existingBucketID,omitempty"`
}

type BucketStatus struct {
	// BucketReady is a boolean condition to reflect the successful creation
	// of a bucket.
	BucketReady bool `json:"bucketReady,omitempty"`

	// BucketID is the unique id of the bucket in the OSP. This field will be
	// populated by COSI.
	// +optional
	BucketID string `json:"bucketID,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type BucketList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Bucket `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
type BucketClaim struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec BucketClaimSpec `json:"spec,omitempty"`

	// +optional
	Status BucketClaimStatus `json:"status,omitempty"`
}

type BucketClaimSpec struct {
	// Name of the BucketClass
	BucketClassName string `json:"bucketClassName,omitempty"`

	// Protocols are the set of data API this bucket is required to support.
	// The possible values for protocol are:
	// -  S3: Indicates Amazon S3 protocol
	// -  Azure: Indicates Microsoft Azure BlobStore protocol
	// -  GCS: Indicates Google Cloud Storage protocol
	Protocols []Protocol `json:"protocols"`

	// Name of a bucket object that was manually
	// created to import a bucket created outside of COSI
	// If unspecified, then a new Bucket will be dynamically provisioned
	// +optional
	ExistingBucketName string `json:"existingBucketName,omitempty"`
}

type BucketClaimStatus struct {
	// BucketReady indicates that the bucket is ready for consumpotion
	// by workloads
	BucketReady bool `json:"bucketReady"`

	// BucketName is the name of the provisioned Bucket in response
	// to this BucketClaim. It is generated and set by the COSI controller
	// before making the creation request to the OSP backend.
	// +optional
	BucketName string `json:"bucketName,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type BucketClaimList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BucketClaim `json:"items"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:storageversion
type BucketClass struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// DriverName is the name of driver associated with this bucket
	DriverName string `json:"driverName"`

	// DeletionPolicy is used to specify how COSI should handle deletion of this
	// bucket. There are 2 possible values:
	//  - Retain: Indicates that the bucket should not be deleted from the OSP
	//  - Delete: Indicates that the bucket should be deleted from the OSP
	//        once all the workloads accessing this bucket are done
	// +kubebuilder:default:=Retain
	DeletionPolicy DeletionPolicy `json:"deletionPolicy"`

	// Parameters is an opaque map for passing in configuration to a driver
	// for creating the bucket
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type BucketClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BucketClass `json:"items"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:storageversion
type BucketAccessClass struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// DriverName is the name of driver associated with
	// this BucketAccess
	DriverName string `json:"driverName"`

	// AuthenticationType denotes the style of authentication
	// It can be one of
	// Key - access, secret tokens based authentication
	// IAM - implicit authentication of pods to the OSP based on service account mappings
	AuthenticationType AuthenticationType `json:"authenticationType"`

	// Parameters is an opaque map for passing in configuration to a driver
	// for granting access to a bucket
	// +optional
	Parameters map[string]string `json:"parameters,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type BucketAccessClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BucketAccessClass `json:"items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:storageversion
// +kubebuilder:subresource:status
type BucketAccess struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec BucketAccessSpec `json:"spec,omitempty"`

	// +optional
	Status BucketAccessStatus `json:"status"`
}

type BucketAccessSpec struct {
	// BucketClaimName is the name of the BucketClaim.
	BucketClaimName string `json:"bucketClaimName"`

	// Protocol is the name of the Protocol
	// that this access credential is supposed to support
	// If left empty, it will choose the protocol supported
	// by the bucket. If the bucket supports multiple protocols,
	// the end protocol is determined by the driver.
	// +optional
	Protocol Protocol `json:"protocol,omitempty"`

	// BucketAccessClassName is the name of the BucketAccessClass
	BucketAccessClassName string `json:"bucketAccessClassName"`

	// CredentialsSecretName is the name of the secret that COSI should populate
	// with the credentials. If a secret by this name already exists, then it is
	// assumed that credentials have already been generated. It is not overridden.
	// This secret is deleted when the BucketAccess is delted.
	CredentialsSecretName string `json:"credentialsSecretName"`

	// ServiceAccountName is the name of the serviceAccount that COSI will map
	// to the OSP service account when IAM styled authentication is specified
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
}

type BucketAccessStatus struct {
	// AccountID is the unique ID for the account in the OSP. It will be populated
	// by the COSI sidecar once access has been successfully granted.
	// +optional
	AccountID string `json:"accountID,omitempty"`

	// AccessGranted indicates the successful grant of privileges to access the bucket
	// +optional
	AccessGranted bool `json:"accessGranted"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type BucketAccessList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BucketAccess `json:"items"`
}
