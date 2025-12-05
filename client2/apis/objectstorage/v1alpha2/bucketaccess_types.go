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

// BucketAccessAuthenticationType specifies what authentication mechanism is used for provisioning
// bucket access.
// +enum
// +kubebuilder:validation:Enum:="";Key;ServiceAccount
type BucketAccessAuthenticationType string

const (
	// The driver should generate a protocol-appropriate access key that clients can use to
	// authenticate to the backend object store.
	BucketAccessAuthenticationTypeKey = "Key"

	// The driver should configure the system such that Pods using the given ServiceAccount
	// authenticate to the backend object store automatically.
	BucketAccessAuthenticationTypeServiceAccount = "ServiceAccount"
)

// BucketAccessMode describes the Read/Write mode an access should have for a bucket.
// +enum
// +kubebuilder:validation:Enum:=ReadWrite;ReadOnly;WriteOnly
type BucketAccessMode string

const (
	// BucketAccessModeReadWrite represents read-write access mode.
	BucketAccessModeReadWrite BucketAccessMode = "ReadWrite"

	// BucketAccessModeReadOnly represents read-only access mode.
	BucketAccessModeReadOnly BucketAccessMode = "ReadOnly"

	// BucketAccessModeWriteOnly represents write-only access mode.
	BucketAccessModeWriteOnly BucketAccessMode = "WriteOnly"
)

// BucketAccessSpec defines the desired state of BucketAccess
// +kubebuilder:validation:XValidation:message="serviceAccountName is immutable",rule="has(oldSelf.serviceAccountName) == has(self.serviceAccountName)"
type BucketAccessSpec struct {
	// bucketClaims is a list of BucketClaims the provisioned access must have permissions for,
	// along with per-BucketClaim access parameters and system output definitions.
	// At least one BucketClaim must be referenced.
	// Multiple references to the same BucketClaim are not permitted.
	// +required
	// +listType=map
	// +listMapKey=bucketClaimName
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:XValidation:message="bucketClaims list is immutable",rule="self == oldSelf"
	BucketClaims []BucketClaimAccess `json:"bucketClaims"`

	// bucketAccessClassName selects the BucketAccessClass for provisioning the access.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="bucketAccessClassName is immutable",rule="self == oldSelf"
	BucketAccessClassName string `json:"bucketAccessClassName"`

	// protocol is the object storage protocol that the provisioned access must use.
	// +required
	// +kubebuilder:validation:XValidation:message="protocol is immutable",rule="self == oldSelf"
	Protocol ObjectProtocol `json:"protocol"`

	// serviceAccountName is the name of the Kubernetes ServiceAccount that user application Pods
	// intend to use for access to referenced BucketClaims.
	// This has different behavior based on the BucketAccessClass's defined AuthenticationType:
	// - Key: This field is ignored.
	// - ServiceAccount: This field is required. The driver should configure the system so that Pods
	//   using the ServiceAccount authenticate to the object storage backend automatically.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="serviceAccountName is immutable",rule="self == oldSelf"
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
}

// BucketAccessStatus defines the observed state of BucketAccess.
// +kubebuilder:validation:XValidation:message="accountID is immutable once set",rule="!has(oldSelf.accountID) || has(self.accountID)"
// +kubebuilder:validation:XValidation:message="accessedBuckets is immutable once set",rule="!has(oldSelf.accessedBuckets) || has(self.accessedBuckets)"
// +kubebuilder:validation:XValidation:message="driverName is immutable once set",rule="!has(oldSelf.driverName) || has(self.driverName)"
// +kubebuilder:validation:XValidation:message="authenticationType is immutable once set",rule="!has(oldSelf.authenticationType) || has(self.authenticationType)"
// +kubebuilder:validation:XValidation:message="parameters is immutable once set",rule="!has(oldSelf.parameters) || has(self.parameters)"
type BucketAccessStatus struct {
	// readyToUse indicates that the BucketAccess is ready for consumption by workloads.
	ReadyToUse bool `json:"readyToUse"`

	// accountID is the unique identifier for the backend access known to the driver.
	// This field is populated by the COSI Sidecar once access has been successfully granted.
	// +optional
	// +kubebuilder:validation:XValidation:message="accountId is immutable once set",rule="oldSelf == '' || self == oldSelf"
	AccountID string `json:"accountID"`

	// accessedBuckets is a list of Buckets the provisioned access must have permissions for, along
	// with per-Bucket access options. This field is populated by the COSI Controller based on the
	// referenced BucketClaims in the spec.
	// +optional
	// +listType=map
	// +listMapKey=bucketName
	// +kubebuilder:validation:XValidation:message="accessedBuckets is immutable once set",rule="oldSelf.size() == 0 || self == oldSelf"
	AccessedBuckets []AccessedBucket `json:"accessedBuckets"`

	// driverName holds a copy of the BucketAccessClass driver name from the time of BucketAccess
	// provisioning. This field is populated by the COSI Controller.
	// +optional
	// +kubebuilder:validation:XValidation:message="driverName is immutable once set",rule="oldSelf == '' || self == oldSelf"
	DriverName string `json:"driverName"`

	// authenticationType holds a copy of the BucketAccessClass authentication type from the time of
	// BucketAccess provisioning. This field is populated by the COSI Controller.
	// +optional
	// +kubebuilder:validation:XValidation:message="authenticationType is immutable once set",rule="oldSelf == '' || self == oldSelf"
	AuthenticationType BucketAccessAuthenticationType `json:"authenticationType"`

	// parameters holds a copy of the BucketAccessClass parameters from the time of BucketAccess
	// provisioning. This field is populated by the COSI Controller.
	// +optional
	// +kubebuilder:validation:XValidation:message="accessedBuckets is immutable once set",rule="oldSelf.size() == 0 || self == oldSelf"
	Parameters map[string]string `json:"parameters,omitempty"`

	// error holds the most recent error message, with a timestamp.
	// This is cleared when provisioning is successful.
	// +optional
	Error *TimestampedError `json:"error,omitempty"`
}

// BucketClaimAccess selects a BucketClaim for access, defines access parameters for the
// corresponding bucket, and specifies where user-consumable bucket information and access
// credentials for the accessed bucket will be stored.
type BucketClaimAccess struct {
	// bucketClaimName is the name of a BucketClaim the access should have permissions for.
	// The BucketClaim must be in the same Namespace as the BucketAccess.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	BucketClaimName string `json:"bucketClaimName"`

	// accessMode is the Read/Write access mode that the access should have for the bucket.
	// Possible values: ReadWrite, ReadOnly, WriteOnly.
	// +required
	AccessMode BucketAccessMode `json:"accessMode"`

	// accessSecretName is the name of a Kubernetes Secret that COSI should create and populate with
	// bucket info and access credentials for the bucket.
	// The Secret is created in the same Namespace as the BucketAccess and is deleted when the
	// BucketAccess is deleted and deprovisioned.
	// The Secret name must be unique across all bucketClaimRefs for all BucketAccesses in the same
	// Namespace.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	AccessSecretName string `json:"accessSecretName"`
}

// AccessedBucket identifies a Bucket and correlates it to a BucketClaimAccess from the spec.
type AccessedBucket struct {
	// bucketName is the name of a Bucket the access should have permissions for.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	BucketName string `json:"bucketName"`

	// bucketClaimName must match a BucketClaimAccess's BucketClaimName from the spec.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	BucketClaimName string `json:"bucketClaimName"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:metadata:annotations="api-approved.kubernetes.io=unapproved, experimental v1alpha2 changes"

// BucketAccess is the Schema for the bucketaccesses API
type BucketAccess struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of BucketAccess
	// +required
	Spec BucketAccessSpec `json:"spec"`

	// status defines the observed state of BucketAccess
	// +optional
	Status BucketAccessStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// BucketAccessList contains a list of BucketAccess
type BucketAccessList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BucketAccess `json:"items"`
}

func init() {
	SchemeBuilder.Register(&BucketAccess{}, &BucketAccessList{})
}
