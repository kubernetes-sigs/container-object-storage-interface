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
// +kubebuilder:validation:Enum:=Key;ServiceAccount
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
// +kubebuilder:validation:XValidation:message="serviceAccountName cannot be added or removed after creation",rule="has(oldSelf.serviceAccountName) == has(self.serviceAccountName)"
type BucketAccessSpec struct {
	// bucketClaims is a list of BucketClaims the provisioned access must have permissions for,
	// along with per-BucketClaim access parameters and system output definitions.
	// At least one BucketClaim must be referenced.
	// A maximum of 128 BucketClaims may be referenced.
	// Multiple references to the same BucketClaim are not permitted.
	// +required
	// +listType=map
	// +listMapKey=bucketClaimName
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=128
	// +kubebuilder:validation:XValidation:message="bucketClaims list is immutable",rule="self == oldSelf"
	BucketClaims []BucketClaimAccess `json:"bucketClaims,omitempty"`

	// bucketAccessClassName selects the BucketAccessClass for provisioning the access.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="bucketAccessClassName is immutable",rule="self == oldSelf"
	BucketAccessClassName string `json:"bucketAccessClassName,omitempty"`

	// protocol is the object storage protocol that the provisioned access must use.
	// Access can only be granted for BucketClaims that support the requested protocol.
	// Each BucketClaim status reports which protocols are supported for the BucketClaim's bucket.
	// Possible values: 'S3', 'Azure', 'GCS'.
	// +required
	// +kubebuilder:validation:XValidation:message="protocol is immutable",rule="self == oldSelf"
	Protocol ObjectProtocol `json:"protocol,omitempty"`

	// serviceAccountName is the name of the Kubernetes ServiceAccount that user application Pods
	// intend to use for access to referenced BucketClaims.
	// Required when the BucketAccessClass is configured to use ServiceAccount authentication type.
	// Ignored for all other authentication types.
	// It is recommended to specify this for all BucketAccesses to improve portability.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="serviceAccountName is immutable",rule="self == oldSelf"
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
}

// BucketAccessStatus defines the observed state of BucketAccess.
// +kubebuilder:validation:XValidation:message="accountID cannot be removed once set",rule="!has(oldSelf.accountID) || has(self.accountID)"
// +kubebuilder:validation:XValidation:message="accessedBuckets cannot be removed once set",rule="!has(oldSelf.accessedBuckets) || has(self.accessedBuckets)"
// +kubebuilder:validation:XValidation:message="driverName cannot be removed once set",rule="!has(oldSelf.driverName) || has(self.driverName)"
// +kubebuilder:validation:XValidation:message="authenticationType cannot be removed once set",rule="!has(oldSelf.authenticationType) || has(self.authenticationType)"
// +kubebuilder:validation:XValidation:message="parameters cannot be removed once set",rule="!has(oldSelf.parameters) || has(self.parameters)"
type BucketAccessStatus struct {
	// readyToUse indicates that the BucketAccess is ready for consumption by workloads.
	// +required
	ReadyToUse *bool `json:"readyToUse,omitempty"`

	// accountID is the unique identifier for the backend access known to the driver.
	// This field is populated by the COSI Sidecar once access has been successfully granted.
	// Must be at most 2048 characters and consist only of alphanumeric characters ([a-z0-9A-Z]),
	// dashes (-), dots (.), underscores (_), and forward slash (/).
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9/._-]+$`
	// +kubebuilder:validation:XValidation:message="accountId is immutable once set",rule="self == oldSelf"
	AccountID string `json:"accountID,omitempty"`

	// accessedBuckets is a list of Buckets the provisioned access must have permissions for, along
	// with per-Bucket access options. This field is populated by the COSI Controller based on the
	// referenced BucketClaims in the spec.
	// +optional
	// +listType=map
	// +listMapKey=bucketName
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=128
	// +kubebuilder:validation:XValidation:message="accessedBuckets is immutable once set",rule="self == oldSelf"
	AccessedBuckets []AccessedBucket `json:"accessedBuckets,omitempty"`

	// driverName holds a copy of the BucketAccessClass driver name from the time of BucketAccess
	// provisioning. This field is populated by the COSI Controller.
	// Must be 63 characters or less, beginning and ending with an alphanumeric character
	// ([a-z0-9A-Z]) with dashes (-), dots (.), and alphanumerics between.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9]([a-zA-Z0-9\-\.]{0,61}[a-zA-Z0-9])?$`
	// +kubebuilder:validation:XValidation:message="driverName is immutable once set",rule="self == oldSelf"
	DriverName string `json:"driverName,omitempty"`

	// authenticationType holds a copy of the BucketAccessClass authentication type from the time of
	// BucketAccess provisioning. This field is populated by the COSI Controller.
	// Possible values:
	//  - Key: clients may use a protocol-appropriate access key to authenticate to the backend object store.
	//  - ServiceAccount: Pods using the ServiceAccount given in spec.serviceAccountName may authenticate to the backend object store automatically.
	// +optional
	// +kubebuilder:validation:XValidation:message="authenticationType is immutable once set",rule="self == oldSelf"
	AuthenticationType BucketAccessAuthenticationType `json:"authenticationType,omitempty"`

	// parameters holds a copy of the BucketAccessClass parameters from the time of BucketAccess
	// provisioning. This field is populated by the COSI Controller.
	// +optional
	// +kubebuilder:validation:MinProperties=1
	// +kubebuilder:validation:MaxProperties=512
	// +kubebuilder:validation:XValidation:message="accessedBuckets is immutable once set",rule="self == oldSelf"
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
	// Must be a valid Kubernetes resource name: at most 253 characters, consisting only of
	// lower-case alphanumeric characters, hyphens, and periods, starting and ending with an
	// alphanumeric character.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="name must be a valid resource name",rule="!format.dns1123Subdomain().validate(self).hasValue()"
	BucketClaimName string `json:"bucketClaimName,omitempty"`

	// accessMode is the Read/Write access mode that the access should have for the bucket.
	// The provisioned access will have the corresponding permissions to read and/or write objects
	// the BucketClaim's bucket.
	// The provisioned access can also assume to have corresponding permissions to read and/or write
	// object metadata and object metadata (e.g., tags) except when metadata changes would change
	// object store behaviors or permissions (e.g., changes to object caching behaviors).
	// Possible values: 'ReadWrite', 'ReadOnly', 'WriteOnly'.
	// +required
	AccessMode BucketAccessMode `json:"accessMode,omitempty"`

	// accessSecretName is the name of a Kubernetes Secret that COSI should create and populate with
	// bucket info and access credentials for the bucket.
	// The Secret is created in the same Namespace as the BucketAccess and is deleted when the
	// BucketAccess is deleted and deprovisioned.
	// The Secret name must be unique across all bucketClaimRefs for all BucketAccesses in the same
	// Namespace.
	// Must be a valid Kubernetes resource name: at most 253 characters, consisting only of
	// lower-case alphanumeric characters, hyphens, and periods, starting and ending with an
	// alphanumeric character.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="name must be a valid resource name",rule="!format.dns1123Subdomain().validate(self).hasValue()"
	AccessSecretName string `json:"accessSecretName,omitempty"`
}

// AccessedBucket identifies a Bucket and correlates it to a BucketClaimAccess from the spec.
type AccessedBucket struct {
	// bucketName is the name of a Bucket the access should have permissions for.
	// Must be a valid Kubernetes resource name: at most 253 characters, consisting only of
	// lower-case alphanumeric characters, hyphens, and periods, starting and ending with an
	// alphanumeric character.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="name must be a valid resource name",rule="!format.dns1123Subdomain().validate(self).hasValue()"
	BucketName string `json:"bucketName,omitempty"`

	// bucketID is the unique identifier for the backend bucket known to the driver for which
	// this access should have permissions.
	// Must be at most 2048 characters and consist only of alphanumeric characters ([a-z0-9A-Z]),
	// dashes (-), dots (.), underscores (_), and forward slash (/).
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	// +kubebuilder:validation:Pattern=`^[a-zA-Z0-9/._-]+$`
	BucketID string `json:"bucketID,omitempty"`

	// bucketClaimName must match a BucketClaimAccess's BucketClaimName from the spec.
	// Must be a valid Kubernetes resource name: at most 253 characters, consisting only of
	// lower-case alphanumeric characters, hyphens, and periods, starting and ending with an
	// alphanumeric character.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:XValidation:message="name must be a valid resource name",rule="!format.dns1123Subdomain().validate(self).hasValue()"
	BucketClaimName string `json:"bucketClaimName,omitempty"`
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
	Spec BucketAccessSpec `json:"spec,omitzero"`

	// status defines the observed state of BucketAccess
	// +optional
	Status BucketAccessStatus `json:"status,omitzero"`
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
