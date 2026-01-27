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

/*
This file describes the end-user representation of the various object store protocols.
*/

// TODO: can we write doc generation and linting for the definitions in this file?

// ObjectProtocol represents an object protocol type.
// +kubebuilder:validation:Enum:=S3;Azure;GCS
type ObjectProtocol string

const (
	// ObjectProtocolS3 represents the S3 object protocol type.
	ObjectProtocolS3 ObjectProtocol = "S3"

	// ObjectProtocolS3 represents the Azure Blob object protocol type.
	ObjectProtocolAzure ObjectProtocol = "Azure"

	// ObjectProtocolS3 represents the Google Cloud Storage object protocol type.
	ObjectProtocolGcs ObjectProtocol = "GCS"
)

// A CosiEnvVar defines a COSI environment variable that contains backend bucket or access info.
// Vars marked "Required" will be present with non-empty values in BucketAccess Secrets.
// Some required vars may only be required in certain contexts, like when a specific
// AuthenticationType is used.
// Some vars are only relevant for specific protocols.
// Non-relevant vars will not be present, even when marked "Required".
// Vars are used as data keys in BucketAccess Secrets.
// Vars must be all-caps and must begin with `COSI_`.
type CosiEnvVar string

// A BucketInfoVar defines a protocol-specific COSI environment variable that contains backend
// bucket info.
// All protocol-specific vars include the all-caps protocol name after `COSI_`. E.g., `COSI_AZURE_`.
type BucketInfoVar CosiEnvVar

// A CredentialVar defines a protocol-specific COSI environment variable that contains backend
// bucket access credential info.
// All protocol-specific vars include the all-caps protocol name after `COSI_`. E.g., `COSI_AZURE_`.
type CredentialVar CosiEnvVar

const (
	// Required. The protocol associated with a BucketAccess.
	// Will be a string representing an ObjectProtocol type.
	BucketInfoVar_Protocol BucketInfoVar = "COSI_PROTOCOL"

	// Optional. The certificate authority that clients can use to authenticate a BucketAccess.
	CredentialVar_CertificateAuthority CredentialVar = "COSI_CERTIFICATE_AUTHORITY"
)

/*
 * S3 protocol variables
 */

// bucket info vars
const (
	// Required. The S3 bucket ID as used by clients.
	BucketInfoVar_S3_BucketId BucketInfoVar = "COSI_S3_BUCKET_ID"

	// Required. The S3 endpoint for the bucket.
	BucketInfoVar_S3_Endpoint BucketInfoVar = "COSI_S3_ENDPOINT"

	// Required. The S3 region for the bucket.
	BucketInfoVar_S3_Region BucketInfoVar = "COSI_S3_REGION"

	// Required. The S3 addressing style. One of `path` or `virtual`.
	// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html.
	BucketInfoVar_S3_AddressingStyle BucketInfoVar = "COSI_S3_ADDRESSING_STYLE"
)

// nolint:gosec // credential vars, not hardcoded credentials
const (
	// Required for `AuthenticationType=Key`. The S3 access key ID.
	CredentialVar_S3_AccessKeyId CredentialVar = "COSI_S3_ACCESS_KEY_ID" // nolint:gosec // no a cred

	// Required for `AuthenticationType=Key`. The S3 access secret key.
	CredentialVar_S3_AccessSecretKey CredentialVar = "COSI_S3_ACCESS_SECRET_KEY" // nolint:gosec // no a cred
)

/*
 * Azure protocol variables
 */

// bucket info vars
const (
	// Required. The ID of the Azure storage account.
	BucketInfoVar_Azure_StorageAccount BucketInfoVar = "COSI_AZURE_STORAGE_ACCOUNT"
)

// nolint:gosec // credential vars, not hardcoded credentials
const (
	// Required for `AuthenticationType=Key`. Azure SAS access token.
	// Note that this includes the resource URI as well as token in its definition.
	// See: https://learn.microsoft.com/en-us/azure/storage/common/media/storage-sas-overview/sas-storage-uri.svg
	CredentialVar_Azure_AccessToken CredentialVar = "COSI_AZURE_ACCESS_TOKEN"

	// Optional. The timestamp when access will expire.
	// Empty if unset. Otherwise, date+time in ISO 8601 format.
	CredentialVar_Azure_ExpiryTimestamp CredentialVar = "COSI_AZURE_EXPIRY_TIMESTAMP"
)

/*
 * Google Cloud Storage (GCS) protocol variables
 */

// bucket info vars
const (
	// Required. The GCS project ID.
	BucketInfoVar_GCS_ProjectId BucketInfoVar = "COSI_GCS_PROJECT_ID"

	// Required. GCS bucket name as used by clients.
	BucketInfoVar_GCS_BucketName BucketInfoVar = "COSI_GCS_BUCKET_NAME"
)

// nolint:gosec // credential vars, not hardcoded credentials
const (
	// Required for `AuthenticationType=Key`. HMAC access ID.
	CredentialVar_GCS_AccessId CredentialVar = "COSI_GCS_ACCESS_ID"

	// Required for `AuthenticationType=Key`. HMAC secret.
	CredentialVar_GCS_AccessSecret CredentialVar = "COSI_GCS_ACCESS_SECRET"

	// GCS private key name.
	CredentialVar_GCS_PrivateKeyName CredentialVar = "COSI_GCS_PRIVATE_KEY_NAME"

	// GCS service account name.
	CredentialVar_GCS_ServiceAccount CredentialVar = "COSI_GCS_SERVICE_ACCOUNT"
)
