/* Copyright 2022 The Kubernetes Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package consts

import "errors"

const (
	AccountNamePrefix = "ba-"
	BucketInfoPrefix  = "bc-"

	BABucketFinalizer = "cosi.objectstorage.k8s.io/bucketaccess-bucket-protection"
	BAFinalizer       = "cosi.objectstorage.k8s.io/bucketaccess-protection"
	BCFinalizer       = "cosi.objectstorage.k8s.io/bucketclaim-protection"
	BucketFinalizer   = "cosi.objectstorage.k8s.io/bucket-protection"
	SecretFinalizer   = "cosi.objectstorage.k8s.io/secret-protection"

	S3Key                      = "s3"
	AzureKey                   = "azure"
	S3SecretAccessKeyID        = "accessKeyID"
	S3SecretAccessSecretKey    = "accessSecretKey"
	S3Endpoint                 = "endpoint"
	S3Region                   = "region"
	AzureSecretAccessToken     = "accessToken"
	AzureSecretExpiryTimeStamp = "expiryTs"
	DefaultTimeFormat          = "2006-01-02 15:04:05.999999999 -0700 MST"
)

var (
	ErrInternal                    = errors.New("driverCreateBucket returned a nil response")
	ErrBucketInfoConversionFailed  = errors.New("error converting bucketInfo into Secret")
	ErrEmptyBucketID               = errors.New("driverCreateBucket returned an empty bucketID")
	ErrUndefinedBucketClassName    = errors.New("BucketClassName not defined")
	ErrUndefinedAccountID          = errors.New("AccountId not defined in DriverGrantBucketAccess")
	ErrUndefinedSecretName         = errors.New("CredentialsSecretName not defined in the BucketAccess")
	ErrInvalidBucketState          = errors.New("BucketAccess can't be granted to Bucket not in Ready state")
	ErrInvalidCredentials          = errors.New("Credentials returned in DriverGrantBucketAccessResponse should be of length 1")
	ErrUndefinedServiceAccountName = errors.New("ServiceAccountName required when AuthenticationType is IAM")
)
