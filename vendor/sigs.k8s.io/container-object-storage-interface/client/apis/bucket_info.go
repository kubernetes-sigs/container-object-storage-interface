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

package cosiapi

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
)

type SecretS3 struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"accessKeyID"`
	AccessSecretKey string `json:"accessSecretKey"`
}

type SecretAzure struct {
	AccessToken     string       `json:"accessToken"`
	ExpiryTimeStamp *metav1.Time `json:"expiryTimeStamp"`
}

// +k8s:deepcopy-gen=false
type BucketInfo struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec BucketInfoSpec `json:"spec,omitempty"`
}

type BucketInfoSpec struct {
	// BucketName is the name of the Bucket
	BucketName string `json:"bucketName"`

	// AuthenticationType denotes the style of authentication
	// It can be one of
	// KEY - access, secret tokens based authentication
	// IAM - implicit authentication of pods to the OSP based on service account mappings
	AuthenticationType v1alpha1.AuthenticationType `json:"authenticationType"`

	// S3 - Details of S3 credentials
	S3 *SecretS3 `json:"secretS3"`

	// Azure - Details of Azure credentials
	Azure *SecretAzure `json:"secretAzure"`

	// Protocols are the set of data APIs this bucket is expected to support.
	// The possible values for protocol are:
	// -  S3: Indicates Amazon S3 protocol
	// -  Azure: Indicates Microsoft Azure BlobStore protocol
	// -  GCS: Indicates Google Cloud Storage protocol
	Protocols []v1alpha1.Protocol `json:"protocols"`
}
