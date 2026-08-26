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

package translator

import (
	"testing"

	"github.com/stretchr/testify/assert"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
)

func TestTranslateBucketInfo(t *testing.T) {
	tests := []struct {
		name       string // description of this test case
		pbi        *cosiproto.ObjectProtocolAndBucketInfo
		validation *ValidationConfig
		wantProtos []cosiapi.ObjectProtocol
		wantInfo   map[string]string
		wantErr    string
	}{
		{"no info, no validation",
			&cosiproto.ObjectProtocolAndBucketInfo{}, nil,
			[]cosiapi.ObjectProtocol{}, map[string]string{}, "",
		},
		{"no info, validate S3",
			&cosiproto.ObjectProtocolAndBucketInfo{},
			&ValidationConfig{cosiapi.ObjectProtocolS3, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, `missing response for expected "S3" protocol`,
		},
		{"no info, validate Azure",
			&cosiproto.ObjectProtocolAndBucketInfo{},
			&ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, `missing response for expected "Azure" protocol`,
		},
		{"no info, validate GCS",
			&cosiproto.ObjectProtocolAndBucketInfo{},
			&ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, `missing response for expected "GCS" protocol`,
		},
		{"s3 empty, no validation",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3: &cosiproto.S3BucketInfo{},
			},
			nil, // no validation
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
			map[string]string{
				string(cosiapi.BucketInfoVar_S3_BucketId):        "",
				string(cosiapi.BucketInfoVar_S3_Endpoint):        "",
				string(cosiapi.BucketInfoVar_S3_Region):          "",
				string(cosiapi.BucketInfoVar_S3_AddressingStyle): "",
			},
			"",
		},
		{"s3 empty, validate S3",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3: &cosiproto.S3BucketInfo{},
			},
			&ValidationConfig{cosiapi.ObjectProtocolS3, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, "errors translating S3 bucket info",
		},
		{"s3 empty, validate Azure",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3: &cosiproto.S3BucketInfo{},
			},
			&ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, `missing response for expected "Azure" protocol`,
		},
		{"s3 non-empty, no validation",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3: &cosiproto.S3BucketInfo{
					BucketId: "something",
					Endpoint: "cosi.corp.net",
				},
			},
			nil, // no validation
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
			map[string]string{
				string(cosiapi.BucketInfoVar_S3_BucketId):        "something",
				string(cosiapi.BucketInfoVar_S3_Endpoint):        "cosi.corp.net",
				string(cosiapi.BucketInfoVar_S3_Region):          "",
				string(cosiapi.BucketInfoVar_S3_AddressingStyle): "",
			},
			"",
		},
		{"s3 non-empty, validate S3",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3: &cosiproto.S3BucketInfo{
					BucketId: "something",
					Endpoint: "cosi.corp.net",
					// some required info missing to ensure validation is being activated
				},
			},
			&ValidationConfig{cosiapi.ObjectProtocolS3, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, "errors translating S3 bucket info",
		},
		{"s3 non-empty, validate GCS",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3: &cosiproto.S3BucketInfo{
					BucketId: "something",
					Endpoint: "cosi.corp.net",
					// some required info missing to ensure validation is being activated
				},
			},
			&ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, `missing response for expected "GCS" protocol`,
		},
		{"azure empty, no validation",
			&cosiproto.ObjectProtocolAndBucketInfo{
				Azure: &cosiproto.AzureBucketInfo{},
			},
			nil, // no validation
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolAzure},
			map[string]string{
				string(cosiapi.BucketInfoVar_Azure_StorageAccount): "",
			},
			"",
		},
		{"azure empty, validate Azure",
			&cosiproto.ObjectProtocolAndBucketInfo{
				Azure: &cosiproto.AzureBucketInfo{},
			},
			&ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, "errors translating Azure bucket info",
		},
		{"azure non-empty, no validation",
			&cosiproto.ObjectProtocolAndBucketInfo{
				Azure: &cosiproto.AzureBucketInfo{
					StorageAccount: "something",
				},
			},
			nil, // no validation
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolAzure},
			map[string]string{
				string(cosiapi.BucketInfoVar_Azure_StorageAccount): "something",
			},
			"",
		},
		{"azure non-empty, validate Azure",
			&cosiproto.ObjectProtocolAndBucketInfo{
				Azure: &cosiproto.AzureBucketInfo{
					StorageAccount: "", // empty string to verify validation is being activated
				},
			},
			&ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, "errors translating Azure bucket info",
		},
		{"GCS empty, no validation",
			&cosiproto.ObjectProtocolAndBucketInfo{
				Gcs: &cosiproto.GcsBucketInfo{},
			},
			nil, // no validation
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolGcs},
			map[string]string{
				string(cosiapi.BucketInfoVar_GCS_BucketName): "",
				string(cosiapi.BucketInfoVar_GCS_ProjectId):  "",
			},
			"",
		},
		{"GCS empty, validate GCS",
			&cosiproto.ObjectProtocolAndBucketInfo{
				Gcs: &cosiproto.GcsBucketInfo{},
			},
			&ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, "errors translating GCS bucket info",
		},
		{"GCS non-empty, no validation",
			&cosiproto.ObjectProtocolAndBucketInfo{
				Gcs: &cosiproto.GcsBucketInfo{
					BucketName: "something",
				},
			},
			nil, // no validation
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolGcs},
			map[string]string{
				string(cosiapi.BucketInfoVar_GCS_BucketName): "something",
				string(cosiapi.BucketInfoVar_GCS_ProjectId):  "",
			},
			"",
		},
		{"GCS non-empty, validate GCS",
			&cosiproto.ObjectProtocolAndBucketInfo{
				Gcs: &cosiproto.GcsBucketInfo{
					BucketName: "something",
					// some required info missing to ensure validation is being activated
				},
			},
			&ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, "errors translating GCS bucket info",
		},
		{"s3+azure+GCS empty, no validation",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3:    &cosiproto.S3BucketInfo{},
				Azure: &cosiproto.AzureBucketInfo{},
				Gcs:   &cosiproto.GcsBucketInfo{},
			},
			nil, // no validation
			[]cosiapi.ObjectProtocol{
				cosiapi.ObjectProtocolS3,
				cosiapi.ObjectProtocolAzure,
				cosiapi.ObjectProtocolGcs,
			},
			map[string]string{
				string(cosiapi.BucketInfoVar_S3_BucketId):          "",
				string(cosiapi.BucketInfoVar_S3_Endpoint):          "",
				string(cosiapi.BucketInfoVar_S3_Region):            "",
				string(cosiapi.BucketInfoVar_S3_AddressingStyle):   "",
				string(cosiapi.BucketInfoVar_Azure_StorageAccount): "",
				string(cosiapi.BucketInfoVar_GCS_ProjectId):        "",
			},
			"",
		},
		{"s3+azure+GCS empty, validate S3",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3:    &cosiproto.S3BucketInfo{},
				Azure: &cosiproto.AzureBucketInfo{},
				Gcs:   &cosiproto.GcsBucketInfo{},
			},
			&ValidationConfig{cosiapi.ObjectProtocolS3, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, `only "S3" protocol is expected`,
		},
		{"s3+azure+GCS empty, validate Azure",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3:    &cosiproto.S3BucketInfo{},
				Azure: &cosiproto.AzureBucketInfo{},
				Gcs:   &cosiproto.GcsBucketInfo{},
			},
			&ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, `only "Azure" protocol is expected`,
		},
		{"s3+azure+GCS empty, validate GCS",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3:    &cosiproto.S3BucketInfo{},
				Azure: &cosiproto.AzureBucketInfo{},
				Gcs:   &cosiproto.GcsBucketInfo{},
			},
			&ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, `only "GCS" protocol is expected`,
		},
		{"s3+azure+GCS non-empty, no validation",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3: &cosiproto.S3BucketInfo{
					BucketId: "something",
				},
				Azure: &cosiproto.AzureBucketInfo{
					StorageAccount: "acct",
				},
				Gcs: &cosiproto.GcsBucketInfo{
					BucketName: "something",
				},
			},
			nil, // no validation
			[]cosiapi.ObjectProtocol{
				cosiapi.ObjectProtocolS3,
				cosiapi.ObjectProtocolAzure,
				cosiapi.ObjectProtocolGcs,
			},
			map[string]string{
				string(cosiapi.BucketInfoVar_S3_BucketId):          "something",
				string(cosiapi.BucketInfoVar_S3_Endpoint):          "",
				string(cosiapi.BucketInfoVar_S3_Region):            "",
				string(cosiapi.BucketInfoVar_S3_AddressingStyle):   "",
				string(cosiapi.BucketInfoVar_Azure_StorageAccount): "acct",
				string(cosiapi.BucketInfoVar_GCS_ProjectId):        "",
			},
			"",
		},
		{"s3+azure+GCS non-empty, validate S3",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3: &cosiproto.S3BucketInfo{
					BucketId: "something",
				},
				Azure: &cosiproto.AzureBucketInfo{
					StorageAccount: "acct",
				},
				Gcs: &cosiproto.GcsBucketInfo{
					BucketName: "something",
				},
			},
			&ValidationConfig{cosiapi.ObjectProtocolS3, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, `only "S3" protocol is expected`,
		},
		{"s3+azure+GCS non-empty, validate Azure",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3: &cosiproto.S3BucketInfo{
					BucketId: "something",
				},
				Azure: &cosiproto.AzureBucketInfo{
					StorageAccount: "acct",
				},
				Gcs: &cosiproto.GcsBucketInfo{
					BucketName: "something",
				},
			},
			&ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, `only "Azure" protocol is expected`,
		},
		{"s3+azure+GCS non-empty, validate GCS",
			&cosiproto.ObjectProtocolAndBucketInfo{
				S3: &cosiproto.S3BucketInfo{
					BucketId: "something",
				},
				Azure: &cosiproto.AzureBucketInfo{
					StorageAccount: "acct",
				},
				Gcs: &cosiproto.GcsBucketInfo{
					BucketName: "something",
				},
			},
			&ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			nil, nil, `only "GCS" protocol is expected`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protos, infoVars, err := BucketInfoToApi(tt.pbi, tt.validation)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				t.Log("got error:", err)
				assert.ErrorContains(t, err, tt.wantErr)
			}
			assert.Equal(t, tt.wantProtos, protos)
			if tt.wantErr == "" {
				assert.Equal(t, tt.wantInfo, infoVars)
			} else {
				assert.Nil(t, infoVars)
			}
		})
	}
}

func TestTranslateCredentials(t *testing.T) {
	tests := []struct {
		name       string // description of this test case
		pbi        *cosiproto.CredentialInfo
		validation ValidationConfig
		wantCreds  map[string]string
		wantErr    string
	}{
		{"s3 valid, validate S3",
			&cosiproto.CredentialInfo{
				S3: &cosiproto.S3CredentialInfo{
					AccessKeyId:     "accesskey",
					AccessSecretKey: "secretkey",
				},
			},
			ValidationConfig{cosiapi.ObjectProtocolS3, cosiapi.BucketAccessAuthenticationTypeKey},
			map[string]string{
				string(cosiapi.CredentialVar_S3_AccessKeyId):     "accesskey",
				string(cosiapi.CredentialVar_S3_AccessSecretKey): "secretkey",
			},
			"",
		},
		{"azure valid, validate Azure",
			&cosiproto.CredentialInfo{
				Azure: &cosiproto.AzureCredentialInfo{
					AccessToken: "token",
				},
			},
			ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			map[string]string{
				string(cosiapi.CredentialVar_Azure_AccessToken):     "token",
				string(cosiapi.CredentialVar_Azure_ExpiryTimestamp): "",
			},
			"",
		},
		{"GCS key valid, validate GCS",
			&cosiproto.CredentialInfo{
				Gcs: &cosiproto.GcsCredentialInfo{
					AccessId:     "accessId",
					AccessSecret: "accessSecret",
				},
			},
			ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			map[string]string{
				string(cosiapi.CredentialVar_GCS_AccessId):       "accessId",
				string(cosiapi.CredentialVar_GCS_AccessSecret):   "accessSecret",
				string(cosiapi.CredentialVar_GCS_PrivateKeyName): "",
				string(cosiapi.CredentialVar_GCS_ServiceAccount): "",
			},
			"",
		},
		{"no info, validate S3",
			&cosiproto.CredentialInfo{},
			ValidationConfig{cosiapi.ObjectProtocolS3, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			`missing response for expected "S3" protocol`,
		},
		{"no info, validate Azure",
			&cosiproto.CredentialInfo{},
			ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			`missing response for expected "Azure" protocol`,
		},
		{"no info, validate GCS",
			&cosiproto.CredentialInfo{},
			ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			`missing response for expected "GCS" protocol`,
		},
		{"s3 empty, validate S3",
			&cosiproto.CredentialInfo{
				S3: &cosiproto.S3CredentialInfo{},
			},
			ValidationConfig{cosiapi.ObjectProtocolS3, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			"errors translating S3 bucket credentials",
		},
		{"s3 empty, validate Azure",
			&cosiproto.CredentialInfo{
				S3: &cosiproto.S3CredentialInfo{},
			},
			ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			`missing response for expected "Azure" protocol`,
		},
		{"s3 non-empty, validate S3",
			&cosiproto.CredentialInfo{
				S3: &cosiproto.S3CredentialInfo{
					AccessKeyId: "accesskey",
					// some required info missing to ensure validation is being activated
				},
			},
			ValidationConfig{cosiapi.ObjectProtocolS3, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			"errors translating S3 bucket credentials",
		},
		{"s3 non-empty, validate GCS",
			&cosiproto.CredentialInfo{
				S3: &cosiproto.S3CredentialInfo{
					AccessKeyId: "accesskey",
					// some required info missing to ensure validation is being activated
				},
			},
			ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			`missing response for expected "GCS" protocol`,
		},
		{"azure empty, validate Azure",
			&cosiproto.CredentialInfo{
				Azure: &cosiproto.AzureCredentialInfo{},
			},
			ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			"errors translating Azure bucket credentials",
		},
		{"azure non-empty, validate Azure",
			&cosiproto.CredentialInfo{
				Azure: &cosiproto.AzureCredentialInfo{
					AccessToken: "", // empty string to verify validation is being activated
				},
			},
			ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			"errors translating Azure bucket credentials",
		},
		{"GCS empty, validate GCS",
			&cosiproto.CredentialInfo{
				Gcs: &cosiproto.GcsCredentialInfo{},
			},
			ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			"errors translating GCS bucket credentials",
		},
		{"GCS non-empty, validate GCS",
			&cosiproto.CredentialInfo{
				Gcs: &cosiproto.GcsCredentialInfo{
					AccessId: "accessId",
					// some required info missing to ensure validation is being activated
				},
			},
			ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			"errors translating GCS bucket credentials",
		},
		{"s3+azure+GCS empty, validate S3",
			&cosiproto.CredentialInfo{
				S3:    &cosiproto.S3CredentialInfo{},
				Azure: &cosiproto.AzureCredentialInfo{},
				Gcs:   &cosiproto.GcsCredentialInfo{},
			},
			ValidationConfig{cosiapi.ObjectProtocolS3, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			`only "S3" protocol is expected`,
		},
		{"s3+azure+GCS empty, validate Azure",
			&cosiproto.CredentialInfo{
				S3:    &cosiproto.S3CredentialInfo{},
				Azure: &cosiproto.AzureCredentialInfo{},
				Gcs:   &cosiproto.GcsCredentialInfo{},
			},
			ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			`only "Azure" protocol is expected`,
		},
		{"s3+azure+GCS empty, validate GCS",
			&cosiproto.CredentialInfo{
				S3:    &cosiproto.S3CredentialInfo{},
				Azure: &cosiproto.AzureCredentialInfo{},
				Gcs:   &cosiproto.GcsCredentialInfo{},
			},
			ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			`only "GCS" protocol is expected`,
		},
		{"s3+azure+GCS non-empty, validate S3",
			&cosiproto.CredentialInfo{
				S3: &cosiproto.S3CredentialInfo{
					AccessKeyId: "something",
				},
				Azure: &cosiproto.AzureCredentialInfo{
					AccessToken: "",
				},
				Gcs: &cosiproto.GcsCredentialInfo{
					AccessId: "something",
				},
			},
			ValidationConfig{cosiapi.ObjectProtocolS3, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			`only "S3" protocol is expected`,
		},
		{"s3+azure+GCS non-empty, validate Azure",
			&cosiproto.CredentialInfo{
				S3: &cosiproto.S3CredentialInfo{
					AccessKeyId: "something",
				},
				Azure: &cosiproto.AzureCredentialInfo{
					AccessToken: "",
				},
				Gcs: &cosiproto.GcsCredentialInfo{
					AccessId: "something",
				},
			},
			ValidationConfig{cosiapi.ObjectProtocolAzure, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			`only "Azure" protocol is expected`,
		},
		{"s3+azure+GCS non-empty, validate GCS",
			&cosiproto.CredentialInfo{
				S3: &cosiproto.S3CredentialInfo{
					AccessKeyId: "something",
				},
				Azure: &cosiproto.AzureCredentialInfo{
					AccessToken: "",
				},
				Gcs: &cosiproto.GcsCredentialInfo{
					AccessId: "something",
				},
			},
			ValidationConfig{cosiapi.ObjectProtocolGcs, cosiapi.BucketAccessAuthenticationTypeKey},
			nil,
			`only "GCS" protocol is expected`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			creds, err := CredentialsToApi(tt.pbi, tt.validation)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				t.Log("got error:", err)
				assert.ErrorContains(t, err, tt.wantErr)
			}
			if tt.wantErr == "" {
				assert.Equal(t, tt.wantCreds, creds)
			} else {
				assert.Nil(t, creds)
			}
		})
	}
}

func TestAccessModeToRpc(t *testing.T) {
	tests := []struct {
		name    string
		m       cosiapi.BucketAccessMode
		want    cosiproto.AccessMode_Mode
		wantErr bool
	}{
		{"category not requested maps to ANY, not UNKNOWN", "", cosiproto.AccessMode_ANY, false},
		{"read-write", cosiapi.BucketAccessModeReadWrite, cosiproto.AccessMode_READ_WRITE, false},
		{"read-only", cosiapi.BucketAccessModeReadOnly, cosiproto.AccessMode_READ_ONLY, false},
		{"write-only", cosiapi.BucketAccessModeWriteOnly, cosiproto.AccessMode_WRITE_ONLY, false},
		{"unknown mode is an error and maps to UNKNOWN", cosiapi.BucketAccessMode("bogus"), cosiproto.AccessMode_UNKNOWN, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AccessModeToRpc(tt.m)
			assert.Equal(t, tt.want, got)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
