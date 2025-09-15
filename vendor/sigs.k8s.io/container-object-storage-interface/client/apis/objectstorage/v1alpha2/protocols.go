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
This file contains all definitions for the various object store protocols.
*/

// ObjectProtocol represents an object protocol type.
type ObjectProtocol string

const (
	// ObjectProtocolS3 represents the S3 object protocol type.
	ObjectProtocolS3 = "S3"

	// ObjectProtocolS3 represents the Azure Blob object protocol type.
	ObjectProtocolAzure = "Azure"

	// ObjectProtocolS3 represents the Google Cloud Storage object protocol type.
	ObjectProtocolGcs = "GCS"
)
