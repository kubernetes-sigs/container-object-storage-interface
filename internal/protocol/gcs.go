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

package protocol

import (
	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
)

// GcsBucketInfoTranslator implements RpcApiTranslator for S3 bucket info.
type GcsBucketInfoTranslator struct{}

// TODO: S3CredentialTranslator implements RpcApiTranslator for S3 credentials.

var _ RpcApiTranslator[*cosiproto.GcsBucketInfo, cosiapi.BucketInfoVar] = GcsBucketInfoTranslator{}

func (GcsBucketInfoTranslator) RpcToApi(b *cosiproto.GcsBucketInfo) map[cosiapi.BucketInfoVar]string {
	if b == nil {
		return nil
	}

	out := map[cosiapi.BucketInfoVar]string{
		cosiapi.BucketInfoVar_GCS_BucketName: "",
		cosiapi.BucketInfoVar_GCS_ProjectId:  "",
	}

	// TODO: implement

	return out
}

func (GcsBucketInfoTranslator) ApiToRpc(vars map[cosiapi.BucketInfoVar]string) *cosiproto.GcsBucketInfo {
	if len(vars) == 0 {
		return nil
	}

	// TODO: implement

	return nil
}

func (GcsBucketInfoTranslator) Validate(
	vars map[cosiapi.BucketInfoVar]string, _ cosiapi.BucketAccessAuthenticationType,
) error {
	// TODO: implement

	return nil
}
