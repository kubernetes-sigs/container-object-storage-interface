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
	"fmt"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
)

var (
	// S3 addressing styles
	s3AddressingStylePath    = "path"
	s3AddressingStyleVirtual = "virtual"
	validS3AddressingStyles  = []string{
		s3AddressingStylePath,
		s3AddressingStyleVirtual,
	}
)

// S3BucketInfoTranslator implements RpcApiTranslator for S3 bucket info.
type S3BucketInfoTranslator struct{}

// TODO: S3CredentialTranslator implements RpcApiTranslator for S3 credentials.

var _ RpcApiTranslator[*cosiproto.S3BucketInfo, cosiapi.BucketInfoVar] = S3BucketInfoTranslator{}

func (S3BucketInfoTranslator) RpcToApi(b *cosiproto.S3BucketInfo) map[cosiapi.BucketInfoVar]string {
	if b == nil {
		return nil
	}

	out := map[cosiapi.BucketInfoVar]string{
		cosiapi.BucketInfoVar_S3_BucketId:        b.BucketId,
		cosiapi.BucketInfoVar_S3_Endpoint:        b.Endpoint,
		cosiapi.BucketInfoVar_S3_Region:          b.Region,
		cosiapi.BucketInfoVar_S3_AddressingStyle: "", // set below if possible
	}

	// addressing style
	if b.AddressingStyle != nil {
		switch b.AddressingStyle.Style {
		case cosiproto.S3AddressingStyle_PATH:
			out[cosiapi.BucketInfoVar_S3_AddressingStyle] = s3AddressingStylePath
		case cosiproto.S3AddressingStyle_VIRTUAL:
			out[cosiapi.BucketInfoVar_S3_AddressingStyle] = s3AddressingStyleVirtual
		}
	}

	return out
}

func (S3BucketInfoTranslator) ApiToRpc(vars map[cosiapi.BucketInfoVar]string) *cosiproto.S3BucketInfo {
	if len(vars) == 0 {
		return nil
	}

	out := &cosiproto.S3BucketInfo{}

	out.BucketId = vars[cosiapi.BucketInfoVar_S3_BucketId]
	out.Endpoint = vars[cosiapi.BucketInfoVar_S3_Endpoint]
	out.Region = vars[cosiapi.BucketInfoVar_S3_Region]

	out.AddressingStyle = &cosiproto.S3AddressingStyle{}
	addrStyle := vars[cosiapi.BucketInfoVar_S3_AddressingStyle]
	switch addrStyle {
	case s3AddressingStylePath:
		out.AddressingStyle.Style = cosiproto.S3AddressingStyle_PATH
	case s3AddressingStyleVirtual:
		out.AddressingStyle.Style = cosiproto.S3AddressingStyle_VIRTUAL
	default:
		out.AddressingStyle.Style = cosiproto.S3AddressingStyle_UNKNOWN
	}

	return out
}

func (S3BucketInfoTranslator) Validate(
	vars map[cosiapi.BucketInfoVar]string, _ cosiapi.BucketAccessAuthenticationType,
) error {
	errs := []string{}

	id := vars[cosiapi.BucketInfoVar_S3_BucketId]
	if id == "" {
		errs = append(errs, "S3 bucket ID cannot be unset")
	}

	ep := vars[cosiapi.BucketInfoVar_S3_Endpoint]
	if ep == "" {
		errs = append(errs, "S3 endpoint cannot be unset")
	}

	rg := vars[cosiapi.BucketInfoVar_S3_Region]
	if rg == "" {
		errs = append(errs, "S3 region cannot be unset")
	}

	as := vars[cosiapi.BucketInfoVar_S3_AddressingStyle]
	if !contains(validS3AddressingStyles, as) {
		errs = append(errs, fmt.Sprintf("S3 addressing style %q must be one of %v", as, validS3AddressingStyles))
	}

	if len(errs) > 0 {
		return fmt.Errorf("S3 bucket info is invalid: %v", errs)
	}
	return nil
}
