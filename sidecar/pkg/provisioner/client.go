/* Copyright 2021 The Kubernetes Authors.
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

package provisioner

import (
	"context"

	"google.golang.org/grpc"
	cosi "sigs.k8s.io/container-object-storage-interface-api/proto"
)

var (
	_ cosi.IdentityClient    = &COSIProvisionerClient{}
	_ cosi.ProvisionerClient = &COSIProvisionerClient{}
)

type COSIProvisionerClient struct {
	address           string
	conn              *grpc.ClientConn
	identityClient    cosi.IdentityClient
	provisionerClient cosi.ProvisionerClient
}

func (c *COSIProvisionerClient) DriverGetInfo(ctx context.Context,
	in *cosi.DriverGetInfoRequest,
	opts ...grpc.CallOption) (*cosi.DriverGetInfoResponse, error) {

	return c.identityClient.DriverGetInfo(ctx, in, opts...)
}

func (c *COSIProvisionerClient) DriverCreateBucket(ctx context.Context,
	in *cosi.DriverCreateBucketRequest,
	opts ...grpc.CallOption) (*cosi.DriverCreateBucketResponse, error) {

	return c.provisionerClient.DriverCreateBucket(ctx, in, opts...)
}

func (c *COSIProvisionerClient) DriverDeleteBucket(ctx context.Context,
	in *cosi.DriverDeleteBucketRequest,
	opts ...grpc.CallOption) (*cosi.DriverDeleteBucketResponse, error) {

	return c.provisionerClient.DriverDeleteBucket(ctx, in, opts...)
}

func (c *COSIProvisionerClient) DriverGrantBucketAccess(ctx context.Context,
	in *cosi.DriverGrantBucketAccessRequest,
	opts ...grpc.CallOption) (*cosi.DriverGrantBucketAccessResponse, error) {

	return c.provisionerClient.DriverGrantBucketAccess(ctx, in, opts...)
}

func (c *COSIProvisionerClient) DriverRevokeBucketAccess(ctx context.Context,
	in *cosi.DriverRevokeBucketAccessRequest,
	opts ...grpc.CallOption) (*cosi.DriverRevokeBucketAccessResponse, error) {

	return c.provisionerClient.DriverRevokeBucketAccess(ctx, in, opts...)
}
