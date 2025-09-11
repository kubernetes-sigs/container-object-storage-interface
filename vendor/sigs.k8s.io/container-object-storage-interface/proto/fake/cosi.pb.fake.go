package fake

import (
	"context"
	grpc "google.golang.org/grpc"
	proto "sigs.k8s.io/container-object-storage-interface/proto"
)

type FakeIdentityClient struct {
	FakeDriverGetInfo func(ctx context.Context, in *proto.DriverGetInfoRequest, opts ...grpc.CallOption) (*proto.DriverGetInfoResponse, error)
}

func (f *FakeIdentityClient) DriverGetInfo(ctx context.Context, in *proto.DriverGetInfoRequest, opts ...grpc.CallOption) (*proto.DriverGetInfoResponse, error) {
	return f.FakeDriverGetInfo(ctx, in, opts...)
}

type FakeProvisionerClient struct {
	FakeDriverCreateBucket       func(ctx context.Context, in *proto.DriverCreateBucketRequest, opts ...grpc.CallOption) (*proto.DriverCreateBucketResponse, error)
	FakeDriverDeleteBucket       func(ctx context.Context, in *proto.DriverDeleteBucketRequest, opts ...grpc.CallOption) (*proto.DriverDeleteBucketResponse, error)
	FakeDriverGrantBucketAccess  func(ctx context.Context, in *proto.DriverGrantBucketAccessRequest, opts ...grpc.CallOption) (*proto.DriverGrantBucketAccessResponse, error)
	FakeDriverRevokeBucketAccess func(ctx context.Context, in *proto.DriverRevokeBucketAccessRequest, opts ...grpc.CallOption) (*proto.DriverRevokeBucketAccessResponse, error)
}

func (f *FakeProvisionerClient) DriverCreateBucket(ctx context.Context, in *proto.DriverCreateBucketRequest, opts ...grpc.CallOption) (*proto.DriverCreateBucketResponse, error) {
	return f.FakeDriverCreateBucket(ctx, in, opts...)
}
func (f *FakeProvisionerClient) DriverDeleteBucket(ctx context.Context, in *proto.DriverDeleteBucketRequest, opts ...grpc.CallOption) (*proto.DriverDeleteBucketResponse, error) {
	return f.FakeDriverDeleteBucket(ctx, in, opts...)
}
func (f *FakeProvisionerClient) DriverGrantBucketAccess(ctx context.Context, in *proto.DriverGrantBucketAccessRequest, opts ...grpc.CallOption) (*proto.DriverGrantBucketAccessResponse, error) {
	return f.FakeDriverGrantBucketAccess(ctx, in, opts...)
}
func (f *FakeProvisionerClient) DriverRevokeBucketAccess(ctx context.Context, in *proto.DriverRevokeBucketAccessRequest, opts ...grpc.CallOption) (*proto.DriverRevokeBucketAccessResponse, error) {
	return f.FakeDriverRevokeBucketAccess(ctx, in, opts...)
}
