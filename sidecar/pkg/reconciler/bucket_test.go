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

package reconciler

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	cosierr "sigs.k8s.io/container-object-storage-interface/internal/errors"
	cositest "sigs.k8s.io/container-object-storage-interface/internal/test"
	controllertest "sigs.k8s.io/container-object-storage-interface/internal/test/controller"
	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
)

var (
	// use the opinionated S3 class's driver name so we can use test utils to simulate sidecar
	// behavior tests that depend on Bucket reconciliation
	s3DriverName = cositest.OpinionatedS3BucketClass().Spec.DriverName

	// valid claim used for generating dynamic buckets
	baseDynamicClaim = cosiapi.BucketClaim{
		ObjectMeta: meta.ObjectMeta{
			Name:      "my-bucket",
			Namespace: "my-ns",
			UID:       "dynamicuid",
		},
		Spec: cosiapi.BucketClaimSpec{
			BucketClassName: "s3-class",
			Protocols:       []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
		},
	}

	// valid class compatible with dynamic claim above
	baseBucketClass = cosiapi.BucketClass{
		ObjectMeta: meta.ObjectMeta{
			Name: "s3-class",
		},
		Spec: cosiapi.BucketClassSpec{
			DriverName:     s3DriverName,
			DeletionPolicy: cosiapi.BucketDeletionPolicyRetain,
			Parameters: map[string]string{
				"maxSize": "100Gi",
			},
		},
	}

	baseStaticBucket = cosiapi.Bucket{
		ObjectMeta: meta.ObjectMeta{
			Name: "static-bucket",
		},
		Spec: cosiapi.BucketSpec{
			DriverName:       s3DriverName,
			DeletionPolicy:   cosiapi.BucketDeletionPolicyRetain,
			ExistingBucketID: "static-bucket",
			Protocols:        []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
			Parameters: map[string]string{
				"maxSize": "100Gi",
			},
			BucketClaimRef: cosiapi.BucketClaimReference{
				Name:      "my-bucket",
				Namespace: "my-ns",
				UID:       "qwerty",
			},
		},
	}
)

func bucketReconcilerForClient(client client.Client, driverInfo DriverInfo) *BucketReconciler {
	return &BucketReconciler{
		Client:     client,
		Scheme:     client.Scheme(),
		DriverInfo: driverInfo,
	}
}

type bucketTestHelper interface {
	// Get the Bucket resource from dependencies for this test.
	GetBucket(deps *cositest.Dependencies) *cosiapi.Bucket

	// Return the BucketID that should be expected for this test.
	ExpectBucketId() string

	// Validate whether DriverGenerateBucketId ran, and its request parameters when it did. It
	// only runs while status.bucketID is unset, so a re-reconcile of an already-provisioned
	// Bucket passes expectRequest=false. Static provisioning never calls it and asserts the call
	// never happens either way.
	ValidateGenerateIdRequest(t *testing.T,
		generateBucketIdReq []*cosiproto.DriverGenerateBucketIdRequest,
		expectRequest bool,
	)

	// Validate the provisioning RPC calls: exactly expectBucketIds were requested, in order, on
	// whichever RPC this strategy provisions with, each carrying the expected protocols and
	// parameters. The other strategy's RPC must not have been called at all. Pass nil to assert
	// that provisioning was never attempted.
	ValidateDriverRequest(t *testing.T,
		createBucketReq []*cosiproto.DriverCreateBucketRequest,
		getBucketReq []*cosiproto.DriverGetBucketRequest,
		expectBucketIds []string,
	)
}

// dynamicBucketTestHelper knows how to get get and validate resources for dynamically-provisioned
// Bucket tests.
type dynamicBucketTestHelper struct{}

func (d *dynamicBucketTestHelper) GetBucket(deps *cositest.Dependencies) *cosiapi.Bucket {
	dynamicBucket := &cosiapi.Bucket{}
	err := deps.Client.Get(deps.ContextWithLogger, types.NamespacedName{Name: "bc-dynamicuid"}, dynamicBucket)
	if err != nil {
		dynamicBucket = nil
	}
	return dynamicBucket
}

func (d *dynamicBucketTestHelper) ExpectBucketId() string {
	return "cosi-bc-dynamicuid"
}

func (d *dynamicBucketTestHelper) ValidateGenerateIdRequest(t *testing.T, generateBucketIdReq []*cosiproto.DriverGenerateBucketIdRequest, expectRequest bool) {
	if !expectRequest {
		// e.g., status.bucketID was already persisted, so DriverGenerateBucketId must not run again
		require.Len(t, generateBucketIdReq, 0)
		return
	}

	require.Len(t, generateBucketIdReq, 1)
	genReq := generateBucketIdReq[0]
	assert.Equal(t, "bc-dynamicuid", genReq.Name)
	// DriverGenerateBucketId receives the same protocols and parameters DriverCreateBucket will
	assert.Equal(t,
		[]*cosiproto.ObjectProtocol{{Type: cosiproto.ObjectProtocol_S3}},
		genReq.Protocols,
	)
	assert.Equal(t,
		map[string]string{"maxSize": "100Gi"},
		genReq.Parameters,
	)
}

func (d *dynamicBucketTestHelper) ValidateDriverRequest(t *testing.T, createBucketReq []*cosiproto.DriverCreateBucketRequest, getBucketReq []*cosiproto.DriverGetBucketRequest, expectBucketIds []string) {
	require.Len(t, createBucketReq, len(expectBucketIds))
	require.Len(t, getBucketReq, 0)

	for i, req := range createBucketReq {
		// DriverCreateBucket must request the ID that DriverGenerateBucketId generated and COSI persisted
		assert.Equal(t, expectBucketIds[i], req.BucketId)
		assert.Equal(t,
			[]*cosiproto.ObjectProtocol{{Type: cosiproto.ObjectProtocol_S3}},
			req.Protocols,
		)
		assert.Equal(t,
			map[string]string{"maxSize": "100Gi"},
			req.Parameters,
		)
	}
}

// staticBucketTestHelper knows how to get get and validate resources for statically-provisioned
// Bucket tests.
type staticBucketTestHelper struct{}

func (s *staticBucketTestHelper) GetBucket(deps *cositest.Dependencies) *cosiapi.Bucket {
	staticBucket := &cosiapi.Bucket{}
	err := deps.Client.Get(deps.ContextWithLogger, types.NamespacedName{Name: "static-bucket"}, staticBucket)
	if err != nil {
		staticBucket = nil
	}
	return staticBucket
}

func (s *staticBucketTestHelper) ExpectBucketId() string {
	// spec.existingBucketID, not the driver's DriverGetBucket response: status.bucketID is
	// persisted before provisioning and is immutable once set
	return "static-bucket"
}

func (s *staticBucketTestHelper) ValidateGenerateIdRequest(t *testing.T, generateBucketIdReq []*cosiproto.DriverGenerateBucketIdRequest, _ bool) {
	// static provisioning takes its bucket ID from spec.existingBucketID, so it never calls
	// DriverGenerateBucketId
	require.Len(t, generateBucketIdReq, 0)
}

func (s *staticBucketTestHelper) ValidateDriverRequest(t *testing.T, createBucketReq []*cosiproto.DriverCreateBucketRequest, getBucketReq []*cosiproto.DriverGetBucketRequest, expectBucketIds []string) {
	require.Len(t, createBucketReq, 0)
	require.Len(t, getBucketReq, len(expectBucketIds))

	for i, req := range getBucketReq {
		assert.Equal(t, expectBucketIds[i], req.BucketId)
		assert.Equal(t,
			[]*cosiproto.ObjectProtocol{{Type: cosiproto.ObjectProtocol_S3}},
			req.Protocols,
		)
		assert.Equal(t,
			map[string]string{"maxSize": "100Gi"},
			req.Parameters,
		)
	}
}

// Except for rare corner cases or nonstandard unit tests, deleting a Bucket should always work.
func bucketDeletionTestSuite(t *testing.T,
	previousTestDeps *cositest.Dependencies,
	driverInfo DriverInfo,
	helper bucketTestHelper,
) {
	ctx := previousTestDeps.ContextWithLogger

	// To allow the deletion test to be portable and avoid passing an overwhelming number of args
	// to this func, run a new fake server just for deletion testing. Deletion should not need
	// Get/Create bucket calls.
	deleteBucketReq := []*cosiproto.DriverDeleteBucketRequest{}
	fakeServer := cositest.FakeProvisionerServer{
		DeleteBucketFunc: func(ctx context.Context, ddbr *cosiproto.DriverDeleteBucketRequest) (*cosiproto.DriverDeleteBucketResponse, error) {
			deleteBucketReq = append(deleteBucketReq, ddbr)
			return &cosiproto.DriverDeleteBucketResponse{}, nil
		},
	}

	cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
	defer cleanup()
	require.NoError(t, err)
	go serve()

	conn, err := cositest.RpcClientConn(tmpSock)
	require.NoError(t, err)
	rpcClient := cosiproto.NewProvisionerClient(conn)

	driverInfo.ProvisionerClient = rpcClient

	t.Run("deletionPolicy=Retain", func(t *testing.T) {
		bootstrapped := previousTestDeps.MustCopy() // copy prior test world state
		initBucket := helper.GetBucket(bootstrapped)
		require.NotNil(t, initBucket)

		initBucket.Spec.DeletionPolicy = cosiapi.BucketDeletionPolicyRetain
		require.NoError(t, bootstrapped.Client.Update(ctx, initBucket))

		t.Run("delete with claim deleting annotation", func(t *testing.T) {
			// e.g., admin deleted the Bucket resource after BucketClaim deletion
			deleteBucketReq = []*cosiproto.DriverDeleteBucketRequest{} // reset seen rpc calls

			bootstrapped := bootstrapped.MustCopy() // copy prior test world state
			ctx := bootstrapped.ContextWithLogger
			r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

			initBucket := helper.GetBucket(bootstrapped)
			if initBucket.Annotations == nil {
				initBucket.Annotations = map[string]string{}
			}
			initBucket.Annotations[cosiapi.BucketClaimBeingDeletedAnnotation] = ""
			require.NoError(t, r.Update(ctx, initBucket))
			require.NoError(t, r.Delete(ctx, initBucket))

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
			assert.Error(t, err)
			assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
			assert.ErrorContains(t, err, "will not delete Bucket with non-delete deletion policy")
			assert.Empty(t, res)

			assert.Empty(t, deleteBucketReq) // should not call driver to delete

			bucket := helper.GetBucket(bootstrapped)
			require.NotNil(t, bucket)
			assert.Equal(t, initBucket.Annotations, bucket.Annotations)
			assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer) // finalizer should not be removed
			assert.Equal(t, initBucket.Spec, bucket.Spec)
			assert.Equal(t, initBucket.Status.BucketID, bucket.Status.BucketID)
			assert.Equal(t, initBucket.Status.Protocols, bucket.Status.Protocols)
			assert.Equal(t, initBucket.Status.BucketInfo, bucket.Status.BucketInfo)
			assert.Equal(t, initBucket.Status.ReadyToUse, bucket.Status.ReadyToUse)
			assert.NotNil(t, bucket.Status.Error)
			assert.Contains(t, *bucket.Status.Error.Message, "will not delete Bucket with non-delete deletion policy")
		})

		t.Run("delete without claim deleting annotation", func(t *testing.T) {
			// e.g., admin deleted the Bucket before BucketClaim deletion
			deleteBucketReq = []*cosiproto.DriverDeleteBucketRequest{} // reset seen rpc calls

			bootstrapped := bootstrapped.MustCopy() // copy prior test world state
			ctx := bootstrapped.ContextWithLogger
			r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

			initBucket := helper.GetBucket(bootstrapped)
			require.NoError(t, r.Delete(ctx, initBucket))

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
			assert.Error(t, err)
			assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
			assert.ErrorContains(t, err, "will not delete Bucket with non-delete deletion policy")
			assert.Empty(t, res)

			assert.Empty(t, deleteBucketReq) // should not call driver to delete

			bucket := helper.GetBucket(bootstrapped)
			require.NotNil(t, bucket)
			assert.Empty(t, bucket.Annotations)
			assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer) // finalizer should not be removed
			assert.Equal(t, initBucket.Spec, bucket.Spec)
			assert.Equal(t, initBucket.Status.BucketID, bucket.Status.BucketID)
			assert.Equal(t, initBucket.Status.Protocols, bucket.Status.Protocols)
			assert.Equal(t, initBucket.Status.BucketInfo, bucket.Status.BucketInfo)
			assert.Equal(t, initBucket.Status.ReadyToUse, bucket.Status.ReadyToUse)
			assert.NotNil(t, bucket.Status.Error)
			assert.Contains(t, *bucket.Status.Error.Message, "will not delete Bucket with non-delete deletion policy")
		})

		t.Run("claim deleting annotation without delete", func(t *testing.T) {
			// standard Retain policy behavior
			deleteBucketReq = []*cosiproto.DriverDeleteBucketRequest{} // reset seen rpc calls

			bootstrapped := previousTestDeps.MustCopy() // copy prior test world state
			ctx := bootstrapped.ContextWithLogger
			r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

			initBucket := helper.GetBucket(bootstrapped)
			if initBucket.Annotations == nil {
				initBucket.Annotations = map[string]string{}
			}
			initBucket.Annotations[cosiapi.BucketClaimBeingDeletedAnnotation] = ""
			require.NoError(t, r.Update(ctx, initBucket))

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			assert.Empty(t, deleteBucketReq) // should not call driver to delete

			bucket := helper.GetBucket(bootstrapped)
			require.NotNil(t, bucket)
			assert.Equal(t, initBucket.Annotations, bucket.Annotations)
			assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer) // finalizer should not be removed
			assert.Equal(t, initBucket.Spec, bucket.Spec)
			assert.Equal(t, initBucket.Status.BucketID, bucket.Status.BucketID)
			assert.Equal(t, initBucket.Status.Protocols, bucket.Status.Protocols)
			assert.Equal(t, initBucket.Status.BucketInfo, bucket.Status.BucketInfo)
			assert.False(t, *bucket.Status.ReadyToUse)
			assert.Nil(t, bucket.Status.Error)
		})
	})

	t.Run("deletionPolicy=Delete", func(t *testing.T) {
		bootstrapped := previousTestDeps.MustCopy() // copy prior test world state
		initBucket := helper.GetBucket(bootstrapped)
		require.NotNil(t, initBucket)

		initBucket.Spec.DeletionPolicy = cosiapi.BucketDeletionPolicyDelete
		require.NoError(t, bootstrapped.Client.Update(ctx, initBucket))

		t.Run("delete with claim deleting annotation", func(t *testing.T) {
			// standard Delete policy behavior
			deleteBucketReq = []*cosiproto.DriverDeleteBucketRequest{} // reset seen rpc calls

			bootstrapped := bootstrapped.MustCopy() // copy prior test world state
			ctx := bootstrapped.ContextWithLogger
			r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

			initBucket := helper.GetBucket(bootstrapped)
			if initBucket.Annotations == nil {
				initBucket.Annotations = map[string]string{}
			}
			initBucket.Annotations[cosiapi.BucketClaimBeingDeletedAnnotation] = ""
			require.NoError(t, r.Update(ctx, initBucket))
			require.NoError(t, r.Delete(ctx, initBucket))

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			if initBucket.Status.BucketID == "" {
				// If bucket has no recorded BucketID, we cannot delete it
				// This assumes status.bucketID is being applied by the sidecar when needed, and
				// calling test/suite should verify that before this suite.
				assert.Len(t, deleteBucketReq, 0)
			} else {
				// Otherwise, we must call the driver to delete the bucket
				require.Len(t, deleteBucketReq, 1)
				deleteReq := deleteBucketReq[0]
				assert.Equal(t, initBucket.Status.BucketID, deleteReq.BucketId)
				assert.Equal(t, initBucket.Spec.Parameters, deleteReq.Parameters)
			}

			bootstrapped.AssertResourceDoesNotExist(t, cositest.NsName(initBucket), &cosiapi.Bucket{})
		})

		t.Run("delete without claim deleting annotation", func(t *testing.T) {
			// e.g., admin deleted the Bucket before BucketClaim deletion
			deleteBucketReq = []*cosiproto.DriverDeleteBucketRequest{} // reset seen rpc calls

			bootstrapped := bootstrapped.MustCopy() // copy prior test world state
			ctx := bootstrapped.ContextWithLogger
			r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

			initBucket := helper.GetBucket(bootstrapped)
			require.NoError(t, r.Delete(ctx, initBucket))

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
			assert.Error(t, err)
			assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
			assert.ErrorContains(t, err, "will not delete Bucket bound to a non-deleting BucketClaim")
			assert.Empty(t, res)

			assert.Empty(t, deleteBucketReq) // should not call driver to delete

			bucket := helper.GetBucket(bootstrapped)
			require.NotNil(t, bucket)
			assert.Empty(t, bucket.Annotations)
			assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer) // finalizer should not be removed
			assert.Equal(t, initBucket.Spec, bucket.Spec)
			assert.Equal(t, initBucket.Status.BucketID, bucket.Status.BucketID)
			assert.Equal(t, initBucket.Status.Protocols, bucket.Status.Protocols)
			assert.Equal(t, initBucket.Status.BucketInfo, bucket.Status.BucketInfo)
			assert.Equal(t, initBucket.Status.ReadyToUse, bucket.Status.ReadyToUse)
			assert.NotNil(t, bucket.Status.Error)
			assert.Contains(t, *bucket.Status.Error.Message, "will not delete Bucket bound to a non-deleting BucketClaim")
		})

		t.Run("claim deleting annotation without delete", func(t *testing.T) {
			// BucketClaim reconcile may have been interrupted before it could delete the Bucket
			deleteBucketReq = []*cosiproto.DriverDeleteBucketRequest{} // reset seen rpc calls

			bootstrapped := previousTestDeps.MustCopy() // copy prior test world state
			ctx := bootstrapped.ContextWithLogger
			r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

			initBucket := helper.GetBucket(bootstrapped)
			if initBucket.Annotations == nil {
				initBucket.Annotations = map[string]string{}
			}
			initBucket.Annotations[cosiapi.BucketClaimBeingDeletedAnnotation] = ""
			require.NoError(t, r.Update(ctx, initBucket))

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			assert.Empty(t, deleteBucketReq) // should not call driver to delete

			bucket := helper.GetBucket(bootstrapped)
			require.NotNil(t, bucket)
			assert.Equal(t, initBucket.Annotations, bucket.Annotations)
			assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer) // finalizer should not be removed
			assert.Equal(t, initBucket.Spec, bucket.Spec)
			assert.Equal(t, initBucket.Status.BucketID, bucket.Status.BucketID)
			assert.Equal(t, initBucket.Status.Protocols, bucket.Status.Protocols)
			assert.Equal(t, initBucket.Status.BucketInfo, bucket.Status.BucketInfo)
			assert.False(t, *bucket.Status.ReadyToUse)
			assert.Nil(t, bucket.Status.Error)
		})
	})
}

// run a Bucket test suite, reusable for both dynamic and static provisioning tests
type bucketTestSuiteFunc func(t *testing.T, initBucket *cosiapi.Bucket, helper bucketTestHelper)

func bucketSuccessfulProvisionTestSuite(t *testing.T, initBucket *cosiapi.Bucket, helper bucketTestHelper) {
	requestErr := error(nil) // inject an error into driver return
	getBucketReq := []*cosiproto.DriverGetBucketRequest{}
	createBucketReq := []*cosiproto.DriverCreateBucketRequest{}
	generateBucketIdReq := []*cosiproto.DriverGenerateBucketIdRequest{}
	fakeServer := cositest.FakeProvisionerServer{
		GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
			generateBucketIdReq = append(generateBucketIdReq, dgbir)
			if requestErr != nil {
				return nil, requestErr
			}
			return &cosiproto.DriverGenerateBucketIdResponse{BucketId: "cosi-" + dgbir.Name}, nil
		},
		CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
			createBucketReq = append(createBucketReq, dcbr)
			if requestErr != nil {
				return nil, requestErr
			}
			ret := &cosiproto.DriverCreateBucketResponse{
				Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
					S3: &cosiproto.S3BucketInfo{
						Endpoint:        "s3.corp.net",
						BucketId:        "corp-" + dcbr.BucketId,
						Region:          "us-east-1",
						AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
					},
				},
			}
			return ret, nil
		},
		GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
			getBucketReq = append(getBucketReq, dgebr)
			if requestErr != nil {
				return nil, requestErr
			}
			ret := cosiproto.DriverGetBucketResponse{
				Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
					S3: &cosiproto.S3BucketInfo{
						Endpoint:        "s3.corp.net",
						BucketId:        "corp-" + dgebr.BucketId,
						Region:          "us-east-1",
						AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
					},
				},
			}
			return &ret, nil
		},
	}

	cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
	defer cleanup()
	require.NoError(t, err)
	go serve()

	conn, err := cositest.RpcClientConn(tmpSock)
	require.NoError(t, err)
	rpcClient := cosiproto.NewProvisionerClient(conn)

	driverInfo := DriverInfo{
		Name:               s3DriverName,
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
		ProvisionerClient:  rpcClient,
	}

	// the test

	bootstrapped := cositest.MustBootstrap(t, initBucket)
	ctx := bootstrapped.ContextWithLogger

	r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
	assert.NoError(t, err)
	assert.Empty(t, res)

	// validate RPC request params
	helper.ValidateGenerateIdRequest(t, generateBucketIdReq, true)
	helper.ValidateDriverRequest(t, createBucketReq, getBucketReq, []string{helper.ExpectBucketId()})

	bucket := helper.GetBucket(bootstrapped)
	require.NotNil(t, bucket)

	assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer)
	assert.Equal(t, initBucket.Spec, bucket.Spec) // spec should not change
	assert.True(t, *bucket.Status.ReadyToUse)
	assert.Equal(t, helper.ExpectBucketId(), bucket.Status.BucketID)
	assert.Equal(t,
		[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
		bucket.Status.Protocols,
	)
	assert.NotEmpty(t, bucket.Status.BucketInfo)
	assert.Equal(t, "corp-"+helper.ExpectBucketId(), bucket.Status.BucketInfo["COSI_S3_BUCKET_ID"])
	for k := range bucket.Status.BucketInfo {
		assert.True(t, strings.HasPrefix(k, "COSI_S3_"))
	}

	t.Run("reconcile again", func(t *testing.T) {
		createBucketReq = []*cosiproto.DriverCreateBucketRequest{}         // reset seen rpc calls
		getBucketReq = []*cosiproto.DriverGetBucketRequest{}               // reset seen rpc calls
		generateBucketIdReq = []*cosiproto.DriverGenerateBucketIdRequest{} // reset seen rpc calls

		bootstrapped := bootstrapped.MustCopy() // copy prior test world state
		ctx := bootstrapped.ContextWithLogger
		r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

		initBucket := helper.GetBucket(bootstrapped)

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
		assert.NoError(t, err)
		assert.Empty(t, res)

		// RPC requests happen on re-reconcile also
		helper.ValidateGenerateIdRequest(t, generateBucketIdReq, false)
		helper.ValidateDriverRequest(t, createBucketReq, getBucketReq, []string{helper.ExpectBucketId()})

		bucket := helper.GetBucket(bootstrapped)
		require.NotNil(t, bucket)

		// no change to Bucket
		assert.Equal(t, initBucket.Finalizers, bucket.Finalizers)
		assert.Equal(t, initBucket.Spec, bucket.Spec)
		assert.Equal(t, initBucket.Status, bucket.Status)
	})

	t.Run("subsequent deletion", func(t *testing.T) {
		bucketDeletionTestSuite(t, bootstrapped, driverInfo, helper)
	})

	t.Run("rpc error reported", func(t *testing.T) {
		// Even though this is part of the successful provision suite, this test should be
		// sufficient to exercise RPC error handling code enough to also validate initial errors,
		// not just subsequent errors.
		requestErr = fmt.Errorf("fake rpc error") // unspecified rpc err should always be retryable

		createBucketReq = []*cosiproto.DriverCreateBucketRequest{}         // reset seen rpc calls
		getBucketReq = []*cosiproto.DriverGetBucketRequest{}               // reset seen rpc calls
		generateBucketIdReq = []*cosiproto.DriverGenerateBucketIdRequest{} // reset seen rpc calls

		bootstrapped := bootstrapped.MustCopy() // copy prior test world state
		ctx := bootstrapped.ContextWithLogger
		r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

		initBucket := helper.GetBucket(bootstrapped)

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
		assert.Error(t, err)
		assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		// Func needs to be called to return err
		helper.ValidateGenerateIdRequest(t, generateBucketIdReq, false)
		helper.ValidateDriverRequest(t, createBucketReq, getBucketReq, []string{helper.ExpectBucketId()})

		// ensure bucket status has error but no other status changes
		bucket := helper.GetBucket(bootstrapped)
		require.NotNil(t, bucket)

		assert.Equal(t, initBucket.Finalizers, bucket.Finalizers)
		assert.Equal(t, initBucket.Spec, bucket.Spec)
		assert.Equal(t, initBucket.Status.ReadyToUse, bucket.Status.ReadyToUse)
		assert.Equal(t, initBucket.Status.BucketID, bucket.Status.BucketID)
		assert.Equal(t, initBucket.Status.BucketInfo, bucket.Status.BucketInfo)
		assert.Equal(t, initBucket.Status.Protocols, bucket.Status.Protocols)
		serr := bucket.Status.Error
		require.NotNil(t, serr)
		assert.NotNil(t, serr.Time)
		assert.NotNil(t, serr.Message)
		assert.Contains(t, *serr.Message, "fake rpc error")

		requestErr = nil

		t.Run("error cleared", func(t *testing.T) {
			createBucketReq = []*cosiproto.DriverCreateBucketRequest{}         // reset seen rpc calls
			getBucketReq = []*cosiproto.DriverGetBucketRequest{}               // reset seen rpc calls
			generateBucketIdReq = []*cosiproto.DriverGenerateBucketIdRequest{} // reset seen rpc calls

			bootstrapped := bootstrapped.MustCopy() // copy prior test world state
			ctx := bootstrapped.ContextWithLogger
			r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

			initBucket := helper.GetBucket(bootstrapped)

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			// RPC requests happen on re-reconcile also
			helper.ValidateGenerateIdRequest(t, generateBucketIdReq, false)
			helper.ValidateDriverRequest(t, createBucketReq, getBucketReq, []string{helper.ExpectBucketId()})

			bucket := helper.GetBucket(bootstrapped)
			require.NotNil(t, bucket)

			// no change to Bucket
			assert.Equal(t, initBucket.Finalizers, bucket.Finalizers)
			assert.Equal(t, initBucket.Spec, bucket.Spec)
			assert.Equal(t, initBucket.Status.BucketID, bucket.Status.BucketID)
			assert.Equal(t, initBucket.Status.BucketInfo, bucket.Status.BucketInfo)
			assert.Equal(t, initBucket.Status.Protocols, bucket.Status.Protocols)
			require.Nil(t, bucket.Status.Error)
		})

		t.Run("subsequent deletion", func(t *testing.T) {
			bucketDeletionTestSuite(t, bootstrapped, driverInfo, helper)
		})
	})
}

func bucketDriverNameMismatchTestSuite(t *testing.T, baseBucket *cosiapi.Bucket, helper bucketTestHelper) {
	fakeServer := cositest.FakeProvisionerServer{} // panic on any call

	cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
	defer cleanup()
	require.NoError(t, err)
	go serve()

	conn, err := cositest.RpcClientConn(tmpSock)
	require.NoError(t, err)
	rpcClient := cosiproto.NewProvisionerClient(conn)

	driverInfo := DriverInfo{
		Name:               s3DriverName,
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
		ProvisionerClient:  rpcClient,
	}

	initBucket := baseBucket.DeepCopy()
	initBucket.Spec.DriverName = "cosi.NOMATCH.corp.net"
	bootstrapped := cositest.MustBootstrap(t, initBucket)
	ctx := bootstrapped.ContextWithLogger
	r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
	assert.NoError(t, err)
	assert.Empty(t, res)

	// ensure bucket hasn't been changed at all
	bucket := helper.GetBucket(bootstrapped)
	assert.Empty(t, bucket.Finalizers)
	assert.Equal(t, initBucket.Spec, bucket.Spec)
	assert.Equal(t, initBucket.Status, bucket.Status)

	// Deletion tests don't apply here because no finalizers present
}

func bucketProtoNotSupportedTestSuite(t *testing.T, baseBucket *cosiapi.Bucket, helper bucketTestHelper) {
	fakeServer := cositest.FakeProvisionerServer{} // panic on any call

	cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
	defer cleanup()
	require.NoError(t, err)
	go serve()

	conn, err := cositest.RpcClientConn(tmpSock)
	require.NoError(t, err)
	rpcClient := cosiproto.NewProvisionerClient(conn)

	driverInfo := DriverInfo{
		Name:               s3DriverName,
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
		ProvisionerClient:  rpcClient,
	}

	initBucket := baseBucket.DeepCopy()
	initBucket.Spec.Protocols = []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolGcs} // not supported
	bootstrapped := cositest.MustBootstrap(t, initBucket)
	ctx := bootstrapped.ContextWithLogger
	r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
	assert.Error(t, err)
	assert.ErrorIs(t, err, reconcile.TerminalError(nil))
	assert.ErrorContains(t, err, "does not support protocols")
	assert.ErrorContains(t, err, "GCS") // the unsupported protocol is listed
	assert.Empty(t, res)

	bucket := helper.GetBucket(bootstrapped)
	assert.Empty(t, bucket.Finalizers)
	assert.Equal(t, initBucket.Spec, bucket.Spec)
	assert.Equal(t, initBucket.Status.BucketID, bucket.Status.BucketID)
	assert.Equal(t, initBucket.Status.BucketInfo, bucket.Status.BucketInfo)
	assert.Equal(t, initBucket.Status.Protocols, bucket.Status.Protocols)
	require.NotNil(t, bucket.Status.Error)
	assert.Contains(t, *bucket.Status.Error.Message, "does not support protocols")
	assert.Contains(t, *bucket.Status.Error.Message, "GCS")
	assert.NotNil(t, bucket.Status.Error.Time)

	// Deletion tests don't apply here because no finalizers present
}

func bucketProvisionedWithWrongProtoTestSuite(t *testing.T, initBucket *cosiapi.Bucket, helper bucketTestHelper) {
	getBucketReq := []*cosiproto.DriverGetBucketRequest{}
	createBucketReq := []*cosiproto.DriverCreateBucketRequest{}
	generateBucketIdReq := []*cosiproto.DriverGenerateBucketIdRequest{}
	fakeServer := cositest.FakeProvisionerServer{
		GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
			generateBucketIdReq = append(generateBucketIdReq, dgbir)
			return &cosiproto.DriverGenerateBucketIdResponse{BucketId: "cosi-" + dgbir.Name}, nil
		},
		CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
			createBucketReq = append(createBucketReq, dcbr)
			ret := &cosiproto.DriverCreateBucketResponse{
				Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
					Azure: &cosiproto.AzureBucketInfo{}, // bucket.spec wants S3
				},
			}
			return ret, nil
		},
		GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
			getBucketReq = append(getBucketReq, dgebr)
			ret := cosiproto.DriverGetBucketResponse{
				Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
					Azure: &cosiproto.AzureBucketInfo{}, // bucket.spec wants S3
				},
			}
			return &ret, nil
		},
	}

	cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
	defer cleanup()
	require.NoError(t, err)
	go serve()

	conn, err := cositest.RpcClientConn(tmpSock)
	require.NoError(t, err)
	rpcClient := cosiproto.NewProvisionerClient(conn)

	driverInfo := DriverInfo{
		Name:               s3DriverName,
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
		ProvisionerClient:  rpcClient,
	}

	bootstrapped := cositest.MustBootstrap(t, initBucket)
	ctx := bootstrapped.ContextWithLogger
	r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
	assert.Error(t, err)
	assert.ErrorIs(t, err, reconcile.TerminalError(nil))
	assert.ErrorContains(t, err, "protocols are not supported")
	assert.ErrorContains(t, err, "S3") // required proto
	assert.Empty(t, res)

	// validate RPC request params
	helper.ValidateGenerateIdRequest(t, generateBucketIdReq, true)
	helper.ValidateDriverRequest(t, createBucketReq, getBucketReq, []string{helper.ExpectBucketId()})

	bucket := helper.GetBucket(bootstrapped)
	require.NotNil(t, bucket)

	assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer)
	assert.Equal(t, initBucket.Spec, bucket.Spec)
	assert.False(t, *bucket.Status.ReadyToUse)
	// the ID is persisted before provisioning, so a provisioning failure still leaves it behind
	assert.Equal(t, helper.ExpectBucketId(), bucket.Status.BucketID)
	assert.Empty(t, bucket.Status.BucketInfo)
	assert.Empty(t, bucket.Status.Protocols)
	serr := bucket.Status.Error
	require.NotNil(t, serr)
	assert.NotNil(t, serr.Time)
	assert.NotNil(t, serr.Message)
	assert.Contains(t, *serr.Message, "protocols are not supported")
	assert.Contains(t, *serr.Message, "S3") // required proto

	t.Run("subsequent deletion", func(t *testing.T) {
		bucketDeletionTestSuite(t, bootstrapped, driverInfo, helper)
	})
}

// Bucket ID persistence behaviors that apply to both provisioning strategies. Both record
// status.bucketID before asking the driver to provision, so both must be checked: dynamic mints
// the ID with DriverGenerateBucketId, static copies it from spec.existingBucketID.
func bucketIdPersistenceTestSuite(t *testing.T, initBucket *cosiapi.Bucket, helper bucketTestHelper) {
	nsName := cositest.NsName(initBucket)

	// provisionFailedErrMsg is what the provisioning RPC returns to stop a reconcile after
	// status.bucketID is
	// recorded. The code is retryable, so the reconcile ends the way a transient backend failure
	// would rather than terminally.
	const provisionFailedErrMsg = "fake backend unavailable"

	// DriverGenerateBucketId always succeeds here; these tests turn on provisioning failing after
	// the ID was recorded, so only the provisioning RPC needs an error knob.
	provisionErr := error(nil) // inject an error into the provisioning driver return

	generateBucketIdReq := []*cosiproto.DriverGenerateBucketIdRequest{}
	createBucketReq := []*cosiproto.DriverCreateBucketRequest{}
	getBucketReq := []*cosiproto.DriverGetBucketRequest{}
	resetSeenRequests := func() {
		generateBucketIdReq = []*cosiproto.DriverGenerateBucketIdRequest{}
		createBucketReq = []*cosiproto.DriverCreateBucketRequest{}
		getBucketReq = []*cosiproto.DriverGetBucketRequest{}
	}

	fakeServer := cositest.FakeProvisionerServer{
		GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
			generateBucketIdReq = append(generateBucketIdReq, dgbir)
			return &cosiproto.DriverGenerateBucketIdResponse{BucketId: "cosi-" + dgbir.Name}, nil
		},
		CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
			createBucketReq = append(createBucketReq, dcbr)
			if provisionErr != nil {
				return nil, provisionErr
			}
			return &cosiproto.DriverCreateBucketResponse{
				Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
					S3: &cosiproto.S3BucketInfo{
						Endpoint:        "s3.corp.net",
						BucketId:        "corp-" + dcbr.BucketId,
						Region:          "us-east-1",
						AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
					},
				},
			}, nil
		},
		GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
			getBucketReq = append(getBucketReq, dgebr)
			if provisionErr != nil {
				return nil, provisionErr
			}
			return &cosiproto.DriverGetBucketResponse{
				Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
					S3: &cosiproto.S3BucketInfo{
						Endpoint:        "s3.corp.net",
						BucketId:        "corp-" + dgebr.BucketId,
						Region:          "us-east-1",
						AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
					},
				},
			}, nil
		},
	}

	cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
	defer cleanup()
	require.NoError(t, err)
	go serve()

	conn, err := cositest.RpcClientConn(tmpSock)
	require.NoError(t, err)
	rpcClient := cosiproto.NewProvisionerClient(conn)

	driverInfo := DriverInfo{
		Name:               s3DriverName,
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
		ProvisionerClient:  rpcClient,
	}

	t.Run("bucket ID persisted before bucket created", func(t *testing.T) {
		// Setup: the ID is generated and recorded, then provisioning fails. Stopping the reconcile
		// between those two steps is
		// what makes the ordering observable from outside: whatever is in status afterwards is what
		// COSI had durably recorded before it asked the driver to create anything.
		provisionErr = status.Error(codes.Unavailable, provisionFailedErrMsg)
		resetSeenRequests()

		bootstrapped := cositest.MustBootstrap(t, initBucket.DeepCopy())
		ctx := bootstrapped.ContextWithLogger
		r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

		// Act: reconcile, which records the bucket ID and then fails to provision.
		res, reconcileErr := r.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		require.Error(t, reconcileErr) // provisioning must have failed, or this is not the state wanted
		require.Empty(t, res)

		// classified retryable, so the Bucket is requeued rather than abandoned; this reconcile
		// does not itself retry, and the driver keeps failing until the subtest below clears
		// provisionErr
		assert.NotErrorIs(t, reconcileErr, reconcile.TerminalError(nil))

		// Validate: provisioning was asked for the ID that was just recorded.
		helper.ValidateGenerateIdRequest(t, generateBucketIdReq, true)
		helper.ValidateDriverRequest(t, createBucketReq, getBucketReq, []string{helper.ExpectBucketId()})

		// Validate: the ID outlived the failed provision, so any backend bucket the driver may
		// have created before failing is still reachable by an ID Kubernetes holds.
		bucket := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, nsName, bucket))
		assert.Equal(t, helper.ExpectBucketId(), bucket.Status.BucketID)
		require.NotNil(t, bucket.Status.ReadyToUse)
		assert.False(t, *bucket.Status.ReadyToUse) // provisioning never completed
		assert.Empty(t, bucket.Status.Protocols)
		assert.Empty(t, bucket.Status.BucketInfo)

		serr := bucket.Status.Error
		require.NotNil(t, serr)
		assert.NotNil(t, serr.Time)
		require.NotNil(t, serr.Message)
		assert.Contains(t, *serr.Message, provisionFailedErrMsg)

		t.Run("successful provision after", func(t *testing.T) {
			// Setup: same Bucket, now with provisioning succeeding. DriverGenerateBucketId still
			// answers with a usable
			// ID, so only a deliberate refusal to regenerate keeps the persisted one. The starting
			// state comes from the parent test's reconcile, not from hand-written status.
			provisionErr = nil
			resetSeenRequests()

			// Act: reconcile again, resuming from the persisted ID.
			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
			assert.NoError(t, err)
			assert.Empty(t, res)

			// Validate: DriverGenerateBucketId never ran again, and provisioning reused the same ID.
			helper.ValidateGenerateIdRequest(t, generateBucketIdReq, false)
			helper.ValidateDriverRequest(t, createBucketReq, getBucketReq, []string{helper.ExpectBucketId()})

			bucket := &cosiapi.Bucket{}
			require.NoError(t, r.Get(ctx, nsName, bucket))
			assert.Equal(t, helper.ExpectBucketId(), bucket.Status.BucketID)
			assert.True(t, *bucket.Status.ReadyToUse)
			assert.Nil(t, bucket.Status.Error) // the earlier provisioning failure is cleared
			assert.NotEmpty(t, bucket.Status.BucketInfo)

			t.Run("subsequent deletion", func(t *testing.T) {
				bucketDeletionTestSuite(t, bootstrapped, driverInfo, helper)
			})
		})

		t.Run("subsequent deletion", func(t *testing.T) {
			bucketDeletionTestSuite(t, bootstrapped, driverInfo, helper)
		})
	})

	t.Run("bucket not created if ID not persisted", func(t *testing.T) {
		// Setup: DriverGenerateBucketId succeeds but the status write that records its ID fails.
		// This is what pins the ordering: a failed write must abort the reconcile before
		// provisioning. Were the ID
		// instead recorded after provisioning, the driver would already have been asked to create
		// the bucket by the time that write failed.
		provisionErr = nil
		resetSeenRequests()

		scheme := runtime.NewScheme()
		require.NoError(t, cosiapi.AddToScheme(scheme))

		b := initBucket.DeepCopy()
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(b).
			WithStatusSubresource(&cosiapi.Bucket{}).
			Build()
		interceptedClient := interceptor.NewClient(fakeClient, interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				return apierrors.NewConflict(
					schema.GroupResource{Group: cosiapi.GroupVersion.Group, Resource: "buckets"},
					obj.GetName(),
					fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again"),
				)
			},
		})

		r := bucketReconcilerForClient(interceptedClient, driverInfo)

		// Act: reconcile, which generates an ID and then fails to record it.
		res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nsName})
		require.Error(t, err)
		assert.Empty(t, res)

		// Validate: no backend bucket was requested for an ID Kubernetes does not hold.
		helper.ValidateDriverRequest(t, createBucketReq, getBucketReq, nil)

		// Deletion tests don't apply here because this test needs an intercepted client to fail
		// status writes, and bucketDeletionTestSuite takes cositest.Dependencies. The Bucket ends
		// in the same state as "bucket ID generation fails" - finalizer present, no bucketID -
		// whose subsequent deletion test covers deleting from it.
	})

	t.Run("readyToUse=true with no bucket ID refused", func(t *testing.T) {
		// A Bucket with readyToUse=true but no bucketID is a combination this sidecar never
		// produces. COSI cannot tell whether status data was lost or hand-edited, so it must
		// refuse to provision rather than act on an impossible state.
		provisionErr = nil
		resetSeenRequests()

		b := initBucket.DeepCopy()
		b.Status.ReadyToUse = ptr.To(true)
		bootstrapped := cositest.MustBootstrap(t, b)
		ctx := bootstrapped.ContextWithLogger

		reconciler := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

		// Act: reconcile the inconsistent Bucket.
		res, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})

		// Expect: a terminal error, since no retry can resolve a status this sidecar cannot have
		// written.
		require.Error(t, err)
		assert.ErrorContains(t, err, "readyToUse is true but no bucket ID is assigned")
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		// Validate: the refusal happened before any driver call, and is visible via
		// `kubectl describe`, not just the sidecar log.
		helper.ValidateGenerateIdRequest(t, generateBucketIdReq, false)
		helper.ValidateDriverRequest(t, createBucketReq, getBucketReq, nil)

		bucket := &cosiapi.Bucket{}
		require.NoError(t, reconciler.Get(ctx, nsName, bucket))
		assert.Empty(t, bucket.Status.BucketID)
		require.NotNil(t, bucket.Status.Error)
		require.NotNil(t, bucket.Status.Error.Message)
		assert.Contains(t, *bucket.Status.Error.Message, "readyToUse is true but no bucket ID is assigned")

		t.Run("subsequent deletion", func(t *testing.T) {
			bucketDeletionTestSuite(t, bootstrapped, driverInfo, helper)
		})
	})

}

// DriverGenerateBucketId failure behaviors, which only apply to dynamic provisioning: static
// provisioning takes its bucket ID from spec.existingBucketID and never calls it.
func bucketGenerateIdFailureTestSuite(t *testing.T, initBucket *cosiapi.Bucket, helper bucketTestHelper) {
	nsName := cositest.NsName(initBucket)

	generateIdErr := error(nil) // inject an error into the DriverGenerateBucketId return
	generatedId := ""           // override the ID DriverGenerateBucketId returns; empty means the default

	generateBucketIdReq := []*cosiproto.DriverGenerateBucketIdRequest{}
	createBucketReq := []*cosiproto.DriverCreateBucketRequest{}
	getBucketReq := []*cosiproto.DriverGetBucketRequest{}
	resetSeenRequests := func() {
		generateBucketIdReq = []*cosiproto.DriverGenerateBucketIdRequest{}
		createBucketReq = []*cosiproto.DriverCreateBucketRequest{}
		getBucketReq = []*cosiproto.DriverGetBucketRequest{}
	}

	// Provisioning succeeds here so that a regression which provisioned anyway would produce a
	// healthy Bucket, which the assertions below would catch; a provisioning RPC that errored
	// could not tell that apart from DriverGenerateBucketId having stopped the reconcile.
	fakeServer := cositest.FakeProvisionerServer{
		GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
			generateBucketIdReq = append(generateBucketIdReq, dgbir)
			if generateIdErr != nil {
				return nil, generateIdErr
			}
			if generatedId != "" {
				return &cosiproto.DriverGenerateBucketIdResponse{BucketId: generatedId}, nil
			}
			return &cosiproto.DriverGenerateBucketIdResponse{BucketId: "cosi-" + dgbir.Name}, nil
		},
		CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
			createBucketReq = append(createBucketReq, dcbr)
			return &cosiproto.DriverCreateBucketResponse{
				Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
					S3: &cosiproto.S3BucketInfo{
						Endpoint:        "s3.corp.net",
						BucketId:        "corp-" + dcbr.BucketId,
						Region:          "us-east-1",
						AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
					},
				},
			}, nil
		},
		GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
			getBucketReq = append(getBucketReq, dgebr)
			return &cosiproto.DriverGetBucketResponse{
				Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
					S3: &cosiproto.S3BucketInfo{
						Endpoint:        "s3.corp.net",
						BucketId:        "corp-" + dgebr.BucketId,
						Region:          "us-east-1",
						AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
					},
				},
			}, nil
		},
	}

	cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
	defer cleanup()
	require.NoError(t, err)
	go serve()

	conn, err := cositest.RpcClientConn(tmpSock)
	require.NoError(t, err)
	rpcClient := cosiproto.NewProvisionerClient(conn)

	driverInfo := DriverInfo{
		Name:               s3DriverName,
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
		ProvisionerClient:  rpcClient,
	}

	t.Run("bucket ID generation fails", func(t *testing.T) {
		// Setup: DriverGenerateBucketId returns a non-retryable RPC error.
		generateIdErr = status.Error(codes.InvalidArgument, "fake invalid arg err")
		generatedId = ""
		resetSeenRequests()

		bootstrapped := cositest.MustBootstrap(t, initBucket.DeepCopy())
		ctx := bootstrapped.ContextWithLogger
		r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

		// Act: reconcile; ID generation must fail before provisioning ever runs.
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		// Validate: no ID was persisted, readyToUse stays false, no bucket was provisioned, and
		// the terminal error surfaces in status.error.
		helper.ValidateDriverRequest(t, createBucketReq, getBucketReq, nil)

		bucket := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, nsName, bucket))
		assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer)
		assert.Empty(t, bucket.Status.BucketID)
		assert.False(t, *bucket.Status.ReadyToUse)
		serr := bucket.Status.Error
		require.NotNil(t, serr)
		assert.Contains(t, *serr.Message, "fake invalid arg err")

		t.Run("subsequent deletion", func(t *testing.T) {
			bucketDeletionTestSuite(t, bootstrapped, driverInfo, helper)
		})
	})

	t.Run("generated ID violates pattern", func(t *testing.T) {
		// Setup: DriverGenerateBucketId returns an ID containing characters outside the
		// status.bucketID schema.
		generateIdErr = nil
		generatedId = "acme:bucket 1" // colon and space are outside ^[a-zA-Z0-9/._-]+$
		resetSeenRequests()

		bootstrapped := cositest.MustBootstrap(t, initBucket.DeepCopy())
		ctx := bootstrapped.ContextWithLogger
		r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

		// Act: reconcile with a driver that returns a schema-violating ID.
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		assert.Error(t, err)
		// must not retry forever against a driver ID that will never satisfy the schema
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		// Validate: rejected before persisting and before provisioning, with a terminal error
		// naming the bad ID.
		helper.ValidateDriverRequest(t, createBucketReq, getBucketReq, nil)

		bucket := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, nsName, bucket))
		assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer)
		assert.Empty(t, bucket.Status.BucketID)
		require.NotNil(t, bucket.Status.ReadyToUse)
		assert.False(t, *bucket.Status.ReadyToUse)
		serr := bucket.Status.Error
		require.NotNil(t, serr)
		assert.NotNil(t, serr.Time)
		require.NotNil(t, serr.Message)
		assert.Contains(t, *serr.Message, "acme:bucket 1")
		assert.Contains(t, *serr.Message, "is invalid")

		t.Run("subsequent deletion", func(t *testing.T) {
			bucketDeletionTestSuite(t, bootstrapped, driverInfo, helper)
		})
	})

	t.Run("generated ID exceeds length limit", func(t *testing.T) {
		// Setup: DriverGenerateBucketId returns an ID one character over the 2048-char limit.
		generateIdErr = nil
		generatedId = strings.Repeat("a", 2049)
		resetSeenRequests()

		bootstrapped := cositest.MustBootstrap(t, initBucket.DeepCopy())
		ctx := bootstrapped.ContextWithLogger
		r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

		// Act: reconcile with a driver that returns an over-length ID.
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		// Validate: rejected before persisting and before provisioning, with a terminal error
		// citing the schema violation.
		helper.ValidateDriverRequest(t, createBucketReq, getBucketReq, nil)

		bucket := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, nsName, bucket))
		assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer)
		assert.Empty(t, bucket.Status.BucketID)
		require.NotNil(t, bucket.Status.ReadyToUse)
		assert.False(t, *bucket.Status.ReadyToUse)
		serr := bucket.Status.Error
		require.NotNil(t, serr)
		assert.NotNil(t, serr.Time)
		require.NotNil(t, serr.Message)
		assert.Contains(t, *serr.Message, "is invalid")

		t.Run("subsequent deletion", func(t *testing.T) {
			bucketDeletionTestSuite(t, bootstrapped, driverInfo, helper)
		})
	})

}
func bucketResourceMissingTestSuite(t *testing.T, initBucket *cosiapi.Bucket, helper bucketTestHelper) {
	fakeServer := cositest.FakeProvisionerServer{} // panic on any call

	cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
	defer cleanup()
	require.NoError(t, err)
	go serve()

	conn, err := cositest.RpcClientConn(tmpSock)
	require.NoError(t, err)
	rpcClient := cosiproto.NewProvisionerClient(conn)

	driverInfo := DriverInfo{
		Name:               s3DriverName,
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
		ProvisionerClient:  rpcClient,
	}

	bootstrapped := cositest.MustBootstrap(t) // no bucket!
	ctx := bootstrapped.ContextWithLogger
	r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(initBucket)})
	assert.NoError(t, err)
	assert.Empty(t, res)

	// Deletion tests don't apply here because no bucket resource exists
}

func TestBucketReconciler_Reconcile(t *testing.T) {
	// generate the dynamically-provisioned bucket's starting state
	var baseDynamicBucket *cosiapi.Bucket
	{
		bootstrapped := cositest.MustBootstrap(t,
			baseDynamicClaim.DeepCopy(),
			baseBucketClass.DeepCopy(),
		)

		_, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(&baseDynamicClaim))
		require.NoError(t, err)

		baseDynamicBucket = new(dynamicBucketTestHelper).GetBucket(bootstrapped)
		require.NotNil(t, baseDynamicBucket)
	}

	type testDef struct {
		name          string
		testSuiteFunc bucketTestSuiteFunc
	}
	tests := []testDef{
		{"successful provision", bucketSuccessfulProvisionTestSuite},
		{"driver name mismatch", bucketDriverNameMismatchTestSuite},
		{"proto not supported", bucketProtoNotSupportedTestSuite},
		{"provisioned with wrong proto", bucketProvisionedWithWrongProtoTestSuite},
		{"bucket resource missing", bucketResourceMissingTestSuite},
		{"bucket ID persistence", bucketIdPersistenceTestSuite},
	}

	t.Run("dynamic provisioning", func(t *testing.T) {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				test.testSuiteFunc(t, baseDynamicBucket.DeepCopy(), &dynamicBucketTestHelper{})
			})
		}

		// unique condition for dynamic provisioning
		t.Run("generate id failures", func(t *testing.T) {
			bucketGenerateIdFailureTestSuite(t, baseDynamicBucket.DeepCopy(), &dynamicBucketTestHelper{})
		})
	})

	t.Run("static provisioning", func(t *testing.T) {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				test.testSuiteFunc(t, baseStaticBucket.DeepCopy(), &staticBucketTestHelper{})
			})
		}

		// unique condition for static provisioning
		t.Run("backend bucket not found", func(t *testing.T) {
			getBucketReq := []*cosiproto.DriverGetBucketRequest{}
			fakeServer := cositest.FakeProvisionerServer{
				GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
					getBucketReq = append(getBucketReq, dgebr)
					return nil, status.Error(codes.NotFound, "bucket does not exist in backend")
				},
			}

			cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
			defer cleanup()
			require.NoError(t, err)
			go serve()

			conn, err := cositest.RpcClientConn(tmpSock)
			require.NoError(t, err)
			rpcClient := cosiproto.NewProvisionerClient(conn)

			driverInfo := DriverInfo{
				Name:               s3DriverName,
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  rpcClient,
			}

			bootstrapped := cositest.MustBootstrap(t, baseStaticBucket.DeepCopy())
			ctx := bootstrapped.ContextWithLogger
			r := bucketReconcilerForClient(bootstrapped.Client, driverInfo)

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseStaticBucket)})
			assert.Error(t, err)
			assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
			assert.Empty(t, res)
			require.Len(t, getBucketReq, 1)
			assert.Equal(t, "static-bucket", getBucketReq[0].BucketId)

			bucket := new(staticBucketTestHelper).GetBucket(bootstrapped)
			require.NotNil(t, bucket)
			assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer)
			assert.Equal(t, baseStaticBucket.Spec, bucket.Spec)
			serr := bucket.Status.Error
			require.NotNil(t, serr)
			assert.NotNil(t, serr.Time)
			assert.NotNil(t, serr.Message)
			assert.Contains(t, *serr.Message, "waiting for backend bucket to exist")
		})
	})
}

func TestBucketReconciler_dynamicProvision(t *testing.T) {
	validClaimRef := cosiapi.BucketClaimReference{
		Name:      "userbucket",
		Namespace: "usernamespace",
		UID:       "qwerty",
	}
	t.Run("valid driver and bucket, successful provision", func(t *testing.T) {
		requestParams := map[string]string{} // record the params sent in the request to verify later

		fakeServer := cositest.FakeProvisionerServer{
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				requestParams = dcbr.Parameters
				ret := &cosiproto.DriverCreateBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{
							Endpoint:        "s3.corp.net",
							BucketId:        "backend-" + dcbr.BucketId, // example of backend bucket with slight variation from the request
							Region:          "us-east-1",
							AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
						},
					},
				}
				return ret, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		inputParams := map[string]string{
			"key":    "value",
			"option": "setting",
		}

		details, err := r.dynamicProvision(context.Background(), logr.Discard(), dynamicProvisionParams{
			bucketID: "bc-qwerty",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: inputParams,
			claimRef:   validClaimRef,
		})
		assert.NoError(t, err)
		assert.Equal(t, []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3}, details.supportedProtos)
		// If we check the exact results of details.allProtoBucketInfo, we will tie the unit tests
		// to the specific implementation of the S3 bucket info translator, tested elsewhere.
		// Instead, check only COSI_S3_BUCKET_ID which is unlikely to change in the future, and
		// check that all info is prefixed `COSI_S3_`.
		assert.NotEmpty(t, details.allProtoBucketInfo)
		assert.Equal(t, "backend-bc-qwerty", details.allProtoBucketInfo[string(cosiapi.BucketInfoVar_S3_BucketId)])
		for k := range details.allProtoBucketInfo {
			assert.True(t, strings.HasPrefix(k, "COSI_S3_"))
		}
		assert.Equal(t, inputParams, requestParams)
	})

	t.Run("valid driver and bucket, retryable provision error", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				if len(dcbr.Parameters) != 0 {
					t.Errorf("expecting request parameters to be empty")
				}
				return &cosiproto.DriverCreateBucketResponse{}, status.Error(codes.Unknown, "fake unknown err")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		details, err := r.dynamicProvision(context.Background(), logr.Discard(), dynamicProvisionParams{
			bucketID: "bc-qwerty",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: map[string]string{}, // intentionally empty
			claimRef:   validClaimRef,
		})
		assert.Error(t, err)
		assert.ErrorContains(t, err, "fake unknown err")
		assert.NotErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, details)
	})

	t.Run("valid driver and bucket, non-retryable provision error", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				return &cosiproto.DriverCreateBucketResponse{}, status.Error(codes.InvalidArgument, "fake invalid arg err")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		details, err := r.dynamicProvision(context.Background(), logr.Discard(), dynamicProvisionParams{
			bucketID: "bc-qwerty",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: map[string]string{}, // intentionally empty
			claimRef:   validClaimRef,
		})
		assert.Error(t, err)
		assert.ErrorContains(t, err, "fake invalid arg err")
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, details)
	})

	t.Run("valid driver, claim ref malformed", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				return &cosiproto.DriverCreateBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{
							Endpoint:        "s3.corp.net",
							BucketId:        "backend-" + dcbr.BucketId, // example of backend bucket with slight variation from the request
							Region:          "us-east-1",
							AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
						},
					},
				}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		details, err := r.dynamicProvision(context.Background(), logr.Discard(), dynamicProvisionParams{
			bucketID: "bc-qwerty",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: map[string]string{}, // intentionally empty
			claimRef:   cosiapi.BucketClaimReference{},
		})
		assert.Error(t, err)
		assert.ErrorContains(t, err, "all bucketClaimRef fields must be set")
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, details)
	})

	t.Run("valid driver, proto response nil", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				return &cosiproto.DriverCreateBucketResponse{
					Protocols: nil,
				}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		details, err := r.dynamicProvision(context.Background(), logr.Discard(), dynamicProvisionParams{
			bucketID: "bc-qwerty",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: map[string]string{}, // intentionally empty
			claimRef:   validClaimRef,
		})
		assert.Error(t, err)
		assert.ErrorContains(t, err, "protocol response missing")
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, details)
	})

	t.Run("valid driver, empty S3 proto response", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				return &cosiproto.DriverCreateBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{},
					},
				}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		details, err := r.dynamicProvision(context.Background(), logr.Discard(), dynamicProvisionParams{
			bucketID: "bc-qwerty",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: map[string]string{}, // intentionally empty
			claimRef:   validClaimRef,
		})
		assert.NoError(t, err)
		assert.Equal(t, []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3}, details.supportedProtos)
		assert.NotEmpty(t, details.allProtoBucketInfo) // bucket info should be present
		for k, v := range details.allProtoBucketInfo {
			assert.True(t, strings.HasPrefix(k, "COSI_S3_"))
			assert.Empty(t, v) // but all info will be empty string
		}
	})

	t.Run("valid driver, empty Azure proto response", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				return &cosiproto.DriverCreateBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						Azure: &cosiproto.AzureBucketInfo{},
					},
				}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_AZURE},
				ProvisionerClient:  client,
			},
		}

		details, err := r.dynamicProvision(context.Background(), logr.Discard(), dynamicProvisionParams{
			bucketID: "bc-qwerty",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_AZURE},
			},
			parameters: map[string]string{}, // intentionally empty
			claimRef:   validClaimRef,
		})
		assert.NoError(t, err)
		assert.Equal(t, []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolAzure}, details.supportedProtos)
		assert.NotEmpty(t, details.allProtoBucketInfo) // bucket info should be present
		for k, v := range details.allProtoBucketInfo {
			assert.True(t, strings.HasPrefix(k, "COSI_AZURE_"))
			assert.Empty(t, v) // but all info will be empty string
		}
	})

	t.Run("valid driver, empty GCS proto response", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				return &cosiproto.DriverCreateBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						Gcs: &cosiproto.GcsBucketInfo{},
					},
				}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_GCS},
				ProvisionerClient:  client,
			},
		}

		details, err := r.dynamicProvision(context.Background(), logr.Discard(), dynamicProvisionParams{
			bucketID: "bc-qwerty",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_GCS},
			},
			parameters: map[string]string{}, // intentionally empty
			claimRef:   validClaimRef,
		})
		assert.NoError(t, err)
		assert.Equal(t, []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolGcs}, details.supportedProtos)
		assert.NotEmpty(t, details.allProtoBucketInfo) // bucket info should be present
		for k, v := range details.allProtoBucketInfo {
			assert.True(t, strings.HasPrefix(k, "COSI_GCS_"))
			assert.Empty(t, v) // but all info will be empty string
		}
	})

	t.Run("valid driver, empty S3+Azure proto response", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				return &cosiproto.DriverCreateBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						S3:    &cosiproto.S3BucketInfo{},
						Azure: &cosiproto.AzureBucketInfo{},
					},
				}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name: "cosi.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{
					cosiproto.ObjectProtocol_S3,
					cosiproto.ObjectProtocol_AZURE,
				},
				ProvisionerClient: client,
			},
		}

		details, err := r.dynamicProvision(context.Background(), logr.Discard(), dynamicProvisionParams{
			bucketID: "bc-qwerty",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3}, // example of request for S3, returned support for S3+Azure
			},
			parameters: map[string]string{}, // intentionally empty
			claimRef:   validClaimRef,
		})
		assert.NoError(t, err)
		assert.ElementsMatch(t,
			[]cosiapi.ObjectProtocol{
				cosiapi.ObjectProtocolS3,
				cosiapi.ObjectProtocolAzure,
			},
			details.supportedProtos,
		)
		assert.NotEmpty(t, details.allProtoBucketInfo) // bucket info should be present
		for k, v := range details.allProtoBucketInfo {
			assert.True(t, strings.HasPrefix(k, "COSI_S3_") || strings.HasPrefix(k, "COSI_AZURE_"))
			assert.Empty(t, v) // but all info will be empty string
		}
	})

}

func TestBucketReconciler_generateBucketID(t *testing.T) {
	// DriverGenerateBucketId receives the same protocol list DriverCreateBucket will
	testProtos := []*cosiproto.ObjectProtocol{{Type: cosiproto.ObjectProtocol_S3}}

	baseBucket := cosiapi.Bucket{
		ObjectMeta: meta.ObjectMeta{
			Name: "bc-qwerty",
		},
		Spec: cosiapi.BucketSpec{
			DriverName:     "cosi.s3.corp.net",
			DeletionPolicy: cosiapi.BucketDeletionPolicyRetain,
			Protocols:      []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
			Parameters:     map[string]string{"maxSize": "10Gi"},
			BucketClaimRef: cosiapi.BucketClaimReference{
				Name:      "my-bucket",
				Namespace: "my-ns",
				UID:       "qwerty",
			},
		},
	}

	bucketNsName := types.NamespacedName{Name: "bc-qwerty"}

	// newReconciler wires a reconciler to an RPC server backed by generateFunc, and returns the
	// reconciler plus the Bucket it operates on.
	// nolint:lll
	newReconciler := func(t *testing.T, generateFunc func(context.Context, *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error)) (*BucketReconciler, *cosiapi.Bucket, context.Context) {
		t.Helper()

		fakeServer := cositest.FakeProvisionerServer{GenerateBucketIdFunc: generateFunc}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		t.Cleanup(cleanup)
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)

		b := baseBucket.DeepCopy()
		bootstrapped := cositest.MustBootstrap(t, b)

		r := bucketReconcilerForClient(bootstrapped.Client, DriverInfo{
			Name:               "cosi.s3.corp.net",
			SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
			ProvisionerClient:  cosiproto.NewProvisionerClient(conn),
		})

		bucket := &cosiapi.Bucket{}
		require.NoError(t, r.Get(bootstrapped.ContextWithLogger, bucketNsName, bucket))
		return r, bucket, bootstrapped.ContextWithLogger
	}

	// genParams builds the DriverGenerateBucketId params from the Bucket under test, mirroring
	// what reconcile()
	// passes in.
	genParams := func(b *cosiapi.Bucket) generateIdParams {
		return generateIdParams{
			name:           b.Name,
			requiredProtos: testProtos,
			parameters:     b.Spec.Parameters,
		}
	}

	t.Run("ID generated", func(t *testing.T) {
		// Setup: fake driver that echoes back a deterministic ID derived from the request name.
		seenReq := []*cosiproto.DriverGenerateBucketIdRequest{}
		r, bucket, ctx := newReconciler(t, func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
			seenReq = append(seenReq, dgbir)
			return &cosiproto.DriverGenerateBucketIdResponse{BucketId: "cosi-" + dgbir.Name}, nil
		})

		// Act: run DriverGenerateBucketId in isolation.
		bucketID, err := r.generateBucketID(ctx, logr.Discard(), genParams(bucket))
		assert.NoError(t, err)

		// Validate: the driver is asked for an ID using the Bucket resource name
		require.Len(t, seenReq, 1)
		assert.Equal(t, "bc-qwerty", seenReq[0].Name)
		// DriverGenerateBucketId receives the same protocols and parameters DriverCreateBucket will
		assert.Equal(t,
			[]*cosiproto.ObjectProtocol{{Type: cosiproto.ObjectProtocol_S3}},
			seenReq[0].Protocols,
		)
		assert.Equal(t, map[string]string{"maxSize": "10Gi"}, seenReq[0].Parameters)

		// generateBucketID only returns the ID; recording and persisting it is the caller's job,
		// done in reconcile() so it happens before provisioning.
		assert.Equal(t, "cosi-bc-qwerty", bucketID)

		persisted := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, bucketNsName, persisted))
		assert.Empty(t, persisted.Status.BucketID)
	})

	t.Run("generated ID missing", func(t *testing.T) {
		// Setup: fake driver returns an OK response with an empty bucket_id.
		r, bucket, ctx := newReconciler(t, func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
			return &cosiproto.DriverGenerateBucketIdResponse{BucketId: ""}, nil
		})

		// Act: run DriverGenerateBucketId in isolation.
		_, err := r.generateBucketID(ctx, logr.Discard(), genParams(bucket))
		// Validate: rejected as non-retryable; nothing persisted.
		assert.Error(t, err)
		assert.ErrorContains(t, err, "generated bucket ID missing")
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))

		persisted := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, bucketNsName, persisted))
		assert.Empty(t, persisted.Status.BucketID)
	})

	t.Run("retryable RPC error", func(t *testing.T) {
		// Setup: fake driver returns a gRPC code that classifies as retryable (codes.Unknown).
		r, bucket, ctx := newReconciler(t, func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
			return nil, status.Error(codes.Unknown, "fake unknown err")
		})

		// Act: run DriverGenerateBucketId in isolation.
		_, err := r.generateBucketID(ctx, logr.Discard(), genParams(bucket))
		// Validate: the raw error is returned unwrapped, so controller-runtime will requeue.
		assert.Error(t, err)
		assert.ErrorContains(t, err, "fake unknown err")
		assert.NotErrorIs(t, err, cosierr.NonRetryableError(nil))

		persisted := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, bucketNsName, persisted))
		assert.Empty(t, persisted.Status.BucketID)
	})

	// The codes proto/spec.md documents under "Important return codes" for this RPC. Both MUST be
	// terminal: retrying either against an unchanged request cannot succeed.
	for _, tc := range []struct {
		name string
		code codes.Code
		msg  string
	}{
		{"non-retryable RPC error", codes.InvalidArgument, "fake invalid arg err"},
		{"already exists RPC error", codes.AlreadyExists, "fake already exists err"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Setup: fake driver returns a gRPC code that classifies as non-retryable.
			r, bucket, ctx := newReconciler(t, func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
				return nil, status.Error(tc.code, tc.msg)
			})

			// Act: run DriverGenerateBucketId in isolation.
			_, err := r.generateBucketID(ctx, logr.Discard(), genParams(bucket))
			// Validate: wrapped as a NonRetryableError; nothing persisted.
			assert.Error(t, err)
			assert.ErrorContains(t, err, tc.msg)
			assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))

			persisted := &cosiapi.Bucket{}
			require.NoError(t, r.Get(ctx, bucketNsName, persisted))
			assert.Empty(t, persisted.Status.BucketID)
		})
	}
	// newReconcilerWithStatusUpdateInterceptor is like newReconciler, but wraps the fake client's
	// status subresource Update with subResourceUpdate so a test can force r.Status().Update to
	// fail in ways the fake client cannot produce on its own (e.g., a rejected-object error).
	newReconcilerWithStatusUpdateInterceptor := func(
		t *testing.T,
		subResourceUpdate func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error,
	) (*BucketReconciler, *cosiapi.Bucket, context.Context) {
		t.Helper()

		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
				return &cosiproto.DriverGenerateBucketIdResponse{BucketId: "cosi-" + dgbir.Name}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		t.Cleanup(cleanup)
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)

		scheme := runtime.NewScheme()
		require.NoError(t, cosiapi.AddToScheme(scheme))

		b := baseBucket.DeepCopy()
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(b).
			WithStatusSubresource(&cosiapi.Bucket{}).
			Build()
		interceptedClient := interceptor.NewClient(fakeClient, interceptor.Funcs{
			SubResourceUpdate: subResourceUpdate,
		})

		r := bucketReconcilerForClient(interceptedClient, DriverInfo{
			Name:               "cosi.s3.corp.net",
			SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
			ProvisionerClient:  cosiproto.NewProvisionerClient(conn),
		})

		ctx := context.Background()
		bucket := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, bucketNsName, bucket))
		return r, bucket, ctx
	}

	t.Run("status update fails with a retryable conflict", func(t *testing.T) {
		// Setup: intercept the status subresource update to force a Conflict response, simulating
		// a concurrent writer racing this update.
		r, bucket, ctx := newReconcilerWithStatusUpdateInterceptor(t,
			func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				return apierrors.NewConflict(
					schema.GroupResource{Group: cosiapi.GroupVersion.Group, Resource: "buckets"},
					obj.GetName(),
					fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again"),
				)
			},
		)

		// Act: the driver call succeeds, but reconcile's persist of the generated ID fails.
		err := r.reconcile(ctx, logr.Discard(), bucket)
		// Validate: left retryable, since a refreshed object can clear a conflict.
		assert.Error(t, err)
		// a conflict is a transient condition that a later retry with a refreshed object can clear
		assert.NotErrorIs(t, err, cosierr.NonRetryableError(nil))

		persisted := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, bucketNsName, persisted))
		assert.Empty(t, persisted.Status.BucketID)
	})

	t.Run("driver does not implement the RPC", func(t *testing.T) {
		// Setup: A sidecar upgraded ahead of its driver gets codes.Unimplemented, which
		// rpcErrorIsRetryable classifies as non-retryable. Provisioning stops rather than
		// looping; the operator must upgrade the driver.
		r, bucket, ctx := newReconciler(t, func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
			return nil, status.Error(codes.Unimplemented, "unknown method DriverGenerateBucketId")
		})

		// Act: run DriverGenerateBucketId in isolation.
		_, err := r.generateBucketID(ctx, logr.Discard(), genParams(bucket))
		// Validate: rejected as non-retryable, so provisioning stops instead of looping.
		assert.Error(t, err)
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))

		persisted := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, bucketNsName, persisted))
		assert.Empty(t, persisted.Status.BucketID)
	})
}

func TestBucketReconciler_staticProvision(t *testing.T) {
	validClaimRef := cosiapi.BucketClaimReference{
		Name:      "userbucket",
		Namespace: "usernamespace",
		UID:       "", // optional for static, left unset
	}

	t.Run("valid driver and bucket, successful provision", func(t *testing.T) {
		requestParams := map[string]string{}
		fakeServer := cositest.FakeProvisionerServer{
			GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
				requestParams = dgebr.Parameters
				ret := &cosiproto.DriverGetBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{
							Endpoint:        "s3.corp.net",
							BucketId:        dgebr.BucketId,
							Region:          "us-east-1",
							AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
						},
					},
				}
				return ret, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		inputParams := map[string]string{
			"key":    "value",
			"option": "setting",
		}
		details, err := r.staticProvision(context.Background(), logr.Discard(), staticProvisionParams{
			existingBucketID: "static-bucket",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: inputParams,
			claimRef:   validClaimRef,
		})
		assert.NoError(t, err)
		assert.Equal(t, []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3}, details.supportedProtos)
		assert.NotEmpty(t, details.allProtoBucketInfo)
		assert.Equal(t, "static-bucket", details.allProtoBucketInfo[string(cosiapi.BucketInfoVar_S3_BucketId)])
		for k := range details.allProtoBucketInfo {
			assert.True(t, strings.HasPrefix(k, "COSI_S3_"))
		}
		assert.Equal(t, inputParams, requestParams)
	})

	t.Run("valid driver, claim ref malformed", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
				return &cosiproto.DriverGetBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{
							Endpoint:        "s3.corp.net",
							BucketId:        dgebr.BucketId,
							Region:          "us-east-1",
							AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
						},
					},
				}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		tests := []struct {
			name     string
			claimRef cosiapi.BucketClaimReference
		}{
			{"namespace missing", cosiapi.BucketClaimReference{Name: validClaimRef.Name, Namespace: "", UID: validClaimRef.UID}},
			{"name missing", cosiapi.BucketClaimReference{Name: "", Namespace: validClaimRef.Namespace, UID: validClaimRef.UID}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				details, err := r.staticProvision(context.Background(), logr.Discard(), staticProvisionParams{
					existingBucketID: "static-bucket",
					requiredProtos: []*cosiproto.ObjectProtocol{
						{Type: cosiproto.ObjectProtocol_S3},
					},
					parameters: map[string]string{},
					claimRef:   tt.claimRef,
				})
				assert.Error(t, err)
				assert.ErrorContains(t, err, "bucketClaimRef namespace and name must be set")
				assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
				assert.Nil(t, details)
			})
		}
	})

	t.Run("valid driver, retryable provision error", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
				return nil, status.Error(codes.Unknown, "fake unknown err")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		details, err := r.staticProvision(context.Background(), logr.Discard(), staticProvisionParams{
			existingBucketID: "static-bucket",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: map[string]string{},
			claimRef:   validClaimRef,
		})
		assert.Error(t, err)
		assert.ErrorContains(t, err, "fake unknown err")
		assert.NotErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, details)
	})

	t.Run("valid driver, non-retryable provision error", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
				return nil, status.Error(codes.InvalidArgument, "fake invalid arg err")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		details, err := r.staticProvision(context.Background(), logr.Discard(), staticProvisionParams{
			existingBucketID: "static-bucket",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: map[string]string{},
			claimRef:   validClaimRef,
		})
		assert.Error(t, err)
		assert.ErrorContains(t, err, "fake invalid arg err")
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, details)
	})

	t.Run("valid driver, bucket does not exist", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
				return nil, status.Error(codes.NotFound, "bucket does not exist")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		details, err := r.staticProvision(context.Background(), logr.Discard(), staticProvisionParams{
			existingBucketID: "static-bucket",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: map[string]string{},
			claimRef:   validClaimRef,
		})
		assert.Error(t, err)
		assert.ErrorContains(t, err, "waiting for backend bucket to exist")
		assert.NotErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, details)
	})

	t.Run("valid driver, proto response nil", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
				return &cosiproto.DriverGetBucketResponse{
					Protocols: nil,
				}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		details, err := r.staticProvision(context.Background(), logr.Discard(), staticProvisionParams{
			existingBucketID: "static-bucket",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: map[string]string{},
			claimRef:   validClaimRef,
		})
		assert.Error(t, err)
		assert.ErrorContains(t, err, "existing bucket protocol response missing")
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, details)
	})

	t.Run("valid driver, empty S3 proto response", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
				return &cosiproto.DriverGetBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{},
					},
				}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		client := cosiproto.NewProvisionerClient(conn)

		r := BucketReconciler{
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  client,
			},
		}

		details, err := r.staticProvision(context.Background(), logr.Discard(), staticProvisionParams{
			existingBucketID: "static-bucket",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: map[string]string{},
			claimRef:   validClaimRef,
		})
		assert.NoError(t, err)
		assert.Equal(t, []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3}, details.supportedProtos)
		assert.NotEmpty(t, details.allProtoBucketInfo)
		for k, v := range details.allProtoBucketInfo {
			assert.True(t, strings.HasPrefix(k, "COSI_S3_"))
			assert.Empty(t, v)
		}
	})
}

func Test_objectProtocolListFromApiList(t *testing.T) {
	tests := []struct {
		name    string                   // description of this test case
		apiList []cosiapi.ObjectProtocol // input
		want    []*cosiproto.ObjectProtocol
		wantErr bool
	}{
		{"nil list", nil, []*cosiproto.ObjectProtocol{}, false},
		{"empty list", nil, []*cosiproto.ObjectProtocol{}, false},
		{"S3 only",
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
			[]*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			false,
		},
		{"Azure only",
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolAzure},
			[]*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_AZURE},
			},
			false,
		},
		{"S3 and Azure",
			[]cosiapi.ObjectProtocol{
				cosiapi.ObjectProtocolS3,
				cosiapi.ObjectProtocolAzure,
			},
			[]*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
				{Type: cosiproto.ObjectProtocol_AZURE},
			},
			false,
		},
		{"unknown proto",
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocol("unknown-proto")},
			nil,
			true,
		},
		{"S3 and unknown proto",
			[]cosiapi.ObjectProtocol{
				cosiapi.ObjectProtocolS3,
				cosiapi.ObjectProtocol("unknown-proto"),
			},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := objectProtocolListFromApiList(tt.apiList)
			if tt.wantErr {
				assert.Error(t, gotErr)
			} else {
				assert.NoError(t, gotErr)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_validateDriverSupportsProtocols(t *testing.T) {
	driverSupportsS3 := DriverInfo{
		Name: "cosi.s3.mycorp.net",
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{
			cosiproto.ObjectProtocol_S3,
		},
	}
	driverSupportsS3andAzure := DriverInfo{
		Name: "cosi.azure-s3-meta.mycorp.net",
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{
			cosiproto.ObjectProtocol_S3,
			cosiproto.ObjectProtocol_AZURE,
		},
	}
	driverSupportsNothing := DriverInfo{
		Name:               "cosi.nil.mycorp.net",
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{},
	}

	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		driver   DriverInfo
		required []*cosiproto.ObjectProtocol
		wantErr  bool
	}{
		{"no support, no required",
			driverSupportsNothing,
			[]*cosiproto.ObjectProtocol{},
			false,
		},
		{"no support, S3 required",
			driverSupportsNothing,
			[]*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			true,
		},
		{"no support, S3+Azure required",
			driverSupportsNothing,
			[]*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
				{Type: cosiproto.ObjectProtocol_AZURE},
			},
			true,
		},
		{"s3 support, no required",
			driverSupportsS3,
			[]*cosiproto.ObjectProtocol{},
			false,
		},
		{"s3 support, S3 required",
			driverSupportsS3,
			[]*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			false,
		},
		{"s3 support, S3+Azure required",
			driverSupportsS3,
			[]*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
				{Type: cosiproto.ObjectProtocol_AZURE},
			},
			true,
		},
		{"s3+Azure support, no required",
			driverSupportsS3andAzure,
			[]*cosiproto.ObjectProtocol{},
			false,
		},
		{"s3+Azure support, S3 required",
			driverSupportsS3andAzure,
			[]*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			false,
		},
		{"s3+Azure support, S3+Azure required",
			driverSupportsS3andAzure,
			[]*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
				{Type: cosiproto.ObjectProtocol_AZURE},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := validateDriverSupportsProtocols(tt.driver, tt.required)
			if tt.wantErr {
				assert.Error(t, gotErr)
			} else {
				assert.NoError(t, gotErr)
			}
		})
	}
}

func Test_validateBucketSupportsProtocols(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		required  []cosiapi.ObjectProtocol
		supported []cosiapi.ObjectProtocol
		wantErr   bool
	}{
		{"no support, no required",
			[]cosiapi.ObjectProtocol{},
			[]cosiapi.ObjectProtocol{},
			false,
		},
		{"no support, S3 required",
			[]cosiapi.ObjectProtocol{},
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
			true,
		},
		{"no support, S3+Azure required",
			[]cosiapi.ObjectProtocol{},
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3, cosiapi.ObjectProtocolAzure},
			true,
		},
		{"S3 support, no required",
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
			[]cosiapi.ObjectProtocol{},
			false,
		},
		{"S3 support, S3 required",
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
			false,
		},
		{"S3 support, S3+Azure required",
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3, cosiapi.ObjectProtocolAzure},
			true,
		},
		{"S3+Azure support, no required",
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3, cosiapi.ObjectProtocolAzure},
			[]cosiapi.ObjectProtocol{},
			false,
		},
		{"S3+Azure support, S3 required",
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3, cosiapi.ObjectProtocolAzure},
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
			false,
		},
		{"S3+Azure support, S3+Azure required",
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3, cosiapi.ObjectProtocolAzure},
			[]cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3, cosiapi.ObjectProtocolAzure},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := validateBucketSupportsProtocols(tt.required, tt.supported)
			if tt.wantErr {
				assert.Error(t, gotErr)
			} else {
				assert.NoError(t, gotErr)
			}
		})
	}
}

func Test_validateBucketID(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		isValid bool
	}{
		{"empty string", "", false}, // pattern requires at least one char
		{"a", "a", true},
		{"upper case char", "Z", true}, // upper case chars are allowed, unlike driver names
		{"digit", "0", true},
		{"dot", ".", true},
		{"underscore", "_", true},
		{"dash", "-", true},
		{"forward slash", "/", true},
		{"space", " ", false},
		{"nul char", string([]byte{0}), false},
		{"kitchen sink of allowed chars", "bucket-id.123_abc/def", true},
		{"space inside otherwise-valid ID", "bucket id", false},
		{"exactly BucketIDMaxLength", strings.Repeat("a", 2048), true},
		{"one over BucketIDMaxLength", strings.Repeat("a", 2049), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := validateBucketID(tt.id)
			if !tt.isValid {
				assert.Error(t, gotErr)
			} else {
				assert.NoError(t, gotErr)
			}
		})
	}
}
