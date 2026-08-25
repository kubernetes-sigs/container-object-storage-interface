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
	"k8s.io/apimachinery/pkg/util/validation/field"
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

	// Return the BucketID that should be recorded in status before provisioning completes.
	// Dynamic provisioning persists the phase 1 ID before phase 2 provisions the backend bucket,
	// so a failure in phase 2 still leaves the ID behind. Static provisioning has no phase 1 and
	// records nothing until it succeeds.
	ExpectBucketIdBeforeProvision() string

	// Validate the expected RPC request parameters for this test.
	// expectGenerateBucketId reports whether phase 1 of dynamic provisioning is expected to run
	// during the reconcile being validated. Phase 1 only runs while status.bucketID is unset, so
	// re-reconciles of an already-provisioned Bucket expect false. Static provisioning has no
	// phase 1 and ignores this.
	ValidateDriverRequest(t *testing.T,
		generateBucketIdReq []*cosiproto.DriverGenerateBucketIdRequest,
		createBucketReq []*cosiproto.DriverCreateBucketRequest,
		getBucketReq []*cosiproto.DriverGetBucketRequest,
		expectGenerateBucketId bool,
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

func (d *dynamicBucketTestHelper) ExpectBucketIdBeforeProvision() string {
	return d.ExpectBucketId() // phase 1 persists the ID before phase 2 runs
}

func (d *dynamicBucketTestHelper) ValidateDriverRequest(t *testing.T, generateBucketIdReq []*cosiproto.DriverGenerateBucketIdRequest, createBucketReq []*cosiproto.DriverCreateBucketRequest, getBucketReq []*cosiproto.DriverGetBucketRequest, expectGenerateBucketId bool) {
	require.Len(t, getBucketReq, 0)

	if expectGenerateBucketId {
		require.Len(t, generateBucketIdReq, 1)
		genReq := generateBucketIdReq[0]
		assert.Equal(t, "bc-dynamicuid", genReq.Name)
		// phase 1 receives the same protocols and parameters phase 2 will
		assert.Equal(t,
			[]*cosiproto.ObjectProtocol{{Type: cosiproto.ObjectProtocol_S3}},
			genReq.Protocols,
		)
		assert.Equal(t,
			map[string]string{"maxSize": "100Gi"},
			genReq.Parameters,
		)
	} else {
		// status.bucketID was already persisted, so phase 1 must not run again
		require.Len(t, generateBucketIdReq, 0)
	}

	require.Len(t, createBucketReq, 1)
	req := createBucketReq[0]
	// phase 2 must request the ID that phase 1 generated and COSI persisted
	assert.Equal(t, d.ExpectBucketId(), req.BucketId)
	assert.Equal(t,
		[]*cosiproto.ObjectProtocol{{Type: cosiproto.ObjectProtocol_S3}},
		req.Protocols,
	)
	assert.Equal(t,
		map[string]string{"maxSize": "100Gi"},
		req.Parameters,
	)
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
	return "cosi-static-bucket"
}

func (s *staticBucketTestHelper) ExpectBucketIdBeforeProvision() string {
	return "" // no phase 1; nothing is recorded until provisioning succeeds
}

func (s *staticBucketTestHelper) ValidateDriverRequest(t *testing.T, generateBucketIdReq []*cosiproto.DriverGenerateBucketIdRequest, createBucketReq []*cosiproto.DriverCreateBucketRequest, getBucketReq []*cosiproto.DriverGetBucketRequest, expectGenerateBucketId bool) {
	// static provisioning takes its bucket ID from spec.existingBucketID, so it has no phase 1
	require.Len(t, generateBucketIdReq, 0)
	require.Len(t, createBucketReq, 0)
	require.Len(t, getBucketReq, 1)
	req := getBucketReq[0]
	assert.Equal(t, "static-bucket", req.BucketId)
	assert.Equal(t,
		[]*cosiproto.ObjectProtocol{{Type: cosiproto.ObjectProtocol_S3}},
		req.Protocols,
	)
	assert.Equal(t,
		map[string]string{"maxSize": "100Gi"},
		req.Parameters,
	)
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
				BucketId: "cosi-" + dgebr.BucketId,
				Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
					S3: &cosiproto.S3BucketInfo{
						Endpoint:        "s3.corp.net",
						BucketId:        "corp-cosi-" + dgebr.BucketId,
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
	helper.ValidateDriverRequest(t, generateBucketIdReq, createBucketReq, getBucketReq, true)

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
		helper.ValidateDriverRequest(t, generateBucketIdReq, createBucketReq, getBucketReq, false)

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
		helper.ValidateDriverRequest(t, generateBucketIdReq, createBucketReq, getBucketReq, false)

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
			helper.ValidateDriverRequest(t, generateBucketIdReq, createBucketReq, getBucketReq, false)

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
				BucketId: "cosi-" + dgebr.BucketId,
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
	helper.ValidateDriverRequest(t, generateBucketIdReq, createBucketReq, getBucketReq, true)

	bucket := helper.GetBucket(bootstrapped)
	require.NotNil(t, bucket)

	assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer)
	assert.Equal(t, initBucket.Spec, bucket.Spec)
	assert.False(t, *bucket.Status.ReadyToUse)
	assert.Equal(t, helper.ExpectBucketIdBeforeProvision(), bucket.Status.BucketID)
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

// Two-phase provisioning behaviors that only apply to dynamic provisioning. Static provisioning
// takes its bucket ID from spec.existingBucketID and therefore has no phase 1.
func bucketTwoPhaseProvisionTestSuite(t *testing.T, initBucket *cosiapi.Bucket, helper bucketTestHelper) {
	nsName := cositest.NsName(initBucket)

	t.Run("bucket ID is persisted before the bucket is created", func(t *testing.T) {
		// Setup: a fake driver whose CreateBucketFunc reads the Bucket back mid-call, capturing
		// status.bucketID/readyToUse exactly as phase 2 sees them.
		var bucketIDAtCreateTime string
		var readyToUseAtCreateTime *bool
		var reconciler *BucketReconciler // set below; needed inside the RPC handler

		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
				return &cosiproto.DriverGenerateBucketIdResponse{BucketId: "cosi-" + dgbir.Name}, nil
			},
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				// Read the Bucket as it exists at the moment phase 2 runs. This is the whole point
				// of 2-phase provisioning: the ID must already be durable in Kubernetes before any
				// backend resource is created.
				current := &cosiapi.Bucket{}
				if err := reconciler.Get(ctx, nsName, current); err != nil {
					return nil, err
				}
				bucketIDAtCreateTime = current.Status.BucketID
				readyToUseAtCreateTime = current.Status.ReadyToUse

				return &cosiproto.DriverCreateBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{
							Endpoint:        "s3.corp.net",
							BucketId:        "backend-" + dcbr.BucketId,
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

		b := initBucket.DeepCopy()
		bootstrapped := cositest.MustBootstrap(t, b)
		ctx := bootstrapped.ContextWithLogger

		reconciler = &BucketReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
			DriverInfo: DriverInfo{
				Name:               s3DriverName,
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  rpcClient,
			},
		}

		// Act: reconcile end-to-end through both phases.
		res, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		assert.NoError(t, err)
		assert.Empty(t, res)

		// Validate: phase 2 already saw the ID persisted and readyToUse=false; the bucket ends ready.
		assert.Equal(t, helper.ExpectBucketId(), bucketIDAtCreateTime)
		require.NotNil(t, readyToUseAtCreateTime)
		assert.False(t, *readyToUseAtCreateTime) // not ready until phase 2 completes

		bucket := &cosiapi.Bucket{}
		require.NoError(t, reconciler.Get(ctx, nsName, bucket))
		assert.Equal(t, helper.ExpectBucketId(), bucket.Status.BucketID)
		assert.True(t, *bucket.Status.ReadyToUse)
	})

	t.Run("bucket ID already persisted", func(t *testing.T) {
		// Setup: fake driver whose GenerateBucketIdFunc fails the test if called; Bucket already
		// has status.bucketID/readyToUse=false, simulating a sidecar restart between phases.
		seenReq := []*cosiproto.DriverCreateBucketRequest{}
		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
				return nil, fmt.Errorf("DriverGenerateBucketId must not be called when the ID is already persisted")
			},
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				seenReq = append(seenReq, dcbr)
				return &cosiproto.DriverCreateBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{
							Endpoint:        "s3.corp.net",
							BucketId:        "backend-" + dcbr.BucketId,
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

		// simulate a sidecar restart between phase 1 and phase 2
		b := initBucket.DeepCopy()
		b.Status.BucketID = "cosi-persisted-id"
		b.Status.ReadyToUse = ptr.To(false)
		bootstrapped := cositest.MustBootstrap(t, b)
		ctx := bootstrapped.ContextWithLogger

		r := BucketReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
			DriverInfo: DriverInfo{
				Name:               s3DriverName,
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  rpcClient,
			},
		}

		// Act: reconcile a Bucket that already has a persisted ID.
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		assert.NoError(t, err)
		assert.Empty(t, res)

		// Validate: phase 1 was skipped; the persisted ID was reused for phase 2 unchanged.
		require.Len(t, seenReq, 1)
		assert.Equal(t, "cosi-persisted-id", seenReq[0].BucketId)

		bucket := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, nsName, bucket))
		assert.Equal(t, "cosi-persisted-id", bucket.Status.BucketID) // ID is never regenerated
		assert.True(t, *bucket.Status.ReadyToUse)
	})

	t.Run("bucket ID generation fails", func(t *testing.T) {
		// Setup: fake driver whose GenerateBucketIdFunc (phase 1) returns a non-retryable RPC
		// error; CreateBucketFunc (phase 2) fails the test if ever called.
		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
				return nil, status.Error(codes.InvalidArgument, "fake invalid arg err")
			},
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				return nil, fmt.Errorf("DriverCreateBucket must not be called when phase 1 fails")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		b := initBucket.DeepCopy()
		bootstrapped := cositest.MustBootstrap(t, b)
		ctx := bootstrapped.ContextWithLogger

		r := BucketReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
			DriverInfo: DriverInfo{
				Name:               s3DriverName,
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  rpcClient,
			},
		}

		// Act: reconcile; phase 1 must fail before phase 2 ever runs.
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		// Validate: no ID was persisted, readyToUse stays false, and the terminal error surfaces
		// in status.error.
		bucket := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, nsName, bucket))
		assert.Contains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer)
		assert.Empty(t, bucket.Status.BucketID)
		assert.False(t, *bucket.Status.ReadyToUse)
		serr := bucket.Status.Error
		require.NotNil(t, serr)
		assert.Contains(t, *serr.Message, "fake invalid arg err")
	})

	t.Run("generated bucket ID violates the status.bucketID pattern", func(t *testing.T) {
		// Setup: fake driver's phase 1 returns an ID containing characters outside the
		// status.bucketID schema.
		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
				// colon and space are outside ^[a-zA-Z0-9/._-]+$
				return &cosiproto.DriverGenerateBucketIdResponse{BucketId: "acme:bucket 1"}, nil
			},
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				return nil, fmt.Errorf("DriverCreateBucket must not be called when phase 1 fails")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		b := initBucket.DeepCopy()
		bootstrapped := cositest.MustBootstrap(t, b)
		ctx := bootstrapped.ContextWithLogger

		r := BucketReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
			DriverInfo: DriverInfo{
				Name:               s3DriverName,
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  rpcClient,
			},
		}

		// Act: reconcile with a driver that returns a schema-violating ID.
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		assert.Error(t, err)
		// must not retry forever against a driver ID that will never satisfy the schema
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		// Validate: rejected before persisting, with a terminal error naming the bad ID.
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
	})

	t.Run("generated bucket ID exceeds the status.bucketID length limit", func(t *testing.T) {
		// Setup: fake driver's phase 1 returns an ID one character over the 2048-char limit.
		overLong := strings.Repeat("a", 2049)
		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
				return &cosiproto.DriverGenerateBucketIdResponse{BucketId: overLong}, nil
			},
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				return nil, fmt.Errorf("DriverCreateBucket must not be called when phase 1 fails")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		b := initBucket.DeepCopy()
		bootstrapped := cositest.MustBootstrap(t, b)
		ctx := bootstrapped.ContextWithLogger

		r := BucketReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
			DriverInfo: DriverInfo{
				Name:               s3DriverName,
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  rpcClient,
			},
		}

		// Act: reconcile with a driver that returns an over-length ID.
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		// Validate: rejected before persisting, with a terminal error citing the schema violation.
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
	})

	t.Run("phase 2 fails then retries with the same bucket ID", func(t *testing.T) {
		// Setup: fake driver whose CreateBucketFunc (phase 2) can be toggled to fail via createErr,
		// while counting GenerateBucketIdFunc (phase 1) invocations.
		seenReq := []*cosiproto.DriverCreateBucketRequest{}
		generateCalls := 0
		var createErr error
		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
				generateCalls++
				return &cosiproto.DriverGenerateBucketIdResponse{BucketId: "cosi-" + dgbir.Name}, nil
			},
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				seenReq = append(seenReq, dcbr)
				if createErr != nil {
					return nil, createErr
				}
				return &cosiproto.DriverCreateBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{
							Endpoint:        "s3.corp.net",
							BucketId:        "backend-" + dcbr.BucketId,
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

		b := initBucket.DeepCopy()
		bootstrapped := cositest.MustBootstrap(t, b)
		ctx := bootstrapped.ContextWithLogger

		r := BucketReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
			DriverInfo: DriverInfo{
				Name:               s3DriverName,
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  rpcClient,
			},
		}

		// Act: reconcile #1 - phase 2 fails with a retryable error.
		t.Log("reconcile #1: phase 2 fails with a retryable error")
		createErr = status.Error(codes.Unavailable, "backend temporarily unavailable")
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		assert.Error(t, err)
		assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		// Validate: the ID phase 1 generated is durable even though phase 2 failed.
		require.Len(t, seenReq, 1)
		firstBucketID := seenReq[0].BucketId
		assert.NotEmpty(t, firstBucketID)
		assert.Equal(t, 1, generateCalls)

		afterFirst := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, nsName, afterFirst))
		assert.Equal(t, firstBucketID, afterFirst.Status.BucketID) // persisted despite phase 2 failure
		require.NotNil(t, afterFirst.Status.ReadyToUse)
		assert.False(t, *afterFirst.Status.ReadyToUse)
		serr := afterFirst.Status.Error
		require.NotNil(t, serr)
		assert.NotNil(t, serr.Time)
		require.NotNil(t, serr.Message)
		assert.Contains(t, *serr.Message, "backend temporarily unavailable")

		// Act: reconcile #2 - phase 2 succeeds on retry.
		t.Log("reconcile #2: phase 2 succeeds; phase 1 must not re-run")
		seenReq = nil
		createErr = nil
		res, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		assert.NoError(t, err)
		assert.Empty(t, res)

		// Validate: phase 1 did not re-run, and the same ID from reconcile #1 is reused to completion.
		assert.Equal(t, 1, generateCalls) // DriverGenerateBucketId was not called a second time
		require.Len(t, seenReq, 1)
		assert.Equal(t, firstBucketID, seenReq[0].BucketId) // same ID requested as reconcile #1

		final := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, nsName, final))
		assert.Equal(t, firstBucketID, final.Status.BucketID)
		require.NotNil(t, final.Status.ReadyToUse)
		assert.True(t, *final.Status.ReadyToUse)
	})

	t.Run("upgrade from single-phase sidecar does not re-run phase 1", func(t *testing.T) {
		// Setup: fake driver whose GenerateBucketIdFunc fails the test if called; Bucket already has
		// bucketID+readyToUse=true set together, as an old single-phase sidecar would leave it.
		seenReq := []*cosiproto.DriverCreateBucketRequest{}
		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
				return nil, fmt.Errorf("DriverGenerateBucketId must not be called when the ID is already persisted")
			},
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				seenReq = append(seenReq, dcbr)
				return &cosiproto.DriverCreateBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{
							Endpoint:        "s3.corp.net",
							BucketId:        "backend-" + dcbr.BucketId,
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

		// simulate a Bucket fully provisioned by an old, single-phase sidecar: status.bucketID and
		// readyToUse=true were both written in one shot, with no intervening phase-1-only state.
		b := initBucket.DeepCopy()
		b.Status.BucketID = "cosi-legacy-id"
		b.Status.ReadyToUse = ptr.To(true)
		bootstrapped := cositest.MustBootstrap(t, b)
		ctx := bootstrapped.ContextWithLogger

		r := BucketReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
			DriverInfo: DriverInfo{
				Name:               s3DriverName,
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  rpcClient,
			},
		}

		// Act: reconcile a Bucket that looks fully provisioned by an old, single-phase sidecar.
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		assert.NoError(t, err)
		assert.Empty(t, res)

		// Validate: phase 1 was skipped and the legacy ID was reused unchanged.
		require.Len(t, seenReq, 1)
		assert.Equal(t, "cosi-legacy-id", seenReq[0].BucketId) // ID not regenerated

		bucket := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, nsName, bucket))
		assert.Equal(t, "cosi-legacy-id", bucket.Status.BucketID)
		require.NotNil(t, bucket.Status.ReadyToUse)
		assert.True(t, *bucket.Status.ReadyToUse)
	})

	t.Run("foreign readyToUse=true with no bucket ID is corrected", func(t *testing.T) {
		// Setup: fake driver that reads the Bucket back mid-CreateBucket; the Bucket starts with
		// readyToUse=true but no bucketID, a combination this sidecar never produces itself.
		var readyToUseAtCreateTime *bool
		var errorAtCreateTime *cosiapi.TimestampedError
		var reconciler *BucketReconciler // set below; needed inside the RPC handler

		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketIdFunc: func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
				return &cosiproto.DriverGenerateBucketIdResponse{BucketId: "cosi-" + dgbir.Name}, nil
			},
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				// Read the Bucket as it exists at the moment phase 2 runs, the same technique used
				// by "bucket ID is persisted before the bucket is created" above.
				current := &cosiapi.Bucket{}
				if err := reconciler.Get(ctx, nsName, current); err != nil {
					return nil, err
				}
				readyToUseAtCreateTime = current.Status.ReadyToUse
				errorAtCreateTime = current.Status.Error

				return &cosiproto.DriverCreateBucketResponse{
					Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{
							Endpoint:        "s3.corp.net",
							BucketId:        "backend-" + dcbr.BucketId,
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

		// a foreign writer set readyToUse=true without ever assigning a bucket ID; this sidecar
		// never produces that combination itself and must correct it rather than propagate it.
		b := initBucket.DeepCopy()
		b.Status.ReadyToUse = ptr.To(true)
		bootstrapped := cositest.MustBootstrap(t, b)
		ctx := bootstrapped.ContextWithLogger

		reconciler = &BucketReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
			DriverInfo: DriverInfo{
				Name:               s3DriverName,
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  rpcClient,
			},
		}

		// Act: reconcile the inconsistent Bucket end-to-end.
		res, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: nsName})
		assert.NoError(t, err)
		assert.Empty(t, res)

		// Validate: readyToUse was forced false while phase 2 ran, with the anomaly recorded in
		// status.error so it's visible via `kubectl describe`, not just the sidecar log.
		require.NotNil(t, readyToUseAtCreateTime)
		assert.False(t, *readyToUseAtCreateTime) // forced false for the duration of phase 2
		require.NotNil(t, errorAtCreateTime)
		require.NotNil(t, errorAtCreateTime.Message)
		assert.Contains(t, *errorAtCreateTime.Message, "readyToUse was true before a bucket ID was assigned")

		// Validate: normal provisioning proceeds and completes despite the foreign state, and the
		// now-resolved anomaly is cleared from status.error along with everything else phase 2's
		// success clears.
		bucket := &cosiapi.Bucket{}
		require.NoError(t, reconciler.Get(ctx, nsName, bucket))
		assert.Equal(t, helper.ExpectBucketId(), bucket.Status.BucketID)
		require.NotNil(t, bucket.Status.ReadyToUse)
		assert.True(t, *bucket.Status.ReadyToUse)
		assert.Nil(t, bucket.Status.Error)
	})

	t.Run("deletion of a bucket that completed only phase 1", func(t *testing.T) {
		// A Bucket that finished phase 1 has a durable status.bucketID but no backend bucket. It
		// must still be deletable through the normal path, and the driver must receive the
		// phase-1 ID: persisting the ID before provisioning exists precisely so that a backend
		// bucket which may or may not have been created is always reachable for cleanup. Drivers
		// are therefore required to treat DriverDeleteBucket for an unprovisioned ID as a success.
		b := initBucket.DeepCopy()
		b.Finalizers = []string{cosiapi.ProtectionFinalizer}
		b.Status.BucketID = "cosi-phase-1-only"
		b.Status.ReadyToUse = ptr.To(false) // phase 2 never ran
		bootstrapped := cositest.MustBootstrap(t, b)

		driverInfo := DriverInfo{
			Name:               s3DriverName,
			SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
			// bucketDeletionTestSuite supplies its own delete-only ProvisionerClient
		}

		bucketDeletionTestSuite(t, bootstrapped, driverInfo, helper)
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
	}

	t.Run("dynamic provisioning", func(t *testing.T) {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				test.testSuiteFunc(t, baseDynamicBucket.DeepCopy(), &dynamicBucketTestHelper{})
			})
		}

		// unique condition for dynamic provisioning
		t.Run("two-phase provisioning", func(t *testing.T) {
			bucketTwoPhaseProvisionTestSuite(t, baseDynamicBucket.DeepCopy(), &dynamicBucketTestHelper{})
		})
	})

	t.Run("static provisioning", func(t *testing.T) {
		for _, test := range tests {
			test.testSuiteFunc(t, baseStaticBucket.DeepCopy(), &staticBucketTestHelper{})
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
		assert.Equal(t, "bc-qwerty", details.bucketId)
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
		assert.Equal(t, "bc-qwerty", details.bucketId)
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
		assert.Equal(t, "bc-qwerty", details.bucketId)
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
		assert.Equal(t, "bc-qwerty", details.bucketId)
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
		assert.Equal(t, "bc-qwerty", details.bucketId)
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

	t.Run("valid driver, requested bucket ID missing", func(t *testing.T) {
		// Setup: fake driver that fails the test if CreateBucket is called without a bucket ID.
		fakeServer := cositest.FakeProvisionerServer{
			CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
				return nil, fmt.Errorf("DriverCreateBucket must not be called without a bucket ID")
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

		// Act: call dynamicProvision directly with an empty bucketID, as if phase 1 never ran.
		details, err := r.dynamicProvision(context.Background(), logr.Discard(), dynamicProvisionParams{
			bucketID: "", // phase 1 did not run
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			claimRef: validClaimRef,
		})
		// Validate: rejected before any RPC, with a non-retryable error.
		assert.Error(t, err)
		assert.ErrorContains(t, err, "bucket ID missing before bucket creation")
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, details)
	})

}

func TestBucketReconciler_generateBucketID(t *testing.T) {
	// phase 1 receives the same protocol list phase 2 will
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
	// nolint:lll // long line is fine for test code
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

		r := &BucketReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  cosiproto.NewProvisionerClient(conn),
			},
		}

		bucket := &cosiapi.Bucket{}
		require.NoError(t, r.Get(bootstrapped.ContextWithLogger, bucketNsName, bucket))
		return r, bucket, bootstrapped.ContextWithLogger
	}

	t.Run("ID generated", func(t *testing.T) {
		// Setup: fake driver that echoes back a deterministic ID derived from the request name.
		seenReq := []*cosiproto.DriverGenerateBucketIdRequest{}
		r, bucket, ctx := newReconciler(t, func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
			seenReq = append(seenReq, dgbir)
			return &cosiproto.DriverGenerateBucketIdResponse{BucketId: "cosi-" + dgbir.Name}, nil
		})

		// Act: run phase 1 in isolation.
		err := r.generateBucketID(ctx, logr.Discard(), bucket, testProtos)
		assert.NoError(t, err)

		// Validate: the driver is asked for an ID using the Bucket resource name
		require.Len(t, seenReq, 1)
		assert.Equal(t, "bc-qwerty", seenReq[0].Name)
		// phase 1 receives the same protocols and parameters phase 2 will
		assert.Equal(t,
			[]*cosiproto.ObjectProtocol{{Type: cosiproto.ObjectProtocol_S3}},
			seenReq[0].Protocols,
		)
		assert.Equal(t, map[string]string{"maxSize": "10Gi"}, seenReq[0].Parameters)

		// generateBucketID only stages the result on the caller's object; it does not persist it
		// (that is the caller's responsibility, done in reconcile() so it can run before phase 2).
		assert.Equal(t, "cosi-bc-qwerty", bucket.Status.BucketID)
		require.NotNil(t, bucket.Status.ReadyToUse)
		assert.False(t, *bucket.Status.ReadyToUse)

		persisted := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, bucketNsName, persisted))
		assert.Empty(t, persisted.Status.BucketID)
	})

	t.Run("generated ID missing", func(t *testing.T) {
		// Setup: fake driver returns an OK response with an empty bucket_id.
		r, bucket, ctx := newReconciler(t, func(ctx context.Context, dgbir *cosiproto.DriverGenerateBucketIdRequest) (*cosiproto.DriverGenerateBucketIdResponse, error) {
			return &cosiproto.DriverGenerateBucketIdResponse{BucketId: ""}, nil
		})

		// Act: run phase 1 in isolation.
		err := r.generateBucketID(ctx, logr.Discard(), bucket, testProtos)
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

		// Act: run phase 1 in isolation.
		err := r.generateBucketID(ctx, logr.Discard(), bucket, testProtos)
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

			// Act: run phase 1 in isolation.
			err := r.generateBucketID(ctx, logr.Discard(), bucket, testProtos)
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

		r := &BucketReconciler{
			Client: interceptedClient,
			Scheme: interceptedClient.Scheme(),
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  cosiproto.NewProvisionerClient(conn),
			},
		}

		ctx := context.Background()
		bucket := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, bucketNsName, bucket))
		return r, bucket, ctx
	}

	t.Run("status update rejected by the API server (invalid)", func(t *testing.T) {
		// Setup: intercept the status subresource update to force an Invalid response, simulating
		// the API server rejecting the write outright.
		r, bucket, ctx := newReconcilerWithStatusUpdateInterceptor(t,
			func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				return apierrors.NewInvalid(
					schema.GroupKind{Group: cosiapi.GroupVersion.Group, Kind: "Bucket"},
					obj.GetName(),
					field.ErrorList{field.Invalid(field.NewPath("status", "bucketID"), "cosi-bc-qwerty", "field is immutable")},
				)
			},
		)

		// Act: the driver call succeeds, but reconcile's persist of the generated ID fails.
		err := r.reconcile(ctx, logr.Discard(), bucket)
		// Validate: treated as non-retryable, since the object itself was rejected.
		assert.Error(t, err)
		// the API server rejected the object itself, so retrying with the same content cannot succeed
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))

		persisted := &cosiapi.Bucket{}
		require.NoError(t, r.Get(ctx, bucketNsName, persisted))
		assert.Empty(t, persisted.Status.BucketID)
	})

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

		// Act: run phase 1 in isolation.
		err := r.generateBucketID(ctx, logr.Discard(), bucket, testProtos)
		// Validate: rejected as non-retryable, with a status.error message that tells the
		// operator the remedy (upgrade + restart), since a driver upgrade alone won't retry this
		// Bucket.
		assert.Error(t, err)
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.ErrorContains(t, err, "restart the sidecar")

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
					BucketId: dgebr.BucketId,
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
		assert.Equal(t, "static-bucket", details.bucketId)
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
					BucketId: dgebr.BucketId,
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

	t.Run("valid driver, bucket ID missing in response", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
				return &cosiproto.DriverGetBucketResponse{
					BucketId: "", // MISSING
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

		details, err := r.staticProvision(context.Background(), logr.Discard(), staticProvisionParams{
			existingBucketID: "static-bucket",
			requiredProtos: []*cosiproto.ObjectProtocol{
				{Type: cosiproto.ObjectProtocol_S3},
			},
			parameters: map[string]string{},
			claimRef:   validClaimRef,
		})
		assert.Error(t, err)
		assert.ErrorContains(t, err, "existing bucket ID missing")
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, details)
	})

	t.Run("valid driver, proto response nil", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GetBucketFunc: func(ctx context.Context, dgebr *cosiproto.DriverGetBucketRequest) (*cosiproto.DriverGetBucketResponse, error) {
				return &cosiproto.DriverGetBucketResponse{
					BucketId:  dgebr.BucketId,
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
					BucketId: dgebr.BucketId,
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
		assert.Equal(t, "static-bucket", details.bucketId)
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
