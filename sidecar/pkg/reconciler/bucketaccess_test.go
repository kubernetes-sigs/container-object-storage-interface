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

package reconciler_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	cositest "sigs.k8s.io/container-object-storage-interface/internal/test"
	controllertest "sigs.k8s.io/container-object-storage-interface/internal/test/controller"
	sidecartest "sigs.k8s.io/container-object-storage-interface/internal/test/sidecar"
	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
	sidecar "sigs.k8s.io/container-object-storage-interface/sidecar/pkg/reconciler"
)

func TestBucketAccessReconciler_Reconcile(t *testing.T) {
	// valid base access used for subtests
	baseAccess := cosiapi.BucketAccess{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-access",
			Namespace: "my-ns",
			UID:       "zxcvbn",
			// no finalizer so that tests can ensure finalizer is always re-added as needed
		},
		Spec: cosiapi.BucketAccessSpec{
			BucketClaims: []cosiapi.BucketClaimAccess{
				{
					BucketClaimName:  "readwrite-bucket",
					AccessModes:      cosiapi.BucketAccessModes{ObjectData: cosiapi.BucketAccessModeReadWrite},
					AccessSecretName: "readwrite-bucket-creds",
				},
				{
					BucketClaimName:  "readonly-bucket",
					AccessModes:      cosiapi.BucketAccessModes{ObjectData: cosiapi.BucketAccessModeReadOnly},
					AccessSecretName: "readonly-bucket-creds",
				},
			},
			BucketAccessClassName: "s3-class",
			Protocol:              cosiapi.ObjectProtocolS3,
			ServiceAccountName:    "my-app-sa",
		},
	}

	// valid base class used by subests
	baseClass := cosiapi.BucketAccessClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "s3-class",
		},
		Spec: cosiapi.BucketAccessClassSpec{
			// must match the driver that provisions the Buckets used by these tests, i.e. the
			// driver named by cositest.OpinionatedS3BucketClass()
			DriverName:         cositest.OpinionatedS3BucketClass().Spec.DriverName,
			AuthenticationType: cosiapi.BucketAccessAuthenticationTypeKey,
			Parameters: map[string]string{
				"maxSize": "100Gi",
				"maxIops": "10",
			},
			// by default, test the more complex multi-bucket cases
			MultiBucketAccess: cosiapi.MultiBucketAccessMultipleBuckets,
			// DisallowedBucketAccessModes: unset
		},
	}

	// valid bucketclaims referenced by above valid access
	baseReadWriteClaim := cositest.OpinionatedS3BucketClaim("my-ns", "readwrite-bucket")
	baseReadOnlyClaim := cositest.OpinionatedS3BucketClaim("my-ns", "readonly-bucket")

	newReconciler := func(api client.Client, proto cosiproto.ProvisionerClient) sidecar.BucketAccessReconciler {
		return sidecar.BucketAccessReconciler{
			Client: api,
			Scheme: api.Scheme(),
			DriverInfo: sidecar.DriverInfo{
				Name:               cositest.OpinionatedS3BucketClass().Spec.DriverName,
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  proto,
			},
		}
	}

	// valid RPC response corresponding to above access and buckets
	newBaseGrantResponse := func() *cosiproto.DriverGrantBucketAccessResponse {
		return &cosiproto.DriverGrantBucketAccessResponse{
			Credentials: &cosiproto.CredentialInfo{
				S3: &cosiproto.S3CredentialInfo{
					AccessKeyId:     "sharedaccesskey",
					AccessSecretKey: "sharedsecretkey",
				},
			},
			Buckets: []*cosiproto.DriverGrantBucketAccessResponse_BucketInfo{
				{
					BucketId: "cosi-bc-my-ns-readwrite-bucket",
					BucketInfo: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{
							// normally, S3 BucketID would probably be based on the rwBucketId, but
							// we want something static for unit tests
							BucketId:        "corp-cosi-bc-qwerty",
							Endpoint:        "s3.corp.net",
							Region:          "us-east-1",
							AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
						},
					},
				},
				{
					BucketId: "cosi-bc-my-ns-readonly-bucket",
					BucketInfo: &cosiproto.ObjectProtocolAndBucketInfo{
						S3: &cosiproto.S3BucketInfo{
							// normally, S3 BucketID would probably be based on the rwBucketId, but
							// we want something static for unit tests
							BucketId:        "corp-cosi-bc-asdfgh",
							Endpoint:        "s3.corp.net",
							Region:          "us-east-1",
							AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
						},
					},
				},
			},
		}
	}

	// Each test needs to have BucketClaims and Buckets set up using Controller and Sidecar logic.
	// Each test also needs the BucketAccess to be initialized with Controller logic.
	// This is a lot of boilerplate that makes tests harder to read.
	reconcileBucketClaimsAndAccessInitialization := func(t *testing.T, bootstrapped *cositest.Dependencies) {
		// reconcile BucketClaims to create intermediate Buckets
		rwClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)
		roClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadOnlyClaim))
		require.NoError(t, err)

		// reconcile intermediate Buckets
		_, err = sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.BucketNsName(rwClaim))
		require.NoError(t, err)
		_, err = sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.BucketNsName(roClaim))
		require.NoError(t, err)

		// reconcile BucketClaims to mirror finished status from Buckets
		rwClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)
		require.True(t, *rwClaim.Status.ReadyToUse)
		roClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadOnlyClaim))
		require.NoError(t, err)
		require.True(t, *roClaim.Status.ReadyToUse)

		// initialize the BucketAccess using the COSI Controller logic
		initAccess, err := controllertest.ReconcileBucketAccess(t, bootstrapped, cositest.NsName(&baseAccess))
		require.NoError(t, err)
		require.NotEmpty(t, initAccess.Status.AccessedBuckets)
	}

	// resultant resource will be nil if not found
	getAllResources := func(
		bootstrapped *cositest.Dependencies,
	) (
		access *cosiapi.BucketAccess,
		rwBucket, roBucket *cosiapi.Bucket,
		rwSecret, roSecret *corev1.Secret,
	) {
		ctx := bootstrapped.ContextWithLogger
		client := bootstrapped.Client
		var err error

		access = &cosiapi.BucketAccess{}
		err = client.Get(ctx, cositest.NsName(&baseAccess), access)
		if err != nil {
			access = nil
		}
		rwBucket = &cosiapi.Bucket{}
		err = client.Get(ctx, cositest.BucketNsName(baseReadWriteClaim), rwBucket)
		if err != nil {
			rwBucket = nil
		}
		roBucket = &cosiapi.Bucket{}
		err = client.Get(ctx, cositest.BucketNsName(baseReadOnlyClaim), roBucket)
		if err != nil {
			roBucket = nil
		}
		rwSecret = &corev1.Secret{}
		err = client.Get(ctx, cositest.SecretNsName(&baseAccess, 0), rwSecret)
		if err != nil {
			rwSecret = nil
		}
		roSecret = &corev1.Secret{}
		err = client.Get(ctx, cositest.SecretNsName(&baseAccess, 1), roSecret)
		if err != nil {
			roSecret = nil
		}

		return access, rwBucket, roBucket, rwSecret, roSecret
	}

	t.Run("successful provision", func(t *testing.T) {
		// reuse th same fake RPC server and client for all tests
		grantRequests := []*cosiproto.DriverGrantBucketAccessRequest{}
		revokeRequests := []*cosiproto.DriverRevokeBucketAccessRequest{}
		var grantError, revokeError error
		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketAccessIdFunc: sidecartest.OpinionatedGenerateBucketAccessIdFunc,
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				grantRequests = append(grantRequests, dgbar)
				ret := newBaseGrantResponse()
				return ret, grantError
			},
			RevokeBucketAccessFunc: func(ctx context.Context, drbar *cosiproto.DriverRevokeBucketAccessRequest) (*cosiproto.DriverRevokeBucketAccessResponse, error) {
				revokeRequests = append(revokeRequests, drbar)
				return &cosiproto.DriverRevokeBucketAccessResponse{}, revokeError
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		// grant requests, when made, should be identical for all child tests
		assertGrantRequest := func(t *testing.T, req *cosiproto.DriverGrantBucketAccessRequest) {
			assert.Equal(t, "cosi-ba-zxcvbn", req.AccountId)
			assert.Equal(t, cosiproto.AuthenticationType_KEY, req.AuthenticationType.Type)
			assert.Equal(t, cosiproto.ObjectProtocol_S3, req.Protocol.Type)
			assert.Equal(t, "", req.ServiceAccountName)
			assert.Equal(t,
				map[string]string{
					"maxSize": "100Gi",
					"maxIops": "10",
				},
				req.Parameters,
			)
			require.Len(t, req.Buckets, 2) // by RPC spec, order of requested accessed buckets is random
			assert.True(t, accessedBucketRequestExists(req.Buckets, &cosiproto.DriverGrantBucketAccessRequest_AccessedBucket{
				BucketId:                 "cosi-bc-my-ns-readwrite-bucket",
				ObjectDataAccessMode:     &cosiproto.AccessMode{Mode: cosiproto.AccessMode_READ_WRITE},
				ObjectMetadataAccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_ANY},
				BucketMetadataAccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_ANY},
			}))
			assert.True(t, accessedBucketRequestExists(req.Buckets, &cosiproto.DriverGrantBucketAccessRequest_AccessedBucket{
				BucketId:                 "cosi-bc-my-ns-readonly-bucket",
				ObjectDataAccessMode:     &cosiproto.AccessMode{Mode: cosiproto.AccessMode_READ_ONLY},
				ObjectMetadataAccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_ANY},
				BucketMetadataAccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_ANY},
			}))
		}

		// revoke requests, when made, should be identical for all child tests
		assertRevokeRequest := func(t *testing.T, req *cosiproto.DriverRevokeBucketAccessRequest) {
			assert.Equal(t, "cosi-ba-zxcvbn", req.AccountId)
			assert.Equal(t, cosiproto.AuthenticationType_KEY, req.AuthenticationType.Type)
			assert.Equal(t, cosiproto.ObjectProtocol_S3, req.Protocol.Type)
			assert.Equal(t, "", req.ServiceAccountName)
			assert.Equal(t,
				map[string]string{
					"maxSize": "100Gi",
					"maxIops": "10",
				},
				req.Parameters,
			)
			require.Len(t, req.Buckets, 2)
			assert.Equal(t, "cosi-bc-my-ns-readwrite-bucket", req.Buckets[0].BucketId)
			assert.Equal(t, "cosi-bc-my-ns-readonly-bucket", req.Buckets[1].BucketId)
		}

		// the base test that other tests build on
		testSuccessfulProvision := func(
			t *testing.T, rpcClient cosiproto.ProvisionerClient,
		) (
			*cositest.Dependencies,
			*sidecar.BucketAccessReconciler,
		) {
			bootstrapped := cositest.MustBootstrap(t,
				baseAccess.DeepCopy(),
				baseClass.DeepCopy(),
				baseReadWriteClaim.DeepCopy(),
				baseReadOnlyClaim.DeepCopy(),
				cositest.OpinionatedS3BucketClass(),
			)
			ctx := bootstrapped.ContextWithLogger

			reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)
			initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

			grantRequests = []*cosiproto.DriverGrantBucketAccessRequest{} // empty the seen rpc requests
			grantError = nil
			revokeRequests = []*cosiproto.DriverRevokeBucketAccessRequest{} // empty the seen rpc requests
			revokeError = nil

			r := newReconciler(bootstrapped.Client, rpcClient)

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			// ensure the expected RPC call was made
			require.Len(t, grantRequests, 1)
			require.Len(t, revokeRequests, 0)
			assertGrantRequest(t, grantRequests[0])

			access, rwBucket, roBucket, rwSec, roSec := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
			assert.Equal(t, baseAccess.Spec, access.Spec) // spec should not change from base
			assert.True(t, *access.Status.ReadyToUse)
			assert.Nil(t, access.Status.Error)
			assert.Equal(t, "cosi-ba-zxcvbn", access.Status.AccountID)
			// should not modify what the Controller initialized
			assert.Equal(t, initAccess.Status.AccessedBuckets, access.Status.AccessedBuckets)
			assert.Equal(t, initAccess.Status.AuthenticationType, access.Status.AuthenticationType)
			assert.Equal(t, initAccess.Status.DriverName, access.Status.DriverName)
			assert.Equal(t, initAccess.Status.Parameters, access.Status.Parameters)

			for _, s := range []*corev1.Secret{rwSec, roSec} {
				assert.Contains(t, s.GetFinalizers(), cosiapi.ProtectionFinalizer)
				require.Len(t, s.OwnerReferences, 1)
				assert.Equal(t, "zxcvbn", string(s.OwnerReferences[0].UID))
				assert.Equal(t, "sharedaccesskey", s.StringData[string(cosiapi.CredentialVar_S3_AccessKeyId)])
			}
			assert.Equal(t, "corp-cosi-bc-qwerty", rwSec.StringData[string(cosiapi.BucketInfoVar_S3_BucketId)])
			assert.Equal(t, "corp-cosi-bc-asdfgh", roSec.StringData[string(cosiapi.BucketInfoVar_S3_BucketId)])

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)

			return bootstrapped, &r
		}

		t.Run("initial provision", func(t *testing.T) {
			testSuccessfulProvision(t, rpcClient)
		})

		t.Run("subsequent reconcile, no changes", func(t *testing.T) {
			bootstrapped, r := testSuccessfulProvision(t, rpcClient)
			ctx := bootstrapped.ContextWithLogger

			grantRequests = []*cosiproto.DriverGrantBucketAccessRequest{} // empty the seen rpc requests
			grantError = nil
			revokeRequests = []*cosiproto.DriverRevokeBucketAccessRequest{} // empty the seen rpc requests
			revokeError = nil

			initAccess, initRwBucket, initRoBucket, initRwSec, initRoSec := getAllResources(bootstrapped)

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			require.Len(t, grantRequests, 1)
			require.Len(t, revokeRequests, 0)
			assertGrantRequest(t, grantRequests[0])

			access, rwBucket, roBucket, rwSec, roSec := getAllResources(bootstrapped)

			// access doesn't change
			assert.Equal(t, initAccess.Finalizers, access.Finalizers)
			assert.Equal(t, initAccess.Spec, access.Spec)
			assert.Equal(t, initAccess.Status, access.Status)

			// secrets don't change
			assert.Equal(t, initRwSec.StringData, rwSec.StringData)
			assert.Equal(t, initRoSec.StringData, roSec.StringData)

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)
		})

		t.Run("subsequent deletion", func(t *testing.T) {
			bootstrapped, r := testSuccessfulProvision(t, rpcClient)
			ctx := bootstrapped.ContextWithLogger

			initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

			require.NoError(t, bootstrapped.Client.Delete(ctx, initAccess.DeepCopy()))

			grantRequests = []*cosiproto.DriverGrantBucketAccessRequest{} // empty the seen rpc requests
			grantError = nil
			revokeRequests = []*cosiproto.DriverRevokeBucketAccessRequest{} // empty the seen rpc requests
			revokeError = nil

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			require.Len(t, grantRequests, 0)
			require.Len(t, revokeRequests, 1)
			assertRevokeRequest(t, revokeRequests[0])

			access, rwBucket, roBucket, _, _ := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer) // Controller still needs to clean up
			assert.Equal(t, initAccess.Spec, access.Spec)
			assert.False(t, *access.Status.ReadyToUse)
			assert.Nil(t, access.Status.Error)
			{ // non-error fields stay the same
				delReady := access.DeepCopy()
				delReady.Status.ReadyToUse = ptr.To(true)
				assert.Equal(t, initAccess.Status, delReady.Status)
			}

			// secrets are deleted
			bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 0), &corev1.Secret{})
			bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 1), &corev1.Secret{})

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)
		})

		t.Run("subsequent deletion fails", func(t *testing.T) {
			bootstrapped, r := testSuccessfulProvision(t, rpcClient)
			ctx := bootstrapped.ContextWithLogger

			initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

			require.NoError(t, bootstrapped.Client.Delete(ctx, initAccess.DeepCopy()))

			grantRequests = []*cosiproto.DriverGrantBucketAccessRequest{} // empty the seen rpc requests
			grantError = nil
			revokeRequests = []*cosiproto.DriverRevokeBucketAccessRequest{} // empty the seen rpc requests
			revokeError = grpcstatus.Error(codes.Unknown, "fake rpc error")

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.Error(t, err)
			assert.ErrorContains(t, err, "fake rpc error")
			assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
			assert.Empty(t, res)

			require.Len(t, grantRequests, 0)
			require.Len(t, revokeRequests, 1)
			assertRevokeRequest(t, revokeRequests[0])

			access, rwBucket, roBucket, _, _ := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer) // Controller still needs to clean up
			assert.Equal(t, initAccess.Spec, access.Spec)
			assert.False(t, *access.Status.ReadyToUse)
			require.NotNil(t, access.Status.Error)
			assert.NotNil(t, access.Status.Error.Time)
			assert.NotNil(t, access.Status.Error.Message)
			assert.Contains(t, *access.Status.Error.Message, "fake rpc error")
			{ // non-error/non-ready fields stay the same
				delNoErr := access.DeepCopy()
				delNoErr.Status.ReadyToUse = ptr.To(true)
				delNoErr.Status.Error = nil
				assert.Equal(t, initAccess.Status, delNoErr.Status)
			}

			// secrets are deleted
			bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 0), &corev1.Secret{})
			bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 1), &corev1.Secret{})

			// buckets must not change
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)
		})

		t.Run("backend credentials rotated", func(t *testing.T) {
			bootstrapped, r := testSuccessfulProvision(t, rpcClient)
			ctx := bootstrapped.ContextWithLogger

			initAccess, initRwBucket, initRoBucket, initRwSec, initRoSec := getAllResources(bootstrapped)

			// restore parent's fake grant access func after this
			oldGrantFunc := fakeServer.GrantBucketAccessFunc
			defer func() {
				fakeServer.GrantBucketAccessFunc = oldGrantFunc
			}()
			fakeServer.GrantBucketAccessFunc = func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				grantRequests = append(grantRequests, dgbar)
				ret := newBaseGrantResponse()
				ret.Credentials.S3.AccessKeyId = "rotatedsharedaccesskey"
				return ret, nil
			}

			grantRequests = []*cosiproto.DriverGrantBucketAccessRequest{} // empty the seen rpc requests
			grantError = nil
			revokeRequests = []*cosiproto.DriverRevokeBucketAccessRequest{} // empty the seen rpc requests
			revokeError = nil

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			require.Len(t, grantRequests, 1)
			require.Len(t, revokeRequests, 0)
			assertGrantRequest(t, grantRequests[0])

			access, rwBucket, roBucket, rwSec, roSec := getAllResources(bootstrapped)

			// access remains happy
			assert.Equal(t, initAccess.Finalizers, access.Finalizers)
			assert.Equal(t, initAccess.Spec, access.Spec)
			assert.Equal(t, initAccess.Status, access.Status)

			// secrets change their access key ID only
			assert.Equal(t, "rotatedsharedaccesskey", rwSec.StringData[string(cosiapi.CredentialVar_S3_AccessKeyId)])
			assert.Equal(t, "rotatedsharedaccesskey", roSec.StringData[string(cosiapi.CredentialVar_S3_AccessKeyId)])
			{ // other secret data stays the same
				rwsCopy := rwSec.DeepCopy()
				rosCopy := roSec.DeepCopy()
				rwsCopy.StringData[string(cosiapi.CredentialVar_S3_AccessKeyId)] = "sharedaccesskey"
				rosCopy.StringData[string(cosiapi.CredentialVar_S3_AccessKeyId)] = "sharedaccesskey"
				assert.Equal(t, initRwSec.StringData, rwsCopy.StringData)
				assert.Equal(t, initRoSec.StringData, rosCopy.StringData)
			}

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)
		})

		t.Run("subsequent error reporting and clearing", func(t *testing.T) {
			// RPC errors should be reported for debugging without modifying provisioned status

			// base for error reporting tests, build on successful provision test
			testReconcileErrorsReported := func(
				t *testing.T,
				bootstrapped *cositest.Dependencies,
				reconciler *sidecar.BucketAccessReconciler,
			) {
				ctx := bootstrapped.ContextWithLogger

				initAccess, initRwBucket, initRoBucket, initRwSec, initRoSec := getAllResources(bootstrapped)

				grantRequests = []*cosiproto.DriverGrantBucketAccessRequest{} // empty the seen rpc requests
				grantError = grpcstatus.Error(codes.Unknown, "fake rpc error")
				revokeRequests = []*cosiproto.DriverRevokeBucketAccessRequest{} // empty the seen rpc requests
				revokeError = nil

				res, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
				assert.Error(t, err)
				assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
				assert.Empty(t, res)

				require.Len(t, grantRequests, 1)
				require.Len(t, revokeRequests, 0)
				assertGrantRequest(t, grantRequests[0])

				access, rwBucket, roBucket, rwSec, roSec := getAllResources(bootstrapped)

				// access has error but otherwise doesn't change
				assert.Equal(t, initAccess.Finalizers, access.Finalizers)
				assert.Equal(t, initAccess.Spec, access.Spec)
				require.NotNil(t, access.Status.Error)
				assert.NotNil(t, access.Status.Error.Time)
				assert.NotNil(t, access.Status.Error.Message)
				assert.Contains(t, *access.Status.Error.Message, "fake rpc error")
				{ // non-error fields stay the same
					noErr := access.DeepCopy()
					noErr.Status.Error = nil
					assert.Equal(t, initAccess.Status, noErr.Status)
				}

				// secrets don't change
				assert.Equal(t, initRwSec.StringData, rwSec.StringData)
				assert.Equal(t, initRoSec.StringData, roSec.StringData)

				// buckets must not be changed
				assert.Equal(t, initRwBucket, rwBucket)
				assert.Equal(t, initRoBucket, roBucket)
			}

			t.Run("error reported", func(t *testing.T) {
				bootstrapped, r := testSuccessfulProvision(t, rpcClient)
				testReconcileErrorsReported(t, bootstrapped, r)
			})

			t.Run("error cleared", func(t *testing.T) {
				bootstrapped, r := testSuccessfulProvision(t, rpcClient)
				ctx := bootstrapped.ContextWithLogger

				initAccess, initRwBucket, initRoBucket, initRwSec, initRoSec := getAllResources(bootstrapped)

				testReconcileErrorsReported(t, bootstrapped, r)

				grantRequests = []*cosiproto.DriverGrantBucketAccessRequest{} // empty the seen rpc requests
				grantError = nil
				revokeRequests = []*cosiproto.DriverRevokeBucketAccessRequest{} // empty the seen rpc requests
				revokeError = nil

				res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
				assert.NoError(t, err)
				assert.Empty(t, res)

				require.Len(t, grantRequests, 1)
				require.Len(t, revokeRequests, 0)
				assertGrantRequest(t, grantRequests[0])

				access, rwBucket, roBucket, rwSec, roSec := getAllResources(bootstrapped)

				// access returns to happy state
				assert.Equal(t, initAccess.Finalizers, access.Finalizers)
				assert.Equal(t, initAccess.Spec, access.Spec)
				assert.Equal(t, initAccess.Status, access.Status)

				// secrets don't change
				assert.Equal(t, initRwSec.StringData, rwSec.StringData)
				assert.Equal(t, initRoSec.StringData, roSec.StringData)

				// buckets must not be changed
				assert.Equal(t, initRwBucket, rwBucket)
				assert.Equal(t, initRoBucket, roBucket)
			})

			t.Run("deletion after errs reported", func(t *testing.T) {
				bootstrapped, r := testSuccessfulProvision(t, rpcClient)
				ctx := bootstrapped.ContextWithLogger

				initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

				testReconcileErrorsReported(t, bootstrapped, r)

				require.NoError(t, bootstrapped.Client.Delete(ctx, initAccess.DeepCopy()))

				grantRequests = []*cosiproto.DriverGrantBucketAccessRequest{} // empty the seen rpc requests
				grantError = nil
				revokeRequests = []*cosiproto.DriverRevokeBucketAccessRequest{} // empty the seen rpc requests
				revokeError = nil

				res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
				assert.NoError(t, err)
				assert.Empty(t, res)

				require.Len(t, grantRequests, 0)
				require.Len(t, revokeRequests, 1)
				assertRevokeRequest(t, revokeRequests[0])

				access, rwBucket, roBucket, _, _ := getAllResources(bootstrapped)

				// access doesn't change
				assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer) // Controller still needs to clean up
				assert.Equal(t, initAccess.Spec, access.Spec)
				assert.False(t, *access.Status.ReadyToUse)
				assert.Nil(t, access.Status.Error)
				{ // non-error fields stay the same
					delReady := access.DeepCopy()
					delReady.Status.ReadyToUse = ptr.To(true)
					assert.Equal(t, initAccess.Status, delReady.Status)
				}

				// secrets are deleted
				bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 0), &corev1.Secret{})
				bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 1), &corev1.Secret{})

				// buckets must not be changed
				assert.Equal(t, initRwBucket, rwBucket)
				assert.Equal(t, initRoBucket, roBucket)
			})
		})
	})

	t.Run("secret already exists with incompatible owner", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{} // no RPC calls should be made

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		testIncompatibleOwner := func(
			t *testing.T, owner *metav1.OwnerReference,
		) (
			bootstrapped *cositest.Dependencies,
			reconciler *sidecar.BucketAccessReconciler,
		) {
			preExistingSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cositest.SecretNsName(&baseAccess, 0).Name,
					Namespace: baseAccess.Namespace,
				},
				StringData: map[string]string{
					"PRE_EXISTING_DATA": "important_thing",
				},
			}
			if owner != nil {
				preExistingSecret.OwnerReferences = []metav1.OwnerReference{*owner}
			}

			bootstrapped = cositest.MustBootstrap(t,
				baseAccess.DeepCopy(),
				baseClass.DeepCopy(),
				baseReadWriteClaim.DeepCopy(),
				baseReadOnlyClaim.DeepCopy(),
				cositest.OpinionatedS3BucketClass(),
				preExistingSecret,
			)
			ctx := bootstrapped.ContextWithLogger

			reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)
			initAccess, _, _, _, _ := getAllResources(bootstrapped)

			r := newReconciler(bootstrapped.Client, rpcClient)

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.Error(t, err)
			assert.ErrorIs(t, err, reconcile.TerminalError(nil))
			assert.Empty(t, res)

			access, _, _, rwSec, roSec := getAllResources(bootstrapped)

			assert.Equal(t, initAccess.Finalizers, access.Finalizers)
			assert.Equal(t, baseAccess.Spec, access.Spec)
			assert.False(t, *access.Status.ReadyToUse)
			require.NotNil(t, access.Status.Error)
			assert.NotNil(t, access.Status.Error.Time)
			assert.NotNil(t, access.Status.Error.Message)
			assert.Contains(t, *access.Status.Error.Message, "failed to reserve one or more Secrets")
			{ // non-error fields stay the same
				accessNoError := access.DeepCopy()
				accessNoError.Status.Error = nil
				assert.Equal(t, initAccess.Status, accessNoError.Status)
			}

			// pre-existing secret that was already owned hasn't been touched
			assert.Equal(t, preExistingSecret, rwSec)

			// other secret has been reserved successfully
			assert.Contains(t, roSec.GetFinalizers(), cosiapi.ProtectionFinalizer)
			require.Len(t, roSec.OwnerReferences, 1)
			assert.Equal(t, "zxcvbn", string(roSec.OwnerReferences[0].UID))
			assert.Len(t, roSec.StringData, 0)

			return bootstrapped, &r
		}

		testSubsequentDeletion := func(
			t *testing.T,
			bootstrapped *cositest.Dependencies,
			r *sidecar.BucketAccessReconciler,
		) {
			ctx := bootstrapped.ContextWithLogger

			initAccess, initRwBucket, initRoBucket, preExistingSec, _ := getAllResources(bootstrapped)

			require.NoError(t, bootstrapped.Client.Delete(ctx, initAccess.DeepCopy()))

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			access, rwBucket, roBucket, preExistingSecReconciled, _ := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer) // Controller still needs to clean up
			assert.Equal(t, initAccess.Spec, access.Spec)
			assert.False(t, *access.Status.ReadyToUse)
			assert.Nil(t, access.Status.Error)
			{ // non-error fields stay the same
				initNoErr := initAccess.DeepCopy()
				initNoErr.Status.Error = nil
				assert.Equal(t, initNoErr.Status, access.Status)
			}

			// pre-existing secret unmodified
			assert.Equal(t, preExistingSec, preExistingSecReconciled)

			// other secret deleted
			bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 1), &corev1.Secret{})

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)
		}

		t.Run("no owners", func(t *testing.T) {
			var owner *metav1.OwnerReference = nil
			t.Run("reconcile", func(t *testing.T) {
				testIncompatibleOwner(t, owner)
			})

			t.Run("subsequent deletion", func(t *testing.T) {
				bootstrapped, r := testIncompatibleOwner(t, owner)
				testSubsequentDeletion(t, bootstrapped, r)
			})
		})

		t.Run("different controller-owner", func(t *testing.T) {
			owner := &metav1.OwnerReference{
				APIVersion:         "other.controller.io/v1",
				Kind:               "gvk.Kind",
				Name:               "other-owner",
				UID:                "aaaaaa",
				BlockOwnerDeletion: ptr.To(true),
				Controller:         ptr.To(true),
			}

			t.Run("reconcile", func(t *testing.T) {
				testIncompatibleOwner(t, owner)
			})

			t.Run("subsequent deletion", func(t *testing.T) {
				bootstrapped, r := testIncompatibleOwner(t, owner)
				testSubsequentDeletion(t, bootstrapped, r)
			})
		})
	})

	// Most cases where provisioning can't proceed due to invalid/malformed spec/status fields
	// should have the same deletion behavior.
	testDeletionReleasesSecretsAndStatus := func(
		t *testing.T,
		bootstrapped *cositest.Dependencies,
		r *sidecar.BucketAccessReconciler,
	) {
		ctx := bootstrapped.ContextWithLogger

		initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

		require.NoError(t, bootstrapped.Client.Delete(ctx, initAccess.DeepCopy()))

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.NoError(t, err)
		assert.Empty(t, res)

		access, rwBucket, roBucket, _, _ := getAllResources(bootstrapped)

		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer) // Controller still needs to clean up
		assert.Equal(t, initAccess.Spec, access.Spec)
		assert.False(t, *access.Status.ReadyToUse)
		assert.Nil(t, access.Status.Error)
		{ // non-error fields stay the same
			initNoErr := initAccess.DeepCopy()
			initNoErr.Status.Error = nil
			assert.Equal(t, initNoErr.Status, access.Status)
		}

		// all secrets are gone
		bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 0), &corev1.Secret{})
		bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 1), &corev1.Secret{})

		// buckets must not be changed
		assert.Equal(t, initRwBucket, rwBucket)
		assert.Equal(t, initRoBucket, roBucket)
	}

	t.Run("repeated secret name in spec.bucketClaims", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{} // no RPC calls should be made

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		testRepeatedSecretName := func(t *testing.T) (
			bootstrapped *cositest.Dependencies,
			reconciler *sidecar.BucketAccessReconciler,
		) {
			accessWithRepeatedSecret := baseAccess.DeepCopy()
			accessWithRepeatedSecret.Spec.BucketClaims[0].AccessSecretName = cositest.SecretNsName(&baseAccess, 0).Name
			accessWithRepeatedSecret.Spec.BucketClaims[1].AccessSecretName = cositest.SecretNsName(&baseAccess, 0).Name

			bootstrapped = cositest.MustBootstrap(t,
				accessWithRepeatedSecret,
				baseClass.DeepCopy(),
				baseReadWriteClaim.DeepCopy(),
				baseReadOnlyClaim.DeepCopy(),
				cositest.OpinionatedS3BucketClass(),
			)
			ctx := bootstrapped.ContextWithLogger

			reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)
			initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

			r := newReconciler(bootstrapped.Client, rpcClient)

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.Error(t, err)
			assert.ErrorIs(t, err, reconcile.TerminalError(nil))
			assert.Empty(t, res)

			access, rwBucket, roBucket, rwSec, _ := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
			assert.Equal(t, accessWithRepeatedSecret.Spec, access.Spec)
			require.NotNil(t, access.Status.Error)
			assert.NotNil(t, access.Status.Error.Time)
			assert.NotNil(t, access.Status.Error.Message)
			assert.Contains(t, *access.Status.Error.Message, "same accessSecretName")
			assert.Contains(t, *access.Status.Error.Message, cositest.SecretNsName(&baseAccess, 0).Name)
			{ // non-error fields stay the same
				accessNoError := access.DeepCopy()
				accessNoError.Status.Error = nil
				assert.Equal(t, initAccess.Status, accessNoError.Status)
			}

			// first secret has been reserved successfully
			assert.Contains(t, rwSec.GetFinalizers(), cosiapi.ProtectionFinalizer)
			require.Len(t, rwSec.OwnerReferences, 1)
			assert.Equal(t, "zxcvbn", string(rwSec.OwnerReferences[0].UID))
			assert.Len(t, rwSec.StringData, 0)

			// second secret normally expected in other tests should be absent
			bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 1), &corev1.Secret{})

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)

			return bootstrapped, &r
		}

		t.Run("reconcile", func(t *testing.T) {
			testRepeatedSecretName(t)
		})

		t.Run("subsequent deletion", func(t *testing.T) {
			bootstrapped, r := testRepeatedSecretName(t)
			testDeletionReleasesSecretsAndStatus(t, bootstrapped, r)
		})
	})

	t.Run("status.accessedBuckets doesn't match spec.bucketClaims", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{} // no RPC calls should be made

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		testAccessedBucketsMalformed := func(t *testing.T) (
			bootstrapped *cositest.Dependencies,
			reconciler *sidecar.BucketAccessReconciler,
		) {
			bootstrapped = cositest.MustBootstrap(t,
				baseAccess.DeepCopy(),
				baseClass.DeepCopy(),
				baseReadWriteClaim.DeepCopy(),
				baseReadOnlyClaim.DeepCopy(),
				cositest.OpinionatedS3BucketClass(),
			)
			ctx := bootstrapped.ContextWithLogger

			reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)
			initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

			// make status.accessedBuckets not match spec.bucketClaims
			// e.g., what if COSI Controller has a bug
			malformedAccess := initAccess.DeepCopy()
			malformedAccess.Spec.BucketClaims[0].BucketClaimName = "something-different"
			require.NoError(t, bootstrapped.Client.Update(ctx, malformedAccess))

			r := newReconciler(bootstrapped.Client, rpcClient)

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.Error(t, err)
			assert.ErrorIs(t, err, reconcile.TerminalError(nil))
			assert.Empty(t, res)

			access, rwBucket, roBucket, _, _ := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
			assert.Equal(t, malformedAccess.Spec, access.Spec)
			require.NotNil(t, access.Status.Error)
			assert.NotNil(t, access.Status.Error.Time)
			assert.NotNil(t, access.Status.Error.Message)
			assert.Contains(t, *access.Status.Error.Message, "something-different")
			{ // non-error fields stay the same
				accessNoError := access.DeepCopy()
				accessNoError.Status.Error = nil
				assert.Equal(t, malformedAccess.Status, accessNoError.Status)
			}

			// don't care if secrets exist

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)

			return bootstrapped, &r
		}

		t.Run("reconcile", func(t *testing.T) {
			testAccessedBucketsMalformed(t)
		})

		t.Run("subsequent deletion", func(t *testing.T) {
			bootstrapped, r := testAccessedBucketsMalformed(t)
			testDeletionReleasesSecretsAndStatus(t, bootstrapped, r)
		})
	})

	t.Run("a BucketClaim has no access modes set", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{} // no RPC calls should be made

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		testEmptyAccessModes := func(t *testing.T) (
			bootstrapped *cositest.Dependencies,
			reconciler *sidecar.BucketAccessReconciler,
		) {
			bootstrapped = cositest.MustBootstrap(t,
				baseAccess.DeepCopy(),
				baseClass.DeepCopy(),
				baseReadWriteClaim.DeepCopy(),
				baseReadOnlyClaim.DeepCopy(),
				cositest.OpinionatedS3BucketClass(),
			)
			ctx := bootstrapped.ContextWithLogger

			reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)
			initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

			// clear access modes after Controller-side validation already passed
			// e.g., what if COSI Controller has a bug
			malformedAccess := initAccess.DeepCopy()
			malformedAccess.Spec.BucketClaims[0].AccessModes = cosiapi.BucketAccessModes{}
			require.NoError(t, bootstrapped.Client.Update(ctx, malformedAccess))

			r := newReconciler(bootstrapped.Client, rpcClient)

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.Error(t, err)
			assert.ErrorIs(t, err, reconcile.TerminalError(nil))
			assert.Empty(t, res)

			access, rwBucket, roBucket, _, _ := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
			assert.Equal(t, malformedAccess.Spec, access.Spec)
			require.NotNil(t, access.Status.Error)
			assert.NotNil(t, access.Status.Error.Time)
			assert.NotNil(t, access.Status.Error.Message)
			assert.Contains(t, *access.Status.Error.Message, "has no access modes set")
			{ // non-error fields stay the same
				accessNoError := access.DeepCopy()
				accessNoError.Status.Error = nil
				assert.Equal(t, malformedAccess.Status, accessNoError.Status)
			}

			// don't care if secrets exist

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)

			return bootstrapped, &r
		}

		t.Run("reconcile", func(t *testing.T) {
			testEmptyAccessModes(t)
		})

		t.Run("subsequent deletion", func(t *testing.T) {
			bootstrapped, r := testEmptyAccessModes(t)
			testDeletionReleasesSecretsAndStatus(t, bootstrapped, r)
		})
	})

	t.Run("a bucket has deleting annotation", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{} // no RPC calls should be made

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		testBucketDeletingAnnotation := func(t *testing.T) (
			bootstrapped *cositest.Dependencies,
			reconciler *sidecar.BucketAccessReconciler,
		) {
			bootstrapped = cositest.MustBootstrap(t,
				baseAccess.DeepCopy(),
				baseClass.DeepCopy(),
				baseReadWriteClaim.DeepCopy(),
				baseReadOnlyClaim.DeepCopy(),
				cositest.OpinionatedS3BucketClass(),
			)
			ctx := bootstrapped.ContextWithLogger

			reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)
			initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

			// BucketClaim controller doesn't know how to do deletion to Buckets yet, so simulate Bucket deletion annotation here
			deletingBucket := initRwBucket.DeepCopy()
			deletingBucket.Annotations = map[string]string{
				cosiapi.BucketClaimBeingDeletedAnnotation: "",
			}
			require.NoError(t, bootstrapped.Client.Update(ctx, deletingBucket))

			r := newReconciler(bootstrapped.Client, rpcClient)

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.Error(t, err)
			assert.ErrorIs(t, err, reconcile.TerminalError(nil))
			assert.Empty(t, res)

			access, rwBucket, roBucket, _, _ := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
			assert.Equal(t, baseAccess.Spec, access.Spec)
			require.NotNil(t, access.Status.Error)
			assert.NotNil(t, access.Status.Error.Time)
			assert.NotNil(t, access.Status.Error.Message)
			assert.Contains(t, *access.Status.Error.Message, deletingBucket.Name)
			{ // non-error fields stay the same
				accessNoError := access.DeepCopy()
				accessNoError.Status.Error = nil
				assert.Equal(t, initAccess.Status, accessNoError.Status)
			}

			// don't care if secrets exist

			// buckets must not be changed
			assert.Equal(t, deletingBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)

			return bootstrapped, &r
		}

		t.Run("reconcile", func(t *testing.T) {
			testBucketDeletingAnnotation(t)
		})

		t.Run("subsequent deletion", func(t *testing.T) {
			bootstrapped, r := testBucketDeletingAnnotation(t)
			testDeletionReleasesSecretsAndStatus(t, bootstrapped, r)
		})
	})

	t.Run("a bucket was provisioned by a different driver", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{} // no RPC calls should be made

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		testBucketDriverNameMismatch := func(t *testing.T) (
			bootstrapped *cositest.Dependencies,
			reconciler *sidecar.BucketAccessReconciler,
		) {
			bootstrapped = cositest.MustBootstrap(t,
				baseAccess.DeepCopy(),
				baseClass.DeepCopy(),
				baseReadWriteClaim.DeepCopy(),
				baseReadOnlyClaim.DeepCopy(),
				cositest.OpinionatedS3BucketClass(),
			)
			ctx := bootstrapped.ContextWithLogger

			reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)
			initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

			// Simulate a Bucket that was actually provisioned by a different driver than the one
			// named by the BucketAccessClass/BucketAccess. This must never be granted access by
			// this reconciler's driver.
			mismatchedBucket := initRwBucket.DeepCopy()
			mismatchedBucket.Spec.DriverName = "some-other-driver"
			require.NoError(t, bootstrapped.Client.Update(ctx, mismatchedBucket))

			r := newReconciler(bootstrapped.Client, rpcClient)

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.Error(t, err)
			assert.ErrorIs(t, err, reconcile.TerminalError(nil))
			assert.Empty(t, res)

			access, rwBucket, roBucket, _, _ := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
			assert.Equal(t, baseAccess.Spec, access.Spec)
			require.NotNil(t, access.Status.Error)
			assert.NotNil(t, access.Status.Error.Time)
			assert.NotNil(t, access.Status.Error.Message)
			assert.Contains(t, *access.Status.Error.Message, mismatchedBucket.Name)
			assert.Contains(t, *access.Status.Error.Message, "some-other-driver")
			{ // non-error fields stay the same
				accessNoError := access.DeepCopy()
				accessNoError.Status.Error = nil
				assert.Equal(t, initAccess.Status, accessNoError.Status)
			}

			// don't care if secrets exist

			// buckets must not be changed
			assert.Equal(t, mismatchedBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)

			return bootstrapped, &r
		}

		t.Run("reconcile", func(t *testing.T) {
			testBucketDriverNameMismatch(t)
		})

		t.Run("subsequent deletion", func(t *testing.T) {
			bootstrapped, r := testBucketDriverNameMismatch(t)
			testDeletionReleasesSecretsAndStatus(t, bootstrapped, r)
		})
	})

	t.Run("a bucket does not exist", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{} // no RPC calls should be made

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		testBucketDoesNotExist := func(t *testing.T) (
			bootstrapped *cositest.Dependencies,
			reconciler *sidecar.BucketAccessReconciler,
		) {
			bootstrapped = cositest.MustBootstrap(t,
				baseAccess.DeepCopy(),
				baseClass.DeepCopy(),
				baseReadWriteClaim.DeepCopy(),
				baseReadOnlyClaim.DeepCopy(),
				cositest.OpinionatedS3BucketClass(),
			)
			ctx := bootstrapped.ContextWithLogger

			reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)
			initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

			// delete one Bucket to simulate non-existence
			deletedBucket := initRoBucket.DeepCopy()
			deletedBucket.Finalizers = nil
			require.NoError(t, bootstrapped.Client.Update(ctx, deletedBucket)) // remove finalizer
			require.NoError(t, bootstrapped.Client.Delete(ctx, deletedBucket))
			err = bootstrapped.Client.Get(ctx, cositest.NsName(deletedBucket), deletedBucket)
			require.True(t, kerrors.IsNotFound(err)) // make sure it's gone from the client

			r := newReconciler(bootstrapped.Client, rpcClient)

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.Error(t, err)
			assert.ErrorIs(t, err, reconcile.TerminalError(nil))
			assert.Empty(t, res)

			access, rwBucket, _, _, _ := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
			assert.Equal(t, baseAccess.Spec, access.Spec)
			require.NotNil(t, access.Status.Error)
			assert.NotNil(t, access.Status.Error.Time)
			assert.NotNil(t, access.Status.Error.Message)
			assert.Contains(t, *access.Status.Error.Message, initRoBucket.Name)
			{ // non-error fields stay the same
				accessNoError := access.DeepCopy()
				accessNoError.Status.Error = nil
				assert.Equal(t, initAccess.Status, accessNoError.Status)
			}

			// don't care if secrets exist

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			// read-only bucket is deleted though

			return bootstrapped, &r
		}

		t.Run("reconcile", func(t *testing.T) {
			testBucketDoesNotExist(t)
		})

		t.Run("subsequent deletion", func(t *testing.T) {
			bootstrapped, r := testBucketDoesNotExist(t)
			testDeletionReleasesSecretsAndStatus(t, bootstrapped, r)
		})
	})

	t.Run("driver name mismatch", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{} // no RPC calls should be made

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		testDriverNameMismatch := func(t *testing.T) (
			bootstrapped *cositest.Dependencies,
			reconciler *sidecar.BucketAccessReconciler,
		) {
			bootstrapped = cositest.MustBootstrap(t,
				baseAccess.DeepCopy(),
				baseClass.DeepCopy(),
				baseReadWriteClaim.DeepCopy(),
				baseReadOnlyClaim.DeepCopy(),
				cositest.OpinionatedS3BucketClass(),
			)
			ctx := bootstrapped.ContextWithLogger

			reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)
			initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

			// remove finalizer from access to determine if reconcile runs (added back) or is skipped (remains removed)
			noFinalizer := &cosiapi.BucketAccess{}
			require.NoError(t, bootstrapped.Client.Get(ctx, cositest.NsName(&baseAccess), noFinalizer))
			noFinalizer.Finalizers = nil
			require.NoError(t, bootstrapped.Client.Update(ctx, noFinalizer))

			r := newReconciler(bootstrapped.Client, rpcClient)
			r.DriverInfo.Name = "wrong.name"

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			access, rwBucket, roBucket, _, _ := getAllResources(bootstrapped)

			assert.NotContains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer) // should skip reconcile
			assert.Equal(t, baseAccess.Spec, access.Spec)
			assert.Equal(t, initAccess.Status, access.Status)

			// don't care if secrets exist

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)

			return bootstrapped, &r
		}

		t.Run("reconcile", func(t *testing.T) {
			testDriverNameMismatch(t)
		})

		t.Run("subsequent deletion", func(t *testing.T) {
			bootstrapped, r := testDriverNameMismatch(t)
			ctx := bootstrapped.ContextWithLogger

			initAccess, initRwBucket, initRoBucket, initRwSec, initRoSec := getAllResources(bootstrapped)

			delAccess := initAccess.DeepCopy()
			delAccess.Finalizers = append(delAccess.Finalizers, cosiapi.ProtectionFinalizer)
			require.NoError(t, bootstrapped.Client.Update(ctx, delAccess)) // put finalizer back before deleting
			require.NoError(t, bootstrapped.Client.Delete(ctx, delAccess))
			require.NoError(t, bootstrapped.Client.Get(ctx, cositest.NsName(delAccess), delAccess))

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			access, rwBucket, roBucket, rwSec, roSec := getAllResources(bootstrapped)

			t.Logf("access: %#v", access)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
			assert.NotContains(t, access.Annotations, cosiapi.SidecarCleanupFinishedAnnotation)
			assert.Equal(t, delAccess.ObjectMeta, access.ObjectMeta)
			assert.Equal(t, initAccess.Spec, access.Spec)
			assert.Equal(t, initAccess.Status, access.Status)

			// secrets still present
			assert.Equal(t, initRwSec, rwSec)
			assert.Equal(t, initRoSec, roSec)

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)
		})
	})

	t.Run("rpc return mistakes", func(t *testing.T) {

		type rpcReturnMistakeTest struct {
			testName  string
			rpcReturn *cosiproto.DriverGrantBucketAccessResponse
		}
		tests := []rpcReturnMistakeTest{
			{
				"credentials nil",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Credentials = nil
					return ret
				}(),
			},
			{
				"credentials expected proto nil",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Credentials.S3 = nil
					return ret
				}(),
			},
			{
				"credentials add wrong proto",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Credentials.Gcs = &cosiproto.GcsCredentialInfo{}
					return ret
				}(),
			},
			{
				"credentials invalid",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Credentials.S3.AccessKeyId = "" // S3 requires access key ID for Key auth
					return ret
				}(),
			},
			{
				"bucket info response nil",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Buckets = nil
					return ret
				}(),
			},
			{
				"bucket info response empty",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Buckets = []*cosiproto.DriverGrantBucketAccessResponse_BucketInfo{}
					return ret
				}(),
			},
			{
				"a bucket info response is missing",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Buckets = ret.Buckets[0:1]
					return ret
				}(),
			},
			{
				"extra bucket info response",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Buckets = append(ret.Buckets, &cosiproto.DriverGrantBucketAccessResponse_BucketInfo{
						BucketId: "something",
						BucketInfo: &cosiproto.ObjectProtocolAndBucketInfo{
							S3: &cosiproto.S3BucketInfo{
								BucketId:        "something",
								Endpoint:        "something",
								Region:          "something",
								AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
							},
						},
					})
					return ret
				}(),
			},
			{
				"a bucket id missing",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Buckets[0].BucketId = ""
					return ret
				}(),
			},
			{
				"a bucket info nil",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Buckets[0].BucketInfo = nil
					return ret
				}(),
			},
			{
				"a bucket info adds wrong proto",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Buckets[0].BucketInfo.Azure = &cosiproto.AzureBucketInfo{}
					return ret
				}(),
			},
			{
				"a bucket info expected proto nil",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Buckets[0].BucketInfo.S3 = nil
					return ret
				}(),
			},
			{
				"a bucket info proto invalid",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Buckets[0].BucketInfo.S3.Endpoint = "" // S3 requires endpoint to be set
					return ret
				}(),
			},
			{
				"a bucket info unknown bucket id",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse()
					ret.Buckets[0].BucketId = "something-random"
					return ret
				}(),
			},
		}

		var requestReturn *cosiproto.DriverGrantBucketAccessResponse
		revokeRequests := []*cosiproto.DriverRevokeBucketAccessRequest{}
		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketAccessIdFunc: sidecartest.OpinionatedGenerateBucketAccessIdFunc,
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				return requestReturn, nil
			},
			RevokeBucketAccessFunc: func(ctx context.Context, drbar *cosiproto.DriverRevokeBucketAccessRequest) (*cosiproto.DriverRevokeBucketAccessResponse, error) {
				revokeRequests = append(revokeRequests, drbar)
				return &cosiproto.DriverRevokeBucketAccessResponse{}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		for _, tt := range tests {
			testRpcReturnMistake := func(t *testing.T, testDef *rpcReturnMistakeTest) (
				bootstrapped *cositest.Dependencies,
				reconciler *sidecar.BucketAccessReconciler,
			) {
				requestReturn = testDef.rpcReturn

				bootstrapped = cositest.MustBootstrap(t,
					baseAccess.DeepCopy(),
					baseClass.DeepCopy(),
					baseReadWriteClaim.DeepCopy(),
					baseReadOnlyClaim.DeepCopy(),
					cositest.OpinionatedS3BucketClass(),
				)
				ctx := bootstrapped.ContextWithLogger

				reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)
				initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

				r := newReconciler(bootstrapped.Client, rpcClient)

				res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
				assert.Error(t, err)
				assert.ErrorIs(t, err, reconcile.TerminalError(nil))
				assert.Empty(t, res)

				access, rwBucket, roBucket, rwSec, roSec := getAllResources(bootstrapped)

				assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
				assert.Equal(t, baseAccess.Spec, access.Spec)
				require.NotNil(t, access.Status.Error)
				assert.NotNil(t, access.Status.Error.Time)
				assert.NotNil(t, access.Status.Error.Message)
				assert.Contains(t, *access.Status.Error.Message, "granted access")
				assert.Contains(t, *access.Status.Error.Message, "invalid")
				{ // non-error fields stay the same
					accessNoError := access.DeepCopy()
					accessNoError.Status.Error = nil
					// the account ID was persisted before the invalid grant response was received
					wantStatus := initAccess.Status.DeepCopy()
					wantStatus.AccountID = "cosi-ba-zxcvbn"
					wantStatus.ReadyToUse = ptr.To(false)
					assert.Equal(t, *wantStatus, accessNoError.Status)
				}

				// secrets should have been created to claim them, but not updated with data
				for _, s := range []*corev1.Secret{rwSec, roSec} {
					assert.Contains(t, s.GetFinalizers(), cosiapi.ProtectionFinalizer)
					require.Len(t, s.OwnerReferences, 1)
					assert.Len(t, s.StringData, 0)
				}

				// buckets must not have been updated
				assert.Equal(t, initRwBucket, rwBucket)
				assert.Equal(t, initRoBucket, roBucket)

				return bootstrapped, &r
			}

			t.Run(tt.testName, func(t *testing.T) {
				t.Run("reconcile", func(t *testing.T) {
					testRpcReturnMistake(t, &tt)
				})

				t.Run("subsequent deletion", func(t *testing.T) {
					bootstrapped, r := testRpcReturnMistake(t, &tt)
					revokeRequests = nil
					testDeletionReleasesSecretsAndStatus(t, bootstrapped, r)

					// the backend account may exist, so deletion must revoke the persisted ID
					require.Len(t, revokeRequests, 1)
					assert.Equal(t, "cosi-ba-zxcvbn", revokeRequests[0].AccountId)
				})
			})
		}
	})

	t.Run("azure protocol and serviceaccount auth", func(t *testing.T) {
		grantRequests := []*cosiproto.DriverGrantBucketAccessRequest{}
		revokeRequests := []*cosiproto.DriverRevokeBucketAccessRequest{}
		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketAccessIdFunc: sidecartest.OpinionatedGenerateBucketAccessIdFunc,
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				grantRequests = append(grantRequests, dgbar)
				ret := &cosiproto.DriverGrantBucketAccessResponse{
					Credentials: &cosiproto.CredentialInfo{
						Azure: &cosiproto.AzureCredentialInfo{
							// empty for ServiceAccount auth
						},
					},
					Buckets: []*cosiproto.DriverGrantBucketAccessResponse_BucketInfo{
						{
							BucketId: "cosi-bc-my-ns-readwrite-bucket",
							BucketInfo: &cosiproto.ObjectProtocolAndBucketInfo{
								Azure: &cosiproto.AzureBucketInfo{
									StorageAccount: "outputaccount",
								},
							},
						},
						{
							BucketId: "cosi-bc-my-ns-readonly-bucket",
							BucketInfo: &cosiproto.ObjectProtocolAndBucketInfo{
								Azure: &cosiproto.AzureBucketInfo{
									StorageAccount: "inputaccount",
								},
							},
						},
					},
				}
				return ret, nil
			},
			RevokeBucketAccessFunc: func(ctx context.Context, drbar *cosiproto.DriverRevokeBucketAccessRequest) (*cosiproto.DriverRevokeBucketAccessResponse, error) {
				revokeRequests = append(revokeRequests, drbar)
				return &cosiproto.DriverRevokeBucketAccessResponse{}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		azureAccess := cosiapi.BucketAccess{
			ObjectMeta: baseAccess.DeepCopy().ObjectMeta,
			Spec: cosiapi.BucketAccessSpec{
				Protocol:              cosiapi.ObjectProtocolAzure,
				BucketAccessClassName: "azure-class",
				ServiceAccountName:    "azure-sa",
				BucketClaims:          baseAccess.DeepCopy().Spec.BucketClaims,
			},
		}

		azureClass := cosiapi.BucketAccessClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "azure-class",
			},
			Spec: cosiapi.BucketAccessClassSpec{
				DriverName:         cositest.OpinionatedAzureBucketClass().Spec.DriverName,
				AuthenticationType: cosiapi.BucketAccessAuthenticationTypeServiceAccount,
				MultiBucketAccess:  cosiapi.MultiBucketAccessMultipleBuckets,
			},
		}

		azureRwClaim := cositest.OpinionatedAzureBucketClaim("my-ns", "readwrite-bucket")
		azureRoClaim := cositest.OpinionatedAzureBucketClaim("my-ns", "readonly-bucket")

		testAzureAndServiceAccount := func(t *testing.T) (
			bootstrapped *cositest.Dependencies,
			reconciler *sidecar.BucketAccessReconciler,
		) {
			bootstrapped = cositest.MustBootstrap(t,
				azureAccess.DeepCopy(),
				azureClass.DeepCopy(),
				azureRwClaim.DeepCopy(),
				azureRoClaim.DeepCopy(),
				cositest.OpinionatedAzureBucketClass(),
			)
			ctx := bootstrapped.ContextWithLogger

			// reconcile BucketClaims to create intermediate Buckets
			initRwClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(azureRwClaim))
			require.NoError(t, err)
			initRoClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(azureRoClaim))
			require.NoError(t, err)

			// reconcile intermediate Buckets
			initRwBucket, err := sidecartest.ReconcileOpinionatedAzureBucket(t, bootstrapped, cositest.BucketNsName(initRwClaim))
			require.NoError(t, err)
			initRoBucket, err := sidecartest.ReconcileOpinionatedAzureBucket(t, bootstrapped, cositest.BucketNsName(initRoClaim))
			require.NoError(t, err)

			// reconcile BucketClaims to mirror finished status from Buckets
			initRwClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(azureRwClaim))
			require.NoError(t, err)
			require.True(t, *initRwClaim.Status.ReadyToUse)
			initRoClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(azureRoClaim))
			require.NoError(t, err)
			require.True(t, *initRoClaim.Status.ReadyToUse)

			// initialize the BucketAccess using the COSI Controller logic
			initAccess, err := controllertest.ReconcileBucketAccess(t, bootstrapped, cositest.NsName(&azureAccess))
			require.NoError(t, err)
			require.NotEmpty(t, initAccess.Status.AccessedBuckets)

			grantRequests = []*cosiproto.DriverGrantBucketAccessRequest{}   // empty the seen rpc requests
			revokeRequests = []*cosiproto.DriverRevokeBucketAccessRequest{} // empty the seen rpc requests

			r := newReconciler(bootstrapped.Client, rpcClient)
			r.DriverInfo = sidecar.DriverInfo{
				Name:               cositest.OpinionatedAzureBucketClass().Spec.DriverName,
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_AZURE},
				ProvisionerClient:  rpcClient,
			}

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&azureAccess)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			// ensure the expected RPC call was made
			require.Len(t, grantRequests, 1)
			assert.Len(t, revokeRequests, 0)
			req := grantRequests[0]
			assert.Equal(t, "cosi-ba-zxcvbn", req.AccountId)
			assert.Equal(t, cosiproto.AuthenticationType_SERVICE_ACCOUNT, req.AuthenticationType.Type)
			assert.Equal(t, cosiproto.ObjectProtocol_AZURE, req.Protocol.Type)
			assert.Equal(t, "azure-sa", req.ServiceAccountName)
			assert.Empty(t, req.Parameters)
			require.Len(t, req.Buckets, 2) // by RPC spec, order of requested accessed buckets is random
			assert.True(t, accessedBucketRequestExists(req.Buckets, &cosiproto.DriverGrantBucketAccessRequest_AccessedBucket{
				BucketId:                 initRwBucket.Status.BucketID,
				ObjectDataAccessMode:     &cosiproto.AccessMode{Mode: cosiproto.AccessMode_READ_WRITE},
				ObjectMetadataAccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_ANY},
				BucketMetadataAccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_ANY},
			}))
			assert.True(t, accessedBucketRequestExists(req.Buckets, &cosiproto.DriverGrantBucketAccessRequest_AccessedBucket{
				BucketId:                 initRoBucket.Status.BucketID,
				ObjectDataAccessMode:     &cosiproto.AccessMode{Mode: cosiproto.AccessMode_READ_ONLY},
				ObjectMetadataAccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_ANY},
				BucketMetadataAccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_ANY},
			}))

			access, rwBucket, roBucket, rwSec, roSec := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
			assert.Equal(t, azureAccess.Spec, access.Spec) // spec should not change
			assert.True(t, *access.Status.ReadyToUse)
			assert.Nil(t, access.Status.Error)
			assert.Equal(t, "cosi-ba-zxcvbn", access.Status.AccountID)
			assert.Equal(t, initAccess.Status.AccessedBuckets, access.Status.AccessedBuckets)
			assert.Equal(t, initAccess.Status.AuthenticationType, access.Status.AuthenticationType)
			assert.Equal(t, initAccess.Status.DriverName, access.Status.DriverName)
			assert.Empty(t, access.Status.Parameters)

			// ensure secrets are present with info
			for _, s := range []*corev1.Secret{rwSec, roSec} {
				assert.Contains(t, s.GetFinalizers(), cosiapi.ProtectionFinalizer)
				require.Len(t, s.OwnerReferences, 1)
				assert.Equal(t, "zxcvbn", string(s.OwnerReferences[0].UID))
			}
			assert.Equal(t, "outputaccount", rwSec.StringData[string(cosiapi.BucketInfoVar_Azure_StorageAccount)])
			assert.Equal(t, "inputaccount", roSec.StringData[string(cosiapi.BucketInfoVar_Azure_StorageAccount)])

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)

			return bootstrapped, &r
		}

		t.Run("reconcile", func(t *testing.T) {
			testAzureAndServiceAccount(t)
		})

		t.Run("subsequent deletion", func(t *testing.T) {
			bootstrapped, r := testAzureAndServiceAccount(t)
			ctx := bootstrapped.ContextWithLogger

			initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

			require.NoError(t, bootstrapped.Client.Delete(ctx, initAccess.DeepCopy()))

			grantRequests = []*cosiproto.DriverGrantBucketAccessRequest{}   // empty the seen rpc requests
			revokeRequests = []*cosiproto.DriverRevokeBucketAccessRequest{} // empty the seen rpc requests

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&azureAccess)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			require.Len(t, grantRequests, 0)
			require.Len(t, revokeRequests, 1)
			req := revokeRequests[0]
			assert.Equal(t, "cosi-ba-zxcvbn", req.AccountId)
			assert.Equal(t, cosiproto.ObjectProtocol_AZURE, req.Protocol.Type)
			assert.Equal(t, cosiproto.AuthenticationType_SERVICE_ACCOUNT, req.AuthenticationType.Type)
			assert.Equal(t, "azure-sa", req.ServiceAccountName)
			require.Len(t, req.Buckets, 2)
			assert.Equal(t, "cosi-bc-my-ns-readwrite-bucket", req.Buckets[0].BucketId)
			assert.Equal(t, "cosi-bc-my-ns-readonly-bucket", req.Buckets[1].BucketId)
			assert.Empty(t, req.Parameters)

			access, rwBucket, roBucket, _, _ := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer) // Controller still needs to clean up
			assert.Equal(t, initAccess.Spec, access.Spec)
			assert.False(t, *access.Status.ReadyToUse)
			assert.Nil(t, access.Status.Error)
			{ // non-ready fields stay the same
				delReady := access.DeepCopy()
				delReady.Status.ReadyToUse = ptr.To(true)
				assert.Equal(t, initAccess.Status, delReady.Status)
			}

			// secrets are deleted
			bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 0), &corev1.Secret{})
			bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 1), &corev1.Secret{})

			// buckets must not be changed
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)
		})
	})

	t.Run("GCS protocol", func(t *testing.T) {
		grantRequests := []*cosiproto.DriverGrantBucketAccessRequest{}
		revokeRequests := []*cosiproto.DriverRevokeBucketAccessRequest{}
		fakeServer := cositest.FakeProvisionerServer{
			GenerateBucketAccessIdFunc: sidecartest.OpinionatedGenerateBucketAccessIdFunc,
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				grantRequests = append(grantRequests, dgbar)
				ret := &cosiproto.DriverGrantBucketAccessResponse{
					Credentials: &cosiproto.CredentialInfo{
						Gcs: &cosiproto.GcsCredentialInfo{
							AccessId:       "accessid",
							AccessSecret:   "accesssecret",
							PrivateKeyName: "privatekeyname",
							ServiceAccount: "serviceaccountname",
						},
					},
					Buckets: []*cosiproto.DriverGrantBucketAccessResponse_BucketInfo{
						{
							BucketId: "cosi-bc-my-ns-readwrite-bucket",
							BucketInfo: &cosiproto.ObjectProtocolAndBucketInfo{
								Gcs: &cosiproto.GcsBucketInfo{
									ProjectId:  "projectid",
									BucketName: "corp-cosi-bc-qwerty",
								},
							},
						},
						{
							BucketId: "cosi-bc-my-ns-readonly-bucket",
							BucketInfo: &cosiproto.ObjectProtocolAndBucketInfo{
								Gcs: &cosiproto.GcsBucketInfo{
									ProjectId:  "projectid",
									BucketName: "corp-cosi-bc-asdfgh",
								},
							},
						},
					},
				}
				return ret, nil
			},
			RevokeBucketAccessFunc: func(ctx context.Context, drbar *cosiproto.DriverRevokeBucketAccessRequest) (*cosiproto.DriverRevokeBucketAccessResponse, error) {
				revokeRequests = append(revokeRequests, drbar)
				return &cosiproto.DriverRevokeBucketAccessResponse{}, nil
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		gcsAccess := cosiapi.BucketAccess{
			ObjectMeta: baseAccess.DeepCopy().ObjectMeta,
			Spec: cosiapi.BucketAccessSpec{
				Protocol:              cosiapi.ObjectProtocolGcs,
				BucketAccessClassName: "gcs-class",
				BucketClaims:          baseAccess.DeepCopy().Spec.BucketClaims,
			},
		}

		gcsClass := cosiapi.BucketAccessClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "gcs-class",
			},
			Spec: cosiapi.BucketAccessClassSpec{
				DriverName:         cositest.OpinionatedGcsBucketClass().Spec.DriverName,
				AuthenticationType: cosiapi.BucketAccessAuthenticationTypeKey,
				MultiBucketAccess:  cosiapi.MultiBucketAccessMultipleBuckets,
				Parameters: map[string]string{
					"maxSize": "100Gi",
					"maxIops": "10",
				},
			},
		}

		gcsRwClaim := cositest.OpinionatedGcsBucketClaim("my-ns", "readwrite-bucket")
		gcsRoClaim := cositest.OpinionatedGcsBucketClaim("my-ns", "readonly-bucket")

		testGcs := func(t *testing.T) (
			bootstrapped *cositest.Dependencies,
			reconciler *sidecar.BucketAccessReconciler,
		) {
			bootstrapped = cositest.MustBootstrap(t,
				gcsAccess.DeepCopy(),
				gcsClass.DeepCopy(),
				gcsRwClaim.DeepCopy(),
				gcsRoClaim.DeepCopy(),
				cositest.OpinionatedGcsBucketClass(),
			)
			ctx := bootstrapped.ContextWithLogger

			// reconcile BucketClaims to create intermediate Buckets
			initRwClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(gcsRwClaim))
			require.NoError(t, err)
			initRoClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(gcsRoClaim))
			require.NoError(t, err)

			// reconcile intermediate Buckets
			initRwBucket, err := sidecartest.ReconcileOpinionatedGcsBucket(t, bootstrapped, cositest.BucketNsName(initRwClaim))
			require.NoError(t, err)
			initRoBucket, err := sidecartest.ReconcileOpinionatedGcsBucket(t, bootstrapped, cositest.BucketNsName(initRoClaim))
			require.NoError(t, err)

			// reconcile BucketClaims to mirror finished status from Buckets
			initRwClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(gcsRwClaim))
			require.NoError(t, err)
			require.True(t, *initRwClaim.Status.ReadyToUse)
			initRoClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(gcsRoClaim))
			require.NoError(t, err)
			require.True(t, *initRoClaim.Status.ReadyToUse)

			// initialize the BucketAccess using the COSI Controller logic
			initAccess, err := controllertest.ReconcileBucketAccess(t, bootstrapped, cositest.NsName(&gcsAccess))
			require.NoError(t, err)
			require.NotEmpty(t, initAccess.Status.AccessedBuckets)

			r := newReconciler(bootstrapped.Client, rpcClient)
			r.DriverInfo = sidecar.DriverInfo{
				Name: cositest.OpinionatedGcsBucketClass().Spec.DriverName,
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{
					cosiproto.ObjectProtocol_S3,
					cosiproto.ObjectProtocol_GCS,
				},
				ProvisionerClient: rpcClient,
			}

			grantRequests = []*cosiproto.DriverGrantBucketAccessRequest{}   // empty the seen rpc requests
			revokeRequests = []*cosiproto.DriverRevokeBucketAccessRequest{} // empty the seen rpc requests

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&gcsAccess)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			// ensure the expected RPC call was made
			require.Len(t, grantRequests, 1)
			assert.Len(t, revokeRequests, 0)
			req := grantRequests[0]
			assert.Equal(t, "cosi-ba-zxcvbn", req.AccountId)
			assert.Equal(t, cosiproto.AuthenticationType_KEY, req.AuthenticationType.Type)
			assert.Equal(t, cosiproto.ObjectProtocol_GCS, req.Protocol.Type)
			assert.Equal(t, "", req.ServiceAccountName)
			assert.Equal(t,
				map[string]string{
					"maxSize": "100Gi",
					"maxIops": "10",
				},
				req.Parameters,
			)
			require.Len(t, req.Buckets, 2) // by RPC spec, order of requested accessed buckets is random
			assert.True(t, accessedBucketRequestExists(req.Buckets, &cosiproto.DriverGrantBucketAccessRequest_AccessedBucket{
				BucketId:                 initRwBucket.Status.BucketID,
				ObjectDataAccessMode:     &cosiproto.AccessMode{Mode: cosiproto.AccessMode_READ_WRITE},
				ObjectMetadataAccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_ANY},
				BucketMetadataAccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_ANY},
			}))
			assert.True(t, accessedBucketRequestExists(req.Buckets, &cosiproto.DriverGrantBucketAccessRequest_AccessedBucket{
				BucketId:                 initRoBucket.Status.BucketID,
				ObjectDataAccessMode:     &cosiproto.AccessMode{Mode: cosiproto.AccessMode_READ_ONLY},
				ObjectMetadataAccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_ANY},
				BucketMetadataAccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_ANY},
			}))

			access, initRwBucket, initRoBucket, rwSec, roSec := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
			assert.Equal(t, gcsAccess.Spec, access.Spec) // spec should not change
			assert.True(t, *access.Status.ReadyToUse)
			assert.Nil(t, access.Status.Error)
			assert.Equal(t, "cosi-ba-zxcvbn", access.Status.AccountID)
			assert.Equal(t, initAccess.Status.AccessedBuckets, access.Status.AccessedBuckets)
			assert.Equal(t, initAccess.Status.AuthenticationType, access.Status.AuthenticationType)
			assert.Equal(t, initAccess.Status.DriverName, access.Status.DriverName)
			assert.Equal(t, initAccess.Status.Parameters, access.Status.Parameters)

			// ensure secrets are present with info
			for _, s := range []*corev1.Secret{rwSec, roSec} {
				assert.Contains(t, s.GetFinalizers(), cosiapi.ProtectionFinalizer)
				require.Len(t, s.OwnerReferences, 1)
				assert.Equal(t, "zxcvbn", string(s.OwnerReferences[0].UID))
				assert.Equal(t, "accessid", s.StringData[string(cosiapi.CredentialVar_GCS_AccessId)])
			}
			assert.Equal(t, "corp-cosi-bc-qwerty", rwSec.StringData[string(cosiapi.BucketInfoVar_GCS_BucketName)])
			assert.Equal(t, "corp-cosi-bc-asdfgh", roSec.StringData[string(cosiapi.BucketInfoVar_GCS_BucketName)])

			// buckets must not change
			assert.Equal(t, initRwBucket, initRwBucket)
			assert.Equal(t, initRoBucket, initRoBucket)

			return bootstrapped, &r
		}

		t.Run("reconcile", func(t *testing.T) {
			testGcs(t)
		})

		t.Run("subsequent deletion", func(t *testing.T) {
			bootstrapped, r := testGcs(t)
			ctx := bootstrapped.ContextWithLogger

			initAccess, initRwBucket, initRoBucket, _, _ := getAllResources(bootstrapped)

			require.NoError(t, bootstrapped.Client.Delete(ctx, initAccess.DeepCopy()))

			grantRequests = []*cosiproto.DriverGrantBucketAccessRequest{}   // empty the seen rpc requests
			revokeRequests = []*cosiproto.DriverRevokeBucketAccessRequest{} // empty the seen rpc requests

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&gcsAccess)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			require.Len(t, grantRequests, 0)
			require.Len(t, revokeRequests, 1)
			req := revokeRequests[0]
			assert.Equal(t, "cosi-ba-zxcvbn", req.AccountId)
			assert.Equal(t, cosiproto.ObjectProtocol_GCS, req.Protocol.Type)
			assert.Equal(t, cosiproto.AuthenticationType_KEY, req.AuthenticationType.Type)
			assert.Equal(t, "", req.ServiceAccountName)
			require.Len(t, req.Buckets, 2)
			assert.Equal(t, "cosi-bc-my-ns-readwrite-bucket", req.Buckets[0].BucketId)
			assert.Equal(t, "cosi-bc-my-ns-readonly-bucket", req.Buckets[1].BucketId)
			assert.Equal(t,
				map[string]string{
					"maxSize": "100Gi",
					"maxIops": "10",
				},
				req.Parameters,
			)

			access, rwBucket, roBucket, _, _ := getAllResources(bootstrapped)

			assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer) // Controller still needs to clean up
			assert.Equal(t, access.Spec, access.Spec)
			assert.False(t, *access.Status.ReadyToUse)
			assert.Nil(t, access.Status.Error)
			{ // non-ready fields stay the same
				delReady := access.DeepCopy()
				delReady.Status.ReadyToUse = ptr.To(true)
				assert.Equal(t, initAccess.Status, delReady.Status)
			}

			// secrets are deleted
			bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 0), &corev1.Secret{})
			bootstrapped.AssertResourceDoesNotExist(t, cositest.SecretNsName(&baseAccess, 1), &corev1.Secret{})

			// buckets must not change
			assert.Equal(t, initRwBucket, rwBucket)
			assert.Equal(t, initRoBucket, roBucket)
		})
	})

	t.Run("two-phase account provisioning", func(t *testing.T) {
		env := &bucketAccessAccountIdTestEnv{
			access:        &baseAccess,
			newReconciler: newReconciler,
			grantResponse: newBaseGrantResponse,
			bootstrap: func(t *testing.T) *cositest.Dependencies {
				bootstrapped := cositest.MustBootstrap(t,
					baseAccess.DeepCopy(),
					baseClass.DeepCopy(),
					baseReadWriteClaim.DeepCopy(),
					baseReadOnlyClaim.DeepCopy(),
					cositest.OpinionatedS3BucketClass(),
				)
				reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)
				return bootstrapped
			},
			getAccess: func(bootstrapped *cositest.Dependencies) *cosiapi.BucketAccess {
				access, _, _, _, _ := getAllResources(bootstrapped)
				return access
			},
		}

		t.Run("account ID persistence", func(t *testing.T) {
			bucketAccessAccountIdPersistenceTestSuite(t, env)
		})
		t.Run("Generate failures", func(t *testing.T) {
			bucketAccessGenerateIdFailureTestSuite(t, env)
		})
		t.Run("readyToUse=true with no account ID refused", func(t *testing.T) {
			bucketAccessReadyWithoutAccountIdTestSuite(t, env)
		})
		t.Run("Generate and Grant request consistency", func(t *testing.T) {
			bucketAccessGenerateGrantConsistencyTestSuite(t, env)
		})
	})
}

// bucketAccessAccountIdTestEnv is the fixture shared by the two-phase account provisioning
// suites: how to bootstrap an initialized BucketAccess and how to read it back.
type bucketAccessAccountIdTestEnv struct {
	access        *cosiapi.BucketAccess
	bootstrap     func(t *testing.T) *cositest.Dependencies
	newReconciler func(client.Client, cosiproto.ProvisionerClient) sidecar.BucketAccessReconciler
	getAccess     func(*cositest.Dependencies) *cosiapi.BucketAccess
	grantResponse func() *cosiproto.DriverGrantBucketAccessResponse
}

// accountIdDriver is the fake driver behavior for one scenario and a record of the calls made to it.
type accountIdDriver struct {
	generateId  string
	generateErr error
	grantErr    error // may be changed between reconciles

	generateReqs []*cosiproto.DriverGenerateBucketAccessIdRequest
	grantReqs    []*cosiproto.DriverGrantBucketAccessRequest
	revokeReqs   []*cosiproto.DriverRevokeBucketAccessRequest

	// BucketAccess as stored at the moment each Grant RPC arrived
	accessAtGrant []*cosiapi.BucketAccess
}

type reconcileErrKind int

const (
	errNone reconcileErrKind = iota
	errTerminal
	errRetryable
)

// accountIdReconcile is one reconcile of the BucketAccess and its expected result.
type accountIdReconcile struct {
	grantErr   error // Grant RPC outcome during this reconcile; nil succeeds
	wantErr    reconcileErrKind
	wantErrMsg string
}

type accountIdCase struct {
	name        string
	generateId  string
	generateErr error
	reconciles  []accountIdReconcile

	wantGenerateCalls int
	wantGrantCalls    int
	wantAccountId     string
	wantReady         bool
	wantStatusErrMsg  string // empty means status.error is not asserted unless wantReady
}

// start returns a reconciler for a freshly initialized BucketAccess whose driver follows d.
func (env *bucketAccessAccountIdTestEnv) start(
	t *testing.T, d *accountIdDriver,
) (*cositest.Dependencies, *sidecar.BucketAccessReconciler) {
	bootstrapped := env.bootstrap(t)

	fakeServer := cositest.FakeProvisionerServer{
		GenerateBucketAccessIdFunc: func(ctx context.Context, req *cosiproto.DriverGenerateBucketAccessIdRequest) (*cosiproto.DriverGenerateBucketAccessIdResponse, error) {
			d.generateReqs = append(d.generateReqs, req)
			if d.generateErr != nil {
				return nil, d.generateErr
			}
			return &cosiproto.DriverGenerateBucketAccessIdResponse{AccountId: d.generateId}, nil
		},
		GrantBucketAccessFunc: func(ctx context.Context, req *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
			d.grantReqs = append(d.grantReqs, req)
			stored := &cosiapi.BucketAccess{}
			assert.NoError(t, bootstrapped.Client.Get(ctx, cositest.NsName(env.access), stored))
			d.accessAtGrant = append(d.accessAtGrant, stored)
			if d.grantErr != nil {
				return nil, d.grantErr
			}
			return env.grantResponse(), nil
		},
		RevokeBucketAccessFunc: func(ctx context.Context, req *cosiproto.DriverRevokeBucketAccessRequest) (*cosiproto.DriverRevokeBucketAccessResponse, error) {
			d.revokeReqs = append(d.revokeReqs, req)
			return &cosiproto.DriverRevokeBucketAccessResponse{}, nil
		},
	}
	cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
	t.Cleanup(cleanup)
	require.NoError(t, err)
	go serve()
	conn, err := cositest.RpcClientConn(tmpSock)
	require.NoError(t, err)

	r := env.newReconciler(bootstrapped.Client, cosiproto.NewProvisionerClient(conn))
	return bootstrapped, &r
}

func (env *bucketAccessAccountIdTestEnv) reconcile(
	bootstrapped *cositest.Dependencies, r *sidecar.BucketAccessReconciler,
) error {
	_, err := r.Reconcile(bootstrapped.ContextWithLogger, ctrl.Request{NamespacedName: cositest.NsName(env.access)})
	return err
}

func assertReconcileErr(t *testing.T, err error, kind reconcileErrKind, msg string) {
	t.Helper()
	switch kind {
	case errNone:
		require.NoError(t, err)
		return
	case errTerminal:
		require.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
	case errRetryable:
		require.Error(t, err)
		assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
	}
	if msg != "" {
		assert.ErrorContains(t, err, msg)
	}
}

// runAccountIdCase bootstraps a BucketAccess, reconciles it as the case describes, and asserts
// the driver calls and persisted status. Every Grant RPC must see the expected account ID
// already persisted with readyToUse still unset.
func runAccountIdCase(t *testing.T, env *bucketAccessAccountIdTestEnv, tt accountIdCase) {
	d := &accountIdDriver{generateId: tt.generateId, generateErr: tt.generateErr}
	bootstrapped, r := env.start(t, d)

	for i, step := range tt.reconciles {
		d.grantErr = step.grantErr
		assertReconcileErr(t, env.reconcile(bootstrapped, r), step.wantErr, step.wantErrMsg)
		if t.Failed() {
			t.Fatalf("reconcile %d returned an unexpected result", i+1)
		}
	}

	assert.Len(t, d.generateReqs, tt.wantGenerateCalls, "DriverGenerateBucketAccessId calls")
	require.Len(t, d.grantReqs, tt.wantGrantCalls, "DriverGrantBucketAccess calls")
	for i, req := range d.grantReqs {
		assert.Equal(t, tt.wantAccountId, req.AccountId, "Grant %d account ID", i)
	}
	require.Len(t, d.accessAtGrant, tt.wantGrantCalls)
	for i, stored := range d.accessAtGrant {
		assert.Equal(t, tt.wantAccountId, stored.Status.AccountID, "account ID persisted before Grant %d", i)
		require.NotNil(t, stored.Status.ReadyToUse)
		assert.False(t, *stored.Status.ReadyToUse, "readyToUse during Grant %d", i)
	}

	access := env.getAccess(bootstrapped)
	assert.Equal(t, tt.wantAccountId, access.Status.AccountID)
	assert.Equal(t, tt.wantReady, ptr.Deref(access.Status.ReadyToUse, false))
	if tt.wantReady {
		assert.Nil(t, access.Status.Error)
	}
	if tt.wantStatusErrMsg != "" {
		require.NotNil(t, access.Status.Error)
		require.NotNil(t, access.Status.Error.Message)
		assert.Contains(t, *access.Status.Error.Message, tt.wantStatusErrMsg)
	}

	bucketAccessAccountDeletionTestSuite(t, env, bootstrapped, r, d, tt.wantAccountId)
}

// bucketAccessAccountDeletionTestSuite deletes the BucketAccess in whatever state the preceding
// reconciles left it. A recorded account ID must be revoked exactly once; with none recorded no
// backend access can exist, so Revoke must not be called. Deletion never grants.
func bucketAccessAccountDeletionTestSuite(t *testing.T,
	env *bucketAccessAccountIdTestEnv,
	bootstrapped *cositest.Dependencies,
	r *sidecar.BucketAccessReconciler,
	d *accountIdDriver,
	wantRevokedAccountId string,
) {
	grantsBefore := len(d.grantReqs)
	require.Empty(t, d.revokeReqs)

	access := env.getAccess(bootstrapped)
	require.NotNil(t, access)
	require.Equal(t, wantRevokedAccountId, access.Status.AccountID)
	require.NoError(t, bootstrapped.Client.Delete(bootstrapped.ContextWithLogger, access.DeepCopy()))
	require.NoError(t, env.reconcile(bootstrapped, r))

	if wantRevokedAccountId == "" {
		assert.Empty(t, d.revokeReqs)
	} else {
		require.Len(t, d.revokeReqs, 1)
		assert.Equal(t, wantRevokedAccountId, d.revokeReqs[0].AccountId)
	}
	assert.Len(t, d.grantReqs, grantsBefore, "deletion must not grant")
}

func bucketAccessAccountIdPersistenceTestSuite(t *testing.T, env *bucketAccessAccountIdTestEnv) {
	grantUnavailableErr := grpcstatus.Error(codes.Unavailable, "fake grant failure")
	okReconcile := accountIdReconcile{}

	for _, tt := range []accountIdCase{
		{
			name:              "ID is persisted before Grant",
			generateId:        "persisted-id",
			reconciles:        []accountIdReconcile{okReconcile},
			wantGenerateCalls: 1,
			wantGrantCalls:    1,
			wantAccountId:     "persisted-id",
			wantReady:         true,
		},
		{
			name:       "Grant failure keeps the ID and the retry skips Generate",
			generateId: "id-1",
			reconciles: []accountIdReconcile{
				{grantErr: grantUnavailableErr, wantErr: errRetryable, wantErrMsg: "fake grant failure"},
				okReconcile,
			},
			wantGenerateCalls: 1,
			wantGrantCalls:    2,
			wantAccountId:     "id-1",
			wantReady:         true,
		},
		{
			name:              "deleting a ready access revokes its ID",
			generateId:        "ready-id",
			reconciles:        []accountIdReconcile{okReconcile},
			wantGenerateCalls: 1,
			wantGrantCalls:    1,
			wantAccountId:     "ready-id",
			wantReady:         true,
		},
		{
			name:       "deleting a phase-1-only access revokes the generated ID",
			generateId: "phase1-id",
			reconciles: []accountIdReconcile{
				{grantErr: grantUnavailableErr, wantErr: errRetryable, wantErrMsg: "fake grant failure"},
			},
			wantGenerateCalls: 1,
			wantGrantCalls:    1,
			wantAccountId:     "phase1-id",
		},
	} {
		t.Run(tt.name, func(t *testing.T) { runAccountIdCase(t, env, tt) })
	}
}

// bucketAccessReadyWithoutAccountIdTestSuite hand-sets readyToUse=true on an access with no account
// ID, a combination this sidecar never writes, and expects a refusal before any driver call.
func bucketAccessReadyWithoutAccountIdTestSuite(t *testing.T, env *bucketAccessAccountIdTestEnv) {
	d := &accountIdDriver{generateId: "unused"}
	bootstrapped, r := env.start(t, d)

	access := env.getAccess(bootstrapped)
	access.Status.ReadyToUse = ptr.To(true)
	require.NoError(t, bootstrapped.Client.Status().Update(bootstrapped.ContextWithLogger, access))

	assertReconcileErr(t, env.reconcile(bootstrapped, r), errTerminal,
		"readyToUse is true but no account ID is assigned")

	assert.Empty(t, d.generateReqs, "DriverGenerateBucketAccessId calls")
	assert.Empty(t, d.grantReqs, "DriverGrantBucketAccess calls")

	access = env.getAccess(bootstrapped)
	assert.Empty(t, access.Status.AccountID)
	require.NotNil(t, access.Status.Error)
	require.NotNil(t, access.Status.Error.Message)
	assert.Contains(t, *access.Status.Error.Message, "readyToUse is true but no account ID is assigned")

	bucketAccessAccountDeletionTestSuite(t, env, bootstrapped, r, d, "")
}

func bucketAccessGenerateIdFailureTestSuite(t *testing.T, env *bucketAccessAccountIdTestEnv) {
	for _, tt := range []accountIdCase{
		{
			name:              "empty generated ID",
			generateId:        "",
			reconciles:        []accountIdReconcile{{wantErr: errTerminal, wantErrMsg: "missing"}},
			wantGenerateCalls: 1,
			wantStatusErrMsg:  "missing",
		},
		{
			name:              "generated ID violates pattern",
			generateId:        "has space!",
			reconciles:        []accountIdReconcile{{wantErr: errTerminal, wantErrMsg: "pattern"}},
			wantGenerateCalls: 1,
			wantStatusErrMsg:  "pattern",
		},
		{
			name:              "generated ID exceeds max length",
			generateId:        strings.Repeat("a", 2049),
			reconciles:        []accountIdReconcile{{wantErr: errTerminal, wantErrMsg: "no more than"}},
			wantGenerateCalls: 1,
			wantStatusErrMsg:  "no more than",
		},
		{
			name:              "retryable Generate error",
			generateErr:       grpcstatus.Error(codes.Unavailable, "fake generate unavailable"),
			reconciles:        []accountIdReconcile{{wantErr: errRetryable, wantErrMsg: "fake generate unavailable"}},
			wantGenerateCalls: 1,
		},
		{
			name:              "non-retryable Generate error",
			generateErr:       grpcstatus.Error(codes.InvalidArgument, "fake generate invalid"),
			reconciles:        []accountIdReconcile{{wantErr: errTerminal, wantErrMsg: "fake generate invalid"}},
			wantGenerateCalls: 1,
			wantStatusErrMsg:  "fake generate invalid",
		},
		{
			name:              "Generate OutOfRange means no multi-bucket support",
			generateErr:       grpcstatus.Error(codes.OutOfRange, "fake one bucket only"),
			reconciles:        []accountIdReconcile{{wantErr: errTerminal, wantErrMsg: "driver does not support multi-bucket access"}},
			wantGenerateCalls: 1,
			wantStatusErrMsg:  "driver does not support multi-bucket access",
		},
	} {
		t.Run(tt.name, func(t *testing.T) { runAccountIdCase(t, env, tt) })
	}
}

func bucketAccessGenerateGrantConsistencyTestSuite(t *testing.T, env *bucketAccessAccountIdTestEnv) {
	d := &accountIdDriver{generateId: "shared-id"}
	bootstrapped, r := env.start(t, d)

	require.NoError(t, env.reconcile(bootstrapped, r))

	require.Len(t, d.generateReqs, 1)
	require.Len(t, d.grantReqs, 1)
	gen, grant := d.generateReqs[0], d.grantReqs[0]
	access := env.getAccess(bootstrapped)

	assert.Equal(t, "ba-"+string(env.access.UID), gen.AccountName)
	assert.Equal(t, access.Status.AccountID, grant.AccountId)
	assert.Equal(t, "shared-id", grant.AccountId)
	assert.Equal(t, gen.Protocol.Type, grant.Protocol.Type)
	assert.Equal(t, gen.AuthenticationType.Type, grant.AuthenticationType.Type)
	assert.Equal(t, gen.ServiceAccountName, grant.ServiceAccountName)
	assert.Equal(t, gen.Parameters, grant.Parameters)
	assert.Equal(t, map[string]string{"maxSize": "100Gi", "maxIops": "10"}, gen.Parameters)
	require.Len(t, gen.Buckets, 2)
	require.Len(t, grant.Buckets, len(gen.Buckets))
	for _, b := range gen.Buckets {
		assert.True(t, accessedBucketRequestExists(grant.Buckets, b), "bucket %q missing from Grant", b.BucketId)
	}
}

func accessedBucketRequestExists(
	requestList []*cosiproto.DriverGrantBucketAccessRequest_AccessedBucket,
	want *cosiproto.DriverGrantBucketAccessRequest_AccessedBucket,
) bool {
	for _, ab := range requestList {
		modeEq := ab.GetObjectDataAccessMode().GetMode() == want.GetObjectDataAccessMode().GetMode() &&
			ab.GetObjectMetadataAccessMode().GetMode() == want.GetObjectMetadataAccessMode().GetMode() &&
			ab.GetBucketMetadataAccessMode().GetMode() == want.GetBucketMetadataAccessMode().GetMode()
		idEq := ab.BucketId == want.BucketId
		if modeEq && idEq {
			return true
		}
	}
	return false
}
