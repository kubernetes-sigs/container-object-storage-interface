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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
					AccessMode:       cosiapi.BucketAccessModeReadWrite,
					AccessSecretName: "readwrite-bucket-creds",
				},
				{
					BucketClaimName:  "readonly-bucket",
					AccessMode:       cosiapi.BucketAccessModeReadOnly,
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
			DriverName:         "cosi.s3.internal",
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
				Name:               "cosi.s3.internal",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  proto,
			},
		}
	}

	// valid RPC response corresponding to above access and buckets
	newBaseGrantResponse := func(accountName string) *cosiproto.DriverGrantBucketAccessResponse {
		return &cosiproto.DriverGrantBucketAccessResponse{
			AccountId: "cosi-" + accountName,
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
	reconcileBucketClaimsAndAccessInitialization := func(t *testing.T, bootstrapped *cositest.Dependencies) (
		rwBucket *cosiapi.Bucket,
		roBucket *cosiapi.Bucket,
		initAccess *cosiapi.BucketAccess,
	) {
		// reconcile BucketClaims to create intermediate Buckets
		rwClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)
		roClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadOnlyClaim))
		require.NoError(t, err)

		// reconcile intermediate Buckets
		rwBucket, err = sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.BucketNsName(rwClaim))
		require.NoError(t, err)
		roBucket, err = sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.BucketNsName(roClaim))
		require.NoError(t, err)

		// reconcile BucketClaims to mirror finished status from Buckets
		rwClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)
		require.True(t, *rwClaim.Status.ReadyToUse)
		roClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadOnlyClaim))
		require.NoError(t, err)
		require.True(t, *roClaim.Status.ReadyToUse)

		// initialize the BucketAccess using the COSI Controller logic
		initAccess, err = controllertest.ReconcileBucketAccess(t, bootstrapped, cositest.NsName(&baseAccess))
		require.NoError(t, err)
		require.NotEmpty(t, initAccess.Status.AccessedBuckets)

		return rwBucket, roBucket, initAccess
	}

	t.Run("happy path", func(t *testing.T) {
		seenReq := []*cosiproto.DriverGrantBucketAccessRequest{}
		var requestError error
		fakeServer := cositest.FakeProvisionerServer{
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				seenReq = append(seenReq, dgbar)
				ret := newBaseGrantResponse(dgbar.AccountName)
				return ret, requestError
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
		)
		ctx := bootstrapped.ContextWithLogger

		rwBucket, roBucket, initAccess := reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)

		r := newReconciler(bootstrapped.Client, rpcClient)

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.NoError(t, err)
		assert.Empty(t, res)

		// ensure the expected RPC call was made
		require.Len(t, seenReq, 1)
		req := seenReq[0]
		assert.Equal(t, "ba-zxcvbn", req.AccountName)
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
			BucketId:   rwBucket.Status.BucketID,
			AccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_READ_WRITE},
		}))
		assert.True(t, accessedBucketRequestExists(req.Buckets, &cosiproto.DriverGrantBucketAccessRequest_AccessedBucket{
			BucketId:   roBucket.Status.BucketID,
			AccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_READ_ONLY},
		}))

		access := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, cositest.NsName(&baseAccess), access))
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

		// ensure secrets are present with info
		rws := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&baseAccess, 0), rws))

		ros := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&baseAccess, 1), ros))

		t.Logf("RWS: %#v\n", rws)
		t.Logf("ROS: %#v\n", ros)

		for _, s := range []*corev1.Secret{rws, ros} {
			assert.Contains(t, s.GetFinalizers(), cosiapi.ProtectionFinalizer)
			require.Len(t, s.OwnerReferences, 1)
			assert.Equal(t, "zxcvbn", string(s.OwnerReferences[0].UID))
			assert.Equal(t, "sharedaccesskey", s.StringData[string(cosiapi.CredentialVar_S3_AccessKeyId)])
		}
		assert.Equal(t, "corp-cosi-bc-qwerty", rws.StringData[string(cosiapi.BucketInfoVar_S3_BucketId)])
		assert.Equal(t, "corp-cosi-bc-asdfgh", ros.StringData[string(cosiapi.BucketInfoVar_S3_BucketId)])

		t.Log("run Reconcile() a second time to ensure nothing is modified")

		seenReq = []*cosiproto.DriverGrantBucketAccessRequest{} // empty the seen rpc requests
		requestError = nil

		res, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.NoError(t, err)
		assert.Empty(t, res)

		// same RPC call is made
		require.Len(t, seenReq, 1)
		assert.Equal(t, "ba-zxcvbn", seenReq[0].AccountName)

		// access doesn't change
		secondAccess := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, cositest.NsName(&baseAccess), secondAccess))
		assert.Equal(t, access.Finalizers, secondAccess.Finalizers)
		assert.Equal(t, access.Spec, secondAccess.Spec)
		assert.Equal(t, access.Status, secondAccess.Status)

		// secrets don't change
		secondRws := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&baseAccess, 0), secondRws))
		secondRos := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&baseAccess, 1), secondRos))
		assert.Equal(t, rws.StringData, secondRws.StringData)
		assert.Equal(t, ros.StringData, secondRos.StringData)

		t.Log("run Reconcile() that fails a third time to ensure status error")

		seenReq = []*cosiproto.DriverGrantBucketAccessRequest{} // empty the seen rpc requests
		requestError = fmt.Errorf("fake rpc error")

		res, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.Error(t, err)
		assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		// same RPC call is made
		require.Len(t, seenReq, 1)
		assert.Equal(t, "ba-zxcvbn", seenReq[0].AccountName)

		// access has error but otherwise doesn't change
		thirdAccess := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, cositest.NsName(&baseAccess), thirdAccess))
		assert.Equal(t, access.Finalizers, thirdAccess.Finalizers)
		assert.Equal(t, access.Spec, thirdAccess.Spec)
		require.NotNil(t, thirdAccess.Status.Error)
		assert.NotNil(t, thirdAccess.Status.Error.Time)
		assert.NotNil(t, thirdAccess.Status.Error.Message)
		assert.Contains(t, *thirdAccess.Status.Error.Message, "fake rpc error")
		{ // non-error fields stay the same
			thirdNoError := thirdAccess.DeepCopy()
			thirdNoError.Status.Error = nil
			assert.Equal(t, access.Status, thirdNoError.Status)
		}

		// secrets don't change
		thirdRws := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&baseAccess, 0), thirdRws))
		thirdRos := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&baseAccess, 1), thirdRos))
		assert.Equal(t, rws.StringData, thirdRws.StringData)
		assert.Equal(t, ros.StringData, thirdRos.StringData)

		t.Log("run Reconcile() that passes a fourth time with rotated creds")

		fakeServer.GrantBucketAccessFunc = func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
			// RPC checking here would be redundant
			ret := newBaseGrantResponse(dgbar.AccountName)
			ret.Credentials.S3.AccessKeyId = "rotatedsharedaccesskey"
			return ret, nil
		}

		res, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.NoError(t, err)
		assert.Empty(t, res)

		fourthAccess := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, cositest.NsName(&baseAccess), fourthAccess))
		assert.Equal(t, access.Finalizers, fourthAccess.Finalizers)
		assert.Equal(t, access.Spec, fourthAccess.Spec)
		assert.Equal(t, access.Status, fourthAccess.Status) // error is cleared

		// secrets change their access key ID only
		fourthRws := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&baseAccess, 0), fourthRws))
		fourthRos := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&baseAccess, 1), fourthRos))
		assert.Equal(t, "rotatedsharedaccesskey", fourthRws.StringData[string(cosiapi.CredentialVar_S3_AccessKeyId)])
		assert.Equal(t, "rotatedsharedaccesskey", fourthRos.StringData[string(cosiapi.CredentialVar_S3_AccessKeyId)])
		{ // other secret data stays the same
			rwsCopy := fourthRws.DeepCopy()
			rosCopy := fourthRos.DeepCopy()
			rwsCopy.StringData[string(cosiapi.CredentialVar_S3_AccessKeyId)] = "sharedaccesskey"
			rosCopy.StringData[string(cosiapi.CredentialVar_S3_AccessKeyId)] = "sharedaccesskey"
			assert.Equal(t, rws.StringData, rwsCopy.StringData)
			assert.Equal(t, ros.StringData, rosCopy.StringData)
		}
	})

	t.Run("secret already exists with incompatible owner", func(t *testing.T) {
		testIncompatibleOwner := func(t *testing.T, owner *metav1.OwnerReference) {
			fakeServer := cositest.FakeProvisionerServer{
				GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
					panic("should not be called")
				},
			}

			cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
			defer cleanup()
			require.NoError(t, err)
			go serve()

			conn, err := cositest.RpcClientConn(tmpSock)
			require.NoError(t, err)
			rpcClient := cosiproto.NewProvisionerClient(conn)

			preExistingSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "readwrite-bucket-creds",
					Namespace: baseAccess.Namespace,
				},
				StringData: map[string]string{
					"PRE_EXISTING_DATA": "important_thing",
				},
			}
			if owner != nil {
				preExistingSecret.OwnerReferences = []metav1.OwnerReference{*owner}
			}

			bootstrapped := cositest.MustBootstrap(t,
				baseAccess.DeepCopy(),
				baseClass.DeepCopy(),
				baseReadWriteClaim.DeepCopy(),
				baseReadOnlyClaim.DeepCopy(),
				cositest.OpinionatedS3BucketClass(),
				preExistingSecret,
			)
			ctx := bootstrapped.ContextWithLogger

			_, _, initAccess := reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)

			r := newReconciler(bootstrapped.Client, rpcClient)

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.Error(t, err)
			assert.ErrorIs(t, err, reconcile.TerminalError(nil))
			assert.Empty(t, res)

			access := &cosiapi.BucketAccess{}
			require.NoError(t, r.Get(ctx, cositest.NsName(&baseAccess), access))
			assert.Equal(t, initAccess.Finalizers, access.Finalizers)
			assert.Equal(t, baseAccess.Spec, access.Spec)
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
			rws := &corev1.Secret{}
			require.NoError(t, r.Get(ctx, cositest.SecretNsName(&baseAccess, 0), rws))
			assert.Equal(t, preExistingSecret, rws)

			// other secret has been reserved successfully
			ros := &corev1.Secret{}
			require.NoError(t, r.Get(ctx, cositest.SecretNsName(&baseAccess, 1), ros))
			assert.Contains(t, ros.GetFinalizers(), cosiapi.ProtectionFinalizer)
			require.Len(t, ros.OwnerReferences, 1)
			assert.Equal(t, "zxcvbn", string(ros.OwnerReferences[0].UID))
			assert.Len(t, ros.StringData, 0)
		}

		t.Run("no owners", func(t *testing.T) {
			testIncompatibleOwner(t, nil)
		})

		t.Run("different controller-owner", func(t *testing.T) {
			o := &metav1.OwnerReference{
				APIVersion:         "other.controller.io/v1",
				Kind:               "gvk.Kind",
				Name:               "other-owner",
				UID:                "aaaaaa",
				BlockOwnerDeletion: ptr.To(true),
				Controller:         ptr.To(true),
			}
			testIncompatibleOwner(t, o)
		})
	})

	t.Run("repeated secret name in spec.bucketClaims", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				panic("should not be called")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		accessWithRepeatedSecret := baseAccess.DeepCopy()
		accessWithRepeatedSecret.Spec.BucketClaims[0].AccessSecretName = cositest.SecretNsName(&baseAccess, 0).Name
		accessWithRepeatedSecret.Spec.BucketClaims[1].AccessSecretName = cositest.SecretNsName(&baseAccess, 0).Name

		bootstrapped := cositest.MustBootstrap(t,
			accessWithRepeatedSecret,
			baseClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
		)
		ctx := bootstrapped.ContextWithLogger

		_, _, initAccess := reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)

		r := newReconciler(bootstrapped.Client, rpcClient)

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, cositest.NsName(&baseAccess), access))
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
		rws := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&baseAccess, 0), rws))
		assert.Contains(t, rws.GetFinalizers(), cosiapi.ProtectionFinalizer)
		require.Len(t, rws.OwnerReferences, 1)
		assert.Equal(t, "zxcvbn", string(rws.OwnerReferences[0].UID))
		assert.Len(t, rws.StringData, 0)
	})

	t.Run("status.accessedBuckets doesn't match spec.bucketClaims", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				panic("should not be called")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
		)
		ctx := bootstrapped.ContextWithLogger

		_, _, initAccess := reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)

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

		access := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, cositest.NsName(&baseAccess), access))
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

		// don't care if secrets exist or not
	})

	t.Run("a bucket has deleting annotation", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				panic("should not be called")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
		)
		ctx := bootstrapped.ContextWithLogger

		rwBucket, _, initAccess := reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)

		// BucketClaim controller doesn't know how to do deletion to Buckets yet, so simulate Bucket deletion annotation here
		deletingBucket := rwBucket.DeepCopy()
		deletingBucket.Annotations = map[string]string{
			cosiapi.BucketClaimBeingDeletedAnnotation: "",
		}
		require.NoError(t, bootstrapped.Client.Update(ctx, deletingBucket))

		r := newReconciler(bootstrapped.Client, rpcClient)

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, cositest.NsName(&baseAccess), access))
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

		// don't care if secrets exist or not
	})

	t.Run("a bucket does not exist", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				panic("should not be called")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
		)
		ctx := bootstrapped.ContextWithLogger

		_, roBucket, initAccess := reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)

		// delete one Bucket to simulate non-existence
		deletedBucket := roBucket.DeepCopy()
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

		access := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, cositest.NsName(&baseAccess), access))
		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		assert.Equal(t, baseAccess.Spec, access.Spec)
		require.NotNil(t, access.Status.Error)
		assert.NotNil(t, access.Status.Error.Time)
		assert.NotNil(t, access.Status.Error.Message)
		assert.Contains(t, *access.Status.Error.Message, roBucket.Name)
		{ // non-error fields stay the same
			accessNoError := access.DeepCopy()
			accessNoError.Status.Error = nil
			assert.Equal(t, initAccess.Status, accessNoError.Status)
		}

		// don't care if secrets exist or not
	})

	t.Run("driver name mismatch", func(t *testing.T) {
		fakeServer := cositest.FakeProvisionerServer{
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				panic("should not be called")
			},
		}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)
		rpcClient := cosiproto.NewProvisionerClient(conn)

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
		)
		ctx := bootstrapped.ContextWithLogger

		_, _, initAccess := reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)

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

		access := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, cositest.NsName(&baseAccess), access))
		assert.NotContains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer) // should skip reconcile
		assert.Equal(t, baseAccess.Spec, access.Spec)
		assert.Equal(t, initAccess.Status, access.Status)

		// don't care if secrets exist or not
	})

	t.Run("rpc return mistakes", func(t *testing.T) {

		type rpcReturnMistakeTest struct {
			testName  string
			rpcReturn *cosiproto.DriverGrantBucketAccessResponse
		}
		tests := []rpcReturnMistakeTest{
			{
				"account id missing",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.AccountId = ""
					return ret
				}(),
			},
			{
				"credentials nil",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Credentials = nil
					return ret
				}(),
			},
			{
				"credentials expected proto nil",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Credentials.S3 = nil
					return ret
				}(),
			},
			{
				"credentials add wrong proto",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Credentials.Gcs = &cosiproto.GcsCredentialInfo{}
					return ret
				}(),
			},
			{
				"credentials invalid",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Credentials.S3.AccessKeyId = "" // S3 requires access key ID for Key auth
					return ret
				}(),
			},
			{
				"bucket info response nil",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Buckets = nil
					return ret
				}(),
			},
			{
				"bucket info response empty",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Buckets = []*cosiproto.DriverGrantBucketAccessResponse_BucketInfo{}
					return ret
				}(),
			},
			{
				"a bucket info response is missing",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Buckets = ret.Buckets[0:1]
					return ret
				}(),
			},
			{
				"extra bucket info response",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
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
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Buckets[0].BucketId = ""
					return ret
				}(),
			},
			{
				"a bucket info nil",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Buckets[0].BucketInfo = nil
					return ret
				}(),
			},
			{
				"a bucket info adds wrong proto",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Buckets[0].BucketInfo.Azure = &cosiproto.AzureBucketInfo{}
					return ret
				}(),
			},
			{
				"a bucket info expected proto nil",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Buckets[0].BucketInfo.S3 = nil
					return ret
				}(),
			},
			{
				"a bucket info proto invalid",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Buckets[0].BucketInfo.S3.Endpoint = "" // S3 requires endpoint to be set
					return ret
				}(),
			},
			{
				"a bucket info unknown bucket id",
				func() *cosiproto.DriverGrantBucketAccessResponse {
					ret := newBaseGrantResponse("ba-" + string(baseAccess.UID))
					ret.Buckets[0].BucketId = "something-random"
					return ret
				}(),
			},
		}

		var requestReturn *cosiproto.DriverGrantBucketAccessResponse
		fakeServer := cositest.FakeProvisionerServer{
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				return requestReturn, nil
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
			t.Run(tt.testName, func(t *testing.T) {
				requestReturn = tt.rpcReturn

				bootstrapped := cositest.MustBootstrap(t,
					baseAccess.DeepCopy(),
					baseClass.DeepCopy(),
					baseReadWriteClaim.DeepCopy(),
					baseReadOnlyClaim.DeepCopy(),
					cositest.OpinionatedS3BucketClass(),
				)
				ctx := bootstrapped.ContextWithLogger

				_, _, initAccess := reconcileBucketClaimsAndAccessInitialization(t, bootstrapped)

				r := newReconciler(bootstrapped.Client, rpcClient)

				res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
				assert.Error(t, err)
				assert.ErrorIs(t, err, reconcile.TerminalError(nil))
				assert.Empty(t, res)

				access := &cosiapi.BucketAccess{}
				require.NoError(t, r.Get(ctx, cositest.NsName(&baseAccess), access))
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
					assert.Equal(t, initAccess.Status, accessNoError.Status)
				}

				// secrets should have been created to claim them, but not updated with data
				rws := &corev1.Secret{}
				require.NoError(t, r.Get(ctx, cositest.SecretNsName(initAccess, 0), rws))
				ros := &corev1.Secret{}
				require.NoError(t, r.Get(ctx, cositest.SecretNsName(initAccess, 1), ros))
				for _, s := range []*corev1.Secret{rws, ros} {
					assert.Contains(t, s.GetFinalizers(), cosiapi.ProtectionFinalizer)
					require.Len(t, s.OwnerReferences, 1)
					assert.Len(t, s.StringData, 0)
				}
			})
		}
	})

	t.Run("azure protocol and serviceaccount auth", func(t *testing.T) {
		seenReq := []*cosiproto.DriverGrantBucketAccessRequest{}
		var requestError error
		fakeServer := cositest.FakeProvisionerServer{
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				seenReq = append(seenReq, dgbar)
				ret := &cosiproto.DriverGrantBucketAccessResponse{
					AccountId: "cosi-" + dgbar.AccountName,
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
				return ret, requestError
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

		bootstrapped := cositest.MustBootstrap(t,
			azureAccess.DeepCopy(),
			azureClass.DeepCopy(),
			azureRwClaim.DeepCopy(),
			azureRoClaim.DeepCopy(),
			cositest.OpinionatedAzureBucketClass(),
		)
		ctx := bootstrapped.ContextWithLogger

		// reconcile BucketClaims to create intermediate Buckets
		rwClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(azureRwClaim))
		require.NoError(t, err)
		roClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(azureRoClaim))
		require.NoError(t, err)

		// reconcile intermediate Buckets
		rwBucket, err := sidecartest.ReconcileOpinionatedAzureBucket(t, bootstrapped, cositest.BucketNsName(rwClaim))
		require.NoError(t, err)
		roBucket, err := sidecartest.ReconcileOpinionatedAzureBucket(t, bootstrapped, cositest.BucketNsName(roClaim))
		require.NoError(t, err)

		// reconcile BucketClaims to mirror finished status from Buckets
		rwClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(azureRwClaim))
		require.NoError(t, err)
		require.True(t, *rwClaim.Status.ReadyToUse)
		roClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(azureRoClaim))
		require.NoError(t, err)
		require.True(t, *roClaim.Status.ReadyToUse)

		// initialize the BucketAccess using the COSI Controller logic
		initAccess, err := controllertest.ReconcileBucketAccess(t, bootstrapped, cositest.NsName(&azureAccess))
		require.NoError(t, err)
		require.NotEmpty(t, initAccess.Status.AccessedBuckets)

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
		require.Len(t, seenReq, 1)
		req := seenReq[0]
		assert.Equal(t, "ba-zxcvbn", req.AccountName)
		assert.Equal(t, cosiproto.AuthenticationType_SERVICE_ACCOUNT, req.AuthenticationType.Type)
		assert.Equal(t, cosiproto.ObjectProtocol_AZURE, req.Protocol.Type)
		assert.Equal(t, "azure-sa", req.ServiceAccountName)
		assert.Empty(t, req.Parameters)
		require.Len(t, req.Buckets, 2) // by RPC spec, order of requested accessed buckets is random
		assert.True(t, accessedBucketRequestExists(req.Buckets, &cosiproto.DriverGrantBucketAccessRequest_AccessedBucket{
			BucketId:   rwBucket.Status.BucketID,
			AccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_READ_WRITE},
		}))
		assert.True(t, accessedBucketRequestExists(req.Buckets, &cosiproto.DriverGrantBucketAccessRequest_AccessedBucket{
			BucketId:   roBucket.Status.BucketID,
			AccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_READ_ONLY},
		}))

		access := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, cositest.NsName(&azureAccess), access))
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
		rws := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&azureAccess, 0), rws))

		ros := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&azureAccess, 1), ros))

		for _, s := range []*corev1.Secret{rws, ros} {
			assert.Contains(t, s.GetFinalizers(), cosiapi.ProtectionFinalizer)
			require.Len(t, s.OwnerReferences, 1)
			assert.Equal(t, "zxcvbn", string(s.OwnerReferences[0].UID))
		}
		assert.Equal(t, "outputaccount", rws.StringData[string(cosiapi.BucketInfoVar_Azure_StorageAccount)])
		assert.Equal(t, "inputaccount", ros.StringData[string(cosiapi.BucketInfoVar_Azure_StorageAccount)])
	})

	t.Run("GCS protocol", func(t *testing.T) {
		seenReq := []*cosiproto.DriverGrantBucketAccessRequest{}
		var requestError error
		fakeServer := cositest.FakeProvisionerServer{
			GrantBucketAccessFunc: func(ctx context.Context, dgbar *cosiproto.DriverGrantBucketAccessRequest) (*cosiproto.DriverGrantBucketAccessResponse, error) {
				seenReq = append(seenReq, dgbar)
				ret := &cosiproto.DriverGrantBucketAccessResponse{
					AccountId: "cosi-" + dgbar.AccountName,
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
				return ret, requestError
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

		bootstrapped := cositest.MustBootstrap(t,
			gcsAccess.DeepCopy(),
			gcsClass.DeepCopy(),
			gcsRwClaim.DeepCopy(),
			gcsRoClaim.DeepCopy(),
			cositest.OpinionatedGcsBucketClass(),
		)
		ctx := bootstrapped.ContextWithLogger

		// reconcile BucketClaims to create intermediate Buckets
		rwClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(gcsRwClaim))
		require.NoError(t, err)
		roClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(gcsRoClaim))
		require.NoError(t, err)

		// reconcile intermediate Buckets
		rwBucket, err := sidecartest.ReconcileOpinionatedGcsBucket(t, bootstrapped, cositest.BucketNsName(rwClaim))
		require.NoError(t, err)
		roBucket, err := sidecartest.ReconcileOpinionatedGcsBucket(t, bootstrapped, cositest.BucketNsName(roClaim))
		require.NoError(t, err)

		// reconcile BucketClaims to mirror finished status from Buckets
		rwClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(gcsRwClaim))
		require.NoError(t, err)
		require.True(t, *rwClaim.Status.ReadyToUse)
		roClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(gcsRoClaim))
		require.NoError(t, err)
		require.True(t, *roClaim.Status.ReadyToUse)

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

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&gcsAccess)})
		assert.NoError(t, err)
		assert.Empty(t, res)

		// ensure the expected RPC call was made
		require.Len(t, seenReq, 1)
		req := seenReq[0]
		assert.Equal(t, "ba-zxcvbn", req.AccountName)
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
			BucketId:   rwBucket.Status.BucketID,
			AccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_READ_WRITE},
		}))
		assert.True(t, accessedBucketRequestExists(req.Buckets, &cosiproto.DriverGrantBucketAccessRequest_AccessedBucket{
			BucketId:   roBucket.Status.BucketID,
			AccessMode: &cosiproto.AccessMode{Mode: cosiproto.AccessMode_READ_ONLY},
		}))

		access := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, cositest.NsName(&gcsAccess), access))
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
		rws := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&gcsAccess, 0), rws))

		ros := &corev1.Secret{}
		require.NoError(t, r.Get(ctx, cositest.SecretNsName(&gcsAccess, 1), ros))

		for _, s := range []*corev1.Secret{rws, ros} {
			assert.Contains(t, s.GetFinalizers(), cosiapi.ProtectionFinalizer)
			require.Len(t, s.OwnerReferences, 1)
			assert.Equal(t, "zxcvbn", string(s.OwnerReferences[0].UID))
			assert.Equal(t, "accessid", s.StringData[string(cosiapi.CredentialVar_GCS_AccessId)])
		}
		assert.Equal(t, "corp-cosi-bc-qwerty", rws.StringData[string(cosiapi.BucketInfoVar_GCS_BucketName)])
		assert.Equal(t, "corp-cosi-bc-asdfgh", ros.StringData[string(cosiapi.BucketInfoVar_GCS_BucketName)])
	})
}

func accessedBucketRequestExists(
	requestList []*cosiproto.DriverGrantBucketAccessRequest_AccessedBucket,
	want *cosiproto.DriverGrantBucketAccessRequest_AccessedBucket,
) bool {
	for _, ab := range requestList {
		modeEq := ab.AccessMode.Mode == want.AccessMode.Mode
		idEq := ab.BucketId == want.BucketId
		if modeEq && idEq {
			return true
		}
	}
	return false
}
