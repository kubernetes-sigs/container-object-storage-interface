/*
Copyright 2026 The Kubernetes Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	controller "sigs.k8s.io/container-object-storage-interface/controller/pkg/reconciler"
	cositest "sigs.k8s.io/container-object-storage-interface/internal/test"
	sidecartest "sigs.k8s.io/container-object-storage-interface/internal/test/sidecar"
)

var (
	// use the opinionated S3 class's driver name so we can use test utils to simulate sidecar
	// behavior tests that depend on Bucket reconciliation
	s3DriverName = cositest.OpinionatedS3BucketClass().Spec.DriverName

	// valid claim used by dynamic provisioning tests
	baseDynamicClaim = cosiapi.BucketClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-bucket",
			Namespace: "my-ns",
			UID:       "dynamicuid",
		},
		Spec: cosiapi.BucketClaimSpec{
			BucketClassName: "s3-class",
			Protocols: []cosiapi.ObjectProtocol{
				cosiapi.ObjectProtocolS3,
			},
		},
	}

	// valid class compatible with dynamic claim above
	baseBucketClass = cosiapi.BucketClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "s3-class",
		},
		Spec: cosiapi.BucketClassSpec{
			DriverName:     s3DriverName,
			DeletionPolicy: cosiapi.BucketDeletionPolicyDelete,
			Parameters: map[string]string{
				"maxSize": "100Gi",
				"maxIops": "10",
			},
		},
	}

	// valid claim used by static provisioning tests
	baseStaticClaim = cosiapi.BucketClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-bucket", // same name as dynamic so Get() can be reused between tests
			Namespace: "my-ns",
			UID:       "staticuid",
		},
		Spec: cosiapi.BucketClaimSpec{
			ExistingBucketName: "static-bucket",
			Protocols: []cosiapi.ObjectProtocol{
				cosiapi.ObjectProtocolS3,
			},
		},
	}

	// valid static bucket compatible with static claim above
	baseStaticBucket = cosiapi.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: "static-bucket",
		},
		Spec: cosiapi.BucketSpec{
			DriverName:     cositest.OpinionatedS3BucketClass().Spec.DriverName,
			DeletionPolicy: cosiapi.BucketDeletionPolicyRetain,
			BucketClaimRef: cosiapi.BucketClaimReference{
				Name:      "my-bucket",
				Namespace: "my-ns",
				UID:       "staticuid",
			},
		},
	}
)

func getClaimResources(
	bootstrapped *cositest.Dependencies,
) (
	claim *cosiapi.BucketClaim,
	dynamicBucket *cosiapi.Bucket,
	staticBucket *cosiapi.Bucket,
) {
	ctx := bootstrapped.ContextWithLogger
	client := bootstrapped.Client
	var err error

	claim = &cosiapi.BucketClaim{}
	err = client.Get(ctx, cositest.NsName(&baseDynamicClaim), claim)
	if err != nil {
		claim = nil
	}
	dynamicBucket = &cosiapi.Bucket{}
	err = client.Get(ctx, types.NamespacedName{Name: "bc-dynamicuid"}, dynamicBucket)
	if err != nil {
		dynamicBucket = nil
	}
	staticBucket = &cosiapi.Bucket{}
	err = client.Get(ctx, cositest.NsName(&baseStaticBucket), staticBucket)
	if err != nil {
		staticBucket = nil
	}

	return claim, dynamicBucket, staticBucket
}

func claimReconcilerForClient(client client.Client) *controller.BucketClaimReconciler {
	return &controller.BucketClaimReconciler{
		Client: client,
		Scheme: client.Scheme(),
	}
}

// Test dynamic provisioning, successful initialization, using base class and claim
func dynamicInitializationTest(t *testing.T) (
	bootstrappedDeps *cositest.Dependencies,
) {
	bootstrapped := cositest.MustBootstrap(t,
		baseDynamicClaim.DeepCopy(),
		baseBucketClass.DeepCopy(),
	)
	r := claimReconcilerForClient(bootstrapped.Client)
	ctx := bootstrapped.ContextWithLogger

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseDynamicClaim)})
	assert.Error(t, err) // TODO: should be NoError when Bucket watcher is set up
	assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
	assert.ErrorContains(t, err, "waiting for Bucket to be provisioned")
	assert.Empty(t, res)

	claim, bucket, _ := getClaimResources(bootstrapped)

	assert.Contains(t, claim.GetFinalizers(), cosiapi.ProtectionFinalizer)
	status := claim.Status
	assert.Equal(t, "bc-dynamicuid", status.BoundBucketName)
	assert.Equal(t, false, *status.ReadyToUse)
	assert.Empty(t, status.Protocols)
	require.NotNil(t, status.Error)
	assert.NotNil(t, status.Error.Time)
	assert.Contains(t, *status.Error.Message, "waiting for Bucket to be provisioned")

	// intermediate bucket generation is already thoroughly tested elsewhere
	// just test a couple basic fields to ensure it's integrated
	assert.NotContains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer)
	assert.Equal(t, baseBucketClass.Spec.DriverName, bucket.Spec.DriverName)
	assert.Equal(t, baseDynamicClaim.Spec.Protocols, bucket.Spec.Protocols)
	assert.Empty(t, bucket.Status)

	return bootstrapped
}

// Test static provisioning, successful initialization, using base static claim and bucket
func staticInitializationTest(
	t *testing.T,
	presetBcUid bool, // whether to "preset" the BucketClaimRef UID to that of the claim
) (
	bootstrappedDeps *cositest.Dependencies,
) {
	initBucket := baseStaticBucket.DeepCopy()
	if presetBcUid {
		initBucket.Spec.BucketClaimRef.UID = baseStaticClaim.UID
	} else {
		initBucket.Spec.BucketClaimRef.UID = ""
	}
	bootstrapped := cositest.MustBootstrap(t,
		baseStaticClaim.DeepCopy(),
		initBucket,
	)
	r := claimReconcilerForClient(bootstrapped.Client)
	ctx := bootstrapped.ContextWithLogger

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseStaticClaim)})
	assert.Error(t, err) // TODO: should be NoError when Bucket watcher is set up
	assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
	assert.ErrorContains(t, err, "waiting for Bucket to be provisioned")
	assert.Empty(t, res)

	claim, dynamicBucket, staticBucket := getClaimResources(bootstrapped)

	assert.Contains(t, claim.GetFinalizers(), cosiapi.ProtectionFinalizer)
	assert.Equal(t, "static-bucket", claim.Status.BoundBucketName)
	assert.Equal(t, false, *claim.Status.ReadyToUse)
	assert.Empty(t, claim.Status.Protocols)
	// claim status should record waiting state
	require.NotNil(t, claim.Status.Error)
	assert.Contains(t, *claim.Status.Error.Message, "waiting for Bucket to be provisioned")

	// Bucket claim ref UID must match claim UID
	// set either by controller or by admin
	assert.Equal(t, "staticuid", string(staticBucket.Spec.BucketClaimRef.UID))

	{ // bucket should be otherwise unchanged since it's statically provisioned
		initWithRefUid := initBucket.DeepCopy()
		initWithRefUid.Spec.BucketClaimRef.UID = "staticuid"
		assert.Equal(t, initWithRefUid.Finalizers, staticBucket.Finalizers)
		assert.Equal(t, initWithRefUid.Spec, staticBucket.Spec)
		assert.Equal(t, initWithRefUid.Status, staticBucket.Status)
	}

	assert.Nil(t, dynamicBucket) // dynamic bucket should not exist

	return bootstrapped
}

func TestBucketClaimReconcile(t *testing.T) {
	// A lot of things can happen after successful initialization. Test all of them, building off of
	// successful initialization with subsequent tests.
	t.Run("successful initialization", func(t *testing.T) {
		type testDef struct {
			name             string
			testInitialFunc  func(t *testing.T) *cositest.Dependencies
			getResourcesFunc func(*cositest.Dependencies) (claim *cosiapi.BucketClaim, bucket *cosiapi.Bucket)
		}
		tests := []testDef{
			{
				name:            "dynamic provisioning",
				testInitialFunc: dynamicInitializationTest,
				getResourcesFunc: func(deps *cositest.Dependencies) (*cosiapi.BucketClaim, *cosiapi.Bucket) {
					claim, bucket, _ := getClaimResources(deps)
					return claim, bucket
				},
			},
			{
				name: "static provisioning (preset UID)",
				testInitialFunc: func(t *testing.T) *cositest.Dependencies {
					return staticInitializationTest(t, true)
				},
				getResourcesFunc: func(deps *cositest.Dependencies) (*cosiapi.BucketClaim, *cosiapi.Bucket) {
					claim, _, bucket := getClaimResources(deps)
					return claim, bucket
				},
			},
			{
				name: "static provisioning (no preset UID)",
				testInitialFunc: func(t *testing.T) *cositest.Dependencies {
					return staticInitializationTest(t, false)
				},
				getResourcesFunc: func(deps *cositest.Dependencies) (*cosiapi.BucketClaim, *cosiapi.Bucket) {
					claim, _, bucket := getClaimResources(deps)
					return claim, bucket
				},
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				initBootstrapped := test.testInitialFunc(t)

				t.Run("reconcile again", func(t *testing.T) {
					bootstrapped := initBootstrapped.MustCopy() // copy prior test world state
					ctx := bootstrapped.ContextWithLogger
					r := claimReconcilerForClient(bootstrapped.Client)

					initClaim, initBucket := test.getResourcesFunc(bootstrapped)

					res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseDynamicClaim)})
					assert.Error(t, err) // TODO: should be NoError when Bucket watcher is set up
					assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
					assert.Empty(t, res)

					claim, bucket := test.getResourcesFunc(bootstrapped)

					// claim should be unchanged since bucket is not ready
					assert.Equal(t, initClaim.Finalizers, claim.Finalizers)
					assert.Equal(t, initClaim.Spec, claim.Spec)
					assert.Equal(t, initClaim.Status, claim.Status)

					assert.Equal(t, initBucket.Spec, bucket.Spec)
					assert.Equal(t, initBucket.Status, bucket.Status)

					buckets := &cosiapi.BucketList{}
					assert.NoError(t, r.List(ctx, buckets))
					assert.Len(t, buckets.Items, 1) // no other bucket should be created
				})

				t.Run("completion after Bucket ready", func(t *testing.T) {
					bootstrapped := initBootstrapped.MustCopy() // copy prior test world state
					ctx := bootstrapped.ContextWithLogger
					r := claimReconcilerForClient(bootstrapped.Client)

					initClaim, initBucket := test.getResourcesFunc(bootstrapped)

					// Reconcile Bucket using sidecar logic
					initBucket, err := sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.NsName(initBucket))
					require.NoError(t, err)

					res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseDynamicClaim)})
					assert.NoError(t, err)
					assert.Empty(t, res)

					claim, bucket := test.getResourcesFunc(bootstrapped)

					assert.Equal(t, initClaim.Finalizers, claim.Finalizers)
					assert.Equal(t, initClaim.Spec, claim.Spec)
					assert.Equal(t, initClaim.Status.BoundBucketName, claim.Status.BoundBucketName) // still bound to same bucket
					assert.True(t, *claim.Status.ReadyToUse)
					assert.Equal(t, []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3}, claim.Status.Protocols)
					assert.Nil(t, claim.Status.Error)

					assert.Equal(t, initBucket.Spec, bucket.Spec)
					assert.Equal(t, initBucket.Status, bucket.Status)

					buckets := &cosiapi.BucketList{}
					assert.NoError(t, r.List(ctx, buckets))
					assert.Len(t, buckets.Items, 1) // no other bucket should be created
				})

				t.Run("still waiting after Bucket error", func(t *testing.T) {
					bootstrapped := initBootstrapped.MustCopy() // copy prior test world state
					ctx := bootstrapped.ContextWithLogger
					r := claimReconcilerForClient(bootstrapped.Client)

					// make Bucket incompatible with Sidecar reconciler so it errors
					_, errBucket := test.getResourcesFunc(bootstrapped)
					errBucket.Spec.Protocols = []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolGcs}
					require.NoError(t, bootstrapped.Client.Update(ctx, errBucket))

					// Reconcile Bucket using sidecar logic
					_, err := sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.NsName(errBucket))
					require.Error(t, err)

					initClaim, initBucket := test.getResourcesFunc(bootstrapped)

					res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseDynamicClaim)})
					assert.Error(t, err) // TODO: should be NoError when Bucket watcher is set up
					assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
					assert.Empty(t, res)

					claim, bucket := test.getResourcesFunc(bootstrapped)

					// claim should be unchanged since bucket is not ready
					assert.Equal(t, initClaim.Finalizers, claim.Finalizers)
					assert.Equal(t, initClaim.Spec, claim.Spec)
					assert.Equal(t, initClaim.Status, claim.Status)

					assert.Equal(t, initBucket.Spec, bucket.Spec)
					assert.Equal(t, initBucket.Status, bucket.Status)

					buckets := &cosiapi.BucketList{}
					assert.NoError(t, r.List(ctx, buckets))
					assert.Len(t, buckets.Items, 1) // no other bucket should be created
				})

				t.Run("err bucket force-deleted", func(t *testing.T) {
					// note: this is also equivalent to the BucketClaim `boundBucketName` not
					// matching the previous/intended bucket.

					bootstrapped := initBootstrapped.MustCopy() // copy prior test world state
					ctx := bootstrapped.ContextWithLogger
					r := claimReconcilerForClient(bootstrapped.Client)

					initClaim, initBucket := test.getResourcesFunc(bootstrapped)
					require.NoError(t, r.Delete(ctx, initBucket)) // simulate force-delete of bucket while claim is bound

					res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseDynamicClaim)})
					assert.Error(t, err) // TODO: should be NoError when Bucket watcher is set up
					assert.ErrorIs(t, err, reconcile.TerminalError(nil))
					assert.ErrorContains(t, err, "unrecoverable degradation")
					assert.ErrorContains(t, err, "no longer exists")
					assert.Empty(t, res)

					claim, _ := test.getResourcesFunc(bootstrapped)
					assert.Contains(t, claim.GetFinalizers(), cosiapi.ProtectionFinalizer)
					assert.Equal(t, initClaim.Spec, claim.Spec)
					assert.False(t, *claim.Status.ReadyToUse)
					assert.Equal(t, initClaim.Status.BoundBucketName, claim.Status.BoundBucketName) // still bound to missing bucket
					assert.Equal(t, initClaim.Status.Protocols, claim.Status.Protocols)
					require.NotNil(t, claim.Status.Error)
					assert.Contains(t, *claim.Status.Error.Message, "unrecoverable degradation")

					buckets := &cosiapi.BucketList{}
					require.NoError(t, r.List(ctx, buckets))
					assert.Len(t, buckets.Items, 0) // no buckets should be created when claim is degraded
				})
			})
		}
	})

	t.Run("dynamic provisioning", func(t *testing.T) {
		t.Run("bucketclass does not exist", func(t *testing.T) {
			bootstrapped := cositest.MustBootstrap(t,
				baseDynamicClaim.DeepCopy(),
				// no bucketclass
			)
			r := controller.BucketClaimReconciler{
				Client: bootstrapped.Client,
				Scheme: bootstrapped.Client.Scheme(),
			}
			ctx := bootstrapped.ContextWithLogger

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseDynamicClaim)})
			assert.Error(t, err)
			assert.NotErrorIs(t, err, reconcile.TerminalError(nil)) // should be terminal error when bucketclass watcher is set up
			assert.Empty(t, res)

			claim, _, _ := getClaimResources(bootstrapped)

			assert.Contains(t, claim.GetFinalizers(), cosiapi.ProtectionFinalizer)
			status := claim.Status
			assert.Empty(t, status.BoundBucketName)
			assert.Equal(t, false, *status.ReadyToUse)
			assert.Empty(t, status.Protocols)
			serr := status.Error
			require.NotNil(t, serr)
			assert.NotNil(t, serr.Time)
			assert.NotNil(t, serr.Message)
			assert.Contains(t, *serr.Message, baseBucketClass.Name)

			bootstrapped.AssertResourceDoesNotExist(t, types.NamespacedName{Name: "bc-dynamicuid"}, &cosiapi.Bucket{})
		})
	})

	t.Run("static provisioning", func(t *testing.T) {
		t.Run("bucket does not exist", func(t *testing.T) {
			bootstrapped := cositest.MustBootstrap(t,
				baseStaticClaim.DeepCopy(),
				// no bucket
			)
			r := controller.BucketClaimReconciler{
				Client: bootstrapped.Client,
				Scheme: bootstrapped.Client.Scheme(),
			}
			ctx := bootstrapped.ContextWithLogger

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseStaticClaim)})
			assert.Error(t, err)
			assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
			assert.ErrorContains(t, err, "waiting for statically-provisioned Bucket")
			assert.ErrorContains(t, err, "static-bucket")
			assert.Empty(t, res)

			claim, _, _ := getClaimResources(bootstrapped)

			assert.Contains(t, claim.GetFinalizers(), cosiapi.ProtectionFinalizer)
			assert.Equal(t, baseStaticClaim.Spec, claim.Spec)
			require.NotNil(t, claim.Status.Error)
			assert.NotNil(t, claim.Status.Error.Time)
			assert.NotNil(t, claim.Status.Error.Message)
			assert.Contains(t, *claim.Status.Error.Message, "waiting for statically-provisioned Bucket")
			assert.Contains(t, *claim.Status.Error.Message, "static-bucket")
			assert.False(t, ptr.Deref(claim.Status.ReadyToUse, true))
			assert.Empty(t, claim.Status.BoundBucketName)
			assert.Empty(t, claim.Status.Protocols)

			buckets := &cosiapi.BucketList{}
			assert.NoError(t, r.List(ctx, buckets))
			assert.Len(t, buckets.Items, 0) // no bucket should be created
		})

		t.Run("bucket bucketClaimRef mismatch", func(t *testing.T) {
			type bucketClaimRefMismatchTest struct {
				testName      string
				bucketRef     cosiapi.BucketClaimReference
				errorContains []string
			}
			tests := []bucketClaimRefMismatchTest{
				{
					"namespace mismatch",
					cosiapi.BucketClaimReference{
						Namespace: "other-namespace",
						Name:      baseStaticClaim.Name,
						UID:       "",
					},
					[]string{"namespace", "other-namespace"},
				},
				{
					"name mismatch",
					cosiapi.BucketClaimReference{
						Namespace: baseStaticClaim.Namespace,
						Name:      "other-claim-name",
						UID:       "",
					},
					[]string{"name", "other-claim-name"},
				},
				{
					"UID mismatch", // already bound to different claim
					cosiapi.BucketClaimReference{
						Namespace: baseStaticClaim.Namespace,
						Name:      baseStaticClaim.Name,
						UID:       types.UID("other-uid"),
					},
					[]string{"Bucket claim ref UID", "other-uid"},
				},
			}
			for _, tt := range tests {
				t.Run(tt.testName, func(t *testing.T) {
					bucketNoMatch := baseStaticBucket.DeepCopy()
					bucketNoMatch.Spec.BucketClaimRef = tt.bucketRef
					bootstrapped := cositest.MustBootstrap(t,
						baseStaticClaim.DeepCopy(),
						bucketNoMatch,
					)
					r := controller.BucketClaimReconciler{
						Client: bootstrapped.Client,
						Scheme: bootstrapped.Client.Scheme(),
					}
					ctx := bootstrapped.ContextWithLogger

					res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseStaticClaim)})
					assert.Error(t, err)
					assert.ErrorIs(t, err, reconcile.TerminalError(nil))
					for _, s := range tt.errorContains {
						assert.ErrorContains(t, err, s)
					}
					assert.Empty(t, res)

					claim, dynamicBucket, staticBucket := getClaimResources(bootstrapped)

					assert.Contains(t, claim.GetFinalizers(), cosiapi.ProtectionFinalizer)
					assert.Equal(t, baseStaticClaim.Spec, claim.Spec)
					require.NotNil(t, claim.Status.Error)
					assert.NotNil(t, claim.Status.Error.Time)
					assert.NotNil(t, claim.Status.Error.Message)
					for _, s := range tt.errorContains {
						assert.Contains(t, *claim.Status.Error.Message, s)
					}
					assert.False(t, ptr.Deref(claim.Status.ReadyToUse, true))
					assert.Empty(t, claim.Status.BoundBucketName)
					assert.Empty(t, claim.Status.Protocols)

					assert.Equal(t, bucketNoMatch, staticBucket)

					assert.Nil(t, dynamicBucket) // no dynamic bucket
				})
			}
		})
	})
}
