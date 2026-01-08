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
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	cosierr "sigs.k8s.io/container-object-storage-interface/internal/errors"
)

func Test_determineBucketName(t *testing.T) {
	baseClaim := cosiapi.BucketClaim{
		ObjectMeta: meta.ObjectMeta{
			Name:      "test-bucket",
			Namespace: "user-ns",
			UID:       types.UID("qwerty"), // not realistic but good enough for tests
		},
		Spec:   cosiapi.BucketClaimSpec{},
		Status: cosiapi.BucketClaimStatus{},
	}

	t.Run("dynamic first provision", func(t *testing.T) {
		claim := baseClaim.DeepCopy()
		claim.Spec.BucketClassName = "some-class"

		n, err := determineBucketName(claim)
		assert.NoError(t, err)
		assert.Equal(t, "bc-qwerty", n)
	})

	t.Run("dynamic subsequent provision", func(t *testing.T) {
		claim := baseClaim.DeepCopy()
		claim.Spec.BucketClassName = "some-class"
		claim.Status.BoundBucketName = "bc-qwerty"

		n, err := determineBucketName(claim)
		assert.NoError(t, err)
		assert.Equal(t, "bc-qwerty", n)
	})

	t.Run("dynamic degraded provision", func(t *testing.T) {
		claim := baseClaim.DeepCopy()
		claim.Spec.BucketClassName = "some-class"
		claim.Status.BoundBucketName = "deliberately-unique"

		n, err := determineBucketName(claim)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "unrecoverable degradation")
		assert.ErrorContains(t, err, "bc-qwerty")
		assert.ErrorContains(t, err, "deliberately-unique")
		assert.Empty(t, n)
	})

	t.Run("static first provision", func(t *testing.T) {
		claim := baseClaim.DeepCopy()
		claim.Spec.ExistingBucketName = "admin-created-bucket"

		n, err := determineBucketName(claim)
		assert.NoError(t, err)
		assert.Equal(t, "admin-created-bucket", n)
	})

	t.Run("static subsequent provision", func(t *testing.T) {
		claim := baseClaim.DeepCopy()
		claim.Spec.ExistingBucketName = "admin-created-bucket"
		claim.Status.BoundBucketName = "admin-created-bucket"

		n, err := determineBucketName(claim)
		assert.NoError(t, err)
		assert.Equal(t, "admin-created-bucket", n)
	})

	t.Run("static degraded provision", func(t *testing.T) {
		claim := baseClaim.DeepCopy()
		claim.Spec.ExistingBucketName = "admin-created-bucket"
		claim.Status.BoundBucketName = "deliberately-unique"

		n, err := determineBucketName(claim)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "unrecoverable degradation")
		assert.ErrorContains(t, err, "admin-created-bucket")
		assert.ErrorContains(t, err, "deliberately-unique")
		assert.Empty(t, n)
	})
}

func Test_createIntermediateBucket(t *testing.T) {
	// valid base claim used for subtests
	baseClaim := cosiapi.BucketClaim{
		ObjectMeta: meta.ObjectMeta{
			Name:      "my-bucket",
			Namespace: "my-ns",
			UID:       "qwerty",
		},
		Spec: cosiapi.BucketClaimSpec{
			BucketClassName: "s3-class",
			Protocols: []cosiapi.ObjectProtocol{
				cosiapi.ObjectProtocolS3,
			},
		},
	}

	// valid base class used by subests
	baseClass := cosiapi.BucketClass{
		ObjectMeta: meta.ObjectMeta{
			Name: "s3-class",
		},
		Spec: cosiapi.BucketClassSpec{
			DriverName:     "cosi.s3.internal",
			DeletionPolicy: cosiapi.BucketDeletionPolicyDelete,
			Parameters: map[string]string{
				"maxSize": "100Gi",
				"maxIops": "10",
			},
		},
	}

	ctx := context.Background()
	nolog := logr.Discard()
	scheme := runtime.NewScheme()
	err := cosiapi.AddToScheme(scheme)
	require.NoError(t, err)

	// create a new test client with the given object(s) for each test
	newClient := func(withObj ...client.Object) client.Client {
		t.Helper()

		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(withObj...).Build()
		require.NotNil(t, client)

		return client
	}

	t.Run("valid claim and existing class", func(t *testing.T) {
		claim := baseClaim.DeepCopy()
		client := newClient(baseClass.DeepCopy())

		bucket, err := createIntermediateBucket(ctx, nolog, client, claim, "bc-qwerty")
		assert.NoError(t, err)

		assert.Empty(t, bucket.Finalizers) // NO finalizers pre-applied

		// from the KEP: these values are just copy-pasted from claim and class
		// any additional unit tests around them won't add much additional value
		assert.Equal(t, "cosi.s3.internal", bucket.Spec.DriverName)
		assert.Equal(t, "Delete", string(bucket.Spec.DeletionPolicy))
		assert.Len(t, bucket.Spec.Protocols, 1)
		assert.Equal(t, "S3", string(bucket.Spec.Protocols[0]))
		assert.Equal(t, map[string]string{"maxSize": "100Gi", "maxIops": "10"}, bucket.Spec.Parameters)

		claimRef := bucket.Spec.BucketClaimRef
		assert.Equal(t, "my-bucket", claimRef.Name)
		assert.Equal(t, "my-ns", claimRef.Namespace)
		assert.Equal(t, "qwerty", string(claimRef.UID))
	})

	t.Run("bucketClass does not exist", func(t *testing.T) {
		claim := baseClaim.DeepCopy()
		client := newClient() // no bucketclass exists

		bucket, err := createIntermediateBucket(ctx, nolog, client, claim, "bc-qwerty")
		assert.Error(t, err)
		assert.ErrorContains(t, err, "s3-class") // the class name
		assert.NotErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, bucket)
	})

	t.Run("claim specifies no class", func(t *testing.T) {
		claim := baseClaim.DeepCopy()
		claim.Spec.BucketClassName = ""
		client := newClient(baseClass.DeepCopy())

		bucket, err := createIntermediateBucket(ctx, nolog, client, claim, "bc-qwerty")
		assert.Error(t, err)
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, bucket)
	})

	t.Run("bucket already exists (race condition)", func(t *testing.T) {
		claim := baseClaim.DeepCopy()
		raceBucket := &cosiapi.Bucket{
			ObjectMeta: meta.ObjectMeta{
				Name: "bc-qwerty",
			},
			Spec: cosiapi.BucketSpec{},
		}
		client := newClient(baseClass.DeepCopy(), raceBucket)

		bucket, err := createIntermediateBucket(ctx, nolog, client, claim, "bc-qwerty")
		assert.Error(t, err)
		assert.NotErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, bucket)
	})
}

func TestBucketClaimReconcile(t *testing.T) {
	// valid base claim used for subtests
	baseClaim := cosiapi.BucketClaim{
		ObjectMeta: meta.ObjectMeta{
			Name:      "my-bucket",
			Namespace: "my-ns",
			UID:       "qwerty",
		},
		Spec: cosiapi.BucketClaimSpec{
			BucketClassName: "s3-class",
			Protocols: []cosiapi.ObjectProtocol{
				cosiapi.ObjectProtocolS3,
			},
		},
	}

	claimNsName := types.NamespacedName{
		Namespace: baseClaim.Namespace,
		Name:      baseClaim.Name,
	}

	// valid base class used by subtests
	baseClass := cosiapi.BucketClass{
		ObjectMeta: meta.ObjectMeta{
			Name: "s3-class",
		},
		Spec: cosiapi.BucketClassSpec{
			DriverName:     "cosi.s3.internal",
			DeletionPolicy: cosiapi.BucketDeletionPolicyDelete,
			Parameters: map[string]string{
				"maxSize": "100Gi",
				"maxIops": "10",
			},
		},
	}

	ctx := context.Background()
	nolog := logr.Discard()
	scheme := runtime.NewScheme()
	err := cosiapi.AddToScheme(scheme)
	require.NoError(t, err)

	newClient := func(withObj ...client.Object) client.Client {
		return fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(withObj...).
			WithStatusSubresource(
				&cosiapi.BucketClaim{},
				&cosiapi.Bucket{},
				&cosiapi.BucketAccess{},
			).
			Build()
	}

	dynamicHappyPathTest := func(t *testing.T, nctx context.Context) (
		*BucketClaimReconciler,
		*cosiapi.BucketClaim, *cosiapi.Bucket,
	) {
		c := newClient(baseClaim.DeepCopy(), baseClass.DeepCopy())
		r := BucketClaimReconciler{
			Client: c,
			Scheme: scheme,
		}

		res, err := r.Reconcile(nctx, ctrl.Request{NamespacedName: claimNsName})
		assert.Error(t, err) // TODO: should be NoError when Bucket watcher is set up
		assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		claim := &cosiapi.BucketClaim{}
		err = c.Get(nctx, claimNsName, claim)
		require.NoError(t, err)
		assert.Contains(t, claim.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := claim.Status
		assert.Equal(t, "bc-qwerty", status.BoundBucketName)
		assert.Equal(t, false, *status.ReadyToUse)
		assert.Empty(t, status.Protocols)
		require.NotNil(t, status.Error)
		assert.NotNil(t, status.Error.Time)
		assert.Contains(t, *status.Error.Message, "waiting for Bucket to be provisioned")

		bucket := &cosiapi.Bucket{}
		err = c.Get(nctx, types.NamespacedName{Name: "bc-qwerty"}, bucket)
		require.NoError(t, err)
		// intermediate bucket generation is already thoroughly tested elsewhere
		// just test a couple basic fields to ensure it's integrated
		assert.NotContains(t, bucket.GetFinalizers(), cosiapi.ProtectionFinalizer)
		assert.Equal(t, baseClass.Spec.DriverName, bucket.Spec.DriverName)
		assert.Equal(t, baseClaim.Spec.Protocols, bucket.Spec.Protocols)
		assert.Empty(t, bucket.Status)

		return &r, claim, bucket
	}

	t.Run("dynamic provisioning, bucket initialization", func(t *testing.T) {
		nctx := logr.NewContext(ctx, nolog)

		t.Run("initial success", func(t *testing.T) {
			dynamicHappyPathTest(t, nctx)
		})

		t.Run("subsequent reconcile no change", func(t *testing.T) {
			r, firstClaim, firstBucket := dynamicHappyPathTest(t, nctx)

			res, err := r.Reconcile(nctx, ctrl.Request{NamespacedName: claimNsName})
			assert.Error(t, err) // TODO: should be NoError when Bucket watcher is set up
			assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
			assert.Empty(t, res)

			secondClaim := &cosiapi.BucketClaim{}
			err = r.Get(nctx, claimNsName, secondClaim)
			require.NoError(t, err)
			assert.Equal(t, firstClaim.Finalizers, secondClaim.Finalizers)
			assert.Equal(t, firstClaim.Spec, secondClaim.Spec)
			assert.Equal(t, firstClaim.Status, secondClaim.Status)

			secondBucket := &cosiapi.Bucket{}
			err = r.Get(nctx, types.NamespacedName{Name: "bc-qwerty"}, secondBucket)
			require.NoError(t, err)
			assert.Equal(t, firstBucket, secondBucket)
		})
	})

	t.Run("dynamic provisioning, bucket completion", func(t *testing.T) {
		nctx := logr.NewContext(ctx, nolog)

		t.Run("bucket finished", func(t *testing.T) {
			r, firstClaim, bucket := dynamicHappyPathTest(t, nctx)

			bucket.Status.BucketID = "cosi-bc-qwerty"
			bucket.Status.ReadyToUse = ptr.To(true)
			bucket.Status.Protocols = []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3}
			err := r.Status().Update(nctx, bucket)
			require.NoError(t, err)

			res, err := r.Reconcile(nctx, ctrl.Request{NamespacedName: claimNsName})
			assert.NoError(t, err)
			assert.Empty(t, res)

			secondClaim := &cosiapi.BucketClaim{}
			err = r.Get(ctx, claimNsName, secondClaim)
			require.NoError(t, err)
			assert.Equal(t, firstClaim.Finalizers, secondClaim.Finalizers)
			assert.Equal(t, firstClaim.Spec, secondClaim.Spec)
			assert.Equal(t, firstClaim.Status.BoundBucketName, secondClaim.Status.BoundBucketName)
			assert.True(t, *secondClaim.Status.ReadyToUse)
			assert.Equal(t, bucket.Status.Protocols, secondClaim.Status.Protocols)
			assert.Nil(t, secondClaim.Status.Error)

			secondBucket := &cosiapi.Bucket{}
			err = r.Get(ctx, types.NamespacedName{Name: "bc-qwerty"}, secondBucket)
			require.NoError(t, err)
			assert.Equal(t, bucket, secondBucket)
		})

		t.Run("bucket failed", func(t *testing.T) {
			r, firstClaim, bucket := dynamicHappyPathTest(t, nctx)

			bucket.Status.ReadyToUse = ptr.To(false)
			bucket.Status.Error = cosiapi.NewTimestampedError(time.Now(), "fake error")
			err := r.Status().Update(nctx, bucket)
			require.NoError(t, err)

			res, err := r.Reconcile(nctx, ctrl.Request{NamespacedName: claimNsName})
			assert.Error(t, err) // TODO: should be NoError when Bucket watcher is set up
			assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
			assert.Empty(t, res)

			secondClaim := &cosiapi.BucketClaim{}
			err = r.Get(nctx, claimNsName, secondClaim)
			require.NoError(t, err)
			assert.Equal(t, firstClaim.Finalizers, secondClaim.Finalizers)
			assert.Equal(t, firstClaim.Spec, secondClaim.Spec)
			assert.Equal(t, firstClaim.Status, secondClaim.Status)

			secondBucket := &cosiapi.Bucket{}
			err = r.Get(nctx, types.NamespacedName{Name: "bc-qwerty"}, secondBucket)
			require.NoError(t, err)
			assert.Equal(t, bucket, secondBucket)
		})
	})

	t.Run("dynamic provisioning, no bucketclass", func(t *testing.T) {
		c := newClient(baseClaim.DeepCopy())
		r := BucketClaimReconciler{
			Client: c,
			Scheme: scheme,
		}
		nctx := logr.NewContext(ctx, nolog)

		res, err := r.Reconcile(nctx, ctrl.Request{NamespacedName: claimNsName})
		assert.Error(t, err)
		assert.NotErrorIs(t, err, reconcile.TerminalError(nil)) // should be terminal error when bucketclass watcher is set up
		assert.Empty(t, res)

		claim := &cosiapi.BucketClaim{}
		err = c.Get(ctx, claimNsName, claim)
		require.NoError(t, err)
		assert.Contains(t, claim.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := claim.Status
		assert.Equal(t, "bc-qwerty", status.BoundBucketName)
		assert.Equal(t, false, *status.ReadyToUse)
		assert.Empty(t, status.Protocols)
		serr := status.Error
		require.NotNil(t, serr)
		assert.NotNil(t, serr.Time)
		assert.NotNil(t, serr.Message)
		assert.Contains(t, *serr.Message, baseClass.Name)

		bucket := &cosiapi.Bucket{}
		err = c.Get(ctx, types.NamespacedName{Name: "bc-qwerty"}, bucket)
		assert.Error(t, err)
		assert.True(t, kerrors.IsNotFound(err))
	})

	t.Run("dynamic provisioning, boundBucketName degraded", func(t *testing.T) {
		badClaim := baseClaim.DeepCopy()
		badClaim.Status.BoundBucketName = "something-unexpected"
		c := newClient(badClaim, baseClass.DeepCopy())
		r := BucketClaimReconciler{
			Client: c,
			Scheme: scheme,
		}
		nctx := logr.NewContext(ctx, nolog)

		res, err := r.Reconcile(nctx, ctrl.Request{NamespacedName: claimNsName})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		claim := &cosiapi.BucketClaim{}
		err = c.Get(ctx, claimNsName, claim)
		require.NoError(t, err)
		assert.NotContains(t, claim.GetFinalizers(), cosiapi.ProtectionFinalizer) // no finalizer added when degraded

		bucket := &cosiapi.Bucket{}
		err = c.Get(ctx, types.NamespacedName{Name: "bc-qwerty"}, bucket)
		assert.Error(t, err)
		assert.True(t, kerrors.IsNotFound(err))
	})

	// TODO: deletion (dynamic and static, Retain/Delete)
	// TODO: static provisioning
}
