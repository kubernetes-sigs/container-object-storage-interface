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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	cosierr "sigs.k8s.io/container-object-storage-interface/internal/errors"
	cositest "sigs.k8s.io/container-object-storage-interface/internal/test"
)

// indexRecorderCache is a cache.Cache that only records the field indexers registered on it.
type indexRecorderCache struct {
	cache.Cache
	indexers map[string]client.IndexerFunc
}

func (c *indexRecorderCache) IndexField(
	_ context.Context, _ client.Object, field string, fn client.IndexerFunc,
) error {
	c.indexers[field] = fn
	return nil
}

// fakeReconciler returns a BucketClaimReconciler backed by a fake client holding objs.
func fakeReconciler(t *testing.T, objs ...client.Object) *BucketClaimReconciler {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, cosiapi.AddToScheme(scheme))

	// A manager can't register indexers on the fake client, and a real cache needs an API server.
	// Give the manager a cache that records the indexers instead, then add them to the fake client below.
	recorder := &indexRecorderCache{indexers: map[string]client.IndexerFunc{}}
	mgr, err := ctrl.NewManager(&rest.Config{}, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: ctrlconfig.Controller{SkipNameValidation: ptr.To(true)},
		NewCache: func(*rest.Config, cache.Options) (cache.Cache, error) {
			return recorder, nil
		},
	})
	require.NoError(t, err)

	r := &BucketClaimReconciler{Scheme: scheme}
	require.NoError(t, r.SetupWithManager(mgr))

	builder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...)
	for field, fn := range recorder.indexers {
		builder = builder.WithIndex(&cosiapi.BucketClaim{}, field, fn)
	}
	r.Client = builder.Build()
	return r
}

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

func Test_mapBucketToBucketClaim(t *testing.T) {
	t.Run("bucket references a claim", func(t *testing.T) {
		bucket := &cosiapi.Bucket{
			ObjectMeta: meta.ObjectMeta{Name: "admin-bucket"},
			Spec: cosiapi.BucketSpec{
				BucketClaimRef: cosiapi.BucketClaimReference{Name: "static-claim", Namespace: "user-ns"},
			},
		}

		requests := mapBucketToBucketClaim(context.Background(), bucket)

		assert.Equal(t, []reconcile.Request{
			{NamespacedName: types.NamespacedName{Name: "static-claim", Namespace: "user-ns"}},
		}, requests)
	})

	t.Run("bucketClaimRef name is empty", func(t *testing.T) {
		bucket := &cosiapi.Bucket{
			ObjectMeta: meta.ObjectMeta{Name: "admin-bucket"},
			Spec: cosiapi.BucketSpec{
				BucketClaimRef: cosiapi.BucketClaimReference{Namespace: "user-ns"},
			},
		}

		assert.Empty(t, mapBucketToBucketClaim(context.Background(), bucket))
	})

	t.Run("bucketClaimRef namespace is empty", func(t *testing.T) {
		bucket := &cosiapi.Bucket{
			ObjectMeta: meta.ObjectMeta{Name: "admin-bucket"},
			Spec: cosiapi.BucketSpec{
				BucketClaimRef: cosiapi.BucketClaimReference{Name: "static-claim"},
			},
		}

		assert.Empty(t, mapBucketToBucketClaim(context.Background(), bucket))
	})

	t.Run("non-Bucket object is ignored", func(t *testing.T) {
		claim := &cosiapi.BucketClaim{ObjectMeta: meta.ObjectMeta{Name: "static-claim", Namespace: "user-ns"}}

		assert.Empty(t, mapBucketToBucketClaim(context.Background(), claim))
	})
}

func Test_mapBucketClassToWaitingBucketClaims(t *testing.T) {
	newClass := &cosiapi.BucketClass{ObjectMeta: meta.ObjectMeta{Name: "s3-class"}}
	newWaitingClaim := func(namespace, name, className string) *cosiapi.BucketClaim {
		return &cosiapi.BucketClaim{
			ObjectMeta: meta.ObjectMeta{Namespace: namespace, Name: name},
			Spec:       cosiapi.BucketClaimSpec{BucketClassName: className},
		}
	}
	newReq := func(namespace, name string) reconcile.Request {
		return reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}}
	}

	t.Run("non-BucketClass object is ignored", func(t *testing.T) {
		r := fakeReconciler(t, newWaitingClaim("ns-a", "claim-1", "s3-class"))
		bucket := &cosiapi.Bucket{ObjectMeta: meta.ObjectMeta{Name: "s3-class"}}

		assert.Empty(t, r.mapBucketClassToWaitingBucketClaims(context.Background(), bucket))
	})

	t.Run("no requests when listing claims fails", func(t *testing.T) {
		scheme := runtime.NewScheme()
		assert.NoError(t, cosiapi.AddToScheme(scheme))
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithInterceptorFuncs(interceptor.Funcs{
				List: func(
					ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption,
				) error {
					return errors.New("list failed")
				},
			}).
			Build()
		r := &BucketClaimReconciler{Client: c, Scheme: scheme}

		assert.Empty(t, r.mapBucketClassToWaitingBucketClaims(context.Background(), newClass))
	})

	t.Run("statically-provisioned claims are ignored", func(t *testing.T) {
		static := newWaitingClaim("ns-a", "claim-1", "s3-class")
		static.Spec.ExistingBucketName = "admin-bucket"
		r := fakeReconciler(t, static)

		assert.Empty(t, r.mapBucketClassToWaitingBucketClaims(context.Background(), newClass))
	})

	t.Run("already-bound claims are ignored", func(t *testing.T) {
		bound := newWaitingClaim("ns-a", "claim-1", "s3-class")
		bound.Status.BoundBucketName = "bc-qwerty"
		r := fakeReconciler(t, bound)

		assert.Empty(t, r.mapBucketClassToWaitingBucketClaims(context.Background(), newClass))
	})

	t.Run("deleting claims are ignored", func(t *testing.T) {
		now := meta.Now()
		deleting := newWaitingClaim("ns-a", "claim-1", "s3-class")
		deleting.DeletionTimestamp = &now
		deleting.Finalizers = []string{cosiapi.ProtectionFinalizer}
		r := fakeReconciler(t, deleting)

		assert.Empty(t, r.mapBucketClassToWaitingBucketClaims(context.Background(), newClass))
	})

	t.Run("claims waiting on class across namespaces", func(t *testing.T) {
		r := fakeReconciler(t,
			newWaitingClaim("ns-a", "claim-1", "s3-class"),
			newWaitingClaim("ns-b", "claim-2", "s3-class"),
		)

		requests := r.mapBucketClassToWaitingBucketClaims(context.Background(), newClass)

		assert.ElementsMatch(t, []reconcile.Request{
			newReq("ns-a", "claim-1"),
			newReq("ns-b", "claim-2"),
		}, requests)
	})

	t.Run("claims for other classes are ignored", func(t *testing.T) {
		r := fakeReconciler(t,
			newWaitingClaim("ns-a", "claim-1", "s3-class"),
			newWaitingClaim("ns-a", "claim-2", "gcs-class"),
			newWaitingClaim("ns-a", "claim-3", "s3-class"),
		)

		requests := r.mapBucketClassToWaitingBucketClaims(context.Background(), newClass)

		assert.Equal(t, []reconcile.Request{newReq("ns-a", "claim-1"), newReq("ns-a", "claim-3")}, requests)
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

	t.Run("valid claim and existing class", func(t *testing.T) {
		claim := baseClaim.DeepCopy()
		bootstrapped := cositest.MustBootstrap(t,
			baseClass.DeepCopy(),
		)

		bucket, err := createIntermediateBucket(
			bootstrapped.ContextWithLogger, bootstrapped.Logger, bootstrapped.Client,
			claim, "bc-qwerty",
		)
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
		bootstrapped := cositest.MustBootstrap(t) // no bucketclass exists

		bucket, err := createIntermediateBucket(
			bootstrapped.ContextWithLogger, bootstrapped.Logger, bootstrapped.Client,
			claim, "bc-qwerty",
		)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "s3-class") // the class name
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, bucket)
	})

	t.Run("claim specifies no class", func(t *testing.T) {
		claim := baseClaim.DeepCopy()
		claim.Spec.BucketClassName = ""
		bootstrapped := cositest.MustBootstrap(t,
			baseClass.DeepCopy(),
		)

		bucket, err := createIntermediateBucket(
			bootstrapped.ContextWithLogger, bootstrapped.Logger, bootstrapped.Client,
			claim, "bc-qwerty",
		)
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
		bootstrapped := cositest.MustBootstrap(t,
			baseClass.DeepCopy(),
			raceBucket,
		)

		bucket, err := createIntermediateBucket(
			bootstrapped.ContextWithLogger, bootstrapped.Logger, bootstrapped.Client,
			claim, "bc-qwerty",
		)
		assert.Error(t, err)
		assert.NotErrorIs(t, err, cosierr.NonRetryableError(nil))
		assert.Nil(t, bucket)
	})
}
