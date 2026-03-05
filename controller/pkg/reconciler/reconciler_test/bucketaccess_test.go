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
	"sigs.k8s.io/container-object-storage-interface/internal/bucketaccess"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	controller "sigs.k8s.io/container-object-storage-interface/controller/pkg/reconciler"
	cositest "sigs.k8s.io/container-object-storage-interface/internal/test"
	controllertest "sigs.k8s.io/container-object-storage-interface/internal/test/controller"
	sidecartest "sigs.k8s.io/container-object-storage-interface/internal/test/sidecar"
)

var (
	// valid base claim used for subtests
	baseAccess = cosiapi.BucketAccess{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-access",
			Namespace: "my-ns",
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
	baseAccessClass = cosiapi.BucketAccessClass{
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
			// DisallowedBucketAccessModes: // unset
		},
	}

	// valid bucketclaims referenced by above valid access
	baseReadWriteClaim = cositest.OpinionatedS3BucketClaim("my-ns", "readwrite-bucket")
	baseReadOnlyClaim  = cositest.OpinionatedS3BucketClaim("my-ns", "readonly-bucket")
)

func getAccessResources(bootstrapped *cositest.Dependencies) (
	*cosiapi.BucketAccess,
	*cosiapi.BucketClaim,
	*cosiapi.BucketClaim,
) {
	ctx := bootstrapped.ContextWithLogger
	client := bootstrapped.Client
	var err error

	access := &cosiapi.BucketAccess{}
	err = client.Get(ctx, cositest.NsName(&baseAccess), access)
	if err != nil {
		access = nil
	}
	rwClaim := &cosiapi.BucketClaim{}
	err = client.Get(ctx, cositest.NsName(baseReadWriteClaim), rwClaim)
	if err != nil {
		rwClaim = nil
	}
	roClaim := &cosiapi.BucketClaim{}
	err = client.Get(ctx, cositest.NsName(baseReadOnlyClaim), roClaim)
	if err != nil {
		roClaim = nil
	}

	return access, rwClaim, roClaim
}

func accessReconcilerForClient(client client.Client) *controller.BucketAccessReconciler {
	return &controller.BucketAccessReconciler{
		Client: client,
		Scheme: client.Scheme(),
	}
}

func TestBucketAccessReconcile(t *testing.T) {
	t.Run("successful initialization", func(t *testing.T) {
		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseAccessClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
		)
		r := accessReconcilerForClient(bootstrapped.Client)
		ctx := bootstrapped.ContextWithLogger

		// reconcile BucketClaims to create intermediate Buckets
		rwClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)
		roClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadOnlyClaim))
		require.NoError(t, err)

		// reconcile intermediate Buckets
		rwBucket, err := sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.BucketNsName(rwClaim))
		require.NoError(t, err)
		roBucket, err := sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.BucketNsName(roClaim))
		require.NoError(t, err)

		// reconcile BucketClaims to mirror finished status from Buckets
		rwClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)
		require.True(t, *rwClaim.Status.ReadyToUse)
		roClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadOnlyClaim))
		require.NoError(t, err)
		require.True(t, *roClaim.Status.ReadyToUse)

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.NoError(t, err)
		assert.Empty(t, res)

		access, rwClaim, roClaim := getAccessResources(bootstrapped)

		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		assert.Nil(t, status.Error)
		assert.Equal(t, "", status.AccountID)
		assert.Equal(t,
			[]cosiapi.AccessedBucket{
				{
					BucketName:      rwBucket.Name,
					BucketID:        rwBucket.Status.BucketID,
					BucketClaimName: "readwrite-bucket",
				},
				{
					BucketName:      roBucket.Name,
					BucketID:        roBucket.Status.BucketID,
					BucketClaimName: "readonly-bucket",
				},
			},
			status.AccessedBuckets,
		)
		assert.Equal(t, "cosi.s3.internal", status.DriverName)
		assert.Equal(t, "Key", string(status.AuthenticationType))
		assert.Equal(t,
			map[string]string{
				"maxSize": "100Gi",
				"maxIops": "10",
			},
			status.Parameters,
		)

		assert.True(t, bucketaccess.ManagedBySidecar(access))                       // MUST hand off to sidecar
		initialized, err := bucketaccess.SidecarRequirementsPresent(&access.Status) // MUST be fully initialized
		assert.NoError(t, err)
		assert.True(t, initialized)

		assert.Contains(t, rwClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
		assert.Contains(t, roClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)

		t.Run("reconcile again", func(t *testing.T) {
			bootstrapped := bootstrapped.MustCopy() // copy prior test world state
			ctx := bootstrapped.ContextWithLogger
			r := claimReconcilerForClient(bootstrapped.Client)

			initAccess, initRwClaim, initRoClaim := getAccessResources(bootstrapped)

			// using the same client and stuff from before
			res, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
			assert.NoError(t, err)
			assert.Empty(t, res)

			access, rwClaim, roClaim := getAccessResources(bootstrapped)

			assert.Equal(t, initAccess, access)

			assert.Equal(t, initRwClaim, rwClaim)
			assert.Equal(t, initRoClaim, roClaim)
		})
	})

	t.Run("one bucketclaim does not exist", func(t *testing.T) {
		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseAccessClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			// readonly-bucket claim doesn't exist
			cositest.OpinionatedS3BucketClass(),
		)
		r := accessReconcilerForClient(bootstrapped.Client)
		ctx := bootstrapped.ContextWithLogger

		// reconcile BucketClaims to create intermediate Buckets
		rwClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)

		// reconcile intermediate Buckets
		_, err = sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.BucketNsName(rwClaim))
		require.NoError(t, err)

		// reconcile BucketClaims to mirror finished status from Buckets
		rwClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)
		require.True(t, *rwClaim.Status.ReadyToUse)

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.Error(t, err)
		assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access, rwClaim, roClaim := getAccessResources(bootstrapped)

		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		require.NotNil(t, status.Error)
		assert.NotNil(t, status.Error.Time)
		assert.NotContains(t, *status.Error.Message, "readwrite-bucket")
		assert.Contains(t, *status.Error.Message, "readonly-bucket")
		assert.Equal(t, "", status.AccountID)
		assert.Empty(t, status.AccessedBuckets)
		assert.Empty(t, status.DriverName)
		assert.Empty(t, status.AuthenticationType)
		assert.Empty(t, status.Parameters)

		assert.False(t, bucketaccess.ManagedBySidecar(access))                      // MUST NOT hand off to sidecar
		initialized, err := bucketaccess.SidecarRequirementsPresent(&access.Status) // MUST NOT be initialized
		assert.NoError(t, err)
		assert.False(t, initialized)

		assert.Contains(t, rwClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
		assert.Nil(t, roClaim) // does not exist
	})

	t.Run("1 claim ready, 1 claim provisioning", func(t *testing.T) {
		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseAccessClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
		)
		r := accessReconcilerForClient(bootstrapped.Client)
		ctx := bootstrapped.ContextWithLogger

		// reconcile BucketClaims to create intermediate Buckets
		_, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)
		roClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadOnlyClaim))
		require.NoError(t, err)

		// reconcile only one intermediate Bucket
		// rwBucket is still provisioning
		_, err = sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.BucketNsName(roClaim))
		require.NoError(t, err)

		// reconcile BucketClaims to mirror finished status from Buckets
		roClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadOnlyClaim))
		require.NoError(t, err)
		require.True(t, *roClaim.Status.ReadyToUse)

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.Error(t, err)
		assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access, rwClaim, roClaim := getAccessResources(bootstrapped)

		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		require.NotNil(t, status.Error)
		assert.NotNil(t, status.Error.Time)
		assert.Contains(t, *status.Error.Message, "readwrite-bucket")
		assert.NotContains(t, *status.Error.Message, "readonly-bucket")
		assert.Equal(t, "", status.AccountID)
		assert.Empty(t, status.AccessedBuckets)
		assert.Empty(t, status.DriverName)
		assert.Empty(t, status.AuthenticationType)
		assert.Empty(t, status.Parameters)

		assert.False(t, bucketaccess.ManagedBySidecar(access))                      // MUST NOT hand off to sidecar
		initialized, err := bucketaccess.SidecarRequirementsPresent(&access.Status) // MUST NOT be initialized
		assert.NoError(t, err)
		assert.False(t, initialized)

		assert.Contains(t, rwClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
		assert.Contains(t, roClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("1 claim provisioning, 1 claim deleting", func(t *testing.T) {
		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseAccessClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
		)
		r := accessReconcilerForClient(bootstrapped.Client)
		ctx := bootstrapped.ContextWithLogger

		// reconcile BucketClaims to create intermediate Buckets
		_, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)
		roClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadOnlyClaim))
		require.NoError(t, err)

		// reconcile intermediate Buckets
		// rwBucket is still provisioning
		_, err = sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.BucketNsName(roClaim))
		require.NoError(t, err)

		// reconcile BucketClaims to mirror finished status from Buckets
		roClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadOnlyClaim))
		require.NoError(t, err)
		require.True(t, *roClaim.Status.ReadyToUse)

		// Fully-reconciled read-only claim begins being deleted
		require.NoError(t, bootstrapped.Client.Delete(ctx, baseReadOnlyClaim.DeepCopy()))

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access, rwClaim, roClaim := getAccessResources(bootstrapped)

		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		require.NotNil(t, status.Error)
		assert.NotNil(t, status.Error.Time)
		assert.Contains(t, *status.Error.Message,
			"data integrity for deleting BucketClaim \"readonly-bucket\" is not guaranteed")
		assert.Equal(t, "", status.AccountID)
		assert.Empty(t, status.AccessedBuckets)
		assert.Empty(t, status.DriverName)
		assert.Empty(t, status.AuthenticationType)
		assert.Empty(t, status.Parameters)

		assert.False(t, bucketaccess.ManagedBySidecar(access))                      // MUST NOT hand off to sidecar
		initialized, err := bucketaccess.SidecarRequirementsPresent(&access.Status) // MUST NOT be initialized
		assert.NoError(t, err)
		assert.False(t, initialized)

		assert.Contains(t, rwClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
		assert.Contains(t, roClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("1 claim ready, 1 claim protocol unsupported", func(t *testing.T) {
		baseGcsClaim := cositest.OpinionatedGcsBucketClaim("my-ns", "readonly-bucket")

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseAccessClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
			// GCS bucket claim and class
			baseGcsClaim,
			cositest.OpinionatedGcsBucketClass(),
		)
		r := accessReconcilerForClient(bootstrapped.Client)
		ctx := bootstrapped.ContextWithLogger

		// reconcile BucketClaims to create intermediate Buckets
		rwClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)
		gcsClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseGcsClaim))
		require.NoError(t, err)

		// reconcile intermediate Buckets
		_, err = sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.BucketNsName(rwClaim))
		require.NoError(t, err)
		_, err = sidecartest.ReconcileOpinionatedGcsBucket(t, bootstrapped, cositest.BucketNsName(gcsClaim))
		require.NoError(t, err)

		// reconcile BucketClaims to mirror finished status from Buckets
		rwClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)
		require.True(t, *rwClaim.Status.ReadyToUse)
		gcsClaim, err = controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseGcsClaim))
		require.NoError(t, err)
		require.True(t, *gcsClaim.Status.ReadyToUse)

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access, rwClaim, gcsClaim := getAccessResources(bootstrapped)

		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		require.NotNil(t, status.Error)
		assert.NotNil(t, status.Error.Time)
		assert.Contains(t, *status.Error.Message, "does not support protocol")
		assert.Contains(t, *status.Error.Message, "readonly-bucket")
		assert.Equal(t, "", status.AccountID)
		assert.Empty(t, status.AccessedBuckets)
		assert.Empty(t, status.DriverName)
		assert.Empty(t, status.AuthenticationType)
		assert.Empty(t, status.Parameters)

		assert.False(t, bucketaccess.ManagedBySidecar(access))                      // MUST NOT hand off to sidecar
		initialized, err := bucketaccess.SidecarRequirementsPresent(&access.Status) // MUST NOT be initialized
		assert.NoError(t, err)
		assert.False(t, initialized)

		assert.Contains(t, rwClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
		assert.Contains(t, gcsClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("bucketaccessclass does not exist", func(t *testing.T) {
		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			// class doesn't exist
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
		)
		r := accessReconcilerForClient(bootstrapped.Client)
		ctx := bootstrapped.ContextWithLogger

		// no need for BucketClaims to be ready

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.Error(t, err)
		assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access, rwClaim, roClaim := getAccessResources(bootstrapped)

		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		require.NotNil(t, status.Error)
		assert.NotNil(t, status.Error.Time)
		assert.Contains(t, *status.Error.Message, "s3-class")
		assert.Equal(t, "", status.AccountID)
		assert.Empty(t, status.AccessedBuckets)
		assert.Empty(t, status.DriverName)
		assert.Empty(t, status.AuthenticationType)
		assert.Empty(t, status.Parameters)

		assert.False(t, bucketaccess.ManagedBySidecar(access))                      // MUST NOT hand off to sidecar
		initialized, err := bucketaccess.SidecarRequirementsPresent(&access.Status) // MUST NOT be initialized
		assert.NoError(t, err)
		assert.False(t, initialized)

		assert.Contains(t, rwClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
		assert.Contains(t, roClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("err bucketaccessclass disallows multi-bucket access", func(t *testing.T) {
		class := baseAccessClass.DeepCopy()
		class.Spec.MultiBucketAccess = cosiapi.MultiBucketAccessSingleBucket

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			class,
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
		)
		r := accessReconcilerForClient(bootstrapped.Client)
		ctx := bootstrapped.ContextWithLogger

		// no need for BucketClaims to be ready

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access, rwClaim, roClaim := getAccessResources(bootstrapped)

		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		require.NotNil(t, status.Error)
		assert.NotNil(t, status.Error.Time)
		assert.Contains(t, *status.Error.Message, "multi-bucket access")
		assert.Equal(t, "", status.AccountID)
		assert.Empty(t, status.AccessedBuckets)
		assert.Empty(t, status.DriverName)
		assert.Empty(t, status.AuthenticationType)
		assert.Empty(t, status.Parameters)

		assert.False(t, bucketaccess.ManagedBySidecar(access))                      // MUST NOT hand off to sidecar
		initialized, err := bucketaccess.SidecarRequirementsPresent(&access.Status) // MUST NOT be initialized
		assert.NoError(t, err)
		assert.False(t, initialized)

		assert.Contains(t, rwClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
		assert.Contains(t, roClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("single-bucket succeeds when multi-bucket access is disallowed", func(t *testing.T) {
		access := baseAccess.DeepCopy()
		access.Spec.BucketClaims = []cosiapi.BucketClaimAccess{
			baseAccess.DeepCopy().Spec.BucketClaims[0],
		}

		class := baseAccessClass.DeepCopy()
		class.Spec.MultiBucketAccess = cosiapi.MultiBucketAccessSingleBucket

		bootstrapped := cositest.MustBootstrap(t,
			access,
			class,
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
		)
		r := accessReconcilerForClient(bootstrapped.Client)
		ctx := bootstrapped.ContextWithLogger

		// reconcile BucketClaims to create intermediate Buckets
		rwClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadWriteClaim))
		require.NoError(t, err)
		roClaim, err := controllertest.ReconcileBucketClaim(t, bootstrapped, cositest.NsName(baseReadOnlyClaim))
		require.NoError(t, err)

		// reconcile intermediate Buckets
		rwBucket, err := sidecartest.ReconcileOpinionatedS3Bucket(t, bootstrapped, cositest.BucketNsName(rwClaim))
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

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.NoError(t, err)
		assert.Empty(t, res)

		access, rwClaim, roClaim = getAccessResources(bootstrapped)

		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		assert.Nil(t, status.Error)
		assert.Equal(t, "", status.AccountID)
		assert.Equal(t,
			[]cosiapi.AccessedBucket{
				{
					BucketName:      rwBucket.Name,
					BucketID:        rwBucket.Status.BucketID,
					BucketClaimName: "readwrite-bucket",
				},
			},
			status.AccessedBuckets,
		)
		assert.Equal(t, "cosi.s3.internal", status.DriverName)
		assert.Equal(t, "Key", string(status.AuthenticationType))
		assert.Equal(t,
			map[string]string{
				"maxSize": "100Gi",
				"maxIops": "10",
			},
			status.Parameters,
		)

		assert.True(t, bucketaccess.ManagedBySidecar(access))                       // MUST hand off to sidecar
		initialized, err := bucketaccess.SidecarRequirementsPresent(&access.Status) // MUST be fully initialized
		assert.NoError(t, err)
		assert.True(t, initialized)

		assert.Contains(t, rwClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
		assert.NotContains(t, roClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation) // not referenced
	})

	t.Run("err bucketaccessclass disallows write modes", func(t *testing.T) {
		class := baseAccessClass.DeepCopy()
		class.Spec.DisallowedBucketAccessModes = []cosiapi.BucketAccessMode{
			cosiapi.BucketAccessModeReadWrite,
			cosiapi.BucketAccessModeWriteOnly,
		}

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			class,
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
		)
		r := accessReconcilerForClient(bootstrapped.Client)
		ctx := bootstrapped.ContextWithLogger

		// no need for BucketClaims to be ready

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access, rwClaim, roClaim := getAccessResources(bootstrapped)

		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		require.NotNil(t, status.Error)
		assert.NotNil(t, status.Error.Time)
		assert.Contains(t, *status.Error.Message, "ReadWrite")
		assert.Contains(t, *status.Error.Message, "readwrite-bucket")
		assert.Equal(t, "", status.AccountID)
		assert.Empty(t, status.AccessedBuckets)
		assert.Empty(t, status.DriverName)
		assert.Empty(t, status.AuthenticationType)
		assert.Empty(t, status.Parameters)

		assert.False(t, bucketaccess.ManagedBySidecar(access))                      // MUST NOT hand off to sidecar
		initialized, err := bucketaccess.SidecarRequirementsPresent(&access.Status) // MUST NOT be initialized
		assert.NoError(t, err)
		assert.False(t, initialized)

		assert.Contains(t, rwClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
		assert.Contains(t, roClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("err duplicate BucketClaim reference", func(t *testing.T) {
		// In testing, CEL validation rules catch this, but test it here to be careful
		access := baseAccess.DeepCopy()
		access.Spec.BucketClaims = []cosiapi.BucketClaimAccess{
			baseAccess.DeepCopy().Spec.BucketClaims[0],
			baseAccess.DeepCopy().Spec.BucketClaims[0],
		}

		bootstrapped := cositest.MustBootstrap(t,
			access,
			baseAccessClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
			cositest.OpinionatedS3BucketClass(),
		)
		r := accessReconcilerForClient(bootstrapped.Client)
		ctx := bootstrapped.ContextWithLogger

		// no need for BucketClaims to be ready

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: cositest.NsName(&baseAccess)})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access, rwClaim, roClaim := getAccessResources(bootstrapped)

		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		require.NotNil(t, status.Error)
		assert.NotNil(t, status.Error.Time)
		assert.Contains(t, *status.Error.Message, "readwrite-bucket")
		assert.Equal(t, "", status.AccountID)
		assert.Empty(t, status.AccessedBuckets)
		assert.Empty(t, status.DriverName)
		assert.Empty(t, status.AuthenticationType)
		assert.Empty(t, status.Parameters)

		assert.False(t, bucketaccess.ManagedBySidecar(access))                      // MUST NOT hand off to sidecar
		initialized, err := bucketaccess.SidecarRequirementsPresent(&access.Status) // MUST NOT be initialized
		assert.NoError(t, err)
		assert.False(t, initialized)

		assert.NotContains(t, rwClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
		assert.NotContains(t, roClaim.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})
}
