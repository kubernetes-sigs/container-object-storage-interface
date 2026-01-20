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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	"sigs.k8s.io/container-object-storage-interface/internal/bucketaccess"
	cositest "sigs.k8s.io/container-object-storage-interface/internal/test"
)

func TestBucketAccessReconcile(t *testing.T) {
	// valid base claim used for subtests
	baseAccess := cosiapi.BucketAccess{
		ObjectMeta: meta.ObjectMeta{
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

	accessNsName := types.NamespacedName{
		Namespace: baseAccess.Namespace,
		Name:      baseAccess.Name,
	}

	// valid base class used by subests
	baseClass := cosiapi.BucketAccessClass{
		ObjectMeta: meta.ObjectMeta{
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

	// first valid bucketclaim referenced by above valid access
	baseReadWriteClaim := cosiapi.BucketClaim{
		ObjectMeta: meta.ObjectMeta{
			Name:       "readwrite-bucket",
			Namespace:  "my-ns",
			UID:        "qwerty",
			Finalizers: []string{cosiapi.ProtectionFinalizer},
		},
		Spec: cosiapi.BucketClaimSpec{
			BucketClassName: "s3-class",
			Protocols: []cosiapi.ObjectProtocol{
				cosiapi.ObjectProtocolS3,
			},
		},
		Status: cosiapi.BucketClaimStatus{
			BoundBucketName: "bc-qwerty",
			Protocols:       []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3},
		},
	}

	readWriteClaimNsName := types.NamespacedName{
		Namespace: baseReadWriteClaim.Namespace,
		Name:      baseReadWriteClaim.Name,
	}

	// second valid bucketclaim referenced by above valid access
	baseReadOnlyClaim := cosiapi.BucketClaim{
		ObjectMeta: meta.ObjectMeta{
			Name:       "readonly-bucket",
			Namespace:  "my-ns",
			UID:        "asdfgh",
			Finalizers: []string{cosiapi.ProtectionFinalizer},
		},
		Spec: cosiapi.BucketClaimSpec{
			BucketClassName: "s3-class",
			Protocols: []cosiapi.ObjectProtocol{
				cosiapi.ObjectProtocolS3,
			},
		},
		Status: cosiapi.BucketClaimStatus{
			BoundBucketName: "bc-asdfgh",
			Protocols:       []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3, cosiapi.ObjectProtocolAzure},
		},
	}

	readOnlyClaimNsName := types.NamespacedName{
		Namespace: baseReadOnlyClaim.Namespace,
		Name:      baseReadOnlyClaim.Name,
	}

	t.Run("dynamic provisioning, happy path", func(t *testing.T) {
		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
		)
		r := BucketAccessReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
		}
		ctx := bootstrapped.ContextWithLogger

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: accessNsName})
		assert.NoError(t, err)
		assert.Empty(t, res)

		access := &cosiapi.BucketAccess{}
		err = r.Get(ctx, accessNsName, access)
		require.NoError(t, err)
		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		assert.Nil(t, status.Error)
		assert.Equal(t, "", status.AccountID)
		assert.Equal(t,
			[]cosiapi.AccessedBucket{
				{
					BucketName:      "bc-qwerty",
					BucketClaimName: "readwrite-bucket",
				},
				{
					BucketName:      "bc-asdfgh",
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

		crw := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readWriteClaimNsName, crw)
		require.NoError(t, err)
		assert.Contains(t, crw.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)

		cro := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readOnlyClaimNsName, cro)
		require.NoError(t, err)
		assert.Contains(t, cro.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)

		t.Log("run Reconcile() a second time to ensure nothing is modified")

		// using the same client and stuff from before
		res, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: accessNsName})
		assert.NoError(t, err)
		assert.Empty(t, res)

		secondAccess := &cosiapi.BucketAccess{}
		err = r.Get(ctx, accessNsName, secondAccess)
		require.NoError(t, err)
		assert.Equal(t, access, secondAccess)

		crw2 := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readWriteClaimNsName, crw2)
		require.NoError(t, err)
		assert.Equal(t, crw, crw2)

		cro2 := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readOnlyClaimNsName, cro2)
		require.NoError(t, err)
		assert.Equal(t, cro, cro2)
	})

	t.Run("dynamic provisioning, a bucketclaim doesn't exist", func(t *testing.T) {
		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			// readonly-bucket claim doesn't exist
		)
		r := BucketAccessReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
		}
		ctx := bootstrapped.ContextWithLogger

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: accessNsName})
		assert.Error(t, err)
		assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access := &cosiapi.BucketAccess{}
		err = r.Get(ctx, accessNsName, access)
		require.NoError(t, err)
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

		crw := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readWriteClaimNsName, crw)
		require.NoError(t, err)
		assert.Contains(t, crw.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("dynamic provisioning, 1 claim ready, 1 claim provisioning", func(t *testing.T) {
		rwc := baseReadWriteClaim.DeepCopy()
		rwc.Status = cosiapi.BucketClaimStatus{}

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseClass.DeepCopy(),
			rwc,
			baseReadOnlyClaim.DeepCopy(),
		)
		r := BucketAccessReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
		}
		ctx := bootstrapped.ContextWithLogger

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: accessNsName})
		assert.Error(t, err)
		assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access := &cosiapi.BucketAccess{}
		err = r.Get(ctx, accessNsName, access)
		require.NoError(t, err)
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

		crw := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readWriteClaimNsName, crw)
		require.NoError(t, err)
		assert.Contains(t, crw.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)

		cro := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readOnlyClaimNsName, cro)
		require.NoError(t, err)
		assert.Contains(t, cro.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("dynamic provisioning, 1 claim provisioning, 1 claim deleting", func(t *testing.T) {
		rwc := baseReadWriteClaim.DeepCopy()
		rwc.Status = cosiapi.BucketClaimStatus{}

		roc := baseReadOnlyClaim.DeepCopy()
		roc.DeletionTimestamp = &meta.Time{Time: time.Now()}

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseClass.DeepCopy(),
			rwc,
			roc,
		)
		r := BucketAccessReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
		}
		ctx := bootstrapped.ContextWithLogger

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: accessNsName})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access := &cosiapi.BucketAccess{}
		err = r.Get(ctx, accessNsName, access)
		require.NoError(t, err)
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

		crw := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readWriteClaimNsName, crw)
		require.NoError(t, err)
		assert.Contains(t, crw.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)

		// being deleted, but still needs to be marked
		cro := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readOnlyClaimNsName, cro)
		require.NoError(t, err)
		assert.Contains(t, cro.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("dynamic provisioning, 1 claim ready, 1 claim protocol unsupported", func(t *testing.T) {
		roc := baseReadOnlyClaim.DeepCopy()
		roc.Status.Protocols = []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolGcs}

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			baseClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			roc,
		)
		r := BucketAccessReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
		}
		ctx := bootstrapped.ContextWithLogger

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: accessNsName})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access := &cosiapi.BucketAccess{}
		err = r.Get(ctx, accessNsName, access)
		require.NoError(t, err)
		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		require.NotNil(t, status.Error)
		assert.NotNil(t, status.Error.Time)
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

		crw := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readWriteClaimNsName, crw)
		require.NoError(t, err)
		assert.Contains(t, crw.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)

		// being deleted, but still needs to be marked
		cro := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readOnlyClaimNsName, cro)
		require.NoError(t, err)
		assert.Contains(t, cro.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("dynamic provisioning, bucketaccessclass doesn't exist", func(t *testing.T) {
		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			// class doesn't exist
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
		)
		r := BucketAccessReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
		}
		ctx := bootstrapped.ContextWithLogger

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: accessNsName})
		assert.Error(t, err)
		assert.NotErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access := &cosiapi.BucketAccess{}
		err = r.Get(ctx, accessNsName, access)
		require.NoError(t, err)
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

		crw := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readWriteClaimNsName, crw)
		require.NoError(t, err)
		assert.Contains(t, crw.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)

		cro := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readOnlyClaimNsName, cro)
		require.NoError(t, err)
		assert.Contains(t, cro.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("dynamic provisioning, bucketaccessclass disallows multi-bucket access", func(t *testing.T) {
		class := baseClass.DeepCopy()
		class.Spec.MultiBucketAccess = cosiapi.MultiBucketAccessSingleBucket

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			class,
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
		)
		r := BucketAccessReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
		}
		ctx := bootstrapped.ContextWithLogger

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: accessNsName})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access := &cosiapi.BucketAccess{}
		err = r.Get(ctx, accessNsName, access)
		require.NoError(t, err)
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

		crw := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readWriteClaimNsName, crw)
		require.NoError(t, err)
		assert.Contains(t, crw.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)

		cro := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readOnlyClaimNsName, cro)
		require.NoError(t, err)
		assert.Contains(t, cro.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("dynamic provisioning, single-bucket passes when multi-bucket access is disallowed", func(t *testing.T) {
		access := baseAccess.DeepCopy()
		access.Spec.BucketClaims = []cosiapi.BucketClaimAccess{
			baseAccess.DeepCopy().Spec.BucketClaims[0],
		}

		class := baseClass.DeepCopy()
		class.Spec.MultiBucketAccess = cosiapi.MultiBucketAccessSingleBucket

		bootstrapped := cositest.MustBootstrap(t,
			access,
			class,
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
		)
		r := BucketAccessReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
		}
		ctx := bootstrapped.ContextWithLogger

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: accessNsName})
		assert.NoError(t, err)
		assert.Empty(t, res)

		access = &cosiapi.BucketAccess{}
		err = r.Get(ctx, accessNsName, access)
		require.NoError(t, err)
		assert.Contains(t, access.GetFinalizers(), cosiapi.ProtectionFinalizer)
		status := access.Status
		assert.False(t, *status.ReadyToUse)
		assert.Nil(t, status.Error)
		assert.Equal(t, "", status.AccountID)
		assert.Equal(t,
			[]cosiapi.AccessedBucket{
				{
					BucketName:      "bc-qwerty",
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

		crw := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readWriteClaimNsName, crw)
		require.NoError(t, err)
		assert.Contains(t, crw.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)

		cro := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readOnlyClaimNsName, cro)
		require.NoError(t, err)
		assert.NotContains(t, cro.Annotations, cosiapi.HasBucketAccessReferencesAnnotation) // not referenced
	})

	t.Run("dynamic provisioning, bucketaccessclass disallows write modes", func(t *testing.T) {
		class := baseClass.DeepCopy()
		class.Spec.DisallowedBucketAccessModes = []cosiapi.BucketAccessMode{
			cosiapi.BucketAccessModeReadWrite,
			cosiapi.BucketAccessModeWriteOnly,
		}

		bootstrapped := cositest.MustBootstrap(t,
			baseAccess.DeepCopy(),
			class,
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
		)
		r := BucketAccessReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
		}
		ctx := bootstrapped.ContextWithLogger

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: accessNsName})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access := &cosiapi.BucketAccess{}
		err = r.Get(ctx, accessNsName, access)
		require.NoError(t, err)
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

		crw := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readWriteClaimNsName, crw)
		require.NoError(t, err)
		assert.Contains(t, crw.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)

		cro := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readOnlyClaimNsName, cro)
		require.NoError(t, err)
		assert.Contains(t, cro.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})

	t.Run("duplicate BucketClaim reference", func(t *testing.T) {
		// In testing, CEL validation rules catch this, but test it here to be careful
		access := baseAccess.DeepCopy()
		access.Spec.BucketClaims = []cosiapi.BucketClaimAccess{
			baseAccess.DeepCopy().Spec.BucketClaims[0],
			baseAccess.DeepCopy().Spec.BucketClaims[0],
		}

		bootstrapped := cositest.MustBootstrap(t,
			access,
			baseClass.DeepCopy(),
			baseReadWriteClaim.DeepCopy(),
			baseReadOnlyClaim.DeepCopy(),
		)
		r := BucketAccessReconciler{
			Client: bootstrapped.Client,
			Scheme: bootstrapped.Client.Scheme(),
		}
		ctx := bootstrapped.ContextWithLogger

		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: accessNsName})
		assert.Error(t, err)
		assert.ErrorIs(t, err, reconcile.TerminalError(nil))
		assert.Empty(t, res)

		access = &cosiapi.BucketAccess{}
		err = r.Get(ctx, accessNsName, access)
		require.NoError(t, err)
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

		crw := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readWriteClaimNsName, crw)
		require.NoError(t, err)
		assert.NotContains(t, crw.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)

		cro := &cosiapi.BucketClaim{}
		err = r.Get(ctx, readOnlyClaimNsName, cro)
		require.NoError(t, err)
		assert.NotContains(t, cro.Annotations, cosiapi.HasBucketAccessReferencesAnnotation)
	})
}

func Test_validateAccessAgainstClass(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		class   *cosiapi.BucketAccessClassSpec
		access  *cosiapi.BucketAccessSpec
		wantErr bool
	}{
		{"key auth, disallow nothing",
			&cosiapi.BucketAccessClassSpec{
				AuthenticationType: cosiapi.BucketAccessAuthenticationTypeKey,
				MultiBucketAccess:  cosiapi.MultiBucketAccessMultipleBuckets,
			},
			&cosiapi.BucketAccessSpec{
				BucketClaims: []cosiapi.BucketClaimAccess{
					{
						BucketClaimName:  "rw",
						AccessMode:       cosiapi.BucketAccessModeReadWrite,
						AccessSecretName: "rw",
					},
					{
						BucketClaimName:  "ro",
						AccessMode:       cosiapi.BucketAccessModeReadOnly,
						AccessSecretName: "ro",
					},
				},
				ServiceAccountName: "",
			},
			false,
		},
		{"key auth, default disallow multi-bucket",
			&cosiapi.BucketAccessClassSpec{
				AuthenticationType: cosiapi.BucketAccessAuthenticationTypeKey,
			},
			&cosiapi.BucketAccessSpec{
				BucketClaims: []cosiapi.BucketClaimAccess{
					{
						BucketClaimName:  "rw",
						AccessMode:       cosiapi.BucketAccessModeReadWrite,
						AccessSecretName: "rw",
					},
					{
						BucketClaimName:  "ro",
						AccessMode:       cosiapi.BucketAccessModeReadOnly,
						AccessSecretName: "ro",
					},
				},
				ServiceAccountName: "",
			},
			true,
		},
		{"key auth, disallow multi-bucket",
			&cosiapi.BucketAccessClassSpec{
				AuthenticationType: cosiapi.BucketAccessAuthenticationTypeKey,
				MultiBucketAccess:  cosiapi.MultiBucketAccessSingleBucket,
			},
			&cosiapi.BucketAccessSpec{
				BucketClaims: []cosiapi.BucketClaimAccess{
					{
						BucketClaimName:  "rw",
						AccessMode:       cosiapi.BucketAccessModeReadWrite,
						AccessSecretName: "rw",
					},
					{
						BucketClaimName:  "ro",
						AccessMode:       cosiapi.BucketAccessModeReadOnly,
						AccessSecretName: "ro",
					},
				},
				ServiceAccountName: "",
			},
			true,
		},
		{"key auth, disallow write modes",
			&cosiapi.BucketAccessClassSpec{
				AuthenticationType: cosiapi.BucketAccessAuthenticationTypeKey,
				MultiBucketAccess:  cosiapi.MultiBucketAccessMultipleBuckets,
				DisallowedBucketAccessModes: []cosiapi.BucketAccessMode{
					cosiapi.BucketAccessModeReadWrite,
					cosiapi.BucketAccessModeWriteOnly,
				},
			},
			&cosiapi.BucketAccessSpec{
				BucketClaims: []cosiapi.BucketClaimAccess{
					{
						BucketClaimName:  "rw",
						AccessMode:       cosiapi.BucketAccessModeReadWrite,
						AccessSecretName: "rw",
					},
					{
						BucketClaimName:  "ro",
						AccessMode:       cosiapi.BucketAccessModeReadOnly,
						AccessSecretName: "ro",
					},
				},
				ServiceAccountName: "",
			},
			true,
		},
		{"serviceaccount auth, sa given",
			&cosiapi.BucketAccessClassSpec{
				AuthenticationType: cosiapi.BucketAccessAuthenticationTypeServiceAccount,
				MultiBucketAccess:  cosiapi.MultiBucketAccessMultipleBuckets,
			},
			&cosiapi.BucketAccessSpec{
				BucketClaims: []cosiapi.BucketClaimAccess{
					{
						BucketClaimName:  "rw",
						AccessMode:       cosiapi.BucketAccessModeReadWrite,
						AccessSecretName: "rw",
					},
					{
						BucketClaimName:  "ro",
						AccessMode:       cosiapi.BucketAccessModeReadOnly,
						AccessSecretName: "ro",
					},
				},
				ServiceAccountName: "my-sa",
			},
			false,
		},
		{"serviceaccount auth, no sa",
			&cosiapi.BucketAccessClassSpec{
				AuthenticationType: cosiapi.BucketAccessAuthenticationTypeServiceAccount,
				MultiBucketAccess:  cosiapi.MultiBucketAccessMultipleBuckets,
			},
			&cosiapi.BucketAccessSpec{
				BucketClaims: []cosiapi.BucketClaimAccess{
					{
						BucketClaimName:  "rw",
						AccessMode:       cosiapi.BucketAccessModeReadWrite,
						AccessSecretName: "rw",
					},
					{
						BucketClaimName:  "ro",
						AccessMode:       cosiapi.BucketAccessModeReadOnly,
						AccessSecretName: "ro",
					},
				},
				ServiceAccountName: "",
			},
			true,
		},
		{"serviceaccount auth, default disallow multi-bucket",
			&cosiapi.BucketAccessClassSpec{
				AuthenticationType: cosiapi.BucketAccessAuthenticationTypeServiceAccount,
			},
			&cosiapi.BucketAccessSpec{
				BucketClaims: []cosiapi.BucketClaimAccess{
					{
						BucketClaimName:  "rw",
						AccessMode:       cosiapi.BucketAccessModeReadWrite,
						AccessSecretName: "rw",
					},
					{
						BucketClaimName:  "ro",
						AccessMode:       cosiapi.BucketAccessModeReadOnly,
						AccessSecretName: "ro",
					},
				},
				ServiceAccountName: "my-sa",
			},
			true,
		},
		{"serviceaccount auth, disallow multi-bucket",
			&cosiapi.BucketAccessClassSpec{
				AuthenticationType: cosiapi.BucketAccessAuthenticationTypeServiceAccount,
				MultiBucketAccess:  cosiapi.MultiBucketAccessSingleBucket,
			},
			&cosiapi.BucketAccessSpec{
				BucketClaims: []cosiapi.BucketClaimAccess{
					{
						BucketClaimName:  "rw",
						AccessMode:       cosiapi.BucketAccessModeReadWrite,
						AccessSecretName: "rw",
					},
					{
						BucketClaimName:  "ro",
						AccessMode:       cosiapi.BucketAccessModeReadOnly,
						AccessSecretName: "ro",
					},
				},
				ServiceAccountName: "my-sa",
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := validateAccessAgainstClass(tt.class, tt.access)
			if tt.wantErr {
				assert.Error(t, gotErr)
			} else {
				assert.NoError(t, gotErr)
			}
		})
	}
}
