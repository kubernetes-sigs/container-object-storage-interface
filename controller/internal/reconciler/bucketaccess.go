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
	"time"

	"github.com/go-logr/logr"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	objectstoragev1alpha2 "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	"sigs.k8s.io/container-object-storage-interface/internal/handoff"
	cosipredicate "sigs.k8s.io/container-object-storage-interface/internal/predicate"
)

// BucketAccessReconciler reconciles a BucketAccess object
type BucketAccessReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=objectstorage.k8s.io,resources=bucketaccesses,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=objectstorage.k8s.io,resources=bucketaccesses/status,verbs=get;update
// +kubebuilder:rbac:groups=objectstorage.k8s.io,resources=bucketaccesses/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *BucketAccessReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	access := &cosiapi.BucketAccess{}
	if err := r.Get(ctx, req.NamespacedName, access); err != nil {
		if kerrors.IsNotFound(err) {
			logger.V(1).Info("not reconciling nonexistent BucketAccess")
			return ctrl.Result{}, nil
		}
		// no resource to add status to or report an event for
		logger.Error(err, "failed to get BucketAccess")
		return ctrl.Result{}, err
	}

	if handoff.BucketAccessManagedBySidecar(access) {
		logger.V(1).Info("not reconciling BucketAccess that should be managed by sidecar")
		return ctrl.Result{}, nil
	}

	retryError, err := r.reconcile(ctx, logger, access)
	if err != nil {
		// Because the BucketAccess status is could be managed by either Sidecar or Controller,
		// indicate that this error is coming from the Controller.
		err = fmt.Errorf("COSI Controller error: %w", err)

		// Record any error as a timestamped error in the status.
		access.Status.Error = cosiapi.NewTimestampedError(time.Now(), err.Error())
		if updErr := r.Status().Update(ctx, access); updErr != nil {
			logger.Error(err, "failed to update BucketAccess status after reconcile error", "updateError", updErr)
			// If status update fails, we must retry the error regardless of the reconcile return.
			// The reconcile needs to run again to make sure the status is eventually be updated.
			return reconcile.Result{}, err
		}

		if !retryError {
			return reconcile.Result{}, reconcile.TerminalError(err)
		}
		return reconcile.Result{}, err
	}

	// NOTE: Do not clear the error in the status on success. Success indicates 1 of 2 things:
	//   1. BucketAccess was initialized successfully, and it's now owned by the Sidecar
	//   2. BucketAccess deletion cleanup was just finished, and no status update is needed

	return reconcile.Result{}, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *BucketAccessReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&objectstoragev1alpha2.BucketAccess{}).
		Named("bucketaccess").
		WithEventFilter(
			ctrlpredicate.And(
				cosipredicate.BucketAccessManagedByController(r.Scheme), // only opt in to reconciles managed by controller
				ctrlpredicate.Or(
					// when managed by controller, we should reconcile ALL Create/Delete/Generic events
					cosipredicate.AnyCreate(),
					cosipredicate.AnyDelete(),
					cosipredicate.AnyGeneric(),
					// opt in to desired update events
					cosipredicate.BucketAccessHandoffOccurred(r.Scheme), // reconcile any handoff change
					cosipredicate.ProtectionFinalizerRemoved(r.Scheme),  // re-add protection finalizer if removed
				),
			),
		).
		Complete(r)
}

func (r *BucketAccessReconciler) reconcile(
	ctx context.Context, logger logr.Logger, access *cosiapi.BucketAccess,
) (retryErrorType, error) {
	if !access.GetDeletionTimestamp().IsZero() {
		logger.V(1).Info("beginning BucketAccess deletion cleanup")

		// TODO: deletion logic

		ctrlutil.RemoveFinalizer(access, cosiapi.ProtectionFinalizer)
		if err := r.Update(ctx, access); err != nil {
			logger.Error(err, "failed to remove finalizer")
			return RetryError, fmt.Errorf("failed to remove finalizer: %w", err)
		}

		return DoNotRetryError, fmt.Errorf("deletion is not yet implemented") // TODO
	}

	needInit, err := readyForControllerInitialization(&access.Status)
	if err != nil {
		logger.Error(err, "processed a degraded BucketAccess")
		return DoNotRetryError, fmt.Errorf("processed a degraded BucketAccess: %w", err)
	}
	if !needInit {
		// BucketAccessClass info should only be copied to the BucketAccess status once, upon
		// initial provisioning. After the info is copied, we can make no attempt to fill in any
		// missing or lost info because we don't know whether the current Class is compatible with
		// the info from the existing (old) Class info. If we reach this condition, something is
		// systemically wrong. Sidecar should have ownership, but we determined otherwise, and the
		// Sidecar will likely also determine us to be the owner.
		logger.Error(nil, "processed a BucketAccess that should be managed by COSI Sidecar")
		return DoNotRetryError, fmt.Errorf("processed a BucketAccess that should be managed by COSI Sidecar")
	}

	// TODO:
	// 1. get referenced BucketClaims
	//    a. for each, add HasBucketAccessReferencesAnnotation to the claim
	//    b. for each, get Bucket info to build corresponding accessedBuckets list
	// 2. get BucketAccessClass
	// 3. build updated status
	//    a. accessedBuckets list
	//    b. driverName, authType, parameters
	//    c. error = nil
	// 4. status().update()

	return NoError, nil
}

// Return true if the Controller needs to initialize the BucketAccess with BucketClaim and
// BucketAccessClass info. Return false if required info is set.
// Return an error if any required info is only partially set. This indicates some sort of
// degradation or bug.
func readyForControllerInitialization(s *cosiapi.BucketAccessStatus) (bool, error) {
	requiredFields := map[string]bool{}
	requiredFieldIsSet := func(fieldName string, isSet bool) {
		requiredFields[fieldName] = isSet
	}

	requiredFieldIsSet("status.accessedBuckets", len(s.AccessedBuckets) > 0)
	requiredFieldIsSet("status.driverName", s.DriverName != "")
	requiredFieldIsSet("status.authenticationType", string(s.AuthenticationType) != "")

	set := []string{}
	for field, isSet := range requiredFields {
		if isSet {
			set = append(set, field)
		}
	}

	if len(set) == 0 {
		return true, nil
	}

	if len(set) == len(requiredFields) {
		return false, nil
	}

	return false, fmt.Errorf("required Controller-managed fields are only partially set: %v", requiredFields)
}
