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
	"fmt"
	"slices"
	"time"

	"github.com/go-logr/logr"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	objectstoragev1alpha2 "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	"sigs.k8s.io/container-object-storage-interface/internal/bucketaccess"
	cosierr "sigs.k8s.io/container-object-storage-interface/internal/errors"
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

	if bucketaccess.ManagedBySidecar(access) {
		logger.V(1).Info("not reconciling BucketAccess that should be managed by sidecar")
		return ctrl.Result{}, nil
	}

	err := r.reconcile(ctx, logger, access)
	if err != nil {
		// Because the BucketAccess status is could be managed by either Sidecar or Controller,
		// indicate that this error is coming from the Controller.
		err = fmt.Errorf("COSI Controller error: %w", err)

		// Record any error as a timestamped error in the status.
		if access.Status.ReadyToUse == nil {
			access.Status.ReadyToUse = ptr.To(false)
		}
		access.Status.Error = cosiapi.NewTimestampedError(time.Now(), err.Error())
		if updErr := r.Status().Update(ctx, access); updErr != nil {
			logger.Error(err, "failed to update BucketAccess status after reconcile error", "updateError", updErr)
			// If status update fails, we must retry the error regardless of the reconcile return.
			// The reconcile needs to run again to make sure the status is eventually updated.
			return reconcile.Result{}, err
		}

		if errors.Is(err, cosierr.NonRetryableError(nil)) {
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
) error {
	if !access.GetDeletionTimestamp().IsZero() {
		logger.V(1).Info("beginning BucketAccess deletion cleanup")

		// TODO: deletion logic

		ctrlutil.RemoveFinalizer(access, cosiapi.ProtectionFinalizer)
		if err := r.Update(ctx, access); err != nil {
			logger.Error(err, "failed to remove finalizer")
			return fmt.Errorf("failed to remove finalizer: %w", err)
		}

		return cosierr.NonRetryableError(fmt.Errorf("deletion is not yet implemented")) // TODO
	}

	initialized, err := bucketaccess.SidecarRequirementsPresent(&access.Status)
	if err != nil {
		logger.Error(err, "processed a degraded BucketAccess")
		return cosierr.NonRetryableError(fmt.Errorf("processed a degraded BucketAccess: %w", err))
	}
	if initialized {
		// BucketAccessClass info should only be copied to the BucketAccess status once, upon
		// initial provisioning. After the info is copied, make no attempt to fill in any missing or
		// lost info because we don't know whether the current Class is compatible with the info
		// from the existing (old) Class info. If we reach this condition, something is systemically
		// wrong. Sidecar should have ownership, but we determined otherwise, and the Sidecar will
		// likely also determine us to be the owner.
		logger.Error(nil, "processed a BucketAccess that should be managed by COSI Sidecar")
		return cosierr.NonRetryableError(fmt.Errorf("processed a BucketAccess that should be managed by COSI Sidecar"))
	}

	logger = logger.WithValues("bucketAccessClassName", access.Spec.BucketAccessClassName)

	logger.V(1).Info("initializing BucketAccess")

	didAdd := ctrlutil.AddFinalizer(access, cosiapi.ProtectionFinalizer)
	if didAdd {
		if err := r.Update(ctx, access); err != nil {
			logger.Error(err, "failed to add protection finalizer")
			return fmt.Errorf("failed to add protection finalizer: %w", err)
		}
	}

	claimsByName, err := getAllBucketClaims(ctx, r.Client, access.Namespace, access.Spec.BucketClaims)
	if err != nil {
		logger.Error(err, "failed to get all referenced BucketClaims")
		return err
	}

	// Mark as many referenced BucketClaims as possible as soon as possible in the reconcile.
	// This ensures that BucketClaims are marked to protect their data from deletion quickly.
	if err := markAllBucketClaimsAsAccessed(ctx, r.Client, claimsByName); err != nil {
		logger.Error(err, "failed to mark all referenced BucketClaims")
		return err
	}

	class := &cosiapi.BucketAccessClass{}
	classNsName := types.NamespacedName{
		Name:      access.Spec.BucketAccessClassName,
		Namespace: "", // global resource
	}
	if err := r.Get(ctx, classNsName, class); err != nil {
		if kerrors.IsNotFound(err) {
			// TODO: for now, return an error and allow the controller to exponential backoff
			// until the access class exists. in the future, optimize this by adding a
			// access class reconciler that enqueues requests for BucketAccesses that reference the
			// class and aren't yet passed to the sidecar.
			logger.Error(err, "BucketAccessClass not found")
			return err
		}
		logger.Error(err, "failed to get BucketAccessClass")
		return err
	}

	if err := validateAccessAgainstClass(&class.Spec, &access.Spec); err != nil {
		logger.Error(err, "BucketAccess failed featureOptions validation")
		return cosierr.NonRetryableError(err)
	}

	blockers := cannotAccessBucketClaims(claimsByName, access.Spec)
	if len(blockers) > 0 {
		logger.Error(nil, "access cannot be provisioned for one or more BucketClaims", "blockers", blockers)
		return cosierr.NonRetryableError(fmt.Errorf("access cannot be provisioned for one or more BucketClaims: %v", blockers))
	}

	waitlist := waitingOnBucketClaims(claimsByName)
	if len(waitlist) > 0 {
		logger.Error(nil, "waiting for prerequisites before provisioning access", "waitlist", waitlist)
		// TODO: for now, return an error and allow the controller to exponential backoff until we
		// are done waiting on the resources. in the future, optimize this by adding a bucketclaim
		// reconciler that enqueues requests for BucketClaims when they finish provisioning.
		return fmt.Errorf("waiting for prerequisites before provisioning access: %v", waitlist)
	}

	accessedBuckets, err := generateAccessedBuckets(access.Spec.BucketClaims, claimsByName)
	if err != nil {
		logger.Error(err, "waiting for BucketClaims to finish provisioning")
		return fmt.Errorf("waiting for BucketClaims to finish provisioning: %w", err)
	}

	// After this status update, resource management should be handed off to the Sidecar
	if access.Status.ReadyToUse == nil {
		access.Status.ReadyToUse = ptr.To(false)
	}
	access.Status.AccessedBuckets = accessedBuckets
	access.Status.DriverName = class.Spec.DriverName
	access.Status.AuthenticationType = class.Spec.AuthenticationType
	access.Status.Parameters = class.Spec.Parameters
	access.Status.Error = nil
	if err := r.Status().Update(ctx, access); err != nil {
		logger.Error(err, "failed to update BucketClaim status after successful initialization")
		return err
	}

	return nil
}

// Get all BucketClaims that this BucketAccess references.
// If any claims don't exist, assume they don't exist YET; mark them nil in the resulting map
// without treating nonexistence as an error.
// When no error is returned, the output map MUST have an entry for every given BucketClaimAccess.
func getAllBucketClaims(
	ctx context.Context, client client.Client, namespace string, claimAccesses []cosiapi.BucketClaimAccess,
) (map[string]*cosiapi.BucketClaim, error) {
	claims := make(map[string]*cosiapi.BucketClaim, len(claimAccesses))
	errs := []error{}

	for _, ref := range claimAccesses {
		if _, ok := claims[ref.BucketClaimName]; ok {
			// In testing, the CEL validation rules prevent this case, but no duplicates is critical
			// to the access initialization, so double check it.
			return nil, cosierr.NonRetryableError(
				fmt.Errorf("BucketClaim %q is referenced more than once", ref.BucketClaimName))
		}

		c := cosiapi.BucketClaim{}
		nsName := types.NamespacedName{
			Namespace: namespace,
			Name:      ref.BucketClaimName,
		}
		err := client.Get(ctx, nsName, &c)
		if kerrors.IsNotFound(err) {
			// BucketClaim doesn't exist (yet)
			claims[ref.BucketClaimName] = nil
		} else if err != nil {
			// Unspecified API server error that probably resolves after exponential backoff
			errs = append(errs, err)
		} else {
			// No error
			claims[ref.BucketClaimName] = &c
		}
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("could not get one or more BucketClaims: %w", errors.Join(errs...))
	}

	if len(claims) != len(claimAccesses) {
		// Should never happen, but double check because the 1:1 requirement is critical.
		return nil, fmt.Errorf("did not get one or more BucketClaims, but no errors observed")
	}

	return claims, nil
}

// Mark all (non-nil) BucketClaims as having a BucketAccess reference.
func markAllBucketClaimsAsAccessed(
	ctx context.Context,
	client client.Client,
	claimsByName map[string]*cosiapi.BucketClaim,
) error {
	errs := []error{}
	for _, claim := range claimsByName {
		if claim == nil {
			continue
		}

		if claim.Annotations == nil {
			claim.Annotations = map[string]string{}
		}
		if _, ok := claim.Annotations[cosiapi.HasBucketAccessReferencesAnnotation]; ok {
			continue // already present
		}
		// Race condition: this will still attempt to apply the annotation even when the deletion
		// timestamp is set. This may interrupt an in-progress BucketClaim deletion before the point
		// of no return, preserving data, or it may be too late. The BucketClaim deletion logic must
		// handle the unexpected appearance of this annotation at any point.
		claim.Annotations[cosiapi.HasBucketAccessReferencesAnnotation] = ""
		if err := client.Update(ctx, claim); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("failed to mark one or more BucketClaims as having a BucketAccess reference: %w", errors.Join(errs...))
	}

	return nil
}

// Return an error if the BucketAccess doesn't meet BucketAccessClass requirements.
func validateAccessAgainstClass(
	class *cosiapi.BucketAccessClassSpec,
	access *cosiapi.BucketAccessSpec,
) error {
	errs := []error{}

	needServiceAccount := class.AuthenticationType == cosiapi.BucketAccessAuthenticationTypeServiceAccount
	if needServiceAccount && access.ServiceAccountName == "" {
		errs = append(errs, fmt.Errorf("serviceAccountName must be specified"))
	}

	if !class.SupportsMultiBucketAccess() && len(access.BucketClaims) > 1 {
		errs = append(errs, fmt.Errorf("multi-bucket access is disallowed"))
	}

	for _, claimRef := range access.BucketClaims {
		if slices.Contains(class.DisallowedBucketAccessModes, claimRef.AccessMode) {
			errs = append(errs,
				fmt.Errorf("accessMode %q requested for BucketClaim %q is disallowed",
					claimRef.AccessMode, claimRef.BucketClaimName),
			)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("one or more features are disallowed by the BucketAccessClass: %w", errors.Join(errs...))
	}
	return nil
}

// Ensure that all BucketClaims can request the access to be provisioned without known errors.
// Return a list of messages that explain what is blocking provisioning.
func cannotAccessBucketClaims(
	claimsByName map[string]*cosiapi.BucketClaim,
	spec cosiapi.BucketAccessSpec,
) []string {
	blockers := []string{}
	for name, claim := range claimsByName {
		if claim == nil {
			continue
		}
		if !claim.DeletionTimestamp.IsZero() {
			// The BucketClaim might not delete while this BucketAccess exists.
			// The BucketAccess can't proceed for the in-deletion BucketClaim.
			// Because this is a data safety race, rely on the user to resolve it as they desire.
			// This race is probably rare in the real world, so going to excessive lengths to
			// resolve it in COSI seems like premature optimization.
			blockers = append(blockers,
				fmt.Sprintf("stuck: data integrity for deleting BucketClaim %q is not guaranteed", name))
		}
		if len(claim.Status.Protocols) > 0 && !slices.Contains(claim.Status.Protocols, spec.Protocol) {
			blockers = append(blockers,
				fmt.Sprintf("BucketClaim %q does not support protocol %q", name, spec.Protocol))
		}
	}
	return blockers
}

// Ensure that all BucketClaims are provisioned enough to continue with access initialization.
// Return a list of messages that explain what needs to be waited on.
func waitingOnBucketClaims(claimsByName map[string]*cosiapi.BucketClaim) []string {
	waitMsgs := []string{}
	for name, claim := range claimsByName {
		if claim == nil {
			waitMsgs = append(waitMsgs, fmt.Sprintf("BucketClaim %q does not (yet?) exist", name))
			continue
		}
		if claim.Status.BoundBucketName == "" || len(claim.Status.Protocols) == 0 {
			waitMsgs = append(waitMsgs, fmt.Sprintf("BucketClaim %q is still provisioning", name))
			continue
		}
	}
	return waitMsgs
}

// Generate the accessedBuckets status list for the BucketAccess.
func generateAccessedBuckets(
	claimAccesses []cosiapi.BucketClaimAccess,
	claimsByName map[string]*cosiapi.BucketClaim,
) (
	[]cosiapi.AccessedBucket,
	error,
) {
	accessedBuckets := make([]cosiapi.AccessedBucket, len(claimAccesses))
	unbound := []string{}

	// It will be helpful for human readability if the ordering of AccessedBuckets in the status
	// matches the ordering of BucketClaims in the spec.
	for i, ref := range claimAccesses {
		claim, ok := claimsByName[ref.BucketClaimName]
		if !ok || claim == nil {
			// Unexpected during runtime because getAllBucketClaims() requires that all input
			// BucketAccessClaims must be represented in the claimsByName map.
			return nil, fmt.Errorf("missing expected BucketClaim internally %q", ref.BucketClaimName)
		}

		if claim.Status.BoundBucketName == "" {
			unbound = append(unbound, ref.BucketClaimName)
			continue
		}

		accessedBuckets[i] = cosiapi.AccessedBucket{
			BucketName:      claim.Status.BoundBucketName,
			BucketClaimName: claim.GetName(),
		}
	}

	if len(unbound) > 0 {
		return nil, fmt.Errorf("one or more BucketClaims are still unbound to a Bucket: %v", unbound)
	}

	return accessedBuckets, nil
}
