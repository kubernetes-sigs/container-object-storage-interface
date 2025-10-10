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
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	cosipredicate "sigs.k8s.io/container-object-storage-interface/internal/predicate"
)

// BucketClaimReconciler reconciles a BucketClaim object
type BucketClaimReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=objectstorage.k8s.io,resources=bucketclaims,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups=objectstorage.k8s.io,resources=bucketclaims/status,verbs=get;update
// +kubebuilder:rbac:groups=objectstorage.k8s.io,resources=bucketclaims/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=events,verbs=create

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *BucketClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	claim := &cosiapi.BucketClaim{}
	if err := r.Get(ctx, req.NamespacedName, claim); err != nil {
		if kerrors.IsNotFound(err) {
			logger.V(1).Info("not reconciling nonexistent BucketClaim")
			return ctrl.Result{}, nil
		}
		// no resource to add status to or report an event for
		return ctrl.Result{}, fmt.Errorf("failed to get BucketClaim: %w", err)
	}

	res, err := r.reconcile(ctx, claim)

	// In order to help keep the main reconcile routine cleaner, update status.Error after every reconcile.
	if err != nil {
		claim.Status.Error = cosiapi.NewTimestampedError(time.Now(), err.Error())
		if err := r.Status().Update(ctx, claim); err != nil {
			logger.Error(err, "failed to update BucketClaim status after reconcile error", "error", err)
		}
	} else {
		claim.Status.Error = nil
		if err := r.Status().Update(ctx, claim); err != nil {
			msg := "failed to update BucketClaim status after reconcile success"
			logger.Error(err, msg)
			return res, fmt.Errorf("%s: %w", msg, err) // retry the reconcile so status can be updated eventually
		}
	}

	return res, err
}

func (r *BucketClaimReconciler) reconcile(ctx context.Context, claim *cosiapi.BucketClaim) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	if !claim.GetDeletionTimestamp().IsZero() {
		logger.V(1).Info("beginning BucketClaim deletion cleanup")

		// TODO: deletion logic

		ctrlutil.RemoveFinalizer(claim, cosiapi.ProtectionFinalizer)
		if err := r.Update(ctx, claim); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to remove finalizer: %w", err)
		}

		return ctrl.Result{}, reconcile.TerminalError(fmt.Errorf("deletion is not implemented"))
	}

	logger.V(1).Info("beginning BucketClaim reconcile")

	didAdd := ctrlutil.AddFinalizer(claim, cosiapi.ProtectionFinalizer)
	if didAdd {
		logger.V(1).Info("adding finalizer to BucketClaim")
		if err := r.Update(ctx, claim); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add protection finalizer: %w", err)
		}
	}

	bucketName := claim.Status.BoundBucketName
	if claim.Spec.ExistingBucketName != "" { // static provisioning
		if bucketName == "" {
			bucketName = claim.Spec.ExistingBucketName
		}
		if bucketName != claim.Spec.ExistingBucketName {
			err := fmt.Errorf("existingBucketName %q does not agree with boundBucketName %q",
				claim.Spec.ExistingBucketName, bucketName)
			return reconcile.Result{}, reconcile.TerminalError(err)
		}
		return ctrl.Result{}, reconcile.TerminalError(
			fmt.Errorf("existingBucketName %q support is not implemented", bucketName),
		)
	} else if bucketName == "" { // dynamic provisioning and no intermediate name generated yet
		bucketName = generateIntermediateBucketName(claim)
	}

	logger = logger.WithValues("bucketName", bucketName)

	if claim.Status.BoundBucketName == "" {
		// This timing/placement is critical for dynamic provisioning. We must commit the name of
		// the intermediate Bucket before creating the intermediate. This prevents any possibility
		// of creating an intermediate Bucket with naming version vX that becomes orphaned when the
		// controller is updated ot vX+1 which has a change to intermediate naming conventions.
		logger.Info("binding BucketClaim to Bucket")
		claim.Status.BoundBucketName = bucketName
		if err := r.Status().Update(ctx, claim); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to set boundBucketName on status: %w", err)
		}
	}

	bucket := &cosiapi.Bucket{}
	bucketNsName := types.NamespacedName{
		Name:      bucketName,
		Namespace: "", // global resource
	}
	if err := r.Get(ctx, bucketNsName, bucket); err != nil {
		if !kerrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("failed to determine if Bucket %q exists", bucketName)
		}

		// TODO: don't do for static provisioning
		logger.Info("creating intermediate Bucket")
		bucket, err = createIntermediateBucket(ctx, logger, r.Client, claim, bucketName)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to create intermediate Bucket %q: %w", bucketName, err)
		}
	}

	if err := bucketReferencesClaim(bucket, claim); err != nil {
		return ctrl.Result{}, reconcile.TerminalError(err)
	}

	// TODO:
	// 4. Controller fills in BucketClaim status to point to intermediate Bucket (claim is now bound to Bucket)
	// 5. Controller waits for the intermediate Bucket to be reconciled by COSI sidecar

	// TODO:
	// 5. Controller detects that the Bucket is provisioned successfully (`ReadyToUse`==true)
	// 1. Controller finishes BucketClaim reconciliation processing
	// 2. Controller validates BucketClaim and Bucket fields to ensure provisioning success
	// 3. Controller copies Bucket status items to BucketClaim status as needed. Importantly:
	//     1. Supported protocols
	//     2. `ReadyToUse`

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *BucketClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cosiapi.BucketClaim{}).
		WithEventFilter(
			ctrlpredicate.Or( //
				// this is the only bucketclaim controller and should reconcile ALL Create/Delete/Generic events
				cosipredicate.AnyCreate(),
				cosipredicate.AnyDelete(),
				cosipredicate.AnyGeneric(),
				// opt in to desired Update events
				cosipredicate.GenerationChangedInUpdateOnly(),      // reconcile spec changes
				cosipredicate.ProtectionFinalizerRemoved(r.Scheme), // re-add protection finalizer if removed
			),
		).
		Named("bucketclaim"). // TODO: .Owns(&cosiapi.Bucket{}, builder.WithPredicates(...))
		Complete(r)
}

// given the name of a bucketclaim, return the name of the intermediate bucket name
func generateIntermediateBucketName(c *cosiapi.BucketClaim) string {
	return "bc-" + string(c.UID) // DO NOT CHANGE UNLESS ABSOLUTELY NECESSARY
}

func createIntermediateBucket(
	ctx context.Context,
	logger logr.Logger,
	client client.Client,
	claim *cosiapi.BucketClaim,
	bucketName string,
) (*cosiapi.Bucket, error) {
	className := claim.Spec.BucketClassName
	if className == "" {
		return nil, reconcile.TerminalError(fmt.Errorf("BucketClaim cannot have empty bucketClassName"))
	}
	logger = logger.WithValues("bucketClassName", className)

	classNsName := types.NamespacedName{
		Name:      className,
		Namespace: "", // global resource
	}
	class := &cosiapi.BucketClass{}
	if err := client.Get(ctx, classNsName, class); err != nil {
		if kerrors.IsNotFound(err) {
			// TODO: for now, return an error and allow the controller to exponential backoff
			// until the BucketClass exists. in the future, optimize this by adding a
			// BucketClass reconciler that enqueues requests for BucketClaims that reference the
			// class and don't yet have a bound Bucket. And return TerminalError() here instead.
			return nil, fmt.Errorf("BucketClass %q not found: %w", className, err)
		}
		return nil, fmt.Errorf("failed to get BucketClass %q: %w", className, err)
	}

	if err := validateBucketClass(class); err != nil {
		return nil, reconcile.TerminalError(err)
	}

	logger.V(1).Info("using BucketClass for intermediate Bucket")

	bucket := generateIntermediateBucket(claim, class, bucketName)

	if err := client.Create(ctx, bucket); err != nil {
		if kerrors.IsAlreadyExists(err) {
			// unlikely race condition - error and allow the next reconcile to attempt to recover
			return nil, fmt.Errorf("intermediate bucket %q already exists: %w", bucketName, err)
		}
		return nil, fmt.Errorf("failed to create intermediate bucket %q: %w", bucketName, err)
	}

	return bucket, nil
}

// TODO: unit test? Move to API? elsewhere?
func validateBucketClass(c *cosiapi.BucketClass) error {
	spec := c.Spec
	if spec.DriverName == "" {
		return fmt.Errorf("BucketClass %q driverName cannot be empty", c.Name)
	}
	if spec.DeletionPolicy == "" {
		return fmt.Errorf("BucketClass %q deletionPolicy cannot be empty", c.Name)
	}
	return nil
}

// TODO: unit test?
func generateIntermediateBucket(
	claim *cosiapi.BucketClaim, class *cosiapi.BucketClass, bucketName string,
) *cosiapi.Bucket {
	return &cosiapi.Bucket{
		ObjectMeta: meta.ObjectMeta{
			Name: bucketName,
			Finalizers: []string{
				cosiapi.ProtectionFinalizer,
			},
		},
		Spec: cosiapi.BucketSpec{
			DriverName:     class.Spec.DriverName,
			DeletionPolicy: class.Spec.DeletionPolicy,
			Parameters:     class.Spec.Parameters,
			Protocols:      claim.Spec.Protocols,
			BucketClaimRef: cosiapi.BucketClaimReference{
				Name:      claim.Name,
				Namespace: claim.Namespace,
				UID:       claim.UID,
			},
		},
	}
}

// TODO: unit test?
func bucketReferencesClaim(bucket *cosiapi.Bucket, claim *cosiapi.BucketClaim) error {
	claimRef := bucket.Spec.BucketClaimRef
	claimNsName := types.NamespacedName{
		Namespace: claim.Namespace,
		Name:      claim.Name,
	}
	if claimRef.Name != claim.Name {
		return fmt.Errorf("Bucket %q bucketClaimRef name %q does not match BucketClaim %q",
			bucket.Name, claimRef.Name, claimNsName)
	}
	if claimRef.Namespace != claim.Namespace {
		return fmt.Errorf("Bucket %q bucketClaimRef namespace %q does not match BucketClaim %q",
			bucket.Name, claimRef.Namespace, claimNsName)
	}
	if claimRef.UID != "" && claimRef.UID != claim.UID {
		// empty claimRef UID is valid for static Buckets before they officially bind to the claim
		return fmt.Errorf("Bucket %q bucketClaimRef UID %q does not match BucketClaim %q",
			bucket.Name, claimRef.Namespace, claimRef.Name)
	}
	return nil
}
