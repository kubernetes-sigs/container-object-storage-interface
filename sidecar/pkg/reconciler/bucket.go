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
	"regexp"
	"slices"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	cosierr "sigs.k8s.io/container-object-storage-interface/internal/errors"
	cosipredicate "sigs.k8s.io/container-object-storage-interface/internal/predicate"
	"sigs.k8s.io/container-object-storage-interface/internal/protocol"
	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
	"sigs.k8s.io/container-object-storage-interface/sidecar/internal/translator"
)

var bucketIDPattern = regexp.MustCompile(cosiapi.BucketIDPattern)

// validateBucketID checks a bucket ID against the length and character constraints shared by the
// bucket_id fields of the DriverGenerateBucketId and DriverCreateBucket RPCs (see proto/spec.md).
func validateBucketID(id string) error {
	allErrs := []string{}

	if len(id) > cosiapi.BucketIDMaxLength {
		allErrs = append(allErrs, fmt.Sprintf("must be no more than %d characters: length=%d", cosiapi.BucketIDMaxLength, len(id)))
	}

	if !bucketIDPattern.MatchString(id) {
		allErrs = append(allErrs, fmt.Sprintf("must match pattern %q", cosiapi.BucketIDPattern))
	}

	if len(allErrs) > 0 {
		return fmt.Errorf("bucket ID %q is invalid: %v", id, allErrs)
	}
	return nil
}

// BucketReconciler reconciles a Bucket object
type BucketReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	DriverInfo DriverInfo
}

// +kubebuilder:rbac:groups=objectstorage.k8s.io,resources=buckets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=objectstorage.k8s.io,resources=buckets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=objectstorage.k8s.io,resources=buckets/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *BucketReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx, "driverName", r.DriverInfo.Name)

	bucket := &cosiapi.Bucket{}
	if err := r.Get(ctx, req.NamespacedName, bucket); err != nil {
		if kerrors.IsNotFound(err) {
			logger.V(1).Info("not reconciling nonexistent Bucket")
			return ctrl.Result{}, nil
		}
		// no resource to add status to or report an event for
		logger.Error(err, "failed to get Bucket")
		return ctrl.Result{}, err
	}

	err := r.reconcile(ctx, logger, bucket)
	if err != nil {
		// Record any error as a timestamped error in the status.
		if bucket.Status.ReadyToUse == nil {
			bucket.Status.ReadyToUse = ptr.To(false)
		}
		bucket.Status.Error = cosiapi.NewTimestampedError(time.Now(), err.Error())
		if updErr := r.Status().Update(ctx, bucket); updErr != nil {
			logger.Error(err, "failed to update Bucket status after reconcile error", "updateError", updErr)
			// If status update fails, we must retry the error regardless of the reconcile return.
			// The reconcile needs to run again to make sure the status is eventually updated.
			return reconcile.Result{}, err
		}

		if errors.Is(err, cosierr.NonRetryableError(nil)) {
			return reconcile.Result{}, reconcile.TerminalError(err)
		}
		return reconcile.Result{}, err
	}

	// On success, clear any errors in the status.
	if bucket.Status.Error != nil && bucket.DeletionTimestamp.IsZero() {
		if bucket.Status.ReadyToUse == nil {
			bucket.Status.ReadyToUse = ptr.To(false)
		}
		bucket.Status.Error = nil
		if err := r.Status().Update(ctx, bucket); err != nil {
			logger.Error(err, "failed to update BucketClaim status after reconcile success")
			// Retry the reconcile so status can be updated eventually.
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *BucketReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cosiapi.Bucket{}).
		WithEventFilter(
			ctrlpredicate.And(
				driverNameMatchesPredicate(r.DriverInfo.Name), // only opt in to reconciles with matching driver name
				ctrlpredicate.Or(
					// this is the primary bucket controller and should reconcile ALL Create/Delete/Generic events
					cosipredicate.AnyCreate(),
					cosipredicate.AnyDelete(),
					cosipredicate.AnyGeneric(),
					// opt in to desired Update events
					cosipredicate.GenerationChangedInUpdateOnly(), // reconcile spec changes
					cosipredicate.DeletionTimestampAdded(),
					cosipredicate.ProtectionFinalizerRemoved(r.Scheme), // re-add protection finalizer if removed
				),
			),
		).
		Named("bucket").
		Complete(r)
}

func (r *BucketReconciler) reconcile(ctx context.Context, logger logr.Logger, bucket *cosiapi.Bucket) error {
	if bucket.Spec.DriverName != r.DriverInfo.Name {
		// keep this log to help debug any issues that might arise with predicate logic
		logger.Info("not reconciling bucket with non-matching driver name %q", bucket.Spec.DriverName)
		return nil
	}

	if !bucket.GetDeletionTimestamp().IsZero() {
		logger.Info("beginning Bucket deletion")
		return r.reconcileDelete(ctx, logger, bucket)
	}

	// BucketClaim is deleting, but Bucket not marked for deletion
	if _, claimDeleting := bucket.Annotations[cosiapi.BucketClaimBeingDeletedAnnotation]; claimDeleting {
		logger.Info("not reconciling Bucket whose BucketClaim is being deleted")
		bucket.Status.ReadyToUse = ptr.To(false)
		bucket.Status.Error = nil // previous error is no longer relevant
		if err := r.Status().Update(ctx, bucket); err != nil {
			logger.Error(err, "failed to update Bucket status before deletion")
			return fmt.Errorf("failed to update Bucket status before deletion: %w", err)
		}
		return nil
	}

	requiredProtos, err := objectProtocolListFromApiList(bucket.Spec.Protocols)
	if err != nil {
		logger.Error(err, "failed to parse protocol list")
		return cosierr.NonRetryableError(err)
	}

	if err := validateDriverSupportsProtocols(r.DriverInfo, requiredProtos); err != nil {
		logger.Error(err, "protocol(s) are unsupported")
		return cosierr.NonRetryableError(err)
	}

	isStaticProvisioning := bucket.Spec.ExistingBucketID != ""
	if isStaticProvisioning {
		logger = logger.WithValues("provisioningStrategy", "static")
	} else {
		logger = logger.WithValues("provisioningStrategy", "dynamic")
	}

	logger.V(1).Info("reconciling Bucket")

	didAdd := ctrlutil.AddFinalizer(bucket, cosiapi.ProtectionFinalizer)
	if didAdd {
		if err := r.Update(ctx, bucket); err != nil {
			logger.Error(err, "failed to add protection finalizer")
			return fmt.Errorf("failed to add protection finalizer: %w", err)
		}
	}

	// Phase 1 of dynamic provisioning. Static provisioning has no phase 1 because the bucket ID
	// is given by the user in spec.existingBucketID.
	if !isStaticProvisioning && bucket.Status.BucketID == "" {
		foreignReadyToUse := ptr.Deref(bucket.Status.ReadyToUse, false)

		if err := r.generateBucketID(ctx, logger, bucket, requiredProtos); err != nil {
			return err
		}

		// A Bucket that has not been assigned an ID cannot have been provisioned, so readyToUse must
		// not be true here. This sidecar never writes that combination, so record the incorrect state
		// in status.error and log it, in addition to correcting it (generateBucketID already reset it
		// to false). Self-clears if phase 2 succeeds in this reconcile, since the final status write
		// resets status.error to nil.
		if foreignReadyToUse {
			logger.Error(nil, "readyToUse was true before the bucket ID was generated; overwriting it as false")
			bucket.Status.Error = cosiapi.NewTimestampedError(time.Now(),
				"readyToUse was true before a bucket ID was assigned; this sidecar never produces that "+
					"combination, so it was reset to false and provisioning continues")
		}

		// Persist status.bucketID before phase 2 (dynamicProvision) provisions the backend bucket;
		// see the generateBucketID doc comment for why this ordering matters.
		if err := r.Status().Update(ctx, bucket); err != nil {
			logger.Error(err, "failed to update Bucket status after bucket ID generation")
			if kerrors.IsInvalid(err) {
				// The API server rejected the object itself; retrying with the same content cannot
				// succeed.
				return cosierr.NonRetryableError(fmt.Errorf("failed to update Bucket status after bucket ID generation: %w", err))
			}
			return fmt.Errorf("failed to update Bucket status after bucket ID generation: %w", err)
		}
		logger.Info("generated bucket ID", "bucketID", bucket.Status.BucketID)
	}

	if !isStaticProvisioning {
		logger = logger.WithValues("bucketID", bucket.Status.BucketID)
	}

	var provisionedBucket *provisionedBucketDetails
	if isStaticProvisioning {
		provisionedBucket, err = r.staticProvision(ctx, logger, staticProvisionParams{
			existingBucketID: bucket.Spec.ExistingBucketID,
			requiredProtos:   requiredProtos,
			parameters:       bucket.Spec.Parameters,
			claimRef:         bucket.Spec.BucketClaimRef,
		})
	} else {
		provisionedBucket, err = r.dynamicProvision(ctx, logger, dynamicProvisionParams{
			bucketID:       bucket.Status.BucketID,
			requiredProtos: requiredProtos,
			parameters:     bucket.Spec.Parameters,
			claimRef:       bucket.Spec.BucketClaimRef,
		})
	}
	if err != nil {
		return err
	}

	// final validation and status updates are the same for dynamic and static provisioning

	if len(provisionedBucket.supportedProtos) == 0 {
		logger.Error(nil, "created bucket supports no protocols")
		return cosierr.NonRetryableError(fmt.Errorf("created bucket supports no protocols"))
	}

	if err := validateBucketSupportsProtocols(provisionedBucket.supportedProtos, bucket.Spec.Protocols); err != nil {
		logger.Error(err, "bucket required protocols missing")
		return cosierr.NonRetryableError(fmt.Errorf("bucket required protocols missing: %w", err))
	}

	bucket.Status = cosiapi.BucketStatus{
		ReadyToUse: ptr.To(true),
		BucketID:   provisionedBucket.bucketId,
		Protocols:  provisionedBucket.supportedProtos,
		BucketInfo: provisionedBucket.allProtoBucketInfo,
		Error:      nil,
	}
	if err := r.Status().Update(ctx, bucket); err != nil {
		logger.Error(err, "failed to update Bucket status after successful bucket creation")
		return fmt.Errorf("failed to update Bucket status after successful bucket creation: %w", err)
	}

	return nil
}

func (r *BucketReconciler) reconcileDelete(
	ctx context.Context, logger logr.Logger, bucket *cosiapi.Bucket,
) error {
	logger = logger.WithValues("deletionPolicy", bucket.Spec.DeletionPolicy)
	if bucket.Spec.DeletionPolicy != cosiapi.BucketDeletionPolicyDelete {
		logger.Error(nil, "will not delete Bucket with non-delete deletion policy")
		return cosierr.NonRetryableError(
			fmt.Errorf("will not delete Bucket with non-delete deletion policy %q", bucket.Spec.DeletionPolicy))
	}

	_, claimDeleting := bucket.Annotations[cosiapi.BucketClaimBeingDeletedAnnotation]
	claimRef := bucket.Spec.BucketClaimRef
	logger = logger.WithValues("bucketClaimBeingDeleted", claimDeleting, "bucketClaimRef", claimRef)
	if !claimDeleting {
		// Annotation means deletion should proceed. Not present, needs more checking.
		if claimRef.UID != types.UID("") {
			// Bucket is bound to a BucketClaim, so cannot delete
			// BucketClaim controller will apply the annotation when it cleans up the claim
			logger.Error(nil, "will not delete Bucket bound to a non-deleting BucketClaim")
			return cosierr.NonRetryableError(
				fmt.Errorf("will not delete Bucket bound to a non-deleting BucketClaim: %#v", claimRef))
		}
		// claimRef.UID == "": not bound to a BucketClaim, so can delete
	}

	logger.Info("proceeding with Bucket deletion")

	bucket.Status.ReadyToUse = ptr.To(false)
	bucket.Status.Error = nil // previous error is no longer relevant
	if err := r.Status().Update(ctx, bucket); err != nil {
		logger.Error(err, "failed to update Bucket status before deletion")
		return fmt.Errorf("failed to update Bucket status before deletion: %w", err)
	}

	if bucket.Status.BucketID != "" {
		logger.Info("calling driver to delete bucket", "bucketID", bucket.Status.BucketID)
		_, err := r.DriverInfo.ProvisionerClient.DriverDeleteBucket(ctx,
			&cosiproto.DriverDeleteBucketRequest{
				BucketId:   bucket.Status.BucketID,
				Parameters: bucket.Spec.Parameters,
			},
		)
		if err != nil {
			logger.Error(err, "DriverDeleteBucket error")
			if rpcErrorIsRetryable(status.Code(err)) {
				return err
			}
			// Do not proceed with k8s resource cleanup after a non-retryable error. Proceeding would
			// risk leaving backend resources orphaned without any clear indication to administrators
			// that they need to do manual cleanup if desired. If this is a sidecar or driver error, an
			// update could resolve the issue to allow a future deletion to succeed.
			return cosierr.NonRetryableError(err)
		}
	} else {
		logger.Info("not calling driver to delete bucket with no recorded bucketID")
	}

	ctrlutil.RemoveFinalizer(bucket, cosiapi.ProtectionFinalizer)
	if err := r.Update(ctx, bucket); err != nil {
		logger.Error(err, "failed to remove finalizer")
		return fmt.Errorf("failed to remove finalizer: %w", err)
	}

	return nil
}

// Details about provisioned bucket for both dynamic and static provisioning.
// A struct with named params allows for future expansion easily.
// When param lists get long, named fields help with readability, review, and maintenance.
type provisionedBucketDetails struct {
	bucketId           string
	supportedProtos    []cosiapi.ObjectProtocol
	allProtoBucketInfo map[string]string
}

// Run phase 1 of the 2-phase dynamic provisioning workflow.
// The driver generates the bucket ID without provisioning any backend resource. The caller is
// responsible for persisting the returned status.bucketID before running phase 2
// (dynamicProvision), which provisions the backend bucket: this guarantees that any backend
// bucket created in phase 2 is reachable by an ID that is already recorded in Kubernetes, even if
// the sidecar crashes between the two phases.
func (r *BucketReconciler) generateBucketID(
	ctx context.Context,
	logger logr.Logger,
	bucket *cosiapi.Bucket,
	requiredProtos []*cosiproto.ObjectProtocol,
) error {
	// The input parameters given here are the same parameters later used for phase-2 provisioning,
	// so a driver may use any of them when determining bucket_id. See proto/spec.md.
	resp, err := r.DriverInfo.ProvisionerClient.DriverGenerateBucketId(ctx,
		&cosiproto.DriverGenerateBucketIdRequest{
			Name:       bucket.Name,
			Protocols:  requiredProtos,
			Parameters: bucket.Spec.Parameters,
		},
	)
	if err != nil {
		logger.Error(err, "DriverGenerateBucketIdRequest error")
		if rpcErrorIsRetryable(status.Code(err)) {
			return err
		}
		if status.Code(err) == codes.Unimplemented {
			// A driver upgrade alone will not retry this Bucket: controller-runtime never
			// requeues a TerminalError, so make the required remedy (restart the sidecar)
			// discoverable from status.error rather than only from drivers.md.
			return cosierr.NonRetryableError(fmt.Errorf(
				"driver does not implement DriverGenerateBucketId; upgrade the driver, then restart the sidecar to retry this Bucket: %w", err))
		}
		return cosierr.NonRetryableError(err)
	}

	if resp.BucketId == "" {
		logger.Error(nil, "generated bucket ID missing")
		// driver behavior is unlikely to change if the request is retried
		return cosierr.NonRetryableError(fmt.Errorf("generated bucket ID missing"))
	}

	if err := validateBucketID(resp.BucketId); err != nil {
		// proto/spec.md constrains DriverGenerateBucketIdResponse.bucket_id to this length and
		// pattern; this is a driver bug, not a Bucket API validation failure, so fail clearly here
		// rather than let it surface later as an API server rejection, and so a persistently
		// non-conforming ID does not get retried forever.
		logger.Error(err, "driver-returned bucket ID does not satisfy the DriverGenerateBucketIdResponse.bucket_id schema", "bucketID", resp.BucketId)
		return cosierr.NonRetryableError(err)
	}

	// readyToUse is a required field and must not report true until phase 2 succeeds.
	bucket.Status.ReadyToUse = ptr.To(false)
	// status.bucketID is immutable once set, so this write reserves the ID for the life of the
	// Bucket. The caller must persist this before running phase 2; see the doc comment above.
	bucket.Status.BucketID = resp.BucketId

	return nil
}

// Parameters for dynamic provisioning workflow.
// A struct with named params allows for future expansion easily.
// When param lists get long, named fields help with readability, review, and maintenance.
type dynamicProvisionParams struct {
	bucketID       string
	requiredProtos []*cosiproto.ObjectProtocol
	parameters     map[string]string
	claimRef       cosiapi.BucketClaimReference
}

// Run dynamic provisioning workflow.
func (r *BucketReconciler) dynamicProvision(
	ctx context.Context,
	logger logr.Logger,
	dynamic dynamicProvisionParams,
) (
	details *provisionedBucketDetails,
	err error,
) {
	cr := dynamic.claimRef
	if cr.Name == "" || cr.Namespace == "" || cr.UID == "" {
		// likely a malformed bucket intended for static provisioning (possible COSI controller bug)
		logger.Error(nil, "all bucketClaimRef fields must be set for dynamic provisioning", "bucketClaimRef", cr)
		return nil, cosierr.NonRetryableError(
			fmt.Errorf("all bucketClaimRef fields must be set for dynamic provisioning: %#v", cr))
	}

	if dynamic.bucketID == "" {
		// phase 1 (generateBucketID) must persist the bucket ID before phase 2 runs
		logger.Error(nil, "bucket ID missing before bucket creation")
		return nil, cosierr.NonRetryableError(fmt.Errorf("bucket ID missing before bucket creation"))
	}

	resp, err := r.DriverInfo.ProvisionerClient.DriverCreateBucket(ctx,
		&cosiproto.DriverCreateBucketRequest{
			BucketId:   dynamic.bucketID,
			Protocols:  dynamic.requiredProtos,
			Parameters: dynamic.parameters,
		},
	)
	if err != nil {
		logger.Error(err, "DriverCreateBucketRequest error")
		if rpcErrorIsRetryable(status.Code(err)) {
			return nil, err
		}
		return nil, cosierr.NonRetryableError(err)
	}

	protoResp := resp.Protocols
	if protoResp == nil {
		logger.Error(nil, "created bucket protocol response missing")
		return nil, cosierr.NonRetryableError(fmt.Errorf("created bucket protocol response missing"))
	}

	var noValidation *translator.ValidationConfig = nil
	supportedProtos, allBucketInfo, err := translator.BucketInfoToApi(protoResp, noValidation)
	if err != nil {
		logger.Error(nil, "errors translating bucket info")
		return nil, cosierr.NonRetryableError(err)
	}

	details = &provisionedBucketDetails{
		bucketId:           dynamic.bucketID,
		supportedProtos:    supportedProtos,
		allProtoBucketInfo: allBucketInfo,
	}
	return details, nil
}

// Parameters for static provisioning workflow.
type staticProvisionParams struct {
	existingBucketID string
	requiredProtos   []*cosiproto.ObjectProtocol
	parameters       map[string]string
	claimRef         cosiapi.BucketClaimReference
}

// Run static provisioning workflow.
func (r *BucketReconciler) staticProvision(
	ctx context.Context,
	logger logr.Logger,
	static staticProvisionParams,
) (*provisionedBucketDetails, error) {
	ref := static.claimRef
	if ref.Name == "" || ref.Namespace == "" {
		logger.Error(nil, "bucketClaimRef namespace and name must be set for static provisioning", "bucketClaimRef", ref)
		return nil, cosierr.NonRetryableError(
			fmt.Errorf("bucketClaimRef namespace and name must be set for static provisioning: %#v", ref))
	}

	resp, err := r.DriverInfo.ProvisionerClient.DriverGetBucket(ctx,
		&cosiproto.DriverGetBucketRequest{
			BucketId:   static.existingBucketID,
			Protocols:  static.requiredProtos,
			Parameters: static.parameters,
		},
	)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			err = fmt.Errorf("waiting for backend bucket to exist: %w", err)
			logger.Error(err, "DriverGetBucket error")
			return nil, err
		}

		logger.Error(err, "DriverGetBucket error")
		if rpcErrorIsRetryable(status.Code(err)) {
			return nil, err
		}
		return nil, cosierr.NonRetryableError(err)
	}

	if resp.BucketId == "" {
		logger.Error(nil, "existing bucket ID missing in response")
		return nil, cosierr.NonRetryableError(fmt.Errorf("existing bucket ID missing in response"))
	}

	protoResp := resp.Protocols
	if protoResp == nil {
		logger.Error(nil, "existing bucket protocol response missing")
		return nil, cosierr.NonRetryableError(fmt.Errorf("existing bucket protocol response missing"))
	}

	var noValidation *translator.ValidationConfig = nil
	supportedProtos, allBucketInfo, err := translator.BucketInfoToApi(protoResp, noValidation)
	if err != nil {
		logger.Error(err, "errors translating existing bucket info")
		return nil, cosierr.NonRetryableError(err)
	}

	return &provisionedBucketDetails{
		bucketId:           resp.BucketId,
		supportedProtos:    supportedProtos,
		allProtoBucketInfo: allBucketInfo,
	}, nil
}

// convert an API proto list into an RPC proto message list
func objectProtocolListFromApiList(apiList []cosiapi.ObjectProtocol) ([]*cosiproto.ObjectProtocol, error) {
	errs := []error{}
	out := []*cosiproto.ObjectProtocol{}

	for _, apiProto := range apiList {
		rpcProto, err := protocol.ObjectProtocolTranslator{}.ApiToRpc(apiProto)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		out = append(out, &cosiproto.ObjectProtocol{
			Type: rpcProto,
		})
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to parse protocol list: %w", errors.Join(errs...))
	}
	return out, nil
}

// validate that the required protocols (if given) are supported by the driver
func validateDriverSupportsProtocols(driver DriverInfo, required []*cosiproto.ObjectProtocol) error {
	unsupportedProtos := []string{}

	for _, proto := range required {
		if !driver.SupportsProtocol(proto.Type) {
			unsupportedProtos = append(unsupportedProtos, proto.Type.String())
		}
	}

	if len(unsupportedProtos) > 0 {
		return fmt.Errorf("driver %q does not support protocols: %v", driver.Name, unsupportedProtos)
	}
	return nil
}

// validate the required protocols (if given) are in the supported list (from bucket provisioning results)
func validateBucketSupportsProtocols(supported, required []cosiapi.ObjectProtocol) error {
	unsupported := []string{}
	for _, req := range required {
		if !slices.Contains(supported, req) {
			unsupported = append(unsupported, string(req))
		}
	}
	if len(unsupported) > 0 {
		return fmt.Errorf("required protocols are not supported: %v", unsupported)
	}
	return nil
}
