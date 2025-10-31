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
	"google.golang.org/grpc/status"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	cosipredicate "sigs.k8s.io/container-object-storage-interface/internal/predicate"
	"sigs.k8s.io/container-object-storage-interface/internal/protocol"
	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
)

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
	logger := ctrl.LoggerFrom(ctx, "driverName", r.DriverInfo.name)

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

	retryError, err := r.reconcile(ctx, logger, bucket)
	if err != nil {
		// Record any error as a timestamped error in the status.
		bucket.Status.Error = cosiapi.NewTimestampedError(time.Now(), err.Error())
		if updErr := r.Status().Update(ctx, bucket); updErr != nil {
			logger.Error(err, "failed to update Bucket status after reconcile error", "updateError", updErr)
			// If status update fails, we must retry the error regardless of the reconcile return.
			// The reconcile needs to run again to make sure the status is eventually be updated.
			return reconcile.Result{}, err
		}

		if !retryError {
			return reconcile.Result{}, reconcile.TerminalError(err)
		}
		return reconcile.Result{}, err
	}

	// On success, clear any errors in the status.
	if bucket.Status.Error != nil && !bucket.DeletionTimestamp.IsZero() {
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
				driverNameMatchesPredicate(r.DriverInfo.name), // only opt in to reconciles with matching driver name
				ctrlpredicate.Or(
					// this is the primary bucket controller and should reconcile ALL Create/Delete/Generic events
					cosipredicate.AnyCreate(),
					cosipredicate.AnyDelete(),
					cosipredicate.AnyGeneric(),
					// opt in to desired Update events
					cosipredicate.GenerationChangedInUpdateOnly(),      // reconcile spec changes
					cosipredicate.ProtectionFinalizerRemoved(r.Scheme), // re-add protection finalizer if removed
				),
			),
		).
		Named("bucket").
		Complete(r)
}

// Type definition enforces readability. true/false in code is hard to read/review/maintain.
type retryErrorType bool

const (
	RetryError      retryErrorType = true  // DO retry the error.
	DoNotRetryError retryErrorType = false // Do NOT retry the error.
	NoError         retryErrorType = false // No error, don't retry. For readability.
)

func (r *BucketReconciler) reconcile(
	ctx context.Context, logger logr.Logger, bucket *cosiapi.Bucket,
) (retryErrorType, error) {
	if bucket.Spec.DriverName != r.DriverInfo.name {
		// TODO: configure the predicate to ignore any reconcile with non-matching driver
		// keep this log to help debug any issues that might arise with predicate logic
		logger.Info("not reconciling bucket with non-matching driver name %q", bucket.Spec.DriverName)
		return NoError, nil
	}

	if !bucket.GetDeletionTimestamp().IsZero() {
		logger.V(1).Info("beginning Bucket deletion cleanup")

		// TODO: deletion logic

		ctrlutil.RemoveFinalizer(bucket, cosiapi.ProtectionFinalizer)
		if err := r.Update(ctx, bucket); err != nil {
			logger.Error(err, "failed to remove finalizer")
			return RetryError, fmt.Errorf("failed to remove finalizer: %w", err)
		}

		return DoNotRetryError, fmt.Errorf("deletion is not yet implemented") // TODO
	}

	requiredProtos, err := objectProtocolListFromApiList(bucket.Spec.Protocols)
	if err != nil {
		logger.Error(err, "failed to parse protocol list")
		return DoNotRetryError, err
	}

	if err := validateDriverSupportsProtocols(r.DriverInfo, requiredProtos); err != nil {
		logger.Error(err, "protocol(s) are unsupported")
		return DoNotRetryError, err
	}

	isStaticProvisioning := false
	if bucket.Spec.ExistingBucketID != "" {
		// isStaticProvisioning = true
		// logger = logger.WithValues("provisioningStrategy", "static")
		return DoNotRetryError, fmt.Errorf("static provisioning is not yet supported") // TODO
	} else {
		logger = logger.WithValues("provisioningStrategy", "dynamic")
	}

	logger.V(1).Info("reconciling BucketClaim")

	didAdd := ctrlutil.AddFinalizer(bucket, cosiapi.ProtectionFinalizer)
	if didAdd {
		if err := r.Update(ctx, bucket); err != nil {
			logger.Error(err, "failed to add protection finalizer")
			return RetryError, fmt.Errorf("failed to add protection finalizer: %w", err)
		}
	}

	var provisionedBucket *provisionedBucketDetails
	var retryErr retryErrorType
	if isStaticProvisioning {
		logger.Error(err, "how did we get here?") // TODO: static
	} else {
		provisionedBucket, retryErr, err = r.dynamicProvision(ctx, logger, dynamicProvisionParams{
			bucketName:     bucket.Name,
			requiredProtos: requiredProtos,
			parameters:     bucket.Spec.Parameters,
			claimRef:       bucket.Spec.BucketClaimRef,
		})
	}
	if err != nil {
		return retryErr, err
	}

	// final validation and status updates are the same for dynamic and static provisioning

	if len(provisionedBucket.supportedProtos) == 0 {
		logger.Error(nil, "created bucket supports no protocols")
		return DoNotRetryError, fmt.Errorf("created bucket supports no protocols")
	}

	if err := validateBucketSupportsProtocols(provisionedBucket.supportedProtos, bucket.Spec.Protocols); err != nil {
		logger.Error(err, "bucket required protocols missing")
		return DoNotRetryError, fmt.Errorf("bucket required protocols missing: %w", err)
	}

	bucket.Status = cosiapi.BucketStatus{
		ReadyToUse: true,
		BucketID:   provisionedBucket.bucketId,
		Protocols:  provisionedBucket.supportedProtos,
		BucketInfo: provisionedBucket.allProtoBucketInfo,
		Error:      nil,
	}
	if err := r.Status().Update(ctx, bucket); err != nil {
		logger.Error(err, "failed to update Bucket status after successful bucket creation")
		return RetryError, fmt.Errorf("failed to update Bucket status after successful bucket creation: %w", err)
	}

	return NoError, nil
}

// Details about provisioned bucket for both dynamic and static provisioning.
// A struct with named params allows for future expansion easily.
// When param lists get long, named fields help with readability, review, and maintenance.
type provisionedBucketDetails struct {
	bucketId           string
	supportedProtos    []cosiapi.ObjectProtocol
	allProtoBucketInfo map[string]string
}

// Parameters for dynamic provisioning workflow.
// A struct with named params allows for future expansion easily.
// When param lists get long, named fields help with readability, review, and maintenance.
type dynamicProvisionParams struct {
	bucketName     string
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
	retry retryErrorType,
	err error,
) {
	cr := dynamic.claimRef
	if cr.Name == "" || cr.Namespace == "" || cr.UID == "" {
		// likely a malformed bucket intended for static provisioning (possible COSI controller bug)
		logger.Error(nil, "all bucketClaimRef fields must be set for dynamic provisioning", "bucketClaimRef", cr)
		return nil, DoNotRetryError, fmt.Errorf("all bucketClaimRef fields must be set for dynamic provisioning: %#v", cr)
	}

	resp, err := r.DriverInfo.provisionerClient.DriverCreateBucket(ctx,
		&cosiproto.DriverCreateBucketRequest{
			Name:       dynamic.bucketName,
			Protocols:  dynamic.requiredProtos,
			Parameters: dynamic.parameters,
		},
	)
	if err != nil {
		logger.Error(err, "DriverCreateBucketRequest error")
		if rpcErrorIsRetryable(status.Code(err)) {
			return nil, RetryError, err
		}
		return nil, DoNotRetryError, err
	}

	if resp.BucketId == "" {
		logger.Error(nil, "created bucket ID missing")
		// driver behavior is unlikely to change if the request is retried
		return nil, DoNotRetryError, fmt.Errorf("created bucket ID missing")
	}

	protoResp := resp.Protocols
	if protoResp == nil {
		logger.Error(nil, "created bucket protocol response missing")
		return nil, DoNotRetryError, fmt.Errorf("created bucket protocol response missing")
	}

	supportedProtos, allBucketInfo := parseProtocolBucketInfo(protoResp)

	details = &provisionedBucketDetails{
		bucketId:           resp.BucketId,
		supportedProtos:    supportedProtos,
		allProtoBucketInfo: allBucketInfo,
	}
	return details, NoError, nil
}

// Parse driver's per-protocol bucket info into raw user-facing info. Input must be non-nil.
func parseProtocolBucketInfo(pbi *cosiproto.ObjectProtocolAndBucketInfo) (
	supportedProtos []cosiapi.ObjectProtocol,
	allProtoBucketInfo map[string]string,
) {
	supportedProtos = []cosiapi.ObjectProtocol{}
	allProtoBucketInfo = map[string]string{}

	if pbi.S3 != nil {
		supportedProtos = append(supportedProtos, cosiapi.ObjectProtocolS3)
		s3Translator := protocol.S3BucketInfoTranslator{}
		mergeApiInfoIntoStringMap(s3Translator.RpcToApi(pbi.S3), allProtoBucketInfo)
	}

	if pbi.Azure != nil {
		supportedProtos = append(supportedProtos, cosiapi.ObjectProtocolAzure)
		azureTranslator := protocol.AzureBucketInfoTranslator{}
		mergeApiInfoIntoStringMap(azureTranslator.RpcToApi(pbi.Azure), allProtoBucketInfo)
	}

	if pbi.Gcs != nil {
		supportedProtos = append(supportedProtos, cosiapi.ObjectProtocolGcs)
		gcsTranslator := protocol.GcsBucketInfoTranslator{}
		mergeApiInfoIntoStringMap(gcsTranslator.RpcToApi(pbi.Gcs), allProtoBucketInfo)
	}

	return supportedProtos, allProtoBucketInfo
}

// convert an API proto list into an RPC proto message list
func objectProtocolListFromApiList(apiList []cosiapi.ObjectProtocol) ([]*cosiproto.ObjectProtocol, error) {
	errs := []string{}
	out := []*cosiproto.ObjectProtocol{}

	for _, apiProto := range apiList {
		rpcProto, err := protocol.ObjectProtocolTranslator{}.ApiToRpc(apiProto)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		out = append(out, &cosiproto.ObjectProtocol{
			Type: rpcProto,
		})
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to parse protocol list: %v", errs)
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
		return fmt.Errorf("driver %q does not support protocols: %v", driver.name, unsupportedProtos)
	}
	return nil
}

// validate the required protocols (if given) are in the supported list (from bucket provisioning results)
func validateBucketSupportsProtocols(supported, required []cosiapi.ObjectProtocol) error {
	unsupported := []string{}
	for _, req := range required {
		if !contains(supported, req) {
			unsupported = append(unsupported, string(req))
		}
	}
	if len(unsupported) > 0 {
		return fmt.Errorf("required protocols are not supported: %v", required)
	}
	return nil
}

func mergeApiInfoIntoStringMap[T cosiapi.BucketInfoVar | cosiapi.CredentialVar](
	varKey map[T]string, target map[string]string,
) {
	if target == nil {
		target = map[string]string{}
	}
	for k, v := range varKey {
		target[string(k)] = v
	}
}

// contains returns true if the given `list` contains the item `key`.
func contains[T comparable](list []T, key T) bool {
	for _, i := range list {
		if i == key {
			return true
		}
	}
	return false
}
