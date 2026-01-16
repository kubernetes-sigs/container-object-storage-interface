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
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrlpredicate "sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	"sigs.k8s.io/container-object-storage-interface/internal/bucketaccess"
	cosierr "sigs.k8s.io/container-object-storage-interface/internal/errors"
	cosipredicate "sigs.k8s.io/container-object-storage-interface/internal/predicate"
	"sigs.k8s.io/container-object-storage-interface/internal/protocol"
	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
)

// BucketAccessReconciler reconciles a BucketAccess object
type BucketAccessReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	DriverInfo DriverInfo
}

// +kubebuilder:rbac:groups=objectstorage.k8s.io,resources=bucketaccesses,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=objectstorage.k8s.io,resources=bucketaccesses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=objectstorage.k8s.io,resources=bucketaccesses/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *BucketAccessReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx, "driverName", r.DriverInfo.name)

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

	if !bucketaccess.ManagedBySidecar(access) {
		logger.V(1).Info("not reconciling BucketAccess that should be managed by controller")
		return ctrl.Result{}, nil
	}

	err := r.reconcile(ctx, logger, access)
	if err != nil {
		// Because the BucketAccess status is could be managed by either Sidecar or Controller,
		// indicate that this error is coming from the Sidecar.
		err = fmt.Errorf("COSI Sidecar error: %w", err)

		// Record any error as a timestamped error in the status.
		if access.Status.ReadyToUse == nil {
			access.Status.ReadyToUse = ptr.To(false)
		}
		access.Status.Error = cosiapi.NewTimestampedError(time.Now(), err.Error())
		if updErr := r.Status().Update(ctx, access); updErr != nil {
			logger.Error(err, "failed to update BucketAccess status after reconcile error", "updateError", updErr)
			// If status update fails, we must retry the error regardless of the reconcile return.
			// The reconcile needs to run again to make sure the status is eventually be updated.
			return reconcile.Result{}, err
		}

		if errors.Is(err, cosierr.NonRetryableError(nil)) {
			return reconcile.Result{}, reconcile.TerminalError(err)
		}
		return reconcile.Result{}, err
	}

	// NOTE: Do not clear the error in the status on success. Success indicates 1 of 2 things:
	//   1. BucketAccess was granted successfully, and error was cleared in reconcile()
	//   2. BucketAccess deletion cleanup was finished, and finalization is now passed to Controller

	return reconcile.Result{}, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *BucketAccessReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// TODO: owns secrets, but don't reconcile secret changes made in this controller

	return ctrl.NewControllerManagedBy(mgr).
		For(&cosiapi.BucketAccess{}).
		WithEventFilter(
			ctrlpredicate.And(
				driverNameMatchesPredicate(r.DriverInfo.name), // only opt in to reconciles with matching driver name
				ctrlpredicate.Or(
					// when managed by sidecar, we should reconcile ALL Create/Delete/Generic events
					cosipredicate.AnyCreate(),
					cosipredicate.AnyDelete(),
					cosipredicate.AnyGeneric(),
					// opt in to desired Update events
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
	if access.Status.DriverName != r.DriverInfo.name {
		// keep this log to help debug any issues that might arise with predicate logic
		logger.Info("not reconciling bucketaccess with non-matching driver name", "driverName", access.Status.DriverName)
		return nil
	}

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
	if !initialized {
		// If we reach this condition, something is systemically wrong. Controller should have
		// ownership, but we determined otherwise, and the Controller will likely also determine us
		// to be the owner.
		logger.Error(nil, "processed a BucketAccess that should be managed by COSI Controller")
		return cosierr.NonRetryableError(fmt.Errorf("processed a BucketAccess that should be managed by COSI Controller"))
	}

	logger.V(1).Info("reconciling BucketAccess")

	didAdd := ctrlutil.AddFinalizer(access, cosiapi.ProtectionFinalizer)
	if didAdd {
		if err := r.Update(ctx, access); err != nil {
			logger.Error(err, "failed to add protection finalizer")
			return fmt.Errorf("failed to add protection finalizer: %w", err)
		}
	}

	bucketsByName, err := getAndValidateAllAccessedBuckets(ctx, r.Client, access)
	if err != nil {
		logger.Error(err, "failed to validate accessed Buckets for BucketAccess")
		return err
	}

	// Ensure COSI can write to user-selected Secrets before attempting access provisioning.
	// Avoids some hard-stop failures that might occur after RPC call is successful.
	secretsByName, err := r.reserveAccessSecrets(ctx, access)
	if err != nil {
		logger.Error(err, "failed to reserve access Secrets for BucketAccess")
		return err
	}

	internalCfg, err := newInternalAccessConfig(access, bucketsByName, secretsByName)
	if err != nil {
		logger.Error(err, "failed to build internal representation of access configuration")
		return fmt.Errorf("failed to build internal representation of access configuration: %w", err)
	}

	resp, err := r.DriverInfo.provisionerClient.DriverGrantBucketAccess(ctx,
		&cosiproto.DriverGrantBucketAccessRequest{
			AccountName:        internalCfg.AccountName,
			Protocol:           &cosiproto.ObjectProtocol{Type: internalCfg.Protocol},
			AuthenticationType: &cosiproto.AuthenticationType{Type: internalCfg.AuthenticationType},
			ServiceAccountName: internalCfg.ServiceAccountName,
			Parameters:         internalCfg.Parameters,
			Buckets:            internalCfg.RpcAccessedBucketsList(),
		},
	)
	if err != nil {
		if status.Code(err) == codes.OutOfRange {
			err = fmt.Errorf("driver does not support multi-bucket access: %w", err)
			logger.Error(err, "DriverGrantBucketAccess error")
			return cosierr.NonRetryableError(err)
		}

		logger.Error(err, "DriverGrantBucketAccess error")
		if rpcErrorIsRetryable(status.Code(err)) {
			return err
		}
		return cosierr.NonRetryableError(err)
	}

	validation := validationConfig{
		ExpectedProtocol:   access.Spec.Protocol,
		AuthenticationType: access.Status.AuthenticationType,
	}
	grantDetails, err := translateDriverGrantBucketAccessResponseToApi(resp, &validation)
	if err != nil {
		logger.Error(err, "failed processing BucketAccess RPC response")
		return cosierr.NonRetryableError(err)
	}

	if err := validateGrantedAccess(internalCfg, grantDetails); err != nil {
		logger.Error(err, "granted BucketAccess is invalid")
		return cosierr.NonRetryableError(err)
	}

	if err := r.updateSecretsWithGrantedInfo(ctx, internalCfg, grantDetails); err != nil {
		logger.Error(err, "failed to update BucketAccess Secret(s)")
		return err
	}

	access.Status.AccountID = grantDetails.AccountId
	access.Status.ReadyToUse = ptr.To(true)
	access.Status.Error = nil
	if err := r.Status().Update(ctx, access); err != nil {
		logger.Error(err, "failed to update BucketAccess status after successful access grant")
		return fmt.Errorf("failed to update BucketAccess status after successful access grant: %w", err)
	}

	return nil
}

// Internal representation of access configuration.
type internalAccessConfig struct {
	AccountName        string
	Protocol           cosiproto.ObjectProtocol_Type
	AuthenticationType cosiproto.AuthenticationType_Type
	ServiceAccountName string
	Parameters         map[string]string

	AccessConfigsByBucketId map[string]bucketAccessConfig

	BucketsByName map[string]*cosiapi.Bucket
	SecretsByName map[string]*corev1.Secret
}

// Internal access configuration for a specific bucket.
type bucketAccessConfig struct {
	AccessMode       cosiproto.AccessMode_Mode
	AccessSecretName string
}

// Parse the access, and generate a new internal access config struct for follow-up.
func newInternalAccessConfig(
	access *cosiapi.BucketAccess,
	bucketsByName map[string]*cosiapi.Bucket,
	secretsByName map[string]*corev1.Secret,
) (*internalAccessConfig, error) {
	acctName := "ba-" + string(access.UID) // DO NOT CHANGE

	proto, err := protocol.ObjectProtocolTranslator{}.ApiToRpc(access.Spec.Protocol)
	if err != nil {
		return nil, cosierr.NonRetryableError(err)
	}

	authType, err := authenticationTypeToRpc(access.Status.AuthenticationType)
	if err != nil {
		return nil, cosierr.NonRetryableError(err)
	}

	// Only use ServiceAccount name in the driver RPC request if auth type is ServiceAccount
	svcAcct := ""
	if authType == cosiproto.AuthenticationType_SERVICE_ACCOUNT {
		svcAcct = access.Spec.ServiceAccountName
	}

	accessConfigsByBucketId, err := generateInternalAccessedBucketConfigs(access, bucketsByName)
	if err != nil {
		return nil, err
	}

	d := &internalAccessConfig{
		AccountName:        acctName,
		Protocol:           proto,
		AuthenticationType: authType,
		ServiceAccountName: svcAcct,
		Parameters:         access.Status.Parameters,

		AccessConfigsByBucketId: accessConfigsByBucketId,
		BucketsByName:           bucketsByName,
		SecretsByName:           secretsByName,
	}
	return d, nil
}

// Parse the referenced BucketClaims and accessed Buckets, then cross-reference and collate the info
// into a form that makes internal operations easier.
// The implementation uses maps to simplify lookups and avoid having to search slices for entries
// repeatedly. This is less for efficiency and more for ease of coding.
func generateInternalAccessedBucketConfigs(
	access *cosiapi.BucketAccess,
	bucketsByName map[string]*cosiapi.Bucket,
) (accessConfigsByBucketId map[string]bucketAccessConfig, err error) {
	errs := []error{}

	bucketNamesByClaimName := make(map[string]string, len(access.Status.AccessedBuckets))
	for _, ab := range access.Status.AccessedBuckets {
		bucketNamesByClaimName[ab.BucketClaimName] = ab.BucketName
	}

	accessConfigsByBucketId = make(map[string]bucketAccessConfig, len(access.Spec.BucketClaims))
	for _, claimRef := range access.Spec.BucketClaims {
		claimName := claimRef.BucketClaimName
		bucketName, ok := bucketNamesByClaimName[claimName]
		if !ok {
			// Should not happen as long as COSI Controller created status.accessedBuckets correctly.
			errs = append(errs, fmt.Errorf("could not map BucketClaim %q to any accessed Bucket", claimName))
			continue
		}

		bucket, ok := bucketsByName[bucketName]
		if !ok {
			// Should not happen except internal bugs in this controller.
			errs = append(errs, fmt.Errorf("could not find Bucket %q by name internally", bucketName))
			continue
		}

		id := bucket.Status.BucketID
		if id == "" {
			// Already checked for this, but double check.
			//nolint:staticcheck // ST1005: okay to capitalize resource kind
			errs = append(errs, fmt.Errorf("Bucket %q has no bucketID", bucketName))
			continue
		}

		rpcMode, err := accessModeToRpc(claimRef.AccessMode)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to parse access mode for BucketClaim %q", claimRef.BucketClaimName))
			continue
		}

		cfg := bucketAccessConfig{
			AccessMode:       rpcMode,
			AccessSecretName: claimRef.AccessSecretName,
		}

		accessConfigsByBucketId[id] = cfg
	}

	if len(errs) > 0 {
		return nil, cosierr.NonRetryableError( // Retry won't resolve any of these issues
			fmt.Errorf("failed to generate internal access configuration: %w", errors.Join(errs...)))
	}
	return accessConfigsByBucketId, nil
}

// Generate bucket access configs for the RPC call.
func (d *internalAccessConfig) RpcAccessedBucketsList() []*cosiproto.DriverGrantBucketAccessRequest_AccessedBucket {
	out := make([]*cosiproto.DriverGrantBucketAccessRequest_AccessedBucket, len(d.AccessConfigsByBucketId))

	i := 0
	for id, cfg := range d.AccessConfigsByBucketId {
		// Map iteration order in go is not predictable. This means the returned output order will
		// differ between repeated reconciles. This is desirable because it ensures that drivers
		// must not implicitly rely on element ordering.
		out[i] = &cosiproto.DriverGrantBucketAccessRequest_AccessedBucket{
			BucketId: id,
			AccessMode: &cosiproto.AccessMode{
				Mode: cfg.AccessMode,
			},
		}
		i++
	}

	return out
}

// Internal API-domain details about a successfully-granted access.
type grantedAccessApiDetails struct {
	AccountId            string
	SharedCredentialInfo map[string]string
	BucketInfoByBucketId map[string]map[string]string
}

// Translate an RPC grant-access response to internal API-domain details.
func translateDriverGrantBucketAccessResponseToApi(
	resp *cosiproto.DriverGrantBucketAccessResponse,
	validation *validationConfig,
) (*grantedAccessApiDetails, error) {
	errs := []error{}

	if resp.AccountId == "" {
		errs = append(errs, fmt.Errorf("missing account ID"))
	}

	credInfo, err := TranslateCredentialsToApi(resp.Credentials, *validation)
	if err != nil {
		errs = append(errs, fmt.Errorf("shared credentials are invalid: %w", err))
	}

	bucketInfoByBucketId := map[string]map[string]string{}
	for i, accessBktInfo := range resp.Buckets {
		id := accessBktInfo.BucketId
		if id == "" {
			errs = append(errs, fmt.Errorf("missing bucket ID at index %d", i))
			continue
		}

		_, info, err := TranslateBucketInfoToApi(accessBktInfo.BucketInfo, validation)
		if err != nil {
			errs = append(errs, fmt.Errorf("invalid bucket info at index %d: %w", i, err))
			continue
		}

		bucketInfoByBucketId[id] = info
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("granted access response is invalid: %w", errors.Join(errs...))
	}

	d := &grantedAccessApiDetails{
		AccountId:            resp.AccountId, // DO NOT ALTER RESPONSE
		SharedCredentialInfo: credInfo,
		BucketInfoByBucketId: bucketInfoByBucketId,
	}
	return d, nil
}

// Even with a granted access response that translates successfully, there could be other errors
// related to the granted access not matching what was requested.
func validateGrantedAccess(internalCfg *internalAccessConfig, granted *grantedAccessApiDetails) error {
	errs := []error{}

	for bucketId := range internalCfg.AccessConfigsByBucketId {
		if _, ok := granted.BucketInfoByBucketId[bucketId]; !ok {
			errs = append(errs, fmt.Errorf("granted access missing for bucket ID %q", bucketId))
		}
	}

	for bucketId := range granted.BucketInfoByBucketId {
		if _, ok := internalCfg.AccessConfigsByBucketId[bucketId]; !ok {
			errs = append(errs, fmt.Errorf("granted access to unknown bucket with ID %q", bucketId))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("granted access is invalid: %w", errors.Join(errs...))
	}
	return nil
}

// Update BucketAccess Secret(s)'s data fields with credential and bucket info.
func (r *BucketAccessReconciler) updateSecretsWithGrantedInfo(
	ctx context.Context,
	internalCfg *internalAccessConfig,
	granted *grantedAccessApiDetails,
) error {
	errs := []error{}

	for bucketId, bucketInfo := range granted.BucketInfoByBucketId {
		cfg, ok := internalCfg.AccessConfigsByBucketId[bucketId]
		if !ok {
			// Should not happen, as checked in validateGrantedAccess()
			errs = append(errs,
				cosierr.NonRetryableError(fmt.Errorf("unknown bucket ID %q", bucketId)))
			continue
		}

		sec, ok := internalCfg.SecretsByName[cfg.AccessSecretName]
		if !ok {
			// Should not happen except developer error in this controller.
			errs = append(errs,
				cosierr.NonRetryableError(fmt.Errorf("failed internal lookup for Secret with name %q", cfg.AccessSecretName)))
			continue
		}

		data := map[string]string{}
		mergeApiInfoIntoStringMap(granted.SharedCredentialInfo, data)
		mergeApiInfoIntoStringMap(bucketInfo, data)
		sec.StringData = data

		if err := r.Update(ctx, sec); err != nil {
			errs = append(errs, fmt.Errorf("failed to update BucketAccess Secret %q with bucket and credential info", sec.Name))
			continue
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to update one or more BucketAccess Secrets: %w", errors.Join(errs...))
	}
	return nil
}

// Get all Buckets from BucketAccess status. Error if any Bucket Get() fails.
// Validate that all gotten buckets are ready for access.
func getAndValidateAllAccessedBuckets(
	ctx context.Context, client client.Client, access *cosiapi.BucketAccess,
) (bucketsByName map[string]*cosiapi.Bucket, err error) {
	errs := []error{}
	bucketsByName = map[string]*cosiapi.Bucket{}

	for _, ab := range access.Status.AccessedBuckets {
		nsName := types.NamespacedName{
			Namespace: "", // global resource
			Name:      ab.BucketName,
		}

		bkt := &cosiapi.Bucket{}
		err := client.Get(ctx, nsName, bkt)
		if err != nil {
			if kerrors.IsNotFound(err) {
				errs = append(errs, cosierr.NonRetryableError(err))
				continue
			}

			// other errors will likely resolve
			errs = append(errs, err)
			continue
		}

		if err := validateBucketIsReadyForAccess(bkt); err != nil {
			errs = append(errs, cosierr.NonRetryableError(err))
			continue
		}

		bucketsByName[bkt.Name] = bkt
	}

	if len(errs) > 0 {
		outErr := fmt.Errorf("failed to get accessed Buckets: %w", errors.Join(errs...))
		return nil, outErr
	}

	if len(bucketsByName) != len(access.Status.AccessedBuckets) {
		// Should never happen, but double check to avoid propagating internal errors.
		return nil, fmt.Errorf("did not get one or more accessed Buckets, but no errors observed")
	}

	return bucketsByName, nil
}

func validateBucketIsReadyForAccess(b *cosiapi.Bucket) error {
	errs := []error{}

	if _, ok := b.Annotations[cosiapi.BucketClaimBeingDeletedAnnotation]; ok {
		//nolint:staticcheck // ST1005: okay to capitalize resource kind
		errs = append(errs, fmt.Errorf("BucketClaim for Bucket %q is deleting", b.Name))
	}

	if !b.DeletionTimestamp.IsZero() {
		//nolint:staticcheck // ST1005: okay to capitalize resource kind
		errs = append(errs, fmt.Errorf("Bucket %q is deleting", b.Name))
	}

	if b.Status.BucketID == "" {
		//nolint:staticcheck // ST1005: okay to capitalize resource kind
		errs = append(errs, fmt.Errorf("Bucket %q has no bucketID", b.Name))
	}

	// TODO: Controller has already verified that the Bucket supports the requested protocol.
	// Do we need/want to check that again?

	if len(errs) > 0 {
		return fmt.Errorf("cannot generate access for one or more Buckets: %w", errors.Join(errs...))
	}
	return nil
}

// Before access is provisioned, reserve all spec.bucketClaims access Secrets by creating new ones
// controller-owned by the BucketAccess. Additionally, update existing Secret metadata as needed.
func (r *BucketAccessReconciler) reserveAccessSecrets(
	ctx context.Context, access *cosiapi.BucketAccess,
) (secretsByName map[string]*corev1.Secret, err error) {
	errs := []error{}
	secretsByName = map[string]*corev1.Secret{}

	for _, claimRef := range access.Spec.BucketClaims {
		secretName := claimRef.AccessSecretName

		if _, ok := secretsByName[secretName]; ok {
			errs = append(errs, cosierr.NonRetryableError(
				fmt.Errorf("multiple referenced BucketClaims use the same accessSecretName %q", secretName)))
			continue
		}

		nsName := types.NamespacedName{
			Namespace: access.Namespace,
			Name:      secretName,
		}
		existingSecret := &corev1.Secret{}
		err := r.Get(ctx, nsName, existingSecret)
		if err != nil {
			if !kerrors.IsNotFound(err) {
				errs = append(errs, err)
				continue
			}

			newSecret, err := r.createNewOwnedAccessSecret(ctx, r.Client, secretName, access)
			if err != nil {
				errs = append(errs, err)
				continue
			}

			secretsByName[secretName] = newSecret
			continue
		}

		if err := r.updateExistingAccessSecretMetadata(ctx, existingSecret, access); err != nil {
			errs = append(errs, err)
			continue
		}

		secretsByName[secretName] = existingSecret
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to reserve one or more Secrets for access info: %w", errors.Join(errs...))
	}

	if len(secretsByName) != len(access.Spec.BucketClaims) {
		// Should never happen, but double check to avoid propagating internal errors.
		return nil, fmt.Errorf("did not reserve one or more access Secrets, but no errors observed")
	}

	return secretsByName, nil
}

// Create a new BucketAccess access Secret without setting data fields.
func (r *BucketAccessReconciler) createNewOwnedAccessSecret(
	ctx context.Context, client client.Client,
	secretName string, owningAccess *cosiapi.BucketAccess,
) (*corev1.Secret, error) {
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: owningAccess.Namespace,
			Name:      secretName,
		},
	}

	// Controller reference identifies the Secret as being controlled exclusively by the BucketAccess
	err := ctrlutil.SetControllerReference(owningAccess, s, r.Scheme, ctrlutil.WithBlockOwnerDeletion(true))
	if err != nil {
		return nil, err
	}

	setAccessSecretRequiredMetadata(s)

	err = client.Create(ctx, s)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// Update an existing BucketAccess access Secret metadata to ensure it matches latest expectations.
// For example, add the protection finalizer if it is removed.
func (r *BucketAccessReconciler) updateExistingAccessSecretMetadata(
	ctx context.Context,
	secret *corev1.Secret,
	owningAccess *cosiapi.BucketAccess,
) error {
	if !metav1.IsControlledBy(secret, owningAccess) {
		// Do not modify Secrets aren't controlled by this BucketAccess
		return cosierr.NonRetryableError(
			fmt.Errorf("existing access Secret %q does not belong to BucketAccess", secret.Name))
	}

	setAccessSecretRequiredMetadata(secret)

	if err := r.Update(ctx, secret); err != nil {
		return err
	}

	return nil
}

// Set required metadata on a BucketAccess's access Secret.
// Importantly (but not exclusively), ensure the protection finalizer is present.
func setAccessSecretRequiredMetadata(secret *corev1.Secret) {
	ctrlutil.AddFinalizer(secret, cosiapi.ProtectionFinalizer)

	// TODO: Consider using Secret type to help hint about COSI usage and the object protocol type?
	secret.Type = corev1.SecretTypeOpaque
}
