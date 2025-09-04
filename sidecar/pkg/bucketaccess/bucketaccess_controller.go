/* Copyright 2021 The Kubernetes Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bucketaccess

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kube "k8s.io/client-go/kubernetes"
	kubecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/consts"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	buckets "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned"
	bucketapi "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned/typed/objectstorage/v1alpha1"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// BucketAccessListener manages Bucket objects
type BucketAccessListener struct {
	provisionerClient cosi.ProvisionerClient
	driverName        string

	eventRecorder record.EventRecorder

	kubeClient   kube.Interface
	bucketClient buckets.Interface
}

// NewBucketAccessListener returns a resource handler for BucketAccess objects
func NewBucketAccessListener(driverName string, client cosi.ProvisionerClient) *BucketAccessListener {
	return &BucketAccessListener{
		driverName:        driverName,
		provisionerClient: client,
	}
}

// Add attempts to provision credentials to access a given bucket. This function must be idempotent
//
// Return values
//   - nil - BucketAccess successfully granted
//   - non-nil err - Internal error                                [requeue'd with exponential backoff]
func (bal *BucketAccessListener) Add(ctx context.Context, inputBucketAccess *v1alpha1.BucketAccess) error {
	bucketAccess := inputBucketAccess.DeepCopy()

	if bucketAccess.Status.AccessGranted && bucketAccess.Status.AccountID != "" {
		klog.V(3).InfoS("BucketAccess already exists", bucketAccess.ObjectMeta.Name)
		return nil
	}

	bucketClaimName := bucketAccess.Spec.BucketClaimName
	klog.V(3).InfoS("Add BucketAccess",
		"name", bucketAccess.ObjectMeta.Name,
		"bucketClaim", bucketClaimName,
	)

	bucketAccessClassName := bucketAccess.Spec.BucketAccessClassName
	klog.V(3).InfoS("Add BucketAccess",
		"name", bucketAccess.ObjectMeta.Name,
		"BucketAccessClassName", bucketAccessClassName,
	)

	secretCredName := bucketAccess.Spec.CredentialsSecretName
	if secretCredName == "" {
		return consts.ErrUndefinedSecretName
	}

	bucketAccessClass, err := bal.bucketAccessClasses().Get(ctx, bucketAccessClassName, metav1.GetOptions{})
	if kubeerrors.IsNotFound(err) {
		return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess, err)
	} else if err != nil {
		klog.ErrorS(err, "Failed to fetch bucketAccessClass", "bucketAccessClass", bucketAccessClassName)
		return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
			fmt.Errorf("failed to fetch BucketAccessClass: %w", err))
	}

	if !strings.EqualFold(bucketAccessClass.DriverName, bal.driverName) {
		klog.V(5).InfoS("Skipping bucketaccess for driver",
			"bucketAccess", bucketAccess.ObjectMeta.Name,
			"driver", bucketAccessClass.DriverName,
		)
		return nil
	}

	namespace := bucketAccess.ObjectMeta.Namespace
	bucketClaim, err := bal.bucketClaims(namespace).Get(ctx, bucketClaimName, metav1.GetOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "Failed to fetch bucketClaim", "bucketClaim", bucketClaimName)
		return fmt.Errorf("failed to fetch bucketClaim: %w", err)
	}

	if bucketClaim.Status.BucketName == "" || bucketClaim.Status.BucketReady != true {
		err := consts.ErrInvalidBucketState
		klog.V(3).ErrorS(err,
			"Invalid arguments",
			"bucketClaim", bucketClaim.Name,
			"bucketAccess", bucketAccess.ObjectMeta.Name,
		)
		return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.WaitingForBucket,
			fmt.Errorf("invalid bucket state: %w", err))
	}

	authType := cosi.AuthenticationType_UnknownAuthenticationType
	if bucketAccessClass.AuthenticationType == v1alpha1.AuthenticationTypeKey {
		authType = cosi.AuthenticationType_Key
	} else if bucketAccessClass.AuthenticationType == v1alpha1.AuthenticationTypeIAM {
		authType = cosi.AuthenticationType_IAM
	}

	if authType == cosi.AuthenticationType_IAM && bucketAccess.Spec.ServiceAccountName == "" {
		err = consts.ErrUndefinedServiceAccountName
		return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess, err)
	}

	if bucketAccess.Status.AccessGranted == true {
		klog.V(5).InfoS("AccessAlreadyGranted",
			"bucketAccess", bucketAccess.ObjectMeta.Name,
			"bucketClaim", bucketClaimName,
		)
		return nil
	}

	bucket, err := bal.buckets().Get(ctx, bucketClaim.Status.BucketName, metav1.GetOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "Failed to fetch bucket", "bucket", bucketClaim.Status.BucketName)
		return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
			fmt.Errorf("failed to fetch bucket: %w", err))
	}

	if bucket.Status.BucketReady != true || bucket.Status.BucketID == "" {
		err = fmt.Errorf("%w: (isReady? %t), (ID empty? %t)",
			consts.ErrInvalidBucketState,
			bucket.Status.BucketReady,
			bucket.Status.BucketID == "")
		return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.WaitingForBucket, err)
	}

	accountName := consts.AccountNamePrefix + string(bucketAccess.UID)

	req := &cosi.DriverGrantBucketAccessRequest{
		BucketId:           bucket.Status.BucketID,
		Name:               accountName,
		AuthenticationType: authType,
		Parameters:         bucketAccessClass.Parameters,
	}

	// This needs to be idempotent
	rsp, err := bal.provisionerClient.DriverGrantBucketAccess(ctx, req)
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
				fmt.Errorf("failed to grant bucket access: %w", err))
		}
	}

	if rsp.AccountId == "" {
		err = consts.ErrUndefinedAccountID
		klog.V(3).ErrorS(err, "BucketAccess", bucketAccess.ObjectMeta.Name)
		return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
			fmt.Errorf("BucketAccess %s: %w", bucketAccess.ObjectMeta.Name, err))
	}

	credentials := rsp.Credentials
	if len(credentials) != 1 {
		err = consts.ErrInvalidCredentials
		klog.V(3).ErrorS(err, "BucketAccess", bucketAccess.ObjectMeta.Name)
		return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
			fmt.Errorf("BucketAccess %s: %w", bucketAccess.ObjectMeta.Name, err))
	}

	bucketInfoName := consts.BucketInfoPrefix + string(bucketAccess.ObjectMeta.UID)

	bucketInfo := cosiapi.BucketInfo{
		ObjectMeta: metav1.ObjectMeta{
			Name: bucketInfoName,
		},
		Spec: cosiapi.BucketInfoSpec{
			BucketName:         bucket.ObjectMeta.Name,
			AuthenticationType: bucketAccessClass.AuthenticationType,
			Protocols:          []v1alpha1.Protocol{bucketAccess.Spec.Protocol},
		},
	}

	var val *cosi.CredentialDetails
	var ok bool

	if val, ok = credentials[consts.S3Key]; ok {
		secretS3 := &cosiapi.SecretS3{
			Endpoint:        val.Secrets[consts.S3Endpoint],
			Region:          val.Secrets[consts.S3Region],
			AccessKeyID:     val.Secrets[consts.S3SecretAccessKeyID],
			AccessSecretKey: val.Secrets[consts.S3SecretAccessSecretKey],
		}

		bucketInfo.Spec.S3 = secretS3
	} else if val, ok = credentials[consts.AzureKey]; ok {
		expiryTs := val.Secrets[consts.AzureSecretExpiryTimeStamp]
		expiryTimestamp, _ := time.Parse(consts.DefaultTimeFormat, expiryTs)
		metav1Time := &metav1.Time{Time: expiryTimestamp}
		secretAzure := &cosiapi.SecretAzure{
			AccessToken:     val.Secrets[consts.AzureSecretAccessToken],
			ExpiryTimeStamp: metav1Time,
		}

		bucketInfo.Spec.Azure = secretAzure
	}

	stringData, err := json.Marshal(bucketInfo)
	if err != nil {
		return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess, consts.ErrBucketInfoConversionFailed)
	}

	if _, err := bal.secrets(namespace).Get(ctx, secretCredName, metav1.GetOptions{}); err != nil {
		if !kubeerrors.IsNotFound(err) {
			klog.V(3).ErrorS(err,
				"Failed to create secrets",
				"bucketAccess", bucketAccess.ObjectMeta.Name,
				"bucket", bucket.ObjectMeta.Name)
			return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
				fmt.Errorf("failed to fetch secrets: %w", err))
		}

		if _, err := bal.secrets(namespace).Create(ctx, &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:       secretCredName,
				Namespace:  namespace,
				Finalizers: []string{consts.SecretFinalizer},
			},
			StringData: map[string]string{
				"BucketInfo": string(stringData),
			},
			Type: v1.SecretTypeOpaque,
		}, metav1.CreateOptions{}); err != nil {
			if !kubeerrors.IsAlreadyExists(err) {
				klog.V(3).ErrorS(err,
					"Failed to create minted secret",
					"bucketAccess", bucketAccess.ObjectMeta.Name,
					"bucket", bucket.ObjectMeta.Name)
				return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
					fmt.Errorf("failed to create minted secret: %w", err))
			}
		}
	}

	if controllerutil.AddFinalizer(bucket, consts.BABucketFinalizer) {
		_, err = bal.buckets().Update(ctx, bucket, metav1.UpdateOptions{})
		if err != nil {
			return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess, err)
		}
	}

	if controllerutil.AddFinalizer(bucketAccess, consts.BAFinalizer) {
		bucketAccess, err = bal.bucketAccesses(bucketAccess.ObjectMeta.Namespace).Update(ctx, bucketAccess, metav1.UpdateOptions{})
		if err != nil {
			klog.V(3).ErrorS(err, "Failed to update BucketAccess finalizer",
				"bucketAccess", bucketAccess.ObjectMeta.Name,
				"bucket", bucket.ObjectMeta.Name)
			return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
				fmt.Errorf("failed to update finalizer on BucketAccess %s: %w", bucketAccess.ObjectMeta.Name, err))
		}
	}

	bucketAccess.Status.AccountID = rsp.AccountId
	bucketAccess.Status.AccessGranted = true

	// if this step fails, then controller will retry with backoff
	if _, err := bal.bucketAccesses(bucketAccess.ObjectMeta.Namespace).UpdateStatus(ctx, bucketAccess, metav1.UpdateOptions{}); err != nil {
		klog.V(3).ErrorS(err, "Failed to update BucketAccess Status",
			"bucketAccess", bucketAccess.ObjectMeta.Name,
			"bucket", bucket.ObjectMeta.Name)
		return bal.recordError(inputBucketAccess, v1.EventTypeWarning, v1alpha1.FailedGrantAccess,
			fmt.Errorf("failed to update Status on BucketAccess %s: %w", bucketAccess.ObjectMeta.Name, err))
	}

	return nil
}

// Update attempts to reconcile changes to a given bucketAccess. This function must be idempotent
// Return values
//   - nil - BucketAccess successfully reconciled
//   - non-nil err - Internal error                                [requeue'd with exponential backoff]
func (bal *BucketAccessListener) Update(ctx context.Context, old, new *v1alpha1.BucketAccess) error {
	klog.V(3).InfoS("Update BucketAccess",
		"name", old.ObjectMeta.Name)

	bucketAccess := new.DeepCopy()
	if !bucketAccess.GetDeletionTimestamp().IsZero() {
		err := bal.deleteBucketAccessOp(ctx, bucketAccess)
		if err != nil {
			return bal.recordError(bucketAccess, v1.EventTypeWarning, v1alpha1.FailedRevokeAccess, err)
		}
	}

	klog.V(3).InfoS("Update BucketAccess success",
		"name", old.ObjectMeta.Name)
	return nil
}

// Delete attemps to delete a bucketAccess. This function must be idempotent
// Return values
//   - nil - BucketAccess successfully deleted
//   - non-nil err - Internal error                                [requeue'd with exponential backoff]
func (bal *BucketAccessListener) Delete(ctx context.Context, bucketAccess *v1alpha1.BucketAccess) error {
	klog.V(3).InfoS("Delete BucketAccess",
		"name", bucketAccess.ObjectMeta.Name,
		"bucketClaim", bucketAccess.Spec.BucketClaimName,
	)

	return nil
}

func (bal *BucketAccessListener) deleteBucketAccessOp(ctx context.Context, bucketAccess *v1alpha1.BucketAccess) error {
	// Fetching bucketClaim and corresponding bucket to get the bucketID
	// for performing DriverRevokeBucketAccess request.
	bucketClaimName := bucketAccess.Spec.BucketClaimName
	bucketClaim, err := bal.bucketClaims(bucketAccess.ObjectMeta.Namespace).Get(ctx, bucketClaimName, metav1.GetOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "Failed to fetch bucketClaim", "bucketClaim", bucketClaimName)
		return fmt.Errorf("failed to fetch bucketClaim: %w", err)
	}

	bucket, err := bal.buckets().Get(ctx, bucketClaim.Status.BucketName, metav1.GetOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "Failed to fetch bucket", "bucket", bucketClaim.Status.BucketName)
		return fmt.Errorf("failed to fetch bucket: %w", err)
	}

	req := &cosi.DriverRevokeBucketAccessRequest{
		BucketId:  bucket.Status.BucketID,
		AccountId: bucketAccess.Status.AccountID,
	}

	// First we revoke the bucketAccess from the driver
	if _, err := bal.provisionerClient.DriverRevokeBucketAccess(ctx, req); err != nil {
		return fmt.Errorf("failed to revoke access: %w", err)
	}

	credSecretName := bucketAccess.Spec.CredentialsSecretName
	secret, err := bal.secrets(bucketAccess.ObjectMeta.Namespace).Get(ctx, credSecretName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if controllerutil.RemoveFinalizer(secret, consts.SecretFinalizer) {
		_, err = bal.secrets(secret.ObjectMeta.Namespace).Update(ctx, secret, metav1.UpdateOptions{})
		if err != nil {
			klog.V(3).ErrorS(err, "Error removing finalizer from secret",
				"secret", secret.ObjectMeta.Name,
				"bucketAccess", bucketAccess.ObjectMeta.Name)
			return err
		}

		klog.V(5).Infof("Successfully removed finalizer from secret: %s, bucketAccess: %s", secret.ObjectMeta.Name, bucketAccess.ObjectMeta.Name)
	}

	err = bal.secrets(secret.ObjectMeta.Namespace).Delete(ctx, credSecretName, metav1.DeleteOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "Error deleting secret",
			"secret", secret.ObjectMeta.Name,
			"bucketAccess", bucketAccess.ObjectMeta.Name,
			"ns", bucketAccess.ObjectMeta.Namespace)
		return nil
	}

	if controllerutil.RemoveFinalizer(bucketAccess, consts.BAFinalizer) {
		_, err = bal.bucketAccesses(bucketAccess.ObjectMeta.Namespace).Update(ctx, bucketAccess, metav1.UpdateOptions{})
		if err != nil {
			klog.V(3).ErrorS(err, "Error removing finalizer from bucketAccess",
				"bucketAccess", bucketAccess.ObjectMeta.Name)
			return err
		}

		klog.V(5).Infof("Successfully removed finalizer from bucketAccess: %s", bucketAccess.ObjectMeta.Name)
	}

	return nil
}

func (bal *BucketAccessListener) secrets(ns string) kubecorev1.SecretInterface {
	if bal.kubeClient != nil {
		return bal.kubeClient.CoreV1().Secrets(ns)
	}
	panic("uninitialized listener")
}

func (bal *BucketAccessListener) bucketAccesses(ns string) bucketapi.BucketAccessInterface {
	if bal.bucketClient != nil {
		return bal.bucketClient.ObjectstorageV1alpha1().BucketAccesses(ns)
	}
	panic("uninitialized listener")
}

func (bal *BucketAccessListener) buckets() bucketapi.BucketInterface {
	if bal.bucketClient != nil {
		return bal.bucketClient.ObjectstorageV1alpha1().Buckets()
	}
	panic("uninitialized listener")
}

func (bal *BucketAccessListener) bucketClaims(namespace string) bucketapi.BucketClaimInterface {
	if bal.bucketClient != nil {
		return bal.bucketClient.ObjectstorageV1alpha1().BucketClaims(namespace)
	}
	panic("uninitialized listener")
}

func (bal *BucketAccessListener) bucketAccessClasses() bucketapi.BucketAccessClassInterface {
	if bal.bucketClient != nil {
		return bal.bucketClient.ObjectstorageV1alpha1().BucketAccessClasses()
	}
	panic("uninitialized listener")
}

// InitializeKubeClient initializes the kubernetes client
func (bal *BucketAccessListener) InitializeKubeClient(k kube.Interface) {
	bal.kubeClient = k
}

// InitializeBucketClient initializes the object storage bucket client
func (bal *BucketAccessListener) InitializeBucketClient(bc buckets.Interface) {
	bal.bucketClient = bc
}

// InitializeEventRecorder initializes the event recorder
func (bal *BucketAccessListener) InitializeEventRecorder(er record.EventRecorder) {
	bal.eventRecorder = er
}

// recordError during the processing of the objects
func (b *BucketAccessListener) recordError(subject runtime.Object, eventtype, reason string, err error) error {
	if b.eventRecorder == nil {
		return err
	}
	b.eventRecorder.Event(subject, eventtype, reason, err.Error())

	return err
}

// recordEvent during the processing of the objects
func (bal *BucketAccessListener) recordEvent(subject runtime.Object, eventtype, reason, message string, args ...any) {
	if bal.eventRecorder == nil {
		return
	}
	bal.eventRecorder.Event(subject, eventtype, reason, fmt.Sprintf(message, args...))
}
