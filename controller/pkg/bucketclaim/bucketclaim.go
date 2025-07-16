package bucketclaim

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	bucketclientset "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned"
	objectstoragev1alpha1 "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned/typed/objectstorage/v1alpha1"
	"sigs.k8s.io/container-object-storage-interface/controller/pkg/util"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// BucketClaimListener is a resource handler for bucket requests objects
type BucketClaimListener struct {
	eventRecorder record.EventRecorder

	kubeClient   kubeclientset.Interface
	bucketClient bucketclientset.Interface
}

func NewBucketClaimListener() *BucketClaimListener {
	return &BucketClaimListener{}
}

// Add creates a bucket in response to a bucketClaim
func (b *BucketClaimListener) Add(ctx context.Context, bucketClaim *v1alpha1.BucketClaim) error {
	if !bucketClaim.GetDeletionTimestamp().IsZero() {
		return b.handleDeletion(ctx, bucketClaim)
	}

	klog.V(3).InfoS("Add BucketClaim",
		"name", bucketClaim.ObjectMeta.Name,
		"ns", bucketClaim.ObjectMeta.Namespace,
		"bucketClass", bucketClaim.Spec.BucketClassName,
	)

	err := b.provisionBucketClaimOperation(ctx, bucketClaim)
	if err != nil {
		switch err {
		case util.ErrInvalidBucketClass:
			klog.V(3).ErrorS(err,
				"bucketClaim", bucketClaim.ObjectMeta.Name,
				"ns", bucketClaim.ObjectMeta.Namespace,
				"bucketClassName", bucketClaim.Spec.BucketClassName)
		case util.ErrBucketAlreadyExists:
			klog.V(3).InfoS("Bucket already exists",
				"bucketClaim", bucketClaim.ObjectMeta.Name,
				"ns", bucketClaim.ObjectMeta.Namespace,
			)
			return nil
		default:
			klog.V(3).ErrorS(err,
				"name", bucketClaim.ObjectMeta.Name,
				"ns", bucketClaim.ObjectMeta.Namespace,
				"err", err)
		}
		return err
	}

	klog.V(3).InfoS("Add BucketClaim success",
		"name", bucketClaim.ObjectMeta.Name,
		"ns", bucketClaim.ObjectMeta.Namespace)
	return nil
}

// update processes any updates  made to the bucket request
func (b *BucketClaimListener) Update(ctx context.Context, old, new *v1alpha1.BucketClaim) error {
	klog.V(3).InfoS("Update BucketClaim",
		"name", old.Name,
		"ns", old.Namespace)

	bucketClaim := new.DeepCopy()

	if !new.GetDeletionTimestamp().IsZero() {
		return b.handleDeletion(ctx, bucketClaim)
	}

	if err := b.Add(ctx, bucketClaim); err != nil {
		return err
	}

	klog.V(3).InfoS("Update BucketClaim success",
		"name", bucketClaim.ObjectMeta.Name,
		"ns", bucketClaim.ObjectMeta.Namespace)
	return nil
}

// handleDeletion processes the deletion of a bucketClaim.
func (b *BucketClaimListener) handleDeletion(ctx context.Context, bucketClaim *v1alpha1.BucketClaim) error {
	if !controllerutil.ContainsFinalizer(bucketClaim, util.BucketClaimFinalizer) {
		return nil
	}

	bucketName := bucketClaim.Status.BucketName
	if bucketName != "" {
		if err := b.buckets().Delete(ctx, bucketName, metav1.DeleteOptions{}); err != nil && !kubeerrors.IsNotFound(err) {
			klog.V(3).ErrorS(err, "Error deleting bucket",
				"bucket", bucketName,
				"bucketClaim", bucketClaim.ObjectMeta.Name)
			return b.recordError(bucketClaim, v1.EventTypeWarning, v1alpha1.FailedDeleteBucket, err)
		}
		klog.V(5).Infof("Successfully requested deletion of bucket: %s for bucketClaim: %s",
			bucketName, bucketClaim.ObjectMeta.Name)
	}

	return nil
}

// Delete processes a bucket for which bucket request is deleted
func (b *BucketClaimListener) Delete(ctx context.Context, bucketClaim *v1alpha1.BucketClaim) error {
	klog.V(3).InfoS("Delete BucketClaim",
		"name", bucketClaim.ObjectMeta.Name,
		"ns", bucketClaim.ObjectMeta.Namespace)

	return nil
}

// provisionBucketClaimOperation attempts to provision a bucket for a given bucketClaim.
//
// Return values
//   - nil - BucketClaim successfully processed
//   - ErrInvalidBucketClass - BucketClass does not exist          [requeue'd with exponential backoff]
//   - ErrBucketAlreadyExists - BucketClaim already processed
//   - non-nil err - Internal error                                [requeue'd with exponential backoff]
func (b *BucketClaimListener) provisionBucketClaimOperation(ctx context.Context, inputBucketClaim *v1alpha1.BucketClaim) error {
	bucketClaim := inputBucketClaim.DeepCopy()
	if bucketClaim.Status.BucketReady {
		return util.ErrBucketAlreadyExists
	}

	var bucketName string
	var err error

	if bucketClaim.Spec.ExistingBucketName != "" {
		bucketName = bucketClaim.Spec.ExistingBucketName
		bucket, err := b.buckets().Get(ctx, bucketName, metav1.GetOptions{})
		if kubeerrors.IsNotFound(err) {
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
		} else if err != nil {
			klog.V(3).ErrorS(err, "Get Bucket with ExistingBucketName error", "name", bucketClaim.Spec.ExistingBucketName)
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
		}

		bucket.Spec.BucketClaim = &v1.ObjectReference{
			Name:      bucketClaim.ObjectMeta.Name,
			Namespace: bucketClaim.ObjectMeta.Namespace,
			UID:       bucketClaim.ObjectMeta.UID,
		}

		protocolCopy := make([]v1alpha1.Protocol, len(bucketClaim.Spec.Protocols))
		copy(protocolCopy, bucketClaim.Spec.Protocols)

		bucket.Spec.Protocols = protocolCopy
		_, err = b.buckets().Update(ctx, bucket, metav1.UpdateOptions{})
		if err != nil {
			klog.V(3).ErrorS(err, "Error updating existing bucket",
				"bucket", bucket.ObjectMeta.Name,
				"bucketClaim", bucketClaim.ObjectMeta.Name)
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
		}

		bucketClaim.Status.BucketName = bucketName
		bucketClaim.Status.BucketReady = true
	} else {
		bucketClassName := bucketClaim.Spec.BucketClassName
		if bucketClassName == "" {
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, util.ErrInvalidBucketClass)
		}

		bucketClass, err := b.bucketClasses().Get(ctx, bucketClassName, metav1.GetOptions{})
		if kubeerrors.IsNotFound(err) {
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
		} else if err != nil {
			klog.V(3).ErrorS(err, "Get Bucketclass Error", "name", bucketClassName)
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
		}

		bucketName = fmt.Sprintf("bucket-%s", bucketClaim.ObjectMeta.UID)

		// create bucket
		bucket := &v1alpha1.Bucket{}
		bucket.Name = bucketName
		bucket.Spec.DriverName = bucketClass.DriverName
		bucket.Status.BucketReady = false
		bucket.Spec.BucketClassName = bucketClassName
		bucket.Spec.DeletionPolicy = bucketClass.DeletionPolicy
		bucket.Spec.Parameters = util.CopySS(bucketClass.Parameters)

		bucket.Spec.BucketClaim = &v1.ObjectReference{
			Name:      bucketClaim.ObjectMeta.Name,
			Namespace: bucketClaim.ObjectMeta.Namespace,
			UID:       bucketClaim.ObjectMeta.UID,
		}

		protocolCopy := make([]v1alpha1.Protocol, len(bucketClaim.Spec.Protocols))
		copy(protocolCopy, bucketClaim.Spec.Protocols)

		bucket.Spec.Protocols = protocolCopy
		bucket, err = b.buckets().Create(ctx, bucket, metav1.CreateOptions{})
		if err != nil && !kubeerrors.IsAlreadyExists(err) {
			klog.V(3).ErrorS(err, "Error creationg bucket",
				"bucket", bucketName,
				"bucketClaim", bucketClaim.ObjectMeta.Name)
			return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
		}

		bucketClaim.Status.BucketName = bucketName
		bucketClaim.Status.BucketReady = false
	}

	// Fetching the updated bucketClaim again, so that the update
	// operation doesn't happen on an outdated version of the bucketClaim.
	bucketClaim, err = b.bucketClaims(bucketClaim.ObjectMeta.Namespace).UpdateStatus(ctx, bucketClaim, metav1.UpdateOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "Failed to update status of BucketClaim", "name", bucketClaim.ObjectMeta.Name)
		return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
	}

	// Add the finalizers so that bucketClaim is deleted
	// only after the associated bucket is deleted.
	controllerutil.AddFinalizer(bucketClaim, util.BucketClaimFinalizer)
	_, err = b.bucketClaims(bucketClaim.ObjectMeta.Namespace).Update(ctx, bucketClaim, metav1.UpdateOptions{})
	if err != nil {
		klog.V(3).ErrorS(err, "Failed to add finalizer BucketClaim", "name", bucketClaim.ObjectMeta.Name)
		return b.recordError(inputBucketClaim, v1.EventTypeWarning, v1alpha1.FailedCreateBucket, err)
	}

	klog.V(3).Infof("Finished creating Bucket %v", bucketName)
	return nil
}

// InitializeKubeClient initializes the kubernetes client
func (b *BucketClaimListener) InitializeKubeClient(k kubeclientset.Interface) {
	b.kubeClient = k
}

// InitializeBucketClient initializes the object storage bucket client
func (b *BucketClaimListener) InitializeBucketClient(bc bucketclientset.Interface) {
	b.bucketClient = bc
}

// InitializeEventRecorder initializes the event recorder
func (b *BucketClaimListener) InitializeEventRecorder(er record.EventRecorder) {
	b.eventRecorder = er
}

func (b *BucketClaimListener) buckets() objectstoragev1alpha1.BucketInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().Buckets()
	}
	panic("uninitialized listener")
}

func (b *BucketClaimListener) bucketClasses() objectstoragev1alpha1.BucketClassInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketClasses()
	}
	panic("uninitialized listener")
}

func (b *BucketClaimListener) bucketClaims(namespace string) objectstoragev1alpha1.BucketClaimInterface {
	if b.bucketClient != nil {
		return b.bucketClient.ObjectstorageV1alpha1().BucketClaims(namespace)
	}
	panic("uninitialized listener")
}

// recordError during the processing of the objects
func (b *BucketClaimListener) recordError(subject runtime.Object, eventtype, reason string, err error) error {
	if b.eventRecorder == nil {
		return err
	}
	b.eventRecorder.Event(subject, eventtype, reason, err.Error())

	return err
}

// recordEvent during the processing of the objects
func (b *BucketClaimListener) recordEvent(subject runtime.Object, eventtype, reason, message string, args ...any) {
	if b.eventRecorder == nil {
		return
	}
	b.eventRecorder.Event(subject, eventtype, reason, fmt.Sprintf(message, args...))
}
