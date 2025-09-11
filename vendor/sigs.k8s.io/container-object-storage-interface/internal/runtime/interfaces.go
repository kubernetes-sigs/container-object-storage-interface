package runtime

import (
	"context"

	// storage
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	bucketclientset "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned"

	// k8s client
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
)

// Set the clients for each of the listeners
type GenericListener interface {
	InitializeKubeClient(kubeclientset.Interface)
	InitializeBucketClient(bucketclientset.Interface)
	InitializeEventRecorder(record.EventRecorder)
}

type BucketListener interface {
	GenericListener

	Add(ctx context.Context, b *v1alpha1.Bucket) error
	Update(ctx context.Context, old *v1alpha1.Bucket, new *v1alpha1.Bucket) error
	Delete(ctx context.Context, b *v1alpha1.Bucket) error
}

func (c *ObjectStorageController) AddBucketListener(b BucketListener) {
	c.initialized = true
	c.BucketListener = b
}

type BucketClaimListener interface {
	GenericListener

	Add(ctx context.Context, b *v1alpha1.BucketClaim) error
	Update(ctx context.Context, old *v1alpha1.BucketClaim, new *v1alpha1.BucketClaim) error
	Delete(ctx context.Context, b *v1alpha1.BucketClaim) error
}

func (c *ObjectStorageController) AddBucketClaimListener(b BucketClaimListener) {
	c.initialized = true
	c.BucketClaimListener = b
}

type BucketAccessListener interface {
	GenericListener

	Add(ctx context.Context, b *v1alpha1.BucketAccess) error
	Update(ctx context.Context, old *v1alpha1.BucketAccess, new *v1alpha1.BucketAccess) error
	Delete(ctx context.Context, b *v1alpha1.BucketAccess) error
}

func (c *ObjectStorageController) AddBucketAccessListener(b BucketAccessListener) {
	c.initialized = true
	c.BucketAccessListener = b
}

type BucketClassListener interface {
	GenericListener

	Add(ctx context.Context, b *v1alpha1.BucketClass) error
	Update(ctx context.Context, old *v1alpha1.BucketClass, new *v1alpha1.BucketClass) error
	Delete(ctx context.Context, b *v1alpha1.BucketClass) error
}

func (c *ObjectStorageController) AddBucketClassListener(b BucketClassListener) {
	c.initialized = true
	c.BucketClassListener = b
}

type BucketAccessClassListener interface {
	GenericListener

	Add(ctx context.Context, b *v1alpha1.BucketAccessClass) error
	Update(ctx context.Context, old *v1alpha1.BucketAccessClass, new *v1alpha1.BucketAccessClass) error
	Delete(ctx context.Context, b *v1alpha1.BucketAccessClass) error
}

func (c *ObjectStorageController) AddBucketAccessClassListener(b BucketAccessClassListener) {
	c.initialized = true
	c.BucketAccessClassListener = b
}
