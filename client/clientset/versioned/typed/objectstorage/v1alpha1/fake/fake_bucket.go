/*
Copyright 2024 The Kubernetes Authors.

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

// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	"context"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
	v1alpha1 "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
)

// FakeBuckets implements BucketInterface
type FakeBuckets struct {
	Fake *FakeObjectstorageV1alpha1
}

var bucketsResource = v1alpha1.SchemeGroupVersion.WithResource("buckets")

var bucketsKind = v1alpha1.SchemeGroupVersion.WithKind("Bucket")

// Get takes name of the bucket, and returns the corresponding bucket object, and an error if there is any.
func (c *FakeBuckets) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.Bucket, err error) {
	emptyResult := &v1alpha1.Bucket{}
	obj, err := c.Fake.
		Invokes(testing.NewRootGetActionWithOptions(bucketsResource, name, options), emptyResult)
	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.Bucket), err
}

// List takes label and field selectors, and returns the list of Buckets that match those selectors.
func (c *FakeBuckets) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.BucketList, err error) {
	emptyResult := &v1alpha1.BucketList{}
	obj, err := c.Fake.
		Invokes(testing.NewRootListActionWithOptions(bucketsResource, bucketsKind, opts), emptyResult)
	if obj == nil {
		return emptyResult, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha1.BucketList{ListMeta: obj.(*v1alpha1.BucketList).ListMeta}
	for _, item := range obj.(*v1alpha1.BucketList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested buckets.
func (c *FakeBuckets) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewRootWatchActionWithOptions(bucketsResource, opts))
}

// Create takes the representation of a bucket and creates it.  Returns the server's representation of the bucket, and an error, if there is any.
func (c *FakeBuckets) Create(ctx context.Context, bucket *v1alpha1.Bucket, opts v1.CreateOptions) (result *v1alpha1.Bucket, err error) {
	emptyResult := &v1alpha1.Bucket{}
	obj, err := c.Fake.
		Invokes(testing.NewRootCreateActionWithOptions(bucketsResource, bucket, opts), emptyResult)
	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.Bucket), err
}

// Update takes the representation of a bucket and updates it. Returns the server's representation of the bucket, and an error, if there is any.
func (c *FakeBuckets) Update(ctx context.Context, bucket *v1alpha1.Bucket, opts v1.UpdateOptions) (result *v1alpha1.Bucket, err error) {
	emptyResult := &v1alpha1.Bucket{}
	obj, err := c.Fake.
		Invokes(testing.NewRootUpdateActionWithOptions(bucketsResource, bucket, opts), emptyResult)
	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.Bucket), err
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *FakeBuckets) UpdateStatus(ctx context.Context, bucket *v1alpha1.Bucket, opts v1.UpdateOptions) (result *v1alpha1.Bucket, err error) {
	emptyResult := &v1alpha1.Bucket{}
	obj, err := c.Fake.
		Invokes(testing.NewRootUpdateSubresourceActionWithOptions(bucketsResource, "status", bucket, opts), emptyResult)
	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.Bucket), err
}

// Delete takes name of the bucket and deletes it. Returns an error if one occurs.
func (c *FakeBuckets) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewRootDeleteActionWithOptions(bucketsResource, name, opts), &v1alpha1.Bucket{})
	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeBuckets) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewRootDeleteCollectionActionWithOptions(bucketsResource, opts, listOpts)

	_, err := c.Fake.Invokes(action, &v1alpha1.BucketList{})
	return err
}

// Patch applies the patch and returns the patched bucket.
func (c *FakeBuckets) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.Bucket, err error) {
	emptyResult := &v1alpha1.Bucket{}
	obj, err := c.Fake.
		Invokes(testing.NewRootPatchSubresourceActionWithOptions(bucketsResource, name, pt, data, opts, subresources...), emptyResult)
	if obj == nil {
		return emptyResult, err
	}
	return obj.(*v1alpha1.Bucket), err
}
