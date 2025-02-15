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

// Code generated by lister-gen. DO NOT EDIT.

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/listers"
	"k8s.io/client-go/tools/cache"
	v1alpha1 "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
)

// BucketAccessLister helps list BucketAccesses.
// All objects returned here must be treated as read-only.
type BucketAccessLister interface {
	// List lists all BucketAccesses in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.BucketAccess, err error)
	// BucketAccesses returns an object that can list and get BucketAccesses.
	BucketAccesses(namespace string) BucketAccessNamespaceLister
	BucketAccessListerExpansion
}

// bucketAccessLister implements the BucketAccessLister interface.
type bucketAccessLister struct {
	listers.ResourceIndexer[*v1alpha1.BucketAccess]
}

// NewBucketAccessLister returns a new BucketAccessLister.
func NewBucketAccessLister(indexer cache.Indexer) BucketAccessLister {
	return &bucketAccessLister{listers.New[*v1alpha1.BucketAccess](indexer, v1alpha1.Resource("bucketaccess"))}
}

// BucketAccesses returns an object that can list and get BucketAccesses.
func (s *bucketAccessLister) BucketAccesses(namespace string) BucketAccessNamespaceLister {
	return bucketAccessNamespaceLister{listers.NewNamespaced[*v1alpha1.BucketAccess](s.ResourceIndexer, namespace)}
}

// BucketAccessNamespaceLister helps list and get BucketAccesses.
// All objects returned here must be treated as read-only.
type BucketAccessNamespaceLister interface {
	// List lists all BucketAccesses in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.BucketAccess, err error)
	// Get retrieves the BucketAccess from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v1alpha1.BucketAccess, error)
	BucketAccessNamespaceListerExpansion
}

// bucketAccessNamespaceLister implements the BucketAccessNamespaceLister
// interface.
type bucketAccessNamespaceLister struct {
	listers.ResourceIndexer[*v1alpha1.BucketAccess]
}
