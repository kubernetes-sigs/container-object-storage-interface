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

package predicate

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
)

func Test_toTypedOrLogError(t *testing.T) {
	// ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	// logger := ctrl.Log.WithName("predicate")
	logger := logr.Discard() // comment this and uncomment above to locally test log messages

	scheme := runtime.NewScheme()
	err := cosiapi.AddToScheme(scheme)
	require.NoError(t, err)

	t.Run("matching type", func(t *testing.T) {
		access := &cosiapi.BucketAccess{
			ObjectMeta: meta.ObjectMeta{
				Namespace: "ns",
				Name:      "name",
			},
		}
		accessObj := client.Object(access)

		gotObj, ok := toTypedOrLogError[*cosiapi.BucketAccess](logger, scheme, accessObj)
		assert.Equal(t, access, gotObj)
		assert.True(t, ok)
	})

	t.Run("nonmatching type", func(t *testing.T) {
		claim := &cosiapi.BucketClaim{
			ObjectMeta: meta.ObjectMeta{
				Namespace: "ns",
				Name:      "name",
			},
		}
		claimObj := client.Object(claim)

		gotObj, ok := toTypedOrLogError[*cosiapi.BucketAccess](logger, scheme, claimObj)
		assert.Empty(t, gotObj)
		assert.False(t, ok)
	})
}

func TestDeletionTimestampAdded(t *testing.T) {
	deletionTimestamp := meta.Now()
	old := &cosiapi.BucketAccess{}
	deleting := old.DeepCopy()
	deleting.DeletionTimestamp = &deletionTimestamp

	predicate := DeletionTimestampAdded()

	assert.True(t, predicate.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: deleting}))
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: deleting, ObjectNew: deleting.DeepCopy()}))
}

func TestBucketClaimBeingDeletedAnnotationAdded(t *testing.T) {
	old := &cosiapi.Bucket{}
	annotated := old.DeepCopy()
	annotated.Annotations = map[string]string{cosiapi.BucketClaimBeingDeletedAnnotation: ""}

	predicate := BucketClaimBeingDeletedAnnotationAdded()

	assert.True(t, predicate.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: annotated}))
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: annotated, ObjectNew: annotated.DeepCopy()}))
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: old.DeepCopy()}))
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: annotated, ObjectNew: old}))
}

func Test_handoffOccurred(t *testing.T) {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	logger := ctrl.Log.WithName("predicate")
	// logger := logr.Discard() // comment this and uncomment above to locally test log messages

	t.Run("no handoff", func(t *testing.T) {
		old := &cosiapi.BucketAccess{}
		new := &cosiapi.BucketAccess{}

		assert.False(t, handoffOccurred(logger, old, new))
	})

	t.Run("handoff", func(t *testing.T) {
		old := &cosiapi.BucketAccess{}
		new := &cosiapi.BucketAccess{
			Status: cosiapi.BucketAccessStatus{
				DriverName: "something",
			},
		}

		assert.True(t, handoffOccurred(logger, old, new))
	})

}

func TestBucketReadinessStatusChanged(t *testing.T) {
	scheme := runtime.NewScheme()
	err := cosiapi.AddToScheme(scheme)
	require.NoError(t, err)

	predicate := BucketReadinessStatusChanged(scheme)

	nilReadyBucket := &cosiapi.Bucket{}
	notReadyBucket := nilReadyBucket.DeepCopy()
	notReadyBucket.Status.ReadyToUse = ptr.To(false)
	readyBucket := nilReadyBucket.DeepCopy()
	readyBucket.Status.ReadyToUse = ptr.To(true)

	// all permutations of readiness changes; nil is treated the same as false
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: nilReadyBucket, ObjectNew: nilReadyBucket.DeepCopy()}))
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: nilReadyBucket, ObjectNew: notReadyBucket}))
	assert.True(t, predicate.Update(event.UpdateEvent{ObjectOld: nilReadyBucket, ObjectNew: readyBucket}))
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: notReadyBucket, ObjectNew: nilReadyBucket}))
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: notReadyBucket, ObjectNew: notReadyBucket.DeepCopy()}))
	assert.True(t, predicate.Update(event.UpdateEvent{ObjectOld: notReadyBucket, ObjectNew: readyBucket}))
	assert.True(t, predicate.Update(event.UpdateEvent{ObjectOld: readyBucket, ObjectNew: nilReadyBucket}))
	assert.True(t, predicate.Update(event.UpdateEvent{ObjectOld: readyBucket, ObjectNew: notReadyBucket}))
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: readyBucket, ObjectNew: readyBucket.DeepCopy()}))

	// other status fields changing alone is ignored
	bucketWithDetails := readyBucket.DeepCopy()
	bucketWithDetails.Status.BucketID = "some-bucket-id"
	bucketWithDetails.Status.Protocols = []cosiapi.ObjectProtocol{cosiapi.ObjectProtocolS3}
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: readyBucket, ObjectNew: bucketWithDetails}))

	// non-Bucket objects are ignored
	claim := &cosiapi.BucketClaim{}
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: claim, ObjectNew: readyBucket}))
	assert.False(t, predicate.Update(event.UpdateEvent{ObjectOld: readyBucket, ObjectNew: claim}))

	// non-Update events are ignored
	assert.False(t, predicate.Create(event.CreateEvent{Object: readyBucket}))
	assert.False(t, predicate.Delete(event.DeleteEvent{Object: readyBucket}))
	assert.False(t, predicate.Generic(event.GenericEvent{Object: readyBucket}))
}
