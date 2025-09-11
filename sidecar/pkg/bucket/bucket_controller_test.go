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

package bucket

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakekubeclientset "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/consts"
	"sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	fakebucketclientset "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned/fake"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
	fakespec "sigs.k8s.io/container-object-storage-interface/proto/fake"
)

func TestInitializeKubeClient(t *testing.T) {
	client := fakekubeclientset.NewSimpleClientset()

	bl := BucketListener{}
	bl.InitializeKubeClient(client)

	if bl.kubeClient == nil {
		t.Errorf("KubeClient was nil")
	}
}

func TestInitializeBucketClient(t *testing.T) {
	client := fakebucketclientset.NewSimpleClientset()

	bl := BucketListener{}
	bl.InitializeBucketClient(client)

	if bl.bucketClient == nil {
		t.Errorf("BucketClient was nil")
	}
}

func TestInitializeEventRecorder(t *testing.T) {
	eventRecorder := record.NewFakeRecorder(1)

	bl := BucketListener{}
	bl.InitializeEventRecorder(eventRecorder)

	if bl.eventRecorder == nil {
		t.Errorf("BucketClient not initialized, expected not nil")
	}
}

func TestAddWrongProvisioner(t *testing.T) {
	driver := "driver1"
	mpc := struct{ fakespec.FakeProvisionerClient }{}
	mpc.FakeDriverCreateBucket = func(ctx context.Context,
		in *cosi.DriverCreateBucketRequest,
		opts ...grpc.CallOption) (*cosi.DriverCreateBucketResponse, error) {
		t.Errorf("grpc client called")
		return nil, nil
	}

	bl := BucketListener{
		driverName:        driver,
		provisionerClient: &mpc,
	}

	b := v1alpha1.Bucket{
		Spec: v1alpha1.BucketSpec{
			DriverName:      "driver2",
			BucketClassName: "test-bucket",
		},
	}
	ctx := context.TODO()
	err := bl.Add(ctx, &b)
	if err != nil {
		t.Errorf("Error returned: %+v", err)
	}
}

func TestMissingBucketClassName(t *testing.T) {
	driver := "driver1"
	mpc := struct{ fakespec.FakeProvisionerClient }{}
	mpc.FakeDriverCreateBucket = func(ctx context.Context,
		in *cosi.DriverCreateBucketRequest,
		opts ...grpc.CallOption) (*cosi.DriverCreateBucketResponse, error) {
		t.Errorf("grpc client called")
		return nil, nil
	}

	bl := BucketListener{
		driverName:        driver,
		provisionerClient: &mpc,
	}

	b := v1alpha1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name: "testbucket",
		},
		Spec: v1alpha1.BucketSpec{
			DriverName: "driver1",
		},
	}
	ctx := context.TODO()
	err := bl.Add(ctx, &b)
	expectedErr := errors.New("BucketClassName not defined for Bucket testbucket")
	if err == nil || err.Error() != expectedErr.Error() {
		t.Errorf("expecter error: %+v \n returned error: %+v", expectedErr, err)
	}
}

// Test recording events
func TestRecordEvents(t *testing.T) {
	t.Parallel()

	var (
		bucketClass = &v1alpha1.BucketClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "bucket-class",
			},
		}
		bucketClaim = &v1alpha1.BucketClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "bucket-claim",
			},
		}
		bucket = &v1alpha1.Bucket{
			ObjectMeta: metav1.ObjectMeta{
				Name: "bucket",
				Finalizers: []string{
					consts.BucketFinalizer,
				},
			},
			Spec: v1alpha1.BucketSpec{
				DriverName:     "test",
				DeletionPolicy: v1alpha1.DeletionPolicyDelete,
				BucketClaim: &v1.ObjectReference{
					Name: bucketClaim.GetObjectMeta().GetName(),
				},
			},
		}
	)

	for _, tc := range []struct {
		name          string
		expectedEvent string
		cosiObjects   []runtime.Object
		driver        struct{ fakespec.FakeProvisionerClient }
		eventTrigger  func(*testing.T, *BucketListener)
	}{
		{
			name: "BucketClassNameNotDefined",
			expectedEvent: newEvent(
				v1.EventTypeWarning,
				v1alpha1.FailedCreateBucket,
				"BucketClassName not defined for Bucket bucket"),
			eventTrigger: func(t *testing.T, bl *BucketListener) {
				if err := bl.Add(context.TODO(), bucket.DeepCopy()); !errors.Is(err, consts.ErrUndefinedBucketClassName) {
					t.Errorf("expected %v error got %v", consts.ErrUndefinedBucketClassName, err)
				}
			},
			driver: struct{ fakespec.FakeProvisionerClient }{
				FakeProvisionerClient: fakespec.FakeProvisionerClient{
					FakeDriverCreateBucket: func(
						_ context.Context,
						_ *cosi.DriverCreateBucketRequest,
						_ ...grpc.CallOption,
					) (*cosi.DriverCreateBucketResponse, error) {
						panic("should not be reached, bucket class name is not defined")
					},
				},
			},
		},
		{
			name: "BucketClassNotFound",
			expectedEvent: newEvent(
				v1.EventTypeWarning,
				v1alpha1.FailedCreateBucket,
				"bucketclasses.objectstorage.k8s.io \"bucket-class\" not found"),
			eventTrigger: func(t *testing.T, bl *BucketListener) {
				bucket := bucket.DeepCopy()
				bucket.Spec.ExistingBucketID = "existing"
				bucket.Spec.BucketClassName = bucketClass.GetObjectMeta().GetName()

				if err := bl.Add(context.TODO(), bucket); !kubeerrors.IsNotFound(err) {
					t.Errorf("expected Not Found error got %v", err)
				}
			},
			driver: struct{ fakespec.FakeProvisionerClient }{
				FakeProvisionerClient: fakespec.FakeProvisionerClient{
					FakeDriverCreateBucket: func(
						_ context.Context,
						_ *cosi.DriverCreateBucketRequest,
						_ ...grpc.CallOption,
					) (*cosi.DriverCreateBucketResponse, error) {
						panic("should not be reached, bucket class does not exist")
					},
				},
			},
		},
		{
			name: "CreateInternalError",
			expectedEvent: newEvent(
				v1.EventTypeWarning,
				v1alpha1.FailedCreateBucket,
				"failed to create bucket: rpc error: code = Internal desc = internal error test"),
			cosiObjects: []runtime.Object{bucketClass},
			eventTrigger: func(t *testing.T, bl *BucketListener) {
				bucket := bucket.DeepCopy()
				bucket.Spec.BucketClassName = bucketClass.GetObjectMeta().GetName()

				if err := bl.Add(context.TODO(), bucket); status.Code(errors.Unwrap(err)) != codes.Internal {
					t.Errorf("expected Internal got %v", err)
				}
			},
			driver: struct{ fakespec.FakeProvisionerClient }{
				FakeProvisionerClient: fakespec.FakeProvisionerClient{
					FakeDriverCreateBucket: func(
						_ context.Context,
						_ *cosi.DriverCreateBucketRequest,
						_ ...grpc.CallOption,
					) (*cosi.DriverCreateBucketResponse, error) {
						return nil, status.Error(codes.Internal, "internal error test")
					},
				},
			},
		},
		{
			name: "DeleteInternalError",
			expectedEvent: newEvent(
				v1.EventTypeWarning,
				v1alpha1.FailedDeleteBucket,
				"failed to delete bucket: rpc error: code = Internal desc = internal error test"),
			cosiObjects: []runtime.Object{bucketClaim},
			eventTrigger: func(t *testing.T, bl *BucketListener) {
				bucket := bucket.DeepCopy()
				time, _ := time.Parse(time.DateTime, "2006-01-02 15:04:05")
				bucket.ObjectMeta.DeletionTimestamp = &metav1.Time{Time: time}

				if err := bl.Update(context.TODO(), bucket, bucket); status.Code(errors.Unwrap(err)) != codes.Internal {
					t.Errorf("expected Internal got %v", err)
				}
			},
			driver: struct{ fakespec.FakeProvisionerClient }{
				FakeProvisionerClient: fakespec.FakeProvisionerClient{
					FakeDriverDeleteBucket: func(
						_ context.Context,
						_ *cosi.DriverDeleteBucketRequest,
						_ ...grpc.CallOption,
					) (*cosi.DriverDeleteBucketResponse, error) {
						return nil, status.Error(codes.Internal, "internal error test")
					},
				},
			},
		},
	} {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := fakebucketclientset.NewSimpleClientset(tc.cosiObjects...)
			kubeClient := fakekubeclientset.NewSimpleClientset()
			eventRecorder := record.NewFakeRecorder(1)

			listener := NewBucketListener("test", &tc.driver)
			listener.InitializeKubeClient(kubeClient)
			listener.InitializeBucketClient(client)
			listener.InitializeEventRecorder(eventRecorder)

			tc.eventTrigger(t, listener)

			select {
			case event, ok := <-eventRecorder.Events:
				if ok {
					if event != tc.expectedEvent {
						t.Errorf("expected %s got %s", tc.expectedEvent, event)
					}
				} else {
					t.Error("channel closed, no event")
				}
			default:
				t.Errorf("no event after trigger")
			}
		})
	}
}

func newEvent(eventType, reason, message string) string {
	return fmt.Sprintf("%s %s %s", eventType, reason, message)
}

// TestAddDeletedBucket tests that the Add method does not call the driver
func TestAddDeletedBucket(t *testing.T) {
	driver := "driver1"

	mpc := struct{ fakespec.FakeProvisionerClient }{}
	mpc.FakeDriverDeleteBucket = func(
		_ context.Context,
		_ *cosi.DriverDeleteBucketRequest,
		_ ...grpc.CallOption,
	) (*cosi.DriverDeleteBucketResponse, error) {
		t.Fatalf("driver should NOT be called from Add when object has DeletionTimestamp")
		return nil, nil
	}

	now := metav1.Now()
	b := v1alpha1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "testbucket",
			DeletionTimestamp: &now,
			ResourceVersion:   "1",
		},
		Spec: v1alpha1.BucketSpec{
			DriverName:      driver,
			BucketClassName: "ignored",
		},
	}

	client := fakebucketclientset.NewSimpleClientset(&b)

	bl := BucketListener{
		driverName:        driver,
		provisionerClient: &mpc,
	}
	bl.InitializeBucketClient(client)

	if err := bl.Add(context.TODO(), &b); err != nil {
		t.Fatalf("Add returned error for deleted bucket: %v", err)
	}
}
