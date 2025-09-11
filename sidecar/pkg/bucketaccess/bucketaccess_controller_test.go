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
	"errors"
	"fmt"
	"strings"
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

	bal := BucketAccessListener{}
	bal.InitializeKubeClient(client)

	if bal.kubeClient == nil {
		t.Errorf("KubeClient was nil")
	}
}

func TestInitializeBucketClient(t *testing.T) {
	client := fakebucketclientset.NewSimpleClientset()

	bal := BucketAccessListener{}
	bal.InitializeBucketClient(client)

	if bal.bucketClient == nil {
		t.Errorf("BucketClient not initialized, expected not nil")
	}
}

func TestInitializeEventRecorder(t *testing.T) {
	eventRecorder := record.NewFakeRecorder(1)

	bal := BucketAccessListener{}
	bal.InitializeEventRecorder(eventRecorder)

	if bal.eventRecorder == nil {
		t.Errorf("BucketClient not initialized, expected not nil")
	}
}

func TestAddWrongProvisioner(t *testing.T) {
	driver := "driver1"
	bucketAccessClassName := "bucketAccessClass1"
	bucketClaimName := "bucketClaim1"
	accountId := "accountId1"
	secretName := "secret1"
	timeNow := time.Now()
	secret := map[string]string{
		"accessToken": "randomValue",
		"expiryTs":    timeNow.String(),
	}

	credentialDetails := &cosi.CredentialDetails{
		Secrets: secret,
	}
	credential := map[string]*cosi.CredentialDetails{
		"azure": credentialDetails,
	}

	mpc := struct{ fakespec.FakeProvisionerClient }{}
	mpc.FakeDriverGrantBucketAccess = func(ctx context.Context,
		in *cosi.DriverGrantBucketAccessRequest,
		opts ...grpc.CallOption) (*cosi.DriverGrantBucketAccessResponse, error) {
		t.Errorf("grpc client called")
		return &cosi.DriverGrantBucketAccessResponse{
			AccountId:   accountId,
			Credentials: credential,
		}, nil
	}

	b := v1alpha1.BucketAccessClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: bucketAccessClassName,
		},
		DriverName:         driver + "-invalid",
		AuthenticationType: v1alpha1.AuthenticationTypeKey,
	}

	ba := v1alpha1.BucketAccess{
		Spec: v1alpha1.BucketAccessSpec{
			BucketClaimName:       bucketClaimName,
			Protocol:              v1alpha1.ProtocolAzure,
			BucketAccessClassName: bucketAccessClassName,
			CredentialsSecretName: secretName,
		},
	}

	client := fakebucketclientset.NewSimpleClientset(&ba, &b)
	kubeClient := fakekubeclientset.NewSimpleClientset()
	bal := BucketAccessListener{
		driverName:        driver,
		provisionerClient: &mpc,
		bucketClient:      client,
		kubeClient:        kubeClient,
	}

	ctx := context.TODO()
	err := bal.Add(ctx, &ba)
	if err != nil {
		t.Errorf("Error returned: %+v", err)
	}
}

func TestAddBucketAccess(t *testing.T) {
	driver := "driver"
	bucketName := "bucket1"
	bucketId := "bucketId1"
	bucketClaimName := "bucketClaim1"
	bucketClassName := "bucketClass1"
	bucketAccessClassName := "bucketAccessClass1"
	bucketAccessName := "bucketAccess1"
	secretName := "secret1"

	accountId := "account1"
	ns := "testns"

	timeNow := time.Now()
	secret := map[string]string{
		"accessToken": "randomValue",
		"expiryTs":    timeNow.String(),
	}

	credentialDetails := &cosi.CredentialDetails{
		Secrets: secret,
	}
	creds := map[string]*cosi.CredentialDetails{
		"azure": credentialDetails,
	}

	mpc := struct{ fakespec.FakeProvisionerClient }{}

	testCases := []struct {
		name      string
		setFields func(ba *v1alpha1.BucketAccess)
		grantFunc func(ctx context.Context,
			in *cosi.DriverGrantBucketAccessRequest,
			opts ...grpc.CallOption) (*cosi.DriverGrantBucketAccessResponse, error)
	}{
		{
			name: "TestAddBucketAccess",
			setFields: func(ba *v1alpha1.BucketAccess) {

			},
			grantFunc: func(ctx context.Context,
				req *cosi.DriverGrantBucketAccessRequest,
				opts ...grpc.CallOption) (*cosi.DriverGrantBucketAccessResponse, error) {

				return &cosi.DriverGrantBucketAccessResponse{
					AccountId:   accountId,
					Credentials: creds,
				}, nil
			},
		},
	}

	for _, tc := range testCases {
		b := v1alpha1.Bucket{
			ObjectMeta: metav1.ObjectMeta{
				Name: bucketName,
			},
			Spec: v1alpha1.BucketSpec{
				DriverName:     driver,
				Protocols:      []v1alpha1.Protocol{v1alpha1.ProtocolAzure},
				DeletionPolicy: v1alpha1.DeletionPolicyRetain,
			},
			Status: v1alpha1.BucketStatus{
				BucketID:    bucketId,
				BucketReady: true,
			},
		}

		bc := v1alpha1.BucketClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bucketClaimName,
				Namespace: ns,
			},
			Spec: v1alpha1.BucketClaimSpec{
				BucketClassName: bucketClassName,
				Protocols:       []v1alpha1.Protocol{v1alpha1.ProtocolAzure},
			},
			Status: v1alpha1.BucketClaimStatus{
				BucketReady: true,
				BucketName:  bucketName,
			},
		}

		bac := v1alpha1.BucketAccessClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: bucketAccessClassName,
			},
			DriverName:         driver,
			AuthenticationType: v1alpha1.AuthenticationTypeKey,
		}

		ba := v1alpha1.BucketAccess{
			ObjectMeta: metav1.ObjectMeta{
				Name:      bucketAccessName,
				Namespace: ns,
			},
			Spec: v1alpha1.BucketAccessSpec{
				BucketClaimName:       bucketClaimName,
				Protocol:              v1alpha1.ProtocolAzure,
				BucketAccessClassName: bucketAccessClassName,
				CredentialsSecretName: secretName,
			},
		}

		ctx := context.TODO()
		tc.setFields(&ba)

		client := fakebucketclientset.NewSimpleClientset(&b, &bc, &bac, &ba)
		kubeClient := fakekubeclientset.NewSimpleClientset()
		mpc.FakeDriverGrantBucketAccess = tc.grantFunc

		bal := BucketAccessListener{
			driverName:        driver,
			provisionerClient: &mpc,
			bucketClient:      client,
			kubeClient:        kubeClient,
		}

		t.Log(tc.name)
		err := bal.Add(ctx, &ba)
		if err != nil {
			t.Errorf("Add returned: %+v", err)
		}

		updatedBA, _ := bal.bucketAccesses(ns).Get(ctx, ba.ObjectMeta.Name, metav1.GetOptions{})
		if updatedBA.Status.AccessGranted != true {
			t.Errorf("expected %t, got %t", true, ba.Status.AccessGranted)
		}
		if !strings.EqualFold(updatedBA.Status.AccountID, accountId) {
			t.Errorf("expected %s, got %s", accountId, updatedBA.Status.AccountID)
		}

		_, err = bal.secrets(ns).Get(ctx, secretName, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Secret creation failed: %v", err)
		}
	}
}

// Test recording events
func TestRecordEvents(t *testing.T) {
	t.Parallel()

	var (
		// bucketClass = &v1alpha1.BucketClass{
		// 	ObjectMeta: metav1.ObjectMeta{
		// 		Name: "bucket-class",
		// 	},
		// }
		bucket = &v1alpha1.Bucket{
			ObjectMeta: metav1.ObjectMeta{
				Name: "bucket",
			},
		}
		bucketReady = &v1alpha1.Bucket{
			ObjectMeta: metav1.ObjectMeta{
				Name: "bucket",
			},
			Status: v1alpha1.BucketStatus{
				BucketReady: true,
				BucketID:    "test",
			},
		}
		bucketClaim = &v1alpha1.BucketClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "bucket-claim",
			},
			Status: v1alpha1.BucketClaimStatus{
				BucketReady: true,
				BucketName:  bucket.GetObjectMeta().GetName(),
			},
		}
		bucketAccessClass = &v1alpha1.BucketAccessClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "bucket-access-class",
			},
			DriverName:         "test",
			AuthenticationType: v1alpha1.AuthenticationTypeIAM,
		}
		bucketAccess = &v1alpha1.BucketAccess{
			ObjectMeta: metav1.ObjectMeta{
				Name: "bucket-access",
			},
			Spec: v1alpha1.BucketAccessSpec{
				CredentialsSecretName: "credentials",
				BucketAccessClassName: bucketAccessClass.GetObjectMeta().GetName(),
				BucketClaimName:       bucketClaim.GetObjectMeta().GetName(),
			},
		}
	)

	for _, tc := range []struct {
		name          string
		expectedEvent string
		cosiObjects   []runtime.Object
		driver        struct{ fakespec.FakeProvisionerClient }
		eventTrigger  func(*testing.T, *BucketAccessListener)
	}{
		{
			name: "BucketAccessClassNotFound",
			expectedEvent: newEvent(
				v1.EventTypeWarning,
				v1alpha1.FailedGrantAccess,
				"bucketaccessclasses.objectstorage.k8s.io \"bucket-access-class\" not found"),
			eventTrigger: func(t *testing.T, bal *BucketAccessListener) {
				bucketAccess := bucketAccess.DeepCopy()

				if err := bal.Add(context.TODO(), bucketAccess); !kubeerrors.IsNotFound(err) {
					t.Errorf("expected Not Found error got %v", err)
				}
			},
			driver: struct{ fakespec.FakeProvisionerClient }{
				FakeProvisionerClient: fakespec.FakeProvisionerClient{
					FakeDriverGrantBucketAccess: func(
						_ context.Context,
						_ *cosi.DriverGrantBucketAccessRequest,
						_ ...grpc.CallOption,
					) (*cosi.DriverGrantBucketAccessResponse, error) {
						panic("should not be reached")
					},
				},
			},
		},
		{
			name: "UndefinedServiceAccountName",
			expectedEvent: newEvent(
				v1.EventTypeWarning,
				v1alpha1.FailedGrantAccess,
				consts.ErrUndefinedServiceAccountName.Error()),
			cosiObjects: []runtime.Object{bucketAccessClass, bucketClaim},
			eventTrigger: func(t *testing.T, bal *BucketAccessListener) {
				bucketAccess := bucketAccess.DeepCopy()

				if err := bal.Add(context.TODO(), bucketAccess); !errors.Is(err, consts.ErrUndefinedServiceAccountName) {
					t.Errorf("expected %v got %v", consts.ErrUndefinedServiceAccountName, err)
				}
			},
			driver: struct{ fakespec.FakeProvisionerClient }{
				FakeProvisionerClient: fakespec.FakeProvisionerClient{
					FakeDriverGrantBucketAccess: func(
						_ context.Context,
						_ *cosi.DriverGrantBucketAccessRequest,
						_ ...grpc.CallOption,
					) (*cosi.DriverGrantBucketAccessResponse, error) {
						panic("should not be reached")
					},
				},
			},
		},
		{
			name: "InvalidBucketState",
			expectedEvent: newEvent(
				v1.EventTypeWarning,
				v1alpha1.WaitingForBucket,
				"BucketAccess can't be granted to Bucket not in Ready state: (isReady? false), (ID empty? true)"),
			cosiObjects: []runtime.Object{bucketAccessClass, bucketClaim, bucket},
			eventTrigger: func(t *testing.T, bal *BucketAccessListener) {
				bucketAccess := bucketAccess.DeepCopy()
				bucketAccess.Spec.ServiceAccountName = "service-account"

				if err := bal.Add(context.TODO(), bucketAccess); !errors.Is(err, consts.ErrInvalidBucketState) {
					t.Errorf("expected %v got %v", consts.ErrInvalidBucketState, err)
				}
			},
			driver: struct{ fakespec.FakeProvisionerClient }{
				FakeProvisionerClient: fakespec.FakeProvisionerClient{
					FakeDriverGrantBucketAccess: func(
						_ context.Context,
						_ *cosi.DriverGrantBucketAccessRequest,
						_ ...grpc.CallOption,
					) (*cosi.DriverGrantBucketAccessResponse, error) {
						panic("should not be reached")
					},
				},
			},
		},
		{
			name: "GrantInternalError",
			expectedEvent: newEvent(
				v1.EventTypeWarning,
				v1alpha1.FailedGrantAccess,
				"failed to grant bucket access: rpc error: code = Internal desc = internal error test"),
			cosiObjects: []runtime.Object{bucketAccessClass, bucketClaim, bucketReady},
			eventTrigger: func(t *testing.T, bal *BucketAccessListener) {
				bucketAccess := bucketAccess.DeepCopy()
				bucketAccess.Spec.ServiceAccountName = "service-account"

				if err := bal.Add(context.TODO(), bucketAccess); status.Code(errors.Unwrap(err)) != codes.Internal {
					t.Errorf("expected Internal got %v", err)
				}
			},
			driver: struct{ fakespec.FakeProvisionerClient }{
				FakeProvisionerClient: fakespec.FakeProvisionerClient{
					FakeDriverGrantBucketAccess: func(
						_ context.Context,
						_ *cosi.DriverGrantBucketAccessRequest,
						_ ...grpc.CallOption,
					) (*cosi.DriverGrantBucketAccessResponse, error) {
						return nil, status.Error(codes.Internal, "internal error test")
					},
				},
			},
		},
		{
			name: "RevokeInternalError",
			expectedEvent: newEvent(
				v1.EventTypeWarning,
				v1alpha1.FailedRevokeAccess,
				"failed to revoke access: rpc error: code = Internal desc = internal error test"),
			cosiObjects: []runtime.Object{bucketAccessClass, bucketClaim, bucketReady},
			eventTrigger: func(t *testing.T, bal *BucketAccessListener) {
				bucketAccess := bucketAccess.DeepCopy()
				time, _ := time.Parse(time.DateTime, "2006-01-02 15:04:05")
				bucketAccess.ObjectMeta.DeletionTimestamp = &metav1.Time{Time: time}

				if err := bal.Update(context.TODO(), bucketAccess, bucketAccess); status.Code(errors.Unwrap(err)) != codes.Internal {
					t.Errorf("expected Internal got %v", err)
				}
			},
			driver: struct{ fakespec.FakeProvisionerClient }{
				FakeProvisionerClient: fakespec.FakeProvisionerClient{
					FakeDriverRevokeBucketAccess: func(
						_ context.Context,
						_ *cosi.DriverRevokeBucketAccessRequest,
						_ ...grpc.CallOption,
					) (*cosi.DriverRevokeBucketAccessResponse, error) {
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

			listener := NewBucketAccessListener("test", &tc.driver)
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

// TestAddDeletedBucketAccess tests that a deleted BucketAccess does not
// trigger a call to the driver to grant access, and that no secrets are created.
func TestAddDeletedBucketAccess(t *testing.T) {
	driver := "driver"
	baName := "bucketaccess-deleted"
	ns := "testns"

	mpc := struct{ fakespec.FakeProvisionerClient }{}
	mpc.FakeDriverGrantBucketAccess = func(
		_ context.Context,
		_ *cosi.DriverGrantBucketAccessRequest,
		_ ...grpc.CallOption,
	) (*cosi.DriverGrantBucketAccessResponse, error) {
		t.Fatalf("driver Grant should NOT be called on deleted BA")
		return nil, nil
	}
	mpc.FakeDriverRevokeBucketAccess = func(
		_ context.Context,
		_ *cosi.DriverRevokeBucketAccessRequest,
		_ ...grpc.CallOption,
	) (*cosi.DriverRevokeBucketAccessResponse, error) {
		return &cosi.DriverRevokeBucketAccessResponse{}, nil
	}

	// minimal stub objects just to satisfy look-ups inside delete-path
	bac := &v1alpha1.BucketAccessClass{
		ObjectMeta: metav1.ObjectMeta{Name: "bac"},
		DriverName: driver,
	}
	claim := &v1alpha1.BucketClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "claim", Namespace: ns},
		Status: v1alpha1.BucketClaimStatus{
			BucketReady: true,
			BucketName:  "bucket",
		},
	}
	bucket := &v1alpha1.Bucket{
		ObjectMeta: metav1.ObjectMeta{Name: "bucket"},
		Status: v1alpha1.BucketStatus{
			BucketReady: true,
			BucketID:    "id",
		},
	}

	now := metav1.Now()
	ba := &v1alpha1.BucketAccess{
		ObjectMeta: metav1.ObjectMeta{
			Name:              baName,
			Namespace:         ns,
			DeletionTimestamp: &now,
			Finalizers:        []string{consts.BAFinalizer},
		},
		Spec: v1alpha1.BucketAccessSpec{
			BucketClaimName:       claim.Name,
			BucketAccessClassName: bac.Name,
			CredentialsSecretName: "creds",
		},
		Status: v1alpha1.BucketAccessStatus{
			AccountID:     "acc",
			AccessGranted: true,
		},
	}

	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "creds",
			Namespace:  ns,
			Finalizers: []string{consts.SecretFinalizer},
		},
		StringData: map[string]string{"dummy": "val"},
	}

	client := fakebucketclientset.NewSimpleClientset(bac, claim, bucket, ba)
	kubeClient := fakekubeclientset.NewSimpleClientset(secret)

	bal := BucketAccessListener{
		driverName:        driver,
		provisionerClient: &mpc,
		bucketClient:      client,
		kubeClient:        kubeClient,
	}

	if err := bal.Add(context.TODO(), ba); err != nil {
		t.Fatalf("Add returned error for deleted BucketAccess: %v", err)
	}

	if _, err := bal.secrets(ns).Get(context.TODO(), "creds", metav1.GetOptions{}); !kubeerrors.IsNotFound(err) {
		t.Fatalf("secret was not cleaned up, err=%v", err)
	}
}
