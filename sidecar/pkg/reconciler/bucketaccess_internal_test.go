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
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	cosierr "sigs.k8s.io/container-object-storage-interface/internal/errors"
	cositest "sigs.k8s.io/container-object-storage-interface/internal/test"
	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
)

func TestBucketAccessReconciler_generateAccountID(t *testing.T) {
	baseAccess := cosiapi.BucketAccess{
		ObjectMeta: meta.ObjectMeta{
			Name:      "my-access",
			Namespace: "my-ns",
			UID:       "zxcvbn",
		},
		Spec: cosiapi.BucketAccessSpec{
			BucketAccessClassName: "s3-class",
			Protocol:              cosiapi.ObjectProtocolS3,
		},
	}

	accessNsName := types.NamespacedName{Name: "my-access", Namespace: "my-ns"}

	// DriverGenerateBucketAccessId receives the same configuration DriverGrantBucketAccess will
	grantCfg := &internalGrantAccessConfig{
		internalAccessConfig: internalAccessConfig{
			Protocol:           cosiproto.ObjectProtocol_S3,
			AuthenticationType: cosiproto.AuthenticationType_KEY,
			Parameters:         map[string]string{"maxSize": "10Gi"},
		},
		AccountName: "ba-zxcvbn",
		AccessConfigsByBucketId: map[string]bucketGrantAccessConfig{
			"bc-qwerty": {
				ObjectDataMode:     cosiproto.AccessMode_READ_WRITE,
				ObjectMetadataMode: cosiproto.AccessMode_READ_WRITE,
				BucketMetadataMode: cosiproto.AccessMode_READ_WRITE,
			},
		},
	}

	// newReconciler wires a reconciler to an RPC server backed by generateFunc, and returns the
	// reconciler plus the BucketAccess it operates on. wrap may be nil.
	// nolint:lll
	newReconciler := func(t *testing.T, generateFunc func(context.Context, *cosiproto.DriverGenerateBucketAccessIdRequest) (*cosiproto.DriverGenerateBucketAccessIdResponse, error), wrap func(client.WithWatch) client.Client) (*BucketAccessReconciler, *cosiapi.BucketAccess, context.Context) {
		t.Helper()

		fakeServer := cositest.FakeProvisionerServer{GenerateBucketAccessIdFunc: generateFunc}

		cleanup, serve, tmpSock, err := cositest.RpcServer(nil, &fakeServer)
		t.Cleanup(cleanup)
		require.NoError(t, err)
		go serve()

		conn, err := cositest.RpcClientConn(tmpSock)
		require.NoError(t, err)

		bootstrapped := cositest.MustBootstrap(t, baseAccess.DeepCopy())

		var c client.Client = bootstrapped.Client
		if wrap != nil {
			withWatch, ok := c.(client.WithWatch)
			require.True(t, ok)
			c = wrap(withWatch)
		}

		r := &BucketAccessReconciler{
			Client: c,
			Scheme: c.Scheme(),
			DriverInfo: DriverInfo{
				Name:               "cosi.s3.corp.net",
				SupportedProtocols: []cosiproto.ObjectProtocol_Type{cosiproto.ObjectProtocol_S3},
				ProvisionerClient:  cosiproto.NewProvisionerClient(conn),
			},
		}

		access := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(bootstrapped.ContextWithLogger, accessNsName, access))
		return r, access, bootstrapped.ContextWithLogger
	}

	assertNothingPersisted := func(t *testing.T, r *BucketAccessReconciler, ctx context.Context) {
		t.Helper()
		persisted := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, accessNsName, persisted))
		assert.Empty(t, persisted.Status.AccountID)
	}

	// A successful call returns the driver's ID. The request carries the same configuration Grant
	// will receive.
	t.Run("ID generated", func(t *testing.T) {
		// Setup: fake driver that echoes back a deterministic ID derived from the request name.
		seenReq := []*cosiproto.DriverGenerateBucketAccessIdRequest{}
		r, _, ctx := newReconciler(t, func(ctx context.Context, req *cosiproto.DriverGenerateBucketAccessIdRequest) (*cosiproto.DriverGenerateBucketAccessIdResponse, error) {
			seenReq = append(seenReq, req)
			return &cosiproto.DriverGenerateBucketAccessIdResponse{AccountId: "cosi-" + req.AccountName}, nil
		}, nil)

		// Act: run DriverGenerateBucketAccessId in isolation.
		accountID, err := r.generateAccountID(ctx, logr.Discard(), grantCfg)
		assert.NoError(t, err)

		// Validate: the driver is asked for an ID using the same configuration Grant will receive.
		require.Len(t, seenReq, 1)
		assert.Equal(t, "ba-zxcvbn", seenReq[0].AccountName)
		assert.Equal(t, cosiproto.ObjectProtocol_S3, seenReq[0].Protocol.Type)
		assert.Equal(t, cosiproto.AuthenticationType_KEY, seenReq[0].AuthenticationType.Type)
		assert.Equal(t, map[string]string{"maxSize": "10Gi"}, seenReq[0].Parameters)
		require.Len(t, seenReq[0].Buckets, 1)
		assert.Equal(t, "bc-qwerty", seenReq[0].Buckets[0].BucketId)

		// generateAccountID only returns the ID; recording it is the caller's job, done in
		// reconcile() so it happens before granting access.
		assert.Equal(t, "cosi-ba-zxcvbn", accountID)
		assertNothingPersisted(t, r, ctx)
	})

	// A driver that returns OK without an account_id is rejected as non-retryable.
	t.Run("generated ID missing", func(t *testing.T) {
		// Setup: fake driver returns an OK response with an empty account_id.
		r, _, ctx := newReconciler(t, func(ctx context.Context, req *cosiproto.DriverGenerateBucketAccessIdRequest) (*cosiproto.DriverGenerateBucketAccessIdResponse, error) {
			return &cosiproto.DriverGenerateBucketAccessIdResponse{AccountId: ""}, nil
		}, nil)

		// Act: run DriverGenerateBucketAccessId in isolation.
		_, err := r.generateAccountID(ctx, logr.Discard(), grantCfg)
		// Validate: rejected as non-retryable; nothing persisted.
		assert.Error(t, err)
		assert.ErrorContains(t, err, "generated account ID missing")
		assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
		assertNothingPersisted(t, r, ctx)
	})

	// A retryable driver error is returned as-is so the reconcile is requeued.
	t.Run("retryable RPC error", func(t *testing.T) {
		// Setup: fake driver returns a gRPC code that classifies as retryable (codes.Unknown).
		r, _, ctx := newReconciler(t, func(ctx context.Context, req *cosiproto.DriverGenerateBucketAccessIdRequest) (*cosiproto.DriverGenerateBucketAccessIdResponse, error) {
			return nil, status.Error(codes.Unknown, "fake unknown err")
		}, nil)

		// Act: run DriverGenerateBucketAccessId in isolation.
		_, err := r.generateAccountID(ctx, logr.Discard(), grantCfg)
		// Validate: the raw error is returned unwrapped, so controller-runtime will requeue.
		assert.Error(t, err)
		assert.ErrorContains(t, err, "fake unknown err")
		assert.NotErrorIs(t, err, cosierr.NonRetryableError(nil))
		assertNothingPersisted(t, r, ctx)
	})

	// The codes proto/spec.md documents under "Important driver return codes" for this RPC, plus
	// Unimplemented, which a sidecar upgraded ahead of its driver receives. All MUST be terminal:
	// retrying any of them against an unchanged request cannot succeed.
	for _, tc := range []struct {
		name string
		code codes.Code
		msg  string
	}{
		{"invalid argument RPC error", codes.InvalidArgument, "fake invalid arg err"},
		{"already exists RPC error", codes.AlreadyExists, "fake already exists err"},
		{"out of range RPC error", codes.OutOfRange, "fake out of range err"},
		{"driver does not implement the RPC", codes.Unimplemented, "unknown method DriverGenerateBucketAccessId"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Setup: fake driver returns a gRPC code that classifies as non-retryable.
			r, _, ctx := newReconciler(t, func(ctx context.Context, req *cosiproto.DriverGenerateBucketAccessIdRequest) (*cosiproto.DriverGenerateBucketAccessIdResponse, error) {
				return nil, status.Error(tc.code, tc.msg)
			}, nil)

			// Act: run DriverGenerateBucketAccessId in isolation.
			_, err := r.generateAccountID(ctx, logr.Discard(), grantCfg)
			// Validate: wrapped as a NonRetryableError; nothing persisted.
			assert.Error(t, err)
			assert.ErrorContains(t, err, tc.msg)
			assert.ErrorIs(t, err, cosierr.NonRetryableError(nil))
			assertNothingPersisted(t, r, ctx)
		})
	}

	// A conflict while recording the ID is retryable and leaves nothing persisted.
	t.Run("status update fails with a retryable conflict", func(t *testing.T) {
		// Setup: intercept the status subresource update to force a Conflict response, simulating
		// a concurrent writer racing this update.
		r, access, ctx := newReconciler(t, nil, func(c client.WithWatch) client.Client {
			return interceptor.NewClient(c, interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					return apierrors.NewConflict(
						schema.GroupResource{Group: cosiapi.GroupVersion.Group, Resource: "bucketaccesses"},
						obj.GetName(),
						fmt.Errorf("the object has been modified; please apply your changes to the latest version and try again"),
					)
				},
			})
		})

		// Act: persist a generated ID.
		err := r.recordAccountId(ctx, logr.Discard(), access, "cosi-ba-zxcvbn")
		// Validate: left retryable, since a refreshed object can clear a conflict.
		assert.Error(t, err)
		assert.NotErrorIs(t, err, cosierr.NonRetryableError(nil))
		assertNothingPersisted(t, r, ctx)
	})

	// Recording persists the ID with readyToUse=false, since no backend access exists yet.
	t.Run("ID recorded with readyToUse=false", func(t *testing.T) {
		r, access, ctx := newReconciler(t, nil, nil)

		require.NoError(t, r.recordAccountId(ctx, logr.Discard(), access, "cosi-ba-zxcvbn"))

		persisted := &cosiapi.BucketAccess{}
		require.NoError(t, r.Get(ctx, accessNsName, persisted))
		assert.Equal(t, "cosi-ba-zxcvbn", persisted.Status.AccountID)
		assert.Equal(t, ptr.To(false), persisted.Status.ReadyToUse)
	})
}

func Test_validateAccountID(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		isValid bool
	}{
		{"empty ID", "", false}, // pattern requires at least one char
		{"single lowercase letter", "a", true},
		{"single uppercase letter", "Z", true}, // unlike driver names, upper case is allowed
		{"single digit", "0", true},
		{"single dot", ".", true},
		{"single underscore", "_", true},
		{"single dash", "-", true},
		{"single forward slash", "/", true},
		{"single space", " ", false},
		{"NUL character", string([]byte{0}), false},
		{"all allowed character types combined", "account-id.123_abc/def", true},
		{"embedded space in otherwise valid ID", "account id", false},
		{"length equal to the maximum", strings.Repeat("a", 2048), true},
		{"length one over the maximum", strings.Repeat("a", 2049), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr := validateAccountID(tt.id)
			if !tt.isValid {
				assert.Error(t, gotErr)
			} else {
				assert.NoError(t, gotErr)
			}
		})
	}
}
