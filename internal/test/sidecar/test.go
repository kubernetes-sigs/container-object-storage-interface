/*
Copyright 2026 The Kubernetes Authors.

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

// Package sidecar contains utilities for unit testing that help approximate COSI Sidecar
// behaviors.
package sidecar

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cosiapi "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha2"
	cositest "sigs.k8s.io/container-object-storage-interface/internal/test"
	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
	sidecar "sigs.k8s.io/container-object-storage-interface/sidecar/pkg/reconciler"
)

// ReconcileBucket reconciles the Bucket with the given namespaced name for unit tests.
// Bucket should exist (if desired) in the bootstrapped dependencies's client.
// It is suitable for unit testing behavior that relies on a Bucket to be reconciled as a
// prerequisite. It is not suitable for unit testing Bucket reconciliation.
func ReconcileBucket(
	t *testing.T,
	bootstrapped *cositest.Dependencies,
	fakeServer *cositest.FakeProvisionerServer,
	driverInfo sidecar.DriverInfo, // minus the RPC client
	nsName types.NamespacedName,
) (*cosiapi.Bucket, error) {
	cleanup, serve, tmpSock, err := cositest.RpcServer(nil, fakeServer)
	defer cleanup()
	require.NoError(t, err)
	go serve()

	conn, err := cositest.RpcClientConn(tmpSock)
	require.NoError(t, err)
	rpcClient := cosiproto.NewProvisionerClient(conn)

	r := sidecar.BucketReconciler{
		Client:     bootstrapped.Client,
		Scheme:     bootstrapped.Client.Scheme(),
		DriverInfo: driverInfo,
	}
	r.DriverInfo.ProvisionerClient = rpcClient

	_, err = r.Reconcile(bootstrapped.ContextWithLogger, ctrl.Request{NamespacedName: nsName})
	if err != nil {
		return nil, err
	}

	bucket := &cosiapi.Bucket{}
	if err := bootstrapped.Client.Get(bootstrapped.ContextWithLogger, nsName, bucket); err != nil {
		return nil, err
	}
	return bucket, nil
}

// ReconcileOpinionatedS3Bucket reconciles the Bucket with the given namespaced name for unit tests.
// It uses a configurations that are compatible with the opinionated S3 driver and BucketClass.
// It is suitable for unit testing behavior that relies on a Bucket to be reconciled as a
// prerequisite. It is not suitable for unit testing Bucket reconciliation.
func ReconcileOpinionatedS3Bucket(
	t *testing.T,
	bootstrapped *cositest.Dependencies,
	nsName types.NamespacedName,
) (*cosiapi.Bucket, error) {
	fakeServer := cositest.FakeProvisionerServer{
		// nolint:lll // long line is fine for test code
		CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
			ret := &cosiproto.DriverCreateBucketResponse{
				BucketId: "cosi-" + dcbr.Name,
				Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
					S3: &cosiproto.S3BucketInfo{
						Endpoint:        cositest.OpinionatedS3BucketClass().Spec.DriverName,
						BucketId:        "cosi-" + dcbr.Name,
						Region:          "us-east-1",
						AddressingStyle: &cosiproto.S3AddressingStyle{Style: cosiproto.S3AddressingStyle_PATH},
					},
				},
			}
			return ret, nil
		},
	}

	driverInfo := sidecar.DriverInfo{
		Name: cositest.OpinionatedS3BucketClass().Spec.DriverName,
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{
			cosiproto.ObjectProtocol_S3,
		},
	}

	return ReconcileBucket(t, bootstrapped, &fakeServer, driverInfo, nsName)
}

// ReconcileOpinionatedGcsBucket reconciles the Bucket with the given namespaced name for unit tests.
// It uses a configurations that are compatible with the opinionated GCS driver and BucketClass.
// It is suitable for unit testing behavior that relies on a Bucket to be reconciled as a
// prerequisite. It is not suitable for unit testing Bucket reconciliation.
func ReconcileOpinionatedGcsBucket(
	t *testing.T,
	bootstrapped *cositest.Dependencies,
	nsName types.NamespacedName,
) (*cosiapi.Bucket, error) {
	fakeServer := cositest.FakeProvisionerServer{
		// nolint:lll // long line is fine for test code
		CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
			ret := &cosiproto.DriverCreateBucketResponse{
				BucketId: "cosi-" + dcbr.Name,
				Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
					Gcs: &cosiproto.GcsBucketInfo{
						ProjectId:  "cosi",
						BucketName: "cosi-" + dcbr.Name,
					},
				},
			}
			return ret, nil
		},
	}

	driverInfo := sidecar.DriverInfo{
		Name: cositest.OpinionatedGcsBucketClass().Spec.DriverName,
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{
			cosiproto.ObjectProtocol_GCS,
		},
	}

	return ReconcileBucket(t, bootstrapped, &fakeServer, driverInfo, nsName)
}

// ReconcileOpinionatedAzureBucket reconciles the Bucket with the given namespaced name for unit tests.
// It uses a configurations that are compatible with the opinionated Azure driver and BucketClass.
// It is suitable for unit testing behavior that relies on a Bucket to be reconciled as a
// prerequisite. It is not suitable for unit testing Bucket reconciliation.
func ReconcileOpinionatedAzureBucket(
	t *testing.T,
	bootstrapped *cositest.Dependencies,
	nsName types.NamespacedName,
) (*cosiapi.Bucket, error) {
	fakeServer := cositest.FakeProvisionerServer{
		// nolint:lll // long line is fine for test code
		CreateBucketFunc: func(ctx context.Context, dcbr *cosiproto.DriverCreateBucketRequest) (*cosiproto.DriverCreateBucketResponse, error) {
			ret := &cosiproto.DriverCreateBucketResponse{
				BucketId: "cosi-" + dcbr.Name,
				Protocols: &cosiproto.ObjectProtocolAndBucketInfo{
					Azure: &cosiproto.AzureBucketInfo{
						StorageAccount: "cosi",
					},
				},
			}
			return ret, nil
		},
	}

	driverInfo := sidecar.DriverInfo{
		Name: cositest.OpinionatedAzureBucketClass().Spec.DriverName,
		SupportedProtocols: []cosiproto.ObjectProtocol_Type{
			cosiproto.ObjectProtocol_AZURE,
		},
	}

	return ReconcileBucket(t, bootstrapped, &fakeServer, driverInfo, nsName)
}
