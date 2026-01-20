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

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	cositest "sigs.k8s.io/container-object-storage-interface/internal/test"
	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
)

func Test_connectRpcAndGetDriverInfo(t *testing.T) {
	ctx := context.Background()
	grpcConnectDelay = 30 * time.Millisecond
	// ctrl.SetLogger(zap.New(zap.UseDevMode(true))) // uncomment locally to see debug logs

	t.Run("socket not unix protocol", func(t *testing.T) {
		conn, err := connectRpcAndGetDriverInfo(ctx, "/some/dir/cosi.sock")
		assert.Error(t, err)
		assert.Nil(t, conn)
	})

	t.Run("no .sock extension", func(t *testing.T) {
		conn, err := connectRpcAndGetDriverInfo(ctx, "unix:///some/dir/cosi.soc")
		assert.Error(t, err)
		assert.Nil(t, conn)
	})

	t.Run("s3 driver", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()

		identityServer := fakeIdentityServer{
			getInfoResponse: &cosiproto.DriverGetInfoResponse{
				Name: "s3.cosi.mydriver.net",
				SupportedProtocols: []*cosiproto.ObjectProtocol{
					{Type: cosiproto.ObjectProtocol_S3},
				},
			},
			getInfoErr: nil,
		}

		cleanup, serve, tmpSockUri, err := cositest.RpcServer(&identityServer, nil)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		driverInfo, err := connectRpcAndGetDriverInfo(timeoutCtx, tmpSockUri)
		assert.NoError(t, err)
		assert.Equal(t, "s3.cosi.mydriver.net", driverInfo.Name)
	})

	t.Run("azure driver", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()

		identityServer := fakeIdentityServer{
			getInfoResponse: &cosiproto.DriverGetInfoResponse{
				Name: "azure.cosi.mydriver.net",
				SupportedProtocols: []*cosiproto.ObjectProtocol{
					{Type: cosiproto.ObjectProtocol_AZURE},
				},
			},
			getInfoErr: nil,
		}

		cleanup, serve, tmpSockUri, err := cositest.RpcServer(&identityServer, nil)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		driverInfo, err := connectRpcAndGetDriverInfo(timeoutCtx, tmpSockUri)
		assert.NoError(t, err)
		assert.Equal(t, "azure.cosi.mydriver.net", driverInfo.Name)
	})

	t.Run("gcs driver", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()

		identityServer := fakeIdentityServer{
			getInfoResponse: &cosiproto.DriverGetInfoResponse{
				Name: "gcs.cosi.mydriver.net",
				SupportedProtocols: []*cosiproto.ObjectProtocol{
					{Type: cosiproto.ObjectProtocol_GCS},
				},
			},
			getInfoErr: nil,
		}

		cleanup, serve, tmpSockUri, err := cositest.RpcServer(&identityServer, nil)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		driverInfo, err := connectRpcAndGetDriverInfo(timeoutCtx, tmpSockUri)
		assert.NoError(t, err)
		assert.Equal(t, "gcs.cosi.mydriver.net", driverInfo.Name)
	})

	t.Run("getInfo error", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()

		identityServer := fakeIdentityServer{
			getInfoResponse: &cosiproto.DriverGetInfoResponse{
				Name: "s3.cosi.mydriver.net",
				SupportedProtocols: []*cosiproto.ObjectProtocol{
					{Type: cosiproto.ObjectProtocol_S3},
				},
			},
			getInfoErr: fmt.Errorf("fake error"),
		}

		cleanup, serve, tmpSockUri, err := cositest.RpcServer(&identityServer, nil)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		driverInfo, err := connectRpcAndGetDriverInfo(timeoutCtx, tmpSockUri)
		assert.ErrorContains(t, err, "fake error")
		assert.Nil(t, driverInfo)
	})

	t.Run("getInfo timeout", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()

		identityServer := fakeIdentityServer{
			getInfoSleep: 3 * time.Second,
			getInfoResponse: &cosiproto.DriverGetInfoResponse{
				Name: "s3.cosi.mydriver.net",
				SupportedProtocols: []*cosiproto.ObjectProtocol{
					{Type: cosiproto.ObjectProtocol_S3},
				},
			},
			getInfoErr: nil,
		}

		cleanup, serve, tmpSockUri, err := cositest.RpcServer(&identityServer, nil)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		driverInfo, err := connectRpcAndGetDriverInfo(timeoutCtx, tmpSockUri)
		assert.ErrorContains(t, err, "unable to get driver info")
		assert.Nil(t, driverInfo)
	})

	t.Run("invalid driver name", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()

		identityServer := fakeIdentityServer{
			getInfoResponse: &cosiproto.DriverGetInfoResponse{
				Name: "3",
				SupportedProtocols: []*cosiproto.ObjectProtocol{
					{Type: cosiproto.ObjectProtocol_S3},
				},
			},
			getInfoErr: nil,
		}

		cleanup, serve, tmpSockUri, err := cositest.RpcServer(&identityServer, nil)
		defer cleanup()
		require.NoError(t, err)
		go serve()

		driverInfo, err := connectRpcAndGetDriverInfo(timeoutCtx, tmpSockUri)
		assert.ErrorContains(t, err, "driver info is invalid")
		assert.Nil(t, driverInfo)
	})

	t.Run("driver never ready", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()

		cleanup, _, tmpSockUri, err := cositest.RpcServer(nil, nil)
		defer cleanup()
		require.NoError(t, err)
		// do not call serve()

		driverInfo, err := connectRpcAndGetDriverInfo(timeoutCtx, tmpSockUri)
		assert.ErrorContains(t, err, "timed out waiting for RPC client to connect")
		assert.Nil(t, driverInfo)
	})
}

func Test_connectRpc(t *testing.T) {
	ctx := context.Background()
	grpcConnectDelay = 30 * time.Millisecond
	// ctrl.SetLogger(zap.New(zap.UseDevMode(true))) // uncomment locally to see debug logs

	t.Run("server running", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()

		tmpDir := mkTempD(t)
		defer func() { _ = os.RemoveAll(tmpDir) }()
		sockPath := tmpDir + "/cosi.sock"
		sockUri := "unix://" + sockPath

		server := grpc.NewServer()
		go startServer(t, sockPath, server)
		defer server.Stop()

		conn, err := connectRpc(timeoutCtx, sockUri)
		assert.NoError(t, err)
		state := conn.GetState()
		assert.Equal(t, connectivity.Ready, state)
	})

	t.Run("server eventually runs", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()

		tmpDir := mkTempD(t)
		defer func() { _ = os.RemoveAll(tmpDir) }()
		sockPath := tmpDir + "/cosi.sock"
		sockUri := "unix://" + sockPath

		server := grpc.NewServer()
		go func() {
			time.Sleep(90 * time.Millisecond)
			startServer(t, sockPath, server)
		}()
		defer server.Stop()

		conn, err := connectRpc(timeoutCtx, sockUri)
		assert.NoError(t, err)
		state := conn.GetState()
		assert.Equal(t, connectivity.Ready, state)
	})

	t.Run("server never runs", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()

		tmpDir := mkTempD(t)
		defer func() { _ = os.RemoveAll(tmpDir) }()
		sockPath := tmpDir + "/cosi.sock"
		sockUri := "unix://" + sockPath

		conn, err := connectRpc(timeoutCtx, sockUri)
		assert.ErrorContains(t, err, "timed out waiting for RPC client to connect")
		assert.Nil(t, conn)
	})
}

// test helper to start gRPC server
func startServer(t *testing.T, sockPath string, server *grpc.Server) {
	t.Helper()

	listener, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	err = server.Serve(listener)
	require.NoError(t, err)
}

// test helper to create temp dir for server socket
func mkTempD(t *testing.T) string {
	// os.tempDir() path is really long sometimes (e.g., on macos)
	// unix socket has ~100 char limit, so need to keep tmpdir location short
	tmpDir, err := os.MkdirTemp("/tmp", "tempDir")
	require.NoError(t, err)
	return tmpDir
}

type fakeIdentityServer struct {
	cosiproto.UnimplementedIdentityServer

	getInfoSleep    time.Duration
	getInfoResponse *cosiproto.DriverGetInfoResponse
	getInfoErr      error
}

func (s *fakeIdentityServer) DriverGetInfo(
	context.Context, *cosiproto.DriverGetInfoRequest) (*cosiproto.DriverGetInfoResponse, error) {
	if s.getInfoSleep > 0 {
		time.Sleep(s.getInfoSleep)
	}
	return s.getInfoResponse, s.getInfoErr
}
