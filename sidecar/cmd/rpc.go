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
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	cosiproto "sigs.k8s.io/container-object-storage-interface/proto"
	"sigs.k8s.io/container-object-storage-interface/sidecar/pkg/reconciler"
)

var (
	// max connection backoff delay - overrideable to speed up unit tests
	grpcConnectDelay = 1 * time.Second
)

func connectRpcAndGetDriverInfo(ctx context.Context, rpcEndpoint string) (*reconciler.DriverInfo, error) {
	if !strings.HasPrefix(rpcEndpoint, "unix://") {
		return nil, fmt.Errorf("rpc endpoint must be a unix socket prefix 'unix://': %s", rpcEndpoint)
	}
	if !strings.HasSuffix(rpcEndpoint, ".sock") {
		return nil, fmt.Errorf("rpc endpoint must be a unix socket with extension '.sock': %s", rpcEndpoint)
	}

	// establish a timeout for RPC connection and driver info retrieval
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 120*time.Second)
	defer timeoutCancel()

	rpcConn, err := connectRpc(timeoutCtx, rpcEndpoint)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to RPC endpoint %q: %w", rpcEndpoint, err)
	}

	client := cosiproto.NewIdentityClient(rpcConn)
	driverResponse, err := client.DriverGetInfo(timeoutCtx, &cosiproto.DriverGetInfoRequest{})
	if err != nil {
		return nil, fmt.Errorf("unable to get driver info: %w", err)
	}

	validatedInfo, err := reconciler.ValidateAndSetDriverConnectionInfo(driverResponse, rpcConn)
	if err != nil {
		return nil, fmt.Errorf("driver info is invalid: %w", err)
	}

	return validatedInfo, nil
}

func connectRpc(timeoutCtx context.Context, rpcEndpoint string) (*grpc.ClientConn, error) {
	bc := backoff.DefaultConfig
	bc.BaseDelay = grpcConnectDelay
	bc.MaxDelay = grpcConnectDelay // no need to backoff because not connected over network
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), // no TLS because restricted to unix sockets
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bc}),  // retry every second after failure
		grpc.WithIdleTimeout(time.Duration(0)),                   // never close connection because of inactivity
	}

	conn, err := grpc.NewClient(rpcEndpoint, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("unable to create gRPC client: %w", err)
	}

	// TODO: do we need WithContextDialer() to handle reconnects if/when the driver
	//   (gRPC server) pod crashes and restarts? (if so don't use timeoutCtx)
	// TODO: add interceptor that logs gRPC calls?
	// TODO: add metrics to gRPC calls?
	// TODO: could possibly consume CSI lib for all/some of the above, but needs investigation
	//   ref: https://github.com/kubernetes-csi/csi-lib-utils/blob/master/connection/connection.go

	// grpc.WithBlock() is strongly deprecated - validate connection following anti-patterns doc:
	// https://github.com/grpc/grpc-go/blob/master/Documentation/anti-patterns.md
	state := conn.GetState()
	if state != connectivity.Idle { // should be idle when first connected
		return nil, fmt.Errorf("gRPC client is not idle: %v", state)
	}
	conn.Connect()
	for { // wait for client to become ready
		if !conn.WaitForStateChange(timeoutCtx, state) {
			return nil, fmt.Errorf("timed out waiting for RPC client to connect")
		}
		state = conn.GetState()
		logger.V(1).Info("RPC connection state change", "time", time.Now(), "state", state)
		if state == connectivity.Ready {
			break // SUCCESS
		}
	}

	return conn, nil
}
