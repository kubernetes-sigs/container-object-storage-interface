// Copyright 2021 The Kubernetes Authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package provisioner

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
)

type COSIProvisionerServer struct {
	address           string
	identityServer    cosi.IdentityServer
	provisionerServer cosi.ProvisionerServer

	listenOpts []grpc.ServerOption
}

func (s *COSIProvisionerServer) Run(ctx context.Context) error {
	addr, err := url.Parse(s.address)
	if err != nil {
		return err
	}

	if addr.Scheme != "unix" {
		err := errors.New("Address must be a unix domain socket")
		klog.ErrorS(err, "Unsupported scheme", "expected", "unix", "found", addr.Scheme)
		return fmt.Errorf("invalid argument: %w", err)
	}

	if err := removeStaleSocket(ctx, addr.Path); err != nil {
		return fmt.Errorf("failed to prepare server socket: %w", err)
	}

	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(ctx, "unix", addr.Path)
	if err != nil {
		klog.ErrorS(err, "Failed to start server")
		return fmt.Errorf("failed to start server: %w", err)
	}
	defer listener.Close()

	server := grpc.NewServer(s.listenOpts...)

	if s.provisionerServer == nil || s.identityServer == nil {
		err := errors.New("ProvisionerServer and IdentityServer cannot be nil")
		klog.ErrorS(err, "Invalid args")
		return fmt.Errorf("invalid args: %w", err)
	}

	cosi.RegisterIdentityServer(server, s.identityServer)
	cosi.RegisterProvisionerServer(server, s.provisionerServer)

	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		server.GracefulStop()
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

// removeStaleSocket removes a socket left behind by an unclean shutdown.
// An active listener or any other kind of file must not be replaced.
func removeStaleSocket(ctx context.Context, path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("socket path %q is not a socket", path)
	}

	dialer := net.Dialer{Timeout: time.Second}
	conn, err := dialer.DialContext(ctx, "unix", path)
	if err == nil {
		conn.Close()
		return fmt.Errorf("socket %q already has an active listener", path)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("failed to check socket %q: %w", path, err)
	}

	// Avoid removing a file that replaced the socket while it was being checked.
	current, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !os.SameFile(info, current) {
		return fmt.Errorf("socket path %q changed while checking it", path)
	}
	return os.Remove(path)
}
