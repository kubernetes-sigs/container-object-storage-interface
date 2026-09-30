// Copyright 2026 The Kubernetes Authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
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
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	cosi "sigs.k8s.io/container-object-storage-interface/proto"
)

type testIdentityServer struct {
	cosi.UnimplementedIdentityServer
}

func (testIdentityServer) DriverGetInfo(context.Context,
	*cosi.DriverGetInfoRequest) (*cosi.DriverGetInfoResponse, error) {
	return &cosi.DriverGetInfoResponse{Name: "test.cosi.example"}, nil
}

func testSocketPath(t *testing.T) string {
	t.Helper()
	// Keep the path short enough for Unix socket limits, including on macOS.
	dir, err := os.MkdirTemp("", "cosi-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove socket directory: %v", err)
		}
	})
	return filepath.Join(dir, "cosi.sock")
}

func testServer(t *testing.T, path string) *COSIProvisionerServer {
	t.Helper()
	server, err := NewDefaultCOSIProvisionerServer("unix://"+path,
		testIdentityServer{}, &cosi.UnimplementedProvisionerServer{})
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func TestRunUnixSocket(t *testing.T) {
	for _, stale := range []bool{false, true} {
		name := "fresh"
		if stale {
			name = "stale"
		}
		t.Run(name, func(t *testing.T) {
			path := testSocketPath(t)
			if stale {
				listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
				if err != nil {
					t.Fatal(err)
				}
				listener.SetUnlinkOnClose(false)
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			server := testServer(t, path)
			go func() { result <- server.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-result:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("Run returned %v, want context cancellation", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("server did not stop")
				}
			})
			rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer rpcCancel()
			conn, err := grpc.DialContext(rpcCtx, "unix://"+path,
				grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			info, err := cosi.NewIdentityClient(conn).DriverGetInfo(rpcCtx, &cosi.DriverGetInfoRequest{})
			if err != nil || info.GetName() != "test.cosi.example" {
				t.Fatalf("DriverGetInfo returned %v, %v", info, err)
			}
		})
	}
}

func TestRunPreservesExistingSocketPath(t *testing.T) {
	for _, kind := range []string{"active socket", "regular file", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			path := testSocketPath(t)
			switch kind {
			case "active socket":
				listener, err := net.Listen("unix", path)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
			case "regular file":
				if err := os.WriteFile(path, []byte("preserve me"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink("missing-target", path); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := testServer(t, path).Run(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Run should reject the occupied path, got %v", err)
			}
			after, err := os.Lstat(path)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("existing path was replaced: %v", err)
			}
			if kind == "active socket" {
				conn, err := net.DialTimeout("unix", path, time.Second)
				if err != nil {
					t.Fatalf("existing socket became unreachable: %v", err)
				}
				conn.Close()
			}
		})
	}
}
