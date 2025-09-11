# Developing a COSI Driver

## Overview

The starting point for developing a COSI driver is the [COSI Driver Sample](https://github.com/kubernetes-sigs/cosi-driver-sample). This repository provides a foundational implementation that you can build upon.

## Implementing the Servers

A COSI driver requires implementing two main servers:

### Identity Server

The `IdentityServer` provides driver metadata and implements the following interface:

```go
type IdentityServer interface {
	DriverGetInfo(context.Context, *cosi.DriverGetInfoRequest) (*cosi.DriverGetInfoResponse, error)
}
```

### Provisioner Server

The `ProvisionerServer` handles bucket provisioning and access management:

```go
type ProvisionerServer interface {
	DriverCreateBucket(context.Context, *cosi.DriverCreateBucketRequest) (*cosi.DriverCreateBucketResponse, error)
	DriverDeleteBucket(context.Context, *cosi.DriverDeleteBucketRequest) (*cosi.DriverDeleteBucketResponse, error)
	DriverGrantBucketAccess(context.Context, *cosi.DriverGrantBucketAccessRequest) (*cosi.DriverGrantBucketAccessResponse, error)
	DriverRevokeBucketAccess(context.Context, *cosi.DriverRevokeBucketAccessRequest) (*cosi.DriverRevokeBucketAccessResponse, error)
}
```

## Entrypoint

The driver entrypoint initializes logging, parses flags, and starts the gRPC server:

```go
package main

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	if err := run(context.Background()); err != nil {
		klog.ErrorS(err, "Exiting on error")
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx,
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	cfg := config.Load() // placeholder

	identityServer := &driver.IdentityServer{Name: "cosi.example.com"}
	provisionerServer := &driver.ProvisionerServer{
		Config: cfg,
	}

	server, err := grpcServer(identityServer, provisionerServer)
	if err != nil {
		return fmt.Errorf("gRPC server creation failed: %w", err)
	}

	cosiEndpoint, ok := os.LookupEnv("COSI_ENDPOINT")
	if !ok {
		cosiEndpoint = "unix:///var/lib/cosi/cosi.sock"
	}

	lis, cleanup, err := listener(ctx, cosiEndpoint)
	if err != nil {
		return fmt.Errorf("failed to create listener for %s: %w", cosiEndpoint, err)
	}
	defer cleanup()

	var wg sync.WaitGroup
	wg.Add(1)
	go shutdown(ctx, &wg, server)

	if err = server.Serve(lis); err != nil {
		return fmt.Errorf("gRPC server failed: %w", err)
	}

	wg.Wait()
	return nil
}
```

### Creating Listener

The listener sets up a gRPC connection for handling requests:

```go
func listener(
	ctx context.Context,
	cosiEndpoint string,
) (net.Listener, func(), error) {
	endpointURL, err := url.Parse(cosiEndpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to parse COSI endpoint: %w", err)
	}

	listenConfig := net.ListenConfig{}

	if endpointURL.Scheme == "unix" {
		_ = os.Remove(endpointURL.Path) // Cleanup stale socket
	}

	listener, err := listenConfig.Listen(ctx, endpointURL.Scheme, endpointURL.Path)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to create listener: %w", err)
	}

	cleanup := func() {
		if endpointURL.Scheme == "unix" {
			if err := os.Remove(endpointURL.Path); err != nil {
				klog.ErrorS(err, "Failed to remove old socket")
			}
		}
	}

	return listener, cleanup, nil
}
```

### Creating gRPC Server

The gRPC server registers both the `IdentityServer` and `ProvisionerServer`:

```go
func grpcServer(
	identity cosi.IdentityServer,
	provisioner cosi.ProvisionerServer,
) (*grpc.Server, error) {
	server := grpc.NewServer()

	if identity == nil || provisioner == nil {
		return nil, errors.New("provisioner and identity servers cannot be nil")
	}

	cosi.RegisterIdentityServer(server, identity)
	cosi.RegisterProvisionerServer(server, provisioner)

	return server, nil
}
```

### Graceful Shutdown

To ensure clean shutdown, implement a graceful termination mechanism:

```go
const (
	gracePeriod = 5 * time.Second
)

func shutdown(
	ctx context.Context,
	wg *sync.WaitGroup,
	g *grpc.Server,
) {
	<-ctx.Done()
	defer wg.Done()
	defer klog.Info("Stopped")

	klog.Info("Shutting down")

	dctx, stop := context.WithTimeout(context.Background(), gracePeriod)
	defer stop()

	c := make(chan struct{})

	if g != nil {
		go func() {
			g.GracefulStop()
			c <- struct{}{}
		}()

		for {
			select {
			case <-dctx.Done():
				klog.Info("Forcing shutdown")
				g.Stop()
				return
			case <-c:
				return
			}
		}
	}
}
```
