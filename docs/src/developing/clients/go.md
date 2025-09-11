# Developing Client Apps in Go

## Configuration Structure

The Config struct is the primary configuration object for the storage package. It encapsulates all necessary settings for interacting with different storage providers. This design ensures that all configuration details are centralized and easily maintainable, allowing your application to switch storage backends with minimal code changes.

The nested `Spec` struct defines both generic and provider-specific parameters:

* `BucketName`: Specifies the target storage container or bucket. This value directs where the data will be stored or retrieved.
* `AuthenticationType`: Indicates the method of authentication (for example, "key"). This ensures that the correct credentials are used when accessing a storage provider.
* `Protocols`: An array of strings that informs the system which storage protocols (e.g., "s3" or "azure") are supported. The factory uses this to determine the appropriate client to initialize.
* `SecretS3` / `SecretAzure`: These fields hold pointers to the respective secret structures needed for authenticating with S3 or Azure services. Their presence is conditional on the protocols configured.

```go
// import "example.com/pkg/storage"
package storage

type Config struct {
	Spec Spec `json:"spec"`
}

type Spec struct {
	BucketName         string             `json:"bucketName"`
	AuthenticationType string             `json:"authenticationType"`
	Protocols          []string           `json:"protocols"`
	SecretS3           *s3.SecretS3       `json:"secretS3,omitempty"`
	SecretAzure        *azure.SecretAzure `json:"secretAzure,omitempty"`
}
```

### Azure Secret Structure

The `SecretAzure` struct holds authentication credentials for accessing Azure-based storage services. It is essential when interacting with Azure Blob storage, as it contains a shared access token along with an expiration timestamp. The inclusion of the `ExpiryTimestamp` allows your application to check token validity. 

> While current COSI implementation doesn't auto-renew tokens, the `ExpiryTimestamp` provides hooks for future refresh logic.

```go
// import "example.com/pkg/storage/azure"
package azure

type SecretAzure struct {
	AccessToken     string    `json:"accessToken"`
	ExpiryTimestamp time.Time `json:"expiryTimeStamp"`
}
```

### S3 Secret Structure

The `SecretS3` struct holds authentication credentials for accessing S3-compatible storage services. This struct includes the endpoint, region, and access credentials required to securely interact with the S3 service. By isolating these values into a dedicated structure, the design helps maintain clear separation between configuration types, thus enhancing code clarity.

```go
// import "example.com/pkg/storage/s3"
package s3

type SecretS3 struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"accessKeyID"`
	AccessSecretKey string `json:"accessSecretKey"`
}
```

## Factory

The factory pattern[^1] is used to instantiate the appropriate storage backend based on the provided configuration. We will hide the implementation behind the interface.

The factory function examines the configuration’s `Protocols` array and validates the `AuthenticationType` along with the corresponding secret. It then returns a concrete implementation of the Storage interface. This method of instantiation promotes extensibility, making it easier to support additional storage protocols in the future, as the COSI specification evolves.

Here is a minimal interface that supports only basic `Delete`/`Get`/`Put` operations:

```go
type Storage interface {
	Delete(ctx context.Context, key string) error
	Get(ctx context.Context, key string, wr io.Writer) error
	Put(ctx context.Context, key string, data io.Reader, size int64) error
}
```

Our implementation of factory method can be defined as following:

```go
// import "example.com/pkg/storage"
package storage

import (
	"fmt"
	"slices"
	"strings"

	"example.com/pkg/storage/azure"
	"example.com/pkg/storage/s3"
)

func New(config Config, ssl bool) (Storage, error) {
	if slices.ContainsFunc(config.Spec.Protocols, func(s string) bool { return strings.EqualFold(s, "s3") }) {
		if !strings.EqualFold(config.Spec.AuthenticationType, "key") {
			return nil, fmt.Errorf("invalid authentication type for s3")
		}

		s3secret := config.Spec.SecretS3
		if s3secret == nil {
			return nil, fmt.Errorf("s3 secret missing")
		}

		return s3.New(config.Spec.BucketName, *s3secret, ssl)
	}

	if slices.ContainsFunc(config.Spec.Protocols, func(s string) bool { return strings.EqualFold(s, "azure") }) {
		if !strings.EqualFold(config.Spec.AuthenticationType, "key") {
			return nil, fmt.Errorf("invalid authentication type for azure")
		}

		azureSecret := config.Spec.SecretAzure
		if azureSecret == nil {
			return nil, fmt.Errorf("azure secret missing")
		}

		return azure.New(config.Spec.BucketName, *azureSecret)
	}

	return nil, fmt.Errorf("invalid protocol (%v)", config.Spec.Protocols)
}
```

## Clients

As we alredy defined the factory and uppermost configuration, let's get into the details of the clients, that will implement the `Storage` interface.

### S3

In the implementation of S3 client, we will use [MinIO](https://github.com/minio/minio-go) client library, as it's more lightweight than [AWS SDK](https://github.com/aws/aws-sdk-go-v2).

```go
// import "example.com/pkg/storage/s3"
package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Client struct {
	s3cli      *minio.Client
	bucketName string
}

func New(bucketName string, s3secret SecretS3, ssl bool) (*Client, error) {
	s3cli, err := minio.New(s3secret.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(s3secret.AccessKeyID, s3secret.AccessSecretKey, ""),
		Region: s3secret.Region,
		Secure: ssl,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create client: %w", err)
	}

	return &Client{
		s3cli:      s3cli,
		bucketName: bucketName,
	}, nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	return c.s3cli.RemoveObject(ctx, c.bucketName, key, minio.RemoveObjectOptions{})
}

func (c *Client) Get(ctx context.Context, key string, wr io.Writer) error {
	obj, err := c.s3cli.GetObject(ctx, c.bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	_, err = io.Copy(wr, obj)
	return err
}

func (c *Client) Put(ctx context.Context, key string, data io.Reader, size int64) error {
	_, err := c.s3cli.PutObject(ctx, c.bucketName, key, data, size, minio.PutObjectOptions{})
	return err
}
```

### Azure Blob

In the implementation of Azure client, we will use [Azure SDK](https://github.com/Azure/azure-sdk-for-go) client library. Note, that the configuration is done with `NoCredentials` client, as the Azure secret contains shared access signatures (SAS)[^2].

```go
// import "example.com/pkg/storage/azure"
package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type Client struct {
	azCli         *azblob.Client
	containerName string
}

func New(containerName string, azureSecret SecretAzure) (*Client, error) {
	azCli, err := azblob.NewClientWithNoCredential(azureSecret.AccessToken, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create client: %w", err)
	}

	return &Client{
		azCli:         azCli,
		containerName: containerName,
	}, nil
}

func (c *Client) Delete(ctx context.Context, blobName string) error {
	_, err := c.azCli.DeleteBlob(ctx, c.containerName, blobName, nil)
	return err
}

func (c *Client) Get(ctx context.Context, blobName string, wr io.Writer) error {
	stream, err := c.azCli.DownloadStream(ctx, c.containerName, blobName, nil)
	if err != nil {
		return fmt.Errorf("unable to get download stream: %w", err)
	}
	_, err = io.Copy(wr, stream.Body)
	return err
}

func (c *Client) Put(ctx context.Context, blobName string, data io.Reader, size int64) error {
	_, err := c.azCli.UploadStream(ctx, c.containerName, blobName, data, nil)
	return err
}
```

## Summing up

Once all components are in place, using the storage package in your application becomes straightforward. The process starts with reading a JSON configuration file, which is then decoded into the `Config` struct. The factory method selects and initializes the appropriate storage client based on the configuration, enabling seamless integration with either S3 or Azure storage.

```go
import (
	"encoding/json"
	"os"

	"example.com/pkg/storage"
)

func example() {
	f, err := os.Open("/opt/cosi/BucketInfo")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var cfg storage.Config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		panic(err)
	}

	client, err := storage.New(cfg, true)
	if err != nil {
		panic(err)
	}

	// use client Put/Get/Delete
	// ...
}
```

[^1]: [https://en.wikipedia.org/wiki/Factory_method_pattern](https://en.wikipedia.org/wiki/Factory_method_pattern)
[^2]: [https://learn.microsoft.com/en-us/azure/storage/common/storage-sas-overview](https://en.wikipedia.org/wiki/Factory_method_pattern)
