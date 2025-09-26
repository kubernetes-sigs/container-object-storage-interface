# Container Object Storage Interface (COSI)

Authors:

* Blaine Gardner [@BlaineEXE](https://github.com/BlaineEXE)
* Mateusz Urbanek [@shanduur](https://github.com/shanduur)
* Sidharth Mani [@wlan0](https://github.com/wlan0)
* Jeff Vance [@jeffvance](https://github.com/jeffvance)
* Srini Brahmaroutu [@brahmaroutu](https://github.com/brahmaroutu)

## Notational Conventions

The keywords "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and "OPTIONAL" are to be interpreted as described in [RFC 2119](http://tools.ietf.org/html/rfc2119) (Bradner, S., "Key words for use in RFCs to Indicate Requirement Levels", BCP 14, RFC 2119, March 1997).

The key words "unspecified", "undefined", and "implementation-defined" are to be interpreted as described in the [rationale for the C99 standard](http://www.open-std.org/jtc1/sc22/wg14/www/C99RationaleV5.10.pdf#page=18).

An implementation is not compliant if it fails to satisfy one or more of the MUST, REQUIRED, or SHALL requirements for the protocols it implements.
An implementation is compliant if it satisfies all the MUST, REQUIRED, and SHALL requirements for the protocols it implements.

## Objective

This document is the primary gRPC spec for the Container Object Storage Interface (COSI).
`cosi.proto` is generated from this file.
COSI's design is approved by Kubernetes sig-storage.
The latest approved and merged design can be found in [kubernetes/enhancements](https://github.com/kubernetes/enhancements/tree/master/keps/sig-storage/1979-object-storage-support).
The COSI KEP version targeted by this document is [`v1alpha2`](https://github.com/kubernetes/enhancements/pull/4599).

Because the KEP design document is the primary source of truth, this document avoids repeating unnecessary information.
Concise information that serves as useful documentation for driver implementers may be duplicated.

## Container Object Storage Interface

This section describes the interface between the COSI system and vendor driver Plugins.

### RPC Interface

COSI interacts with a Plugin through RPCs.
Each Plugin MUST provide:

* **Plugin**: A gRPC endpoint serving COSI RPCs that MAY be run anywhere.

```protobuf
syntax = "proto3";
package sigs.k8s.io.cosi.v1alpha2;

import "google/protobuf/descriptor.proto";

option go_package = "sigs.k8s.io/container-object-storage-interface/proto;cosi";

extend google.protobuf.EnumOptions {
    // Indicates that this enum is OPTIONAL and part of an experimental
    // API that may be deprecated and eventually removed between minor
    // releases.
    bool alpha_enum = 1116;
}

extend google.protobuf.EnumValueOptions {
    // Indicates that this enum value is OPTIONAL and part of an
    // experimental API that may be deprecated and eventually removed
    // between minor releases.
    bool alpha_enum_value = 1116;
}

extend google.protobuf.FieldOptions {
    // Indicates that a field MAY contain information that is sensitive
    // and MUST be treated as such (e.g. not logged).
    bool cosi_secret = 1115;

    // Indicates that this field is OPTIONAL and part of an experimental
    // API that may be deprecated and eventually removed between minor
    // releases.
    bool alpha_field = 1116;
}

extend google.protobuf.MessageOptions {
    // Indicates that this message is OPTIONAL and part of an experimental
    // API that may be deprecated and eventually removed between minor
    // releases.
    bool alpha_message = 1116;
}

extend google.protobuf.MethodOptions {
    // Indicates that this method is OPTIONAL and part of an experimental
    // API that may be deprecated and eventually removed between minor
    // releases.
    bool alpha_method = 1116;
}

extend google.protobuf.ServiceOptions {
    // Indicates that this service is OPTIONAL and part of an experimental
    // API that may be deprecated and eventually removed between minor
    // releases.
    bool alpha_service = 1116;
}
```

There are three sets of RPCs:

* **Identity Service**: Plugin MUST implement this set of RPCs.
* **Provisioner Service**: Plugin MUST implement this set of RPCs.

```protobuf
service Identity {
    // Retrieve the unique provisioner identity.
    rpc DriverGetInfo (DriverGetInfoRequest) returns (DriverGetInfoResponse) {}
}

service Provisioner {
    // Create the bucket in the backend.
    //
    // Important return codes:
    // - MUST return OK if a backend bucket with matching identity and parameters already exists.
    // - MUST return ALREADY_EXISTS if a backend bucket with matching identity exists but with incompatible parameters.
    rpc DriverCreateBucket (DriverCreateBucketRequest) returns (DriverCreateBucketResponse) {}

    // Get details about a statically-provisioned bucket that should already exist in the OSP backend.
    //
    // Important return codes:
    // - MUST return OK if a backend bucket with matching identity and parameters already exists.
    // - MUST return NOT_FOUND if a bucket with matching identity does not exist.
    rpc DriverGetExistingBucket (DriverGetExistingBucketRequest) returns (DriverGetExistingBucketResponse) {}

    // Delete the bucket in the backend.
    //
    // Important return codes:
    // - MUST return OK if the bucket has already been deleted.
    rpc DriverDeleteBucket (DriverDeleteBucketRequest) returns (DriverDeleteBucketResponse) {}

    // Grant access to a bucket.
    //
    // Important return codes:
    // - MUST return OK if a principal with matching identity and parameters already exists.
    // - MUST return ALREADY_EXISTS if a principal with matching identity exists but with incompatible parameters.
    rpc DriverGrantBucketAccess (DriverGrantBucketAccessRequest) returns (DriverGrantBucketAccessResponse);

    // Revokes access to given bucket(s) from a principal.
    //
    // Important return codes:
    // - MUST return OK if access has already been removed from a principal.
    rpc DriverRevokeBucketAccess (DriverRevokeBucketAccessRequest) returns (DriverRevokeBucketAccessResponse);
}
```

#### Concurrency

In general the COSI system is responsible for ensuring that there is no more than one call "in-flight" per volume at a given time.
However, in some circumstances, the system MAY lose state (for example, a Controller or Driver crashes and restarts), and MAY issue multiple calls simultaneously for the same volume.
The Plugin SHOULD handle this as gracefully as possible.
The error code `ABORTED` MAY be returned by the Plugin in this case (see the [Error Scheme](#error-scheme) section for details).

#### Field Requirements

The requirements documented herein apply equally and without exception, unless otherwise noted, for the fields of all protobuf message types defined by this specification.
Violation of these requirements MAY result in RPC message data that is not compatible with all COSI systems, Plugin, and/or COSI middleware implementations.

##### Size Limits

COSI defines general size limits for fields of various types (see table below).
The general size limit for a particular field MAY be overridden by specifying a different size limit in said field's description.
Unless otherwise specified, fields SHALL NOT exceed the limits documented here.
These limits apply for messages generated by both COSI systems and plugins.

| Size       | Field Type          |
|------------|---------------------|
| 128 bytes  | string              |
| 4 KiB      | map<string, string> |

##### `REQUIRED` vs. `OPTIONAL`

* A field noted as `REQUIRED` MUST be specified, subject to any per-RPC caveats; caveats SHOULD be rare.
* A `repeated` or `map` field listed as `REQUIRED` MUST contain at least 1 element.
* A field noted as `OPTIONAL` MAY be specified and the specification SHALL clearly define expected behavior for the default, zero-value of such fields.

Scalar fields, even REQUIRED ones, will be defaulted if not specified and any field set to the default value will not be serialized over the wire as per [proto3](https://developers.google.com/protocol-buffers/docs/proto3#default).

#### Timeouts

Any of the RPCs defined in this spec MAY timeout and MAY be retried.
The COSI system MAY choose the maximum time it is willing to wait for a call, how long it waits between retries, and how many time it retries (these values are not negotiated between plugin and COSI system).

Idempotency requirements ensure that a retried call with the same fields continues where it left off when retried.
The only way to cancel a call is to issue a "negation" call if one exists.
For example, issue a `DeleteBucket` call to cancel a pending `CreateBucket` operation, etc.

### Error Scheme

All COSI API calls defined in this spec MUST return a [standard gRPC status](https://github.com/grpc/grpc/blob/master/src/proto/grpc/status/status.proto).
Most gRPC libraries provide helper methods to set and read the status fields.

The status `code` MUST contain a [canonical error code](https://github.com/grpc/grpc-go/blob/master/codes/codes.go).
COSI systems MUST handle all valid error codes.
Each RPC defines a set of gRPC error codes that MUST be returned by the plugin when specified conditions are encountered.
In addition to those, if the conditions defined below are encountered, the plugin MUST return the associated gRPC error code.

| Condition | gRPC Code | Description | Recovery Behavior |
|-----------|-----------|-------------|-------------------|
| Missing required field | 3 MISSING_ARGUMENT | A required field is missing from the request. | Not retryable. Caller SHOULD fix the request by adding the missing required field before retrying. |
| Invalid or unsupported field in the request | 3 INVALID_ARGUMENT | One or more fields in this field is either not allowed by the Plugin or has an invalid value. | Not retryable. Caller SHOULD fix the field(s) before retrying. |
| Permission denied | 7 PERMISSION_DENIED | The Plugin is able to derive or otherwise infer an identity from the secrets present within an RPC, but that identity does not have permission to invoke the RPC. | Retryable with exponential backoff. System administrator SHOULD ensure that requisite permissions are granted before retrying. |
| Resource exists with non-matching parameters | 6 ALREADY_EXISTS | The resource exists but has non-matching parameters configured. | Not retryable. Caller SHOULD fix the request by modifying the backend resource or request parameters to have matching parameters before retrying. |
| Operation pending for resource | 10 ABORTED | There is already an operation pending for the specified resource. See [Concurrency](#concurrency) | Retryable with exponential backoff. Caller SHOULD ensure that there are no other calls pending for the specified volume before retrying. |
| Call not implemented | 12 UNIMPLEMENTED | The invoked RPC is not implemented by the Plugin or disabled in the Plugin's current mode of operation. | Not retryable. |
| Not authenticated | 16 UNAUTHENTICATED | The invoked RPC does not carry secrets that are valid for authentication. | Retryable with exponential backoff. Caller SHOULD either fix the secrets provided in the RPC, or otherwise regalvanize said secrets such that they will pass authentication by the Plugin for the attempted RPC before retrying. |

The status `message` MUST contain a human readable description of error, if the status `code` is not `OK`.
This string MAY be surfaced by COSI system to end users.
It is NOT RECOMMENDED to include any sensitive information any status message that could risk the security of the COSI System, Plugin, or OSP backend.

The status `details` MUST be empty. In the future, this spec MAY require `details` to return a machine-parsable protobuf message if the status `code` is not `OK` to enable COSI system's to implement smarter error handling and fault resolution.

### Secrets Requirements (where applicable)

Secrets MAY be required by plugin to complete a RPC request.
A secret is a string to string map where the key identifies the name of the secret (e.g. "username" or "password"), and the value contains the secret data (e.g. "bob" or "abc123").
Each key MUST consist of alphanumeric characters, '-', '_' or '.'.
Each value MUST contain a valid string.
An SP MAY choose to accept binary (non-string) data by using a binary-to-text encoding scheme, like base64.
An SP SHALL advertise the requirements for required secret keys and values in documentation.
COSI system SHALL permit passing through the required secrets.
A COSI system MAY pass the same secrets to all RPCs, therefore the keys for all unique secrets that an SP expects MUST be unique across all COSI operations.
This information is sensitive and MUST be treated as such (not logged, etc.) by the COSI system.

### Identity Service RPC

#### DriverGetInfo

```protobuf
message DriverGetInfoRequest {
    // Intentionally left blank
}

message DriverGetInfoResponse {
    // TODO: unimplemented
}
```

### Provisioner Service RPC

#### S3 Protocol Definitions

```protobuf
message S3BucketInfo {
    // S3 endpoint URL.
    string endpoint = 1;

    // S3 bucket ID needed for client access.
    string bucket_id = 2;

    // Geographical region where the S3 server is running.
    string region = 3;

    // sS3 signature version for signing all s3 requests.
    S3SignatureVersion signature_version = 4;
}

message S3AccessInfo {
    // S3 access key ID.
    string access_key_id = 1;

    // S3 access secret key.
    string access_secret_key = 2;
}

// S3SignatureVersion is the version of the signing algorithm for all s3 requests
enum S3SignatureVersion {
    UNKNOWN = 0;

    // Signature Version 2
    S3V2 = 1;

    // Signature Version 4
    S3V4 = 2;
}
```

#### Azure Protocol Definitions

```protobuf
message AzureBucketInfo {
    // ID of the Azure storage account.
    string storage_account = 1;
}

message AzureAccessInfo {
    // Azure access token.
    // Note that the Azure spec includes the resource URI as well as token in its definition.
    // https://learn.microsoft.com/en-us/azure/storage/common/media/storage-sas-overview/sas-storage-uri.svg
    string access_token = 1;

    // Expiry time of the access.
    // Empty if unset. Otherwise, date+time in ISO 8601 format.
    string expiry_timestamp = 2;
}
```

#### Google Cloud Storage (GCS) Protocol Definitions

```protobuf
message GcsBucketInfo {
    // GCS project ID.
    string project_id = 1;

    // GCS bucket name needed for client access.
    string bucket_name = 2;
}

message GcsAccessInfo {
    // HMAC access ID.
    string access_id = 1;

    // HMAC secret.
    string access_secret = 2;

    // GCS private key name.
    string private_key_name = 3;

    // GCS service account name.
    string service_account = 4;
}
```

#### DriverCreateBucket

```protobuf
message DriverCreateBucketRequest {
    // TODO: unimplemented
}

message DriverCreateBucketResponse {
    // TODO: unimplemented
}
```

#### DriverGetExistingBucket

```protobuf
message DriverGetExistingBucketRequest {
    // TODO: unimplemented
}

message DriverGetExistingBucketResponse {
    // TODO: unimplemented
}
```

#### DriverDeleteBucket

```protobuf
message DriverDeleteBucketRequest {
    // TODO: unimplemented
}

message DriverDeleteBucketResponse {
    // Intentionally left blank
}
```

#### DriverGrantBucketAccess

```protobuf
message DriverGrantBucketAccessRequest {
    // TODO: unimplemented
}

message DriverGrantBucketAccessResponse {
    // TODO: unimplemented
}
```

#### DriverRevokeBucketAccess

```protobuf
message DriverRevokeBucketAccessRequest {
    // TODO: unimplemented
}

message DriverRevokeBucketAccessResponse {
    // Intentionally left blank
}
```

## Protocol

### Connectivity

* A COSI system SHALL communicate with a Plugin using gRPC to access the `Provisioner` service.
  * proto3 SHOULD be used with gRPC, as per the [official recommendations](http://www.grpc.io/docs/guides/#protocol-buffer-versions).
  * All Plugins SHALL implement the REQUIRED Identity service RPCs.
* The COSI system SHALL provide the listen-address for the Plugin by way of the `COSI_ENDPOINT` environment variable.
  Plugin components SHALL create, bind, and listen for RPCs on the specified listen address.
  * Only UNIX Domain Sockets MAY be used as endpoints.
    This will likely change in a future version of this specification to support non-UNIX platforms.
* All supported RPC services MUST be available at the listen address of the Plugin.

### Security

* The COSI system operator and Provisioner Sidecar SHOULD take steps to ensure that any and all communication between the COSI system and Plugin Service are secured according to best practices.
* Communication between a COSI system and a Plugin SHALL be transported over UNIX Domain Sockets.
  * gRPC is compatible with UNIX Domain Sockets; it is the responsibility of the COSI system operator and Provisioner Sidecar to properly secure access to the Domain Socket using OS filesystem ACLs and/or other OS-specific security context tooling.
  * SP’s supplying stand-alone Plugin controller appliances, or other remote components that are incompatible with UNIX Domain Sockets MUST provide a software component that proxies communication between a UNIX Domain Socket and the remote component(s).
    Proxy components transporting communication over IP networks SHALL be responsible for securing communications over such networks.
* Both the COSI system and Plugin SHOULD avoid accidental leakage of sensitive information (such as redacting such information from log files).

### Debugging

* Debugging and tracing are supported by external, COSI-independent additions and extensions to gRPC APIs, such as [OpenTracing](https://github.com/grpc-ecosystem/grpc-opentracing).

## Configuration and Operation

### General Configuration

* The `COSI_ENDPOINT` environment variable SHALL be supplied to the Plugin by the Provisioner Sidecar.
* An operator SHALL configure the COSI system to connect to the Plugin via the listen address identified by `COSI_ENDPOINT` variable.
* With exception to sensitive data, Plugin configuration SHOULD be specified by environment variables, whenever possible, instead of by command line flags or bind-mounted/injected files.

#### Filesystem

* Plugins SHALL NOT specify requirements that include or otherwise reference directories and/or files on the root filesystem of the COSI system.
* Plugins SHALL NOT create additional files or directories adjacent to the UNIX socket specified by `COSI_ENDPOINT`; violations of this requirement constitute "abuse".
  * The Provisioner Sidecar is the ultimate authority of the directory in which the UNIX socket endpoint is created and MAY enforce policies to prevent and/or mitigate abuse of the directory by Plugins.

### Supervised Lifecycle Management

* For Plugins packaged in software form:
  * Plugin Packages SHOULD use a well-documented container image format (e.g., Docker, OCI).
  * The chosen package image format MAY expose configurable Plugin properties as environment variables, unless otherwise indicated in the section below.
    Variables so exposed SHOULD be assigned default values in the image manifest.
  * A Provisioner Sidecar MAY programmatically evaluate or otherwise scan a Plugin Package’s image manifest in order to discover configurable environment variables.
  * A Plugin SHALL NOT assume that an operator or Provisioner Sidecar will scan an image manifest for environment variables.

#### Environment Variables

* Variables defined by this specification SHALL be identifiable by their `COSI_` name prefix.
* Configuration properties not defined by the COSI specification SHALL NOT use the same `COSI_` name prefix; this prefix is reserved for common configuration properties defined by the COSI specification.
* The Provisioner Sidecar SHOULD supply all RECOMMENDED COSI environment variables to a Plugin.
* The Provisioner Sidecar SHALL supply all REQUIRED COSI environment variables to a Plugin.

##### `COSI_ENDPOINT`

Network endpoint at which a Plugin SHALL host COSI RPC services. The general format is:

    {scheme}://{authority}{endpoint}

The following address types SHALL be supported by Plugins:

    unix:///path/to/unix/socket.sock

Note: All UNIX endpoints SHALL end with `.sock`. See [gRPC Name Resolution](https://github.com/grpc/grpc/blob/master/doc/naming.md).

This variable is REQUIRED.

#### Operational Recommendations

The Provisioner Sidecar expects that a Plugin SHALL act as a long-running service vs. an on-demand, CLI-driven process.

Supervised plugins MAY be isolated and/or resource-bounded.
