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

These are the sets of RPCs:

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
    // - MUST return OUT_OF_RANGE - if (and only if) the driver/backend does not support multi-bucket access.
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

A Plugin MUST implement this RPC call.

```protobuf
message DriverGetInfoRequest {
    // Intentionally left blank
}

message DriverGetInfoResponse {
    // REQUIRED. The unique name of the driver.
    // The name MUST follow domain name notation format
    // (https://tools.ietf.org/html/rfc1035#section-2.3.1). It SHOULD
    // include the plugin's host company name and the plugin name,
    // to minimize the possibility of collisions. It MUST be 63
    // characters or less, beginning and ending with an alphanumeric
    // character ([a-z0-9A-Z]) with dashes (-), dots (.), and
    // alphanumerics between.
    string name = 1;

    // A list of all object storage protocols supported by the driver.
    // At least one protocol is REQUIRED.
    repeated ObjectProtocol supported_protocols = 2;
}
```

If the Plugin is unable to complete the call successfully, it MUST return a non-ok gRPC status code.

### Provisioner Service RPC

#### Protocol Definitions

```protobuf
message ObjectProtocol {
    enum Type {
        UNKNOWN = 0;

        // S3 represents the S3 object protocol type.
        S3 = 1;

        // AZURE represents the Azure Blob object protocol type.
        AZURE = 2;

        // GCS represents the Google Cloud Storage object protocol type.
        GCS = 3;
    }

    Type type = 1;
}

// Bucket info for the backend bucket corresponding to each protocol.
// If a protocol is not supported, the message MUST be empty/nil.
message ObjectProtocolAndBucketInfo {
    // Protocol support and bucket info for S3 protocol access.
    S3BucketInfo s3 = 1;

    // Protocol support and bucket info for Azure (Blob) protocol access.
    AzureBucketInfo azure = 2;

    // Protocol support and bucket info for Google Cloud Storage protocol access.
    GcsBucketInfo gcs = 3;
}

// Credential info for the backend access corresponding to each protocol.
// If a protocol is not supported, the message MUST be empty/nil.
message CredentialInfo {
    // Credential info for S3 protocol access.
    S3CredentialInfo s3 = 1;

    // Credential info for Azure (Blob) protocol access.
    AzureCredentialInfo azure = 2;

    // Credential info for Google Cloud Storage protocol access.
    GcsCredentialInfo gcs = 3;
}
```

##### S3 Protocol Definitions

```protobuf
message S3BucketInfo {
    // S3 bucket ID needed for client access.
    string bucket_id = 1;

    // S3 endpoint URL.
    string endpoint = 2;

    // Geographical region where the S3 server is running.
    string region = 3;

    // S3 addressing style. Drivers should return an addressing style that the backend supports and
    // that is most likely to have the broadest client support.
    // See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
    S3AddressingStyle addressing_style = 4;
}

message S3CredentialInfo {
    // S3 access key ID.
    string access_key_id = 1;

    // S3 access secret key.
    string access_secret_key = 2;
}

// S3 addressing style.
// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
message S3AddressingStyle {
    enum Style {
        UNKNOWN = 0;

        // Path-style addressing.
        PATH = 1;

        // Virtual-hosted-style addressing.
        VIRTUAL = 2;
    }
    Style style = 1;
}
```

##### Azure Protocol Definitions

```protobuf
message AzureBucketInfo {
    // ID of the Azure storage account.
    string storage_account = 1;
}

message AzureCredentialInfo {
    // Azure access token.
    // Note that the Azure spec includes the resource URI as well as token in its definition.
    // https://learn.microsoft.com/en-us/azure/storage/common/media/storage-sas-overview/sas-storage-uri.svg
    string access_token = 1;

    // Expiry time of the access.
    // Empty if unset. Otherwise, date+time in ISO 8601 format.
    string expiry_timestamp = 2;
}
```

##### Google Cloud Storage (GCS) Protocol Definitions

```protobuf
message GcsBucketInfo {
    // GCS project ID.
    string project_id = 1;

    // GCS bucket name needed for client access.
    string bucket_name = 2;
}

message GcsCredentialInfo {
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

#### Shared Definitions

```protobuf
message AuthenticationType {
    enum Type {
        UNKNOWN = 0;

        // The Provisioner should generate a protocol-appropriate access key that clients can use to
        // authenticate to the backend object store.
        KEY = 1;

        // The Provisioner should configure the system such that Pods using the given ServiceAccount
        // authenticate to the backend object store automatically.
        SERVICE_ACCOUNT = 2;
    }
    Type type = 1;
}

message AccessMode {
    enum Mode {
        UNKNOWN = 0;

        // Read/Write access mode.
        READ_WRITE = 1;

        // Read-only access mode.
        READ_ONLY = 2;

        // Write-only access mode.
        WRITE_ONLY = 3;
    }
}
```

#### DriverCreateBucket

A Plugin MUST implement this RPC call.

This operation MUST be idempotent. If a bucket corresponding to the specified name already exists
and is compatible with the given parameters, the Plugin MUST reply OK.

Important return codes:
* `AlreadyExists` (not retryable) when the bucket already exists but is incompatible with the request.
* `InvalidArgument` (not retryable) if any parameters are invalid for the backend.

```protobuf
message DriverCreateBucketRequest {
    // REQUIRED. The suggested name for the backend bucket.
    // It serves two purposes:
    // 1) Suggested name - COSI WILL suggest a name that includes a UID component that is
    //    statistically likely to be globally unique.
    // 2) Idempotency - This name is generated by COSI to achieve idempotency. The Plugin SHOULD
    //    ensure that multiple DriverCreateBucket calls for the same name do not result in more
    //    than one Bucket being provisioned corresponding to the name.
    //    The COSI Sidecar WILL call DriverCreateBucket, with the same name, periodically to ensure
    //    the bucket exists.
    //    Using or appending random identifiers can lead to multiple unused buckets being created in
    //    the storage backend in the event of timing-related Driver/Sidecar failures or restarts.
    // COSI WILL use DNS subdomain format (https://datatracker.ietf.org/doc/html/rfc1123).
    // It WILL contain contain no more than 253 characters, contain only lowercase alphanumeric
    // characters, '-' or '.', start with an alphanumeric character, and end with an alphanumeric
    // character.
    string name = 1;

    // OPTIONAL. A list of all object storage protocols the provisioned bucket MUST support.
    // If none are given, the provisioner MAY provision with a set of default protocol(s) or return
    // `InvalidArgument` with a message indicating that it requires this input.
    // If any protocol cannot be supported, the Provisioner MUST return `InvalidArgument`.
    repeated ObjectProtocol protocols = 2;

    // OPTIONAL. Plugin specific parameters passed in as opaque key-value pairs.
    // The Plugin is responsible for parsing and validating these parameters.
    map<string, string> parameters = 4;
}

message DriverCreateBucketResponse {
    // REQUIRED. The unique identifier for the backend bucket known to the Provisioner.
    // This value WILL be used by COSI to make subsequent calls related to the bucket, so the
    // Provisioner MUST be able to correlate `bucket_id` to the backend bucket.
    // It is RECOMMENDED to use the backend storage system's bucket ID.
    // To prevent abuse, this must be at most 2048 characters long, consisting of alphanumeric
    // characters ([a-z0-9A-Z]), dashes (-), and dots (.).
    string bucket_id = 1;

    // REQUIRED: At least one protocol bucket info result MUST be non-nil.
    //
    // The primary purpose of this response is to indicate which protocols are supported for
    // subsequent DriverGrantBucketAccess requests referencing this provisioned bucket. A non-nil
    // bucket info corresponding to a protocol indicates support.
    //
    // The Provisioner MUST indicate support for the protocols in the request. It MAY indicate
    // support for more protocols than the request. It SHOULD indicate support for all supported
    // protocols. It MUST NOT indicate support (return a non-nil result) for unsupported protocols.
    //
    // The secondary purpose of this response is to report non-credential information about the
    // bucket. COSI does not expose this information to end-users until a subsequent
    // DriverGrantBucketAccess is provisioned referencing this bucket. Instead, the info is exposed
    // to administrators so that they might more easily debug errors in their configuration of COSI.
    // It is thus RECOMMENDED to return all relevant bucket info for all supported protocols.
    // However, the Provisioner MAY omit any or all bucket info fields as desired.
    ObjectProtocolAndBucketInfo protocols = 2;
}
```

#### DriverGetExistingBucket

A Plugin MUST implement this RPC call.

This operation MUST be idempotent. If a bucket corresponding to the specified name already exists
and is compatible with the given parameters, the Plugin MUST reply OK.

Important return codes:
* `AlreadyExists` (not retryable) when the bucket already exists but is incompatible with the request.
* `NotFound` (retryable) when the bucket does not exist.
* `InvalidArgument` (not retryable) if any parameters are invalid for the backend.

```protobuf
message DriverGetExistingBucketRequest {
    // REQUIRED. The unique identifier for the existing backend bucket known to the Provisioner.
    // Provisioner MUST be able to correlate `bucket_id` to the backend bucket.
    // It is RECOMMENDED to use the backend storage system's bucket ID.
    // To prevent abuse, this must be at most 2048 characters long, consisting of alphanumeric
    // characters ([a-z0-9A-Z]), dashes (-), and dots (.).
    string existing_bucket_id = 1;

    // OPTIONAL. A list of all object storage protocols the provisioned bucket MUST support.
    // If none are given, the provisioner MAY provision with a set of default protocol(s) or return
    // `InvalidArgument` with a message indicating that it requires this input.
    // If any protocol cannot be supported, the Provisioner MUST return `InvalidArgument`.
    repeated ObjectProtocol protocols = 2;

    // OPTIONAL. Plugin specific parameters passed in as opaque key-value pairs.
    // The Plugin is responsible for parsing and validating these parameters.
    map<string, string> parameters = 4;
}

message DriverGetExistingBucketResponse {
    // REQUIRED. The unique identifier for the backend bucket known to the Provisioner.
    // This value WILL be used by COSI to make subsequent calls related to the bucket, so the
    // Provisioner MUST be able to correlate `bucket_id` to the backend bucket.
    // It is RECOMMENDED to use the backend storage system's bucket ID.
    // To prevent abuse, this must be at most 2048 characters long, consisting of alphanumeric
    // characters ([a-z0-9A-Z]), dashes (-), and dots (.).
    string bucket_id = 1;

    // REQUIRED: At least one protocol bucket info result MUST be non-nil.
    //
    // The primary purpose of this response is to indicate which protocols are supported for
    // subsequent DriverGrantBucketAccess requests referencing this provisioned bucket. A non-nil
    // bucket info corresponding to a protocol indicates support.
    //
    // The Provisioner MUST indicate support for the protocols in the request. It MAY indicate
    // support for more protocols than the request. It SHOULD indicate support for all supported
    // protocols. It MUST NOT indicate support (return a non-nil result) for unsupported protocols.
    //
    // The secondary purpose of this response is to report non-credential information about the
    // bucket. COSI does not expose this information to end-users until a subsequent
    // DriverGrantBucketAccess is provisioned referencing this bucket. Instead, the info is exposed
    // to administrators so that they might more easily debug errors in their configuration of COSI.
    // It is thus RECOMMENDED to return all relevant bucket info for all supported protocols.
    // However, the Provisioner MAY omit any or all bucket info fields as desired.
    ObjectProtocolAndBucketInfo protocols = 2;
}
```

#### DriverDeleteBucket

A Plugin MUST implement this RPC call.

This operation MUST be idempotent. If a bucket corresponding to the specified name does not exist,
the Plugin MUST reply OK.

```protobuf
message DriverDeleteBucketRequest {
    // REQUIRED. The unique identifier for the existing backend bucket known to the Provisioner.
    // To prevent abuse, this must be at most 2048 characters long, consisting of alphanumeric
    // characters ([a-z0-9A-Z]), dashes (-), and dots (.).
    string bucket_id = 1;

    // OPTIONAL. Plugin specific parameters associated with the provisioned bucket.
    map<string, string> parameters = 4;
}

message DriverDeleteBucketResponse {
    // Intentionally left blank
}
```

#### DriverGrantBucketAccess

A Plugin MUST implement this RPC call.

This operation MUST be idempotent. If an access corresponding to the specified name already exists
and is compatible with the given parameters, the Plugin MUST reply OK.

Important driver return codes:
* `AlreadyExists` (not retryable) when the bucket already exists but is incompatible with the request.
* `InvalidArgument` (not retryable) if `AuthenticationType` is not supported.
* `InvalidArgument` (not retryable) if any parameters are invalid for the backend.
* `OutOfRange` (not retryable) if (and only if) the driver does not support creating a single shared access credential for multiple buckets.

```protobuf
message DriverGrantBucketAccessRequest {
    // REQUIRED. The suggested name for the backend bucket access.
    // It serves two purposes:
    // 1) Suggested name - COSI WILL suggest a name that includes a UID component that is
    //    statistically likely to be globally unique.
    // 2) Idempotency - This name is generated by COSI to achieve idempotency. The Plugin SHOULD
    //    ensure that multiple DriverGrantBucketAccess calls for the same name do not result in more
    //    than one Bucket being provisioned corresponding to the name.
    //    The COSI Sidecar WILL call DriverGrantBucketAccess, with the same name, periodically to
    //    ensure the bucket exists.
    //    Using or appending random identifiers can lead to multiple unused buckets being created in
    //    the storage backend in the event of timing-related Driver/Sidecar failures or restarts.
    // COSI WILL use DNS subdomain format (https://datatracker.ietf.org/doc/html/rfc1123).
    // It WILL contain contain no more than 253 characters, contain only lowercase alphanumeric
    // characters, '-' or '.', start with an alphanumeric character, and end with an alphanumeric
    // character.
    string account_name = 1;

    // REQUIRED. The object storage protocol the provisioned access MUST support.
    // If the protocol cannot be supported, the Provisioner MUST return `InvalidArgument`.
    ObjectProtocol protocol = 2;

    // REQUIRED. The authentication type that the Provisioner MUST provision for this request.
    AuthenticationType authentication_type = 3;

    // REQUIRED when `authentication_type` is `SERVICE_ACCOUNT`.
    // OPTIONAL for all other authentication types.
    // COSI WILL NOT set this when the requested `authentication_type` is not `SERVICE_ACCOUNT`.
    string service_account_name = 4;

    // OPTIONAL. Plugin specific parameters passed in as opaque key-value pairs.
    // The Plugin is responsible for parsing and validating these parameters.
    map<string, string> parameters = 5;

    message AccessedBucket {
        // REQUIRED. The unique identifier for the backend bucket known to the Provisioner.
        // To prevent abuse, this must be at most 2048 characters long, consisting of alphanumeric
        // characters ([a-z0-9A-Z]), dashes (-), and dots (.).
        string bucket_id = 1;

        // REQUIRED. The read/write access mode that the Provisioner SHOULD provision for the bucket
        // associated with `bucket_id`.
        AccessMode access_mode = 2;
    }

    // REQUIRED. Access to at least one bucket MUST be requested.
    repeated AccessedBucket buckets = 6;
}

message DriverGrantBucketAccessResponse {
    // REQUIRED. The unique identifier for the backend access account known to the Provisioner.
    // This value WILL be used by COSI to make subsequent calls related to the access, so the
    // Provisioner MUST be able to correlate `account_id` to the backend access.
    // It is RECOMMENDED to use the backend storage system's bucket ID.
    // To prevent abuse, this must be at most 2048 characters long, consisting of alphanumeric
    // characters ([a-z0-9A-Z]), dashes (-), and dots (.).
    string account_id = 1;

    message BucketInfo {
        // REQUIRED. The unique identifier for the backend bucket known to the Provisioner.
        // To prevent abuse, this must be at most 2048 characters long, consisting of alphanumeric
        // characters ([a-z0-9A-Z]), dashes (-), and dots (.).
        string bucket_id = 1;

        // REQUIRED: EXACTLY one protocol bucket info result MUST be non-nil.
        // The Provisioner MUST fill in all required bucket info for the requested protocol.
        // The Provisioner SHOULD fill in as much bucket info as is known given the parameters.
        // It MUST NOT support (return a non-nil result) non-requested protocols.
        // COSI WILL expose this information to users, and it WILL be treated as sensitive/secret
        // information.
        // COSI WILL not log the information or store it in plaintext.
        ObjectProtocolAndBucketInfo bucket_info = 2;
    }

    // REQUIRED. The Provisioner MUST return info for all `buckets` in the request.
    repeated BucketInfo buckets = 2;

    // REQUIRED.
    CredentialInfo credentials = 3;
}
```

#### DriverRevokeBucketAccess

A Plugin MUST implement this RPC call.

This operation MUST be idempotent. If an access corresponding to the specified name already doesn't
exist, the Plugin MUST reply OK.

```protobuf
message DriverRevokeBucketAccessRequest {
    // REQUIRED. The unique identifier for the backend access account
    // To prevent abuse, this must be at most 2048 characters long, consisting of alphanumeric
    // characters ([a-z0-9A-Z]), dashes (-), and dots (.).
    string account_id = 1;

    // REQUIRED. The object storage protocol associated with the provisioned access.
    ObjectProtocol protocol = 2;

    // REQUIRED. The authentication type associated with the provisioned access.
    AuthenticationType authentication_type = 3;

    // REQUIRED when `authentication_type` is `SERVICE_ACCOUNT`.
    // OPTIONAL for all other authentication types.
    // COSI WILL NOT set this when the requested `authentication_type` is not `SERVICE_ACCOUNT`.
    string service_account_name = 4;

    // OPTIONAL. Plugin specific parameters associated with the provisioned access.
    map<string, string> parameters = 5;

    message AccessedBucket {
        // REQUIRED. The unique identifier for the backend bucket known to the Provisioner.
        string bucket_id = 1;
    }

    // REQUIRED. Buckets associated with the provisioned access.
    repeated AccessedBucket buckets = 6;
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
  * Providers supplying stand-alone Plugin controller appliances, or other remote components that are incompatible with UNIX Domain Sockets MUST provide a software component that proxies communication between a UNIX Domain Socket and the remote component(s).
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
