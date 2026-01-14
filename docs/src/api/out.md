# API Reference

## Packages
- [objectstorage.k8s.io/v1alpha2](#objectstoragek8siov1alpha2)


## objectstorage.k8s.io/v1alpha2

Package v1alpha2 contains API Schema definitions for the objectstorage v1alpha2 API group.

### Resource Types
- [Bucket](#bucket)
- [BucketAccess](#bucketaccess)
- [BucketAccessClass](#bucketaccessclass)
- [BucketAccessClassList](#bucketaccessclasslist)
- [BucketAccessList](#bucketaccesslist)
- [BucketClaim](#bucketclaim)
- [BucketClaimList](#bucketclaimlist)
- [BucketClass](#bucketclass)
- [BucketClassList](#bucketclasslist)
- [BucketList](#bucketlist)



#### AccessedBucket



AccessedBucket identifies a Bucket and correlates it to a BucketClaimAccess from the spec.



_Appears in:_
- [BucketAccessStatus](#bucketaccessstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `bucketName` _string_ | bucketName is the name of a Bucket the access should have permissions for.<br />Must be a valid Kubernetes resource name: at most 253 characters, consisting only of<br />lower-case alphanumeric characters, hyphens, and periods, starting and ending with an<br />alphanumeric character. |  | MaxLength: 253 <br />MinLength: 1 <br /> |
| `bucketClaimName` _string_ | bucketClaimName must match a BucketClaimAccess's BucketClaimName from the spec.<br />Must be a valid Kubernetes resource name: at most 253 characters, consisting only of<br />lower-case alphanumeric characters, hyphens, and periods, starting and ending with an<br />alphanumeric character. |  | MaxLength: 253 <br />MinLength: 1 <br /> |


#### Bucket



Bucket is the Schema for the buckets API



_Appears in:_
- [BucketList](#bucketlist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `Bucket` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketSpec](#bucketspec)_ | spec defines the desired state of Bucket |  |  |
| `status` _[BucketStatus](#bucketstatus)_ | status defines the observed state of Bucket |  |  |


#### BucketAccess



BucketAccess is the Schema for the bucketaccesses API



_Appears in:_
- [BucketAccessList](#bucketaccesslist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketAccess` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketAccessSpec](#bucketaccessspec)_ | spec defines the desired state of BucketAccess |  |  |
| `status` _[BucketAccessStatus](#bucketaccessstatus)_ | status defines the observed state of BucketAccess |  |  |


#### BucketAccessAuthenticationType

_Underlying type:_ _string_

BucketAccessAuthenticationType specifies what authentication mechanism is used for provisioning
bucket access.

_Validation:_
- Enum: [Key ServiceAccount]

_Appears in:_
- [BucketAccessClassSpec](#bucketaccessclassspec)
- [BucketAccessStatus](#bucketaccessstatus)



#### BucketAccessClass



BucketAccessClass is the Schema for the bucketaccessclasses API



_Appears in:_
- [BucketAccessClassList](#bucketaccessclasslist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketAccessClass` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketAccessClassSpec](#bucketaccessclassspec)_ | spec defines the desired state of BucketAccessClass |  |  |


#### BucketAccessClassList



BucketAccessClassList contains a list of BucketAccessClass





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketAccessClassList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[BucketAccessClass](#bucketaccessclass) array_ |  |  |  |


#### BucketAccessClassSpec



BucketAccessClassSpec defines the desired state of BucketAccessClass



_Appears in:_
- [BucketAccessClass](#bucketaccessclass)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `driverName` _string_ | driverName is the name of the driver that fulfills requests for this BucketAccessClass.<br />See driver documentation to determine the correct value to set.<br />Must be 63 characters or less, beginning and ending with an alphanumeric character<br />([a-z0-9A-Z]) with dashes (-), dots (.), and alphanumerics between. |  | MaxLength: 63 <br />MinLength: 1 <br />Pattern: `^[a-zA-Z0-9]([a-zA-Z0-9\-\.]\{0,61\}[a-zA-Z0-9])?$` <br /> |
| `authenticationType` _[BucketAccessAuthenticationType](#bucketaccessauthenticationtype)_ | authenticationType specifies which authentication mechanism is used bucket access.<br />See driver documentation to determine which values are supported.<br />Possible values:<br /> - Key: The driver should generate a protocol-appropriate access key that clients can use to<br />   authenticate to the backend object store.<br /> - ServiceAccount: The driver should configure the system such that Pods using the given<br />   ServiceAccount authenticate to the backend object store automatically. |  | Enum: [Key ServiceAccount] <br /> |
| `parameters` _object (keys:string, values:string)_ | parameters is an opaque map of driver-specific configuration items passed to the driver that<br />fulfills requests for this BucketAccessClass.<br />See driver documentation to determine supported parameters and their effects.<br />A maximum of 512 parameters are allowed. |  | MaxProperties: 512 <br />MinProperties: 1 <br /> |
| `featureOptions` _[BucketAccessFeatureOptions](#bucketaccessfeatureoptions)_ | featureOptions can be used to adjust various COSI access provisioning behaviors.<br />If specified, at least one option must be set. |  | MinProperties: 1 <br /> |


#### BucketAccessFeatureOptions



BucketAccessFeatureOptions defines various COSI access provisioning behaviors.

_Validation:_
- MinProperties: 1

_Appears in:_
- [BucketAccessClassSpec](#bucketaccessclassspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `disallowedBucketAccessModes` _[BucketAccessMode](#bucketaccessmode) array_ | disallowedBucketAccessModes is a list of disallowed Read/Write access modes. A BucketAccess<br />using this class will not be allowed to request access to a BucketClaim with any access mode<br />listed here.<br />This is particularly useful for administrators to restrict access to a statically-provisioned<br />bucket that is managed outside the BucketAccess Namespace or Kubernetes cluster.<br />Possible values: 'ReadWrite', 'ReadOnly', 'WriteOnly'. |  | Enum: [ReadWrite ReadOnly WriteOnly] <br />MaxItems: 3 <br />MinItems: 1 <br /> |
| `disallowMultiBucketAccess` _boolean_ | disallowMultiBucketAccess disables the ability for a BucketAccess to reference multiple<br />BucketClaims when set. |  |  |


#### BucketAccessList



BucketAccessList contains a list of BucketAccess





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketAccessList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[BucketAccess](#bucketaccess) array_ |  |  |  |


#### BucketAccessMode

_Underlying type:_ _string_

BucketAccessMode describes the Read/Write mode an access should have for a bucket.

_Validation:_
- Enum: [ReadWrite ReadOnly WriteOnly]

_Appears in:_
- [BucketAccessFeatureOptions](#bucketaccessfeatureoptions)
- [BucketClaimAccess](#bucketclaimaccess)

| Field | Description |
| --- | --- |
| `ReadWrite` | BucketAccessModeReadWrite represents read-write access mode.<br /> |
| `ReadOnly` | BucketAccessModeReadOnly represents read-only access mode.<br /> |
| `WriteOnly` | BucketAccessModeWriteOnly represents write-only access mode.<br /> |


#### BucketAccessSpec



BucketAccessSpec defines the desired state of BucketAccess



_Appears in:_
- [BucketAccess](#bucketaccess)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `bucketClaims` _[BucketClaimAccess](#bucketclaimaccess) array_ | bucketClaims is a list of BucketClaims the provisioned access must have permissions for,<br />along with per-BucketClaim access parameters and system output definitions.<br />At least one BucketClaim must be referenced.<br />A maximum of 128 BucketClaims may be referenced.<br />Multiple references to the same BucketClaim are not permitted. |  | MaxItems: 128 <br />MinItems: 1 <br /> |
| `bucketAccessClassName` _string_ | bucketAccessClassName selects the BucketAccessClass for provisioning the access. |  | MaxLength: 253 <br />MinLength: 1 <br /> |
| `protocol` _[ObjectProtocol](#objectprotocol)_ | protocol is the object storage protocol that the provisioned access must use.<br />Access can only be granted for BucketClaims that support the requested protocol.<br />Each BucketClaim status reports which protocols are supported for the BucketClaim's bucket.<br />Possible values: 'S3', 'Azure', 'GCS'. |  | Enum: [S3 Azure GCS] <br /> |
| `serviceAccountName` _string_ | serviceAccountName is the name of the Kubernetes ServiceAccount that user application Pods<br />intend to use for access to referenced BucketClaims.<br />Required when the BucketAccessClass is configured to use ServiceAccount authentication type.<br />Ignored for all other authentication types.<br />It is recommended to specify this for all BucketAccesses to improve portability. |  | MaxLength: 253 <br />MinLength: 1 <br /> |


#### BucketAccessStatus



BucketAccessStatus defines the observed state of BucketAccess.



_Appears in:_
- [BucketAccess](#bucketaccess)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `readyToUse` _boolean_ | readyToUse indicates that the BucketAccess is ready for consumption by workloads. |  |  |
| `accountID` _string_ | accountID is the unique identifier for the backend access known to the driver.<br />This field is populated by the COSI Sidecar once access has been successfully granted.<br />Must be at most 2048 characters and consist only of alphanumeric characters ([a-z0-9A-Z]),<br />dashes (-), dots (.), underscores (_), and forward slash (/). |  | MaxLength: 2048 <br />MinLength: 1 <br />Pattern: `^[a-zA-Z0-9/._-]+$` <br /> |
| `accessedBuckets` _[AccessedBucket](#accessedbucket) array_ | accessedBuckets is a list of Buckets the provisioned access must have permissions for, along<br />with per-Bucket access options. This field is populated by the COSI Controller based on the<br />referenced BucketClaims in the spec. |  | MaxItems: 128 <br />MinItems: 1 <br /> |
| `driverName` _string_ | driverName holds a copy of the BucketAccessClass driver name from the time of BucketAccess<br />provisioning. This field is populated by the COSI Controller.<br />Must be 63 characters or less, beginning and ending with an alphanumeric character<br />([a-z0-9A-Z]) with dashes (-), dots (.), and alphanumerics between. |  | MaxLength: 63 <br />MinLength: 1 <br />Pattern: `^[a-zA-Z0-9]([a-zA-Z0-9\-\.]\{0,61\}[a-zA-Z0-9])?$` <br /> |
| `authenticationType` _[BucketAccessAuthenticationType](#bucketaccessauthenticationtype)_ | authenticationType holds a copy of the BucketAccessClass authentication type from the time of<br />BucketAccess provisioning. This field is populated by the COSI Controller.<br />Possible values:<br /> - Key: clients may use a protocol-appropriate access key to authenticate to the backend object store.<br /> - ServiceAccount: Pods using the ServiceAccount given in spec.serviceAccountName may authenticate to the backend object store automatically. |  | Enum: [Key ServiceAccount] <br /> |
| `parameters` _object (keys:string, values:string)_ | parameters holds a copy of the BucketAccessClass parameters from the time of BucketAccess<br />provisioning. This field is populated by the COSI Controller. |  | MaxProperties: 512 <br />MinProperties: 1 <br /> |
| `error` _[TimestampedError](#timestampederror)_ | error holds the most recent error message, with a timestamp.<br />This is cleared when provisioning is successful. |  | MinProperties: 0 <br /> |


#### BucketClaim



BucketClaim is the Schema for the bucketclaims API



_Appears in:_
- [BucketClaimList](#bucketclaimlist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketClaim` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketClaimSpec](#bucketclaimspec)_ | spec defines the desired state of BucketClaim |  | MinProperties: 1 <br /> |
| `status` _[BucketClaimStatus](#bucketclaimstatus)_ | status defines the observed state of BucketClaim |  |  |


#### BucketClaimAccess



BucketClaimAccess selects a BucketClaim for access, defines access parameters for the
corresponding bucket, and specifies where user-consumable bucket information and access
credentials for the accessed bucket will be stored.



_Appears in:_
- [BucketAccessSpec](#bucketaccessspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `bucketClaimName` _string_ | bucketClaimName is the name of a BucketClaim the access should have permissions for.<br />The BucketClaim must be in the same Namespace as the BucketAccess.<br />Must be a valid Kubernetes resource name: at most 253 characters, consisting only of<br />lower-case alphanumeric characters, hyphens, and periods, starting and ending with an<br />alphanumeric character. |  | MaxLength: 253 <br />MinLength: 1 <br /> |
| `accessMode` _[BucketAccessMode](#bucketaccessmode)_ | accessMode is the Read/Write access mode that the access should have for the bucket.<br />The provisioned access will have the corresponding permissions to read and/or write objects<br />the BucketClaim's bucket.<br />The provisioned access can also assume to have corresponding permissions to read and/or write<br />object metadata and object metadata (e.g., tags) except when metadata changes would change<br />object store behaviors or permissions (e.g., changes to object caching behaviors).<br />Possible values: 'ReadWrite', 'ReadOnly', 'WriteOnly'. |  | Enum: [ReadWrite ReadOnly WriteOnly] <br /> |
| `accessSecretName` _string_ | accessSecretName is the name of a Kubernetes Secret that COSI should create and populate with<br />bucket info and access credentials for the bucket.<br />The Secret is created in the same Namespace as the BucketAccess and is deleted when the<br />BucketAccess is deleted and deprovisioned.<br />The Secret name must be unique across all bucketClaimRefs for all BucketAccesses in the same<br />Namespace.<br />Must be a valid Kubernetes resource name: at most 253 characters, consisting only of<br />lower-case alphanumeric characters, hyphens, and periods, starting and ending with an<br />alphanumeric character. |  | MaxLength: 253 <br />MinLength: 1 <br /> |


#### BucketClaimList



BucketClaimList contains a list of BucketClaim





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketClaimList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[BucketClaim](#bucketclaim) array_ |  |  |  |


#### BucketClaimReference



BucketClaimReference is a reference to a BucketClaim object.



_Appears in:_
- [BucketSpec](#bucketspec)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `name` _string_ | name is the name of the BucketClaim being referenced.<br />Must be a valid Kubernetes resource name: at most 253 characters, consisting only of<br />lower-case alphanumeric characters, hyphens, and periods, starting and ending with an<br />alphanumeric character. |  | MaxLength: 253 <br />MinLength: 1 <br /> |
| `namespace` _string_ | namespace is the namespace of the BucketClaim being referenced.<br />Must be a valid Kubernetes Namespace name: at most 63 characters, consisting only of<br />lower-case alphanumeric characters and hyphens, starting and ending with alphanumerics. |  | MaxLength: 63 <br />MinLength: 1 <br /> |
| `uid` _[UID](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#uid-types-pkg)_ | uid is the UID of the BucketClaim being referenced.<br />Must be a valid Kubernetes UID: RFC 4122 form with lowercase hexadecimal characters<br />(xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx). |  | MaxLength: 36 <br />MinLength: 36 <br />Pattern: `^[0-9a-f]\{8\}-([0-9a-f]\{4\}\-)\{3\}[0-9a-f]\{12\}$` <br />Type: string <br /> |


#### BucketClaimSpec



BucketClaimSpec defines the desired state of BucketClaim

_Validation:_
- MinProperties: 1

_Appears in:_
- [BucketClaim](#bucketclaim)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `bucketClassName` _string_ | bucketClassName selects the BucketClass for provisioning the BucketClaim.<br />This field is used only for BucketClaim dynamic provisioning.<br />If unspecified, existingBucketName must be specified for binding to an existing Bucket.<br />Must be a valid Kubernetes resource name: at most 253 characters, consisting only of<br />lower-case alphanumeric characters, hyphens, and periods, starting and ending with an<br />alphanumeric character. |  | MaxLength: 253 <br />MinLength: 1 <br /> |
| `protocols` _[ObjectProtocol](#objectprotocol) array_ | protocols lists object storage protocols that the provisioned Bucket must support.<br />If specified, COSI will verify that each item is advertised as supported by the driver.<br />It is recommended to specify all protocols that applications will rely on in BucketAccesses<br />referencing this BucketClaim.<br />Possible values: 'S3', 'Azure', 'GCS'. |  | Enum: [S3 Azure GCS] <br />MaxItems: 3 <br />MinItems: 1 <br /> |
| `existingBucketName` _string_ | existingBucketName selects the name of an existing Bucket resource that this BucketClaim<br />should bind to.<br />This field is used only for BucketClaim static provisioning.<br />If unspecified, bucketClassName must be specified for dynamically provisioning a new bucket.<br />Must be a valid Kubernetes resource name: at most 253 characters, consisting only of<br />lower-case alphanumeric characters, hyphens, and periods, starting and ending with an<br />alphanumeric character. |  | MaxLength: 253 <br />MinLength: 1 <br /> |


#### BucketClaimStatus



BucketClaimStatus defines the observed state of BucketClaim.



_Appears in:_
- [BucketClaim](#bucketclaim)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `boundBucketName` _string_ | boundBucketName is the name of the Bucket this BucketClaim is bound to.<br />Must be a valid Kubernetes resource name: at most 253 characters, consisting only of<br />lower-case alphanumeric characters, hyphens, and periods, starting and ending with an<br />alphanumeric character. |  | MaxLength: 253 <br />MinLength: 1 <br /> |
| `readyToUse` _boolean_ | readyToUse indicates that the bucket is ready for consumption by workloads. |  |  |
| `protocols` _[ObjectProtocol](#objectprotocol) array_ | protocols is the set of protocols the bound Bucket reports to support. BucketAccesses can<br />request access to this BucketClaim using any of the protocols reported here.<br />Possible values: 'S3', 'Azure', 'GCS'. |  | Enum: [S3 Azure GCS] <br />MaxItems: 3 <br />MinItems: 1 <br /> |
| `error` _[TimestampedError](#timestampederror)_ | error holds the most recent error message, with a timestamp.<br />This is cleared when provisioning is successful. |  | MinProperties: 0 <br /> |


#### BucketClass



BucketClass defines a named "class" of object storage buckets.
Different classes might map to different object storage protocols, quality-of-service levels,
backup policies, or any other arbitrary configuration determined by storage administrators.
The name of a BucketClass object is significant, and is how users can request a particular class.



_Appears in:_
- [BucketClassList](#bucketclasslist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketClass` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketClassSpec](#bucketclassspec)_ | spec defines the BucketClass. spec is entirely immutable. |  |  |


#### BucketClassList



BucketClassList contains a list of BucketClass





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketClassList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[BucketClass](#bucketclass) array_ |  |  |  |


#### BucketClassSpec



BucketClassSpec defines the BucketClass.



_Appears in:_
- [BucketClass](#bucketclass)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `driverName` _string_ | driverName is the name of the driver that fulfills requests for this BucketClass.<br />See driver documentation to determine the correct value to set.<br />Must be 63 characters or less, beginning and ending with an alphanumeric character<br />([a-z0-9A-Z]) with dashes (-), dots (.), and alphanumerics between. |  | MaxLength: 63 <br />MinLength: 1 <br />Pattern: `^[a-zA-Z0-9]([a-zA-Z0-9\-\.]\{0,61\}[a-zA-Z0-9])?$` <br /> |
| `deletionPolicy` _[BucketDeletionPolicy](#bucketdeletionpolicy)_ | deletionPolicy determines whether a Bucket created through the BucketClass should be deleted<br />when its bound BucketClaim is deleted.<br />Possible values:<br /> - Retain: keep both the Bucket object and the backend bucket<br /> - Delete: delete both the Bucket object and the backend bucket |  | Enum: [Retain Delete] <br /> |
| `parameters` _object (keys:string, values:string)_ | parameters is an opaque map of driver-specific configuration items passed to the driver that<br />fulfills requests for this BucketClass.<br />See driver documentation to determine supported parameters and their effects.<br />A maximum of 512 parameters are allowed. |  | MaxProperties: 512 <br />MinProperties: 1 <br /> |


#### BucketDeletionPolicy

_Underlying type:_ _string_

BucketDeletionPolicy configures COSI's behavior when a Bucket resource is deleted.

_Validation:_
- Enum: [Retain Delete]

_Appears in:_
- [BucketClassSpec](#bucketclassspec)
- [BucketSpec](#bucketspec)

| Field | Description |
| --- | --- |
| `Retain` | BucketDeletionPolicyRetain configures COSI to keep the Bucket object as well as the backend<br />bucket when a Bucket resource is deleted.<br /> |
| `Delete` | BucketDeletionPolicyDelete configures COSI to delete the Bucket object as well as the backend<br />bucket when a Bucket resource is deleted.<br /> |




#### BucketList



BucketList contains a list of Bucket





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[Bucket](#bucket) array_ |  |  |  |


#### BucketSpec



BucketSpec defines the desired state of Bucket



_Appears in:_
- [Bucket](#bucket)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `driverName` _string_ | driverName is the name of the driver that fulfills requests for this Bucket.<br />See driver documentation to determine the correct value to set.<br />Must be 63 characters or less, beginning and ending with an alphanumeric character<br />([a-z0-9A-Z]) with dashes (-), dots (.), and alphanumerics between. |  | MaxLength: 63 <br />MinLength: 1 <br />Pattern: `^[a-zA-Z0-9]([a-zA-Z0-9\-\.]\{0,61\}[a-zA-Z0-9])?$` <br /> |
| `deletionPolicy` _[BucketDeletionPolicy](#bucketdeletionpolicy)_ | deletionPolicy determines whether a Bucket should be deleted when its bound BucketClaim is<br />deleted. This is mutable to allow Admins to change the policy after creation.<br />Possible values:<br /> - Retain: keep both the Bucket object and the backend bucket<br /> - Delete: delete both the Bucket object and the backend bucket |  | Enum: [Retain Delete] <br /> |
| `parameters` _object (keys:string, values:string)_ | parameters is an opaque map of driver-specific configuration items passed to the driver that<br />fulfills requests for this Bucket.<br />See driver documentation to determine supported parameters and their effects.<br />A maximum of 512 parameters are allowed. |  | MaxProperties: 512 <br />MinProperties: 1 <br /> |
| `protocols` _[ObjectProtocol](#objectprotocol) array_ | protocols lists object store protocols that the provisioned Bucket must support.<br />If specified, COSI will verify that each item is advertised as supported by the driver.<br />See driver documentation to determine supported protocols.<br />Possible values: 'S3', 'Azure', 'GCS'. |  | Enum: [S3 Azure GCS] <br />MaxItems: 3 <br />MinItems: 1 <br /> |
| `bucketClaimRef` _[BucketClaimReference](#bucketclaimreference)_ | bucketClaimRef references the BucketClaim that resulted in the creation of this Bucket.<br />For statically-provisioned buckets, set the namespace and name of the BucketClaim that is<br />allowed to bind to this Bucket; UID may be left unset if desired and will be updated by COSI. |  |  |
| `existingBucketID` _string_ | existingBucketID is the unique identifier for an existing backend bucket known to the driver.<br />Use driver documentation to determine the correct value to set.<br />This field is used only for static Bucket provisioning.<br />This field will be empty when the Bucket is dynamically provisioned from a BucketClaim.<br />Must be at most 2048 characters and consist only of alphanumeric characters ([a-z0-9A-Z]),<br />dashes (-), dots (.), underscores (_), and forward slash (/). |  | MaxLength: 2048 <br />MinLength: 1 <br />Pattern: `^[a-zA-Z0-9/._-]+$` <br /> |


#### BucketStatus



BucketStatus defines the observed state of Bucket.



_Appears in:_
- [Bucket](#bucket)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `readyToUse` _boolean_ | readyToUse indicates that the bucket is ready for consumption by workloads. |  |  |
| `bucketID` _string_ | bucketID is the unique identifier for the backend bucket known to the driver.<br />Must be at most 2048 characters and consist only of alphanumeric characters ([a-z0-9A-Z]),<br />dashes (-), dots (.), underscores (_), and forward slash (/). |  | MaxLength: 2048 <br />MinLength: 1 <br />Pattern: `^[a-zA-Z0-9/._-]+$` <br /> |
| `protocols` _[ObjectProtocol](#objectprotocol) array_ | protocols is the set of protocols the Bucket reports to support. BucketAccesses can request<br />access to this Bucket using any of the protocols reported here.<br />Possible values: 'S3', 'Azure', 'GCS'. |  | Enum: [S3 Azure GCS] <br />MaxItems: 3 <br />MinItems: 1 <br /> |
| `bucketInfo` _object (keys:string, values:string)_ | bucketInfo contains info about the bucket reported by the driver, rendered in the same<br />COSI_<PROTOCOL>_<KEY> format used for the BucketAccess Secret.<br />e.g., COSI_S3_ENDPOINT, COSI_AZURE_STORAGE_ACCOUNT.<br />This should not contain any sensitive information. |  | MaxProperties: 128 <br />MinProperties: 1 <br /> |
| `error` _[TimestampedError](#timestampederror)_ | error holds the most recent error message, with a timestamp.<br />This is cleared when provisioning is successful. |  | MinProperties: 0 <br /> |


#### CosiEnvVar

_Underlying type:_ _string_

A CosiEnvVar defines a COSI environment variable that contains backend bucket or access info.
Vars marked "Required" will be present with non-empty values in BucketAccess Secrets.
Some required vars may only be required in certain contexts, like when a specific
AuthenticationType is used.
Some vars are only relevant for specific protocols.
Non-relevant vars will not be present, even when marked "Required".
Vars are used as data keys in BucketAccess Secrets.
Vars must be all-caps and must begin with `COSI_`.



_Appears in:_
- [BucketInfoVar](#bucketinfovar)
- [CredentialVar](#credentialvar)





#### ObjectProtocol

_Underlying type:_ _string_

ObjectProtocol represents an object protocol type.

_Validation:_
- Enum: [S3 Azure GCS]

_Appears in:_
- [BucketAccessSpec](#bucketaccessspec)
- [BucketClaimSpec](#bucketclaimspec)
- [BucketClaimStatus](#bucketclaimstatus)
- [BucketSpec](#bucketspec)
- [BucketStatus](#bucketstatus)

| Field | Description |
| --- | --- |
| `S3` | ObjectProtocolS3 represents the S3 object protocol type.<br /> |
| `Azure` | ObjectProtocolS3 represents the Azure Blob object protocol type.<br /> |
| `GCS` | ObjectProtocolS3 represents the Google Cloud Storage object protocol type.<br /> |


#### TimestampedError



TimestampedError contains an error message with timestamp.

_Validation:_
- MinProperties: 0

_Appears in:_
- [BucketAccessStatus](#bucketaccessstatus)
- [BucketClaimStatus](#bucketclaimstatus)
- [BucketStatus](#bucketstatus)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `time` _[Time](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.34/#time-v1-meta)_ | time is the timestamp when the error was encountered. |  |  |
| `message` _string_ | message is a string detailing the encountered error.<br />NOTE: message will be logged, and it should not contain sensitive information.<br />Must not exceed 1.5MB. |  | MaxLength: 1572864 <br />MinLength: 0 <br /> |


