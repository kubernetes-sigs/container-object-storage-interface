# API Reference

## Packages
- [objectstorage.k8s.io/v1alpha1](#objectstoragek8siov1alpha1)


## objectstorage.k8s.io/v1alpha1




#### AuthenticationType

_Underlying type:_ _string_





_Appears in:_
- [BucketAccessClass](#bucketaccessclass)

| Field | Description |
| --- | --- |
| `Key` |  |
| `IAM` |  |


#### Bucket







_Appears in:_
- [BucketList](#bucketlist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketSpec](#bucketspec)_ |  |  |  |
| `status` _[BucketStatus](#bucketstatus)_ |  |  |  |


#### BucketAccess







_Appears in:_
- [BucketAccessList](#bucketaccesslist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketAccessSpec](#bucketaccessspec)_ |  |  |  |
| `status` _[BucketAccessStatus](#bucketaccessstatus)_ |  |  |  |


#### BucketAccessClass







_Appears in:_
- [BucketAccessClassList](#bucketaccessclasslist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `driverName` _string_ | DriverName is the name of driver associated with<br />this BucketAccess |  |  |
| `authenticationType` _[AuthenticationType](#authenticationtype)_ | AuthenticationType denotes the style of authentication<br />It can be one of<br />Key - access, secret tokens based authentication<br />IAM - implicit authentication of pods to the OSP based on service account mappings |  |  |
| `parameters` _object (keys:string, values:string)_ | Parameters is an opaque map for passing in configuration to a driver<br />for granting access to a bucket |  |  |






#### BucketAccessSpec







_Appears in:_
- [BucketAccess](#bucketaccess)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `bucketClaimName` _string_ | BucketClaimName is the name of the BucketClaim. |  |  |
| `protocol` _[Protocol](#protocol)_ | Protocol is the name of the Protocol<br />that this access credential is supposed to support<br />If left empty, it will choose the protocol supported<br />by the bucket. If the bucket supports multiple protocols,<br />the end protocol is determined by the driver. |  |  |
| `bucketAccessClassName` _string_ | BucketAccessClassName is the name of the BucketAccessClass |  |  |
| `credentialsSecretName` _string_ | CredentialsSecretName is the name of the secret that COSI should populate<br />with the credentials. If a secret by this name already exists, then it is<br />assumed that credentials have already been generated. It is not overridden.<br />This secret is deleted when the BucketAccess is delted. |  |  |
| `serviceAccountName` _string_ | ServiceAccountName is the name of the serviceAccount that COSI will map<br />to the OSP service account when IAM styled authentication is specified |  |  |


#### BucketAccessStatus







_Appears in:_
- [BucketAccess](#bucketaccess)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `accountID` _string_ | AccountID is the unique ID for the account in the OSP. It will be populated<br />by the COSI sidecar once access has been successfully granted. |  |  |
| `accessGranted` _boolean_ | AccessGranted indicates the successful grant of privileges to access the bucket |  |  |


#### BucketClaim







_Appears in:_
- [BucketClaimList](#bucketclaimlist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketClaimSpec](#bucketclaimspec)_ |  |  |  |
| `status` _[BucketClaimStatus](#bucketclaimstatus)_ |  |  |  |




#### BucketClaimSpec







_Appears in:_
- [BucketClaim](#bucketclaim)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `bucketClassName` _string_ | Name of the BucketClass |  |  |
| `protocols` _[Protocol](#protocol) array_ | Protocols are the set of data API this bucket is required to support.<br />The possible values for protocol are:<br />-  S3: Indicates Amazon S3 protocol<br />-  Azure: Indicates Microsoft Azure BlobStore protocol<br />-  GCS: Indicates Google Cloud Storage protocol |  |  |
| `existingBucketName` _string_ | Name of a bucket object that was manually<br />created to import a bucket created outside of COSI<br />If unspecified, then a new Bucket will be dynamically provisioned |  |  |


#### BucketClaimStatus







_Appears in:_
- [BucketClaim](#bucketclaim)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `bucketReady` _boolean_ | BucketReady indicates that the bucket is ready for consumpotion<br />by workloads |  |  |
| `bucketName` _string_ | BucketName is the name of the provisioned Bucket in response<br />to this BucketClaim. It is generated and set by the COSI controller<br />before making the creation request to the OSP backend. |  |  |


#### BucketClass







_Appears in:_
- [BucketClassList](#bucketclasslist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `driverName` _string_ | DriverName is the name of driver associated with this bucket |  |  |
| `deletionPolicy` _[DeletionPolicy](#deletionpolicy)_ | DeletionPolicy is used to specify how COSI should handle deletion of this<br />bucket. There are 2 possible values:<br /> - Retain: Indicates that the bucket should not be deleted from the OSP<br /> - Delete: Indicates that the bucket should be deleted from the OSP<br />       once all the workloads accessing this bucket are done | Retain |  |
| `parameters` _object (keys:string, values:string)_ | Parameters is an opaque map for passing in configuration to a driver<br />for creating the bucket |  |  |






#### BucketSpec







_Appears in:_
- [Bucket](#bucket)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `driverName` _string_ | DriverName is the name of driver associated with this bucket |  |  |
| `bucketClassName` _string_ | Name of the BucketClass specified in the BucketRequest |  |  |
| `bucketClaim` _[ObjectReference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#objectreference-v1-core)_ | Name of the BucketClaim that resulted in the creation of this Bucket<br />In case the Bucket object was created manually, then this should refer<br />to the BucketClaim with which this Bucket should be bound |  |  |
| `protocols` _[Protocol](#protocol) array_ | Protocols are the set of data APIs this bucket is expected to support.<br />The possible values for protocol are:<br />-  S3: Indicates Amazon S3 protocol<br />-  Azure: Indicates Microsoft Azure BlobStore protocol<br />-  GCS: Indicates Google Cloud Storage protocol |  |  |
| `parameters` _object (keys:string, values:string)_ |  |  |  |
| `deletionPolicy` _[DeletionPolicy](#deletionpolicy)_ | DeletionPolicy is used to specify how COSI should handle deletion of this<br />bucket. There are 2 possible values:<br /> - Retain: Indicates that the bucket should not be deleted from the OSP (default)<br /> - Delete: Indicates that the bucket should be deleted from the OSP<br />       once all the workloads accessing this bucket are done | Retain |  |
| `existingBucketID` _string_ | ExistingBucketID is the unique id of the bucket in the OSP. This field should be<br />used to specify a bucket that has been created outside of COSI.<br />This field will be empty when the Bucket is dynamically provisioned by COSI. |  |  |


#### BucketStatus







_Appears in:_
- [Bucket](#bucket)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `bucketReady` _boolean_ | BucketReady is a boolean condition to reflect the successful creation<br />of a bucket. |  |  |
| `bucketID` _string_ | BucketID is the unique id of the bucket in the OSP. This field will be<br />populated by COSI. |  |  |


#### DeletionPolicy

_Underlying type:_ _string_





_Appears in:_
- [BucketClass](#bucketclass)
- [BucketSpec](#bucketspec)

| Field | Description |
| --- | --- |
| `Retain` |  |
| `Delete` |  |


#### Protocol

_Underlying type:_ _string_





_Appears in:_
- [BucketAccessSpec](#bucketaccessspec)
- [BucketClaimSpec](#bucketclaimspec)
- [BucketSpec](#bucketspec)

| Field | Description |
| --- | --- |
| `S3` |  |
| `Azure` |  |
| `GCP` |  |


