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
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
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
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketAccessSpec](#bucketaccessspec)_ | spec defines the desired state of BucketAccess |  |  |
| `status` _[BucketAccessStatus](#bucketaccessstatus)_ | status defines the observed state of BucketAccess |  |  |


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
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketAccessClassSpec](#bucketaccessclassspec)_ | spec defines the desired state of BucketAccessClass |  |  |
| `status` _[BucketAccessClassStatus](#bucketaccessclassstatus)_ | status defines the observed state of BucketAccessClass |  |  |


#### BucketAccessClassList



BucketAccessClassList contains a list of BucketAccessClass





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketAccessClassList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[BucketAccessClass](#bucketaccessclass) array_ |  |  |  |


#### BucketAccessClassSpec



BucketAccessClassSpec defines the desired state of BucketAccessClass



_Appears in:_
- [BucketAccessClass](#bucketaccessclass)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `foo` _string_ | foo is an example field of BucketAccessClass. Edit bucketaccessclass_types.go to remove/update |  |  |


#### BucketAccessClassStatus



BucketAccessClassStatus defines the observed state of BucketAccessClass.



_Appears in:_
- [BucketAccessClass](#bucketaccessclass)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#condition-v1-meta) array_ | conditions represent the current state of the BucketAccessClass resource.<br />Each condition has a unique type and reflects the status of a specific aspect of the resource.<br /><br />Standard condition types include:<br />- "Available": the resource is fully functional<br />- "Progressing": the resource is being created or updated<br />- "Degraded": the resource failed to reach or maintain its desired state<br /><br />The status of each condition is one of True, False, or Unknown. |  |  |


#### BucketAccessList



BucketAccessList contains a list of BucketAccess





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketAccessList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[BucketAccess](#bucketaccess) array_ |  |  |  |


#### BucketAccessSpec



BucketAccessSpec defines the desired state of BucketAccess



_Appears in:_
- [BucketAccess](#bucketaccess)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `foo` _string_ | foo is an example field of BucketAccess. Edit bucketaccess_types.go to remove/update |  |  |


#### BucketAccessStatus



BucketAccessStatus defines the observed state of BucketAccess.



_Appears in:_
- [BucketAccess](#bucketaccess)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#condition-v1-meta) array_ | conditions represent the current state of the BucketAccess resource.<br />Each condition has a unique type and reflects the status of a specific aspect of the resource.<br /><br />Standard condition types include:<br />- "Available": the resource is fully functional<br />- "Progressing": the resource is being created or updated<br />- "Degraded": the resource failed to reach or maintain its desired state<br /><br />The status of each condition is one of True, False, or Unknown. |  |  |


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
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketClaimSpec](#bucketclaimspec)_ | spec defines the desired state of BucketClaim |  |  |
| `status` _[BucketClaimStatus](#bucketclaimstatus)_ | status defines the observed state of BucketClaim |  |  |


#### BucketClaimList



BucketClaimList contains a list of BucketClaim





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketClaimList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[BucketClaim](#bucketclaim) array_ |  |  |  |


#### BucketClaimSpec



BucketClaimSpec defines the desired state of BucketClaim



_Appears in:_
- [BucketClaim](#bucketclaim)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `bucketClassName` _string_ | bucketClassName selects the BucketClass for provisioning the BucketClaim.<br />This field is used only for BucketClaim dynamic provisioning.<br />If unspecified, existingBucketName must be specified for binding to an existing Bucket. |  |  |
| `protocols` _[ObjectStoreProtocol](#objectstoreprotocol) array_ | protocols lists object store protocols that the provisioned Bucket must support.<br />If specified, COSI will verify that each item is advertised as supported by the driver. |  | Enum: [S3 Azure GCS] <br /> |
| `existingBucketName` _string_ | existingBucketName selects the name of an existing Bucket resource that this BucketClaim<br />should bind to.<br />This field is used only for BucketClaim static provisioning.<br />If unspecified, bucketClassName must be specified for dynamically provisioning a new bucket. |  |  |


#### BucketClaimStatus



BucketClaimStatus defines the observed state of BucketClaim.



_Appears in:_
- [BucketClaim](#bucketclaim)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `conditions` _[Condition](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#condition-v1-meta) array_ | conditions represent the current state of the BucketClaim resource.<br />Each condition has a unique type and reflects the status of a specific aspect of the resource.<br /><br />Standard condition types include:<br />- "Available": the resource is fully functional<br />- "Progressing": the resource is being created or updated<br />- "Degraded": the resource failed to reach or maintain its desired state<br /><br />The status of each condition is one of True, False, or Unknown. |  |  |


#### BucketClass



BucketClass is the Schema for the bucketclasses API



_Appears in:_
- [BucketClassList](#bucketclasslist)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketClass` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#objectmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `spec` _[BucketClassSpec](#bucketclassspec)_ | spec defines the desired state of BucketClass |  |  |


#### BucketClassList



BucketClassList contains a list of BucketClass





| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `apiVersion` _string_ | `objectstorage.k8s.io/v1alpha2` | | |
| `kind` _string_ | `BucketClassList` | | |
| `kind` _string_ | Kind is a string value representing the REST resource this object represents.<br />Servers may infer this from the endpoint the client submits requests to.<br />Cannot be updated.<br />In CamelCase.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds |  |  |
| `apiVersion` _string_ | APIVersion defines the versioned schema of this representation of an object.<br />Servers should convert recognized schemas to the latest internal value, and<br />may reject unrecognized values.<br />More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources |  |  |
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[BucketClass](#bucketclass) array_ |  |  |  |


#### BucketClassSpec



BucketClassSpec defines the desired state of BucketClass



_Appears in:_
- [BucketClass](#bucketclass)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `driverName` _string_ | driverName is the name of the driver that fulfills requests for this BucketClass. |  | MinLength: 1 <br /> |
| `deletionPolicy` _[BucketDeletionPolicy](#bucketdeletionpolicy)_ | deletionPolicy determines whether a Bucket created through the BucketClass should be deleted<br />when its bound BucketClaim is deleted.<br />Possible values:<br /> - Retain: keep both the Bucket object and the backend bucket<br /> - Delete: delete both the Bucket object and the backend bucket |  | Enum: [Retain Delete] <br /> |
| `parameters` _object (keys:string, values:string)_ | parameters is an opaque map of driver-specific configuration items passed to the driver that<br />fulfills requests for this BucketClass. |  |  |


#### BucketDeletionPolicy

_Underlying type:_ _string_

BucketDeletionPolicy configures COSI's behavior when a Bucket resource is deleted.

_Validation:_
- Enum: [Retain Delete]

_Appears in:_
- [BucketClassSpec](#bucketclassspec)

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
| `metadata` _[ListMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.32/#listmeta-v1-meta)_ | Refer to Kubernetes API documentation for fields of `metadata`. |  |  |
| `items` _[Bucket](#bucket) array_ |  |  |  |


#### BucketSpec



BucketSpec defines the desired state of Bucket



_Appears in:_
- [Bucket](#bucket)

| Field | Description | Default | Validation |
| --- | --- | --- | --- |
| `foo` _string_ | foo is an example field of Bucket. Edit bucket_types.go to remove/update |  |  |


#### BucketStatus



BucketStatus defines the observed state of Bucket.



_Appears in:_
- [Bucket](#bucket)



#### ObjectStoreProtocol

_Underlying type:_ _string_

ObjectStoreProtocol identifies an object storage protocol.

_Validation:_
- Enum: [S3 Azure GCS]

_Appears in:_
- [BucketClaimSpec](#bucketclaimspec)

| Field | Description |
| --- | --- |
| `S3` | ObjectStoreProtocolS3 identifies the AWS S3 object storage protocol.<br /> |
| `Azure` | ObjectStoreProtocolAzure identifies the Azure Blob object storage protocol.<br /> |
| `GCS` | ObjectStoreProtocolGcs identifies the Google Cloud Storage (GCS) object storage protocol.<br /> |


