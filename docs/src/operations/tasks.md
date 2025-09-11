# COSI Management Tasks

This section provides details for some of the operations that need to be performed when managing COSI components.

## Administrative Tasks

### Installing Custom Resources and Controller

Refer to [Quickstart Guide](./quickstart.md) for installation instructions.

### Installing Driver

Refer to [Installing Driver](./installing-driver.md) for detailed steps.

### Creating BucketClasses and BucketAccessClasses

These resources define storage classes and access policies for object storage.

```yaml
---
apiVersion: objectstorage.k8s.io/v1alpha1
kind: BucketAccessClass
metadata:
  name: example-accessclass
driverName: cosi.example.com
authenticationType: Key
parameters:
  foo: bar
---
apiVersion: objectstorage.k8s.io/v1alpha1
kind: BucketClass
metadata:
  name: example-class
driverName: cosi.example.com
deletionPolicy: Delete
parameters:
  foo: bar
```

## User Tasks

### Creating BucketClaims

A `BucketClaim` requests a new bucket provisioned by the COSI driver.

```yaml
apiVersion: objectstorage.k8s.io/v1alpha1
kind: BucketClaim
metadata:
  name: example-claim
spec:
  bucketClassName: example-class
  protocols: [ 'S3' ]
```

### Creating BucketAccesses

A `BucketAccess` grants access to a previously created bucket claim.

```yaml
apiVersion: objectstorage.k8s.io/v1alpha1
kind: BucketAccess
metadata:
  name: example-access
spec:
  bucketClaimName: example-claim
  protocol: S3
  bucketAccessClassName: example-accessclass
  credentialsSecretName: example-secret
```

### Using the COSI-Provisioned Object Storage Credentials

Applications can access COSI-provisioned object storage credentials using Kubernetes Secrets.

```yaml
spec:
  template:
    spec:
      containers:
        - volumeMounts:
            - mountPath: /conf
              name: example-secret-vol
      volumes:
        - name: example-secret-vol
          secret:
            secretName: example-secret
            items:
              - key: BucketInfo
                path: BucketInfo
```
