# End-to-end tests

## Kyverno Chainsaw

Chainsaw provides a declarative approach to test Kubernetes operators and controllers.

While Chainsaw is designed for testing operators and controllers, it can declaratively test any Kubernetes objects.

Chainsaw is an open-source tool that was initially developed for defining and running Kyverno end-to-end tests.

## Configuration

To configure Chainsaw for testing, you need to define parameters for the specific Kubernetes resources and controllers being tested. Below are example configurations.

**Sample Configuration**

```yaml
driverName: sample-driver.objectstorage.k8s.io
deletionPolicy: "Delete" # Options: "Delete" or "Retain"
bucketClassParams:
  foo: bar
  baz: cux
bucketAccessClassParams:
  foo: bar
  baz: cux
authenticationType: "Key" # Options: "Key" or "IAM"
bucketClaimProtocols: ["S3", "Azure"]  # Supported protocols for bucket claims
bucketAccessProtocol: "S3"  # Protocol for bucket access
```

**Example for Linode COSI Driver**

```yaml
driverName: objectstorage.cosi.linode.com
deletionPolicy: "Delete" # Options: "Delete" or "Retain"
bucketClassParams:
  cosi.linode.com/v1/region: us-east  # Specify the region for Linode object storage
  cosi.linode.com/v1/acl: private  # Define the access control list (ACL) settings
  cosi.linode.com/v1/cors: disabled  # Enable or disable Cross-Origin Resource Sharing (CORS)
bucketAccessClassParams:
  cosi.linode.com/v1/permissions: read_write  # Define access permissions
authenticationType: "Key" # Options: "Key" or "IAM"
bucketClaimProtocols: ["S3"]  # Supported protocol for bucket claims
bucketAccessProtocol: "S3"  # Protocol for bucket access
```

### Running tests

To run the Chainsaw end-to-end tests, you can use the following command:

```sh
chainsaw test --values /path/to/values.yaml
```

This command will run the tests using the values defined in the provided YAML configuration file.
Ensure the file is properly configured to suit the Kubernetes objects and controllers you are testing.
