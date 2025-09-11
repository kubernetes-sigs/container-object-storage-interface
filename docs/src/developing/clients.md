# Developing client applications

## No official client libraries

We do not provide official client libraries for interacting with COSI secrets. Instead, we encourage users to build their own clients using standard tools and APIs.

- Different users have different needs, and maintaining an official client library might limit their ability to customize or optimize for specific use cases.
- Providing and maintaining client libraries across multiple languages is a significant effort, requiring continuous updates and support.
- By relying on standard APIs, users can integrate directly with COSI without additional abstraction layers that may introduce unnecessary complexity.

## Stability and breaking changes

We follow a strict versioning policy to ensure stability while allowing for necessary improvements.
- **Patch Releases (`v1alphaX`)**: No breaking changes are introduced between patch versions.
- **Version Upgrades (`v1alpha1` to `v1alpha2`)**: Breaking changes, including format modifications, may occur between these versions.

For more details, refer to the [Kubernetes Versioning and Deprecation Policy](https://kubernetes.io/docs/reference/using-api/deprecation-policy/).

## Existing Guides

For guidance on developing clients, refer to our language-specific documentation:
- [Go Client Guide](./clients/go.md)
<!-- TODO(guides): add new guides -->

If additional client guides are needed, we welcome contributions from the community.

