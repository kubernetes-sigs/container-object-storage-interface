![version](https://img.shields.io/badge/status-pre--alpha-lightgrey) ![apiVersion](https://img.shields.io/badge/apiVersion-v1alpha1-lightgreen)


# Container Object Storage Interface

This repository hosts the Container Object Storage Interface (COSI) project.

## Documentation

To deploy, run `kubectl apply -k .`

## Developer Guide

All API definitions are in [`client/apis/objectstorage`](./client/apis/objectstorage/). All API changes **_MUST_** satisfy the following requirements:

- Must be backwards compatible
- Must be in-sync with the API definitions in [sigs.k8s.io/container-object-storage-interface-spec](https://sigs.k8s.io/container-object-storage-interface-spec)

### Build and Test

See `make help` for assistance

## Adding new fields to protocols

Create a new issue raising a RFC for the changes following this format:

**Title:** [RFC] Changes to protocol xyz

**Description:**
> 1. Protocol:
> 2. Fields Added:
> 3. Why is this change necessary?
>    ...(describe why here)...
> 4. Which other COSI projects are affected by this change?
> 5. Upgrade plan
>    (ignore if it doesn't apply)

## References

 - Weekly Meetings: Thursdays from 13:30 to 14:00 US Eastern Time
 - [Roadmap](https://github.com/orgs/kubernetes-sigs/projects/63/)

## Community, discussion, contribution, and support

You can reach the maintainers of this project at:

 - [#sig-storage-cosi](https://kubernetes.slack.com/messages/sig-storage-cosi) slack channel
 - [container-object-storage-interface](https://groups.google.com/g/container-object-storage-interface-wg?pli=1) mailing list

### Code of conduct

Participation in the Kubernetes community is governed by the [Kubernetes Code of Conduct](code-of-conduct.md).
