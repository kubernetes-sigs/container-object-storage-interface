# Developing a COSI Driver for Kubernetes

## Authoring COSI Driver

The target audience for this documentation is third-party developers interested in developing [Container Object Storage Interface (COSI)](https://github.com/kubernetes/enhancements/tree/master/keps/sig-storage/1979-object-storage-support) driver on Kubernetes.

The goal of [Container Object Storage Interface](https://github.com/kubernetes-sigs/container-object-storage-interface/blob/master/proto/spec.md) is to be the standard for providing Kubernetes cluster users and administrators a normalized and familiar means of managing object storage. With Container Object Storage Interface, third-party storage providers can write and deploy plugins exposing new object storage systems in Kubernetes without ever having to touch the core Kubernetes code.

Kubernetes users interested in how to deploy or manage an existing Container Object Storage Interface driver on Kubernetes should look at the documentation provided by the author of the driver. The list of drivers can be found [here](../drivers.html)

// TODO
