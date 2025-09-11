# Drivers

> - **Platform** will take you to platform documentation;
> - **COSI Driver Name** will take you to driver repository.

| Platform                                                                                  | COSI Driver Name                                                                       | Description                                                                                                                | Compatible with COSI Version(s) |
| ----------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| [Akamai Cloud Object Storage](https://www.linode.com/products/object-storage/)            | [`objectstorage.cosi.linode.com`](https://github.com/linode/linode-cosi-driver)        | A Kubernetes Container Object Storage Interface (COSI) Driver for Linode                                                   | `v1alpha1`                      |
| [Azure Blob](https://azure.microsoft.com/en-us/products/storage/blobs)                    | [`blob.cosi.azure.com`](https://github.com/Azure/azure-cosi-driver)                    | This driver allows Kubernetes to use Azure Blob Storage using the Container Storage Object Interface (COSI) infrastructure | `v1alpha1`                      |
| [Ceph Rados Gateway](https://docs.ceph.com/en/latest/radosgw/)                            | [`ceph.objectstorage.k8s.io`](https://github.com/ceph/ceph-cosi)                       | COSI driver for Ceph Object Store aka RGW                                                                                  | `v1alpha1`                      |
| [Dell ObjectScale](https://www.dell.com/en-us/dt/storage/objectscale.htm)                 | [`cosi.dellemc.com`](https://github.com/dell/cosi)                                     | COSI Driver for Dell ObjectScale                                                                                           | `v1alpha1`                      |
| [HPE Alletra Storage MP X10000](https://www.hpe.com/us/en/alletra-storage-mp-x10000.html) | [`cosi.hpe.com`](https://github.com/hpe-storage/cosi-driver)                           | A Kubernetes Container Object Storage Interface (COSI) driver for HPE Alletra Storage MP X10000                            | `v1alpha1`                      |
| [Scality RING and ARTESCA Object Storage](https://www.scality.com/)                       | [`cosi.scality.com`](https://github.com/scality/cosi-driver)                           | Scality COSI Driver integrates Scality RING, ARTESCA, and other AWS S3 and IAM compatible object storage with Kubernetes   | `v1alpha1`                      |
| [SeaweedFS](https://seaweedfs.github.io)                                                  | [`seaweedfs.objectstorage.k8s.io`](https://github.com/seaweedfs/seaweedfs-cosi-driver) | COSI driver implementation for SeaweedFS                                                                                   | `v1alpha1`                      |

## Deprecated drivers

Deprecated drivers are no longer maintained or recommended for use with COSI; users should migrate to supported alternatives to ensure compatibility and security.

| Name                      | COSI Driver Name                                                                        | Description                                                                                             | Compatible with COSI Version(s) |
| ------------------------- | --------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------- |
| [S3GW](https://s3gw.tech) | [`s3gw.objectstorage.k8s.io`](https://github.com/s3gw-tech/s3gw-cosi-driver)            | COSI driver for s3gw                                                                                    | `v1alpha1`                      |
| [MinIO](https://min.io)   | [`minio.objectstorage.k8s.io`](https://github.com/kubernetes-retired/cosi-driver-minio) | Sample Driver that provides reference implementation for Container Object Storage Interface (COSI) API. | `pre-alpha`                     |
