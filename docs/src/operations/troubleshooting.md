# Troubleshooting

This document provides troubleshooting steps for common issues encountered when using COSI components including Custom Resource Definitions (CRDs), Custom Resources (CRs), the COSI Controller, and Drivers with Sidecars.

## CRD Issues

### Symptoms
- CRDs fail to apply to the cluster.
- Resources are not recognized by `kubectl`.

### Possible Causes & Resolution
1. **Configuration Misalignment**
   - **Check**: Validate whether the installation followed official documentation steps.
   - **Resolve**: Reinstall CRDs by strictly following the [quick-start guide](../quick-start.md), step-by-step.

## Custom Resource (CR) Issues

### Symptoms
- BucketClaim/BucketAccess CRs are not processed.
- Status `bucketReady` or `accessGranted` conditions remain `false`.

### Possible Causes & Resolution
1. **Misconfigured CR Spec**
   - **Check**: Validate required fields (e.g., `bucketName`, `driverName`).
   - **Fix**: Refer to the CR examples in the driver documentation.

2. **Controller Not Responding**
   - **Check**: Verify the controller pods is running (`kubectl get pods -n container-object-storage-system`).
   - **Fix**: Inspect controller logs for errors.

## Controller Issues

### Symptoms
- Controller pod crashes or enters `CrashLoopBackOff`.
- No events generated for CRs.

### Possible Causes & Resolution
1. **Missing Permissions**
   - **Check**: Review RBAC roles for the controller service account.
   - **Fix**: Ensure the controller has permissions to manage CRDs and watch resources.

2. **Reconciliation Failures**
   - **Check**: Look for `Reconcile` errors in controller logs.
   - **Fix**: Validate driver connectivity or CR configurations (e.g., invalid bucket class parameters).

## Driver with Sidecar Issues

### Symptoms
- Sidecar fails to communicate with the driver.
- Bucket provisioning times out.

### Possible Causes & Resolution
1. **Sidecar-Driver Communication Failure**
   - **Check**: Ensure the sidecar and driver share the communication socket (e.g. shared `volumeMounts`).
   - **Fix**: Adjust driver manifests and `COSI_ENDPOINT` for both driver and sidecar.

2. **Resource Conflicts**
   - **Check**: Multiple drivers using the same driver name.
   - **Fix**: Ensure unique `driverName` values per driver instance.

3. **Sidecar Liveness Probe Failures**
   - **Check**: Inspect sidecar logs for health check errors.
   - **Fix**: Adjust liveness/readiness probe thresholds in the sidecar deployment.

## FAQs

* **Q: Why is my CRD not recognized after applying?**  
  A: Ensure the CRD is compatible with your Kubernetes cluster version and the COSI controller is running.

* **Q: The driver isn't responding to provisioning requests. What should I check?**  
  A: Verify driver-sidecar communication.

* **Q: Why is my bucket stuck in `ready: false` state?**  
  A: Check storage quotas, driver availability, and controller logs for reconciliation errors.
