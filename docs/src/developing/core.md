# Developing "core" COSI

With "core" COSI we refer to the common set of API and controllers that are required to run any COSI driver.

Before your first contribution, you should follow [the Kubernetes Contributor Guide](https://www.kubernetes.dev/docs/guide/#contributor-guide).

To further understand the COSI architecture, please refer to [KEP-1979: Object Storage
Support](https://github.com/kubernetes/enhancements/tree/master/keps/sig-storage/1979-object-storage-support).

Before contributing a Pull Request, ensure a [GitHub issue](https://github.com/kubernetes-sigs/container-object-storage-interface/issues) exists corresponding to the change.

## Local code development

For new contributors, use `make help`, and use **Core** targets as needed.
These targets ensure changes build successfully, pass basic checks, and are ready for end-to-end tests run in COSI's automated CI.

Other more advanced targets are available and also described in `make help` output.

## Test environment

The `test-e2e` target runs a **purely functional** Chainsaw suite against
the current `kubectl` context. It asserts that the COSI controller and
sample driver are already available, creates `BucketClass`,
`BucketAccessClass`, `BucketClaim`, and `BucketAccess` objects, validates
the issued credentials Secret, and uses those credentials to upload and
delete a probe object in the S3 backend.

`make test-e2e` does **not** create a cluster, deploy the controller,
deploy the sample driver, or configure object storage. Prepare those
pieces before running the suite.

### Required cluster state

Before invoking `make test-e2e`, the current `kubectl` context must point
to a cluster with:

1. An S3-compatible backend reachable from pods in the cluster. A raw disk
   is only required if you choose a backend such as Rook/Ceph that needs
   one.
2. The COSI controller deployed in `container-object-storage-system`.
3. The sample driver Deployment named `cosi-sample-driver` running in
   `driverNamespace`, which defaults to `cosi-driver-sample-system`.
4. The Secrets `cosi-sample-s3-admin-secret` and
   `cosi-sample-s3-access-secret` in `driverNamespace`. Each Secret must
   contain `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
   `AWS_ENDPOINT_URL`, and optionally `AWS_REGION`.

The Makefile handles controller deployment through `make deploy` and runs
the Chainsaw suite through `make test-e2e`. The sample driver and S3
backend remain external to the Makefile because their setup depends on
the cluster and backend you choose.

### Required local tools

- A Kubernetes cluster with enough capacity for the controller, sample
  driver, and S3 backend.
- `kubectl` configured to point at that cluster.
- `docker`, or another Docker-compatible OCI image builder exposed as
  `DOCKER`.
- `kustomize`, or `make kustomize` to install the pinned version under
  `.cache/tools`.

### Building and deploying the COSI controller

`make deploy` applies the manifests via `hack/dev-kustomize.sh`, which
patches the controller image reference to `CONTROLLER_TAG` and forces
`imagePullPolicy: IfNotPresent` for local development. For remote
clusters, push `CONTROLLER_TAG` to a registry the nodes can pull from
before deploying.

```sh
make -j prebuild       # codegen, fmt, docs, vendor (slow; skip if already done)
make build.controller  # docker build
make deploy            # kustomize build | kubectl apply (uses CONTROLLER_TAG)
```

Override `CONTROLLER_TAG` to control the image reference used by both
the build and the generated manifests:

```sh
export CONTROLLER_TAG="$MY_REPO"/cosi-controller:$(git rev-parse --short HEAD)
make build.controller
docker push "${CONTROLLER_TAG}"   # done manually; the Makefile does not push
make deploy
```

`make undeploy` removes the controller manifests from the current context.

### Providing an S3 backend

Use any S3-compatible service you like. The sample driver repository also
contains a backend setup script that stands up a single-node Ceph RGW via
Rook:

```sh
git clone --depth 1 https://github.com/kubernetes-sigs/cosi-driver-sample.git ../cosi-driver-sample
OUT_CREDS_FILE="$(pwd)/.cache/s3-credentials.yaml" \
  ../cosi-driver-sample/hack/setup-s3-backend.sh
```

The script writes a YAML values file with the keys `s3Endpoint`,
`s3Region`, `adminAccessKeyId`, `adminSecretAccessKey`, `accessKeyId`,
and `accessSecretKey`.

### Deploying the sample driver

The end-to-end suite drives the [`cosi-driver-sample`](https://github.com/kubernetes-sigs/cosi-driver-sample)
project. That repo owns the canonical build tooling; this project only
builds the sidecar image.

```sh
make build.sidecar
make -C ../cosi-driver-sample build SAMPLE_DRIVER_TAG=cosi-driver-sample:latest

# Push both the sidecar and driver images if your cluster cannot use local images.

kustomize build ../cosi-driver-sample/config/default | kubectl apply -f -
kubectl -n cosi-driver-sample-system set image deployment/cosi-sample-driver \
  driver=cosi-driver-sample:latest \
  objectstorage-provisioner-sidecar=cosi-provisioner-sidecar:latest
kubectl -n cosi-driver-sample-system rollout status deployment/cosi-sample-driver
```

Create the S3 credentials Secrets in the same namespace as the sample
driver. If you used the sample driver's backend setup script, translate
its output values into the Secret keys consumed by the driver:

```sh
. <(grep -E '^(s3Endpoint|s3Region|admin|access)' .cache/s3-credentials.yaml | sed 's/:[[:space:]]*"\(.*\)"/="\1"/')

kubectl create namespace cosi-driver-sample-system --dry-run=client -o yaml | kubectl apply -f -

kubectl -n cosi-driver-sample-system create secret generic cosi-sample-s3-admin-secret \
  --from-literal=AWS_ENDPOINT_URL="${s3Endpoint}" \
  --from-literal=AWS_REGION="${s3Region}" \
  --from-literal=AWS_ACCESS_KEY_ID="${adminAccessKeyId}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${adminSecretAccessKey}" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n cosi-driver-sample-system create secret generic cosi-sample-s3-access-secret \
  --from-literal=AWS_ENDPOINT_URL="${s3Endpoint}" \
  --from-literal=AWS_REGION="${s3Region}" \
  --from-literal=AWS_ACCESS_KEY_ID="${accessKeyId}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${accessSecretKey}" \
  --dry-run=client -o yaml | kubectl apply -f -
```

For kind-based development, `hack/setup-sample-driver.sh` can build the
sample driver, load it into the kind cluster, deploy it, and create these
Secrets from a credentials file produced by the sample driver backend
script:

```sh
CREDS_FILE="$(pwd)/.cache/s3-credentials.yaml" \
  ./hack/setup-sample-driver.sh
```

This helper is primarily maintained for the repository's CI flow. Review
it before using it as a local workflow.

### Running the suite

Once the cluster, S3 backend, controller, sample driver Deployment, and
admin/access Secrets are all in place:

```sh
make test-e2e
```

The suite reads `test/e2e/values.yaml` for `driverName`,
`driverNamespace`, and `deletionPolicy`. Override the file or pass an
additional `--values` to Chainsaw if you need to.

### CI reference

Prow runs `hack/prow-e2e.sh`, which creates a kind cluster with
loop-backed raw devices for Rook/Ceph, deploys the sample driver's S3
backend, builds and loads the controller and sidecar images, deploys
COSI, deploys the sample driver, creates the S3 credential Secrets, and
runs `make test-e2e`.

`hack/setup-kind.sh` and `hack/setup-sample-driver.sh` support that CI
path. Treat the CI scripts as references for the CI environment, not as a
stable portable developer API.
