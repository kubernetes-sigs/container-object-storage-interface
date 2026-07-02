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

The `test-e2e` target runs a **purely functional** Chainsaw suite: it
creates `BucketClass`, `BucketAccessClass`, `BucketClaim`, and
`BucketAccess` objects, asserts the controller and sample driver react
correctly, and exercises the issued credentials against the S3 backend.
It does **not** deploy any cluster components.

You are responsible for getting the cluster into the following state
before invoking `make test-e2e`:

1. A Kubernetes cluster reachable via `kubectl`, with a raw disk
   attached for the S3 backend.
2. An S3-compatible backend reachable from inside the cluster. The CI
   environment provisions a Ceph RGW via the
   [`cosi-driver-sample`](https://github.com/kubernetes-sigs/cosi-driver-sample)
   repo's own tooling.
3. The COSI controller deployed (`make deploy`).
4. The sample driver Deployment running in `driverNamespace`
   (`cosi-driver-sample-system` by default).
5. The Secrets `cosi-sample-s3-admin-secret` and
   `cosi-sample-s3-access-secret` populated with backend credentials in
   `driverNamespace`. Each Secret must contain the keys
   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL`, and
   optionally `AWS_REGION` (the names `cosi-driver-sample/internal/s3/s3.go`
   reads).

The Makefile handles step 3 and runs the Chainsaw suite. Steps 1, 2, 4,
and 5 are intentionally **not** wrapped in `make` targets to keep the
developer API of this project small and stable across user environments.
The `hack/` scripts referenced below automate them for the project's
Prow CI and are not maintained as portable user-facing tools.

### Required tools (your responsibility)

- A Kubernetes cluster of your choice with enough capacity for the
  controller, sample driver, and S3 backend.
- `kubectl` configured to point at it.
- `docker` (or any other tool sufficient to build OCI images, exposed as
  `DOCKER`).
- `kustomize` (any sufficiently recent version).

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

Use any S3-compatible service you like, or use the sample driver's
backend setup script (it stands up a single-node Ceph RGW via Rook):

```sh
git clone --depth 1 https://github.com/kubernetes-sigs/cosi-driver-sample.git ../cosi-driver-sample
OUT_CREDS_FILE="$(pwd)/.cache/s3-credentials.yaml" \
  ../cosi-driver-sample/hack/setup-s3-backend.sh
```

The script writes a YAML values file with the keys `s3Endpoint`,
`s3Region`, `adminAccessKeyId`, `adminSecretAccessKey`, `accessKeyId`,
`accessSecretKey`. Translate those into the two Secrets the
`BucketClass` and `BucketAccessClass` reference:

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

### Deploying the sample driver

The end-to-end suite drives the [`cosi-driver-sample`](https://github.com/kubernetes-sigs/cosi-driver-sample)
project. That repo owns the canonical build tooling; this project does
not duplicate it.

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
backend, deploys COSI, and runs `make test-e2e`. Treat the CI scripts as
the source of truth for the CI environment, not a portable developer
workflow.
