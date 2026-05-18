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

Some specific workflows are documented below.

### Building and deploying COSI controller changes locally

The local dev flow targets a [minikube](https://minikube.sigs.k8s.io/) cluster and
sideloads images into the node's container runtime via `minikube image load`.

```sh
make -j prebuild     # codegen, fmt, docs, vendor
make cluster         # start minikube (creates extra disks used by Rook)
make deploy          # build + sideload + apply the controller manifests
```

`make deploy` chains `build.controller` -> `load.controller` (which runs
`minikube image load $(CONTROLLER_TAG)`) -> `kustomize build | kubectl apply`,
so a single command builds the current source tree and rolls it out.

To target a different registry/tag (for example in CI that pushes to a remote
registry), override `CONTROLLER_TAG` and run `build.controller` + `push.controller`
explicitly:

```sh
export CONTROLLER_TAG="$MY_REPO"/cosi-controller:$(git rev-parse --short HEAD)
make build.controller
make push.controller
```

### Running end-to-end tests locally

End-to-end tests rely on a Rook/Ceph backend, the COSI controller, and the
sample COSI driver, all running in the local minikube cluster:

```sh
make cluster                # minikube up
make deploy-rook            # Rook operator + CephCluster + RGW; writes
                            # test/e2e/rook-credentials.yaml
make deploy                 # build + sideload + deploy controller
make deploy-sample-driver   # build + sideload + deploy sample driver
make test-e2e               # run chainsaw suite
```

`make deploy-sample-driver` accepts `SAMPLE_DRIVER_PATH` to reuse an existing
checkout of [`cosi-driver-sample`](https://github.com/kubernetes-sigs/cosi-driver-sample);
otherwise it clones the repo into `.cache/sample-driver` (controlled by
`SAMPLE_DRIVER_BRANCH`, default `main`).

The image tag scheme defaults to `localhost:5000/<component>:dev`. The `:dev`
suffix (instead of `:latest`) is deliberate: kubelet's default
`imagePullPolicy` for non-`:latest` tags is `IfNotPresent`, so sideloaded
images are used directly without any pull attempt against the (unused)
registry host. Override the suffix with `IMAGE_TAG=<tag>` if needed.
