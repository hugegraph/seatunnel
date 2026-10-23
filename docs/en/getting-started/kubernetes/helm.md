---
sidebar_position: 4
---

# Set Up with Helm

This section provides a quick guide to use SeaTunnel with Helm.

:::tip Tip
In Zeta cluster mode, both Master and Worker should be deployed with StatefulSet for production. The current Helm chart is suitable for a quick start, but it still renders Master and Worker as Deployment. Before using Helm in production, read [Kubernetes Deployment](kubernetes.mdx), [Separated Cluster Mode](separated-cluster-mode.md), and [Kubernetes Configuration](configuration.md) to understand the StatefulSet-based topology, Headless Service, checkpoint, IMap MapStore, and slot planning practices.
:::

## Prerequisites

We assume that you have one local installation as follow:

- [docker](https://docs.docker.com/)
- [kubernetes](https://kubernetes.io/)
- [helm](https://helm.sh/docs/intro/quickstart/)

So that the `kubectl` and `helm` commands are available on your local system.

Take kubernetes [minikube](https://minikube.sigs.k8s.io/docs/start/) as an example, you can start a cluster with the following command:

```bash
minikube start --kubernetes-version=v1.23.3
```

## Install

Run from the root of this SeaTunnel fork to install its chart with the default
separated cluster topology:
```bash
helm install seatunnel ./deploy/kubernetes/seatunnel
```
Install with another namespace.
```bash
helm install seatunnel ./deploy/kubernetes/seatunnel -n <your-namespace>
```

For managed Kubernetes services, keep provider-specific changes in a separate values file and pass it with `-f`, for example:

```bash
helm install seatunnel ./deploy/kubernetes/seatunnel -n <your-namespace> -f values-eks.yaml
```

The Apache OCI chart at `oci://registry-1.docker.io/apache/seatunnel-helm` is a
separate published artifact and does not include these fork changes. For release
`seatunnel`, its original master Service is `seatunnel-master`; the source-fork
commands in this guide use `seatunnel-seatunnel-master`.

Common managed-cluster values to review include:

- image repository and `image.pullSecret` for ECR, Artifact Registry, ACR, ACK Container Registry, TCR, SWR, Volcengine Container Registry, or an OpenShift internal registry
- ServiceAccount, RBAC, and OpenShift SecurityContextConstraint requirements for the target namespace
- Service or Ingress annotations for the provider load balancer, subnet, certificate, and internal/external exposure model
- object storage or PersistentVolume settings for checkpoint and state paths
- resource requests, limits, node selectors, tolerations, and affinity rules for the node pools that should run SeaTunnel pods
- log and metrics collection settings for the provider monitoring stack

## Fork chart: standalone or cluster

The chart in this fork's `deploy/kubernetes/seatunnel` directory adds a `mode`
setting. The chart name remains `seatunnel-helm`; the commands below install
from this checkout rather than the published OCI chart:

```bash
# One hybrid master/worker Pod, accepting repeated job submissions.
helm install seatunnel ./deploy/kubernetes/seatunnel --set mode=standalone

# Separate masters and workers; the fork keeps this as its default (2 + 2).
helm install seatunnel ./deploy/kubernetes/seatunnel --set mode=cluster \
  --set master.replicas=2 --set worker.replicas=2
```

Choose one command. Standalone runs `seatunnel-cluster.sh` without `-r`, using
the default hybrid role with exactly one master Deployment replica and no worker
Deployment. Cluster uses `-r master` and `-r worker`. Configure shared checkpoint
storage through `engineConfig` before relying on recovery across Pods; the
default checkpoint path is temporary local storage. Both modes retain the
Helm chart's Deployment layout.

This fork changes resource names and selectors to isolate SeaTunnel from other
charts in the same release. For release `seatunnel`, the REST Service is
`seatunnel-seatunnel-master`. Migrate an existing quick-start installation to a
new release after stopping or savepointing its jobs; do not upgrade old immutable
Deployment selectors in place. When switching a fork installation between modes,
stop jobs first, set `mode` explicitly, and use `master.replicas=1` for standalone.
Jobs are not migrated or resubmitted automatically. These are chart changes;
the engine and connector APIs are unchanged.

## Submit Job

The following commands use the fork chart installed as release `seatunnel`.
Its REST Service is internal, so forward the Master REST API port first.
```bash
kubectl port-forward -n <namespace> svc/seatunnel-seatunnel-master 8080:8080
```
Use `default` as `<namespace>` if you installed the chart without `-n`.
Then you can access REST API with `http://127.0.0.1:8080/`.

To use Ingress, manage an Ingress separately and point it to this Service.

Or you can just go into master pod, and use local curl command.
```commandline
# Enter one of the master Pods.
kubectl -n <namespace> exec -it deploy/seatunnel-seatunnel-master -- /bin/bash

curl http://127.0.0.1:8080/running-jobs
curl http://127.0.0.1:8080/system-monitoring-information
```

After that, submit jobs through [REST API V2](../../engines/zeta/rest-api-v2.md).

> **Note:** The Helm chart disables the deprecated Zeta REST API v1 on the Hazelcast member port
> (5801) by default (`hazelcast.network.rest-api.enabled: false`), matching the standalone
> `config/hazelcast.yaml`. Use REST API v2 on port 8080, the interface shown above. For the same
> reason metrics are available at `8080/metrics`. If you must use REST API v1, set
> `rest-api.enabled: true` in a custom ConfigMap and restrict port 5801 with a `NetworkPolicy`.
> Generated configuration changes roll the Pods automatically; changes to an externally managed
> ConfigMap require an explicit restart because the files use `subPath` mounts.

## What's More

For now, you have taken a quick look at SeaTunnel. See the connector documentation to find all supported sources and sinks.
For handwritten Kubernetes manifests and production deployment recommendations, see [Separated Cluster Mode](separated-cluster-mode.md) and [Kubernetes Operations](operations.md).
