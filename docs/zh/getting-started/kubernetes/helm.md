---
sidebar_position: 4
---

# 使用 Helm 部署

使用 Helm 快速部署 SeaTunnel 集群。

:::tip 提示
在 Zeta 集群模式下，生产环境推荐 Master 和 Worker 都使用 StatefulSet 部署。当前 Helm Chart 适合快速体验，但 Master 和 Worker 仍会渲染为 Deployment。生产环境使用 Helm 前，建议先阅读 [Kubernetes 部署](kubernetes.mdx)、[分离集群模式](separated-cluster-mode.md) 和 [Kubernetes 配置](configuration.md)，了解基于 StatefulSet 的拓扑、Headless Service、checkpoint、IMap MapStore 与 slot 规划等生产实践。
:::

## 准备

我们假设您的本地已经安装如下软件:

- [docker](https://docs.docker.com/)
- [kubernetes](https://kubernetes.io/)
- [helm](https://helm.sh/docs/intro/quickstart/)

在您的本地环境中能够正常执行 `kubectl` 和 `helm` 命令。
 
以 [minikube](https://minikube.sigs.k8s.io/docs/start/) 为例, 您可以使用如下命令启动一个集群:

```bash
minikube start --kubernetes-version=v1.23.3
```

## 安装

在本 SeaTunnel fork 的仓库根目录执行，使用默认的 master/worker 分离集群配置安装：
```bash
helm install seatunnel ./deploy/kubernetes/seatunnel
```

如果您需要使用其他命名空间进行安装：
```bash
helm install seatunnel ./deploy/kubernetes/seatunnel -n <your-namespace>
```

对于托管 Kubernetes 服务，建议将云厂商相关的差异化配置单独维护在一个 values 文件中，并通过 `-f` 传入，例如：

```bash
helm install seatunnel ./deploy/kubernetes/seatunnel -n <your-namespace> -f values-eks.yaml
```

Apache 发布的 OCI Chart `oci://registry-1.docker.io/apache/seatunnel-helm`
是独立制品，不包含本 fork 的这些改动。release 名为 `seatunnel` 时，原公开 Chart 的
master Service 为 `seatunnel-master`；本文源码 fork 的命令使用 `seatunnel-seatunnel-master`。

在托管集群上常见需要复核的 values 包括：

- 用于 ECR、Artifact Registry、ACR、ACK 容器镜像服务、TCR、SWR、Volcengine Container Registry 或 OpenShift 内置镜像仓库的镜像仓库地址与 `image.pullSecret`
- 目标命名空间对应的 ServiceAccount、RBAC，以及 OpenShift 上的 SecurityContextConstraint 要求
- 云厂商负载均衡、子网、证书与内外网暴露模式相关的 Service 或 Ingress annotation
- 用于 checkpoint 与 state 路径的对象存储或 PersistentVolume 配置
- 用于运行 SeaTunnel Pod 的节点池所需的 resource request/limit、node selector、toleration 与 affinity
- 接入云厂商监控栈的日志与指标采集配置

## Fork Chart：单机与集群

本 fork 的 `deploy/kubernetes/seatunnel` Chart 新增了 `mode` 配置，Chart 名称仍为
`seatunnel-helm`。以下命令从当前源码目录安装，不使用已发布的 OCI Chart：

```bash
# 一个同时承担 master 和 worker 的 Pod，可持续接收作业。
helm install seatunnel ./deploy/kubernetes/seatunnel --set mode=standalone

# 分别部署 master 和 worker；fork 保留此默认拓扑（2 + 2）。
helm install seatunnel ./deploy/kubernetes/seatunnel --set mode=cluster \
  --set master.replicas=2 --set worker.replicas=2
```

选择其中一种方式。`standalone` 使用官方 `seatunnel-cluster.sh -r master_and_worker`，
master Deployment 固定为一个副本，不创建 worker Deployment；`cluster` 使用官方
`master`、`worker` 角色。默认 checkpoint 写入临时本地目录；需要跨 Pod 恢复时，
请通过 `engineConfig` 配置共享持久化存储。两种模式均保留 Helm Chart 的 Deployment 部署方式。

此 fork 调整了资源名称和选择器，使 SeaTunnel 能与同一 release 中的其他 Chart 隔离。
release 名为 `seatunnel` 时，REST Service 为 `seatunnel-seatunnel-master`。已有快速体验
部署应先停止作业或创建 savepoint，再迁移到新的 release；不要直接修改旧 Deployment 的
不可变选择器。fork 内切换模式前也应先停止作业，显式指定 `mode`，切回单机时设置
`master.replicas=1`。作业不会自动迁移或重新提交。这些变化仅涉及 Chart，不改变引擎和连接器 API。

## 提交任务

以下命令使用 fork Chart，假设 release 名为 `seatunnel`。REST Service 默认仅供集群内部访问，
可以先转发 Master 的 REST API 端口：
```bash
kubectl port-forward -n <namespace> svc/seatunnel-seatunnel-master 8080:8080
```
如果安装时没有通过 `-n` 指定命名空间，`<namespace>` 使用 `default`。
然后可以通过地址 `http://127.0.0.1:8080/` 访问 Master 的 REST API。

如需 Ingress，请单独管理 Ingress，并将后端指向此 Service。

或者您可以直接进入 Master Pod 执行 curl 命令。
```commandline
# 进入其中一个 Master Pod
kubectl -n <namespace> exec -it deploy/seatunnel-seatunnel-master -- /bin/bash
# 执行 REST API
curl http://127.0.0.1:8080/running-jobs
curl http://127.0.0.1:8080/system-monitoring-information
```

后面就可以使用 [REST API V2](../../engines/zeta/rest-api-v2.md) 提交任务了。

> **注意：** Helm chart 默认关闭了 Hazelcast 成员端口（5801）上已废弃且无鉴权的 Zeta REST API v1
> （`hazelcast.network.rest-api.enabled: false`），与单机版 `config/hazelcast.yaml` 保持一致。请使用
> 8080 端口上的 REST API v2（即上文所示接口），指标位于 `8080/metrics`。如果确实需要
> REST API v1，请在自定义 ConfigMap 中设置 `rest-api.enabled: true`，并使用 `NetworkPolicy`
> 限制 5801 端口。Chart 生成的配置变化会自动触发 Pod 滚动更新；外部管理的 ConfigMap
> 使用 `subPath` 挂载，需要显式重启 Pod 才会加载新配置。

## 下一步
到现在为止，您已经安装好 SeaTunnel 集群了，可以继续查看连接器文档，了解 SeaTunnel 支持哪些 source 和 sink。
如需手写 Kubernetes manifest 或了解生产部署建议，请查看 [分离集群模式](separated-cluster-mode.md) 和 [Kubernetes 运维](operations.md)。
