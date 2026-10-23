# SeaTunnel Helm Chart 的维护归属

Chart 源码由 [HugeGraph SeaTunnel fork](https://github.com/hugegraph/Seatunnel) 的
`deploy/kubernetes/seatunnel` 目录统一维护。HugeGraph 将打包后的 Chart 作为可选依赖，
不再保留第二份模板源码。

初始版本来自 Apache SeaTunnel 的
[`deploy/kubernetes/seatunnel`](https://github.com/apache/seatunnel/tree/14a27ac26a0a71b56722f86cdf8ec2779094ceaf/deploy/kubernetes/seatunnel)，
固定提交为 `14a27ac26a0a71b56722f86cdf8ec2779094ceaf`，对应 Chart `3.0.1`、应用 `3.0.0`。
保留上游 Apache License 和 NOTICE。fork Chart 版本为 `3.0.1-hugegraph.1`，
默认镜像为 `apache/seatunnel:3.0.0`。

fork 中的调整：

- 保留上游分离的 master/worker Deployment，增加单节点混合角色及其所需的
  `hazelcast.yaml`、`jvm_options`。单机运行官方脚本时不传 `-r`，使用其默认混合角色；
  集群分别传 `-r master` 和 `-r worker`。单机引擎备份数为 0，集群为 1。
- 为 SeaTunnel 设置独立的资源名称、发现 DNS 和稳定选择器；发现 Service 发布未就绪节点，
  master API 使用 ClusterIP Service，并允许自定义集群 DNS 后缀。
- 合并重复角色模板，增加探针、配置校验和及退出宽限时间。通过小型 Bash `java` 函数，
  让上游脚本最终启动 Java 时使用 `exec`，保留原有 JVM 参数处理，同时将 Kubernetes
  终止信号直接交给 JVM。
- 支持合并原生引擎配置、完整配置文件覆盖、已有 ConfigMap、外部卷、环境变量、
  调度与资源/安全配置、镜像摘要和全局镜像仓库。
- 移除未使用的 ConfigMap API RBAC 和 Ingress。DNS 发现不需要 API 凭据，
  因此关闭 service-account token 挂载。

fork 保留上游默认的 `mode: cluster`，即两个 master 和两个 worker；
`mode: standalone` 启动一个混合引擎 Pod。资源名称和选择器与上游快速体验 Chart 不同，
已有上游部署应迁移到新 release，不应原地修改不可变选择器。切换 `mode` 会替换 master
进程，可能中断运行中的作业。上游 checkpoint 默认使用临时本地目录；生产使用前需配置
共享持久化存储并验证恢复。

更新时对比固定提交与新的上游目录，保留这些适配，更新提交和版本，并验证渲染、作业、
进程退出及打包。更新默认应用版本前，应重新确认上游前台脚本最终通过 `java` 启动服务。
Chart 不包含 Java 依赖或连接器二进制文件。
