# HugeGraph 连接器实现任务列表

本文档将 HugeGraph 连接器的设计转换为一系列可执行的编码任务。每个任务都以测试驱动的方式进行，确保增量式进展和早期测试。

## 开发常用命令

### 编译相关

使用 `mvnd` 加速编译：

```bash
# 编译 HugeGraph 连接器及其依赖模块
cd /Users/imbajin/github/seatunnel
mvnd clean install -DskipTests -pl seatunnel-connectors-v2/connector-hugegraph -am

# 仅编译 HugeGraph 连接器（前提是依赖已编译）
cd /Users/imbajin/github/seatunnel/seatunnel-connectors-v2/connector-hugegraph
mvnd clean compile -DskipTests

# 运行单元测试
mvnd test -pl seatunnel-connectors-v2/connector-hugegraph
```
### 代码格式化

```bash
# 应用代码格式化（提交前必须执行）
./mvnw spotless:apply
```

## 1. 项目基础设施搭建

- [x] **1.1 创建 Maven 模块结构**
    - 在 `seatunnel-connectors-v2` 下创建 `connector-hugegraph` 模块
    - 配置 `pom.xml`，添加 HugeGraph Client 1.5.0 依赖
    - 添加 SeaTunnel API 依赖
    - 满足需求 1.1, 1.2（SeaTunnel v2 规范，HugeGraph 1.5 支持）

- [x] **1.2 实现基础配置选项类**
    - 创建 `HugeGraphOptions.java`，定义所有配置选项常量
    - 实现连接配置选项（host, port, graph_name, graph_space, username, password）
    - 实现映射配置选项（mapping_type, vertex_label, edge_label 等）
    - 满足需求 7.1, 7.2（
    - ，连接参数）

- [x] **1.3 编写配置选项单元测试**
    - TODO 实际配置文件测试（常见报错）
    - 测试必需参数验证
    - 测试可选参数默认值
    - 测试条件参数验证
    - 满足需求 10.1（单元测试覆盖率 >80%）

## 2. 工厂类和主体 Sink 实现

- [x] **2.1 实现 HugeGraphSinkFactory**
    - 创建 `HugeGraphSinkFactory.java`，实现 `TableSinkFactory` 接口
    - 添加 `@AutoService(Factory.class)` 注解
    - 实现 `factoryIdentifier()` 返回 "HugeGraph"
    - 实现 `optionRule()` 定义参数规则
    - 满足需求 1.1, 7.4（连接器规范，配置验证）

- [x] **2.2 实现 HugeGraphSink 主类**
    - 创建 `HugeGraphSink.java`，继承 `AbstractSimpleSink`
    - 实现 `SupportMultiTableSink` 接口
    - 实现 `createWriter()` 方法
    - 实现 `getWriteCatalogTable()` 方法
    - 满足需求 1.3, 1.5（数据流处理，多图空间支持）

- [x] **2.3 创建配置管理类**
    - 创建 `HugeGraphSinkConfig.java` 管理所有配置
    - 创建 `MappingConfig.java` 管理映射相关配置
    - 实现配置解析和验证逻辑
    - 满足需求 3.1-3.8（数据映射配置）

## 3. 数据映射器实现

- [x] **3.1 创建映射器接口和基类**
    - 创建 `GraphDataMapper.java` 接口
    - 定义 `map(SeaTunnelRow)` 方法
    - 定义 `extractId(SeaTunnelRow)` 方法
    - 满足需求 3.3, 3.4（ID 映射）

- [x] **3.2 实现顶点映射器**
    - 创建 `VertexMapper.java`
    - 实现单表到单个 VertexLabel 映射
    - 实现单表到多个 VertexLabel 映射
    - 实现 PRIMARY_KEY 和 CUSTOMIZE ID 策略
    - 满足需求 3.1, 3.2, 3.3, 3.4

- [x] **3.3 实现边映射器**
    - 创建 `EdgeMapper.java`
    - 实现单表字段到源/目标顶点映射
    - 实现关联表到源/目标顶点映射
    - 实现边方向配置（"a,b" 表示 a→b）
    - 满足需求 3.5, 3.6, 3.7

- [x] **3.4 编写映射器单元测试**
    - 测试顶点 ID 生成逻辑
    - 测试边的源/目标提取
    - 测试属性映射
    - 满足需求 10.1, 10.3

## 4. HugeGraph 客户端封装

- [x] **4.1 创建 HugeGraph 客户端**
    - 创建 `HugeGraphClient.java`
    - 实现客户端初始化和关闭
    - 满足需求 9.3（资源使用可配置）

- [x] **4.2 实现Client客户端封装**
    - 创建 `HugeGraphClient.java`
    - 实现 `writeVertex()` 方法
    - 实现 `writeEdge()` 方法
    - 实现 `deleteVertex()` 方法
    - 实现 `deleteEdge()` 方法
    - 满足需求 1.3（写入 HugeGraph）

- [x] **4.3 实现级联删除逻辑**
    - 在 `HugeGraphClient` 中实现 `deleteVertexWithEdges()` 方法
    - 先查询并删除所有关联边
    - 再删除顶点本身
    - 满足需求 2.5（级联删除）

## 5. Schema 验证实现

- [x] **5.1 创建 Schema 验证器**
    - 创建 `SchemaValidator.java`
    - 实现 VertexLabel 存在性验证
    - 实现 EdgeLabel 存在性验证
    - 满足需求 4.1, 4.2

- [x] **5.2 实现数据类型兼容性验证**
    - 验证源数据类型与目标 schema 类型兼容
    - 实现详细的错误信息报告
    - 满足需求 4.3, 4.4

- [ ] **5.3 编写 Schema 验证测试**
    - 测试 Label 不存在的情况
    - 测试数据类型不兼容的情况
    - 满足需求 10.1

## 6. Writer 核心实现

- [x] **6.1 实现 HugeGraphSinkWriter 基础框架**
    - 创建 `HugeGraphSinkWriter.java`
    - 实现 `SinkWriter` 接口
    - 初始化客户端和映射器
    - 实现 `close()` 方法确保资源释放
    - 满足需求 1.1, 1.3

- [x] **6.2 实现 CDC 事件处理**
    - 实现 `write(SeaTunnelRow)` 方法
    - 根据 RowKind 分发到不同处理方法
    - 实现 `handleUpsert()` 处理 INSERT/UPDATE_AFTER
    - 实现 `handleDelete()` 处理 DELETE/UPDATE_BEFORE
    - 满足需求 2.1-2.6（CDC 事件处理）

- [x] **6.3 实现批量写入缓冲区**
    - 创建 `BatchBuffer` 类管理缓冲区
    - 实现基于记录数的触发（默认 500 条）
    - 实现基于时间窗口的触发（默认 5 秒）
    - 实现混合触发策略
    - 满足需求 5.1-5.5（批量写入策略）

- [ ] **6.4 编写 Writer 集成测试**
    - 使用 Testcontainers 启动 HugeGraph
    - 测试 INSERT 事件处理
    - 测试 UPDATE 事件处理
    - 测试 DELETE 事件处理
    - 测试级联删除功能
    - 满足需求 10.2, 10.3, 10.4

## 7. 错误处理和重试机制

- [x] **7.1 实现重试机制**
    - 创建 `RetryableOperation` 工具类
    - 实现指数退避重试策略
    - 配置默认重试 3 次
    - 满足需求 6.1, 6.2, 6.3

- [ ] **7.2 实现错误日志记录**
    - 记录失败的元素详细信息
    - 实现死信队列概念（记录无法处理的数据）
    - 满足需求 6.4

- [ ] **7.3 编写错误处理测试**
    - 测试网络异常重试
    - 测试数据格式错误处理
    - 测试超过重试次数的处理
    - 满足需求 10.1

## 8. 状态管理和事务支持

- [ ] **8.1 实现 SinkState 状态管理**
    - 创建 `HugeGraphSinkState.java`
    - 实现状态序列化和反序列化
    - 保存未提交的批次信息
    - 满足需求 6.5（最终一致性）

- [ ] **8.2 实现 SinkAggregatedCommitter**
    - 创建 `HugeGraphSinkCommitter.java`
    - 实现 `commit()` 方法
    - 实现 `abort()` 方法
    - 满足需求 1.1（SeaTunnel 规范）

- [ ] **8.3 编写事务测试**
    - 测试正常提交流程
    - 测试异常回滚流程
    - 满足需求 10.1

## 9. 性能优化实现

- [ ] **9.1 实现环形缓冲区优化**
    - 使用 Disruptor 或类似技术实现环形缓冲区
    - 减少 GC 压力
    - 满足需求 9.1（处理百万级记录）

- [ ] **9.2 实现背压机制**
    - 监控内存使用
    - 实现写入速率控制
    - 防止 OOM
    - 满足需求 9.1

- [ ] **9.3 实现性能指标收集**
    - 记录吞吐量指标
    - 记录延迟指标
    - 记录成功/失败率
    - 满足需求 6.6, 9.4

## 10. 配置示例和文档

- [x] **10.1 创建配置示例文件**
    - 创建简单顶点同步示例配置
    - 创建边同步示例配置
    - 创建 CDC 增量同步示例配置
    - 满足需求 7.5, 8.2, 8.3

- [x] **10.2 编写用户文档**
    - 创建 README.md 文档
    - 说明所有配置选项
    - 提供完整的使用示例
    - 解释关系型到图模型的映射
    - 满足需求 8.1, 8.4, 8.5

- [ ] **10.3 添加内联代码注释**
    - 为所有公共 API 添加 JavaDoc
    - 为复杂逻辑添加解释性注释
    - 满足需求 8.5

## 11. 端到端测试

- [ ] **11.1 创建 E2E 测试环境**
    - 使用 Testcontainers 启动 MySQL
    - 使用 Testcontainers 启动 HugeGraph
    - 配置 SeaTunnel 测试环境
    - 满足需求 10.5

- [ ] **11.2 实现完整数据同步测试**
    - 测试 MySQL → SeaTunnel → HugeGraph 完整流程
    - 验证顶点同步正确性
    - 验证边同步正确性
    - 满足需求 10.5

- [ ] **11.3 实现 CDC 增量同步测试**
    - 启用 MySQL binlog
    - 测试 INSERT 事件同步
    - 测试 UPDATE 事件同步
    - 测试 DELETE 事件同步
    - 验证级联删除
    - 满足需求 10.3, 10.4, 10.5

## 12. 集成和注册

- [x] **12.1 注册连接器到 SeaTunnel**
    - 更新 `plugin-mapping.properties` 文件
    - 添加 HugeGraph 连接器映射
    - 满足需求 1.1

- [x] **12.2 更新项目依赖**
    - 更新 `seatunnel-dist/pom.xml`
    - 添加 HugeGraph 连接器依赖
    - 确保连接器包含在二进制发布包中
    - 满足需求 1.1

- [ ] **12.3 运行完整的集成测试套件**
    - 执行所有单元测试
    - 执行所有集成测试
    - 执行所有 E2E 测试
    - 验证代码覆盖率 >80%
    - 满足需求 10.1, 10.2, 10.5
