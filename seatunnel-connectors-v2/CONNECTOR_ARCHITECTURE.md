# SeaTunnel Connector 架构设计分析

## 一、核心数据结构设计

### 1. SeaTunnelRow - 统一数据模型

#### 数据结构组成
```java
public final class SeaTunnelRow implements Serializable {
    // 核心字段
    private String tableId = "";           // 表标识符（多表支持）
    private RowKind rowKind = RowKind.INSERT;  // 行变更类型（CDC支持）
    private final Object[] fields;         // 实际数据数组
    private Map<String, Object> options;   // 扩展选项
    private volatile int size;             // 缓存的字节大小
}
```

#### RowKind 枚举（CDC支持）
- `INSERT (+I)`: 插入操作
- `UPDATE_BEFORE (-U)`: 更新前的旧数据
- `UPDATE_AFTER (+U)`: 更新后的新数据  
- `DELETE (-D)`: 删除操作

#### 关键特性
1. **轻量级设计**: 基于 Object[] 数组，避免复杂的对象嵌套
2. **多表支持**: 通过 tableId 区分不同表的数据
3. **CDC原生支持**: RowKind 内置变更类型
4. **字节大小计算**: 支持内存管理和批量控制
5. **灵活扩展**: options Map 支持自定义元数据

### 2. SeaTunnelRowType - Schema定义

```java
public class SeaTunnelRowType implements CompositeType<SeaTunnelRow> {
    private final String[] fieldNames;      // 字段名称
    private final SeaTunnelDataType<?>[] fieldTypes;  // 字段类型
}
```

支持的数据类型：
- 基础类型：BOOLEAN, BYTE, SHORT, INT, BIGINT, FLOAT, DOUBLE, STRING
- 时间类型：DATE, TIME, TIMESTAMP, TIMESTAMP_TZ
- 复杂类型：ARRAY, MAP, ROW（嵌套）
- 特殊类型：DECIMAL, BYTES, NULL

## 二、Connector 核心接口设计

### Source 端架构

```
SeaTunnelSource<T, SplitT, StateT>
    ├── createEnumerator()     // 创建分片枚举器
    ├── createReader()         // 创建数据读取器
    ├── restoreEnumerator()    // 从检查点恢复
    ├── getBoundedness()       // 有界/无界流
    └── getProducedCatalogTables() // 表元数据
```

### Sink 端架构

```
SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT>
    ├── createWriter()         // 创建写入器
    ├── createCommitter()      // 创建提交器（事务）
    ├── getSaveModeHandler()   // 保存模式处理
    └── getWriteCatalogTable() // 写入表信息
```

## 三、实现新的 KV Connector 关键点

### 1. 核心实现要点

#### Source 实现
```java
public class MyKVSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext context) {
        return new MyKVSourceReader(parameters, context, deserializationSchema);
    }
}
```

#### Sink 实现
```java
public class MyKVSink extends AbstractSimpleSink<SeaTunnelRow, Void> {
    @Override
    public MyKVSinkWriter createWriter(SinkWriter.Context context) {
        return new MyKVSinkWriter(seaTunnelRowType, parameters);
    }
}
```

### 2. 关键设计决策

#### A. Key 生成策略
```java
// 1. 固定 Key
String key = "fixed_key";

// 2. 字段值作为 Key
String key = row.getField(keyFieldIndex).toString();

// 3. 模板化 Key（支持占位符）
String keyTemplate = "user:{id}:profile:{type}";
String key = generateKey(keyTemplate, row, fieldNames);
```

#### B. 值序列化策略
```java
// JSON 序列化（默认）
SerializationSchema schema = new JsonSerializationSchema(rowType);
byte[] value = schema.serialize(row);

// 自定义字段映射
String value = row.getField(valueFieldIndex).toString();

// Hash 类型（多字段）
Map<String, String> hashValue = new HashMap<>();
hashValue.put(hashKeyField, row.getField(hashKeyIndex).toString());
hashValue.put(hashValueField, row.getField(hashValueIndex).toString());
```

#### C. 批量操作优化
```java
public class MyKVSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final List<String> keyBuffer;
    private final List<String> valueBuffer;
    private final int batchSize;
    
    @Override
    public void write(SeaTunnelRow element) {
        keyBuffer.add(generateKey(element));
        valueBuffer.add(generateValue(element));
        
        if (keyBuffer.size() >= batchSize) {
            flush();
        }
    }
    
    private void flush() {
        // 批量写入实现
        client.batchWrite(keyBuffer, valueBuffer);
        clearBuffers();
    }
}
```

### 3. 主要难点及解决方案

#### 难点1：数据类型映射
**问题**：结构化数据 → KV 存储的转换
**解决方案**：
```java
// 1. 扁平化嵌套结构
private Map<String, String> flattenRow(SeaTunnelRow row, SeaTunnelRowType type) {
    Map<String, String> flat = new HashMap<>();
    for (int i = 0; i < type.getTotalFields(); i++) {
        String key = type.getFieldName(i);
        Object value = row.getField(i);
        if (value instanceof SeaTunnelRow) {
            // 递归处理嵌套
            flat.putAll(flattenRow((SeaTunnelRow) value, ...));
        } else {
            flat.put(key, convertToString(value));
        }
    }
    return flat;
}
```

#### 难点2：事务一致性
**问题**：KV 数据库通常不支持事务
**解决方案**：
```java
// 1. 批量原子操作
public void commit() {
    // 使用 Pipeline/Batch 确保批量原子性
    try (Pipeline pipeline = client.pipelined()) {
        for (int i = 0; i < keys.size(); i++) {
            pipeline.set(keys.get(i), values.get(i));
        }
        pipeline.sync();
    }
}

// 2. 两阶段提交（如果支持）
public class MyKVSinkCommitter implements SinkCommitter<CommitInfo> {
    @Override
    public List<CommitInfo> commit(List<CommitInfo> commitInfos) {
        // 实现提交逻辑
    }
}
```

#### 难点3：Schema 演进
**问题**：KV 存储通常无 Schema
**解决方案**：
```java
// 1. 版本化 Key
String versionedKey = key + ":v" + schemaVersion;

// 2. 元数据存储
client.set(key + ":schema", JsonUtils.toJsonString(schema));

// 3. 兼容性读取
public SeaTunnelRow read(String key) {
    String schemaJson = client.get(key + ":schema");
    Schema schema = parseSchema(schemaJson);
    String value = client.get(key);
    return deserialize(value, schema);
}
```

#### 难点4：性能优化
**问题**：网络 RTT 开销
**解决方案**：
```java
// 1. 连接池管理
private final GenericObjectPool<Connection> connectionPool;

// 2. 异步写入
CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
    client.write(key, value);
});

// 3. 批量预聚合
Map<String, List<String>> groupedData = data.stream()
    .collect(Collectors.groupingBy(this::generateKey));
```

### 4. Factory 模式实现

```java
@AutoService(Factory.class)
public class MyKVSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "MyKV";
    }
    
    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new MyKVSink(context.getOptions(), context.getCatalogTable());
    }
    
    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
            .required(HOST, PORT, KEY_FIELD)
            .optional(BATCH_SIZE, EXPIRE_TIME, VALUE_FORMAT)
            .conditional(MODE, CLUSTER, NODES)
            .build();
    }
}
```

## 四、最佳实践建议

### 1. 配置设计
```hocon
sink {
  MyKV {
    # 连接配置
    host = "localhost"
    port = 6379
    auth = "password"
    
    # Key 配置
    key = "data:{id}:{date}"  # 支持占位符
    support_custom_key = true
    
    # 值配置
    data_type = "hash"  # string/hash/list/set/zset
    value_field = "content"
    format = "json"
    
    # 性能配置
    batch_size = 100
    connection_pool_size = 10
    timeout_ms = 5000
    
    # 可靠性配置
    max_retries = 3
    expire_seconds = 3600
  }
}
```

### 2. 错误处理
```java
public class MyKVSinkWriter {
    private void writeWithRetry(String key, String value) {
        int attempts = 0;
        Exception lastException = null;
        
        while (attempts < maxRetries) {
            try {
                client.write(key, value);
                return;
            } catch (Exception e) {
                lastException = e;
                attempts++;
                if (attempts < maxRetries) {
                    Thread.sleep(getBackoffTime(attempts));
                }
            }
        }
        
        handleWriteFailure(key, value, lastException);
    }
}
```

### 3. 监控指标
```java
public class MyKVSinkWriter {
    // 关键指标
    private final Counter writtenRecords;
    private final Counter failedRecords;
    private final Histogram batchSize;
    private final Timer writeLatency;
    
    private void recordMetrics() {
        writtenRecords.inc(batch.size());
        batchSize.update(batch.size());
        try (Timer.Context ignored = writeLatency.time()) {
            flush();
        }
    }
}
```

## 五、参考实现

### 已有 KV Connector 特点

| Connector | Key策略 | 值格式 | 批量支持 | 事务支持 | 特色功能 |
|-----------|---------|--------|----------|----------|----------|
| Redis | 模板化/自定义 | JSON/String | Pipeline | 批量原子 | 多数据类型 |
| Cassandra | 主键映射 | Row → CQL | Batch Statement | 批量CQL | Schema管理 |
| DynamoDB | 分区键+排序键 | Item | BatchWrite | 条件写入 | 自动重试 |
| MongoDB | _id 字段 | Document | BulkWrite | 事务(4.0+) | 嵌套文档 |

## 六、总结

实现新的 KV Connector 需要重点关注：

1. **数据模型转换**：SeaTunnelRow → KV 的映射策略
2. **Key 生成机制**：灵活且高效的 Key 设计
3. **批量操作**：减少网络开销，提高吞吐量
4. **错误恢复**：重试、容错、一致性保证
5. **性能优化**：连接池、异步、预聚合
6. **配置友好**：直观的参数设计

通过遵循 SeaTunnel 的设计模式和接口规范，可以快速实现高质量的 KV Connector。
