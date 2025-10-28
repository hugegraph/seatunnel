# HugeGraph Sink 连接器

`Sink: HugeGraph`

## 描述

HugeGraph sink 连接器允许您将数据从 SeaTunnel 写入 Apache HugeGraph，一个快速且可扩展的图数据库。

该连接器支持将数据作为顶点或边写入，提供了从关系数据模型到图结构的灵活映射。它专为高性能数据加载而设计，可以处理 CDC（变更数据捕获）事件，如插入、更新和删除。

## 特性

- **批量写入**: 数据分批写入以实现高吞吐量。
- **CDC 处理**: 处理 `INSERT`、`UPDATE` 和 `DELETE` 事件。
- **灵活映射**: 支持将源字段灵活映射到顶点/边属性。
- **顶点和边写入**: 可以将数据作为顶点或边写入。
- **自动创建模式**: 如果图模式元素（属性键、顶点标签、边标签）不存在，可以自动创建它们。

## 配置选项

| 名称 | 类型 | 是否必须 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| `host` | String | 是 | - | HugeGraph 服务器的主机。 |
| `port` | Integer | 是 | - | HugeGraph 服务器的端口。 |
| `graph` | String | 是 | - | 要写入的图的名称。 |
| `username` | String | 否 | - | HugeGraph 身份验证的用户名。 |
| `password` | String | 否 | - | HugeGraph 身份验证的密码。 |
| `schema` | List<Object> | 是 | - | 模式映射配置列表。每个对象定义到顶点或边的映射。 |
| `batch_size` | Integer | 否 | 500 | 在单批次写入 HugeGraph 之前缓冲的记录数。 |
| `batch_interval_ms` | Integer | 否 | 5000 | 在刷新批次之前等待的最大时间（毫秒），即使未达到 `batch_size`。 |
| `max_retries` | Integer | 否 | 3 | 重试失败写入操作的最大次数。 |
| `retry_backoff_ms` | Integer | 否 | 1000 | 重试的初始退避时间（毫秒）。 |

### Schema 配置 (`schema_config`)

`schema` 列表中的每个对象都定义了从源数据到 HugeGraph 中特定顶点或边标签的映射。

| 名称 | 类型 | 是否必须 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| `type` | String | 是 | - | 要映射到的图元素的类型。可以是 `VERTEX` 或 `EDGE`。 |
| `label` | String | 是 | - | HugeGraph 中顶点或边的标签。 |
| `idStrategy` | String | 顶点必须 | - | 顶点的 ID 生成策略。支持的值：`PRIMARY_key`、`CUSTOMIZE_STRING`、`CUSTOMIZE_NUMBER`、`CUSTOMIZE_UUID`。 |
| `idFields` | List<String> | 顶点必须 | - | 用于生成顶点 ID 的源字段名称列表，除了 `AUTOMATIC` 之外的所有 `idStrategy` 都需要。 |
| `source` | Object | 边必须 | - | 定义边的源顶点映射的对象。请参阅下面的 `源/目标配置`。 |
| `target` | Object | 边必须 | - | 定义边的目标顶点映射的对象。请参阅下面的 `源/目标配置`。 |
| `mapping` | Object | 否 | - | 定义高级字段和值映射的对象。请参阅下面的 `映射配置`。 |

### 源/目标配置 (`source` 和 `target`)

此对象在 `EDGE` 模式中使用，以定义如何识别源顶点和目标顶点。

| 名称 | 类型 | 是否必须 | 默认值 | 描述 |
| --- | --- | --- | --- |----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `label` | String | 是 | - | 源或目标顶点的标签。 |
| `idFields` | List<String> | 是 | - | 用于构造源/目标顶点 ID 的输入行中的属性名称列表。这些值将被连接起来形成顶点 ID。 |

### 映射配置 (`mapping`)

此对象提供了对字段和值如何映射到属性的高级控制。

| 名称                | 类型 | 是否必须 | 默认值 | 描述                                                       |
|-------------------| --- |----------|----------|----------------------------------------------------------|
| `fieldMapping`    | Map<String, String> | 否 | - | 一个映射，其中键是源字段名，值是 HugeGraph 中的目标属性名。如果未指定，则使用源字段名作为目标属性名。 |
| `valueMapping`    | Map<String, String> | 否 | - | 用于转换特定字段值的映射。键是源的原始值，值是要写入的新值。                           |
| `nullValues`      | List<String> | 否 | - | 应被视为空值的字符串值列表。任何包含这些值的字段都不会被写入。                          |
| `sourceIdMapping` | Map<String, String> | 边必须 | - | 一个映射，其中键是源字段名，值是 HugeGraph 中的目标属性名。只包括边元素起始顶点id的相关列。     |
| `targetIdMapping` | Map<String, String> | 边必须 | - | 一个映射，其中键是源字段名，值是 HugeGraph 中的目标属性名。只包括边元素指向顶点id的相关列。     |

## 关系到图的映射解释

连接器将来自源的扁平关系数据（行和列）映射到图结构（顶点和边）。

### 顶点映射

源数据中的单行映射到 HugeGraph 中的单个顶点。
- **标签**: 顶点标签由模式配置中的 `label` 选项确定。
- **ID**: 顶点 ID 根据 `idStrategy` 和 `idFields` 构建。例如，使用 `idStrategy: PRIMARY_KEY` 和 `idFields: ["user_id"]`，`user_id` 列的值将用于创建顶点 ID。
- **属性**: 源行中不是 ID 字段的每一列都成为顶点的一个属性。您可以使用 `field_mapping` 重命名字段。

### 边映射

源数据中的单行映射到 HugeGraph 中的单个边。
- **标签**: 边标签由 `label` 选项确定。
- **源和目标顶点**: 边必须连接两个顶点。连接器使用 `source` 和 `target` 配置对象来识别源顶点和目标顶点。对于每个顶点，您需要指定顶点标签和输入行中包含其 ID 值的字段。
- **属性**: 源行中未用于识别源或目标顶点的列成为边的属性。

## 使用示例

### 1. 写入顶点

此示例展示了如何从 `FakeSource` 读取数据并将 `person` 顶点写入 HugeGraph。顶点 ID 基于 `name` 字段。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields = {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    selected_fields = ["name", "age"]
    property_mapping = {
      "name":"name"
      "age":"age"
    }
    schema_config = {
      type = "VERTEX"
      label = "person"
      idStrategy = PRIMARY_KEY
      idFields = ["name"]
      mapping = {
        fieldMapping = {
          name = "name"
          age = "age"
        }
      }
    }
  }
}
```

### 2. 写入边

此示例将一个关系表同步为 HugeGraph 中的 `knows` 边。源表包含两个相互认识的人的姓名以及他们相识的年份。

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields = {
        person1_name = "string"
        person2_name = "string"
        since = "int"
      }
    }
  }
}

sink {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    selected_fields = ["src_name", "tgt_name", "duration"]
    property_mapping = {
      "src_name":"name"
      "tgt_name":"name"
      "duration":"duration"
    }
    schema_config = {
      type = "EDGE"
      label = "knows"
      sourceConfig = {
        label:"person"
        idFields:["name"]
      }
      targetConfig = {
        label:"person"
        idFields:["name"]
      }
      mapping = {
        fieldMapping = {
          duration:"duration"
        }
        sourceMapping = {
          src_name:"name"
        }
        targetMapping = {
          tgt_name:"name"
        }
      }
    }
  }
}
```