# HugeGraph Sink Connector

`Sink: HugeGraph`

## Description

The HugeGraph sink connector allows you to write data from SeaTunnel to Apache HugeGraph, a fast and scalable graph database.

This connector supports writing data as vertices or edges, providing flexible mapping from relational data models to graph structures. It is designed for high-performance data loading and can handle CDC (Change Data Capture) events like inserts, updates, and deletes.

## Features

- **Batch Writing**: Data is written in batches for high throughput.
- **CDC Handling**: Processes `INSERT`, `UPDATE`, and `DELETE` events.
- **Flexible Mapping**: Supports flexible mapping of source fields to vertex/edge properties.
- **Vertex and Edge Writing**: Can write data as either vertices or edges.
- **Automatic Schema Creation**: Can automatically create graph schema elements (property keys, vertex labels, edge labels) if they do not exist.

## Configuration Options

| Name | Type | Required | Default Value | Description |
| --- | --- | --- | --- | --- |
| `host` | String | Yes | - | The host of the HugeGraph server. |
| `port` | Integer | Yes | - | The port of the HugeGraph server. |
| `graph` | String | Yes | - | The name of the graph to write to. |
| `username` | String | No | - | The username for HugeGraph authentication. |
| `password` | String | No | - | The password for HugeGraph authentication. |
| `schema` | List<Object> | Yes | - | A list of schema mapping configurations. Each object defines a mapping to a vertex or an edge. |
| `batch_size` | Integer | No | 500 | The number of records to buffer before writing to HugeGraph in a single batch. |
| `batch_interval_ms` | Integer | No | 5000 | The maximum time in milliseconds to wait before flushing a batch, even if `batch_size` is not reached. |
| `max_retries` | Integer | No | 3 | The maximum number of times to retry a failed write operation. |
| `retry_backoff_ms` | Integer | No | 1000 | The initial backoff time in milliseconds for retries. |

### Schema Configuration (`schema_config`)

Each object in the `schema` list defines a mapping from the source data to a specific vertex or edge label in HugeGraph.

| Name | Type | Required | Default Value | Description |
| --- | --- | --- | --- | --- |
| `type` | String | Yes | - | The type of graph element to map to. Can be `VERTEX` or `EDGE`. |
| `label` | String | Yes | - | The label of the vertex or edge in HugeGraph. |
| `idStrategy` | String | For Vertex | - | The ID generation strategy for vertices. Supported values: `PRIMARY_key`, `CUSTOMIZE_STRING`, `CUSTOMIZE_NUMBER`, `CUSTOMIZE_UUID`. |
| `idFields` | List<String> | For Vertex | - | A list of source field names used to generate the vertex ID, required for all `idStrategy` except `AUTOMATIC`. |
| `source` | Object | For Edge | - | An object defining the mapping for the edge's source vertex. See `Source/Target Config` below. |
| `target` | Object | For Edge | - | An object defining the mapping for the edge's target vertex. See `Source/Target Config` below. |
| `mapping` | Object | No | - | An object defining advanced field and value mappings. See `Mapping Config` below. |

### Source/Target Config (`source` and `target`)

This object is used within an `EDGE` schema to define how to identify the source and target vertices.

| Name | Type | Required | Default Value | Description                                                                                                                                              |
| --- | --- | --- | --- |----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `label` | String | Yes | - | The label of the source or target vertex.                                                                                                                |
| `idFields` | List<String> | Yes | - | A list of property names from the input row used to construct the ID of the source/target vertex. The values will be concatenated to form the vertex ID. |

### Mapping Config (`mapping`)

This object provides advanced control over how fields and values are mapped to properties.

| Name              | Type | Required | Default Value | Description                                                                                                                                                                       |
|-------------------| --- |----------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `fieldMapping`    | Map<String, String> | No       | -        | A map where the key is the source field name and the value is the target property name in HugeGraph. If not specified, the source field name is used as the target property name. |
| `valueMapping`    | Map<String, String> | No       | -        | A map to transform specific field values. The key is the original value from the source, and the value is the new value to be written.                                            |
| `nullValues`      | List<String> | No       | -        | A list of string values that should be treated as `null`. Any field containing one of these values will not be written.                                                           |
| `sourceIdMapping` | Map<String, String> | For Edge | -        | A map where the key is the source field name and the value is the target property name in HugeGraph. Only include source vertex idFields                                          |
| `targetIdMapping` | Map<String, String> | For Edge | -        | A map where the key is the source field name and the value is the target property name in HugeGraph. Only include target vertex idFields                                          |

## Relational to Graph Mapping Explained

The connector maps flat, relational data (rows and columns) from a source into a graph structure (vertices and edges).

### Vertex Mapping

A single row from the source data is mapped to a single vertex in HugeGraph.
- **Label**: The vertex label is determined by the `label` option in the schema config.
- **ID**: The vertex ID is constructed based on the `idStrategy` and `idFields`. For example, with `idStrategy: PRIMARY_KEY` and `idFields: ["user_id"]`, the value from the `user_id` column will be used to create the vertex ID.
- **Properties**: Each column in the source row (that is not an ID field) becomes a property of the vertex. You can rename fields using `field_mapping`.

### Edge Mapping

A single row from the source data is mapped to a single edge in HugeGraph.
- **Label**: The edge label is determined by the `label` option.
- **Source & Target Vertices**: The edge must link two vertices. The connector identifies the source and target vertices using the `source` and `target` configuration objects. For each, you specify the vertex label and the fields from the input row that contain its ID values.
- **Properties**: Columns from the source row that are not used to identify the source or target vertices become properties of the edge.

## Usage Examples

### 1. Writing Vertices

This example shows how to read from a `FakeSource` and write `person` vertices to HugeGraph. The vertex ID is based on the `name` field.

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
        field_mapping = {
          name = "name"
          age = "age"
        }
      }
    }
  }
}
```

### 2. Writing Edges

This example syncs a relationship table to `knows` edges in HugeGraph. The source table contains the names of the two people who know each other and the year they met.

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
    graph = "hugegraph"
    schema = [
      {
        type = "EDGE"
        label = "knows"
        source = {
          label = "person"
          idFields = ["person1_name"]
        }
        target = {
          label = "person"
          idFields = ["person2_name"]
        }
        mapping = {
          field_mapping = {
            since = "date"
          }
        }
      }
    ]
  }
}
```
