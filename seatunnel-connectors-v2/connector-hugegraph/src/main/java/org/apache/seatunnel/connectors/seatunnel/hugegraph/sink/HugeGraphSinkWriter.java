/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hugegraph.sink;

import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig.LabelType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.EdgeMapper;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.GraphDataMapper;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.VertexMapper;

import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.GraphElement;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.schema.VertexLabel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class HugeGraphSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphSinkWriter.class);

    private final HugeGraphSinkConfig sinkConfig;
    private final GraphDataMapper mapper;
    private final boolean batchWrite = false;

    // TODO: 优先实现batch提交，按时间提交后续放到 BatchBuffer 类实现
    private final ArrayList<GraphElement> buffer;

    public HugeGraphSinkWriter(HugeGraphSinkConfig sinkConfig, SeaTunnelRowType rowType) {
        this.sinkConfig = sinkConfig;
        SchemaConfig schemaConfig = sinkConfig.getSchemaConfig();
        HugeClient client = HugeGraphClient.getInstance(sinkConfig);

        if (schemaConfig.getType() == LabelType.VERTEX) {
            VertexLabel vertexLabel =
                    client.schema().getVertexLabel(sinkConfig.getSchemaConfig().getLabel());
            String labelId = String.valueOf(vertexLabel.id());
            this.mapper = new VertexMapper(schemaConfig, rowType, client, labelId);
        } else {
            this.mapper = new EdgeMapper(schemaConfig, rowType, client);
        }
        this.buffer = new ArrayList<>();
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                handleUpsert(row);
                break;
            case DELETE:
            case UPDATE_BEFORE:
                handleDelete(row);
                break;
            default:
                LOG.warn("Unsupported row kind: {}", row.getRowKind());
                break;
        }
    }

    private void handleUpsert(SeaTunnelRow row) throws IOException {
        try {
            flush();
            if (batchWrite) {
                GraphElement element = mapper.map(row);
                buffer.add(element);
            } else {
                if (sinkConfig.getSchemaConfig().getType() == LabelType.VERTEX) {
                    Vertex vertex = (Vertex) mapper.map(row);
                    HugeGraphClient.writeVertex(vertex);
                } else {
                    Edge edge = (Edge) mapper.map(row);
                    HugeGraphClient.writeEdge(edge);
                }
            }

        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void handleDelete(SeaTunnelRow row) throws IOException {
        try {
            flush();
            if (sinkConfig.getSchemaConfig().getType() == LabelType.VERTEX) {
                Object vertexId = mapper.extractId(row);
                HugeGraphClient.deleteVertexWithEdges(vertexId);
            } else {
                Edge edge = (Edge) mapper.map(row);
                HugeGraphClient.deleteEdge(edge);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        flush();
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        flush();
        HugeGraphClient.close();
    }

    public synchronized void flush() {
        if (!buffer.isEmpty()) {
            if (sinkConfig.getSchemaConfig().getType() == LabelType.VERTEX) {
                List<Vertex> vertices =
                        buffer.stream()
                                .map(element -> (Vertex) element) // 2. 对流中的每个元素应用一个函数（乘以2）
                                .collect(Collectors.toList());
                HugeGraphClient.batchWriteVertices(vertices);
            } else {
                List<Edge> edges =
                        buffer.stream()
                                .map(element -> (Edge) element) // 2. 对流中的每个元素应用一个函数（乘以2）
                                .collect(Collectors.toList());
                HugeGraphClient.batchWriteEdges(edges);
            }
            buffer.clear();
        }
    }
}
