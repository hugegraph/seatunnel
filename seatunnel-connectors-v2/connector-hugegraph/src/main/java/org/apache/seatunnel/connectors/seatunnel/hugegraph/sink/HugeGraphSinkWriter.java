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
import org.apache.seatunnel.connectors.seatunnel.hugegraph.buffer.BatchBuffer;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig.LabelType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.EdgeMapper;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.GraphDataMapper;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper.VertexMapper;

import org.apache.hugegraph.structure.GraphElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

public class HugeGraphSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphSinkWriter.class);

    private final HugeGraphSinkConfig sinkConfig;
    private final GraphDataMapper mapper;
    private final HugeGraphClient client;

    private final BatchBuffer buffer;

    public HugeGraphSinkWriter(HugeGraphSinkConfig sinkConfig, SeaTunnelRowType rowType) {
        this.sinkConfig = sinkConfig;
        this.client = new HugeGraphClient(sinkConfig);
        this.mapper = getMapper(rowType);
        this.buffer =
                new BatchBuffer(
                        this.client, sinkConfig.getBatchSize(), sinkConfig.getBatchIntervalMs());
    }

    private GraphDataMapper getMapper(SeaTunnelRowType rowType) {
        SchemaConfig schemaConfig = sinkConfig.getSchemaConfig();
        if (schemaConfig.getType() == LabelType.VERTEX) {
            return new VertexMapper(schemaConfig, rowType, client);
        } else {
            return new EdgeMapper(schemaConfig, rowType, client);
        }
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
            GraphElement element = mapper.map(row);
            buffer.add(element);
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new IOException(e);
        }
    }

    private void handleDelete(SeaTunnelRow row) throws IOException {
        try {
            buffer.flush();
            // TODO: Consider batch delete?
            if (sinkConfig.getSchemaConfig().getType() == LabelType.VERTEX) {
                Object vertexId = mapper.extractId(row);
                client.deleteVertexWithEdges(vertexId);
            } else {
                String edgeId = (String) mapper.extractId(row);
                client.deleteEdge(edgeId);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Optional<Void> prepareCommit() {
        try {
            buffer.flush();
        } catch (IOException e) {
            LOG.error("Failed to flush data during prepareCommit, failing checkpoint.", e);
            throw new RuntimeException("Failed to flush data during prepareCommit()", e);
        }
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            buffer.close();
        }

        if (client != null) {
            client.close();
        }
    }
}
