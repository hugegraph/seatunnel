/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hugegraph.client;

import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;

import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.exception.ServerException;
import org.apache.hugegraph.rest.ClientException;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.apache.hugegraph.structure.schema.VertexLabel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;

import java.io.IOException;
import java.util.List;

public final class HugeGraphClient {

    // TODO: Add handling for schema fetch failures.
    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphClient.class);

    private final HugeClient client;
    @Getter private final SchemaManager schema;

    public HugeGraphClient(HugeGraphSinkConfig config) {
        this.client = createClient(config);
        this.schema = client.schema();
    }

    private static HugeClient createClient(HugeGraphSinkConfig config) {
        int maxRetries = config.getMaxRetries() > 0 ? config.getMaxRetries() : 3;
        long retryIntervalMillis =
                config.getRetryBackoffMs() > 0 ? config.getRetryBackoffMs() : 5000L;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                String url = String.format("http://%s:%d", config.getHost(), config.getPort());
                LOG.debug(
                        "Creating new HugeClient for url: {}, graph: {}",
                        url,
                        config.getGraphName());

                HugeClient client =
                        HugeClient.builder(url, config.getGraphName())
                                .configUser(config.getUsername(), config.getPassword())
                                .configIdleTime(60)
                                .build();

                client.graph().listVertices();
                LOG.info(
                        "Successfully created and validated HugeClient instance on attempt {}/{}.",
                        attempt,
                        maxRetries);
                return client;
            } catch (Exception e) {
                LOG.error(
                        "Failed to create HugeClient on attempt {}/{}. Error: {}",
                        attempt,
                        maxRetries,
                        e.getMessage());
                if (attempt < maxRetries) {
                    try {
                        LOG.info("Will retry in {} ms...", retryIntervalMillis);
                        Thread.sleep(retryIntervalMillis);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(
                                "Client creation was interrupted during retry wait.", ie);
                    }
                }
            }
        }
        throw new IllegalStateException(
                "Failed to create HugeClient after " + maxRetries + " attempts.");
    }

    public PropertyKey getPropertyKey(String propertyName) {
        return schema.getPropertyKey(propertyName);
    }

    public String getVertexLabel(String label) {
        VertexLabel vertexLabel = this.client.schema().getVertexLabel(label);
        return String.valueOf(vertexLabel.id());
    }

    public String getEdgeLabel(String label) {
        EdgeLabel edgeLabel = this.client.schema().getEdgeLabel(label);
        return String.valueOf(edgeLabel.id());
    }

    public IdStrategy getIdStrategy(String label) {
        VertexLabel vertexLabel = this.client.schema().getVertexLabel(label);
        return vertexLabel.idStrategy();
    }

    public void writeVertex(Vertex vertex) throws IOException {
        try {
            this.client.graph().addVertex(vertex);
        } catch (ServerException | ClientException e) {
            LOG.error(
                    "Failed to write vertex (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Failed to write vertex, triggering task restart", e);
        } catch (Exception e) {
            LOG.error(
                    "Unknown error writing vertex (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Unknown error writing vertex", e);
        }
    }

    public void writeEdge(Edge edge) throws IOException {
        try {
            this.client.graph().addEdge(edge);
        } catch (ServerException | ClientException e) {
            LOG.error(
                    "Failed to write edge (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Failed to write edge, triggering task restart", e);
        } catch (Exception e) {
            LOG.error(
                    "Unknown error writing edge (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Unknown error writing edge", e);
        }
    }

    public void deleteVertex(Object vertexId) throws IOException {
        try {
            this.client.graph().removeVertex(vertexId);
        } catch (ServerException | ClientException e) {
            LOG.error(
                    "Failed to delete vertex (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Failed to delete vertex, triggering task restart", e);
        } catch (Exception e) {
            LOG.error(
                    "Unknown error deleting vertex (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Unknown error deleting vertex", e);
        }
    }

    public void deleteEdge(String edgeId) throws IOException {
        try {
            this.client.graph().removeEdge(edgeId);
        } catch (ServerException | ClientException e) {
            LOG.error(
                    "Failed to delete edge (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Failed to delete edge, triggering task restart", e);
        } catch (Exception e) {
            LOG.error(
                    "Unknown error deleting edge (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Unknown error deleting edge", e);
        }
    }

    public void deleteVertexWithEdges(Object vertexId) throws IOException {
        try {
            List<Edge> edges = this.client.graph().getEdges(vertexId);
            for (Edge edge : edges) {
                this.client.graph().removeEdge(edge.id());
            }
            this.client.graph().removeVertex(vertexId);
        } catch (ServerException | ClientException e) {
            LOG.error(
                    "Failed to delete vertex (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Failed to delete vertex, triggering task restart", e);
        } catch (Exception e) {
            LOG.error(
                    "Unknown error deleting vertex (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Unknown error deleting vertex", e);
        }
    }

    public void batchWriteVertices(List<Vertex> buffer) throws IOException {
        try {
            this.client.graph().addVertices(buffer);
        } catch (ServerException | ClientException e) {
            LOG.error(
                    "Failed to batch write vertex (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Failed to batch write vertex, triggering task restart", e);
        } catch (Exception e) {
            LOG.error(
                    "Unknown error batch writing vertex (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Unknown error batch writing vertex", e);
        }
    }

    public void batchWriteEdges(List<Edge> buffer) throws IOException {
        try {
            this.client.graph().addEdges(buffer);
        } catch (ServerException | ClientException e) {
            LOG.error(
                    "Failed to batch write edge (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Failed to batch write edge, triggering task restart", e);
        } catch (Exception e) {
            LOG.error(
                    "Unknown error batch writing edge (will trigger task restart). Error: {}",
                    e.getMessage(),
                    e);
            throw new IOException("Unknown error batch writing edge", e);
        }
    }

    public void close() {
        if (this.client != null) {
            LOG.info("Closing HugeClient instance.");
            this.client.close();
        }
    }
}
