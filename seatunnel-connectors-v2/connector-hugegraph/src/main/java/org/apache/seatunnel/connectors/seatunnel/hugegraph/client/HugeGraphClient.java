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
import org.apache.hugegraph.exception.ServerException;
import org.apache.hugegraph.rest.ClientException;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class HugeGraphClient {

    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphClient.class);

    // 【修改】使用 volatile 保证多线程环境下的可见性
    private static volatile HugeClient instance;

    // 【新增】用于保存配置，以便在需要时重建客户端
    private static HugeGraphSinkConfig sinkConfig;

    /** 私有构造函数，防止外部实例化 */
    private HugeGraphClient() {}

    /**
     * 获取 HugeClient 单例实例。 此版本修改为线程安全的懒汉式实现。
     *
     * @param config HugeGraph 的配置对象，在首次创建或重建实例时使用。
     * @return HugeClient 的单例实例。
     */
    public static HugeClient getInstance(HugeGraphSinkConfig config) {
        // 使用双重检查锁定（DCL）来确保线程安全和性能
        if (instance == null) {
            synchronized (HugeGraphClient.class) {
                if (instance == null) {
                    LOG.info("HugeClient instance not found, creating a new one (thread-safe)...");
                    // 【修改】保存配置
                    sinkConfig = config;
                    instance = createClient(config);
                }
            }
        }
        return instance;
    }

    /**
     * 【新增】内部获取客户端的方法，包含检查和按需创建的逻辑 这是所有数据操作方法的入口点
     *
     * @return 可用的 HugeClient 实例
     * @throws IllegalStateException 如果客户端未初始化且无法重建
     */
    private static HugeClient getClient() {
        if (instance == null) {
            LOG.warn("HugeClient instance is null. Attempting to re-initialize...");
            if (sinkConfig == null) {
                // 如果连配置都没有，说明从未成功初始化过，无法重建
                throw new IllegalStateException(
                        "HugeGraphClient has not been initialized. "
                                + "Cannot perform write/delete operations. Please call getInstance(config) first.");
            }
            // 使用保存的配置重建实例
            instance = createClient(sinkConfig);
        }
        return instance;
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

    // ===================================================================================
    //  【重点修改区域】所有写入和删除函数都通过新的 getClient() 方法获取实例
    // ===================================================================================

    public static void writeVertex(Vertex vertex) {
        try {
            getClient().graph().addVertex(vertex);
        } catch (ServerException e) {
            // 处理服务器端错误,如schema验证失败、约束冲突等
            System.err.println("服务器端插入失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (ClientException e) {
            // 处理客户端通信错误,如连接断开、超时等
            System.err.println("客户端通信失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (Exception e) {
            // 处理其他未预期的异常
            System.err.println("未知错误: " + e.getMessage());
            throw e;
        }
    }

    public static void writeEdge(Edge edge) {
        try {
            getClient().graph().addEdge(edge);
        } catch (ServerException e) {
            // 处理服务器端错误,如schema验证失败、约束冲突等
            System.err.println("服务器端插入失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (ClientException e) {
            // 处理客户端通信错误,如连接断开、超时等
            System.err.println("客户端通信失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (Exception e) {
            // 处理其他未预期的异常
            System.err.println("未知错误: " + e.getMessage());
            throw e;
        }
    }

    public static void deleteVertex(Object vertexId) {
        try {
            getClient().graph().removeVertex(vertexId);
        } catch (ServerException e) {
            // 处理服务器端错误,如schema验证失败、约束冲突等
            System.err.println("服务器端插入失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (ClientException e) {
            // 处理客户端通信错误,如连接断开、超时等
            System.err.println("客户端通信失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (Exception e) {
            // 处理其他未预期的异常
            System.err.println("未知错误: " + e.getMessage());
            throw e;
        }
    }

    public static void deleteEdge(Edge edge) {
        try {
            getClient().graph().removeEdge(edge.id());
        } catch (ServerException e) {
            // 处理服务器端错误,如schema验证失败、约束冲突等
            System.err.println("服务器端插入失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (ClientException e) {
            // 处理客户端通信错误,如连接断开、超时等
            System.err.println("客户端通信失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (Exception e) {
            // 处理其他未预期的异常
            System.err.println("未知错误: " + e.getMessage());
            throw e;
        }
    }

    public static void deleteVertexWithEdges(Object vertexId) {
        try {
            HugeClient client = getClient();
            List<Edge> edges = client.graph().getEdges(vertexId);
            for (Edge edge : edges) {
                client.graph().removeEdge(edge.id());
            }
            client.graph().removeVertex(vertexId);
        } catch (ServerException e) {
            // 处理服务器端错误,如schema验证失败、约束冲突等
            System.err.println("服务器端插入失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (ClientException e) {
            // 处理客户端通信错误,如连接断开、超时等
            System.err.println("客户端通信失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (Exception e) {
            // 处理其他未预期的异常
            System.err.println("未知错误: " + e.getMessage());
            throw e;
        }
    }

    public static void batchWriteVertices(List<Vertex> buffer) {
        try {
            HugeClient client = getClient();
            client.graph().addVertices(buffer);
        } catch (ServerException e) {
            // 处理服务器端错误,如schema验证失败、约束冲突等
            System.err.println("服务器端插入失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (ClientException e) {
            // 处理客户端通信错误,如连接断开、超时等
            System.err.println("客户端通信失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (Exception e) {
            // 处理其他未预期的异常
            System.err.println("未知错误: " + e.getMessage());
            throw e;
        }
    }

    public static void batchWriteEdges(List<Edge> buffer) {
        try {
            HugeClient client = getClient();
            client.graph().addEdges(buffer);
        } catch (ServerException e) {
            // 处理服务器端错误,如schema验证失败、约束冲突等
            System.err.println("服务器端插入失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (ClientException e) {
            // 处理客户端通信错误,如连接断开、超时等
            System.err.println("客户端通信失败: " + e.getMessage());
            throw e; // 或者根据业务需求处理
        } catch (Exception e) {
            // 处理其他未预期的异常
            System.err.println("未知错误: " + e.getMessage());
            throw e;
        }
    }

    /** 关闭单例客户端连接。 【修改】同样需要同步以保证线程安全 */
    public static synchronized void close() {
        if (instance != null) {
            try {
                LOG.info("Closing HugeClient singleton instance.");
                instance.close();
            } catch (Exception e) {
                LOG.error("Error closing HugeClient instance.", e);
            } finally {
                instance = null;
            }
        }
    }
}
