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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.buffer;

import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;

import org.apache.hugegraph.structure.GraphElement;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.graph.Vertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** 一个用于向 HugeGraph 批量写入数据的缓冲区。 它支持按批次大小和时间间隔两种策略触发写入操作。 这个类是线程安全的。 */
public class BatchBuffer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BatchBuffer.class);

    private final List<GraphElement> buffer = new ArrayList<>();
    private final int batchSize;
    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> scheduledFuture;

    private volatile boolean closed = false;
    private volatile Exception flushException;
    private final HugeGraphClient client;

    /**
     * 构造函数
     *
     * @param client 用于写入 HugeGraph 的客户端实例
     * @param batchSize 每个批次的最大大小。当缓冲区中的元素数量达到此值时，将触发一次写入。
     * @param batchIntervalMs 批处理的最大时间间隔（毫秒）。如果设置为大于 0 的值， 会有一个后台线程定期检查并刷新缓冲区。
     */
    public BatchBuffer(HugeGraphClient client, int batchSize, long batchIntervalMs) {

        this.batchSize = batchSize;
        this.client = client;

        if (batchIntervalMs > 0) {
            this.scheduler =
                    Executors.newSingleThreadScheduledExecutor(
                            runnable -> {
                                Thread thread = new Thread(runnable, "hugegraph-sink-flusher");
                                thread.setDaemon(true);
                                return thread;
                            });
            this.scheduledFuture =
                    this.scheduler.scheduleAtFixedRate(
                            () -> {
                                try {
                                    // 定期 flush，即使缓冲区未满
                                    flush();
                                } catch (Exception e) {
                                    // 记录后台线程中的异常，以便主线程可以感知到
                                    flushException = e;
                                }
                            },
                            batchIntervalMs,
                            batchIntervalMs,
                            TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.scheduledFuture = null;
        }
    }

    /**
     * 将图元素添加到缓冲区。如果缓冲区大小达到阈值，则触发 flush。
     *
     * @param element 要添加的图元素
     * @throws IOException 如果后台刷新任务失败或本次刷新失败
     */
    public synchronized void add(GraphElement element) throws IOException {
        checkFlushException();
        if (closed) {
            throw new IOException("BatchBuffer is already closed.");
        }

        try {
            buffer.add(element);
            if (buffer.size() >= batchSize) {
                // 在持有锁的情况下调用内部 flush 方法
                doFlush();
            }
        } catch (Exception e) {
            throw new IOException("Failed to add element and flush", e);
        }
    }

    /**
     * 将缓冲区中的数据提交到 HugeGraph。
     *
     * @throws IOException 如果写入 HugeGraph 失败
     */
    public synchronized void flush() throws IOException {
        checkFlushException();
        if (closed && buffer.isEmpty()) {
            return;
        }
        doFlush();
    }

    /**
     * 执行实际的刷新操作，此方法假定调用者已经持有了锁。
     *
     * @throws IOException 如果写入操作失败
     */
    private void doFlush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        try {
            GraphElement firstElement = buffer.get(0);
            if (firstElement instanceof Vertex) {
                List<Vertex> vertices =
                        buffer.stream()
                                .map(element -> (Vertex) element)
                                .collect(Collectors.toList());
                client.batchWriteVertices(vertices);
            } else {
                List<Edge> edges =
                        buffer.stream().map(element -> (Edge) element).collect(Collectors.toList());
                client.batchWriteEdges(edges);
            }
            // 写入成功后清空缓冲区
            buffer.clear();
        } catch (Exception e) {
            LOG.error("Failed to write batch data to HugeGraph", e);
            throw new IOException("Failed to write batch data to HugeGraph", e);
        }
    }

    /**
     * 关闭 BatchBuffer，停止定时任务并执行最后一次 flush，以确保所有缓冲数据都被写入。
     *
     * @throws IOException 如果最后的 flush 操作失败
     */
    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }

        // 停止定时调度
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        // 在关闭前确保所有缓冲的数据都被提交
        LOG.info("Closing BatchBuffer, performing final flush...");
        flush();
        checkFlushException();
        LOG.info("BatchBuffer closed.");
    }

    /** 检查后台线程中是否有异常发生，并在主线程中重新抛出。 */
    private void checkFlushException() throws IOException {
        if (flushException != null) {
            throw new IOException("An error occurred during asynchronous flush", flushException);
        }
    }
}
