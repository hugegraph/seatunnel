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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class HugeGraphSinkConfig implements Serializable {

    private String host;
    private int port;
    private String graphName;
    private String graphSpace;
    private String username;
    private String password;
    private SchemaConfig schemaConfig;
    private int batchSize;
    private int batchIntervalMs;
    private int maxRetries;
    private int retryBackoffMs;

    // mapping config
    private Map<String, String> propertyMapping;
    private List<String> selectedFields;
    private List<String> ignoredFields;

    public static HugeGraphSinkConfig of(ReadonlyConfig config) {
        HugeGraphSinkConfig sinkConfig = new HugeGraphSinkConfig();

        sinkConfig.setHost(config.get(HugeGraphOptions.HOST));
        sinkConfig.setPort(config.get(HugeGraphOptions.PORT));
        sinkConfig.setGraphName(config.get(HugeGraphOptions.GRAPH_NAME));
        sinkConfig.setBatchSize(config.get(HugeGraphOptions.BATCH_SIZE));
        sinkConfig.setBatchIntervalMs(config.get(HugeGraphOptions.BATCH_INTERVAL_MS));
        sinkConfig.setMaxRetries(config.get(HugeGraphOptions.MAX_RETRIES));
        sinkConfig.setRetryBackoffMs(config.get(HugeGraphOptions.RETRY_BACKOFF_MS));
        sinkConfig.setPropertyMapping(config.get(HugeGraphOptions.PROPERTY_MAPPING));
        sinkConfig.setSchemaConfig(config.get(HugeGraphOptions.SCHEMA_CONFIG));

        config.getOptional(HugeGraphOptions.SELECTED_FIELDS)
                .ifPresent(sinkConfig::setSelectedFields);
        config.getOptional(HugeGraphOptions.IGNORED_FIELDS).ifPresent(sinkConfig::setIgnoredFields);

        config.getOptional(HugeGraphOptions.GRAPH_SPACE).ifPresent(sinkConfig::setGraphSpace);
        config.getOptional(HugeGraphOptions.USERNAME).ifPresent(sinkConfig::setUsername);
        config.getOptional(HugeGraphOptions.PASSWORD).ifPresent(sinkConfig::setPassword);

        return sinkConfig;
    }
}
