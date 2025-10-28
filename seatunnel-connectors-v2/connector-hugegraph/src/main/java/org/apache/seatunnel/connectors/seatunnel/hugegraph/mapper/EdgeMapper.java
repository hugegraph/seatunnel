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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig.SourceTargetConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.DataTypeUtil;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.E;

import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.schema.PropertyKey;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class EdgeMapper implements GraphDataMapper {

    private final SchemaConfig schemaConfig;
    private final MappingConfig mappingConfig;
    private final Map<String, Integer> fieldsIndex;
    private final HugeGraphClient client;
    private final Object labelId;

    public EdgeMapper(SchemaConfig schemaConfig, SeaTunnelRowType rowType, HugeGraphClient client) {
        this.schemaConfig = schemaConfig;
        this.mappingConfig = getMappingConfig();
        this.client = client;
        this.labelId = client.getEdgeLabel(schemaConfig.getLabel());
        this.fieldsIndex =
                IntStream.range(0, rowType.getTotalFields())
                        .boxed()
                        .collect(Collectors.toMap(rowType::getFieldName, i -> i));
    }

    private MappingConfig getMappingConfig() {
        MappingConfig mapping =
                schemaConfig.getMapping() == null ? new MappingConfig() : schemaConfig.getMapping();
        if (mapping.getFieldMapping() == null) {
            mapping.setFieldMapping(Collections.emptyMap());
        }
        if (mapping.getValueMapping() == null) {
            mapping.setValueMapping(Collections.emptyMap());
        }
        if (mapping.getSourceIdMapping() == null) {
            mapping.setSourceIdMapping(Collections.emptyMap());
        }
        if (mapping.getTargetIdMapping() == null) {
            mapping.setTargetIdMapping(Collections.emptyMap());
        }
        schemaConfig.setMapping(mapping);
        return mapping;
    }

    @Override
    public Edge map(SeaTunnelRow row) {
        // 1. Build source and target vertex IDs
        Object sourceId =
                buildVertexId(
                        row, schemaConfig.getSourceConfig(), mappingConfig.getSourceIdMapping());
        Object targetId =
                buildVertexId(
                        row, schemaConfig.getTargetConfig(), mappingConfig.getTargetIdMapping());

        // If source or target ID can't be built, we can't create the edge
        if (sourceId == null || targetId == null) {
            return null;
        }

        // 2. Create edge and set identifiers
        Edge edge = new Edge(schemaConfig.getLabel());
        edge.sourceId(sourceId);
        edge.targetId(targetId);
        edge.sourceLabel(schemaConfig.getSourceConfig().getLabel());
        edge.targetLabel(schemaConfig.getTargetConfig().getLabel());

        // 3. Set properties
        Set<String> idFields = new HashSet<>();
        idFields.addAll(schemaConfig.getSourceConfig().getIdFields());
        idFields.addAll(schemaConfig.getTargetConfig().getIdFields());

        Map<String, String> allFieldMapping = new HashMap<>(mappingConfig.getFieldMapping());
        allFieldMapping.putAll(mappingConfig.getSourceIdMapping());
        allFieldMapping.putAll(mappingConfig.getTargetIdMapping());

        for (Map.Entry<String, Integer> fieldEntry : fieldsIndex.entrySet()) {
            String FieldName = fieldEntry.getKey();
            String PropertyName = allFieldMapping.getOrDefault(FieldName, FieldName);

            // Skip fields used for source/target vertex IDs
            if (idFields.contains(PropertyName)) {
                continue;
            }

            PropertyKey propertyKey = client.getPropertyKey(PropertyName);
            Object fieldValue =
                    DataTypeUtil.convert(
                            row.getField(fieldEntry.getValue()),
                            propertyKey,
                            mappingConfig.getDateFormat(),
                            mappingConfig.getTimeZone());

            if (isConsideredNull(fieldValue)) {
                continue;
            }

            edge.property(PropertyName, getMappedValue(fieldValue));
        }
        return edge;
    }

    private Object buildVertexId(
            SeaTunnelRow row, SourceTargetConfig config, Map<String, String> mapping) {

        String LabelId = client.getVertexLabel(config.getLabel());
        IdStrategy strategy = client.getIdStrategy(config.getLabel());
        if (strategy == null || strategy == IdStrategy.AUTOMATIC) {
            return null;
        }

        List<String> idFields = config.getIdFields();
        switch (strategy) {
            case PRIMARY_KEY:
                List<Object> pkValues = getFieldValues(row, idFields, mapping);
                if (pkValues.stream().anyMatch(this::isConsideredNull)) {
                    return null;
                }
                return spliceVertexId(LabelId, pkValues);
            case CUSTOMIZE_STRING:
                List<Object> stringValues = getFieldValues(row, idFields, mapping);
                if (stringValues.stream().anyMatch(this::isConsideredNull)) {
                    return null;
                }
                return stringValues.stream().map(String::valueOf).collect(Collectors.joining(":"));
            case CUSTOMIZE_NUMBER:
                E.checkArgument(
                        idFields.size() == 1,
                        "CUSTOMIZE_NUMBER strategy requires exactly one ID field.");
                Object numValue = getFieldValues(row, idFields, mapping).get(0);
                if (isConsideredNull(numValue)) {
                    return null;
                }
                if (numValue instanceof Number) {
                    return ((Number) numValue).longValue();
                } else {
                    return Long.parseLong(String.valueOf(numValue));
                }
            case CUSTOMIZE_UUID:
                E.checkArgument(
                        idFields.size() == 1,
                        "CUSTOMIZE_UUID strategy requires exactly one ID field.");
                Object uuidValue = getFieldValues(row, idFields, mapping).get(0);
                if (isConsideredNull(uuidValue)) {
                    return null;
                }
                return UUID.fromString(String.valueOf(uuidValue));
            default:
                throw new UnsupportedOperationException("Unsupported IdStrategy: " + strategy);
        }
    }

    private List<Object> getFieldValues(
            SeaTunnelRow row, List<String> fields, Map<String, String> mapping) {
        List<Object> values = new ArrayList<>(fields.size());

        for (Map.Entry<String, Integer> fieldEntry : fieldsIndex.entrySet()) {
            String FieldName = fieldEntry.getKey();
            String PropertyName = mapping.getOrDefault(FieldName, FieldName);

            if (!fields.contains(PropertyName)) {
                continue;
            }

            PropertyKey propertykey = client.getPropertyKey(PropertyName);
            Object fieldValue =
                    DataTypeUtil.convert(
                            row.getField(fieldEntry.getValue()),
                            propertykey,
                            mappingConfig.getDateFormat(),
                            mappingConfig.getTimeZone());
            values.add(getMappedValue(fieldValue));
        }
        return values;
    }

    private boolean isConsideredNull(Object value) {
        if (value == null) {
            return true;
        }
        List<String> nullValues = mappingConfig.getNullValues();
        if (nullValues == null || nullValues.isEmpty()) {
            return false;
        }
        return nullValues.contains(String.valueOf(value));
    }

    private Object getMappedValue(Object originalValue) {
        Map<Object, Object> valueMapping = mappingConfig.getValueMapping();
        if (valueMapping.isEmpty()) {
            return originalValue;
        }
        return valueMapping.getOrDefault(String.valueOf(originalValue), originalValue);
    }

    private String spliceVertexId(String labelId, List<Object> primaryValues) {
        String joinedValues =
                primaryValues.stream().map(Object::toString).collect(Collectors.joining("!"));
        return String.format("%s:%s", labelId, joinedValues);
    }

    private String getSortedKeyValues(SeaTunnelRow row) {
        List<String> sortedKeys = schemaConfig.getSortKeys();
        if (sortedKeys == null || sortedKeys.isEmpty()) {
            return (String) labelId;
        }
        List<Object> skValues = getFieldValues(row, sortedKeys, mappingConfig.getFieldMapping());
        return skValues.stream().map(Object::toString).collect(Collectors.joining(","));
    }

    @Override
    public Object extractId(SeaTunnelRow row) {
        Object sourceId =
                buildVertexId(
                        row, schemaConfig.getSourceConfig(), mappingConfig.getSourceIdMapping());
        Object targetId =
                buildVertexId(
                        row, schemaConfig.getTargetConfig(), mappingConfig.getTargetIdMapping());
        String sortedKeyValues = getSortedKeyValues(row);

        System.out.println(sourceId);
        System.out.println(targetId);
        return String.format("S%s>%s>%s>>S%s", sourceId, labelId, sortedKeyValues, targetId);
    }
}
