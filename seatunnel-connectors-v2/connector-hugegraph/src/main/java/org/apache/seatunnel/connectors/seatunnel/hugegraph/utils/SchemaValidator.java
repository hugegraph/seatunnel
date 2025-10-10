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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.utils;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig.LabelType;

import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.driver.SchemaManager;
import org.apache.hugegraph.structure.constant.DataType;
import org.apache.hugegraph.structure.schema.EdgeLabel;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.apache.hugegraph.structure.schema.VertexLabel;

import java.util.Map;
import java.util.Set;

/** Validates the SeaTunnel schema against the HugeGraph schema. */
public final class SchemaValidator {

    private final HugeGraphSinkConfig sinkConfig;
    private final SeaTunnelRowType rowType;

    private HugeClient client;

    public SchemaValidator(HugeGraphSinkConfig config, SeaTunnelRowType rowType) {
        this.sinkConfig = config;
        this.rowType = rowType;
    }

    public void validateSchema() {
        try {
            this.client = HugeGraphClient.getInstance(sinkConfig);

            SchemaConfig schemaConfig = sinkConfig.getSchemaConfig();
            if (schemaConfig.getType() == LabelType.VERTEX) {
                validateVertex(schemaConfig);
            } else if (schemaConfig.getType() == LabelType.EDGE) {
                validateEdge(schemaConfig);
            } else {
                throw new IllegalArgumentException(
                        "Unsupported schema type: " + schemaConfig.getType());
            }

            HugeGraphClient.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void validateVertex(SchemaConfig schemaConfig) {
        SchemaManager schema = client.schema();
        String label = schemaConfig.getLabel();
        VertexLabel vertexLabel = schema.getVertexLabel(label);
        if (vertexLabel == null) {
            throw new IllegalArgumentException(
                    String.format("Vertex label '%s' does not exist in HugeGraph.", label));
        }
        validateLabelProperties(schema, label, schemaConfig, vertexLabel.properties());
    }

    private void validateEdge(SchemaConfig schemaConfig) {
        SchemaManager schema = client.schema();
        String label = schemaConfig.getLabel();
        EdgeLabel edgeLabel = schema.getEdgeLabel(label);
        if (edgeLabel == null) {
            throw new IllegalArgumentException(
                    String.format("Edge label '%s' does not exist in HugeGraph.", label));
        }
        validateSourceTarget(schemaConfig, edgeLabel);
        validateLabelProperties(schema, label, schemaConfig, edgeLabel.properties());
    }

    private void validateSourceTarget(SchemaConfig schemaConfig, EdgeLabel edgeLabel) {
        String label = schemaConfig.getLabel();
        String schemaSource = edgeLabel.sourceLabel();
        if (!schemaSource.equals(schemaConfig.getSource().getLabel())) {
            throw new IllegalArgumentException(
                    String.format(
                            "EdgeLabel[%s] sourceLabel mismatch: schema=%s, config=%s",
                            label, schemaSource, schemaConfig.getSource()));
        }

        String schemaTarget = edgeLabel.targetLabel();
        if (!schemaTarget.equals(schemaConfig.getTarget().getLabel())) {
            throw new IllegalArgumentException(
                    String.format(
                            "EdgeLabel[%s] targetLabel mismatch: schema=%s, config=%s",
                            label, schemaTarget, schemaConfig.getTarget()));
        }
    }

    /**
     * Validates if the properties from SeaTunnelRowType are compatible with the HugeGraph schema.
     */
    private void validateLabelProperties(
            SchemaManager schema,
            String label,
            SchemaConfig schemaConfig,
            Set<String> hugegraphProperties) {

        Map<String, String> field_mapping = schemaConfig.getMapping().getField_mapping();

        for (int i = 0; i < rowType.getTotalFields(); i++) {
            String sourceFieldName = rowType.getFieldName(i);
            SeaTunnelDataType<?> seaTunnelType = rowType.getFieldType(i);

            // Skip fields that are not in the mapping
            if (!field_mapping.containsKey(sourceFieldName)) {
                continue;
            }

            String targetPropertyName = field_mapping.get(sourceFieldName);

            // 1. Check if the property exists in HugeGraph
            if (!hugegraphProperties.contains(targetPropertyName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Property '%s' for label '%s' is defined in the connector config, but does not exist in the HugeGraph schema.",
                                targetPropertyName, label));
            }

            // 2. Check for data type compatibility
            PropertyKey propertyKey = schema.getPropertyKey(targetPropertyName);
            DataType hugeGraphType = propertyKey.dataType();

            if (!isCompatible(seaTunnelType, hugeGraphType)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Data type mismatch for property '%s' on label '%s'. "
                                        + "SeaTunnel type '%s' is not compatible with HugeGraph type '%s'.",
                                targetPropertyName, label, seaTunnelType, hugeGraphType));
            }
        }
    }

    /** Checks if a SeaTunnelDataType is compatible with a HugeGraph DataType. */
    private boolean isCompatible(SeaTunnelDataType<?> seaTunnelType, DataType hugeGraphType) {
        switch (seaTunnelType.getSqlType()) {
            case BYTES:
                return hugeGraphType == DataType.BLOB;
            case TINYINT:
            case SMALLINT:
            case INT:
                return hugeGraphType == DataType.INT;
            case BIGINT:
                return hugeGraphType == DataType.LONG;
            case FLOAT:
                return hugeGraphType == DataType.FLOAT;
            case DOUBLE:
                return hugeGraphType == DataType.DOUBLE;
            case BOOLEAN:
                return hugeGraphType == DataType.BOOLEAN;
            case DATE:
            case TIMESTAMP:
                return hugeGraphType == DataType.DATE;
            case ARRAY:
                // For arrays, HugeGraph property definitions are for single elements.
                // We check the compatibility of the array's element type.
                SeaTunnelDataType<?> elementType =
                        ((ArrayType<?, ?>) seaTunnelType).getElementType();
                return isCompatible(elementType, hugeGraphType);
            case MAP:
            case DECIMAL: // Decimal is mapped to TEXT to preserve precision
            case ROW:
            case TIME:
            case NULL:
            case STRING:
                return hugeGraphType == DataType.TEXT;
            default:
                // Unsupported types are considered incompatible.
                return false;
        }
    }
}
