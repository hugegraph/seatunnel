package org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig.SourceTargetConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.DataTypeUtil;

import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.graph.Edge;
import org.apache.hugegraph.structure.schema.PropertyKey;
import org.apache.hugegraph.util.E;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class EdgeMapper implements GraphDataMapper {

    private final SchemaConfig schemaConfig;
    private final MappingConfig mappingConfig;
    private final Map<String, Integer> fieldsIndex;
    private final HugeClient client;

    public EdgeMapper(SchemaConfig schemaConfig, SeaTunnelRowType rowType, HugeClient client) {
        this.schemaConfig = schemaConfig;
        this.client = client;
        this.mappingConfig =
                schemaConfig.getMapping() == null ? new MappingConfig() : schemaConfig.getMapping();
        this.fieldsIndex =
                IntStream.range(0, rowType.getTotalFields())
                        .boxed()
                        .collect(Collectors.toMap(rowType::getFieldName, i -> i));
    }

    @Override
    public Edge map(SeaTunnelRow row) {
        // 1. Build source and target vertex IDs
        String sourceId = buildVertexId(row, schemaConfig.getSource());
        String targetId = buildVertexId(row, schemaConfig.getTarget());

        // If source or target ID can't be built, we can't create the edge
        if (sourceId == null || targetId == null) {
            return null;
        }

        // 2. Create edge and set identifiers
        Edge edge = new Edge(schemaConfig.getLabel());
        edge.sourceId(sourceId);
        edge.targetId(targetId);
        edge.sourceLabel(schemaConfig.getSource().getLabel());
        edge.targetLabel(schemaConfig.getTarget().getLabel());

        // 3. Set properties
        Set<String> idFields = new HashSet<>();
        idFields.addAll(schemaConfig.getSource().getIdFields());
        idFields.addAll(schemaConfig.getTarget().getIdFields());

        Map<String, String> fieldMapping = mappingConfig.getField_mapping();
        if (fieldMapping == null) {
            fieldMapping = Collections.emptyMap();
        }

        for (Map.Entry<String, Integer> fieldEntry : fieldsIndex.entrySet()) {
            String sourceFieldName = fieldEntry.getKey();

            // Skip fields used for source/target vertex IDs
            if (idFields.contains(sourceFieldName)) {
                continue;
            }

            String targetPropertyName = fieldMapping.getOrDefault(sourceFieldName, sourceFieldName);
            PropertyKey propertykey = client.schema().getPropertyKey(targetPropertyName);
            Object fieldValue =
                    DataTypeUtil.convert(row.getField(fieldEntry.getValue()), propertykey);

            if (isConsideredNull(fieldValue)) {
                continue;
            }

            fieldValue = getMappedValue(String.valueOf(fieldValue));
            edge.property(targetPropertyName, fieldValue);
        }
        return edge;
    }

    private String buildVertexId(SeaTunnelRow row, SourceTargetConfig config) {
        E.checkArgument(
                config != null, "Source/Target vertex config must be specified for edge mapping.");
        List<String> idFields = config.getIdFields();
        E.checkArgument(
                idFields != null && !idFields.isEmpty(),
                "The 'idFields' must be specified for source/target vertex in edge mapping.");

        List<Object> idValues = getFieldValues(row, idFields);
        if (idValues.stream().anyMatch(this::isConsideredNull)) {
            return null;
        }
        return spliceVertexId(config.getLabel(), idValues.toArray());
    }

    private List<Object> getFieldValues(SeaTunnelRow row, List<String> fieldNames) {
        return fieldNames.stream()
                .map(
                        name -> {
                            Integer index = fieldsIndex.get(name);
                            E.checkArgument(
                                    index != null,
                                    "Cannot find ID field '%s' in SeaTunnelRowType.",
                                    name);
                            return row.getField(index);
                        })
                .collect(Collectors.toList());
    }

    private boolean isConsideredNull(Object value) {
        if (value == null) {
            return true;
        }
        List<String> nullValues = mappingConfig.getNull_values();
        if (nullValues == null || nullValues.isEmpty()) {
            return false;
        }
        return nullValues.contains(String.valueOf(value));
    }

    private Object getMappedValue(String originalValue) {
        Map<Object, Object> valueMapping = mappingConfig.getValue_mapping();
        if (valueMapping == null || valueMapping.isEmpty()) {
            return originalValue;
        }
        return valueMapping.getOrDefault(originalValue, originalValue);
    }

    private String spliceVertexId(String label, Object... primaryValues) {
        StringBuilder vertexId = new StringBuilder();
        vertexId.append(label).append(":");
        for (int i = 0; i < primaryValues.length; i++) {
            vertexId.append(primaryValues[i]);
            if (i < primaryValues.length - 1) {
                vertexId.append("!");
            }
        }
        return vertexId.toString();
    }

    @Override
    public Object extractId(SeaTunnelRow row) {
        throw new UnsupportedOperationException("extractId is not supported for edge mapping.");
    }
}
