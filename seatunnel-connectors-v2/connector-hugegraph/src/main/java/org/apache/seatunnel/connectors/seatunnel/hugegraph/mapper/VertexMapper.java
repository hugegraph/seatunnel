package org.apache.seatunnel.connectors.seatunnel.hugegraph.mapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.DataTypeUtil;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.E;

import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Vertex;
import org.apache.hugegraph.structure.schema.PropertyKey;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VertexMapper implements GraphDataMapper {

    private final SchemaConfig schemaConfig;
    private final MappingConfig mappingConfig;
    private final Map<String, Integer> fieldsIndex;
    private final String labelId;
    private final HugeGraphClient client;

    public VertexMapper(
            SchemaConfig schemaConfig, SeaTunnelRowType rowType, HugeGraphClient client) {
        this.schemaConfig = schemaConfig;
        this.mappingConfig =
                schemaConfig.getMapping() == null ? new MappingConfig() : schemaConfig.getMapping();
        this.fieldsIndex =
                IntStream.range(0, rowType.getTotalFields())
                        .boxed()
                        .collect(Collectors.toMap(rowType::getFieldName, i -> i));
        this.client = client;
        this.labelId = client.getVertexLabel(schemaConfig.getLabel());
    }

    @Override
    public Vertex map(SeaTunnelRow row) {
        String label = schemaConfig.getLabel();
        E.checkArgument(label != null && !label.isEmpty(), "Vertex label can't be null or empty.");
        Vertex vertex = new Vertex(label);

        // 1. Set vertex ID

        Object id = extractId(row, label);
        if (id == null && schemaConfig.getIdStrategy() != IdStrategy.AUTOMATIC) {
            // If ID is null and it's not automatic, we can't create the vertex.
            // This might happen if a PK field value is in null_values.
            return null;
        }
        if (id != null && schemaConfig.getIdStrategy() != IdStrategy.PRIMARY_KEY) {
            vertex.id(id);
        }

        // 2. Set properties
        Map<String, String> fieldMapping = mappingConfig.getField_mapping();
        if (fieldMapping == null) {
            fieldMapping = Collections.emptyMap();
        }
        Map<Object, Object> valueMapping = mappingConfig.getValue_mapping();
        if (valueMapping == null) {
            valueMapping = Collections.emptyMap();
        }
        List<String> idFields = schemaConfig.getIdFields();
        if (idFields == null) {
            idFields = Collections.emptyList();
        }

        for (Map.Entry<String, Integer> fieldEntry : fieldsIndex.entrySet()) {
            String sourceFieldName = fieldEntry.getKey();

            // Skip ID fields, they are not properties unless explicitly mapped
            if (idFields.contains(sourceFieldName)
                    && schemaConfig.getIdStrategy() != IdStrategy.PRIMARY_KEY) {
                continue;
            }

            String targetPropertyName = fieldMapping.getOrDefault(sourceFieldName, sourceFieldName);
            PropertyKey propertykey = client.getPropertyKey(targetPropertyName);
            Object fieldValue =
                    DataTypeUtil.convert(row.getField(fieldEntry.getValue()), propertykey);

            if (isConsideredNull(fieldValue)) {
                continue;
            }

            fieldValue = getMappedValue(fieldValue);

            vertex.property(targetPropertyName, fieldValue);
        }
        return vertex;
    }

    @Override
    public Object extractId(SeaTunnelRow row) {
        // This method from the interface might not be needed if all logic is in map()
        // For now, delegate to the internal method
        return extractId(row, schemaConfig.getLabel());
    }

    private Object extractId(SeaTunnelRow row, String label) {
        IdStrategy strategy = schemaConfig.getIdStrategy();
        if (strategy == null || strategy == IdStrategy.AUTOMATIC) {
            return null;
        }

        List<String> idFields = schemaConfig.getIdFields();
        E.checkArgument(
                idFields != null && !idFields.isEmpty(),
                "The 'idFields' must be specified for ID strategy '%s'.",
                strategy);

        switch (strategy) {
            case PRIMARY_KEY:
                List<Object> pkValues = getFieldValues(row, idFields);
                if (pkValues.stream().anyMatch(this::isConsideredNull)) {
                    return null;
                }
                return spliceVertexId(pkValues);
            case CUSTOMIZE_STRING:
                List<Object> stringValues = getFieldValues(row, idFields);
                if (stringValues.stream().anyMatch(this::isConsideredNull)) {
                    return null;
                }
                return stringValues.stream().map(String::valueOf).collect(Collectors.joining(":"));
            case CUSTOMIZE_NUMBER:
                E.checkArgument(
                        idFields.size() == 1,
                        "CUSTOMIZE_NUMBER strategy requires exactly one ID field.");
                Object numValue = getFieldValues(row, idFields).get(0);
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
                Object uuidValue = getFieldValues(row, idFields).get(0);
                if (isConsideredNull(uuidValue)) {
                    return null;
                }
                return UUID.fromString(String.valueOf(uuidValue));
            default:
                throw new UnsupportedOperationException("Unsupported IdStrategy: " + strategy);
        }
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

    private Object getMappedValue(Object originalValue) {
        Map<Object, Object> valueMapping = mappingConfig.getValue_mapping();
        if (valueMapping == null || valueMapping.isEmpty()) {
            return originalValue;
        }
        return valueMapping.getOrDefault(originalValue, originalValue);
    }

    // Simplified from hugegraph-loader's ElementBuilder.
    // It should use label id instead of name, here is a compromise.
    private String spliceVertexId(List<Object> primaryValues) {
        // 1. 使用 Stream API 將 List<Object> 中的所有元素转换为 String，使用 "!" 连接
        String joinedValues =
                primaryValues.stream()
                        .map(Object::toString) // 將每個元素轉換為字串
                        .collect(Collectors.joining("!")); // 用 "!" 作为分隔符连接

        // 2. 使用 String.format() 將 label 和拼接好的字串組合起來
        return String.format("%s:%s", labelId, joinedValues);
    }
}
