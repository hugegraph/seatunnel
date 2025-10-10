package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class MappingConfig implements Serializable {
    private Map<String, String> field_mapping;
    private Map<Object, Object> value_mapping;
    private List<String> null_values;
    private String id;
    private String source;
    private String target;
}
