package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;
import java.util.Map;

public class HugeGraphSinkOptions {
    public static final Option<Map<String, String>> PROPERTY_MAPPING =
            Options.key("property_mapping")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Property Mapping");

    public static final Option<List<String>> SELECTED_FIELDS =
            Options.key("selected_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Selected Fields");

    public static final Option<List<String>> IGNORED_FIELDS =
            Options.key("ignored_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Ignored Fields");

    public static final Option<SchemaConfig> SCHEMA_CONFIG =
            Options.key("schema_config")
                    .objectType(SchemaConfig.class)
                    .noDefaultValue()
                    .withDescription(
                            "A list of mapping config objects. Each object describes a mapping to a vertex or edge.");
}
