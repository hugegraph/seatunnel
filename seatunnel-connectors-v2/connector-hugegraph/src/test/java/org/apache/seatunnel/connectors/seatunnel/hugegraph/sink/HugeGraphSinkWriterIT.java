package org.apache.seatunnel.connectors.seatunnel.hugegraph.sink;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig;

import org.apache.hugegraph.driver.GremlinManager;
import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.constant.IdStrategy;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Testcontainers
public class HugeGraphSinkWriterIT {

    private static final String HUGE_GRAPH_IMAGE = "hugegraph/hugegraph:latest";
    private static final String GRAPH_NAME = "hugegraph";
    private static final String VERTEX_LABEL_PERSON = "person_for_test";
    private static final SeaTunnelRowType SEATUNNEL_ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"name", "age"},
                    new SeaTunnelDataType<?>[] {
                        org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                        org.apache.seatunnel.api.table.type.BasicType.INT_TYPE
                    });

    private static HugeClient hugeClient;
    private static GremlinManager gremlinManager;

    @Container
    private static final GenericContainer<?> HUGE_GRAPH_CONTAINER =
            new GenericContainer<>(DockerImageName.parse(HUGE_GRAPH_IMAGE))
                    .withExposedPorts(8080, 8182)
                    .waitingFor(Wait.forHttp("/graphs").forPort(8080).forStatusCode(200))
                    .withStartupTimeout(Duration.ofMinutes(3));

    @BeforeAll
    public static void setup() {
        String host = HUGE_GRAPH_CONTAINER.getHost();
        Integer port = HUGE_GRAPH_CONTAINER.getMappedPort(8080);
        Integer gremlinPort = HUGE_GRAPH_CONTAINER.getMappedPort(8182);
        String url = String.format("http://%s:%d", "127.0.0.1", 8080);
        hugeClient = HugeClient.builder(url, GRAPH_NAME).build();
        gremlinManager = hugeClient.gremlin();
        setupSchema();
    }

    @AfterAll
    public static void cleanup() {
        if (hugeClient != null) {
            hugeClient.close();
        }
    }

    @BeforeEach
    public void clearGraph() {
        // Clear all vertices and edges before each test
        gremlinManager.gremlin("g.V().drop().iterate()").execute();
    }

    private static void setupSchema() {
        hugeClient.schema().propertyKey("name").asText().ifNotExist().create();
        hugeClient.schema().propertyKey("age").asInt().ifNotExist().create();
        hugeClient
                .schema()
                .vertexLabel(VERTEX_LABEL_PERSON)
                .idStrategy(IdStrategy.PRIMARY_KEY)
                .primaryKeys("name")
                .properties("name", "age")
                .ifNotExist()
                .create();
        hugeClient.schema().edgeLabel("knows").properties("name", "age").ifNotExist().create();
    }

    private HugeGraphSinkWriter createSinkWriter(SchemaConfig schemaConfig) throws IOException {
        HugeGraphSinkConfig config = new HugeGraphSinkConfig();
        config.setHost("127.0.0.1");
        config.setPort(8080);
        config.setGraphName(GRAPH_NAME);
        config.setSchemaConfig(schemaConfig);
        return new HugeGraphSinkWriter(config, SEATUNNEL_ROW_TYPE);
    }

    @Test
    public void testInsert() throws IOException {
        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setType(SchemaConfig.LabelType.VERTEX);
        schemaConfig.setLabel(VERTEX_LABEL_PERSON);
        schemaConfig.setIdStrategy(IdStrategy.PRIMARY_KEY);
        schemaConfig.setIdFields(Collections.singletonList("name"));

        try {
            HugeGraphSinkWriter writer = createSinkWriter(schemaConfig);
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"marko", 29});
            row.setRowKind(RowKind.INSERT);
            writer.write(row);
            writer.close();
        } finally {

        }

        List<Object> results =
                gremlinManager
                        .gremlin(
                                "g.V().hasLabel('person_for_test').has('name', 'marko').values('age')")
                        .execute()
                        .data();
        assertEquals(1, results.size());
        assertEquals(29, ((Number) results.get(0)).intValue());
    }

    @Test
    public void testUpdate() throws IOException {
        // First, insert a vertex
        gremlinManager
                .gremlin("g.addV('person_for_test').property('name', 'vadas').property('age', 27)")
                .execute();

        MappingConfig mappingConfig = new MappingConfig();
        Map<String, String> map = new HashMap();
        map.put("name", "name");
        map.put("age", "age");
        mappingConfig.setField_mapping(map);
        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setType(SchemaConfig.LabelType.VERTEX);
        schemaConfig.setLabel(VERTEX_LABEL_PERSON);
        schemaConfig.setIdStrategy(IdStrategy.PRIMARY_KEY);
        schemaConfig.setIdFields(Collections.singletonList("name"));
        schemaConfig.setMapping(mappingConfig);

        try {
            HugeGraphSinkWriter writer = createSinkWriter(schemaConfig);
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"vadas", 28});
            row.setRowKind(RowKind.UPDATE_AFTER);
            writer.write(row);
            writer.close();
        } finally {
        }

        List<Object> results =
                gremlinManager
                        .gremlin(
                                "g.V().hasLabel('person_for_test').has('name', 'vadas').values('age')")
                        .execute()
                        .data();
        assertEquals(1, results.size());
        assertEquals(28, ((Number) results.get(0)).intValue());
    }

    @Test
    public void testDelete() throws IOException {
        // First, insert a vertex
        gremlinManager
                .gremlin("g.addV('person_for_test').property('name', 'josh').property('age', 32)")
                .execute();

        SchemaConfig schemaConfig = new SchemaConfig();
        schemaConfig.setType(SchemaConfig.LabelType.VERTEX);
        schemaConfig.setLabel(VERTEX_LABEL_PERSON);
        schemaConfig.setIdStrategy(IdStrategy.PRIMARY_KEY);
        schemaConfig.setIdFields(Collections.singletonList("name"));

        try {
            HugeGraphSinkWriter writer = createSinkWriter(schemaConfig);
            // The row only needs to contain the ID fields for a delete operation
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {"josh", 32});
            row.setRowKind(RowKind.DELETE);
            writer.write(row);
            writer.close();
        } finally {
        }

        List<Object> results =
                gremlinManager
                        .gremlin("g.V().hasLabel('person_for_test').has('name', 'josh')")
                        .execute()
                        .data();
        assertFalse(results.iterator().hasNext(), "Vertex should have been deleted");
    }
}
