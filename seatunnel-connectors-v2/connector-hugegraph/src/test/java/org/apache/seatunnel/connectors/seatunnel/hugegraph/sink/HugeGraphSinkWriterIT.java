package org.apache.seatunnel.connectors.seatunnel.hugegraph.sink;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.SchemaConfig;

import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Vertex;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        String url = String.format("http://%s:%d", host, port);
        hugeClient = HugeClient.builder(url, GRAPH_NAME).build();
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
        // Clear all vertices and edges before each test using GraphsManager.clearGraph()
        try {
            hugeClient.graphs().clearGraph(GRAPH_NAME, "I'm sure to delete all data");
            // After clearing, need to recreate schema
            setupSchema();
        } catch (Exception e) {
            // Ignore errors during clear
        }
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
        hugeClient
                .schema()
                .edgeLabel("knows")
                .sourceLabel(VERTEX_LABEL_PERSON)
                .targetLabel(VERTEX_LABEL_PERSON)
                .properties("name", "age")
                .ifNotExist()
                .create();
    }

    private HugeGraphSinkWriter createSinkWriter(SchemaConfig schemaConfig) throws IOException {
        HugeGraphSinkConfig config = new HugeGraphSinkConfig();
        config.setHost(HUGE_GRAPH_CONTAINER.getHost());
        config.setPort(HUGE_GRAPH_CONTAINER.getMappedPort(8080));
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

        // Verify using REST API
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "marko");
        List<Vertex> vertices =
                hugeClient.graph().listVertices(VERTEX_LABEL_PERSON, properties, 10);
        assertEquals(1, vertices.size());
        assertEquals(29, vertices.get(0).property("age"));
    }

    @Test
    public void testUpdate() throws IOException {
        // First, insert a vertex using REST API
        Vertex vadas = new Vertex(VERTEX_LABEL_PERSON);
        vadas.property("name", "vadas");
        vadas.property("age", 27);
        hugeClient.graph().addVertex(vadas);

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

        // Verify using REST API
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "vadas");
        List<Vertex> vertices =
                hugeClient.graph().listVertices(VERTEX_LABEL_PERSON, properties, 10);
        assertEquals(1, vertices.size());
        assertEquals(28, vertices.get(0).property("age"));
    }

    @Test
    public void testDelete() throws IOException {
        // First, insert a vertex using REST API
        Vertex josh = new Vertex(VERTEX_LABEL_PERSON);
        josh.property("name", "josh");
        josh.property("age", 32);
        hugeClient.graph().addVertex(josh);

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

        // Verify using REST API
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", "josh");
        List<Vertex> vertices =
                hugeClient.graph().listVertices(VERTEX_LABEL_PERSON, properties, 10);
        assertTrue(vertices.isEmpty(), "Vertex should have been deleted");
    }
}
