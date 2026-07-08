package com.mercari.solution.util.pipeline.lookup.source;

import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Query2;
import org.joda.time.Instant;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * End-to-end lookup-join tests against a descriptor-driven in-JVM gRPC server:
 * unary point lookups with descriptor-derived schemas (nested message → element
 * row, repeated → array), rowsFrom fan-out, server streaming, LEFT joins,
 * explicit field overrides and the DoFn-style Java serialization round trip.
 */
public class GrpcLookupSourceTest {

    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    private static DynamicGrpcTestServer server;
    private static Path descriptorSetPath;

    @BeforeAll
    public static void startServer() throws Exception {
        server = new DynamicGrpcTestServer();
        descriptorSetPath = server.writeDescriptorSet(
                Files.createTempDirectory("grpc-lookup-test"));
    }

    @AfterAll
    public static void stopServer() {
        if (server != null) {
            server.shutdown();
        }
    }

    private static GrpcLookupSource.Builder source() {
        return GrpcLookupSource.builder()
                .withName("grpc")
                .withTarget("localhost:" + server.port())
                .withPlaintext(true)
                .withDescriptorSetPath(descriptorSetPath.toString());
    }

    private static GrpcLookupSource.TableConfig usersTable() {
        return GrpcLookupSource.TableConfig.builder()
                .withName("users")
                .withMethod("demo.UserService/GetUser")
                .withKeyField("id")
                .build();
    }

    private static GrpcLookupSource.TableConfig classifyTable() {
        return GrpcLookupSource.TableConfig.builder()
                .withName("classify")
                .withMethod("demo.Classifier/Classify")
                .withKeyField("text")
                .withRowsFrom("labels")
                .build();
    }

    private static Schema userInputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("userId", Schema.FieldType.INT64)));
    }

    private static MElement userInput(long userId) {
        return MElement.of(Map.of("userId", userId), TIMESTAMP);
    }

    private static Schema textInputSchema() {
        return Schema.of(List.of(
                Schema.Field.of("word", Schema.FieldType.STRING)));
    }

    private static MElement textInput(String word) {
        return MElement.of(Map.of("word", word), TIMESTAMP);
    }

    @Test
    public void testUnaryPointLookupWithDerivedSchema() {
        // Schema fully derived from the descriptor set: the response echoes the
        // key (id), address is a nested element row, tags a string array.
        final Query2 query = Query2.builder()
                .withInput("INPUT", userInputSchema())
                .withSource(source().withTable(usersTable()).build())
                .withSql("""
                        SELECT i.userId AS userId, u.name AS name,
                               u.address.city AS city,
                               CARDINALITY(u.tags) AS tagCount
                        FROM INPUT AS i
                        JOIN grpc.users AS u ON u.id = i.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(userInput(1L), userInput(2L)), TIMESTAMP);
            Assertions.assertEquals(2, outputs.size());
            final MElement alice = byUser(outputs, 1L);
            Assertions.assertEquals("alice", alice.getAsString("name"));
            Assertions.assertEquals("NYC", alice.getAsString("city"));
            Assertions.assertEquals(2, ((Number) alice.getPrimitiveValue("tagCount")).intValue());
            final MElement bob = byUser(outputs, 2L);
            Assertions.assertEquals("bob", bob.getAsString("name"));
            Assertions.assertEquals(0, ((Number) bob.getPrimitiveValue("tagCount")).intValue());
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testRowsFromFanOutAndLeftJoin() {
        // ClassifyResponse does not echo the key: the 'text' column is prepended
        // to the derived schema and filled from the request key. One term fans
        // out to many label rows; a term with no labels survives via LEFT JOIN.
        final Query2 query = Query2.builder()
                .withInput("INPUT", textInputSchema())
                .withSource(source().withTable(classifyTable()).build())
                .withSql("""
                        SELECT i.word AS word, c.name AS label, c.score AS score
                        FROM INPUT AS i
                        LEFT JOIN grpc.classify AS c ON c.text = i.word
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(textInput("spanner"), textInput("unknown")), TIMESTAMP);
            Assertions.assertEquals(3, outputs.size());
            final List<MElement> spanner = outputs.stream()
                    .filter(o -> "spanner".equals(o.getAsString("word"))).toList();
            Assertions.assertEquals(2, spanner.size());
            Assertions.assertEquals("db", spanner.get(0).getAsString("label"));
            Assertions.assertEquals(0.9d,
                    ((Number) spanner.get(0).getPrimitiveValue("score")).doubleValue(), 1e-9);
            final MElement unknown = outputs.stream()
                    .filter(o -> "unknown".equals(o.getAsString("word"))).findFirst().orElseThrow();
            Assertions.assertNull(unknown.getPrimitiveValue("label"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testServerStreamingFanOut() {
        // Note: not named "stream" — STREAM is a Calcite reserved word.
        final Query2 query = Query2.builder()
                .withInput("INPUT", textInputSchema())
                .withSource(source().withTable(GrpcLookupSource.TableConfig.builder()
                        .withName("streamedLabels")
                        .withMethod("demo.Streamer/Stream")
                        .withKeyField("text")
                        .withServerStreaming(true)
                        .build()).build())
                .withSql("""
                        SELECT i.word AS word, s.name AS label
                        FROM INPUT AS i
                        JOIN grpc.streamedLabels AS s ON s.text = i.word
                        ORDER BY s.name
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(textInput("spanner")), TIMESTAMP);
            Assertions.assertEquals(2, outputs.size());
            Assertions.assertEquals("cloud", outputs.get(0).getAsString("label"));
            Assertions.assertEquals("db", outputs.get(1).getAsString("label"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testExplicitFieldsOverrideDerivedColumns() {
        // Declared fields replace the derived response columns (a subset here);
        // the key column is still prepended and typed from the request field.
        final Query2 query = Query2.builder()
                .withInput("INPUT", textInputSchema())
                .withSource(source().withTable(GrpcLookupSource.TableConfig.builder()
                        .withName("classify")
                        .withMethod("demo.Classifier/Classify")
                        .withKeyField("text")
                        .withRowsFrom("labels")
                        .withFields(List.of(
                                Schema.Field.of("name", Schema.FieldType.STRING)))
                        .build()).build())
                .withSql("""
                        SELECT c.name AS label
                        FROM INPUT AS i
                        JOIN grpc.classify AS c ON c.text = i.word
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(
                    List.of(textInput("bigtable")), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals("nosql", outputs.getFirst().getAsString("label"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testSerializedRoundTripLikeDoFn() throws Exception {
        // Pipeline-construction setup derives the schema and reads the
        // descriptor set bytes; the serialized copy must work without the file.
        final GrpcLookupSource original = source().withTable(usersTable()).build();
        original.setup();
        original.close();

        final Query2 query = Query2.builder()
                .withInput("INPUT", userInputSchema())
                .withSource(original)
                .withSql("""
                        SELECT i.userId AS userId, u.name AS name
                        FROM INPUT AS i
                        JOIN grpc.users AS u ON u.id = i.userId
                        """)
                .build();

        final java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream out = new java.io.ObjectOutputStream(bytes)) {
            out.writeObject(query);
        }
        final Query2 restored;
        try (java.io.ObjectInputStream in = new java.io.ObjectInputStream(
                new java.io.ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (Query2) in.readObject();
        }
        restored.setup();
        try {
            final List<MElement> outputs = restored.execute(
                    List.of(userInput(2L)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals("bob", outputs.getFirst().getAsString("name"));
        } finally {
            restored.teardown();
        }
    }

    private static MElement byUser(List<MElement> outputs, long userId) {
        return outputs.stream()
                .filter(o -> ((Number) o.getPrimitiveValue("userId")).longValue() == userId)
                .findFirst()
                .orElseThrow();
    }
}
