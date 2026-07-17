package com.mercari.solution.util.pipeline.lookup.source;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Query2;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.FirestoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration tests (run via maven-failsafe: {@code mvn verify -DskipITs=false
 * -Dit.test=FirestoreQueryLookupIT}) for the firestore lookup source — Firestore
 * collections exposed as key-driven lookup tables of the query engine — against
 * the Firestore emulator managed by Testcontainers.
 */
@Testcontainers(disabledWithoutDocker = true)
public class FirestoreQueryLookupIT {

    private static final String PROJECT = "test-project";
    private static final Instant TIMESTAMP = Instant.parse("2026-01-01T00:00:00Z");

    @Container
    private static final FirestoreEmulatorContainer EMULATOR = new FirestoreEmulatorContainer(
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"));

    @BeforeAll
    static void setUp() throws Exception {
        try (Firestore firestore = FirestoreOptions.newBuilder()
                .setProjectId(PROJECT)
                .setEmulatorHost(EMULATOR.getEmulatorEndpoint())
                .build()
                .getService()) {

            firestore.collection("Users").document("u1")
                    .set(user("alice", 20L, "tokyo", List.of("a", "b"))).get();
            firestore.collection("Users").document("u2")
                    .set(user("bob", 30L, "osaka", List.of())).get();
            // Subcollection: orders of user u1.
            firestore.collection("Users").document("u1")
                    .collection("Orders").document("o1")
                    .set(Map.of("total", 500L)).get();
        }
    }

    private static Map<String, Object> user(String name, long age, String city,
            List<String> tags) {
        final Map<String, Object> user = new LinkedHashMap<>();
        user.put("name", name);
        user.put("age", age);
        user.put("registeredAt", com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1735689600L, 0));
        user.put("address", Map.of("city", city));
        user.put("tags", tags);
        return user;
    }

    private static FirestoreLookupSource.Builder source() {
        return FirestoreLookupSource.builder()
                .withName("fs")
                .withProjectId(PROJECT)
                .withEmulatorHost(EMULATOR.getEmulatorEndpoint());
    }

    @Test
    public void testLookupJoinAndLeftJoinNull() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", Schema.of(List.of(
                        Schema.Field.of("userId", Schema.FieldType.STRING))))
                .withSource(source()
                        .withTable(FirestoreLookupSource.table()
                                .withName("users")
                                .withCollection("Users")
                                .withField("name", Schema.FieldType.STRING)
                                .withField("age", Schema.FieldType.INT64)
                                .withField("registeredAt", Schema.FieldType.TIMESTAMP)
                                .withField("address", Schema.FieldType.JSON)
                                .withField("tags",
                                        Schema.FieldType.array(Schema.FieldType.STRING))
                                .build())
                        .build())
                .withSql("""
                        SELECT i.userId AS userId, u.name AS name, u.age AS age,
                               u.registeredAt AS registeredAt, u.address AS address,
                               u.tags AS tags
                        FROM INPUT AS i
                        LEFT JOIN fs.users AS u ON u.__name__ = i.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("userId", "u1"), TIMESTAMP),
                    MElement.of(Map.of("userId", "u2"), TIMESTAMP),
                    MElement.of(Map.of("userId", "u9"), TIMESTAMP)), TIMESTAMP);
            Assertions.assertEquals(3, outputs.size());
            final Map<String, MElement> byUser = new HashMap<>();
            for (final MElement output : outputs) {
                byUser.put(output.getAsString("userId"), output);
            }
            Assertions.assertEquals("alice", byUser.get("u1").getAsString("name"));
            Assertions.assertEquals(20L, byUser.get("u1").getAsLong("age"));
            // Calcite epoch millis surface as the project's epoch-micros primitive.
            Assertions.assertEquals(1735689600_000_000L,
                    byUser.get("u1").getAsLong("registeredAt"));
            Assertions.assertEquals("{\"city\":\"tokyo\"}",
                    byUser.get("u1").getAsString("address"));
            Assertions.assertEquals(List.of("a", "b"),
                    byUser.get("u1").getPrimitiveValue("tags"));
            Assertions.assertEquals("bob", byUser.get("u2").getAsString("name"));
            // LEFT JOIN: missing document pads null.
            Assertions.assertNull(byUser.get("u9").getPrimitiveValue("name"));
            Assertions.assertNull(byUser.get("u9").getPrimitiveValue("age"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testSubcollectionLookup() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", Schema.of(List.of(
                        Schema.Field.of("orderId", Schema.FieldType.STRING))))
                .withSource(source()
                        .withTable(FirestoreLookupSource.table()
                                .withName("orders")
                                .withCollection("Users/u1/Orders")
                                .withKeyField("orderId")
                                .withField("total", Schema.FieldType.INT64)
                                .build())
                        .build())
                .withSql("""
                        SELECT i.orderId AS orderId, o.total AS total
                        FROM INPUT AS i
                        JOIN fs.orders AS o ON o.orderId = i.orderId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("orderId", "o1"), TIMESTAMP),
                    MElement.of(Map.of("orderId", "o9"), TIMESTAMP)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertEquals(500L, outputs.getFirst().getAsLong("total"));
        } finally {
            query.teardown();
        }
    }
}
