package com.mercari.solution.util.pipeline.lookup.source;

import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreFactory;
import com.google.datastore.v1.client.DatastoreOptions;
import com.google.protobuf.Timestamp;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Query2;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DatastoreEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration tests (run via maven-failsafe: {@code mvn verify -DskipITs=false
 * -Dit.test=DatastoreQueryLookupIT}) for the datastore lookup source — Datastore
 * kinds exposed as key-driven lookup tables of the query engine — against the
 * Cloud Datastore emulator managed by Testcontainers.
 */
@Testcontainers(disabledWithoutDocker = true)
public class DatastoreQueryLookupIT {

    private static final String PROJECT = "test-project";
    private static final Instant TIMESTAMP = Instant.parse("2026-01-01T00:00:00Z");

    @Container
    private static final DatastoreEmulatorContainer EMULATOR = new DatastoreEmulatorContainer(
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"));

    @BeforeAll
    static void setUp() throws Exception {
        final Datastore datastore = DatastoreFactory.get().create(new DatastoreOptions.Builder()
                .projectId(PROJECT)
                .localHost(EMULATOR.getEmulatorEndpoint())
                .initializer(new RetryHttpRequestInitializer())
                .build());

        final CommitRequest.Builder commit = CommitRequest.newBuilder()
                .setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
        // Users: string key names, with a nested entity for the json column.
        commit.addMutations(upsert(userEntity("u1", "alice", 20L, "tokyo")));
        commit.addMutations(upsert(userEntity("u2", "bob", 30L, "osaka")));
        // Items: numeric key ids.
        for (long id = 1; id <= 2; id++) {
            final Key key = Key.newBuilder()
                    .addPath(Key.PathElement.newBuilder().setKind("Items").setId(id))
                    .build();
            commit.addMutations(upsert(Entity.newBuilder()
                    .setKey(key)
                    .putProperties("title", string(id == 1 ? "apple" : "banana"))
                    .putProperties("price", integer(id * 100))
                    .build()));
        }
        datastore.commit(commit.build());
    }

    private static Entity userEntity(String id, String name, long age, String city) {
        final Key key = Key.newBuilder()
                .addPath(Key.PathElement.newBuilder().setKind("Users").setName(id))
                .build();
        return Entity.newBuilder()
                .setKey(key)
                .putProperties("name", string(name))
                .putProperties("age", integer(age))
                .putProperties("registeredAt", Value.newBuilder()
                        .setTimestampValue(Timestamp.newBuilder().setSeconds(1735689600L))
                        .build())
                .putProperties("address", Value.newBuilder()
                        .setEntityValue(Entity.newBuilder()
                                .putProperties("city", string(city)))
                        .build())
                .build();
    }

    private static Mutation upsert(Entity entity) {
        return Mutation.newBuilder().setUpsert(entity).build();
    }

    private static Value string(String value) {
        return Value.newBuilder().setStringValue(value).build();
    }

    private static Value integer(long value) {
        return Value.newBuilder().setIntegerValue(value).build();
    }

    private static DatastoreLookupSource.Builder source() {
        return DatastoreLookupSource.builder()
                .withName("ds")
                .withProjectId(PROJECT)
                .withEmulatorHost(EMULATOR.getEmulatorEndpoint());
    }

    @Test
    public void testLookupJoinAndLeftJoinNull() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", Schema.of(List.of(
                        Schema.Field.of("userId", Schema.FieldType.STRING))))
                .withSource(source()
                        .withTable(DatastoreLookupSource.table()
                                .withName("users")
                                .withKind("Users")
                                .withField("name", Schema.FieldType.STRING)
                                .withField("age", Schema.FieldType.INT64)
                                .withField("registeredAt", Schema.FieldType.TIMESTAMP)
                                .withField("address", Schema.FieldType.JSON)
                                .build())
                        .build())
                .withSql("""
                        SELECT i.userId AS userId, u.name AS name, u.age AS age,
                               u.registeredAt AS registeredAt, u.address AS address
                        FROM INPUT AS i
                        LEFT JOIN ds.users AS u ON u.__name__ = i.userId
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
            Assertions.assertEquals("bob", byUser.get("u2").getAsString("name"));
            // LEFT JOIN: missing entity pads null.
            Assertions.assertNull(byUser.get("u9").getPrimitiveValue("name"));
            Assertions.assertNull(byUser.get("u9").getPrimitiveValue("age"));
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testNumericIdKeyLookup() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", Schema.of(List.of(
                        Schema.Field.of("itemId", Schema.FieldType.INT64),
                        Schema.Field.of("qty", Schema.FieldType.INT64))))
                .withSource(source()
                        .withTable(DatastoreLookupSource.table()
                                .withName("items")
                                .withKind("Items")
                                .withKeyField("itemId")
                                .withNumericKey(true)
                                .withField("title", Schema.FieldType.STRING)
                                .withField("price", Schema.FieldType.INT64)
                                .build())
                        .build())
                .withSql("""
                        SELECT i.itemId AS itemId, m.title AS title, i.qty * m.price AS total
                        FROM INPUT AS i
                        JOIN ds.items AS m ON m.itemId = i.itemId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("itemId", 1L, "qty", 2L), TIMESTAMP),
                    MElement.of(Map.of("itemId", 2L, "qty", 3L), TIMESTAMP),
                    MElement.of(Map.of("itemId", 9L, "qty", 1L), TIMESTAMP)), TIMESTAMP);
            // INNER JOIN drops the missing id 9.
            Assertions.assertEquals(2, outputs.size());
            final Map<Long, MElement> byItem = new HashMap<>();
            for (final MElement output : outputs) {
                byItem.put(output.getAsLong("itemId"), output);
            }
            Assertions.assertEquals("apple", byItem.get(1L).getAsString("title"));
            Assertions.assertEquals(200L, byItem.get(1L).getAsLong("total"));
            Assertions.assertEquals(600L, byItem.get(2L).getAsLong("total"));
        } finally {
            query.teardown();
        }
    }
}
