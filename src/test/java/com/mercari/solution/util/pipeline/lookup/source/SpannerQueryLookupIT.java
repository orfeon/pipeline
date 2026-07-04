package com.mercari.solution.util.pipeline.lookup.source;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Query2;
import org.joda.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Integration tests (run via maven-failsafe: {@code mvn verify -DskipITs=false
 * -Dit.test=SpannerQueryLookupIT}) for Spanner query tables — parameterized
 * GoogleSQL/GQL statements exposed as key-driven lookup tables — against the
 * Cloud Spanner emulator managed by Testcontainers:
 *
 * <ul>
 *   <li>ARRAY bind + schema derivation via {@code analyzeQuery(PLAN)} (plain SQL)</li>
 *   <li>Spanner Graph (GQL) traversal — the graph-source shape</li>
 *   <li>Full-text search ({@code SEARCH}/{@code SCORE}, PER_KEY bind) — the
 *       search-source shape, fanning one term out to many hits</li>
 *   <li>a correlated LATERAL top-N over the search hits, evaluated in-process</li>
 * </ul>
 */
@Testcontainers(disabledWithoutDocker = true)
public class SpannerQueryLookupIT {

    private static final String PROJECT = "test-project";
    private static final String INSTANCE = "test-instance";
    private static final String DATABASE = "querydb";
    private static final Instant TIMESTAMP = Instant.parse("2025-05-01T00:00:00Z");

    @Container
    private static final SpannerEmulatorContainer EMULATOR = new SpannerEmulatorContainer(
            DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator:1.5.43"));

    @BeforeAll
    static void setUp() throws Exception {
        // SpannerUtil.getEmulatorHost() honors this system property.
        System.setProperty("SPANNER_EMULATOR_HOST", EMULATOR.getEmulatorGrpcEndpoint());

        try (Spanner spanner = SpannerOptions.newBuilder()
                .setProjectId(PROJECT)
                .setEmulatorHost(EMULATOR.getEmulatorGrpcEndpoint())
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService()) {

            spanner.getInstanceAdminClient()
                    .createInstance(InstanceInfo.newBuilder(InstanceId.of(PROJECT, INSTANCE))
                            .setInstanceConfigId(InstanceConfigId.of(PROJECT, "emulator-config"))
                            .setDisplayName("query-lookup-it")
                            .setNodeCount(1)
                            .build())
                    .get(60, TimeUnit.SECONDS);

            spanner.getDatabaseAdminClient()
                    .createDatabase(INSTANCE, DATABASE, List.of(
                            "CREATE TABLE Items ("
                                    + " id INT64 NOT NULL,"
                                    + " title STRING(64),"
                                    + " price INT64"
                                    + ") PRIMARY KEY (id)",
                            "CREATE TABLE Users ("
                                    + " id STRING(36) NOT NULL, name STRING(64)"
                                    + ") PRIMARY KEY (id)",
                            "CREATE TABLE Interactions ("
                                    + " id STRING(36) NOT NULL, peer_id STRING(36) NOT NULL"
                                    + ") PRIMARY KEY (id, peer_id),"
                                    + " INTERLEAVE IN PARENT Users ON DELETE CASCADE"))
                    .get(120, TimeUnit.SECONDS);

            // Graph and full-text search DDL separately: abort (skip) all tests
            // when the emulator build lacks support for them.
            try {
                spanner.getDatabaseAdminClient()
                        .updateDatabaseDdl(INSTANCE, DATABASE, List.of(
                                "CREATE PROPERTY GRAPH SocialGraph"
                                        + " NODE TABLES (Users)"
                                        + " EDGE TABLES (Interactions"
                                        + "   SOURCE KEY (id) REFERENCES Users (id)"
                                        + "   DESTINATION KEY (peer_id) REFERENCES Users (id)"
                                        + "   LABEL Interacted)",
                                "CREATE TABLE Documents ("
                                        + " docId INT64 NOT NULL,"
                                        + " title STRING(MAX),"
                                        + " title_tokens TOKENLIST"
                                        + "   AS (TOKENIZE_FULLTEXT(title)) HIDDEN"
                                        + ") PRIMARY KEY (docId)",
                                "CREATE SEARCH INDEX DocumentsIndex ON Documents(title_tokens)"),
                                null)
                        .get(120, TimeUnit.SECONDS);
            } catch (Exception e) {
                Assumptions.abort("Spanner emulator build lacks Graph/Search support: "
                        + e.getMessage());
            }

            final DatabaseClient client = spanner.getDatabaseClient(
                    DatabaseId.of(PROJECT, INSTANCE, DATABASE));
            final List<Mutation> mutations = new ArrayList<>();
            mutations.add(insert("Items", Map.of("id", 1L, "title", "apple", "price", 100L)));
            mutations.add(insert("Items", Map.of("id", 2L, "title", "banana", "price", 200L)));
            for (final String id : List.of("u1", "u2", "u3", "u4")) {
                mutations.add(insert("Users", Map.of("id", id, "name", "name-" + id)));
            }
            // Undirected interactions u1-u2, u1-u3, u2-u3 (u4 is isolated).
            mutations.add(insert("Interactions", Map.of("id", "u1", "peer_id", "u2")));
            mutations.add(insert("Interactions", Map.of("id", "u1", "peer_id", "u3")));
            mutations.add(insert("Interactions", Map.of("id", "u2", "peer_id", "u3")));
            mutations.add(insert("Documents", Map.of(
                    "docId", 1L, "title", "intro to spanner graph")));
            mutations.add(insert("Documents", Map.of(
                    "docId", 2L, "title", "spanner search deep dive")));
            mutations.add(insert("Documents", Map.of(
                    "docId", 3L, "title", "bigtable basics")));
            client.write(mutations);
        }
    }

    private static Mutation insert(String table, Map<String, Object> values) {
        final Mutation.WriteBuilder builder = Mutation.newInsertBuilder(table);
        for (final Map.Entry<String, Object> entry : values.entrySet()) {
            switch (entry.getValue()) {
                case Long l -> builder.set(entry.getKey()).to(l);
                case String s -> builder.set(entry.getKey()).to(s);
                default -> throw new IllegalArgumentException();
            }
        }
        return builder.build();
    }

    private static SpannerLookupSource.Builder source(String name) {
        return SpannerLookupSource.builder()
                .withName(name)
                .withProjectId(PROJECT)
                .withInstanceId(INSTANCE)
                .withDatabaseId(DATABASE)
                .withEmulator(true);
    }

    @Test
    public void testArrayBindWithDerivedSchema() {
        // No fields given: the result schema is derived via analyzeQuery(PLAN).
        final Query2 query = Query2.builder()
                .withInput("INPUT", Schema.of(List.of(
                        Schema.Field.of("itemId", Schema.FieldType.INT64),
                        Schema.Field.of("qty", Schema.FieldType.INT64))))
                .withSource(source("sp")
                        .withQueryTable(SpannerLookupSource.queryTable()
                                .withName("items")
                                .withSql("SELECT id AS itemId, title, price FROM Items"
                                        + " WHERE id IN UNNEST(@keys)")
                                .withKeyField("itemId")
                                .build())
                        .build())
                .withSql("""
                        SELECT i.itemId AS itemId, m.title AS title, i.qty * m.price AS total
                        FROM INPUT AS i
                        JOIN sp.items AS m ON m.itemId = i.itemId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("itemId", 1L, "qty", 2L), TIMESTAMP),
                    MElement.of(Map.of("itemId", 2L, "qty", 3L), TIMESTAMP),
                    MElement.of(Map.of("itemId", 9L, "qty", 1L), TIMESTAMP)), TIMESTAMP);
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

    @Test
    public void testGraphTraversalLookupJoin() {
        // The graph-source shape: a GQL traversal keyed by the start node,
        // ARRAY-binding the batch's distinct start keys to @keys.
        final Query2 query = Query2.builder()
                .withInput("INPUT", Schema.of(List.of(
                        Schema.Field.of("userId", Schema.FieldType.STRING))))
                .withSource(source("graph")
                        .withQueryTable(SpannerLookupSource.queryTable()
                                .withName("relatedPeople")
                                .withSql("GRAPH SocialGraph"
                                        + " MATCH (a:Users)-[:Interacted]-(b:Users)"
                                        + " WHERE a.id IN UNNEST(@keys)"
                                        + " RETURN a.id AS userId,"
                                        + " COUNT(DISTINCT b.id) AS relatedCount"
                                        + " GROUP BY userId")
                                .withKeyField("userId")
                                .withFields(List.of(
                                        Schema.Field.of("userId", Schema.FieldType.STRING),
                                        Schema.Field.of("relatedCount", Schema.FieldType.INT64)))
                                .build())
                        .build())
                .withSql("""
                        SELECT i.userId AS userId, COALESCE(g.relatedCount, 0) AS relatedCount
                        FROM INPUT AS i
                        LEFT JOIN graph.relatedPeople AS g ON g.userId = i.userId
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("userId", "u1"), TIMESTAMP),
                    MElement.of(Map.of("userId", "u2"), TIMESTAMP),
                    MElement.of(Map.of("userId", "u4"), TIMESTAMP)), TIMESTAMP);
            Assertions.assertEquals(3, outputs.size());
            final Map<String, Long> counts = new HashMap<>();
            for (final MElement output : outputs) {
                counts.put(output.getAsString("userId"), output.getAsLong("relatedCount"));
            }
            // u1 interacts with u2,u3; u2 with u1,u3; u4 is isolated (LEFT → 0)
            Assertions.assertEquals(Map.of("u1", 2L, "u2", 2L, "u4", 0L), counts);
        } finally {
            query.teardown();
        }
    }

    private static SpannerLookupSource searchSource() {
        // The search-source shape: SEARCH takes a single query string (no array
        // form), so the statement runs once per distinct term (PER_KEY) and
        // returns the term via @query AS queryKey so hits join back.
        return source("fts")
                .withQueryTable(SpannerLookupSource.queryTable()
                        .withName("docs")
                        .withSql("SELECT @query AS queryKey, docId, title,"
                                + " SCORE(title_tokens, @query) AS score"
                                + " FROM Documents WHERE SEARCH(title_tokens, @query)")
                        .withKeyField("queryKey")
                        .withParamName("query")
                        .withBindMode(SpannerLookupSource.BindMode.PER_KEY)
                        .withFields(List.of(
                                Schema.Field.of("queryKey", Schema.FieldType.STRING),
                                Schema.Field.of("docId", Schema.FieldType.INT64),
                                Schema.Field.of("title", Schema.FieldType.STRING),
                                Schema.Field.of("score", Schema.FieldType.FLOAT64)))
                        .build())
                .build();
    }

    @Test
    public void testSearchLookupJoinFansOut() {
        final Query2 query = Query2.builder()
                .withInput("INPUT", Schema.of(List.of(
                        Schema.Field.of("term", Schema.FieldType.STRING))))
                .withSource(searchSource())
                .withSql("""
                        SELECT i.term AS term, d.docId AS docId, d.title AS title
                        FROM INPUT AS i
                        JOIN fts.docs AS d ON d.queryKey = i.term
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("term", "spanner"), TIMESTAMP),
                    MElement.of(Map.of("term", "bigtable"), TIMESTAMP)), TIMESTAMP);
            // "spanner" hits docs 1 and 2; "bigtable" hits doc 3.
            Assertions.assertEquals(3, outputs.size());
            long spannerHits = outputs.stream()
                    .filter(o -> o.getAsString("term").equals("spanner")).count();
            Assertions.assertEquals(2, spannerHits);
        } finally {
            query.teardown();
        }
    }

    @Test
    public void testLateralTopNOverSearchHits() {
        // Per-term best hit: ORDER BY score DESC LIMIT 1 inside a correlated
        // LATERAL block, evaluated over each term's fetched hits in-process.
        final Query2 query = Query2.builder()
                .withInput("INPUT", Schema.of(List.of(
                        Schema.Field.of("term", Schema.FieldType.STRING))))
                .withSource(searchSource())
                .withSql("""
                        SELECT i.term AS term, s.docId AS docId, s.title AS title
                        FROM INPUT AS i
                        JOIN LATERAL (
                          SELECT d.docId, d.title
                          FROM fts.docs AS d
                          WHERE d.queryKey = i.term
                          ORDER BY d.score DESC, d.docId ASC
                          LIMIT 1
                        ) AS s ON TRUE
                        """)
                .build();
        query.setup();
        try {
            final List<MElement> outputs = query.execute(List.of(
                    MElement.of(Map.of("term", "spanner"), TIMESTAMP)), TIMESTAMP);
            Assertions.assertEquals(1, outputs.size());
            Assertions.assertTrue(outputs.getFirst().getAsString("title").contains("spanner"));
        } finally {
            query.teardown();
        }
    }
}
