package com.mercari.solution.util.pipeline.lookup.source;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.Query2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Client-free tests for Spanner query tables (parameterized GoogleSQL/GQL as
 * key-driven tables): configuration validation and SQL planning with an
 * explicit result schema. Planning never contacts Spanner — the client is only
 * used at lookup time (and for schema derivation when fields are omitted),
 * which the emulator integration test covers.
 */
public class SpannerQueryTableTest {

    private static final String GQL = """
            GRAPH SocialGraph
            MATCH (a:Users)-[:Interacted]-(b:Users)
            WHERE a.id IN UNNEST(@keys)
            RETURN a.id AS userId, COUNT(DISTINCT b.id) AS relatedCount
            GROUP BY userId
            """;

    private static SpannerLookupSource graphSource() {
        return SpannerLookupSource.builder()
                .withName("graph")
                .withProjectId("p").withInstanceId("i").withDatabaseId("d")
                .withQueryTable(SpannerLookupSource.queryTable()
                        .withName("relatedPeople")
                        .withSql(GQL)
                        .withKeyField("userId")
                        .withBindMode(SpannerLookupSource.BindMode.ARRAY)
                        .withFields(List.of(
                                Schema.Field.of("userId", Schema.FieldType.STRING),
                                Schema.Field.of("relatedCount", Schema.FieldType.INT64)))
                        .build())
                .build();
    }

    @Test
    public void testPlansLookupJoinOverQueryTable() {
        // Construction plans the SQL and derives the output schema; with explicit
        // fields this must succeed without any Spanner RPC.
        final Query2 query = Query2.builder()
                .withInput("INPUT", Schema.of(List.of(
                        Schema.Field.of("userId", Schema.FieldType.STRING))))
                .withSource(graphSource())
                .withSql("""
                        SELECT i.userId AS userId, g.relatedCount AS relatedCount
                        FROM INPUT AS i
                        JOIN graph.relatedPeople AS g ON g.userId = i.userId
                        """)
                .build();
        final Schema outputSchema = query.getOutputSchema();
        Assertions.assertEquals(2, outputSchema.countFields());
        Assertions.assertEquals(Schema.Type.int64,
                outputSchema.getField("relatedCount").getFieldType().getType());
    }

    @Test
    public void testPlansLateralOverQueryTable() {
        // The LATERAL machinery is source-agnostic: a point-equality correlation
        // on the query key plans without contacting Spanner.
        final Query2 query = Query2.builder()
                .withInput("INPUT", Schema.of(List.of(
                        Schema.Field.of("userId", Schema.FieldType.STRING))))
                .withSource(graphSource())
                .withSql("""
                        SELECT i.userId AS userId, s.maxRelated AS maxRelated
                        FROM INPUT AS i
                        JOIN LATERAL (
                          SELECT MAX(g.relatedCount) AS maxRelated
                          FROM graph.relatedPeople AS g
                          WHERE g.userId = i.userId
                        ) AS s ON TRUE
                        """)
                .build();
        Assertions.assertEquals(2, query.getOutputSchema().countFields());
    }

    @Test
    public void testQueryMustReferenceBindParameter() {
        final IllegalArgumentException e = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> SpannerLookupSource.queryTable()
                        .withName("t")
                        .withSql("SELECT id FROM T")   // no @keys
                        .withKeyField("id")
                        .build());
        Assertions.assertTrue(e.getMessage().contains("@keys"),
                "unexpected message: " + e.getMessage());
    }

    @Test
    public void testFieldsMustContainKeyField() {
        final IllegalArgumentException e = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> SpannerLookupSource.queryTable()
                        .withName("t")
                        .withSql("SELECT id, score FROM T WHERE term = @key")
                        .withParamName("key")
                        .withKeyField("term")
                        .withFields(List.of(
                                Schema.Field.of("id", Schema.FieldType.STRING),
                                Schema.Field.of("score", Schema.FieldType.FLOAT64)))
                        .build());
        Assertions.assertTrue(e.getMessage().contains("key field"),
                "unexpected message: " + e.getMessage());
    }
}
