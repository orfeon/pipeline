package com.mercari.solution.util.pipeline.feature;

import com.mercari.solution.module.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class FeatureLineageTest {

    private static Schema.Field field(final String name, final String role) {
        return Schema.Field.of(name, Schema.FieldType.FLOAT64).withOptions(role == null
                ? Map.of("feature.scope", "sequence") : Map.of("feature.scope", "sequence", "feature.role", role));
    }

    /** A future window's column: status {@code label}, no role. */
    private static Schema.Field future(final String name) {
        return Schema.Field.of(name, Schema.FieldType.FLOAT64).withOptions(Map.of("feature.scope", "sequence", "feature.status", "label"));
    }

    /**
     * The columns of a future window carry the status {@code label}, the role {@code label} stays with the declared
     * label: the schema alone resolves the role (a directly-downstream consumer keeps its label default) and every
     * label column is known.
     */
    @Test
    public void testDeclaredLabelResolvesAmongFutureColumns() {
        final Schema schema = Schema.builder()
                .withField(future("next_7d_price_last"))
                .withField(field("ret", "label"))
                .withField(field("w", "weight"))
                .withField(field("x", null))
                .build();
        final FeatureLineage lineage = FeatureLineage.fromSchema(schema);
        Assertions.assertEquals("ret", lineage.roles.get("label"));
        Assertions.assertEquals("w", lineage.roles.get("weight"));
        Assertions.assertEquals(4, lineage.columns.size());
        Assertions.assertEquals(java.util.Set.of("next_7d_price_last", "ret"), lineage.labels);
    }

    /** A screen never reads a label column as a candidate feature — not only the label it selected. */
    @Test
    public void testScreenExcludesEveryLabelColumn() {
        final Schema schema = Schema.builder()
                .withField(future("next_7d_price_last"))
                .withField(field("ret", "label"))
                .withField(field("x", null))
                .build();
        final com.mercari.solution.util.pipeline.screen.ScreenSpec spec = com.mercari.solution.util.pipeline.screen.ScreenSpec
                .parse(com.google.gson.JsonParser.parseString("{family: binomial, label: ret}").getAsJsonObject())
                .resolve(schema, FeatureLineage.fromSchema(schema));
        Assertions.assertEquals(java.util.List.of("x"), spec.candidates);

        // the manifest marks them too (a table read back through a sink)
        final FeatureLineage manifest = FeatureLineage.fromManifest("{\"columns\": [{\"name\": \"next_7d_price_last\", \"status\": \"label\"},"
                + " {\"name\": \"x\", \"status\": \"staticSafe\"}]}", "manifest");
        Assertions.assertEquals(java.util.Set.of("next_7d_price_last"), manifest.labels);
    }
}
