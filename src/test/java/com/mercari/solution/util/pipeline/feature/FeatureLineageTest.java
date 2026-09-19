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

    /**
     * Every column of a future window carries the role {@code label}: from the schema alone the declared label cannot
     * be told apart, so the role is left unresolved (a consumer then needs its own parameter) and the manifest — which
     * records the declared one — fills it on merge. A role carried by one field resolves as before.
     */
    @Test
    public void testRoleCarriedBySeveralFieldsIsNotGuessed() {
        final Schema schema = Schema.builder()
                .withField(field("next_7d_price_last", "label"))
                .withField(field("ret", "label"))
                .withField(field("w", "weight"))
                .withField(field("x", null))
                .build();
        final FeatureLineage lineage = FeatureLineage.fromSchema(schema);
        Assertions.assertNull(lineage.roles.get("label"));
        Assertions.assertEquals("w", lineage.roles.get("weight"));
        Assertions.assertEquals(4, lineage.columns.size());

        final FeatureLineage manifest = FeatureLineage.fromManifest("{\"roles\": {\"label\": {\"name\": \"ret\", \"column\": \"ret\"}}}", "manifest");
        Assertions.assertEquals("ret", lineage.merge(manifest).roles.get("label"));
    }
}
