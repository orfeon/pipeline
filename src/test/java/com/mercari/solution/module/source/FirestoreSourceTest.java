package com.mercari.solution.module.source;

import com.google.firestore.v1.StructuredQuery;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MCollection;
import com.mercari.solution.module.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class FirestoreSourceTest {

    private final transient TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void testParametersSchemaIsTheDefinition() throws Exception {
        // schema-redesign.md Phase 4 (fields-only group): Firestore is schemaless,
        // schema.fields is the sole definition
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "firestoreinput",
                      "module": "firestore",
                      "parameters": {
                        "projectId": "myproject",
                        "collection": "users",
                        "schema": {
                          "fields": [
                            { "name": "stringField", "type": "string" }
                          ]
                        }
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        Assertions.assertTrue(outputs.get("firestoreinput").getSchema().hasField("stringField"));
    }

    @Test
    public void testMissingSchemaThrows() throws Exception {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "firestoreinput",
                      "module": "firestore",
                      "parameters": {
                        "projectId": "myproject",
                        "collection": "users"
                      }
                    }
                  ]
                }
                """;
        final Config config = Config.load(configJson);
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class,
                () -> MPipeline.apply(pipeline, config));
        Assertions.assertTrue(e.getMessage().contains("parameters.schema with fields is required"),
                "unexpected message: " + e.getMessage());
    }

    private static final Schema SCHEMA = Schema.builder()
            .withField("values", Schema.FieldType.INT64)
            .withField("brand", Schema.FieldType.STRING)
            .withField("order", Schema.FieldType.INT64)
            .withField("longvalue", Schema.FieldType.INT64)
            .build();

    private static FirestoreSource.Parameters createParameters(final String filter) {
        final JsonObject json = new JsonObject();
        json.addProperty("collection", "MyCollection");
        json.add("fields", new JsonArray());
        json.addProperty("allDescendants", false);
        json.addProperty("filter", filter);
        return new Gson().fromJson(json, FirestoreSource.Parameters.class);
    }

    @Test
    public void testFilterConditionWithSpaces() {
        // existing spaced form must keep working
        final StructuredQuery query = FirestoreSource.createQuery(SCHEMA, createParameters("longvalue >= 2"));

        Assertions.assertTrue(query.getWhere().hasFieldFilter());
        final StructuredQuery.FieldFilter filter = query.getWhere().getFieldFilter();
        Assertions.assertEquals("longvalue", filter.getField().getFieldPath());
        Assertions.assertEquals(StructuredQuery.FieldFilter.Operator.GREATER_THAN_OR_EQUAL, filter.getOp());
        Assertions.assertEquals(2L, filter.getValue().getIntegerValue());
    }

    @Test
    public void testFilterFieldNameEndingWithS() {
        // a field name ending with "s" must not be eaten by the operator pattern
        final StructuredQuery query = FirestoreSource.createQuery(SCHEMA, createParameters("values=2"));

        Assertions.assertTrue(query.getWhere().hasFieldFilter());
        final StructuredQuery.FieldFilter filter = query.getWhere().getFieldFilter();
        Assertions.assertEquals("values", filter.getField().getFieldPath());
        Assertions.assertEquals(StructuredQuery.FieldFilter.Operator.EQUAL, filter.getOp());
        Assertions.assertEquals(2L, filter.getValue().getIntegerValue());

        // also with surrounding spaces
        final StructuredQuery query2 = FirestoreSource.createQuery(SCHEMA, createParameters("values = 2"));
        Assertions.assertEquals("values", query2.getWhere().getFieldFilter().getField().getFieldPath());
        Assertions.assertEquals(2L, query2.getWhere().getFieldFilter().getValue().getIntegerValue());
    }

    @Test
    public void testFilterFieldNamesContainingAndOrKeywords() {
        // field names containing "and" ("brand") or "or" ("order") must not be split as conditions
        final StructuredQuery query = FirestoreSource
                .createQuery(SCHEMA, createParameters("brand = \"x\" and order >= 1"));

        Assertions.assertTrue(query.getWhere().hasCompositeFilter());
        final StructuredQuery.CompositeFilter compositeFilter = query.getWhere().getCompositeFilter();
        Assertions.assertEquals(StructuredQuery.CompositeFilter.Operator.AND, compositeFilter.getOp());
        Assertions.assertEquals(2, compositeFilter.getFiltersCount());

        final StructuredQuery.FieldFilter filter1 = compositeFilter.getFilters(0).getFieldFilter();
        Assertions.assertEquals("brand", filter1.getField().getFieldPath());
        Assertions.assertEquals(StructuredQuery.FieldFilter.Operator.EQUAL, filter1.getOp());
        Assertions.assertEquals("x", filter1.getValue().getStringValue());

        final StructuredQuery.FieldFilter filter2 = compositeFilter.getFilters(1).getFieldFilter();
        Assertions.assertEquals("order", filter2.getField().getFieldPath());
        Assertions.assertEquals(StructuredQuery.FieldFilter.Operator.GREATER_THAN_OR_EQUAL, filter2.getOp());
        Assertions.assertEquals(1L, filter2.getValue().getIntegerValue());
    }

}
