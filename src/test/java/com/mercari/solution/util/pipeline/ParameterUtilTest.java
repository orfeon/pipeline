package com.mercari.solution.util.pipeline;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.values.KV;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ParameterUtilTest {

    @Test
    public void testGetSingleMultiAttribute() {
        final JsonObject single = new Gson().fromJson("""
                { "field": "a" }
                """, JsonObject.class);
        final KV<List<String>, Boolean> singleResult = ParameterUtil.getSingleMultiAttribute(single, "field", "fields");
        Assertions.assertEquals(List.of("a"), singleResult.getKey());
        Assertions.assertTrue(singleResult.getValue());

        final JsonObject multi = new Gson().fromJson("""
                { "fields": ["a", "b"] }
                """, JsonObject.class);
        final KV<List<String>, Boolean> multiResult = ParameterUtil.getSingleMultiAttribute(multi, "field", "fields");
        Assertions.assertEquals(List.of("a", "b"), multiResult.getKey());
        Assertions.assertFalse(multiResult.getValue());

        // single name takes precedence when both are present
        final JsonObject both = new Gson().fromJson("""
                { "field": "a", "fields": ["b"] }
                """, JsonObject.class);
        final KV<List<String>, Boolean> bothResult = ParameterUtil.getSingleMultiAttribute(both, "field", "fields");
        Assertions.assertEquals(List.of("a"), bothResult.getKey());
        Assertions.assertTrue(bothResult.getValue());
    }

    @Test
    public void testGetSingleMultiAttributeErrors() {
        final JsonObject missing = new Gson().fromJson("{}", JsonObject.class);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ParameterUtil.getSingleMultiAttribute(missing, "field", "fields"));

        final JsonObject singleNotPrimitive = new Gson().fromJson("""
                { "field": ["a"] }
                """, JsonObject.class);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ParameterUtil.getSingleMultiAttribute(singleNotPrimitive, "field", "fields"));

        final JsonObject multiNotArray = new Gson().fromJson("""
                { "fields": "a" }
                """, JsonObject.class);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ParameterUtil.getSingleMultiAttribute(multiNotArray, "field", "fields"));

        final JsonObject multiNotPrimitiveItem = new Gson().fromJson("""
                { "fields": [ { "a": 1 } ] }
                """, JsonObject.class);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ParameterUtil.getSingleMultiAttribute(multiNotPrimitiveItem, "field", "fields"));
    }

}
