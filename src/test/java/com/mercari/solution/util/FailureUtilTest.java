package com.mercari.solution.util;

import com.google.gson.JsonObject;
import com.mercari.solution.module.IllegalModuleException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FailureUtilTest {

    private static final String QUERY = "SELECT a, b, c\nFROM `project.dataset.table`\nWHERE d = 1";

    /** The shape of a rejected query: a wrapper whose message is the whole query, around the reason. */
    private static RuntimeException queryFailure() {
        return new RuntimeException("Failed to dry run query: " + QUERY + ", for projectId: project",
                new IllegalStateException("Unrecognized name: d at [3:7]"));
    }

    @Test
    public void testRootCauseMessage() {
        Assertions.assertEquals("IllegalStateException: Unrecognized name: d at [3:7]", FailureUtil.rootCauseMessage(queryFailure()));
        Assertions.assertEquals("IllegalArgumentException: bad", FailureUtil.rootCauseMessage(new IllegalArgumentException("bad")));
        // no message: the exception itself
        Assertions.assertEquals("java.lang.NullPointerException", FailureUtil.rootCauseMessage(new NullPointerException()));
        // a root that says nothing: the deepest exception with a message is the reason
        Assertions.assertEquals("IllegalStateException: field x is unknown (caused by java.lang.NullPointerException)",
                FailureUtil.rootCauseMessage(new RuntimeException("outer", new IllegalStateException("field x is unknown", new NullPointerException()))));
    }

    @Test
    public void testErrorJsonSeparatesReasonContextAndStackTrace() {
        final JsonObject error = FailureUtil.errorJson(queryFailure());
        // the reason alone, first
        Assertions.assertEquals("IllegalStateException: Unrecognized name: d at [3:7]", error.get("message").getAsString());
        Assertions.assertEquals("message", error.keySet().iterator().next());
        // the context (the query text) is in the chain, outermost first
        Assertions.assertEquals(2, error.getAsJsonArray("causes").size());
        Assertions.assertTrue(error.getAsJsonArray("causes").get(0).getAsString().contains(QUERY));
        Assertions.assertTrue(error.getAsJsonArray("causes").get(1).getAsString().contains("Unrecognized name"));
        Assertions.assertTrue(error.get("stackTrace").getAsString().contains("FailureUtilTest"));
    }

    @Test
    public void testErrorJsonOfModuleFailure() {
        // a module whose assembly threw: the step, the reason as the only message, the rest apart
        final JsonObject thrown = FailureUtil.errorJson(new IllegalModuleException("input", "bigquery", queryFailure()));
        Assertions.assertEquals("input", thrown.get("name").getAsString());
        Assertions.assertEquals("bigquery", thrown.get("module").getAsString());
        Assertions.assertEquals("IllegalStateException: Unrecognized name: d at [3:7]", thrown.get("message").getAsString());
        Assertions.assertEquals(1, thrown.getAsJsonArray("messages").size());
        Assertions.assertEquals(thrown.get("message").getAsString(), thrown.getAsJsonArray("messages").get(0).getAsString());
        Assertions.assertTrue(thrown.has("causes") && thrown.has("stackTrace"), thrown::toString);

        // validation messages are the reason themselves: no stack trace to report
        final JsonObject validation = FailureUtil.errorJson(new IllegalModuleException("input", "bigquery", List.of("query is required", "table is required")));
        Assertions.assertEquals(2, validation.getAsJsonArray("messages").size());
        Assertions.assertEquals("query is required\ntable is required", validation.get("message").getAsString());
        Assertions.assertFalse(validation.has("stackTrace"), validation::toString);
    }

}
