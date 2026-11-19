package com.mercari.solution.server.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FeatureServiceTest {

    @Test
    public void testPlanErrors() {
        final JsonArray ok = JsonParser.parseString("""
                [{"name": "a", "ok": true, "describe": "..."}, {"name": "b", "ok": true}]
                """).getAsJsonArray();
        Assertions.assertEquals(List.of(), FeatureService.planErrors(ok));

        // every reason a plan carries is reported under its step; a plan that only says "not ok" still fails the run
        final JsonArray failed = JsonParser.parseString("""
                [
                  {"name": "a", "ok": true},
                  {"name": "b", "ok": false, "error": "sources is required"},
                  {"name": "c", "ok": false, "engineErrors": ["no streaming"], "errors": ["[reference.unresolved] features.x: unknown field y"]},
                  {"name": "d", "ok": false}
                ]
                """).getAsJsonArray();
        Assertions.assertEquals(List.of(
                "b: sources is required",
                "c: no streaming",
                "c: [reference.unresolved] features.x: unknown field y",
                "d: the feature plan did not compile"), FeatureService.planErrors(failed));
    }

}
