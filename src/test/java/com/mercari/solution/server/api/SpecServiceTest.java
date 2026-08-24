package com.mercari.solution.server.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SpecServiceTest {

    // Regression: an unquoted description containing ": " in module/index.yaml broke
    // SnakeYAML parsing and made every /api/spec request fail at servlet init.
    @Test
    public void testModuleIndexLoads() {
        SpecService.init();
        for (final String type : new String[]{"source", "transform", "sink"}) {
            final JsonArray modules = SpecService.getModuleAbstracts(type);
            Assertions.assertNotNull(modules, type);
            Assertions.assertFalse(modules.isEmpty(), type);
        }
    }

    // Action modules are listed once under "actions" and no longer duplicated into
    // sources/transforms/sinks (the Builder UI shows them as their own group).
    @Test
    public void testActionsListedOnlyUnderActions() {
        SpecService.init();
        final JsonObject modules = SpecService.getAllSchemasAsArrays().getAsJsonObject("modules");
        final JsonArray actions = modules.getAsJsonArray("actions");
        Assertions.assertFalse(actions.isEmpty());
        for (final String type : new String[]{"sources", "transforms", "sinks"}) {
            for (final var e : modules.getAsJsonArray(type)) {
                final String name = e.getAsJsonObject().get("name").getAsString();
                Assertions.assertFalse(name.startsWith("action."), type + " contains " + name);
            }
        }
    }
}
