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

    // Action services are listed under "actions" by their plain service name (the config's
    // actions[].module value), matching the Action registry.
    @Test
    public void testActionsListedUnderActionsByServiceName() {
        SpecService.init();
        final JsonObject modules = SpecService.getAllSchemasAsArrays().getAsJsonObject("modules");
        final JsonArray actions = modules.getAsJsonArray("actions");
        Assertions.assertFalse(actions.isEmpty());
        final var serviceNames = com.mercari.solution.module.Action.serviceNames();
        for (final var e : actions) {
            final String name = e.getAsJsonObject().get("name").getAsString();
            Assertions.assertFalse(name.contains("."), "actions contains " + name);
            Assertions.assertTrue(serviceNames.contains(name), "unknown action service in index.yaml: " + name);
        }
    }
}
