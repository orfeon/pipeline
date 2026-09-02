package com.mercari.solution.server.launch;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.server.api.LaunchService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LaunchSchemaTest {

    private static JsonObject schema() {
        try(final InputStream is = LaunchSchemaTest.class.getClassLoader()
                .getResourceAsStream("server/schema/server/api/spec/launch.json")) {
            Assertions.assertNotNull(is, "launch.json on the classpath");
            final JsonElement schema = new Gson().fromJson(new InputStreamReader(is, StandardCharsets.UTF_8), JsonElement.class);
            Assertions.assertTrue(schema.isJsonObject());
            return schema.getAsJsonObject();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    public void testSchemaDeclaresExactlyTheRegisteredLaunchers() {
        final Set<String> declared = new HashSet<>();
        LaunchSchema.keys(schema()).forEach(e -> declared.add(e.getAsString()));
        final Set<String> registered = new HashSet<>(LaunchService.launcherKeys());
        // flink is declared (hidden) but has no launcher yet
        declared.remove("flink");
        Assertions.assertEquals(registered, declared);
    }

    @Test
    public void testDispatch() {
        Assertions.assertEquals("dataflow/flexTemplate", LaunchService.findLauncher("dataflow", null).key());
        Assertions.assertEquals("dataflow/flexTemplate", LaunchService.findLauncher("dataflowTemplate", null).key());
        Assertions.assertEquals("dataflow/inProcess", LaunchService.findLauncher("dataflow", "inProcess").key());
        Assertions.assertEquals("direct/cloudRunJob", LaunchService.findLauncher("direct", null).key());
        Assertions.assertEquals("direct/cloudRunWorkerPool", LaunchService.findLauncher("direct", "cloudRunWorkerPool").key());
        Assertions.assertEquals("prism/cloudRunJob", LaunchService.findLauncher("prism", null).key());
        Assertions.assertEquals("prism/cloudRunWorkerPool", LaunchService.findLauncher("prism", "cloudRunWorkerPool").key());
        Assertions.assertEquals("spark/dataprocServerless", LaunchService.findLauncher("spark", null).key());
        Assertions.assertThrows(IllegalArgumentException.class, () -> LaunchService.findLauncher("flink", null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> LaunchService.findLauncher("direct", "gce"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> LaunchService.findLauncher(null, null));
    }

    @Test
    public void testHintsInjectedFromEnvironment() {
        final LaunchDefaults defaults = LaunchDefaults.of(Map.of(
                "MERCARI_PIPELINE_LAUNCH_PROJECT", "env-project",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_REGION", "asia-northeast1",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_JOB", "mp-job",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_TASK_TIMEOUT", "1800",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_INSTANCES", "3",
                "MERCARI_PIPELINE_LAUNCH_PRISM_JOB", "mp-job-prism",
                "MERCARI_PIPELINE_DATAFLOW_TEMPLATE_LOCATION", "gs://legacy/template.json"));
        final JsonObject original = schema();
        final JsonObject filled = LaunchSchema.withDefaults(original, defaults);
        Assertions.assertNotSame(original, filled);

        final String HINT = LaunchSchema.X_DEFAULT_HINT;
        final JsonObject dataflow = runner(filled, "dataflow");
        Assertions.assertEquals("env-project", dataflow.getAsJsonObject("properties").getAsJsonObject("project").get(HINT).getAsString());
        // hints never become defaults: the form must submit an empty value so config options win over env
        Assertions.assertFalse(dataflow.getAsJsonObject("properties").getAsJsonObject("project").has("default"));
        Assertions.assertFalse(dataflow.getAsJsonObject("properties").getAsJsonObject("region").has(HINT));
        final JsonObject flex = environment(dataflow, "flexTemplate");
        Assertions.assertEquals("gs://legacy/template.json",
                flex.getAsJsonObject("properties").getAsJsonObject("templateLocation").get(HINT).getAsString());

        final JsonObject direct = runner(filled, "direct");
        Assertions.assertEquals("asia-northeast1", direct.getAsJsonObject("properties").getAsJsonObject("region").get(HINT).getAsString());
        final JsonObject job = environment(direct, "cloudRunJob");
        Assertions.assertEquals("mp-job", job.getAsJsonObject("properties").getAsJsonObject("jobName").get(HINT).getAsString());
        Assertions.assertEquals("1800", job.getAsJsonObject("properties").getAsJsonObject("taskTimeout").get(HINT).getAsString());
        final JsonObject pool = environment(direct, "cloudRunWorkerPool");
        Assertions.assertEquals("3", pool.getAsJsonObject("properties").getAsJsonObject("instances").get(HINT).getAsString());
        Assertions.assertEquals(1, pool.getAsJsonObject("properties").getAsJsonObject("instances").get("default").getAsInt());
        Assertions.assertFalse(pool.getAsJsonObject("properties").getAsJsonObject("image").has(HINT));

        // prism resolves its own runner-specific keys and falls back to the common ones (project), never to _DIRECT_
        final JsonObject prism = runner(filled, "prism");
        Assertions.assertEquals("env-project", prism.getAsJsonObject("properties").getAsJsonObject("project").get(HINT).getAsString());
        Assertions.assertFalse(prism.getAsJsonObject("properties").getAsJsonObject("region").has(HINT));
        final JsonObject prismJob = environment(prism, "cloudRunJob");
        Assertions.assertEquals("mp-job-prism", prismJob.getAsJsonObject("properties").getAsJsonObject("jobName").get(HINT).getAsString());
        Assertions.assertFalse(prismJob.getAsJsonObject("properties").getAsJsonObject("taskTimeout").has(HINT));
        Assertions.assertFalse(environment(prism, "cloudRunWorkerPool").getAsJsonObject("properties").getAsJsonObject("instances").has(HINT));

        // the classpath schema itself is untouched
        Assertions.assertFalse(runner(original, "direct").getAsJsonObject("properties").getAsJsonObject("region").has(HINT));
    }

    @Test
    public void testRequiredOnlyWhereTheServerHasNoConfigFallback() {
        final JsonObject schema = schema();
        // templateLocation can come from options.dataflow.templateLocation, which the UI cannot see
        Assertions.assertFalse(environment(runner(schema, "dataflow"), "flexTemplate").has("required"));
        Assertions.assertEquals("jobName", environment(runner(schema, "direct"), "cloudRunJob").getAsJsonArray("required").get(0).getAsString());
        Assertions.assertEquals("image", environment(runner(schema, "direct"), "cloudRunWorkerPool").getAsJsonArray("required").get(0).getAsString());
        Assertions.assertEquals("jobName", environment(runner(schema, "prism"), "cloudRunJob").getAsJsonArray("required").get(0).getAsString());
        Assertions.assertEquals("image", environment(runner(schema, "prism"), "cloudRunWorkerPool").getAsJsonArray("required").get(0).getAsString());
    }

    @Test
    public void testArgsMapKeepsStringsUnquoted() {
        final JsonObject args = new JsonObject();
        args.addProperty("table", "orders");
        args.addProperty("n", 3);
        args.add("filter", new Gson().fromJson("{\"a\":1}", JsonObject.class));
        final Map<String, String> map = LaunchService.argsMap(args);
        Assertions.assertEquals("orders", map.get("table"));
        Assertions.assertEquals("3", map.get("n"));
        Assertions.assertEquals("{\"a\":1}", map.get("filter"));
    }

    @Test
    public void testHiddenTargets() {
        final JsonObject schema = schema();
        Assertions.assertTrue(runner(schema, "flink").get(LaunchSchema.X_HIDDEN).getAsBoolean());
        Assertions.assertTrue(environment(runner(schema, "dataflow"), "inProcess").get(LaunchSchema.X_HIDDEN).getAsBoolean());
        for(final String visible : List.of("dataflow", "direct", "prism", "spark")) {
            Assertions.assertFalse(runner(schema, visible).has(LaunchSchema.X_HIDDEN), visible);
        }
    }

    private static JsonObject runner(final JsonObject schema, final String name) {
        for(final JsonElement element : schema.getAsJsonArray("oneOf")) {
            if(name.equals(LaunchSchema.idSuffix(element.getAsJsonObject()))) {
                return element.getAsJsonObject();
            }
        }
        throw new AssertionError("runner not found: " + name);
    }

    private static JsonObject environment(final JsonObject runner, final String name) {
        for(final JsonElement element : runner.getAsJsonArray("oneOf")) {
            if(name.equals(LaunchSchema.idSuffix(element.getAsJsonObject()))) {
                return element.getAsJsonObject();
            }
        }
        throw new AssertionError("environment not found: " + name);
    }

}
