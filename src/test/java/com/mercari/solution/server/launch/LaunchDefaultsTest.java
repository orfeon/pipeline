package com.mercari.solution.server.launch;

import com.mercari.solution.config.Config;
import com.mercari.solution.config.Options;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LaunchDefaultsTest {

    @Test
    public void testPrecedenceExplicitBeforeEnv() {
        final LaunchDefaults defaults = LaunchDefaults.of(Map.of(
                "MERCARI_PIPELINE_LAUNCH_PROJECT", "env-project"));
        Assertions.assertEquals(Optional.of("ui-project"),
                defaults.resolve("direct", LaunchDefaults.KEY_PROJECT, "ui-project", "options-project"));
        Assertions.assertEquals(Optional.of("options-project"),
                defaults.resolve("direct", LaunchDefaults.KEY_PROJECT, " ", "options-project"));
        Assertions.assertEquals(Optional.of("env-project"),
                defaults.resolve("direct", LaunchDefaults.KEY_PROJECT, null, null));
    }

    @Test
    public void testRunnerSpecificBeforeCommon() {
        final LaunchDefaults defaults = LaunchDefaults.of(Map.of(
                "MERCARI_PIPELINE_LAUNCH_REGION", "us-central1",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_REGION", "asia-northeast1"));
        Assertions.assertEquals("asia-northeast1", defaults.fromEnv("direct", LaunchDefaults.KEY_REGION));
        Assertions.assertEquals("us-central1", defaults.fromEnv("dataflow", LaunchDefaults.KEY_REGION));
        Assertions.assertEquals("us-central1", defaults.fromEnv("spark", LaunchDefaults.KEY_REGION));
    }

    @Test
    public void testLegacyAliases() {
        final LaunchDefaults defaults = LaunchDefaults.of(Map.of(
                "MERCARI_PIPELINE_DATAFLOW_PROJECT", "legacy-project",
                "MERCARI_PIPELINE_DATAFLOW_TEMPLATE_LOCATION", "gs://legacy/template.json",
                "MERCARI_PIPELINE_TEMP_LOCATION", "gs://legacy/temp"));
        // legacy project/region/temp names are common defaults (they were the only launch target before)
        Assertions.assertEquals("legacy-project", defaults.fromEnv("dataflow", LaunchDefaults.KEY_PROJECT));
        Assertions.assertEquals("legacy-project", defaults.fromEnv("direct", LaunchDefaults.KEY_PROJECT));
        // Dataflow-shaped values (worker SA, subnetwork, staging) never leak into other runners
        final LaunchDefaults dataflowOnly = LaunchDefaults.of(Map.of(
                "MERCARI_PIPELINE_DATAFLOW_SERVICE_ACCOUNT", "df-worker@p.iam.gserviceaccount.com",
                "MERCARI_PIPELINE_DATAFLOW_SUBNETWORK", "regions/r/subnetworks/df",
                "MERCARI_PIPELINE_DATAFLOW_STAGING_LOCATION", "gs://df/staging"));
        Assertions.assertEquals("df-worker@p.iam.gserviceaccount.com", dataflowOnly.fromEnv("dataflow", LaunchDefaults.KEY_SERVICE_ACCOUNT));
        Assertions.assertNull(dataflowOnly.fromEnv("direct", LaunchDefaults.KEY_SERVICE_ACCOUNT));
        Assertions.assertNull(dataflowOnly.fromEnv("direct", LaunchDefaults.KEY_SUBNETWORK));
        Assertions.assertNull(dataflowOnly.fromEnv("direct", LaunchDefaults.KEY_STAGING_LOCATION));
        Assertions.assertEquals("gs://df/staging", dataflowOnly.fromEnv("dataflow", LaunchDefaults.KEY_STAGING_LOCATION));
        Assertions.assertEquals("gs://legacy/template.json",
                defaults.fromEnv("dataflow", DataflowFlexTemplateLauncher.KEY_TEMPLATE_LOCATION));
        Assertions.assertEquals("gs://legacy/temp", defaults.fromEnv("dataflow", LaunchDefaults.KEY_TEMP_LOCATION));
        // the new name wins over the legacy one
        final LaunchDefaults both = LaunchDefaults.of(Map.of(
                "MERCARI_PIPELINE_DATAFLOW_PROJECT", "legacy-project",
                "MERCARI_PIPELINE_LAUNCH_PROJECT", "new-project"));
        Assertions.assertEquals("new-project", both.fromEnv("dataflow", LaunchDefaults.KEY_PROJECT));
    }

    @Test
    public void testRuntimeFallbacks() {
        final LaunchDefaults googleCloudProject = LaunchDefaults.of(Map.of("GOOGLE_CLOUD_PROJECT", "gcp-project"));
        Assertions.assertEquals("gcp-project", googleCloudProject.fromEnv("direct", LaunchDefaults.KEY_PROJECT));

        final int[] calls = new int[1];
        final LaunchDefaults metadata = new LaunchDefaults(new HashMap<>(),
                () -> { calls[0]++; return "meta-project"; },
                () -> "asia-northeast1",
                () -> "sa@meta.iam.gserviceaccount.com");
        Assertions.assertEquals("meta-project", metadata.fromEnv("direct", LaunchDefaults.KEY_PROJECT));
        Assertions.assertEquals("meta-project", metadata.fromEnv("dataflow", LaunchDefaults.KEY_PROJECT));
        Assertions.assertEquals(1, calls[0], "metadata lookups are cached");
        Assertions.assertEquals("asia-northeast1", metadata.fromEnv("direct", LaunchDefaults.KEY_REGION));
        Assertions.assertEquals("sa@meta.iam.gserviceaccount.com", metadata.fromEnv("direct", LaunchDefaults.KEY_SERVICE_ACCOUNT));
        // keys without a runtime fallback stay unresolved
        Assertions.assertNull(metadata.fromEnv("direct", CloudRunJobLauncher.KEY_JOB));

        final LaunchDefaults failing = new LaunchDefaults(new HashMap<>(),
                () -> { throw new IllegalStateException("no metadata"); }, () -> null, () -> null);
        Assertions.assertNull(failing.fromEnv("direct", LaunchDefaults.KEY_PROJECT));
    }

    @Test
    public void testRequireMessageNamesEnvVars() {
        final LaunchDefaults defaults = LaunchDefaults.of(Map.of());
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> defaults.require("direct", LaunchDefaults.KEY_REGION));
        Assertions.assertTrue(e.getMessage().contains("MERCARI_PIPELINE_LAUNCH_DIRECT_REGION"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("MERCARI_PIPELINE_LAUNCH_REGION"), e.getMessage());
    }

    @Test
    public void testOptionsProjectAndRegion() throws Exception {
        final Config config = Config.load("""
                options:
                  gcp:
                    project: gcp-project
                    workerRegion: asia-northeast1
                  dataflow:
                    project: df-project
                    region: us-central1
                sources:
                  - name: in
                    module: create
                    parameters:
                      elements: [1]
                """, null, Config.Format.yaml, (String) null);
        final Options options = config.getOptions();
        Assertions.assertEquals("df-project", LaunchDefaults.optionsProject("dataflow", options));
        Assertions.assertEquals("us-central1", LaunchDefaults.optionsRegion("dataflow", options));
        // other runners use the common gcp options
        Assertions.assertEquals("gcp-project", LaunchDefaults.optionsProject("direct", options));
        Assertions.assertEquals("asia-northeast1", LaunchDefaults.optionsRegion("direct", options));
        Assertions.assertEquals("gcp-project", LaunchDefaults.optionsProject("spark", options));
        Assertions.assertNull(LaunchDefaults.optionsProject("direct", null));

        // a Dataflow-only config still resolves for other runners (last resort), and vice versa
        final Config dataflowOnly = Config.load("""
                options:
                  dataflow:
                    project: df-project
                    region: us-central1
                sources:
                  - name: in
                    module: create
                    parameters:
                      elements: [1]
                """, null, Config.Format.yaml, (String) null);
        Assertions.assertEquals("df-project", LaunchDefaults.optionsProject("spark", dataflowOnly.getOptions()));
        Assertions.assertEquals("us-central1", LaunchDefaults.optionsRegion("direct", dataflowOnly.getOptions()));
        final Config gcpOnly = Config.load("""
                options:
                  gcp:
                    project: gcp-project
                sources:
                  - name: in
                    module: create
                    parameters:
                      elements: [1]
                """, null, Config.Format.yaml, (String) null);
        Assertions.assertEquals("gcp-project", LaunchDefaults.optionsProject("dataflow", gcpOnly.getOptions()));
        Assertions.assertNull(LaunchDefaults.optionsRegion("dataflow", gcpOnly.getOptions()));
    }

    @Test
    public void testLabels() {
        final LaunchDefaults defaults = LaunchDefaults.of(Map.of(
                "MERCARI_PIPELINE_LAUNCH_LABELS", "team=data, env=prod"));
        final Map<String, String> labels = defaults.labels("direct");
        Assertions.assertEquals("data", labels.get("team"));
        Assertions.assertEquals("prod", labels.get("env"));
        Assertions.assertTrue(LaunchDefaults.of(Map.of()).labels("direct").isEmpty());
    }

}
