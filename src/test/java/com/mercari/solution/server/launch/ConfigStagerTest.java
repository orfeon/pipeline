package com.mercari.solution.server.launch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConfigStagerTest {

    @Test
    public void testInlineWhenNoStagingLocation() throws Exception {
        final ConfigStager stager = new ConfigStager((path, content) -> Assertions.fail("must not write"));
        final String value = stager.stage(null, "abc", "sources: []\n");
        Assertions.assertTrue(value.startsWith("data:"));
        Assertions.assertEquals("sources: []\n",
                new String(Base64.getDecoder().decode(value.substring(5)), StandardCharsets.UTF_8));
    }

    @Test
    public void testInlineTooLargeNamesStagingEnvVar() {
        final ConfigStager stager = new ConfigStager((path, content) -> Assertions.fail("must not write"));
        final String big = "x".repeat(ConfigStager.MAX_INLINE_BYTES + 1);
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> stager.stage("", "abc", big));
        Assertions.assertTrue(e.getMessage().contains("MERCARI_PIPELINE_LAUNCH_STAGING_LOCATION"), e.getMessage());
    }

    @Test
    public void testStageToGcs() throws Exception {
        final List<String[]> written = new ArrayList<>();
        final ConfigStager stager = new ConfigStager((path, content) -> written.add(new String[]{path, content}));
        final String yaml = stager.stage("gs://bucket/prefix", "id1", "sources: []\n");
        Assertions.assertTrue(yaml.matches("^gs://bucket/prefix/launch/\\d{4}/\\d{2}/\\d{2}/id1/config\\.yaml$"), yaml);
        final String json = stager.stage("gs://bucket/prefix/", "id2", "{\"sources\":[]}");
        Assertions.assertTrue(json.endsWith("/id2/config.json"), json);
        Assertions.assertEquals(2, written.size());
        Assertions.assertEquals(yaml, written.get(0)[0]);
        Assertions.assertEquals("sources: []\n", written.get(0)[1]);
        Assertions.assertThrows(IllegalArgumentException.class, () -> stager.stage("s3://bucket", "id3", "x"));
    }

    @Test
    public void testContainerArgs() {
        final Map<String, String> args = new LinkedHashMap<>();
        args.put("date", "2026-08-28");
        args.put("filter", "{\"a\":1}");
        Assertions.assertEquals(List.of("--config=gs://b/c.yaml", "--args.date=2026-08-28", "--args.filter={\"a\":1}"),
                ConfigStager.containerArgs("gs://b/c.yaml", args));
        Assertions.assertEquals(16, ConfigStager.newLaunchId().length());
    }

}
