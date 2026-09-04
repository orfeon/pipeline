package com.mercari.solution.config.options;

import com.mercari.solution.config.Options;
import com.mercari.solution.util.domain.file.JsonUtil;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class GCPOptionsTest {

    @Test
    public void testResolveProjectKeepsExplicitValue() {
        final AtomicInteger calls = new AtomicInteger();
        Assertions.assertEquals("explicit", GCPOptions.resolveProject("explicit", () -> {
            calls.incrementAndGet();
            return "env";
        }));
        Assertions.assertEquals(0, calls.get(), "the environment is not consulted when the project is set");
    }

    @Test
    public void testResolveProjectFallsBackToEnvironment() {
        Assertions.assertEquals("env", GCPOptions.resolveProject(null, () -> "env"));
        Assertions.assertEquals("env", GCPOptions.resolveProject("  ", () -> "env"));
        Assertions.assertNull(GCPOptions.resolveProject(null, () -> null));
        Assertions.assertNull(GCPOptions.resolveProject(null, () -> ""));
    }

    @Test
    public void testConfiguredProjectSurvivesDefaulting() {
        final Options options = JsonUtil.fromJson("""
                {
                  "gcp": {
                    "project": "configured-project"
                  }
                }
                """, Options.class);
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Options.setOptions(pipelineOptions, options);
        Assertions.assertEquals("configured-project", pipelineOptions.as(GcpOptions.class).getProject());
    }

    @Test
    public void testApplyDefaultProjectIsIdempotent() {
        final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        GCPOptions.applyDefaultProject(pipelineOptions);
        final String first = pipelineOptions.as(GcpOptions.class).getProject();
        GCPOptions.applyDefaultProject(pipelineOptions);
        // whatever the environment resolves to (gcloud, GOOGLE_CLOUD_PROJECT, metadata server, or nothing)
        // a second application must not change it
        Assertions.assertEquals(first, pipelineOptions.as(GcpOptions.class).getProject());
    }

}
