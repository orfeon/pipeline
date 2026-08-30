package com.mercari.solution.server.launch;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DataflowFlexTemplateLauncherTest {

    @Test
    public void testJobNameResolutionAndNormalisation() {
        // explicit launch parameter wins and is normalised to Dataflow's rules
        Assertions.assertEquals("my-feature-job", DataflowFlexTemplateLauncher.jobName("My_Feature Job", "opt", "cfg"));
        // then options.jobName
        Assertions.assertEquals("feature-backfill", DataflowFlexTemplateLauncher.jobName(null, "feature_backfill", "cfg"));
        Assertions.assertEquals("feature-backfill", DataflowFlexTemplateLauncher.jobName(" ", "Feature.Backfill.", "cfg"));
        // then the config name with a timestamp suffix (repeated launches must not collide)
        final String derived = DataflowFlexTemplateLauncher.jobName(null, null, "Race Features v2");
        Assertions.assertTrue(derived.matches("race-features-v2-[0-9]{8}-[0-9]{6}"), derived);
        final String fallback = DataflowFlexTemplateLauncher.jobName(null, null, null);
        Assertions.assertTrue(fallback.startsWith("mercari-pipeline-"), fallback);
        // every result satisfies the API constraint
        for (final String name : new String[]{"123abc", "___", "-x-", "A".repeat(100) + "_end"}) {
            final String sanitized = DataflowFlexTemplateLauncher.sanitizeJobName(name);
            Assertions.assertTrue(sanitized.matches("[a-z][-a-z0-9]*[a-z0-9]|[a-z]"), name + " -> " + sanitized);
            Assertions.assertTrue(sanitized.length() <= 63, sanitized);
        }
        Assertions.assertEquals("abc", DataflowFlexTemplateLauncher.sanitizeJobName("123abc"));
        Assertions.assertEquals("job", DataflowFlexTemplateLauncher.sanitizeJobName("___"));
        final String longDerived = DataflowFlexTemplateLauncher.jobName(null, null, "x".repeat(200));
        Assertions.assertTrue(longDerived.length() <= 63, longDerived);
        Assertions.assertTrue(longDerived.matches("x+-[0-9]{8}-[0-9]{6}"), longDerived);
    }

}
