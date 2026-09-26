package com.mercari.solution.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class BuildInfoTest {

    @Test
    public void testDescribeFollowsTheStampedRevision() {
        final String describe = BuildInfo.describe();
        Assertions.assertNotNull(describe);
        final String revision = BuildInfo.revision();
        if (revision == null) {
            // a build without a git checkout: no revision, and the line says so
            Assertions.assertEquals(BuildInfo.UNKNOWN, describe);
            Assertions.assertNull(BuildInfo.commitId());
        } else {
            Assertions.assertTrue(describe.startsWith(revision), describe);
            Assertions.assertFalse(revision.isBlank());
            // the full id starts with the abbreviated one when both were stamped
            final String full = BuildInfo.commitId();
            if (full != null) Assertions.assertTrue(full.startsWith(revision), full + " vs " + revision);
        }
    }

    @Test
    public void testDescribeFormats() {
        // a build outside a git checkout: git.properties exists but carries no keys
        Assertions.assertEquals(BuildInfo.UNKNOWN, BuildInfo.describe(new Properties()));
        Assertions.assertEquals(BuildInfo.UNKNOWN, BuildInfo.describe(properties(" ", "main", null)));

        Assertions.assertEquals("abc1234", BuildInfo.describe(properties("abc1234", null, null)));
        Assertions.assertEquals("abc1234 (branch main)", BuildInfo.describe(properties("abc1234", "main", null)));
        Assertions.assertEquals("abc1234 (committed 2026-09-26T17:34:22+09:00)",
                BuildInfo.describe(properties("abc1234", null, "2026-09-26T17:34:22+09:00")));
        Assertions.assertEquals("abc1234 (branch main, committed 2026-09-26T17:34:22+09:00)",
                BuildInfo.describe(properties(" abc1234 ", "main", "2026-09-26T17:34:22+09:00")));
    }

    private static Properties properties(final String abbrev, final String branch, final String time) {
        final Properties properties = new Properties();
        if (abbrev != null) properties.setProperty("git.commit.id.abbrev", abbrev);
        if (branch != null) properties.setProperty("git.branch", branch);
        if (time != null) properties.setProperty("git.commit.time", time);
        return properties;
    }

}
