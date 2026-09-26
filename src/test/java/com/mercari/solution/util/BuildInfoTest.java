package com.mercari.solution.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BuildInfoTest {

    @Test
    public void testDescribeFollowsTheStampedRevision() {
        final String describe = BuildInfo.describe();
        Assertions.assertNotNull(describe);
        final String revision = BuildInfo.revision();
        if (revision == null) {
            // a build without a git checkout: no revision, and the line says so
            Assertions.assertEquals("unknown (no git.properties)", describe);
            Assertions.assertNull(BuildInfo.commitId());
        } else {
            Assertions.assertTrue(describe.startsWith(revision), describe);
            Assertions.assertFalse(revision.isBlank());
            // the full id starts with the abbreviated one when both were stamped
            final String full = BuildInfo.commitId();
            if (full != null) Assertions.assertTrue(full.startsWith(revision), full + " vs " + revision);
        }
    }

}
