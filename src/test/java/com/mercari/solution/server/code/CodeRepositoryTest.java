package com.mercari.solution.server.code;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CodeRepositoryTest {

    @Test
    public void testNormalizeSourcePath() {
        Assertions.assertEquals(
                "src/main/java/com/mercari/solution/MPipeline.java",
                CodeRepository.normalizeSourcePath("src/main/java/com/mercari/solution/MPipeline.java"));
        Assertions.assertEquals(
                "src/main/java/com/mercari/solution/MPipeline.java",
                CodeRepository.normalizeSourcePath("/src/main/java/com/mercari/solution/MPipeline.java"));
        Assertions.assertEquals(
                "src/main/java/com/mercari/solution/MPipeline.java",
                CodeRepository.normalizeSourcePath("src\\main\\java\\com\\mercari\\solution\\MPipeline.java"));

        // package-style paths get the source prefix prepended
        Assertions.assertEquals(
                "src/main/java/com/mercari/solution/MPipeline.java",
                CodeRepository.normalizeSourcePath("com/mercari/solution/MPipeline.java"));

        // '..' segments resolve within the tree
        Assertions.assertEquals(
                "src/main/java/com/mercari/solution/MPipeline.java",
                CodeRepository.normalizeSourcePath("src/main/java/com/mercari/solution/module/../MPipeline.java"));

        // escapes and non-java files are rejected
        Assertions.assertNull(CodeRepository.normalizeSourcePath("../pom.xml"));
        Assertions.assertNull(CodeRepository.normalizeSourcePath("src/main/java/../../../../secrets.java"));
        // resolving exactly back to the root cannot escape: the result stays inside src/main/java
        Assertions.assertEquals(
                "src/main/java/secrets.java",
                CodeRepository.normalizeSourcePath("src/main/java/../../../secrets.java"));
        Assertions.assertNull(CodeRepository.normalizeSourcePath("src/main/resources/log4j.properties"));
        Assertions.assertNull(CodeRepository.normalizeSourcePath(null));
        Assertions.assertNull(CodeRepository.normalizeSourcePath("  "));
    }

    @Test
    public void testSearch() {
        final String result = CodeRepository.search("class MPipeline", null);
        Assertions.assertTrue(
                result.contains("src/main/java/com/mercari/solution/MPipeline.java"), result);

        final String filtered = CodeRepository.search("class StorageSink", "module/sink");
        Assertions.assertTrue(
                filtered.contains("src/main/java/com/mercari/solution/module/sink/StorageSink.java"), filtered);

        final String excluded = CodeRepository.search("class StorageSink", "module/source");
        Assertions.assertTrue(excluded.startsWith("No matches found"), excluded);

        // invalid regex falls back to a literal search
        final String literal = CodeRepository.search("((", null);
        Assertions.assertFalse(literal.startsWith("Error"), literal);
    }

    @Test
    public void testRead() {
        final String result = CodeRepository.read("src/main/java/com/mercari/solution/MPipeline.java", 1, 20);
        Assertions.assertTrue(result.contains("package com.mercari.solution;"), result);
        Assertions.assertTrue(result.startsWith("## src/main/java/com/mercari/solution/MPipeline.java"), result);

        final String notFound = CodeRepository.read("src/main/java/com/mercari/solution/NoSuchClass.java", null, null);
        Assertions.assertTrue(notFound.startsWith("Source file not found"), notFound);

        final String invalid = CodeRepository.read("../../etc/passwd", null, null);
        Assertions.assertTrue(invalid.startsWith("Error"), invalid);
    }

    @Test
    public void testResolveStackTrace() {
        final String stackTrace = """
                java.lang.RuntimeException: Failed to setup module
                    at com.mercari.solution.module.sink.StorageSink.expand(StorageSink.java:100)
                    at com.mercari.solution.MPipeline.apply(MPipeline.java:50)
                    at org.apache.beam.sdk.Pipeline.run(Pipeline.java:325)
                """;
        final String result = CodeRepository.resolveStackTrace(stackTrace);
        Assertions.assertTrue(
                result.contains("### src/main/java/com/mercari/solution/module/sink/StorageSink.java:100"), result);
        Assertions.assertTrue(
                result.contains("### src/main/java/com/mercari/solution/MPipeline.java:50"), result);
        // non-framework frames are not resolved
        Assertions.assertFalse(result.contains("Pipeline.java:325"), result);

        final String none = CodeRepository.resolveStackTrace("no frames here");
        Assertions.assertTrue(none.startsWith("No stack frames"), none);
    }

    @Test
    public void testFindModuleSource() {
        final String result = CodeRepository.findModuleSource(CodeRepository.ModuleType.sink, "storage");
        Assertions.assertTrue(
                result.contains("src/main/java/com/mercari/solution/module/sink/StorageSink.java"), result);

        final String unknown = CodeRepository.findModuleSource(CodeRepository.ModuleType.sink, "nosuchmodule");
        Assertions.assertTrue(unknown.startsWith("No sink module named"), unknown);
        Assertions.assertTrue(unknown.contains("storage"), unknown);

        final String missingType = CodeRepository.findModuleSource(null, "storage");
        Assertions.assertTrue(missingType.startsWith("Error"), missingType);
    }

}
