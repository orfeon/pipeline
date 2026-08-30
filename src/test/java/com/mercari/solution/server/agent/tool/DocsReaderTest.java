package com.mercari.solution.server.agent.tool;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DocsReaderTest {

    private final DocsReader docsReader = DocsReader.create();

    @Test
    public void testNormalizeDocPath() {
        Assertions.assertEquals("module/common/filter.md", com.mercari.solution.server.mcp.tool.ReadDocsTool.normalizeDocPath("module/common/filter.md"));
        Assertions.assertEquals("module/common/filter.md", com.mercari.solution.server.mcp.tool.ReadDocsTool.normalizeDocPath("/module/common/filter.md"));
        Assertions.assertEquals("module/common/filter.md", com.mercari.solution.server.mcp.tool.ReadDocsTool.normalizeDocPath("./module/common/filter.md"));
        Assertions.assertEquals("module/common/filter.md", com.mercari.solution.server.mcp.tool.ReadDocsTool.normalizeDocPath("module\\common\\filter.md"));
        Assertions.assertEquals("system.md", com.mercari.solution.server.mcp.tool.ReadDocsTool.normalizeDocPath("system.md"));

        // '..' segments resolve within the docs root
        Assertions.assertEquals(
                "module/common/filter.md",
                com.mercari.solution.server.mcp.tool.ReadDocsTool.normalizeDocPath("module/transform/../common/filter.md"));

        // paths escaping the docs root are rejected
        Assertions.assertNull(com.mercari.solution.server.mcp.tool.ReadDocsTool.normalizeDocPath("../secrets.md"));
        Assertions.assertNull(com.mercari.solution.server.mcp.tool.ReadDocsTool.normalizeDocPath("module/../../secrets.md"));

        // only markdown files are allowed
        Assertions.assertNull(com.mercari.solution.server.mcp.tool.ReadDocsTool.normalizeDocPath("module/index.yaml"));
        Assertions.assertNull(com.mercari.solution.server.mcp.tool.ReadDocsTool.normalizeDocPath("module/common"));

        Assertions.assertNull(com.mercari.solution.server.mcp.tool.ReadDocsTool.normalizeDocPath(null));
        Assertions.assertNull(com.mercari.solution.server.mcp.tool.ReadDocsTool.normalizeDocPath("  "));
    }

    @Test
    public void testGetDocumentSharedDocs() {
        for (final String path : new String[]{
                "module/common/filter.md",
                "module/common/select.md",
                "module/common/strategy.md",
                "module/common/expression.md",
                "module/common/schema.md",
                "module/common/logging.md",
                "module/common/template.md",
                "system.md"}) {
            final String content = docsReader.getDocument(path);
            Assertions.assertFalse(content.startsWith("ERROR"), path + " -> " + content);
            Assertions.assertFalse(content.startsWith("Document not found"), path + " -> " + content);
            Assertions.assertFalse(content.isBlank(), path);
        }
    }

    @Test
    public void testGetDocumentNotFound() {
        final String content = docsReader.getDocument("module/common/no-such-doc.md");
        Assertions.assertTrue(content.contains("Document not found"), content);
    }

    @Test
    public void testGetDocumentInvalidPath() {
        Assertions.assertTrue(docsReader.getDocument("../outside.md").startsWith("ERROR"));
        Assertions.assertTrue(docsReader.getDocument("module/index.yaml").startsWith("ERROR"));
    }

}
