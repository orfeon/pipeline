package com.mercari.solution.server.mcp;

/**
 * Caching hints (MCP 2026-07-28 {@code ttlMs} / {@code cacheScope}) of what this server publishes.
 * Tools, prompts, the docs tree and the docs themselves are bundled in the image: they are the same for
 * every caller and only change with a deployment, so they are published with scope {@code public}.
 * Tool results are never cacheable (they read live job state or run pipelines).
 */
public final class McpCaching {

    private McpCaching() {}

    /** {@code server/discover} and the tools / resources / prompts lists. */
    public static final long CATALOG_TTL_MS = 10 * 60 * 1000L;

    /** The content of a bundled document. */
    public static final long DOCUMENT_TTL_MS = 60 * 60 * 1000L;

}
