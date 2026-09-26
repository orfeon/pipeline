package com.mercari.solution.server;

import com.mercari.solution.util.BuildInfo;

/**
 * Build version of this server, used to label launched Dataflow jobs and to detect version skew
 * between the server's bundled sources and a running job. Resolution order:
 * MERCARI_PIPELINE_VERSION env var, then git.properties generated at build time ({@link BuildInfo}), else null.
 */
public class ServerVersion {

    private static volatile String version;
    private static volatile boolean resolved;

    public static String get() {
        if (resolved) {
            return version;
        }
        synchronized (ServerVersion.class) {
            if (resolved) {
                return version;
            }
            version = resolve();
            resolved = true;
            return version;
        }
    }

    private static String resolve() {
        final String env = System.getenv("MERCARI_PIPELINE_VERSION");
        if (env != null && !env.isBlank()) {
            return env.trim();
        }
        return BuildInfo.revision();
    }

}
