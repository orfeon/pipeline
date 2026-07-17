package com.mercari.solution.server;

import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

/**
 * Build version of this server, used to label launched Dataflow jobs and to detect version skew
 * between the server's bundled sources and a running job. Resolution order:
 * MERCARI_PIPELINE_VERSION env var, then git.properties generated at build time, else null.
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
        try (final InputStream is = ServerVersion.class.getResourceAsStream("/git.properties")) {
            if (is == null) {
                return null;
            }
            final Properties properties = new Properties();
            properties.load(is);
            return Optional.ofNullable(properties.getProperty("git.commit.id.abbrev"))
                    .filter(v -> !v.isBlank())
                    .orElse(null);
        } catch (final Exception e) {
            return null;
        }
    }

}
