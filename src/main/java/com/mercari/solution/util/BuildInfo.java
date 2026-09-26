package com.mercari.solution.util;

import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

/**
 * The build this code came from: the git revision stamped into {@code git.properties} at build time
 * (git-commit-id-maven-plugin; see the pom). The same revision labels the Jib images
 * ({@code org.opencontainers.image.revision}) and opens the pipeline launcher's log, so the image a job ran
 * can be matched to a commit. Every accessor returns null when the build carried no git information.
 */
public class BuildInfo {

    private static volatile Properties properties;

    /** The abbreviated commit id, or null. */
    public static String revision() {
        return property("git.commit.id.abbrev");
    }

    /** The full commit id, or null. */
    public static String commitId() {
        return property("git.commit.id");
    }

    /** The branch the build was made from, or null. */
    public static String branch() {
        return property("git.branch");
    }

    /** The commit's time as the plugin formats it, or null. */
    public static String commitTime() {
        return property("git.commit.time");
    }

    /** One line for a log: {@code <abbrev> (branch <name>, committed <time>)}, or "unknown (no git.properties)". */
    public static String describe() {
        final String revision = revision();
        if (revision == null) return "unknown (no git.properties)";
        final StringBuilder sb = new StringBuilder(revision);
        final String branch = branch(), time = commitTime();
        if (branch != null || time != null) {
            sb.append(" (");
            if (branch != null) sb.append("branch ").append(branch);
            if (time != null) sb.append(branch != null ? ", " : "").append("committed ").append(time);
            sb.append(")");
        }
        return sb.toString();
    }

    private static String property(final String name) {
        return Optional.ofNullable(load().getProperty(name)).map(String::trim).filter(v -> !v.isEmpty()).orElse(null);
    }

    private static Properties load() {
        Properties p = properties;
        if (p != null) return p;
        synchronized (BuildInfo.class) {
            if (properties != null) return properties;
            p = new Properties();
            try (final InputStream is = BuildInfo.class.getResourceAsStream("/git.properties")) {
                if (is != null) p.load(is);
            } catch (final Exception e) {
                // a build without git information is not an error
            }
            properties = p;
            return p;
        }
    }

}
