package com.mercari.solution.util;

import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

/**
 * The build this code came from: the git revision stamped into {@code git.properties} at build time
 * (git-commit-id-maven-plugin; see the pom). The same revision labels the Jib images
 * ({@code org.opencontainers.image.revision}) and opens the pipeline launcher's log, so the image a job ran
 * can be matched to a commit. Every accessor returns null when the build carried no git information
 * (a build outside a git checkout still writes git.properties, without any of these keys).
 */
public final class BuildInfo {

    static final String UNKNOWN = "unknown (no git information)";

    private BuildInfo() {
    }

    /** Loaded once, on first use (class-holder idiom). */
    private static final class Holder {
        static final Properties PROPERTIES = load();
    }

    /** The abbreviated commit id, or null. */
    public static String revision() {
        return property(Holder.PROPERTIES, "git.commit.id.abbrev");
    }

    /** The full commit id, or null. */
    public static String commitId() {
        return property(Holder.PROPERTIES, "git.commit.id");
    }

    /** The branch the build was made from, or null. */
    public static String branch() {
        return property(Holder.PROPERTIES, "git.branch");
    }

    /** The commit's time as the plugin formats it, or null. */
    public static String commitTime() {
        return property(Holder.PROPERTIES, "git.commit.time");
    }

    /** One line for a log: {@code <abbrev> (branch <name>, committed <time>)}, or {@value #UNKNOWN}. */
    public static String describe() {
        return describe(Holder.PROPERTIES);
    }

    static String describe(final Properties properties) {
        final String revision = property(properties, "git.commit.id.abbrev");
        if (revision == null) return UNKNOWN;
        final StringBuilder sb = new StringBuilder(revision);
        final String branch = property(properties, "git.branch");
        final String time = property(properties, "git.commit.time");
        if (branch != null || time != null) {
            sb.append(" (");
            if (branch != null) sb.append("branch ").append(branch);
            if (time != null) sb.append(branch != null ? ", " : "").append("committed ").append(time);
            sb.append(")");
        }
        return sb.toString();
    }

    private static String property(final Properties properties, final String name) {
        return Optional.ofNullable(properties.getProperty(name)).map(String::trim).filter(v -> !v.isEmpty()).orElse(null);
    }

    private static Properties load() {
        final Properties p = new Properties();
        try (final InputStream is = BuildInfo.class.getResourceAsStream("/git.properties")) {
            if (is != null) p.load(is);
        } catch (final Exception e) {
            // a build without git information is not an error
        }
        return p;
    }

}
