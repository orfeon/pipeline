package com.mercari.solution.server.launch;

import com.mercari.solution.config.Options;
import com.mercari.solution.util.cloud.google.IAMUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Resolves the values a launcher needs (project, region, service account, ...) from, in order:
 * <ol>
 *   <li>the launch request parameters (UI input),</li>
 *   <li>the config {@code options} (runner-specific block first, then the common {@code options.gcp}),</li>
 *   <li>the environment: {@code MERCARI_PIPELINE_LAUNCH_<RUNNER>_<KEY>} then {@code MERCARI_PIPELINE_LAUNCH_<KEY>}
 *       (legacy {@code MERCARI_PIPELINE_DATAFLOW_*} / {@code MERCARI_PIPELINE_TEMP_LOCATION} names are still read,
 *       with a deprecation warning),</li>
 *   <li>the runtime environment the server runs in: {@code GOOGLE_CLOUD_PROJECT} and the GCE metadata server
 *       (project, region, default service account).</li>
 * </ol>
 * This is the only place in the server that reads launch-related environment variables.
 */
public class LaunchDefaults {

    private static final Logger LOG = LoggerFactory.getLogger(LaunchDefaults.class);

    public static final String ENV_PREFIX = "MERCARI_PIPELINE_LAUNCH_";
    public static final String ENV_GOOGLE_CLOUD_PROJECT = "GOOGLE_CLOUD_PROJECT";

    public static final String KEY_PROJECT = "PROJECT";
    public static final String KEY_REGION = "REGION";
    public static final String KEY_SERVICE_ACCOUNT = "SERVICE_ACCOUNT";
    public static final String KEY_SUBNETWORK = "SUBNETWORK";
    public static final String KEY_STAGING_LOCATION = "STAGING_LOCATION";
    public static final String KEY_TEMP_LOCATION = "TEMP_LOCATION";
    public static final String KEY_LABELS = "LABELS";

    /** Legacy env var names, keyed by "<runner>/<key>" or "<key>" (common). Read as a fallback with a warning. */
    private static final Map<String, String> LEGACY = Map.of(
            KEY_PROJECT, "MERCARI_PIPELINE_DATAFLOW_PROJECT",
            KEY_REGION, "MERCARI_PIPELINE_DATAFLOW_REGION",
            KEY_SERVICE_ACCOUNT, "MERCARI_PIPELINE_DATAFLOW_SERVICE_ACCOUNT",
            KEY_SUBNETWORK, "MERCARI_PIPELINE_DATAFLOW_SUBNETWORK",
            KEY_STAGING_LOCATION, "MERCARI_PIPELINE_DATAFLOW_STAGING_LOCATION",
            KEY_TEMP_LOCATION, "MERCARI_PIPELINE_TEMP_LOCATION",
            "dataflow/TEMPLATE_LOCATION", "MERCARI_PIPELINE_DATAFLOW_TEMPLATE_LOCATION");

    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet();

    private static volatile LaunchDefaults instance;

    private final Map<String, String> env;
    private final Supplier<String> metadataProject;
    private final Supplier<String> metadataRegion;
    private final Supplier<String> metadataServiceAccount;

    // Metadata lookups are cached for the lifetime of this instance (they never change on one host).
    private final Map<String, Optional<String>> metadataCache = new ConcurrentHashMap<>();

    public LaunchDefaults(
            final Map<String, String> env,
            final Supplier<String> metadataProject,
            final Supplier<String> metadataRegion,
            final Supplier<String> metadataServiceAccount) {
        this.env = env;
        this.metadataProject = metadataProject;
        this.metadataRegion = metadataRegion;
        this.metadataServiceAccount = metadataServiceAccount;
    }

    /** Defaults backed by the real process environment and the GCE metadata server. */
    public static LaunchDefaults get() {
        LaunchDefaults cached = instance;
        if(cached == null) {
            synchronized (LaunchDefaults.class) {
                if(instance == null) {
                    instance = new LaunchDefaults(
                            System.getenv(),
                            IAMUtil::getMetadataProject,
                            IAMUtil::getMetadataRegion,
                            IAMUtil::getMetadataServiceAccount);
                }
                cached = instance;
            }
        }
        return cached;
    }

    /** Test hook: defaults from a fixed env map, with no metadata server. */
    public static LaunchDefaults of(final Map<String, String> env) {
        return new LaunchDefaults(env, () -> null, () -> null, () -> null);
    }

    /** {@code MERCARI_PIPELINE_LAUNCH_<RUNNER>_<KEY>} */
    public static String envName(final String runner, final String key) {
        return ENV_PREFIX + runner.toUpperCase(Locale.ROOT) + "_" + key;
    }

    /** {@code MERCARI_PIPELINE_LAUNCH_<KEY>} */
    public static String envName(final String key) {
        return ENV_PREFIX + key;
    }

    /**
     * Resolve a runner-specific or common key from the environment only (steps 3-4).
     * Callers supply explicit values (request parameters, config options) via {@link #resolve}.
     */
    public String fromEnv(final String runner, final String key) {
        String value = env(envName(runner, key));
        if(value == null) {
            value = env(envName(key));
        }
        if(value == null) {
            value = legacy(runner + "/" + key);
        }
        if(value == null) {
            value = legacy(key);
        }
        if(value == null) {
            value = fromRuntime(key);
        }
        return value;
    }

    /**
     * Full resolution: the first non-blank of {@code explicit} values (request parameter, then config options),
     * then the environment, then the runtime environment.
     */
    public Optional<String> resolve(final String runner, final String key, final String... explicit) {
        for(final String value : explicit) {
            if(value != null && !value.isBlank()) {
                return Optional.of(value.trim());
            }
        }
        return Optional.ofNullable(fromEnv(runner, key));
    }

    /** Like {@link #resolve} but fails with a message naming the env vars to set. */
    public String require(final String runner, final String key, final String... explicit) {
        return resolve(runner, key, explicit).orElseThrow(() -> new IllegalArgumentException(
                "Could not resolve " + key.toLowerCase(Locale.ROOT).replace('_', ' ') + " for the " + runner
                        + " launch: specify it in the launch parameters or set the environment variable "
                        + envName(runner, key) + " (or " + envName(key) + ")"));
    }

    /** Project from the config options: the runner block (Dataflow) first, then {@code options.gcp.project}. */
    public static String optionsProject(final String runner, final Options options) {
        if(options == null) {
            return null;
        }
        if("dataflow".equals(runner) && options.getDataflow() != null && options.getDataflow().getProject() != null) {
            return options.getDataflow().getProject();
        }
        if(options.getGcp() != null) {
            return options.getGcp().getProject();
        }
        return null;
    }

    /** Region from the config options: {@code options.dataflow.region} (Dataflow) first, then {@code options.gcp.workerRegion}. */
    public static String optionsRegion(final String runner, final Options options) {
        if(options == null) {
            return null;
        }
        if("dataflow".equals(runner) && options.getDataflow() != null && options.getDataflow().getRegion() != null) {
            return options.getDataflow().getRegion();
        }
        if(options.getGcp() != null) {
            return options.getGcp().getWorkerRegion();
        }
        return null;
    }

    /** Additional labels from {@code MERCARI_PIPELINE_LAUNCH_LABELS} ({@code k=v,k=v}). */
    public Map<String, String> labels(final String runner) {
        final Map<String, String> labels = new HashMap<>();
        final String text = fromEnv(runner, KEY_LABELS);
        if(text == null) {
            return labels;
        }
        for(final String pair : text.split(",")) {
            final int eq = pair.indexOf('=');
            if(eq > 0) {
                labels.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
            }
        }
        return labels;
    }

    private String fromRuntime(final String key) {
        return switch (key) {
            case KEY_PROJECT -> Optional.ofNullable(env(ENV_GOOGLE_CLOUD_PROJECT))
                    .orElseGet(() -> metadata(KEY_PROJECT, metadataProject));
            case KEY_REGION -> metadata(KEY_REGION, metadataRegion);
            case KEY_SERVICE_ACCOUNT -> metadata(KEY_SERVICE_ACCOUNT, metadataServiceAccount);
            default -> null;
        };
    }

    private String metadata(final String key, final Supplier<String> supplier) {
        return metadataCache.computeIfAbsent(key, k -> {
            try {
                final String value = supplier.get();
                return Optional.ofNullable(value == null || value.isBlank() ? null : value.trim());
            } catch (final Throwable e) {
                LOG.warn("Failed to read {} from the metadata server: {}", key, e.getMessage());
                return Optional.empty();
            }
        }).orElse(null);
    }

    private String legacy(final String lookup) {
        final String legacyName = LEGACY.get(lookup);
        if(legacyName == null) {
            return null;
        }
        final String value = env(legacyName);
        if(value != null && WARNED.add(legacyName)) {
            final String key = lookup.substring(lookup.indexOf('/') + 1);
            final String replacement = lookup.contains("/")
                    ? envName(lookup.substring(0, lookup.indexOf('/')), key)
                    : envName(key);
            LOG.warn("Environment variable {} is deprecated; use {} instead", legacyName, replacement);
        }
        return value;
    }

    private String env(final String name) {
        final String value = env.get(name);
        return value == null || value.isBlank() ? null : value.trim();
    }

}
