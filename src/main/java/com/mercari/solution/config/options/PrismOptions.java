package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;

public class PrismOptions implements Serializable {

    /**
     * The prism binary bundled in the {@code prism} image (set by the image build). It is the default for
     * {@code prismLocation} — applied only when neither the config ({@code options.prism.prismLocation}) nor the
     * command line ({@code --prismLocation}) set one, so either still wins; an explicitly empty config value
     * re-enables the runner's own download (and with it {@code prismVersionOverride}).
     */
    public static final String ENV_PRISM_LOCATION = "MERCARI_PIPELINE_PRISM_LOCATION";

    private Boolean enableWebUI;
    private String idleShutdownTimeout;
    private String prismLocation;
    private String prismVersionOverride;
    private String prismLogLevel;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final PrismOptions prism) {

        setOptions(pipelineOptions, prism, System.getenv(ENV_PRISM_LOCATION));
    }

    static void setOptions(
            final PipelineOptions pipelineOptions,
            final PrismOptions prism,
            final String bundledLocation) {

        if(prism == null && (bundledLocation == null || bundledLocation.isBlank())) {
            return;
        }

        final RunnerOptionSetter setter;
        try {
            setter = RunnerOptionSetter.of(pipelineOptions, "org.apache.beam.runners.prism.PrismPipelineOptions");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException("Failed to set prism runner pipeline options", e);
        }

        if(prism != null) {
            // setEnableWebUI takes a Boolean: a getMethod(name, boolean.class) lookup failed at run time
            setter.set("enableWebUI", prism.enableWebUI)
                    .set("idleShutdownTimeout", prism.idleShutdownTimeout)
                    .set("prismLocation", prism.prismLocation)
                    .set("prismVersionOverride", prism.prismVersionOverride)
                    .set("prismLogLevel", prism.prismLogLevel);
        }

        final String current = (String) setter.get("prismLocation");
        // a null location leaves the option alone
        setter.set("prismLocation", defaultPrismLocation(prism == null ? null : prism.prismLocation, current, bundledLocation));
    }

    /**
     * The {@code prismLocation} to apply for the bundled binary, or {@code null} to leave the option alone:
     * a config value (even an empty one) or a command-line value already in the options takes precedence.
     */
    static String defaultPrismLocation(final String configured, final String current, final String bundled) {
        if(configured != null) {
            return null;
        }
        if(current != null && !current.isEmpty()) {
            return null;
        }
        if(bundled == null || bundled.isBlank()) {
            return null;
        }
        return bundled.trim();
    }
}

