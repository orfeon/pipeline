package com.mercari.solution.config.options;

import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

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

        try {
            final Class<? extends PipelineOptions> clazz = (Class<? extends PipelineOptions>)Class.forName("org.apache.beam.runners.prism.PrismPipelineOptions");
            final PipelineOptions prismOptions = pipelineOptions.as(clazz);

            if(prism != null) {
                if(prism.enableWebUI != null) {
                    clazz.getMethod("setEnableWebUI", boolean.class).invoke(prismOptions, prism.enableWebUI);
                }
                if(prism.idleShutdownTimeout != null) {
                    clazz.getMethod("setIdleShutdownTimeout", String.class).invoke(prismOptions, prism.idleShutdownTimeout);
                }
                if(prism.prismLocation != null) {
                    clazz.getMethod("setPrismLocation", String.class).invoke(prismOptions, prism.prismLocation);
                }
                if(prism.prismVersionOverride != null) {
                    clazz.getMethod("setPrismVersionOverride", String.class).invoke(prismOptions, prism.prismVersionOverride);
                }
            }

            final String current = (String) clazz.getMethod("getPrismLocation").invoke(prismOptions);
            final String location = defaultPrismLocation(prism == null ? null : prism.prismLocation, current, bundledLocation);
            if(location != null) {
                clazz.getMethod("setPrismLocation", String.class).invoke(prismOptions, location);
            }

        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Failed to set prism runner pipeline options", e);
        }
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

