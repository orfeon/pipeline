package com.mercari.solution.config.options;

import org.apache.beam.sdk.expansion.service.ExpansionServiceOptions;
import org.apache.beam.sdk.extensions.python.PythonExternalTransformOptions;
import org.apache.beam.sdk.options.PipelineOptions;

import java.io.Serializable;

public class ExpansionOptions implements Serializable {

    private Boolean alsoStartLoopbackWorker;
    private String expansionServiceConfigFile;
    private Boolean useAltsServer;
    private Boolean useConfigDependenciesForManaged;

    private PythonOptions python;

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final ExpansionOptions expansion) {

        if(expansion == null) {
            return;
        }

        if(expansion.alsoStartLoopbackWorker != null) {
            pipelineOptions.as(ExpansionServiceOptions.class).setAlsoStartLoopbackWorker(expansion.alsoStartLoopbackWorker);
        }
        if(expansion.useAltsServer != null) {
            pipelineOptions.as(ExpansionServiceOptions.class).setUseAltsServer(expansion.useAltsServer);
        }
        if(expansion.useConfigDependenciesForManaged != null) {
            pipelineOptions.as(ExpansionServiceOptions.class).setUseConfigDependenciesForManaged(expansion.useConfigDependenciesForManaged);
        }
        if(expansion.expansionServiceConfigFile != null) {
            pipelineOptions.as(ExpansionServiceOptions.class).setExpansionServiceConfigFile(expansion.expansionServiceConfigFile);
        }

        PythonOptions.setOptions(pipelineOptions, expansion.python);
    }

    public static class PythonOptions implements Serializable {

        private Boolean useTransformService;
        private String customBeamRequirement;

        private static void setOptions(
                final PipelineOptions pipelineOptions,
                final PythonOptions python) {

            if(python == null) {
                return;
            }

            if(python.useTransformService != null) {
                pipelineOptions.as(PythonExternalTransformOptions.class).setUseTransformService(python.useTransformService);
            }
            if(python.customBeamRequirement != null) {
                pipelineOptions.as(PythonExternalTransformOptions.class).setCustomBeamRequirement(python.customBeamRequirement);
            }
        }

    }

}
