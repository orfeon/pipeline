package com.mercari.solution.server.launch;

import com.google.gson.JsonObject;
import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Options;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * {@code dataflow/inProcess}: build the pipeline in the server JVM and submit it with
 * {@code DataflowRunner} (stages the server's own classpath). Kept for development; the
 * Flex Template environment is the default and the one the UI shows.
 */
public class DataflowInProcessLauncher implements Launcher {

    @Override
    public String runner() {
        return "dataflow";
    }

    @Override
    public String environment() {
        return "inProcess";
    }

    @Override
    public JsonObject launch(final LaunchRequest request) {
        final PipelineOptions pipelineOptions = PipelineOptionsFactory
                .fromArgs("--runner=DataflowRunner")
                .as(MPipeline.MPipelineOptions.class);
        Options.setOptions(pipelineOptions, request.config().getOptions());

        final Pipeline pipeline = Pipeline.create(pipelineOptions);
        MPipeline.apply(pipeline, request.config());
        final PipelineResult pipelineResult = pipeline.run();

        return LaunchResult.job(this)
                .state(pipelineResult.getState().name())
                .build();
    }

}
