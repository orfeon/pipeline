package com.mercari.solution.server.api;

import com.mercari.solution.MPipeline;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.MCollection;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ProbeService {

    private static final Logger LOG = LoggerFactory.getLogger(ProbeService.class);

    public static void serve(
            final HttpServletRequest request,
            final HttpServletResponse response) {

        switch (request.getPathInfo()) {
            case "/ready" -> ready(request, response);
            case "/health" -> health(request, response);
        }
    }

    private static void ready(
            final HttpServletRequest request,
            final HttpServletResponse response) {

        LOG.info("ready");

        final PipelineOptions pipelineOptions = PipelineOptionsFactory
                .fromArgs()
                .create();
        final Pipeline pipeline = Pipeline.create(pipelineOptions);
        final Config config = createSimpleConfig();
        final Map<String, MCollection> outputs = MPipeline.apply(pipeline, config);
        pipeline.run();

        response.setStatus(200);
    }

    private static void health(
            final HttpServletRequest request,
            final HttpServletResponse response) {

        LOG.info("health");
        response.setStatus(200);
    }

    private static Config createSimpleConfig() {
        final String configJson = """
                {
                  "sources": [
                    {
                      "name": "source",
                      "module": "create",
                      "parameters": {
                        "type": "int32",
                        "elements": [1]
                      }
                    }
                  ]
                }
                """;
        return Config.parse(configJson, null, Config.Format.json, new String[0]);
    }

}
