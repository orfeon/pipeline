package com.mercari.solution.server.agent.tool;

import com.mercari.solution.server.api.PipelineService;
import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

public class PipelineExecutor {

    @Tool("""
        Validate and dry-run a pipeline configuration.
        Use this tool to check if a pipeline config is valid.
        The tool returns the validation result including any errors.
    """)
    public String execute(
            @P(name = "config", description = "Pipeline configuration content in YAML format") String config,
            @P(name = "dryRun",  description = "If true, only validate without executing. Set to true for validation.") boolean dryRun,
            @P(name = "args", description = "Optional template arguments as JSON string", required = false) String args) {

        try {
            final PipelineService.RunResult result = PipelineService.run(config, args, dryRun);
            if (result.isError) {
                return "ERROR: " + result.errorMessage;
            } else {
                return "SUCCESS: " + (result.responseText != null ? result.responseText : "Pipeline is valid.");
            }
        } catch (Exception e) {
            return "ERROR: " + e.getMessage();
        }
    }

    public static PipelineExecutor create() {
        return new PipelineExecutor();
    }

}
