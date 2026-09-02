package com.mercari.solution.server.agent.tool;

import dev.langchain4j.agent.tool.P;
import dev.langchain4j.agent.tool.Tool;

/** Agent tool {@code runPipeline}: wrapper of the MCP tool {@code run-pipeline}. */
public class PipelineExecutor {

    @Tool(name = "runPipeline", value = """
        Validate and dry-run a pipeline configuration.
        Use this tool to check if a pipeline config is valid.
        The tool returns the validation result including any errors.
    """)
    public String execute(
            @P(name = "config", description = "Pipeline configuration content in YAML format") String config,
            @P(name = "dryRun",  description = "If true, only validate without executing. Set to true for validation.") boolean dryRun,
            @P(name = "args", description = "Optional template arguments as JSON string", required = false) String args) {
        return McpToolBridge.call("run-pipeline", McpToolBridge.args("config", config, "dryRun", dryRun, "args", args), "SUCCESS: ");
    }

    public static PipelineExecutor create() {
        return new PipelineExecutor();
    }

}
