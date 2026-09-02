package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.job.JobReader;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

/** Why a job is slow or not scaling: workers, stage timeline, running stage composition, feature plan mapping. */
@Tool.Module(
    name = "get-job-progress",
    openWorld = true,
    title = "Get Job Progress",
    description = """
        Progress and performance picture of a Dataflow job (id or exact name): current / target workers
        with the autoscaler's decisions and reasons, the execution stages in completion order with the
        time each took, the stage currently running (its transforms and the element counts of its inputs /
        outputs — a stage that read few groups but emitted most rows is stuck on a few hot keys), and,
        when the job runs a feature transform, the feature plan's stages with their keys mapped to the
        Dataflow stages. Use it for "the job is slow / stays on one worker / seems stuck" questions;
        use list-job-errors for failures and get-job-logs for log context. For a Cloud Run Job execution
        (runner 'direct' / 'prism') it returns the execution summary.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "job": {
              "type": "string",
              "description": "Dataflow job id / exact job name (or a Cloud Run execution name)."
            },
            "runner": {
              "type": "string",
              "enum": ["dataflow", "direct", "prism"],
              "description": "Force the runner (default: inferred from the job reference)."
            },
            "project": {
              "type": "string",
              "description": "GCP project id. Defaults to the server's configured project."
            },
            "region": {
              "type": "string",
              "description": "Region. Defaults to the server's configured region."
            }
          },
          "required": ["job"]
        }
        """,
    outputSchema = """
        {
          "type": "string"
        }
        """
)
public class GetJobProgressTool implements Tool {

    @Override
    public void init(final ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {
        final String job = GetJobTool.optionalString(request, "job");
        if (job == null) {
            return McpSchema.CallToolResult.builder().addTextContent("get-job-progress mcp tool requires job parameter").isError(true).build();
        }
        final String result = JobReader.getJobProgress(job,
                GetJobTool.optionalString(request, "runner"),
                GetJobTool.optionalString(request, "project"),
                GetJobTool.optionalString(request, "region"));
        return McpSchema.CallToolResult.builder().addTextContent(result).isError(result.startsWith("ERROR")).build();
    }

}
