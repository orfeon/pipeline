package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.job.JobReader;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

/** Status of a launched job, whatever the runner: a Dataflow job or a Cloud Run Job execution. */
@Tool.Module(
    name = "get-job",
    title = "Get Job",
    description = """
        Get the status of a launched pipeline job. Dataflow: pass the job id (e.g.
        '2026-07-17_22_25_11-123...') or the exact job name; returns state, timing, SDK version, labels and
        the pipeline config recovered from the job's launch parameters. Cloud Run Job (runner 'direct'):
        pass the execution name from launch-pipeline (projects/.../jobs/.../executions/...) — or, with
        runner 'direct' and no job, list the latest executions of the server's configured Cloud Run Job;
        returns state (RUNNING | SUCCEEDED | FAILED | CANCELLED), timings, task counts, conditions,
        log and console links. The runner is inferred from the reference; set 'runner' to force it.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "job": {
              "type": "string",
              "description": "Dataflow job id / exact job name, or a Cloud Run execution name (projects/{project}/locations/{region}/jobs/{job}/executions/{execution}); omit with runner 'direct' to list the latest executions."
            },
            "runner": {
              "type": "string",
              "enum": ["dataflow", "direct"],
              "description": "Force the runner (default: inferred from the job reference; 'direct' = Cloud Run Job)."
            },
            "project": {
              "type": "string",
              "description": "GCP project id. Defaults to the server's configured project."
            },
            "region": {
              "type": "string",
              "description": "Region (Dataflow region / Cloud Run location). Defaults to the server's configured region."
            },
            "limit": {
              "type": "integer",
              "description": "Cloud Run only: number of executions to list when no job is given (default 5)."
            }
          }
        }
        """,
    outputSchema = """
        {
          "type": "string"
        }
        """
)
public class GetJobTool implements Tool {

    @Override
    public void init(final ServletContext servletContext) {
    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {
        final String result = JobReader.getJob(
                optionalString(request, "job"),
                optionalString(request, "runner"),
                optionalString(request, "project"),
                optionalString(request, "region"),
                optionalInt(request, "limit"));
        return McpSchema.CallToolResult.builder().addTextContent(result).isError(result.startsWith("ERROR")).build();
    }

    static String optionalString(final McpSchema.CallToolRequest request, final String key) {
        final Object value = request.arguments().get(key);
        return value == null || value.toString().isBlank() ? null : value.toString();
    }

    static Integer optionalInt(final McpSchema.CallToolRequest request, final String key) {
        final Object value = request.arguments().get(key);
        if (value instanceof Number number) return number.intValue();
        if (value == null || value.toString().isBlank()) return null;
        try {
            return Integer.parseInt(value.toString().trim());
        } catch (final NumberFormatException e) {
            return null;
        }
    }

}
