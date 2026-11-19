package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.job.JobReader;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.McpSchema;
import jakarta.servlet.ServletContext;

/** Status of a launched job, whatever the runner: a Dataflow job or a Cloud Run Job execution. */
@Tool.Module(
    name = "get-job",
    openWorld = true,
    title = "Get Job",
    description = """
        Get the status of a launched pipeline job. Dataflow: pass the job id (e.g.
        '2026-07-17_22_25_11-123...') or the exact job name; returns state, timing, SDK version, labels and
        the pipeline config recovered from the job's launch parameters. Cloud Run Job (runner 'direct' or
        'prism'): pass the execution name from launch-pipeline (projects/.../jobs/.../executions/...) — or,
        with runner 'direct' / 'prism' and no job, list the latest executions of the Cloud Run Job the
        server has configured for that runner;
        returns state (RUNNING | SUCCEEDED | FAILED | CANCELLED), timings, task counts, conditions,
        log and console links. The runner is inferred from the reference; set 'runner' to force it.
        While waiting for a job to finish, poll with view 'status': the answer is then the status alone,
        without the config text. A Dataflow job reports JOB_STATE_UNKNOWN / QUEUED / PENDING (and type
        JOB_TYPE_UNKNOWN) for about a minute after a launch: it is starting, not lost.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "job": {
              "type": "string",
              "description": "Dataflow job id / exact job name, or a Cloud Run execution name (projects/{project}/locations/{region}/jobs/{job}/executions/{execution}); omit with runner 'direct' / 'prism' to list the latest executions."
            },
            "runner": {
              "type": "string",
              "enum": ["dataflow", "direct", "prism"],
              "description": "Force the runner (default: inferred from the job reference; 'direct' / 'prism' = the Cloud Run Job of that image)."
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
            },
            "view": {
              "type": "string",
              "enum": ["full", "status"],
              "description": "'status': state, timing, SDK version and labels only — no pipeline config (use it when polling). 'full' (default): also the config a Dataflow job was launched with."
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
            final McpRequestContext ctx,
            final McpSchema.CallToolRequest request) {
        final String result = JobReader.getJob(
                optionalString(request, "job"),
                optionalString(request, "runner"),
                optionalString(request, "project"),
                optionalString(request, "region"),
                optionalInt(request, "limit"),
                optionalString(request, "view"));
        return Tool.result(result, result.startsWith("ERROR"));
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
