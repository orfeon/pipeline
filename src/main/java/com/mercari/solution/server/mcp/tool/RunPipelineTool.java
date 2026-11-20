package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.api.PipelineService;
import io.modelcontextprotocol.modern.server.McpRequestContext;
import io.modelcontextprotocol.modern.McpSchema;
import jakarta.servlet.ServletContext;

@Tool.Module(
    name="run-pipeline",
    readOnly = false, idempotent = false, longRunning = true,
    title="Run Pipeline",
    description= """
        Run the pipeline defined in config parameter in the server process (DirectRunner), or with
        dryRun=true only assemble it: every module is validated, schemas are resolved and declarative plans
        such as the feature transform's are compiled against the real input schemas, without running.
        The dry run is the way to iterate on a config before launching it on Dataflow / Cloud Run
        (launch-pipeline): its response holds the resolved output schema of every step ('spec.modules') and,
        for each feature transform, 'featurePlans' (the validate --expand report with stages, columns,
        availability status, hot-key audit SQL and diagnostics). That report is one text ('describe') by
        default; featurePlanFormat 'json' returns it as structure instead ('plan': stages, columns,
        diagnostics as {level, code, location, message}, audit) - use it to read columns or diagnostics by
        program rather than parsing the text.
        A dry run's 'status' is 'ok' only when the whole config assembled and every feature plan compiled
        ('featurePlans[].ok'): launch only a config whose dry run is 'ok'. When a plan fails, 'status' is
        'error', 'error.messages' lists the reasons per step, and 'featurePlans' / 'spec' are still returned.
        'args' in the response holds the template arguments the config was assembled with (its defaults
        overridden by the 'args' parameter).
        Results are returned in JSON format. If the pipeline definition has errors, the error content is
        output in 'error' attribute: 'error.message' (and 'error.messages') is the reason, first; when an
        API call failed, 'error.causes' holds the exception chain (with context such as the query text) and
        'error.stackTrace' the stack trace. A real run returns the debug outputs in 'outputs' and 'metrics';
        it is meant for small local data, not for production-sized inputs.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "config": {
              "type": "string",
              "description": "Definition of pipeline. YAML or JSON format."
            },
            "dryRun": {
              "type": "boolean",
              "description": "Assemble and validate only (no execution). Default false."
            },
            "featurePlanFormat": {
              "type": "string",
              "enum": ["text", "json", "both"],
              "description": "dryRun only: how each entry of 'featurePlans' reports the plan. 'text' (default): 'describe', the readable report. 'json': 'plan', the structured report (stages, columns, diagnostics, audit). 'both': both (the largest answer)."
            },
            "args": {
              "type": ["object", "string"],
              "description": "Template arguments as a JSON object or JSON text, referred to as ${args.<name>} in the config (defaults come from the config's own args block)."
            }
          },
          "required": ["config"]
        }
        """,
    outputSchema = """
        {
          "type": "object",
          "properties": {
            "status": {
              "type": "string",
              "description": "ok or error"
            },
            "spec": {
              "type": "object",
              "description": "modules: resolved output schema per step"
            },
            "args": {
              "type": "object",
              "description": "the resolved template arguments (config defaults overridden by the request's args)"
            },
            "featurePlans": {
              "type": "array",
              "description": "dryRun only: per feature transform {name, ok, describe | plan (see featurePlanFormat), engineErrors}"
            },
            "outputs": {
              "type": "array",
              "description": "real run only: debug outputs"
            }
          },
          "required": ["status"]
        }
        """
)
public class RunPipelineTool implements Tool {

    @Override
    public void init(ServletContext servletContext) {

    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpRequestContext ctx,
            final McpSchema.CallToolRequest request) {

        if(!request.arguments().containsKey("config")) {
            return Tool.result("run-pipeline mcp tool requires config parameter", true);
        }

        final String config = request.arguments().get("config").toString();
        final Object dryRunValue = request.arguments().get("dryRun");
        final boolean dryRun = dryRunValue != null && Boolean.parseBoolean(dryRunValue.toString());
        final Object argsValue = request.arguments().get("args");
        final String args = argsValue == null ? null
                : argsValue instanceof String s ? s : new com.google.gson.Gson().toJson(argsValue);
        final Object formatValue = request.arguments().get("featurePlanFormat");
        final String featurePlanFormat = formatValue == null || formatValue.toString().isBlank() ? null : formatValue.toString();
        if(featurePlanFormat != null && !com.mercari.solution.server.api.FeatureService.PLAN_FORMATS.contains(featurePlanFormat)) {
            // the MCP server validates arguments against the schema; the in-server agent's bridge does not
            return Tool.result("run-pipeline: unknown featurePlanFormat '" + featurePlanFormat + "' (available: "
                    + com.mercari.solution.server.api.FeatureService.PLAN_FORMATS + ")", true);
        }
        final PipelineService.RunResult result = PipelineService.run(config, args, dryRun, featurePlanFormat);
        return Tool.result(result.responseText, result.isError);
    }

}
