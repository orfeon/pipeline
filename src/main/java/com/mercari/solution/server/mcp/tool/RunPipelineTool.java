package com.mercari.solution.server.mcp.tool;

import com.mercari.solution.server.api.PipelineService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

@Tool.Module(
    name="run-pipeline",
    title="Run Pipeline",
    description= """
        Run the pipeline defined in config parameter in the server process (DirectRunner), or with
        dryRun=true only assemble it: every module is validated, schemas are resolved and declarative plans
        such as the feature transform's are compiled against the real input schemas, without running.
        The dry run is the way to iterate on a config before launching it on Dataflow / Cloud Run
        (launch-pipeline): its response holds the resolved output schema of every step ('spec.modules') and,
        for each feature transform, 'featurePlans' (the validate --expand report with stages, columns,
        availability status, hot-key audit SQL and diagnostics).
        Results are returned in JSON format. If the pipeline definition has errors, the error content is
        output in 'error' attribute. A real run returns the debug outputs in 'outputs' and 'metrics';
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
            "featurePlans": {
              "type": "array",
              "description": "dryRun only: per feature transform {name, ok, describe, engineErrors}"
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
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        if(!request.arguments().containsKey("config")) {
            return McpSchema.CallToolResult.builder()
                    .addTextContent("run-pipeline mcp tool requires config parameter")
                    .isError(true)
                    .build();
        }

        final String config = request.arguments().get("config").toString();
        final Object dryRunValue = request.arguments().get("dryRun");
        final boolean dryRun = dryRunValue != null && Boolean.parseBoolean(dryRunValue.toString());
        final Object argsValue = request.arguments().get("args");
        final String args = argsValue == null ? null
                : argsValue instanceof String s ? s : new com.google.gson.Gson().toJson(argsValue);
        final PipelineService.RunResult result = PipelineService.run(config, args, dryRun);
        return McpSchema.CallToolResult.builder()
                .addTextContent(result.responseText)
                .isError(result.isError)
                .build();
    }

}
