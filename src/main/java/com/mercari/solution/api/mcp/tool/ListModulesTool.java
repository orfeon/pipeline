package com.mercari.solution.api.mcp.tool;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.servlet.ServletContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


@Tool.Module(
    name="list-modules",
    title="List Pipeline Modules",
    description= """
        List of pipeline modules available in mercari/pipeline.
        The response is in JSON format and includes the id and summary as well as the name of the module.
        By specifying this id in the 'ids' parameter of tool: 'describe-modules', you can check the detailed specification of the module.
        """,
    inputSchema = """
        {
          "type": "object",
          "properties": {
            "type": {
              "type": "string",
              "title": "Module Type",
              "enum": ["source", "transform", "sink"],
              "description": "Specify module type (source, transform, or sink). If not specified, all types of modules are listed."
            }
          },
          "required": []
        }
        """,
    outputSchema = """
        {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {
            },
            "required": []
          }
        }
        """

)
public class ListModulesTool implements Tool {

    public static class ModuleSpec implements Serializable {

        public String uri;
        public Type type;
        public String name;
        public String description;
        public List<Mode> mode;

        public enum Type {
            source,
            transform,
            sink,
            failure
        }

        public enum Mode {
            batch,
            streaming
        }

        public static ModuleSpec of(
                final Gson gson,
                final JsonElement jsonElement) {

            final ModuleSpec spec = gson.fromJson(jsonElement, ModuleSpec.class);
            if(spec == null) {
                return null;
            }
            spec.uri = String.format("config/module/%s/%s", spec.type, spec.name);
            return spec;
        }

        public static List<ModuleSpec> list(final String jsonText) {
            final List<ModuleSpec> specs = new ArrayList<>();
            final Gson gson = new Gson();
            final JsonElement jsonElement = gson.fromJson(jsonText, JsonElement.class);
            if(!jsonElement.isJsonArray()) {
                return specs;
            }
            for(final JsonElement specElement : jsonElement.getAsJsonArray()) {
                final ModuleSpec spec = of(gson, specElement);
                if(spec == null) {
                    continue;
                }
                specs.add(spec);
            }

            return specs;
        }

    }

    @Override
    public void init(ServletContext servletContext) {

    }

    @Override
    public McpSchema.CallToolResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.CallToolRequest request) {

        final JsonArray jsonArray = new JsonArray();
        {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("id", "source/spanner");
            jsonObject.addProperty("type", "source");
            jsonObject.addProperty("name", "spanner");
            jsonObject.addProperty("description", "spanner source module to get data from Cloud Spanner. The data can be retrieved from the specified table or query.");
            jsonArray.add(jsonObject);
        }
        {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("id", "source/bigquery");
            jsonObject.addProperty("type", "source");
            jsonObject.addProperty("name", "bigquery");
            jsonObject.addProperty("description", "bigquery source module to get data from BigQuery. The data can be retrieved from the specified table or query.");
            jsonArray.add(jsonObject);
        }
        {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("id", "sink/storage");
            jsonObject.addProperty("type", "sink");
            jsonObject.addProperty("name", "storage");
            jsonObject.addProperty("description", "storage sink module to write data to Storage such as Cloud Storage, S3, local storage.");
            jsonArray.add(jsonObject);
        }
        return new McpSchema.CallToolResult(jsonArray.toString(), false);
    }

}
