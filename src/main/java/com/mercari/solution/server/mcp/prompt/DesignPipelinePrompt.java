package com.mercari.solution.server.mcp.prompt;

import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;

import java.util.List;

@Prompt.Module(
    name="design-pipeline",
    description= """
        support design data pipeline configuration.
        """
)
public class DesignPipelinePrompt implements Prompt {

    private static final String PROMPT = """
        mercari/pipeline is a tool that enables data pipelines defined in YAML or JSON format to be executed in various execution environments.
        Data pipelines are defined in config files in YAML or JSON format.
        Please refer to the resource 'docs://config/README.md' for details on how to define the config file.
        In this context, an overview of how to define a config file.
        The config file contains the following five sections.
        
        * system (resource: 'docs://config/system.md')
          * Options run environment.
        * options (resource: 'docs://config/options/README.md')
          * Options for the pipeline when executing the pipeline.
        * sources
          * Defines modules that specifies the input source of the data.
        * transforms
          * Defines modules that specifies the processing content of the data
        * sinks
          * Defines modules that specifies the output destination of the data
       
        'system' and 'options' specify pipeline options and execution environment conditions.
        'sources', 'transforms', and 'sinks' define specific pipeline processing details by combining multiple modules.
        The 'module' attribute in module specification the type of module to be used.
        The 'name' attribute in module specification is used as an identifier when specifying the output of this module as the input of another module. It should be unique in the pipeline.
        For 'transforms' and 'sinks' modules, the 'inputs' attribute specifies the name of the module to be used as input.
        They can receive output results from other source modules or transform modules.
        The contents of the module definition are specified in 'parameters' attribute. 'parameters' attribute items are different for each module.
       
        For a list of modules, refer to the resource 'docs://config/module/README.md'.
       
        To validate the pipeline you have defined, you can use tool 'validate-pipeline'.
        You can check for syntax problems in the pipeline definition, execution permissions, and so on.
        If there is a problem, check the error messages in response 'errors' attribute and correct them.
        If there are no problems, response 'outputs' attribute contains the schema of the output of each module, so you can check if the output is what you expect.
        You can also use the tool 'run-pipeline' to run the pipeline in MCP server local environment.
        This is used when you want to check not only the pipeline definition but also the actual processing results.
        For both tools, specify the pipeline definition in YAML or JSON format in the config parameter.
        """;

    public List<McpSchema.PromptArgument> arguments() {
        return List.of();
    }

    public McpSchema.GetPromptResult sync(
            final McpSyncServerExchange exchange,
            final McpSchema.GetPromptRequest request) {

        final McpSchema.Content content = new McpSchema.TextContent(PROMPT);
        final McpSchema.PromptMessage message = new McpSchema.PromptMessage(McpSchema.Role.ASSISTANT, content);
        return new McpSchema.GetPromptResult("support design data pipeline configuration", List.of(message));
    }

}
