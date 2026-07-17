package com.mercari.solution.server.agent;

import com.mercari.solution.server.agent.tool.CodeReader;
import com.mercari.solution.server.agent.tool.DataflowReader;
import com.mercari.solution.server.agent.tool.DocsReader;
import dev.langchain4j.memory.chat.MessageWindowChatMemory;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.service.AiServices;
import dev.langchain4j.service.SystemMessage;
import dev.langchain4j.service.UserMessage;
import dev.langchain4j.service.V;

/**
 * Autonomous diagnosis agent for failed Dataflow jobs. Unlike PipelineAgent (interactive
 * builder chat), this runs once per failure event with no user in the loop and must return
 * a single structured JSON verdict (see dataflow-diagnosis-system.md).
 */
public interface DiagnosisAgent {

    @SystemMessage(fromResource = "server/agents/prompts/dataflow-diagnosis-system.md")
    @UserMessage("{{facts}}")
    String diagnose(@V("facts") String facts);

    static String diagnose(final ChatModel chatModel, final String facts) {
        return AiServices.builder(DiagnosisAgent.class)
                .chatModel(chatModel)
                .chatMemory(MessageWindowChatMemory.withMaxMessages(30))
                .tools(
                        DocsReader.create(),
                        CodeReader.create(),
                        DataflowReader.create()
                )
                .build()
                .diagnose(facts);
    }

}
