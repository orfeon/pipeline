package com.mercari.solution.server.agent;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class PipelineAgentTest {

    @Test
    public void testEndsWithAnswer() {
        final ToolExecutionRequest request = ToolExecutionRequest.builder().id("a").name("readDocs").arguments("{}").build();
        final AiMessage toolCall = AiMessage.from(request);
        final ToolExecutionResultMessage toolResult = ToolExecutionResultMessage.from(request, "docs");

        Assertions.assertTrue(PipelineAgent.endsWithAnswer(List.of(AiMessage.from("done"))));
        Assertions.assertTrue(PipelineAgent.endsWithAnswer(List.of(toolCall, toolResult, AiMessage.from("done"))));

        // nothing came back at all
        Assertions.assertFalse(PipelineAgent.endsWithAnswer(List.of()));
        // the model answered with an empty candidate (e.g. MALFORMED_RESPONSE)
        Assertions.assertFalse(PipelineAgent.endsWithAnswer(List.of(AiMessage.builder().build())));
        Assertions.assertFalse(PipelineAgent.endsWithAnswer(List.of(toolCall, toolResult, AiMessage.from(" "))));
        Assertions.assertFalse(PipelineAgent.endsWithAnswer(List.of(toolCall, toolResult)));
    }

}
