package com.mercari.solution.server.api;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.gson.*;
import com.mercari.solution.server.agent.PipelineAgent;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ResponseFormat;
import dev.langchain4j.model.google.genai.GoogleGenAiChatModel;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class AgentService {

    private static final Logger LOG = LoggerFactory.getLogger(AgentService.class);

    public static final String CONTEXT_ATTRIBUTE_CHAT_MODEL = "PipelineBuilderChatModel";

    public static void init(final ServletContext context) {
        final ChatModel chatModel = createChatModel();
        context.setAttribute(CONTEXT_ATTRIBUTE_CHAT_MODEL, chatModel);
    }

    private static ChatModel createChatModel() {
        try {
            final GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
            credentials.refreshIfExpired();
            final String project = ServiceOptions.getDefaultProjectId();
            return GoogleGenAiChatModel.builder()
                    .googleCredentials(credentials)
                    .projectId(project)
                    .location("global")
                    .modelName("gemini-3.1-flash-lite")
                    .responseFormat(ResponseFormat.JSON)
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create chat model", e);
        }
    }

    public static void serve(
            final HttpServletRequest request,
            final HttpServletResponse response) {

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        try {
            final ChatModel chatModel = (ChatModel) Optional
                    .ofNullable(request.getServletContext().getAttribute(CONTEXT_ATTRIBUTE_CHAT_MODEL))
                    .orElseThrow(() -> new RuntimeException("ChatModel not initialized. Check server initialization."));

            final JsonObject body;
            try (final Reader reader = new InputStreamReader(request.getInputStream(), StandardCharsets.UTF_8)) {
                body = JsonParser.parseReader(reader).getAsJsonObject();
            }

            final String responseText = PipelineAgent.process(chatModel, body);
            LOG.info("response: {}", responseText);
            response.getWriter().println(responseText);
        } catch (final Throwable e) {
            LOG.error("Agent service error", e);
            try {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                final List<Map<String, Object>> errorResponse = List.of(
                        Map.of("role", "assistant", "content",
                                JsonParser.parseString(new Gson().toJson(
                                        Map.of("message", "An error occurred: " + e.getMessage())
                                )).toString())
                );
                response.getWriter().println(new Gson().toJson(errorResponse));
            } catch (IOException ioe) {
                LOG.error("Failed to write error response", ioe);
            }
        }
    }

}
