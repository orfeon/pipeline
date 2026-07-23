package com.mercari.solution.server.api;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.gson.*;
import com.mercari.solution.server.agent.AgentInteractionLogger;
import com.mercari.solution.server.agent.ChatModelFileLogger;
import com.mercari.solution.server.agent.PipelineAgent;
import com.mercari.solution.util.cloud.google.GcpCredentialsCache;
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
import java.nio.file.Path;
import java.util.*;

public class AgentService {

    private static final Logger LOG = LoggerFactory.getLogger(AgentService.class);

    public static final String CONTEXT_ATTRIBUTE_CHAT_MODEL = "PipelineBuilderChatModel";

    private static volatile String agentModelName = "";

    public static void init(final ServletContext context) {
        final ChatModel chatModel = createChatModel();
        context.setAttribute(CONTEXT_ATTRIBUTE_CHAT_MODEL, chatModel);
    }

    private static ChatModel createChatModel() {
        try {
            final GoogleCredentials credentials = GcpCredentialsCache.credentials();
            credentials.refreshIfExpired();
            final String project = ServiceOptions.getDefaultProjectId();
            final String modelName = env("MERCARI_PIPELINE_AGENT_MODEL", "gemini-3.5-flash-lite");
            agentModelName = modelName;
            final String location = env("MERCARI_PIPELINE_AGENT_LOCATION", "global");
            final String debugLog = env("MERCARI_PIPELINE_AGENT_DEBUG_LOG", "");
            final GoogleGenAiChatModel.Builder builder = GoogleGenAiChatModel.builder()
                    .googleCredentials(credentials)
                    .logRequestsAndResponses(!debugLog.isEmpty())
                    .projectId(project)
                    .location(location)
                    .modelName(modelName);
            // Do not force responseMimeType=application/json: combined with tools, gemini-3.5+
            // keeps re-issuing the last tool call instead of emitting the final text response.
            // The system prompt's JSON contract (plus the UI's fenced-JSON fallback) is sufficient.
            if ("json".equalsIgnoreCase(env("MERCARI_PIPELINE_AGENT_RESPONSE_FORMAT", "text"))) {
                builder.responseFormat(ResponseFormat.JSON);
            }
            final ChatModel chatModel = builder.build();
            if (debugLog.isEmpty()) {
                return chatModel;
            }
            LOG.info("Agent model debug logging enabled: {}", debugLog);
            return new ChatModelFileLogger(chatModel, Path.of(debugLog));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create chat model", e);
        }
    }

    private static String env(final String name, final String defaultValue) {
        return Optional.ofNullable(System.getenv(name))
                .filter(v -> !v.isBlank())
                .map(String::trim)
                .orElse(defaultValue);
    }

    public static void serve(
            final HttpServletRequest request,
            final HttpServletResponse response) {

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        AgentInteractionLogger interactionLog = null;
        try {
            final ChatModel chatModel = (ChatModel) Optional
                    .ofNullable(request.getServletContext().getAttribute(CONTEXT_ATTRIBUTE_CHAT_MODEL))
                    .orElseThrow(() -> new RuntimeException("ChatModel not initialized. Check server initialization."));

            final JsonObject body;
            try (final Reader reader = new InputStreamReader(request.getInputStream(), StandardCharsets.UTF_8)) {
                body = JsonParser.parseReader(reader).getAsJsonObject();
            }

            interactionLog = new AgentInteractionLogger(
                    body.has("conversationId") && body.get("conversationId").isJsonPrimitive()
                            ? body.get("conversationId").getAsString() : "",
                    agentModelName,
                    body.has("history") && body.get("history").isJsonArray()
                            ? body.getAsJsonArray("history").size() : 0);

            final String responseText = PipelineAgent.process(chatModel, body, interactionLog);
            interactionLog.completion();
            response.getWriter().println(responseText);
        } catch (final Throwable e) {
            LOG.error("Agent service error", e);
            if (interactionLog != null) {
                interactionLog.error(e);
            }
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
