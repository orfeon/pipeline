package com.mercari.solution.server.agent;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.langchain4j.http.client.HttpClient;
import dev.langchain4j.http.client.HttpClientBuilder;
import dev.langchain4j.http.client.HttpClientBuilderLoader;
import dev.langchain4j.http.client.HttpRequest;
import dev.langchain4j.http.client.SuccessfulHttpResponse;
import dev.langchain4j.http.client.sse.ServerSentEventListener;
import dev.langchain4j.http.client.sse.ServerSentEventParser;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * HTTP client for running LangChain4j's Gemini (Developer API) chat model against Vertex AI.
 *
 * <p>The two APIs share the {@code generateContent} wire format, with one difference that matters to
 * a tool-calling agent: when the model issues several function calls in one turn, LangChain4j sends
 * each tool result as its own {@code user} content, while Vertex AI requires all the function
 * responses of that turn in a single content ("the number of function response parts is equal to the
 * number of function call parts of the function call turn") and rejects the request with 400
 * otherwise. This client merges adjacent function-response-only {@code user} contents before the
 * request is sent; everything else is delegated to the JDK-based client untouched.
 */
public class VertexAiGeminiHttpClient implements HttpClient {

    private final HttpClient delegate;

    private VertexAiGeminiHttpClient(final HttpClient delegate) {
        this.delegate = delegate;
    }

    public static HttpClientBuilder builder() {
        return new Builder();
    }

    @Override
    public SuccessfulHttpResponse execute(final HttpRequest request) {
        return delegate.execute(rewrite(request));
    }

    @Override
    public CompletableFuture<SuccessfulHttpResponse> executeAsync(final HttpRequest request) {
        return delegate.executeAsync(rewrite(request));
    }

    @Override
    public void execute(final HttpRequest request, final ServerSentEventParser parser, final ServerSentEventListener listener) {
        delegate.execute(rewrite(request), parser, listener);
    }

    private static HttpRequest rewrite(final HttpRequest request) {
        final String body = request.body();
        final String merged = mergeFunctionResponses(body);
        if (merged == body) {
            return request;
        }
        return HttpRequest.builder()
                .method(request.method())
                .url(request.url())
                .headers(request.headers())
                .body(merged)
                .build();
    }

    /**
     * Merges adjacent {@code user} contents that hold only {@code functionResponse} parts into one
     * content. Returns the given string itself when there is nothing to merge (or the body is not a
     * {@code generateContent} request).
     */
    public static String mergeFunctionResponses(final String body) {
        if (body == null || !body.contains("\"functionResponse\"")) {
            return body;
        }
        final JsonElement root;
        try {
            root = JsonParser.parseString(body);
        } catch (final RuntimeException e) {
            return body;
        }
        if (!root.isJsonObject() || !root.getAsJsonObject().has("contents")
                || !root.getAsJsonObject().get("contents").isJsonArray()) {
            return body;
        }
        final JsonArray contents = root.getAsJsonObject().getAsJsonArray("contents");
        final JsonArray mergedContents = new JsonArray();
        JsonObject previous = null;
        boolean changed = false;
        for (final JsonElement content : contents) {
            if (previous != null && isFunctionResponseTurn(previous) && isFunctionResponseTurn(content)) {
                previous.getAsJsonArray("parts").addAll(content.getAsJsonObject().getAsJsonArray("parts"));
                changed = true;
                continue;
            }
            mergedContents.add(content);
            previous = content.isJsonObject() ? content.getAsJsonObject() : null;
        }
        if (!changed) {
            return body;
        }
        root.getAsJsonObject().add("contents", mergedContents);
        return root.toString();
    }

    private static boolean isFunctionResponseTurn(final JsonElement content) {
        if (!content.isJsonObject()) {
            return false;
        }
        final JsonObject object = content.getAsJsonObject();
        if (!object.has("role") || !object.get("role").isJsonPrimitive()
                || !"user".equals(object.get("role").getAsString())
                || !object.has("parts") || !object.get("parts").isJsonArray()
                || object.getAsJsonArray("parts").isEmpty()) {
            return false;
        }
        for (final JsonElement part : object.getAsJsonArray("parts")) {
            if (!part.isJsonObject() || !part.getAsJsonObject().has("functionResponse")) {
                return false;
            }
        }
        return true;
    }

    private static class Builder implements HttpClientBuilder {

        // the JDK HttpClient based builder (langchain4j-http-client-jdk, the only one on the classpath)
        private final HttpClientBuilder delegate = HttpClientBuilderLoader.loadHttpClientBuilder();

        @Override
        public Duration connectTimeout() {
            return delegate.connectTimeout();
        }

        @Override
        public HttpClientBuilder connectTimeout(final Duration timeout) {
            delegate.connectTimeout(timeout);
            return this;
        }

        @Override
        public Duration readTimeout() {
            return delegate.readTimeout();
        }

        @Override
        public HttpClientBuilder readTimeout(final Duration timeout) {
            delegate.readTimeout(timeout);
            return this;
        }

        @Override
        public HttpClient build() {
            return new VertexAiGeminiHttpClient(delegate.build());
        }
    }

}
