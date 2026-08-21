package com.mercari.solution.util.pipeline.outbound;

import java.util.List;
import java.util.Map;

/**
 * A fully rendered outbound HTTP request: the pure-function output of request building,
 * handed to a {@link HttpTransport}. Auth headers are NOT part of it — the
 * {@link AuthProvider} adds them right before sending (so retries after token refresh
 * reuse the same request).
 */
public record OutboundRequest(
        String url,
        String method,
        Map<String, String> headers,
        byte[] body,
        int elementCount) {

    public OutboundRequest {
        headers = headers == null ? Map.of() : headers;
    }

    public int bodySize() {
        return body == null ? 0 : body.length;
    }

    public String host() {
        try {
            return java.net.URI.create(url).getHost();
        } catch (final RuntimeException e) {
            return null;
        }
    }

    /** Response of one attempt. */
    public record Response(
            int statusCode,
            Map<String, List<String>> headers,
            byte[] body,
            long durationMs) {

        public String bodyText() {
            return body == null ? null : new String(body, java.nio.charset.StandardCharsets.UTF_8);
        }

        public String header(final String name) {
            if(headers == null) {
                return null;
            }
            for(final Map.Entry<String, List<String>> entry : headers.entrySet()) {
                if(entry.getKey() != null && entry.getKey().equalsIgnoreCase(name) && entry.getValue() != null && !entry.getValue().isEmpty()) {
                    return entry.getValue().get(0);
                }
            }
            return null;
        }
    }
}
