package com.mercari.solution.util.pipeline.outbound;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Sends {@link OutboundRequest}s with the JDK {@link HttpClient}. One instance per DoFn
 * (connections are pooled inside the client). Applies the {@link AuthProvider} right before
 * each send; transparently gunzips gzip-encoded responses.
 */
public class HttpTransport implements AutoCloseable {

    /** JDK HttpClient refuses these as user-set headers. */
    private static final Set<String> RESTRICTED_HEADERS = Set.of("connection", "content-length", "expect", "host", "upgrade");

    public static class Parameters implements Serializable {
        public String version;            // HTTP_1_1 | HTTP_2
        public String followRedirects;    // never | normal | always
        public String proxy;              // host:port
        public List<String> allowedHosts;

        public List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(version != null) {
                try {
                    HttpClient.Version.valueOf(version.toUpperCase());
                } catch (final IllegalArgumentException e) {
                    errorMessages.add(prefix + ".version must be HTTP_1_1 or HTTP_2 but: " + version);
                }
            }
            if(followRedirects != null) {
                try {
                    HttpClient.Redirect.valueOf(followRedirects.toUpperCase());
                } catch (final IllegalArgumentException e) {
                    errorMessages.add(prefix + ".followRedirects must be never, normal or always but: " + followRedirects);
                }
            }
            if(proxy != null && !proxy.contains(":")) {
                errorMessages.add(prefix + ".proxy must be host:port but: " + proxy);
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(version == null) {
                version = "HTTP_2";
            }
            if(followRedirects == null) {
                followRedirects = "normal";
            }
        }
    }

    public static class TimeoutParameters implements Serializable {
        public String connect;
        public String request;

        public List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            for(final Map.Entry<String, String> e : Map.of("connect", connect == null ? "" : connect, "request", request == null ? "" : request).entrySet()) {
                if(!e.getValue().isEmpty()) {
                    try {
                        Durations.parse(e.getValue());
                    } catch (final IllegalArgumentException ex) {
                        errorMessages.add(prefix + "." + e.getKey() + " is illegal: " + ex.getMessage());
                    }
                }
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(connect == null) {
                connect = "10s";
            }
            if(request == null) {
                request = "60s";
            }
        }
    }

    private final HttpClient client;
    private final Duration requestTimeout;
    private final AuthProvider auth;
    private final Set<String> allowedHosts;

    public HttpTransport(final Parameters parameters, final TimeoutParameters timeout, final AuthProvider auth) {
        final HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(Durations.parse(timeout.connect))
                .version(HttpClient.Version.valueOf(parameters.version.toUpperCase()))
                .followRedirects(HttpClient.Redirect.valueOf(parameters.followRedirects.toUpperCase()));
        if(parameters.proxy != null) {
            final int i = parameters.proxy.lastIndexOf(':');
            builder.proxy(ProxySelector.of(new InetSocketAddress(parameters.proxy.substring(0, i), Integer.parseInt(parameters.proxy.substring(i + 1)))));
        }
        this.client = builder.build();
        this.requestTimeout = Durations.parse(timeout.request);
        this.auth = auth == null ? AuthProvider.NONE : auth;
        this.allowedHosts = parameters.allowedHosts == null ? null : Set.copyOf(parameters.allowedHosts.stream().map(String::toLowerCase).toList());
    }

    public AuthProvider auth() {
        return auth;
    }

    /** Sends one request asynchronously; the future completes exceptionally on connect/timeout errors. */
    public CompletableFuture<OutboundRequest.Response> send(final OutboundRequest request) {
        final HttpRequest httpRequest;
        try {
            httpRequest = toHttpRequest(request);
        } catch (final Exception e) {
            return CompletableFuture.failedFuture(e);
        }
        final long start = System.currentTimeMillis();
        return client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                .thenApply(response -> new OutboundRequest.Response(
                        response.statusCode(),
                        response.headers().map(),
                        decode(response),
                        System.currentTimeMillis() - start));
    }

    /** Synchronous convenience (token endpoints, tests). */
    public OutboundRequest.Response sendSync(final OutboundRequest request) throws IOException {
        try {
            return send(request).get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (final java.util.concurrent.ExecutionException e) {
            throw new IOException(e.getCause() == null ? e : e.getCause());
        }
    }

    HttpRequest toHttpRequest(final OutboundRequest request) throws IOException {
        String url = request.url();
        final Map<String, String> authQuery = auth.queryParams();
        if(!authQuery.isEmpty()) {
            final StringBuilder sb = new StringBuilder(url);
            sb.append(url.contains("?") ? '&' : '?');
            sb.append(AuthProvider.formEncode(authQuery));
            url = sb.toString();
        }
        final URI uri = URI.create(url);
        if(allowedHosts != null && (uri.getHost() == null || !allowedHosts.contains(uri.getHost().toLowerCase()))) {
            throw new IllegalArgumentException("host " + uri.getHost() + " is not in allowedHosts " + allowedHosts + ": " + request.url());
        }
        final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(uri)
                .timeout(requestTimeout);
        for(final Map.Entry<String, String> header : request.headers().entrySet()) {
            if(header.getValue() != null && !RESTRICTED_HEADERS.contains(header.getKey().toLowerCase())) {
                builder.header(header.getKey(), header.getValue());
            }
        }
        for(final Map.Entry<String, String> header : auth.headers().entrySet()) {
            builder.setHeader(header.getKey(), header.getValue());
        }
        final HttpRequest.BodyPublisher publisher = request.body() == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofByteArray(request.body());
        builder.method(request.method().toUpperCase(), publisher);
        return builder.build();
    }

    private static byte[] decode(final HttpResponse<byte[]> response) {
        final byte[] body = response.body();
        if(body == null || body.length == 0) {
            return body;
        }
        final String encoding = response.headers().firstValue("Content-Encoding").orElse("");
        if(encoding.equalsIgnoreCase("gzip")) {
            try(final GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(body))) {
                return in.readAllBytes();
            } catch (final IOException e) {
                return body;
            }
        }
        return body;
    }

    public static byte[] gzip(final byte[] bytes) {
        try(final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try(final GZIPOutputStream gz = new GZIPOutputStream(out)) {
                gz.write(bytes);
            }
            return out.toByteArray();
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to gzip body", e);
        }
    }

    public static String text(final byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        // JDK HttpClient has close() since 21
        client.close();
    }
}
