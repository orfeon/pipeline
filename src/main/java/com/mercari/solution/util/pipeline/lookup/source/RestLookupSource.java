package com.mercari.solution.util.pipeline.lookup.source;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.pipeline.lookup.CalciteValues;
import com.mercari.solution.util.pipeline.lookup.LookupBatch;
import com.mercari.solution.util.pipeline.lookup.LookupKey;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import com.mercari.solution.util.pipeline.lookup.PerKeyLookup;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * REST/HTTP-backed {@link LookupSource}: exposes an HTTP API as key-driven lookup
 * tables. The whole request is built from {@code {column}} placeholder templates —
 * endpoint / method / header values / query params / body — whose placeholders name
 * the table's key columns; the caller decides per part whether it is static (bind
 * the key column to a literal in SQL) or dynamic (bind it to an input column).
 *
 * <p>One HTTP request is issued per distinct key tuple (point equality only).
 * {@code rowsFrom} is a JSON pointer to a response array for fan-out (one row per
 * element); without it the whole response is one row. HTTP 404 → no row (use a
 * LEFT JOIN for a default); other non-2xx → error. Key columns are filled from the
 * request key (the response need not echo them). Nested objects are supported as
 * {@code json} string columns; arrays of scalars as {@code array} columns.
 *
 * <p><b>Read-only / idempotent by contract</b>: {@code lookup()} runs many times in
 * arbitrary order — never wire side-effecting requests. Set {@code allowedHosts} to
 * block SSRF when an endpoint is input-derived.
 */
public class RestLookupSource extends LookupSource {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{([^{}]+)}");

    /** One exposed table = one HTTP request shape. */
    public static class TableConfig implements Serializable {

        private final String name;
        private final String endpoint;
        private final String method;
        private final Map<String, String> headers;
        private final Map<String, String> params;
        private final String body;
        private final List<String> keyFields;
        private final String rowsFrom;
        private final Schema schema;             // key columns + response columns
        private final int[] keyPosForField;      // row field → key tuple position, or -1

        private TableConfig(String name, String endpoint, String method,
                Map<String, String> headers, Map<String, String> params, String body,
                List<String> keyFields, String rowsFrom, Schema schema, int[] keyPosForField) {
            this.name = name;
            this.endpoint = endpoint;
            this.method = method;
            this.headers = headers;
            this.params = params;
            this.body = body;
            this.keyFields = keyFields;
            this.rowsFrom = rowsFrom;
            this.schema = schema;
            this.keyPosForField = keyPosForField;
        }

        public static TableBuilder builder() {
            return new TableBuilder();
        }
    }

    public static class TableBuilder {

        private String name;
        private String endpoint;
        private String method = "GET";
        private final Map<String, String> headers = new LinkedHashMap<>();
        private final Map<String, String> params = new LinkedHashMap<>();
        private String body;
        private List<String> keyFields;
        private String rowsFrom;
        private final List<Schema.Field> fields = new ArrayList<>();

        public TableBuilder withName(String name) {
            this.name = name;
            return this;
        }

        public TableBuilder withEndpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public TableBuilder withMethod(String method) {
            this.method = method;
            return this;
        }

        public TableBuilder withHeader(String name, String valueTemplate) {
            this.headers.put(name, valueTemplate);
            return this;
        }

        public TableBuilder withParam(String name, String valueTemplate) {
            this.params.put(name, valueTemplate);
            return this;
        }

        public TableBuilder withBody(String bodyTemplate) {
            this.body = bodyTemplate;
            return this;
        }

        /** Key columns in key order; defaults to the template placeholders in appearance order. */
        public TableBuilder withKeyFields(List<String> keyFields) {
            this.keyFields = new ArrayList<>(keyFields);
            return this;
        }

        /** JSON pointer (e.g. {@code /items}) to a response array for row fan-out. */
        public TableBuilder withRowsFrom(String rowsFrom) {
            this.rowsFrom = rowsFrom;
            return this;
        }

        /** Response columns (key columns may also be declared here to type them). */
        public TableBuilder withField(String name, Schema.FieldType fieldType) {
            this.fields.add(Schema.Field.of(name, fieldType));
            return this;
        }

        public TableBuilder withFields(List<Schema.Field> fields) {
            this.fields.addAll(fields);
            return this;
        }

        public TableConfig build() {
            if (name == null || endpoint == null) {
                throw new IllegalArgumentException("rest table requires name and endpoint");
            }
            List<String> keys = keyFields;
            if (keys == null || keys.isEmpty()) {
                final Set<String> placeholders = new LinkedHashSet<>();
                for (final String template : templates()) {
                    if (template == null) {
                        continue;
                    }
                    final Matcher matcher = PLACEHOLDER.matcher(template);
                    while (matcher.find()) {
                        placeholders.add(matcher.group(1));
                    }
                }
                keys = new ArrayList<>(placeholders);
            }
            if (keys.isEmpty()) {
                throw new IllegalArgumentException(
                        "rest table '" + name + "' has no key columns (no placeholders)");
            }
            // Row schema: key columns first (typed string unless declared in fields),
            // then the declared response fields.
            final Map<String, Schema.FieldType> declared = new LinkedHashMap<>();
            for (final Schema.Field field : fields) {
                declared.put(field.getName(), field.getFieldType());
            }
            final Schema.Builder schemaBuilder = Schema.builder();
            for (final String key : keys) {
                schemaBuilder.withField(key,
                        declared.getOrDefault(key, Schema.FieldType.STRING));
            }
            for (final Schema.Field field : fields) {
                if (!keys.contains(field.getName())) {
                    schemaBuilder.withField(field);
                }
            }
            final Schema schema = schemaBuilder.build();
            final int[] keyPosForField = new int[schema.countFields()];
            for (int i = 0; i < schema.countFields(); i++) {
                keyPosForField[i] = keys.indexOf(schema.getField(i).getName());
            }
            return new TableConfig(name, endpoint, method, Map.copyOf(headers),
                    Map.copyOf(params), body, List.copyOf(keys), rowsFrom, schema, keyPosForField);
        }

        private List<String> templates() {
            final List<String> all = new ArrayList<>();
            all.add(endpoint);
            all.add(method);
            all.addAll(headers.values());
            all.addAll(params.values());
            all.add(body);
            return all;
        }
    }

    private final String baseUrl;
    private final Map<String, String> defaultHeaders;
    private final Set<String> allowedHosts;
    private final long timeoutMillis;
    private final Map<String, TableConfig> tables;

    private transient HttpClient client;

    private RestLookupSource(Builder builder) {
        super(builder.name);
        this.baseUrl = builder.baseUrl;
        this.defaultHeaders = Map.copyOf(builder.defaultHeaders);
        final Set<String> hosts = new LinkedHashSet<>();
        for (final String host : builder.allowedHosts) {
            hosts.add(host.toLowerCase(Locale.ROOT));
        }
        this.allowedHosts = Set.copyOf(hosts);
        this.timeoutMillis = builder.timeoutMillis;
        final Map<String, TableConfig> map = new LinkedHashMap<>();
        for (final TableConfig table : builder.tables) {
            map.put(table.name, table);
        }
        if (map.isEmpty()) {
            throw new IllegalArgumentException("rest lookup source requires at least one table");
        }
        this.tables = map;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String name;
        private String baseUrl;
        private final Map<String, String> defaultHeaders = new LinkedHashMap<>();
        private final List<String> allowedHosts = new ArrayList<>();
        private long timeoutMillis = 60_000L;
        private final List<TableConfig> tables = new ArrayList<>();

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withBaseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
            return this;
        }

        public Builder withDefaultHeader(String name, String value) {
            this.defaultHeaders.put(name, value);
            return this;
        }

        public Builder withAllowedHosts(List<String> hosts) {
            this.allowedHosts.addAll(hosts);
            return this;
        }

        public Builder withTimeoutMillis(long timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
            return this;
        }

        public Builder withTable(TableConfig table) {
            this.tables.add(table);
            return this;
        }

        public RestLookupSource build() {
            return new RestLookupSource(this);
        }
    }

    @Override
    protected void setupInternal() {
        if (client == null) {
            client = HttpClient.newBuilder()
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build();
        }
    }

    @Override
    protected void closeInternal() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public Map<String, Schema> tableSchemas() {
        final Map<String, Schema> schemas = new LinkedHashMap<>();
        for (final TableConfig table : tables.values()) {
            schemas.put(table.name, table.schema);
        }
        return schemas;
    }

    @Override
    public List<LookupKey> keyCandidates(String table) {
        final TableConfig config = tables.get(table);
        return config == null ? List.of() : List.of(LookupKey.primaryKey(config.keyFields));
    }

    @Override
    public Iterable<Object[]> lookup(String table, String indexName, LookupBatch batch,
            int[] projects) {
        final TableConfig config = tables.get(table);
        if (config == null) {
            throw new IllegalStateException("unknown lookup table: " + getName() + "." + table);
        }
        final int[] outCols = projects != null
                ? projects : CalciteValues.allColumns(config.schema.countFields());
        return PerKeyLookup.run(batch, "REST table '" + getName() + "." + table + "'",
                keyValues -> request(config, keyValues),
                (node, keyValues) -> decodeRow(config, node, outCols, keyValues));
    }

    /** Issues the HTTP request for one key tuple and returns the JSON nodes to decode. */
    private List<JsonElement> request(TableConfig config, List<Object> keyValues) {
        final Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < config.keyFields.size(); i++) {
            values.put(config.keyFields.get(i), String.valueOf(keyValues.get(i)));
        }
        final String method =
                substitute(config.method, values, false).trim().toUpperCase(Locale.ROOT);
        final String url = buildUrl(config, values);
        checkHost(url);
        final Map<String, String> headers = new LinkedHashMap<>();
        for (final Map.Entry<String, String> header : config.headers.entrySet()) {
            headers.put(header.getKey(), substitute(header.getValue(), values, false));
        }
        final String body = config.body == null ? null : substitute(config.body, values, false);
        if (body != null && headers.keySet().stream()
                .noneMatch(h -> h.equalsIgnoreCase("content-type"))) {
            headers.put("Content-Type", "application/json");
        }

        final HttpResponse<String> response = send(method, url, headers, body);
        if (response.statusCode() == 404) {
            return List.of(); // treat "not found" as no hit (use LEFT JOIN for a default)
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("HTTP " + method + " " + url + " returned status "
                    + response.statusCode() + ": " + abbreviate(response.body()));
        }
        final JsonElement root = JsonParser.parseString(
                response.body() == null || response.body().isEmpty() ? "null" : response.body());
        return rowSources(config, root);
    }

    private HttpResponse<String> send(String method, String url, Map<String, String> headers,
            String body) {
        final HttpRequest.Builder builder;
        try {
            builder = HttpRequest.newBuilder(URI.create(url));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                    "Invalid request URL '" + url + "': " + e.getMessage(), e);
        }
        if (timeoutMillis > 0) {
            builder.timeout(Duration.ofMillis(timeoutMillis));
        }
        defaultHeaders.forEach(builder::header);
        headers.forEach(builder::header);
        builder.method(method, body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body));
        try {
            return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException(
                    "HTTP " + method + " " + url + " failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("HTTP " + method + " " + url + " was interrupted", e);
        }
    }

    private String buildUrl(TableConfig config, Map<String, String> values) {
        String url = substitute(config.endpoint, values, false);
        if (!isAbsolute(url) && baseUrl != null) {
            url = baseUrl + url;
        }
        if (!config.params.isEmpty()) {
            final StringBuilder query = new StringBuilder();
            for (final Map.Entry<String, String> param : config.params.entrySet()) {
                if (!query.isEmpty()) {
                    query.append('&');
                }
                query.append(param.getKey()).append('=')
                        .append(substitute(param.getValue(), values, true));
            }
            url += (url.indexOf('?') >= 0 ? "&" : "?") + query;
        }
        return url;
    }

    private void checkHost(String url) {
        if (allowedHosts.isEmpty()) {
            return;
        }
        final String host;
        try {
            host = URI.create(url).getHost();
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(
                    "Invalid request URL '" + url + "': " + e.getMessage(), e);
        }
        if (host == null || !allowedHosts.contains(host.toLowerCase(Locale.ROOT))) {
            throw new IllegalStateException("Host '" + host + "' of URL '" + url
                    + "' is not in the allowed hosts " + allowedHosts);
        }
    }

    /** Expands the response into the nodes that each become one row. */
    private static List<JsonElement> rowSources(TableConfig config, JsonElement root) {
        if (config.rowsFrom == null || config.rowsFrom.isEmpty()) {
            return List.of(root);
        }
        final JsonElement target = atPointer(root, config.rowsFrom);
        if (target == null || target.isJsonNull()) {
            return List.of();
        }
        if (!target.isJsonArray()) {
            throw new IllegalStateException("rowsFrom='" + config.rowsFrom + "' of REST table '"
                    + config.name + "' must point to a JSON array");
        }
        final List<JsonElement> sources = new ArrayList<>();
        for (final JsonElement element : target.getAsJsonArray()) {
            sources.add(element);
        }
        return sources;
    }

    /** Minimal JSON-pointer (RFC 6901) resolution: {@code /a/0/b}; returns null when missing. */
    private static JsonElement atPointer(JsonElement root, String pointer) {
        JsonElement current = root;
        for (final String rawToken : pointer.split("/")) {
            if (rawToken.isEmpty()) {
                continue;
            }
            final String token = rawToken.replace("~1", "/").replace("~0", "~");
            if (current == null) {
                return null;
            }
            if (current.isJsonObject()) {
                current = current.getAsJsonObject().get(token);
            } else if (current.isJsonArray()) {
                final JsonArray array = current.getAsJsonArray();
                try {
                    final int index = Integer.parseInt(token);
                    current = index >= 0 && index < array.size() ? array.get(index) : null;
                } catch (NumberFormatException e) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return current;
    }

    /**
     * Decodes one source node into a row in {@code outCols} order. Key columns are
     * filled from the request key (so they join back exactly); other columns are
     * read from the JSON by field name.
     */
    private static Object[] decodeRow(TableConfig config, JsonElement node, int[] outCols,
            List<Object> keyValues) {
        final Object[] row = new Object[outCols.length];
        for (int i = 0; i < outCols.length; i++) {
            final int col = outCols[i];
            final int keyPos = config.keyPosForField[col];
            if (keyPos >= 0) {
                row[i] = keyValues.get(keyPos);
            } else {
                final Schema.Field field = config.schema.getField(col);
                final JsonElement value = node != null && node.isJsonObject()
                        ? node.getAsJsonObject().get(field.getName()) : null;
                row[i] = decodeValue(value, field.getFieldType());
            }
        }
        return row;
    }

    /** JSON value → Calcite-internal value by field type. */
    private static Object decodeValue(JsonElement node, Schema.FieldType fieldType) {
        if (node == null || node.isJsonNull()) {
            return null;
        }
        return switch (fieldType.getType()) {
            case string -> node.isJsonPrimitive() ? node.getAsString() : node.toString();
            case json -> node.toString();
            case int8, int16, int32 -> node.getAsInt();
            case int64 -> node.getAsLong();
            case float32, float64 -> node.getAsDouble();
            case bool -> node.getAsBoolean();
            case decimal -> new BigDecimal(node.getAsString());
            case date -> decodeDate(node);
            case timestamp -> decodeTimestamp(node);
            case array -> {
                if (!node.isJsonArray()) {
                    throw new IllegalStateException(
                            "Expected a JSON array for an array column, but got: " + node);
                }
                final List<Object> list = new ArrayList<>();
                for (final JsonElement element : node.getAsJsonArray()) {
                    list.add(decodeValue(element, fieldType.getArrayValueType()));
                }
                yield list;
            }
            default -> throw new IllegalStateException(
                    "Unsupported REST result type: " + fieldType.getType()
                            + " (use 'json' for nested objects)");
        };
    }

    /** ISO-8601 date string → epoch-day int; a numeric node is taken as the epoch day. */
    private static int decodeDate(JsonElement node) {
        if (node.getAsJsonPrimitive().isNumber()) {
            return node.getAsInt();
        }
        return (int) LocalDate.parse(node.getAsString()).toEpochDay();
    }

    /** ISO-8601 timestamp → epoch-millis long; a numeric node is taken as epoch millis. */
    private static long decodeTimestamp(JsonElement node) {
        if (node.getAsJsonPrimitive().isNumber()) {
            return node.getAsLong();
        }
        final String text = node.getAsString();
        try {
            return Instant.parse(text).toEpochMilli();
        } catch (Exception ignore) {
            // not an instant with a trailing 'Z'
        }
        try {
            return OffsetDateTime.parse(text).toInstant().toEpochMilli();
        } catch (Exception ignore) {
            // not an offset date-time
        }
        try {
            return LocalDateTime.parse(text).toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot parse '" + text + "' as a TIMESTAMP", e);
        }
    }

    /**
     * Replaces each {@code {name}} with {@code values.get(name)}. When
     * {@code urlEncode} is true (query-string values) the substituted value is
     * URL-encoded; otherwise (URL/path, header, body) it is inserted verbatim.
     */
    private static String substitute(String template, Map<String, String> values,
            boolean urlEncode) {
        final Matcher matcher = PLACEHOLDER.matcher(template);
        final StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            final String name = matcher.group(1);
            if (!values.containsKey(name)) {
                throw new IllegalStateException("Template placeholder '{" + name
                        + "}' has no value (not a declared key column)");
            }
            final String value = values.get(name);
            final String replacement = value == null ? ""
                    : urlEncode ? URLEncoder.encode(value, StandardCharsets.UTF_8) : value;
            matcher.appendReplacement(out, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(out);
        return out.toString();
    }

    private static boolean isAbsolute(String url) {
        final String lower = url.toLowerCase(Locale.ROOT);
        return lower.startsWith("http://") || lower.startsWith("https://");
    }

    private static String abbreviate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 200 ? body : body.substring(0, 200) + "...";
    }
}
