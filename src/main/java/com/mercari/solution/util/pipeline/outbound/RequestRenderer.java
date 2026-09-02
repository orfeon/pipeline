package com.mercari.solution.util.pipeline.outbound;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.mercari.solution.module.MElement;
import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.pipeline.Serialize;
import com.mercari.solution.util.schema.converter.ElementToAvroConverter;
import com.mercari.solution.util.schema.converter.ElementToJsonConverter;
import freemarker.template.Template;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Turns one element (or one batch of elements, or nothing for signal-only firings) into an
 * {@link OutboundRequest}: pure, unit-testable request building shared by the http sink and
 * the http action.
 *
 * <p>Templates are compiled once per instance; templates that reference neither element fields nor
 * per-element variables are rendered once at {@link #setup()} (secret-bearing headers must not hit
 * Secret Manager per element). Headers referencing {@code __body} are rendered after the body
 * (signature headers).
 *
 * <p>Batch mode: per-request templates (url / params / headers) are rendered with the first
 * element's fields — callers validate at assembly time that they only reference the batch key
 * fields — plus {@code elements} (list of maps), {@code size} and {@code key}.
 */
public class RequestRenderer implements Serializable {

    public static final String VAR_TIMESTAMP = "__timestamp";
    public static final String VAR_SOURCE = "__source";
    public static final String VAR_ELEMENT = "__element";
    public static final String VAR_DOC = "__doc";
    public static final String VAR_BODY = "__body";
    /** Marker in {@code dynamicVariables}: every template is rendered per request (variables unknown at assembly). */
    public static final String DYNAMIC_ALL = "*";

    private static final Pattern PATTERN_DYNAMIC_VAR = Pattern
            .compile("\\$\\{[^}]*\\b(__timestamp|__source|__element|__doc|__body|elements|size|key)\\b[^}]*}");
    private static final Pattern PATTERN_BODY_VAR = Pattern.compile("\\$\\{[^}]*\\b__body\\b[^}]*}");

    /** Thrown when a rendered body exceeds body.maxBytes (callers may split a batch and retry). */
    public static class BodyTooLargeException extends IllegalArgumentException {
        public BodyTooLargeException(final String message) {
            super(message);
        }
    }

    private final String name;
    private final RequestSpec.Target target;
    private final RequestSpec.Body body;
    private final String batchKey;
    private final boolean batch;
    private final Schema inputSchema;
    private final Schema outputSchema;
    private final List<String> inputNames;
    private final List<String> templateArgs;
    private final Serialize serialize;
    private final Pattern dynamicVariablePattern;

    private transient Template urlTemplate;
    private transient String staticUrl;
    private transient Map<String, Template> paramTemplates;
    private transient Map<String, String> staticParams;
    private transient Map<String, Template> headerTemplates;
    private transient Map<String, Template> bodyHeaderTemplates;
    private transient Map<String, String> staticHeaders;
    private transient Template bodyTemplate;
    private transient Map<String, Template> partTemplates;
    private transient Map<String, Template> partFilenameTemplates;
    private transient Template keyTemplate;
    private transient org.apache.avro.Schema avroSchema;
    private transient long maxBytes;

    /**
     * @param batchKey   batch.key template (null when not batching on a key)
     * @param batch      whether requests carry several elements
     * @param inputSchema union schema of the inputs (null for signal-only firings)
     * @param outputSchema schema for avro / protobuf bodies
     * @param inputNames input step names (for {@code __source})
     */
    public RequestRenderer(
            final String name,
            final RequestSpec.Target target,
            final RequestSpec.Body body,
            final String batchKey,
            final boolean batch,
            final Schema inputSchema,
            final Schema outputSchema,
            final List<String> inputNames) {
        this(name, target, body, batchKey, batch, inputSchema, outputSchema, inputNames, List.of());
    }

    /**
     * @param extraTemplateTexts additional caller templates (e.g. the tasks sink's queue / id /
     *                           scheduleTime) whose element fields must be present in the
     *                           template values
     */
    public RequestRenderer(
            final String name,
            final RequestSpec.Target target,
            final RequestSpec.Body body,
            final String batchKey,
            final boolean batch,
            final Schema inputSchema,
            final Schema outputSchema,
            final List<String> inputNames,
            final List<String> extraTemplateTexts) {
        this(name, target, body, batchKey, batch, inputSchema, outputSchema, inputNames, extraTemplateTexts, Set.of());
    }

    /**
     * @param dynamicVariables names of caller-supplied per-request template variables (e.g. the
     *                         http source's loop vars); templates referencing them are rendered per
     *                         request instead of once at setup
     */
    public RequestRenderer(
            final String name,
            final RequestSpec.Target target,
            final RequestSpec.Body body,
            final String batchKey,
            final boolean batch,
            final Schema inputSchema,
            final Schema outputSchema,
            final List<String> inputNames,
            final List<String> extraTemplateTexts,
            final Set<String> dynamicVariables) {

        this.name = name;
        this.dynamicVariablePattern = dynamicVariables == null || dynamicVariables.isEmpty() ? null
                : dynamicVariables.contains(DYNAMIC_ALL) ? Pattern.compile("\\$\\{")
                : Pattern.compile("\\$\\{[^}]*\\b(" + String.join("|", dynamicVariables.stream().map(Pattern::quote).toList()) + ")\\b[^}]*}");
        this.target = target;
        this.body = body;
        this.batchKey = batchKey;
        this.batch = batch;
        this.inputSchema = inputSchema == null ? MElement.dummySchema() : inputSchema;
        this.outputSchema = outputSchema == null ? this.inputSchema : outputSchema;
        this.inputNames = inputNames == null ? List.of() : inputNames;

        final Set<String> args = new HashSet<>();
        final List<String> texts = new ArrayList<>(target.templateTexts());
        texts.add(body.template);
        texts.add(batchKey);
        texts.addAll(extraTemplateTexts);
        if(body.parts != null) {
            for(final RequestSpec.Part part : body.parts) {
                texts.add(part.template);
                texts.add(part.filename);
            }
        }
        for(final String text : texts) {
            if(text != null) {
                args.addAll(TemplateUtil.extractTemplateArgs(text, this.inputSchema));
            }
        }
        this.templateArgs = new ArrayList<>(args);
        this.serialize = switch (body.format) {
            case avro -> Serialize.of(Serialize.Format.avro, this.outputSchema);
            case protobuf -> Serialize.of(Serialize.Format.protobuf, this.outputSchema);
            default -> null;
        };
    }

    public boolean isBatch() {
        return batch;
    }

    public String method() {
        return target.method;
    }

    public void setup() {
        final Map<String, Object> staticValues = new HashMap<>();
        TemplateUtil.setFunctions(staticValues);

        if(TemplateUtil.isTemplateText(target.url) && !isStatic(target.url)) {
            this.urlTemplate = TemplateUtil.createStrictTemplate(name + ".url", target.url);
        } else {
            this.staticUrl = render(name + ".url", target.url, staticValues);
        }
        this.paramTemplates = new LinkedHashMap<>();
        this.staticParams = new LinkedHashMap<>();
        for(final Map.Entry<String, String> entry : target.params.entrySet()) {
            if(TemplateUtil.isTemplateText(entry.getValue()) && !isStatic(entry.getValue())) {
                paramTemplates.put(entry.getKey(), TemplateUtil.createStrictTemplate(name + ".param." + entry.getKey(), entry.getValue()));
            } else {
                staticParams.put(entry.getKey(), render(name + ".param." + entry.getKey(), entry.getValue(), staticValues));
            }
        }
        this.headerTemplates = new HashMap<>();
        this.bodyHeaderTemplates = new HashMap<>();
        this.staticHeaders = new HashMap<>();
        for(final Map.Entry<String, String> entry : target.headers.entrySet()) {
            if(entry.getValue() == null) {
                continue;
            }
            if(PATTERN_BODY_VAR.matcher(entry.getValue()).find()) {
                bodyHeaderTemplates.put(entry.getKey(), TemplateUtil.createStrictTemplate(name + ".header." + entry.getKey(), entry.getValue()));
            } else if(TemplateUtil.isTemplateText(entry.getValue()) && !isStatic(entry.getValue())) {
                headerTemplates.put(entry.getKey(), TemplateUtil.createStrictTemplate(name + ".header." + entry.getKey(), entry.getValue()));
            } else {
                staticHeaders.put(entry.getKey(), render(name + ".header." + entry.getKey(), entry.getValue(), staticValues));
            }
        }
        if(body.template != null) {
            this.bodyTemplate = TemplateUtil.createStrictTemplate(name + ".body", body.template);
        }
        this.partTemplates = new HashMap<>();
        this.partFilenameTemplates = new HashMap<>();
        if(body.parts != null) {
            for(final RequestSpec.Part part : body.parts) {
                if(part.template != null) {
                    partTemplates.put(part.name, TemplateUtil.createStrictTemplate(name + ".part." + part.name, part.template));
                }
                if(part.filename != null && TemplateUtil.isTemplateText(part.filename)) {
                    partFilenameTemplates.put(part.name, TemplateUtil.createStrictTemplate(name + ".part." + part.name + ".filename", part.filename));
                }
            }
        }
        if(batchKey != null) {
            this.keyTemplate = TemplateUtil.createStrictTemplate(name + ".batchKey", batchKey);
        }
        if(serialize != null) {
            serialize.setupSerialize();
        }
        if(RequestSpec.Format.avro.equals(body.format)) {
            this.avroSchema = outputSchema.getAvroSchema();
        }
        this.maxBytes = body.maxBytesValue();
    }

    /** True when the template references neither element fields nor per-element/batch variables. */
    private boolean isStatic(final String text) {
        return TemplateUtil.extractTemplateArgs(text, inputSchema).isEmpty()
                && !PATTERN_DYNAMIC_VAR.matcher(text).find()
                && (dynamicVariablePattern == null || !dynamicVariablePattern.matcher(text).find());
    }

    private static String render(final String name, final String text, final Map<String, Object> values) {
        if(text == null) {
            return null;
        }
        if(!TemplateUtil.isTemplateText(text)) {
            return text;
        }
        return TemplateUtil.executeStrictTemplate(TemplateUtil.createStrictTemplate(name, text), values);
    }

    /** Template context for a signal-only firing (no element): functions, timestamp, extra values. */
    public Map<String, Object> createTemplateValues(final Map<String, Object> extra) {
        final Map<String, Object> values = new HashMap<>();
        values.put(VAR_TIMESTAMP, Instant.now());
        values.put(VAR_SOURCE, "");
        if(extra != null) {
            values.putAll(extra);
        }
        TemplateUtil.setFunctions(values);
        return values;
    }

    public Map<String, Object> createTemplateValues(final MElement element) {
        final Map<String, Object> values = element.asStandardMap(inputSchema, templateArgs);
        final Map<String, Object> all = element.asStandardMap(inputSchema);
        values.put(VAR_ELEMENT, all);
        values.put(VAR_DOC, body.fields == null ? all : element.asStandardMap(inputSchema, body.fields));
        values.put(VAR_TIMESTAMP, Instant.ofEpochMilli(element.getEpochMillis()));
        values.put(VAR_SOURCE, element.getIndex() < inputNames.size() ? inputNames.get(element.getIndex()) : "");
        TemplateUtil.setFunctions(values);
        return values;
    }

    public Map<String, Object> createTemplateValues(final List<MElement> elements, final String key) {
        final Map<String, Object> values = elements.isEmpty() ? createTemplateValues((Map<String, Object>) null) : createTemplateValues(elements.get(0));
        final List<Map<String, Object>> maps = elements.stream()
                .map(e -> e.asStandardMap(inputSchema))
                .toList();
        values.put("elements", maps);
        values.put("size", maps.size());
        values.put("key", key);
        return values;
    }

    /** Renders the batch grouping key for one element (null when batch.key is omitted). */
    public String renderBatchKey(final MElement element) {
        if(keyTemplate == null) {
            return null;
        }
        return TemplateUtil.executeStrictTemplate(keyTemplate, createTemplateValues(element));
    }

    public OutboundRequest build(final MElement element) {
        return build(List.of(element), createTemplateValues(element));
    }

    public OutboundRequest build(final List<MElement> elements, final String key) {
        return build(elements, createTemplateValues(elements, key));
    }

    /** Builds from explicit template values (signal-only firings, or callers adding context). */
    public OutboundRequest build(final List<MElement> elements, final Map<String, Object> values) {
        String url = urlTemplate != null ? TemplateUtil.executeStrictTemplate(urlTemplate, values) : staticUrl;
        final Map<String, String> params = new LinkedHashMap<>(staticParams);
        for(final Map.Entry<String, Template> entry : paramTemplates.entrySet()) {
            params.put(entry.getKey(), TemplateUtil.executeStrictTemplate(entry.getValue(), values));
        }
        if(!params.isEmpty()) {
            url = url + (url.contains("?") ? "&" : "?") + AuthProvider.formEncode(params);
        }

        String multipartBoundary = null;
        byte[] bytes;
        if(RequestSpec.Format.multipart.equals(body.format)) {
            multipartBoundary = "----mercari-pipeline-" + UUID.randomUUID();
            bytes = createMultipartBody(elements, values, multipartBoundary);
        } else {
            bytes = createBody(elements, values);
        }
        // signature headers see the uncompressed body
        final String bodyText = bytes == null ? "" : new String(bytes, StandardCharsets.UTF_8);
        if(bytes != null && RequestSpec.Compression.gzip.equals(body.compression)) {
            bytes = HttpTransport.gzip(bytes);
        }
        if(maxBytes > 0 && bytes != null && bytes.length > maxBytes) {
            throw new BodyTooLargeException("request body size " + bytes.length + " exceeds body.maxBytes " + body.maxBytes);
        }
        final boolean sendsBody = bytes != null && !"GET".equals(target.method) && !"HEAD".equals(target.method);

        final Map<String, String> headers = new LinkedHashMap<>();
        if(sendsBody && multipartBoundary != null) {
            headers.put("Content-Type", "multipart/form-data; boundary=" + multipartBoundary);
        } else if(sendsBody && body.contentType != null) {
            headers.put("Content-Type", body.contentType);
        }
        if(sendsBody && RequestSpec.Compression.gzip.equals(body.compression)) {
            headers.put("Content-Encoding", "gzip");
        }
        headers.putAll(staticHeaders);
        for(final Map.Entry<String, Template> entry : headerTemplates.entrySet()) {
            final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), values);
            if(value != null) {
                headers.put(entry.getKey(), value);
            }
        }
        if(!bodyHeaderTemplates.isEmpty()) {
            values.put(VAR_BODY, bodyText);
            for(final Map.Entry<String, Template> entry : bodyHeaderTemplates.entrySet()) {
                final String value = TemplateUtil.executeStrictTemplate(entry.getValue(), values);
                if(value != null) {
                    headers.put(entry.getKey(), value);
                }
            }
        }
        return new OutboundRequest(url, target.method, headers, sendsBody ? bytes : null, elements.size());
    }

    private byte[] createBody(final List<MElement> elements, final Map<String, Object> values) {
        if(elements.isEmpty() && !RequestSpec.Format.template.equals(body.format) && !RequestSpec.Format.none.equals(body.format)) {
            // signal-only firing without a template: nothing to serialize
            return null;
        }
        return switch (body.format) {
            case none -> null;
            case json -> {
                if(!batch) {
                    yield bytes(toJson(elements.get(0)).toString());
                }
                final JsonArray array = new JsonArray();
                for(final MElement element : elements) {
                    array.add(toJson(element));
                }
                yield bytes(wrap(array.toString()));
            }
            case ndjson -> {
                final StringBuilder sb = new StringBuilder();
                for(final MElement element : elements) {
                    final String line = bodyTemplate != null
                            ? TemplateUtil.executeStrictTemplate(bodyTemplate, batch ? createTemplateValues(element) : values)
                            : toJson(element).toString();
                    if(line == null || line.isBlank()) {
                        continue;
                    }
                    sb.append(line.strip()).append('\n');
                }
                yield bytes(sb.toString());
            }
            case template -> bytes(TemplateUtil.executeStrictTemplate(bodyTemplate, values));
            case form -> {
                final Map<String, String> form = new LinkedHashMap<>();
                final Map<String, Object> map = body.fields == null
                        ? elements.get(0).asStandardMap(inputSchema)
                        : elements.get(0).asStandardMap(inputSchema, body.fields);
                for(final Map.Entry<String, Object> entry : map.entrySet()) {
                    if(entry.getValue() == null) {
                        if(!body.omitNulls) {
                            form.put(entry.getKey(), "");
                        }
                    } else {
                        form.put(entry.getKey(), entry.getValue().toString());
                    }
                }
                yield bytes(AuthProvider.formEncode(form));
            }
            case bytes -> bytesOf(elements.get(0).getPrimitiveValue(body.field));
            case multipart -> throw new IllegalStateException("multipart is built by createMultipartBody");
            case avro -> {
                if(!batch) {
                    yield serialize.serialize(elements.get(0));
                }
                try(final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
                    final org.apache.avro.file.DataFileWriter<org.apache.avro.generic.GenericRecord> writer =
                            new org.apache.avro.file.DataFileWriter<>(new org.apache.avro.generic.GenericDatumWriter<>(avroSchema))) {
                    writer.create(avroSchema, out);
                    for(final MElement element : elements) {
                        writer.append(ElementToAvroConverter.convert(avroSchema, element));
                    }
                    writer.flush();
                    yield out.toByteArray();
                } catch (final IOException e) {
                    throw new IllegalStateException("Failed to write avro container", e);
                }
            }
            case protobuf -> {
                if(!batch) {
                    yield serialize.serialize(elements.get(0));
                }
                try(final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream()) {
                    final com.google.protobuf.CodedOutputStream coded = com.google.protobuf.CodedOutputStream.newInstance(out);
                    for(final MElement element : elements) {
                        final byte[] b = serialize.serialize(element);
                        coded.writeUInt32NoTag(b.length);
                        coded.writeRawBytes(b);
                    }
                    coded.flush();
                    yield out.toByteArray();
                } catch (final IOException e) {
                    throw new IllegalStateException("Failed to write delimited protobuf", e);
                }
            }
        };
    }

    private static byte[] bytesOf(final Object value) {
        return switch (value) {
            case null -> null;
            case byte[] b -> b;
            case java.nio.ByteBuffer bb -> {
                final byte[] b = new byte[bb.remaining()];
                bb.duplicate().get(b);
                yield b;
            }
            case String s -> s.getBytes(StandardCharsets.UTF_8);
            default -> value.toString().getBytes(StandardCharsets.UTF_8);
        };
    }

    /** multipart/form-data (RFC 7578): one part per body.parts entry; field parts carry raw bytes, template parts rendered text. */
    private byte[] createMultipartBody(final List<MElement> elements, final Map<String, Object> values, final String boundary) {
        try(final java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream()) {
            for(final RequestSpec.Part part : body.parts) {
                final byte[] content;
                final String defaultType;
                if(part.field != null) {
                    content = elements.isEmpty() ? null : bytesOf(elements.get(0).getPrimitiveValue(part.field));
                    defaultType = "application/octet-stream";
                } else {
                    content = bytes(TemplateUtil.executeStrictTemplate(partTemplates.get(part.name), values));
                    defaultType = "text/plain; charset=utf-8";
                }
                if(content == null) {
                    continue;
                }
                final StringBuilder head = new StringBuilder();
                head.append("--").append(boundary).append("\r\n");
                head.append("Content-Disposition: form-data; name=\"").append(part.name).append('"');
                if(part.filename != null) {
                    final String filename = partFilenameTemplates.containsKey(part.name)
                            ? TemplateUtil.executeStrictTemplate(partFilenameTemplates.get(part.name), values)
                            : part.filename;
                    head.append("; filename=\"").append(filename.replace("\"", "%22")).append('"');
                }
                head.append("\r\n");
                head.append("Content-Type: ").append(part.contentType != null ? part.contentType : defaultType).append("\r\n\r\n");
                out.write(head.toString().getBytes(StandardCharsets.UTF_8));
                out.write(content);
                out.write("\r\n".getBytes(StandardCharsets.UTF_8));
            }
            out.write(("--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
            return out.toByteArray();
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to build multipart body", e);
        }
    }

    private String wrap(final String json) {
        return body.wrapper == null ? json : body.wrapper.replace("${body}", json);
    }

    private static byte[] bytes(final String text) {
        return text == null ? null : text.getBytes(StandardCharsets.UTF_8);
    }

    private JsonElement toJson(final MElement element) {
        final JsonObject json = body.fields == null
                ? ElementToJsonConverter.convert(inputSchema, element.asPrimitiveMap())
                : ElementToJsonConverter.convert(inputSchema, element.asPrimitiveMap(), body.fields);
        return body.omitNulls ? omitNulls(json) : json;
    }

    public static JsonElement omitNulls(final JsonElement element) {
        if(element == null || element.isJsonNull()) {
            return null;
        }
        if(element.isJsonObject()) {
            final JsonObject out = new JsonObject();
            for(final Map.Entry<String, JsonElement> entry : element.getAsJsonObject().entrySet()) {
                final JsonElement child = omitNulls(entry.getValue());
                if(child != null) {
                    out.add(entry.getKey(), child);
                }
            }
            return out;
        }
        if(element.isJsonArray()) {
            final JsonArray out = new JsonArray();
            for(final JsonElement child : element.getAsJsonArray()) {
                final JsonElement c = omitNulls(child);
                out.add(c == null ? JsonNull.INSTANCE : c);
            }
            return out;
        }
        return element;
    }
}
