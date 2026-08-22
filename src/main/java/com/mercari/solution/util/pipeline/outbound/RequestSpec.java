package com.mercari.solution.util.pipeline.outbound;

import com.mercari.solution.module.Schema;
import com.mercari.solution.util.TemplateUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Configuration of an outbound HTTP request shared by the http sink, action.http (and the tasks
 * sink): {@code target} (url / method / params / headers / auth) and {@code body} (serialization).
 */
public final class RequestSpec {

    private static final Pattern PATTERN_STATIC_ORIGIN = Pattern.compile("^(https?://[^/?#$]+)([/?#].*)?$");

    private RequestSpec() {}

    public enum Format {
        json,
        ndjson,
        avro,
        protobuf,
        template,
        form,
        multipart,
        bytes,
        none
    }

    /** One part of a multipart/form-data body. */
    public static class Part implements Serializable {
        public String name;
        public String field;        // record field (BYTES or STRING) as the part content
        public String template;     // or a FreeMarker template as the part content
        public String filename;     // optional; template
        public String contentType;  // optional (default: application/octet-stream for field, text/plain for template)
    }

    public enum Compression {
        none,
        gzip
    }

    public static class Target implements Serializable {

        public String url;
        public String method;
        public Map<String, String> params;
        public Map<String, String> headers;
        public AuthProvider.Parameters auth;

        public List<String> validate(final String prefix, final Schema inputSchema, final boolean allowedHostsGiven) {
            final List<String> errorMessages = new ArrayList<>();
            if(url == null) {
                errorMessages.add(prefix + ".url must not be null");
            } else if(!url.startsWith("https://") && !url.startsWith("http://") && !TemplateUtil.isTemplateText(url)) {
                errorMessages.add(prefix + ".url must start with https:// or http:// but: " + url);
            }
            if(auth != null) {
                errorMessages.addAll(auth.validate(prefix + ".auth"));
                if(!auth.isNone() && url != null && staticOrigin(url) == null && !allowedHostsGiven) {
                    // host comes from element data: the caller must pin the hosts auth headers may go to
                    errorMessages.add("http.allowedHosts is required when " + prefix + ".auth is set and the host part of " + prefix + ".url is a template");
                }
                if(!auth.isNone() && inputSchema != null) {
                    for(final String text : authTexts(auth)) {
                        if(text != null && !TemplateUtil.extractTemplateArgs(text, inputSchema).isEmpty()) {
                            errorMessages.add(prefix + ".auth values must not reference element fields: " + text);
                        }
                    }
                }
            }
            return errorMessages;
        }

        private static List<String> authTexts(final AuthProvider.Parameters auth) {
            return Arrays.asList(auth.username, auth.password, auth.token, auth.name, auth.value,
                    auth.tokenUrl, auth.clientId, auth.clientSecret, auth.scope, auth.audience, auth.serviceAccount,
                    auth.issuer, auth.subject, auth.privateKey, auth.keyId, auth.refreshToken);
        }

        public void setDefaults() {
            if(method == null) {
                method = "POST";
            }
            method = method.toUpperCase();
            if(headers == null) {
                headers = new HashMap<>();
            }
            if(params == null) {
                params = new HashMap<>();
            }
            if(auth == null) {
                auth = new AuthProvider.Parameters();
            }
            auth.setDefaults();
        }

        /** All template texts of the target (for template-arg extraction). */
        public List<String> templateTexts() {
            final List<String> texts = new ArrayList<>();
            texts.add(url);
            texts.addAll(headers.values());
            texts.addAll(params.values());
            return texts;
        }
    }

    public static class Body implements Serializable {

        public Format format;
        public String template;
        public List<String> fields;
        public String wrapper;
        public Boolean omitNulls;
        public String maxBytes;
        public Compression compression;
        public String field;
        public String contentType;
        public List<Part> parts;

        public List<String> validate(final String prefix, final Schema inputSchema) {
            final List<String> errorMessages = new ArrayList<>();
            if(Format.multipart.equals(format)) {
                if(parts == null || parts.isEmpty()) {
                    errorMessages.add(prefix + ".parts must not be empty when body.format is multipart");
                } else {
                    for(final Part part : parts) {
                        if(part.name == null) {
                            errorMessages.add(prefix + ".parts[].name must not be null");
                        }
                        if((part.field == null) == (part.template == null)) {
                            errorMessages.add(prefix + ".parts[" + part.name + "] requires exactly one of field / template");
                        }
                        if(part.field != null && inputSchema != null && !inputSchema.hasField(part.field)) {
                            errorMessages.add(prefix + ".parts[" + part.name + "].field " + part.field + " is not in input schema");
                        }
                    }
                }
            }
            if(Format.template.equals(format) && template == null) {
                errorMessages.add(prefix + ".template must not be null when body.format is template");
            }
            if(Format.bytes.equals(format)) {
                if(field == null) {
                    errorMessages.add(prefix + ".field must not be null when body.format is bytes");
                } else if(inputSchema != null && !inputSchema.hasField(field)) {
                    errorMessages.add(prefix + ".field " + field + " is not in input schema");
                }
            }
            if(maxBytes != null) {
                try {
                    Durations.parseBytes(maxBytes);
                } catch (final IllegalArgumentException e) {
                    errorMessages.add(prefix + ".maxBytes is illegal: " + e.getMessage());
                }
            }
            if(fields != null && inputSchema != null) {
                for(final String f : fields) {
                    if(!inputSchema.hasField(f)) {
                        errorMessages.add(prefix + ".fields " + f + " is not in input schema");
                    }
                }
            }
            if(wrapper != null && !wrapper.contains("${body}")) {
                errorMessages.add(prefix + ".wrapper must contain ${body}");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(format == null) {
                format = template != null ? Format.template : Format.json;
            }
            if(omitNulls == null) {
                omitNulls = false;
            }
            if(compression == null) {
                compression = Compression.none;
            }
            if(contentType == null) {
                contentType = switch (format) {
                    case json -> "application/json";
                    case ndjson -> "application/x-ndjson";
                    case form -> "application/x-www-form-urlencoded";
                    case avro -> "avro/binary";
                    case protobuf -> "application/x-protobuf";
                    case bytes -> "application/octet-stream";
                    case multipart -> null; // set per request (boundary)
                    case template -> "application/json";
                    case none -> null;
                };
            }
        }

        public long maxBytesValue() {
            return maxBytes == null ? -1L : Durations.parseBytes(maxBytes);
        }
    }

    /** Scheme + host of a url whose host part contains no template, or null. */
    public static String staticOrigin(final String url) {
        if(url == null) {
            return null;
        }
        final var m = PATTERN_STATIC_ORIGIN.matcher(url);
        if(!m.matches()) {
            return null;
        }
        return m.group(1);
    }
}
