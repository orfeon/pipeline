package com.mercari.solution.module.action;

import com.mercari.solution.module.Action;
import com.mercari.solution.module.Schema;
import com.mercari.solution.module.Action.Trigger;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.module.MElement;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.schema.converter.MapToJsonConverter;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.MimeTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Action service that writes a small file from the triggering elements — the control-plane
 * counterpart of the storage sink. The storage sink writes datasets; this action writes
 * execution artifacts: result histories (e.g. the file list a storage sink emitted),
 * summary reports, or marker files (e.g. an empty {@code _SUCCESS} object with trigger: once).
 *
 * Content: with {@code content} set, the rendered template becomes the file body
 * (perElement: the element's fields, collect: {@code elements}/{@code size}); without it,
 * the elements are written as JSON Lines (one JSON object per element — empty file for once).
 *
 * Note: each execution creates/overwrites the object at {@code output}. With perElement,
 * include element fields in the output template so executions do not overwrite each other.
 * Writes are not idempotent-safe beyond being full overwrites: a retried bundle rewrites the
 * same content.
 */
@Action.Service(name = "storage")
public class StorageAction implements ActionService {

    private static final Logger LOG = LoggerFactory.getLogger(StorageAction.class);

    public static class Parameters implements Serializable {

        public String output;
        public String content;

        public List<String> validate(final String name) {
            final List<String> errorMessages = new ArrayList<>();
            if(this.output == null) {
                errorMessages.add("action module[" + name + "].parameters.output must not be null");
            }
            return errorMessages;
        }

    }

    private String name;
    private Trigger trigger;
    private Parameters parameters;


    @Override
    public void configure(final String name, final Trigger trigger, final String operation, final JsonObject parametersJson, final PipelineOptions options, final Schema inputSchema) {
        this.name = name;
        this.trigger = trigger;
        this.parameters = new Gson().fromJson(parametersJson, Parameters.class);
        if(this.parameters == null) {
            throw new IllegalModuleException("action module[" + name + "].parameters must not be empty");
        }
        final List<String> errorMessages = this.parameters.validate(name);
        if(!errorMessages.isEmpty()) {
            throw new IllegalModuleException(errorMessages);
        }
    }

    @Override
    public void setup() {

    }

    @Override
    public ActionResult execute(final List<MElement> elements) throws Exception {
        final Map<String, Object> data = switch (trigger) {
            case perElement -> elements.getFirst().asPrimitiveMap();
            case once, collect -> Action.createCollectTemplateData(elements);
        };
        final String path = template(parameters.output, data);
        final String body = createBody(data);

        final ResourceId resource = FileSystems.matchNewResource(path, false);
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        try(final WritableByteChannel channel = FileSystems.create(resource, MimeTypes.TEXT)) {
            channel.write(ByteBuffer.wrap(bytes));
        }
        LOG.info("action module[{}] wrote {} bytes to: {}", name, bytes.length, path);
        return ActionResult.of("write", path, "DONE", "bytes: " + bytes.length + ", elements: " + elements.size());
    }

    private String createBody(final Map<String, Object> data) {
        if(parameters.content != null) {
            return template(parameters.content, data);
        }
        // default body: JSON Lines of the elements' fields (empty for trigger: once)
        final StringBuilder sb = new StringBuilder();
        if(data.get("elements") instanceof List<?> elements) {
            for(final Object element : elements) {
                if(element instanceof Map<?, ?> map) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> values = (Map<String, Object>) map;
                    sb.append(MapToJsonConverter.convertObject(values)).append("\n");
                }
            }
        } else {
            sb.append(MapToJsonConverter.convertObject(data)).append("\n");
        }
        return sb.toString();
    }

    private static String template(final String text, final Map<String, Object> data) {
        if(!TemplateUtil.isTemplateText(text)) {
            return text;
        }
        return TemplateUtil.executeStrictTemplate(text, data);
    }

}
