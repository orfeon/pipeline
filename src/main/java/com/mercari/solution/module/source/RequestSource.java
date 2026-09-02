package com.mercari.solution.module.source;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.mercari.solution.MPipeline;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.schema.converter.JsonToElementConverter;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Turns the HTTP request body into source data when the pipeline runs in serve mode
 * (Cloud Run Service). The serve handler stores the body in the requestBody pipeline option,
 * and this source parses it as JSON against the declared schema at assembly time — a JSON
 * array becomes one element per entry, a single object becomes one element.
 *
 * Outside serve mode the same config stays runnable: pass --requestBody='[...]' on the
 * command line, or declare a sample parameter as fallback data.
 */
@Source.Module(name="request", schema=true)
public class RequestSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(RequestSource.class);

    private static class Parameters implements Serializable {

        // dot-notation path selecting a subtree of the body to read elements from (e.g. "items")
        private String path;
        // fallback body used when the requestBody pipeline option is absent
        // (local testing and config validation outside serve mode)
        private JsonElement sample;

    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        if(getSchema() == null) {
            throw new IllegalModuleException("request source module[" + getName() + "] requires parameters.schema");
        }

        final String body = Optional
                .ofNullable(begin.getPipeline().getOptions().as(MPipeline.MPipelineOptions.class).getRequestBody())
                .orElseGet(() -> Optional
                        .ofNullable(parameters.sample)
                        .map(JsonElement::toString)
                        .orElse(null));
        if(body == null) {
            throw new IllegalModuleException(
                    "request source module[" + getName() + "] found no request body."
                            + " It runs in HTTP serve mode (request body), via the --requestBody pipeline option,"
                            + " or with a parameters.sample fallback");
        }

        final List<String> elementJsons = parseElements(getName(), body, parameters.path);
        LOG.info("request source module[{}] received {} element(s)", getName(), elementJsons.size());

        final Schema outputSchema = getSchema()
                .withType(Optional.ofNullable(getOutputType()).orElse(DataType.ELEMENT));

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failuresTag = new TupleTag<>() {};

        final PCollectionTuple outputs = begin
                .apply("Seed", Create
                        .of(elementJsons)
                        .withCoder(StringUtf8Coder.of()))
                .apply("ParseElement", ParDo
                        .of(new ParseDoFn(getName(), outputSchema, getTimestampAttribute(), getLoggings(), failuresTag, getFailFast()))
                        .withOutputTags(outputTag, TupleTagList.of(failuresTag)));

        errorHandler.addError(outputs.get(failuresTag));

        return MCollectionTuple
                .of(outputs.get(outputTag), outputSchema);
    }

    private static List<String> parseElements(final String name, final String body, final String path) {
        JsonElement json;
        try {
            json = JsonParser.parseString(body);
        } catch (final Throwable e) {
            throw new IllegalModuleException("request source module[" + name + "] body is not valid JSON", e);
        }
        if(path != null && !path.isEmpty()) {
            for(final String field : path.split("\\.")) {
                if(!json.isJsonObject() || !json.getAsJsonObject().has(field)) {
                    throw new IllegalModuleException(
                            "request source module[" + name + "] body has no path: " + path);
                }
                json = json.getAsJsonObject().get(field);
            }
        }

        final List<String> elements = new ArrayList<>();
        if(json.isJsonArray()) {
            for(final JsonElement element : json.getAsJsonArray()) {
                if(!element.isJsonObject()) {
                    throw new IllegalModuleException(
                            "request source module[" + name + "] body array entries must be JSON objects: " + element);
                }
                elements.add(element.toString());
            }
        } else if(json.isJsonObject()) {
            elements.add(json.toString());
        } else {
            throw new IllegalModuleException(
                    "request source module[" + name + "] body must be a JSON object or an array of objects");
        }
        return elements;
    }

    private static class ParseDoFn extends DoFn<String, MElement> {

        private final String moduleName;
        private final Schema outputSchema;
        private final String timestampAttribute;
        private final Map<String, Logging> logging;
        private final TupleTag<BadRecord> failuresTag;
        private final boolean failFast;

        ParseDoFn(
                final String moduleName,
                final Schema outputSchema,
                final String timestampAttribute,
                final List<Logging> logging,
                final TupleTag<BadRecord> failuresTag,
                final boolean failFast) {

            this.moduleName = moduleName;
            this.outputSchema = outputSchema;
            this.timestampAttribute = timestampAttribute;
            this.logging = Logging.map(logging);
            this.failuresTag = failuresTag;
            this.failFast = failFast;
        }

        @Setup
        public void setup() {
            outputSchema.setup();
        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final String json = c.element();
            try {
                final Map<String, Object> values = JsonToElementConverter.convert(outputSchema.getFields(), json);

                final org.joda.time.Instant eventTime;
                if(timestampAttribute != null) {
                    eventTime = DateTimeUtil.toJodaInstant(values.get(timestampAttribute));
                } else {
                    // batch request data has no natural timestamp: use the processing (request) time
                    eventTime = org.joda.time.Instant.now();
                }

                final MElement output = MElement.of(values, eventTime).convert(outputSchema);
                c.outputWithTimestamp(output, eventTime);
                Logging.log(LOG, logging, "output", output);
            } catch (final Throwable e) {
                final Map<String, Object> failureValues = Map.of("body", Optional.ofNullable(json).orElse(""));
                final BadRecord badRecord = processError(
                        "Failed to parse request body element for module: " + moduleName, failureValues, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }
    }

}
