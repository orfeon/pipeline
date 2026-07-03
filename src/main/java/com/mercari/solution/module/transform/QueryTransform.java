package com.mercari.solution.module.transform;

import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.ParameterManagerUtil;
import com.mercari.solution.util.cloud.google.StorageUtil;
import com.mercari.solution.util.pipeline.Query;
import com.mercari.solution.util.pipeline.Union;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Runs a Calcite SQL statement over each input element, inside a DoFn.
 *
 * <p>Unlike the {@code beamsql} module (which plans SQL over whole PCollections and may
 * shuffle), this module evaluates the SQL per element as a bounded in-memory query:
 * the element is registered as a one-row table, and any array-of-struct fields can be
 * expanded with {@code UNNEST}/{@code LATERAL} so that aggregation, ORDER BY / LIMIT and
 * set operations run over the per-element collection. The evaluation never shuffles and
 * preserves the input windowing/timestamps, so batch and streaming behave identically.
 * One input element yields zero or more output rows (fan-out or fold, decided by the SQL).
 */
@Transform.Module(name="query")
public class QueryTransform extends Transform {

    private static class Parameters implements Serializable {

        private String sql;
        private String table;

        private void validate() {
            if(this.sql == null) {
                throw new IllegalModuleException("parameters.sql must not be null");
            }
        }

        private void setDefaults(Map<String, String> templateArgs) {
            sql = loadQuery(sql, templateArgs);
            if(table == null) {
                table = "INPUT";
            }
        }

        private String loadQuery(String sql, Map<String, String> templateArgs) {
            if(sql == null) {
                return null;
            }
            String query;
            if(sql.startsWith("gs://")) {
                LOG.info("sql parameter is GCS path: {}", sql);
                final String rawQuery = StorageUtil.readString(sql);
                query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
            } else if(ParameterManagerUtil.isParameterVersionResource(sql)) {
                LOG.info("sql parameter is Parameter Manager resource: {}", sql);
                final ParameterManagerUtil.Version version = ParameterManagerUtil.getParameterVersion(sql);
                if(version.payload == null) {
                    throw new IllegalArgumentException("sql resource does not exists for: " + sql);
                }
                query = new String(version.payload, StandardCharsets.UTF_8);
            } else if(sql.startsWith("data:")) {
                LOG.info("sql parameter is base64 encoded");
                query = new String(Base64.getDecoder().decode(sql.substring("data:".length())), StandardCharsets.UTF_8);
            } else {
                // Query text is not a valid file path on some platforms (e.g. newlines or ':' on Windows)
                Path path;
                try {
                    path = Paths.get(sql);
                } catch (final Throwable e) {
                    path = null;
                }
                if(path != null && Files.exists(path) && !Files.isDirectory(path)) {
                    try {
                        final String rawQuery = Files.readString(path, StandardCharsets.UTF_8);
                        query = TemplateUtil.executeStrictTemplate(rawQuery, templateArgs);
                    } catch (IOException e) {
                        query = sql;
                    }
                } else {
                    query = sql;
                }
            }
            return query;
        }
    }

    @Override
    public MCollectionTuple expand(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults(getTemplateArgs());

        final PCollection<MElement> input = inputs
                .apply("Union", Union.flatten()
                        .withWaits(getWaits())
                        .withStrategy(getStrategy()));
        final Schema inputSchema = Union.createUnionSchema(inputs);

        final Query query;
        try {
            query = Query.of(parameters.table, inputSchema, parameters.sql);
        } catch (final Throwable e) {
            throw new IllegalModuleException(
                    "query transform module[" + getName() + "] failed to plan sql: " + parameters.sql + ", cause: " + e.getMessage());
        }

        final Schema outputSchema = query.getOutputSchema();
        validateOutputSchema(outputSchema);

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failureTag = new TupleTag<>() {};

        final PCollectionTuple outputs = input
                .apply("Query", ParDo
                        .of(new QueryDoFn(query, parameters.table, getLoggings(), getFailFast(), failureTag))
                        .withOutputTags(outputTag, TupleTagList.of(failureTag)));

        errorHandler.addError(outputs.get(failureTag));

        return MCollectionTuple
                .of(outputs.get(outputTag), outputSchema);
    }

    // Auto-generated column names such as EXPR$0 are not legal field names for
    // downstream schema conversions (e.g. Avro). Require an explicit alias instead.
    private void validateOutputSchema(final Schema outputSchema) {
        for(final Schema.Field field : outputSchema.getFields()) {
            if(field.getName().contains("$")) {
                throw new IllegalModuleException(
                        "query transform module[" + getName() + "] output column '" + field.getName()
                                + "' has an auto-generated name. Add an explicit alias (e.g. `AS my_column`) in the select list");
            }
        }
    }

    private static class QueryDoFn extends DoFn<MElement, MElement> {

        private final Query query;
        private final String table;
        private final Map<String, Logging> logs;
        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;

        QueryDoFn(
                final Query query,
                final String table,
                final List<Logging> logs,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.query = query;
            this.table = table;
            this.logs = Logging.map(logs);
            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            query.setup();
        }

        @Teardown
        public void teardown() {
            query.teardown();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }
            try {
                Logging.log(LOG, logs, "input", input);
                final List<MElement> results = query.execute(Map.of(table, List.of(input)), c.timestamp());
                for(final MElement output : results) {
                    Logging.log(LOG, logs, "output", output);
                    c.output(output);
                }
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to execute query", input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

    }

}
