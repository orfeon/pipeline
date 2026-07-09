package com.mercari.solution.module.transform;

import com.mercari.solution.module.*;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.ParameterManagerUtil;
import com.mercari.solution.util.domain.file.ResourceUtil;
import com.mercari.solution.util.schema.RowSchemaUtil;
import com.mercari.solution.util.schema.converter.ElementToRowConverter;
import com.mercari.solution.util.domain.sql.beamsql.udf.AggregateFunctions;
import com.mercari.solution.util.domain.sql.beamsql.udf.ArrayFunctions;
import com.mercari.solution.util.domain.sql.beamsql.udf.MathFunctions;
import com.mercari.solution.util.pipeline.lookup.LookupSeekableTable;
import com.mercari.solution.util.pipeline.lookup.LookupSource;
import com.mercari.solution.util.pipeline.lookup.source.LookupSourceConfig;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Transform.Module(name="beamsql")
public class BeamSQLTransform extends Transform {

    private static class Parameters implements Serializable {

        private String sql;
        private String ddl;
        private Map<String, String> namedParameters;
        private List<String> positionalParameters;
        private Boolean autoLoading;
        private List<LookupSourceConfig.SourceParameters> sources;

        private void validate() {
            if(this.sql == null) {
                throw new IllegalModuleException("parameters.sql must not be null");
            }
            LookupSourceConfig.validate(this.sources);
        }

        private void setDefaults(Map<String, String> templateArgs) {
            sql = loadQuery(sql, templateArgs);
            ddl = loadQuery(ddl, templateArgs);
            if(namedParameters == null) {
                namedParameters = new HashMap<>();
            }
            if(positionalParameters == null) {
                positionalParameters = new ArrayList<>();
            }
            if(sources == null) {
                sources = new ArrayList<>();
            }
        }

        private String loadQuery(String sql, Map<String, String> templateArgs) {
            if(sql == null) {
                return null;
            }
            String query;
            if(ResourceUtil.isStorageUri(sql)) {
                LOG.info("sql parameter is storage path: {}", sql);
                final String rawQuery = ResourceUtil.readString(sql);
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
                query = new String(Base64.getDecoder().decode(sql), StandardCharsets.UTF_8);
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

        final PCollectionTuple input = createInput(inputs, errorHandler);
        final PCollection<Row> output = input
                .apply("SQLTransform", createTransform(parameters, errorHandler));

        final TupleTag<MElement> outputTag = new TupleTag<>() {};
        final TupleTag<BadRecord> failuresTag = new TupleTag<>() {};

        final PCollectionTuple outputs = output
                .apply("ConvertElement", ParDo
                        .of(new ConvertElementDoFn(getLoggings(), getFailFast(), failuresTag))
                        .withOutputTags(outputTag, TupleTagList.of(failuresTag)));

        errorHandler.addError(outputs.get(failuresTag));

        return MCollectionTuple
                .of(outputs.get(outputTag), Schema.of(output.getSchema()));
    }

    private PCollectionTuple createInput(
            final MCollectionTuple inputs,
            final MErrorHandler errorHandler) {

        PCollectionTuple tuple = PCollectionTuple.empty(inputs.getPipeline());
        for(final Map.Entry<String, MCollection> entry : inputs.asCollectionMap().entrySet()) {
            Schema schema = entry.getValue().getSchema();
            schema.setup();
            schema = convertSchema(schema);
            schema.setup(DataType.ROW);

            final TupleTag<Row> outputTag = new TupleTag<>() {};
            final TupleTag<BadRecord> failuresTag = new TupleTag<>() {};

            final PCollectionTuple rowTuple = entry.getValue()
                    .apply("ConvertRow" + entry.getKey(), ParDo
                            .of(new ConvertRowDoFn(entry.getKey(), schema, getLoggings(), getFailFast(), failuresTag))
                            .withOutputTags(outputTag, TupleTagList.of(failuresTag)));

            PCollection<Row> row = rowTuple
                    .get(outputTag)
                    .setCoder(RowCoder.of(schema.getRowSchema()))
                    .setRowSchema(schema.getRowSchema());

            if(getStrategy() != null) {
                row = row.apply("WithWindow" + entry.getKey(), getStrategy().createWindow());
            }
            tuple = tuple.and(entry.getKey(), row);

            errorHandler.addError(rowTuple.get(failuresTag));
        }

        return tuple;
    }

    private SqlTransform createTransform(
            final Parameters parameters,
            final MErrorHandler errorHandler) {

        SqlTransform transform = SqlTransform
                .query(parameters.sql)
                .withQueryPlannerClass(org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner.class);
        if(parameters.ddl != null) {
            transform = transform.withDdlString(parameters.ddl);
        }
        if(!parameters.namedParameters.isEmpty()) {
            transform = transform.withNamedParameters(parameters.namedParameters);
        } else if(!parameters.positionalParameters.isEmpty()) {
            transform = transform.withPositionalParameters(parameters.positionalParameters);
        }
        if(parameters.autoLoading != null) {
            transform = transform.withAutoLoading(parameters.autoLoading);
        }

        // External lookup sources: each source's tables are registered as
        // seekable tables (sourceName.tableName in SQL); a join against one is
        // planned as an inline lookup, one seekRow per input row. Metadata is
        // derived here once (the launcher needs connectivity); workers re-open
        // clients via the table's setUp.
        for(final LookupSource source : LookupSourceConfig.createSources(parameters.sources)) {
            try {
                source.setup();
                transform = transform.withTableProvider(source.getName(),
                        new ReadOnlyTableProvider(source.getName(),
                                LookupSeekableTable.tablesOf(source)));
            } finally {
                source.close();
            }
        }

        transform = transform
                // Math UDFs
                .registerUdf("MDT_GREATEST_INT64", MathFunctions.GreatestInt64Fn.class)
                .registerUdf("MDT_GREATEST_FLOAT64", MathFunctions.GreatestFloat64Fn.class)
                .registerUdf("MDT_LEAST_INT64", MathFunctions.LeastInt64Fn.class)
                .registerUdf("MDT_LEAST_FLOAT64", MathFunctions.LeastFloat64Fn.class)
                .registerUdf("MDT_GENERATE_UUID", MathFunctions.GenerateUUIDFn.class)
                // Array UDFs
                .registerUdf("MDT_CONTAINS_ALL_INT64", ArrayFunctions.ContainsAllInt64sFn.class)
                .registerUdf("MDT_CONTAINS_ALL_STRING", ArrayFunctions.ContainsAllStringsFn.class)
                // UDAFs
                .registerUdaf("MDT_ARRAY_AGG_INT64", new AggregateFunctions.ArrayAggInt64Fn())
                .registerUdaf("MDT_ARRAY_AGG_STRING", new AggregateFunctions.ArrayAggStringFn())
                .registerUdaf("MDT_ARRAY_AGG_DISTINCT_STRING", new AggregateFunctions.ArrayAggDistinctStringFn())
                .registerUdaf("MDT_ARRAY_AGG_DISTINCT_FLOAT64", new AggregateFunctions.ArrayAggDistinctFloat64Fn())
                .registerUdaf("MDT_ARRAY_AGG_DISTINCT_INT64", new AggregateFunctions.ArrayAggDistinctInt64Fn())
                .registerUdaf("MDT_COUNT_DISTINCT_STRING", new AggregateFunctions.CountDistinctStringFn())
                .registerUdaf("MDT_COUNT_DISTINCT_FLOAT64", new AggregateFunctions.CountDistinctFloat64Fn())
                .registerUdaf("MDT_COUNT_DISTINCT_INT64", new AggregateFunctions.CountDistinctInt64Fn());

        errorHandler.apply(transform);

        return transform;
    }

    private static class ConvertRowDoFn extends DoFn<MElement, Row> {

        private final String sourceName;

        private final Schema schema;

        private final Map<String, Logging> logging;

        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;

        ConvertRowDoFn(
                final String sourceName,
                final Schema schema,
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.sourceName = sourceName;

            this.schema = schema;

            this.logging = Logging.map(logging);
            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @Setup
        public void setup() {
            this.schema.setup();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final MElement input = c.element();
            if(input == null) {
                return;
            }

            try {
                Logging.log(LOG, logging, "input", input);
                final Row row = ElementToRowConverter.convert(schema, input);
                c.output(row);
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to convert to row from: " + sourceName, input, e, failFast);
                c.output(failuresTag, badRecord);
            }
        }

    }

    private static class ConvertElementDoFn extends DoFn<Row, MElement> {

        private final Map<String, Logging> logging;

        private final boolean failFast;
        private final TupleTag<BadRecord> failuresTag;

        ConvertElementDoFn(
                final List<Logging> logging,
                final boolean failFast,
                final TupleTag<BadRecord> failuresTag) {

            this.logging = Logging.map(logging);
            this.failFast = failFast;
            this.failuresTag = failuresTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final Row input = c.element();
            if(input == null) {
                return;
            }

            try {
                final MElement output = MElement.of(input, c.timestamp());
                c.output(output);
                Logging.log(LOG, logging, "output", output);
            } catch (final Throwable e) {
                final BadRecord badRecord = processError("Failed to convert from row to element", RowSchemaUtil.asPrimitiveMap(input), e, failFast);
                c.output(failuresTag, badRecord);
            }

        }

    }

    private static Schema convertSchema(Schema schema) {
        var builder = Schema.builder();
        for(final Schema.Field field : schema.getFields()) {
            builder.withField(field.getName(), convertFieldType(field.getFieldType()));
        }
        return builder.build();
    }

    private static Schema.FieldType convertFieldType(Schema.FieldType fieldType) {
        return switch (fieldType.getType()) {
            case enumeration -> Schema.FieldType.STRING.withNullable(fieldType.getNullable());
            case element -> Schema.FieldType.element(convertSchema(fieldType.getElementSchema()));
            case array -> Schema.FieldType.array(convertFieldType(fieldType.getArrayValueType()));
            case map -> Schema.FieldType.map(convertFieldType(fieldType.getMapValueType()));
            default -> fieldType;
        };
    }

}
