package com.mercari.solution.module.source;

import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.mercari.solution.config.options.DataflowOptions;
import com.mercari.solution.module.*;
import com.mercari.solution.util.DateTimeUtil;
import com.mercari.solution.util.coder.ElementCoder;
import com.mercari.solution.util.domain.db.split.RestrictionRecord;
import com.mercari.solution.util.domain.db.split.SeekSource;
import com.mercari.solution.util.schema.converter.ResultSetToRecordConverter;
import com.mercari.solution.util.domain.db.JdbcUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.SecretManagerUtil;
import com.mercari.solution.util.cloud.google.StorageUtil;
import com.mercari.solution.util.schema.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.*;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.util.*;


@Source.Module(name="jdbc")
public class JdbcSource extends Source {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);

    private static class Parameters implements Serializable {

        private String url;
        private String driver;
        private String user;
        private String password;

        // For query extraction
        private String query;
        private List<String> prepareCalls;
        private List<PrepareParameterQuery> prepareParameterQueries;

        // For table extraction
        private String table;
        private List<String> seekFields;
        private String select;
        private List<String> excludeFields;
        private Integer fetchSize;
        private Boolean enableSplit;
        private Boolean enableInitialSplit;
        private Integer initialSplitSize;
        private Map<String, String> collations;

        // common
        private Map<String, String> properties;
        private Integer transactionIsolation;

        public void validate() {

            final List<String> errorMessages = new ArrayList<>();
            if(url == null) {
                errorMessages.add("parameters.url must not be null");
            }
            if(driver == null) {
                errorMessages.add("parameters.driver must not be null");
            }
            if(user != null && password == null) {
                errorMessages.add("parameters.password must not be null");
            }

            if(query == null && table == null) {
                errorMessages.add("parameters.query or parameters.table must not be null");
            } else if(query != null && table != null) {
                errorMessages.add("parameters.query or parameters.table only one of " + query + " : " + table + " must be specified");
            }

            if(prepareParameterQueries != null) {
                for(final PrepareParameterQuery preprocessQuery : prepareParameterQueries) {
                    errorMessages.addAll(preprocessQuery.validate());
                }
            }

            if(!errorMessages.isEmpty()) {
                throw new IllegalModuleException(errorMessages);
            }
        }

        public void setDefaults() {
            if(prepareCalls == null) {
                prepareCalls = new ArrayList<>();
            }
            if(prepareParameterQueries == null) {
                prepareParameterQueries = new ArrayList<>();
            } else {
                for(final PrepareParameterQuery prepareParameterQuery : prepareParameterQueries) {
                    prepareParameterQuery.setDefaults();
                }
            }
            if(select == null) {
                select = "*";
            }
            if(seekFields == null) {
                seekFields = new ArrayList<>();
            }
            if(excludeFields == null) {
                excludeFields = new ArrayList<>();
            }
            if(fetchSize == null) {
                fetchSize = 50_000;
            }
            if(enableSplit == null) {
                enableSplit = false;
            }
            if(enableInitialSplit == null) {
                enableInitialSplit = false;
            }
            if(initialSplitSize == null) {
                initialSplitSize = 10;
            }
            if(collations == null) {
                collations = new HashMap<>();
            }

            if(properties == null) {
                properties = new HashMap<>();
            }
            if(transactionIsolation == null) {
                transactionIsolation = 2;
            }
        }

        public void replaceParameters(final Pipeline pipeline) {

            if(query != null && query.startsWith("gs://")) {
                query = StorageUtil.readString(query);
            }

            if(user == null) {
                final String serviceAccount = DataflowOptions.getServiceAccount(pipeline.getOptions());
                LOG.info("Using worker service account: '{}' for database user", serviceAccount);
                user = serviceAccount.replace(".gserviceaccount.com", "");
                password = "dummy";
                if(!url.contains("enableIamAuth")) {
                    url = url + "&enableIamAuth=true";
                }
            } else if(SecretManagerUtil.isSecretName(user) || SecretManagerUtil.isSecretName(password)) {
                LOG.info("parameters.user|password is secret resource.");
                try(final SecretManagerServiceClient secretClient = SecretManagerUtil.createClient()) {
                    if(SecretManagerUtil.isSecretName(user)) {
                        user = SecretManagerUtil.getSecret(secretClient, user).toStringUtf8();
                    }
                    if(SecretManagerUtil.isSecretName(password)) {
                        password = SecretManagerUtil.getSecret(secretClient, password).toStringUtf8();
                    }
                }
            } else {
                LOG.info("The both parameters.user and password are plain text.");
            }
        }

    }

    public static class PrepareParameterQuery implements Serializable {

        private String query;
        private List<String> prepareCalls;

        public String getQuery() {
            return query;
        }

        public List<String> getPrepareCalls() {
            return prepareCalls;
        }

        public List<String> validate() {
            final List<String> errorMessages = new ArrayList<>();
            if(getQuery() == null) {
                errorMessages.add("Jdbc source module preprocessQuery requires query parameter");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(this.prepareCalls == null) {
                this.prepareCalls = new ArrayList<>();
            }
        }

    }

    @Override
    public MCollectionTuple expand(
            final PBegin begin,
            final MErrorHandler errorHandler) {

        final Parameters parameters = getParameters(Parameters.class);
        parameters.validate();
        parameters.setDefaults();
        parameters.replaceParameters(begin.getPipeline());

        try {
            final Schema outputAvroSchema;
            final PCollection<GenericRecord> output;
            final PCollection<BadRecord> failures;
            final PCollection<MElement> restrictions;
            if(parameters.query != null) {
                outputAvroSchema = Optional
                        .ofNullable(getSchema())
                        .map(com.mercari.solution.module.Schema::getAvroSchema)
                        .orElseGet(() -> getQuerySchema(parameters, getTemplateArgs()));
                output = begin
                        .apply("Query", new JdbcBatchQuerySource(
                                parameters, getTimestampAttribute(), getTimestampDefault(), getTemplateArgs(), outputAvroSchema))
                        .setCoder(AvroCoder.of(outputAvroSchema));
                failures = null; // TODO
                restrictions = null;
            } else if(parameters.table != null) {
                final SeekSource source = SeekSource.of(
                        parameters.table, parameters.select, parameters.seekFields, parameters.fetchSize,
                        parameters.driver, parameters.url, parameters.user, parameters.password,
                        parameters.enableSplit, parameters.enableInitialSplit, parameters.initialSplitSize,
                        parameters.properties, parameters.collations,
                        getLoggings(), getFailFast());
                outputAvroSchema = source.getOutputAvroSchema();

                final PCollectionTuple outputs = begin.apply("ReadTable", source);

                output = outputs.get(source.outputTag);
                failures = outputs.get(source.failureTag);
                restrictions = outputs.get(source.restrictionTag)
                        .apply("ConvertToElement", ParDo.of(new SeekSource.FormatRestrictionDoFn()));
            } else {
                throw new IllegalArgumentException("Jdbc source module: " + getName() + " does not contain parameter both query and table");
            }
            LOG.info("{} outputSchema: {}", getName(), outputAvroSchema);

            final PCollection<MElement> element = output
                    .apply("Convert", ParDo.of(new ConvertDoFn(getTimestampAttribute(), getTimestampDefault())));

            MCollectionTuple tuple = MCollectionTuple
                    .of(element, com.mercari.solution.module.Schema.of(outputAvroSchema));

            if(restrictions != null) {
                final com.mercari.solution.module.Schema schema = com.mercari.solution.module.Schema.of(RestrictionRecord.createAvroSchema());
                tuple = tuple.and("restriction", restrictions.setCoder(ElementCoder.of(schema)), schema);
            }
            if(failures != null) {
                errorHandler.addError(failures);
            }
            return tuple;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Schema getTableSchema(final Parameters parameters) {
        try {
            return JdbcUtil.createAvroSchemaFromTable(
                    parameters.driver, parameters.url,
                    parameters.user, parameters.password,
                    parameters.table);
        } catch (SQLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Schema getQuerySchema(final Parameters parameters, Map<String, String> templateArgs) {
        try {
            final String query = TemplateUtil.executeStrictTemplate(parameters.query, templateArgs);
            final Schema queryOutputSchema = JdbcUtil.createAvroSchemaFromQuery(
                    parameters.driver, parameters.url,
                    parameters.user, parameters.password,
                    query, parameters.prepareCalls);
            if (!parameters.excludeFields.isEmpty()) {
                return AvroSchemaUtil
                        .toSchemaBuilder(queryOutputSchema, null, parameters.excludeFields)
                        .endRecord();
            } else {
                return queryOutputSchema;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to get query schema", e);
        }
    }

    private static class ConvertDoFn extends DoFn<GenericRecord, MElement> {

        private final String timestampAttribute;
        private final String timestampDefault;

        ConvertDoFn(
                final String timestampAttribute,
                final String timestampDefault) {

            this.timestampAttribute = timestampAttribute;
            this.timestampDefault = timestampDefault;
        }

        @Setup
        public void setup() {

        }

        @ProcessElement
        public void processElement(final ProcessContext c) {
            final GenericRecord record = c.element();
            final MElement element = MElement.of(record, c.timestamp());
            if(timestampAttribute == null) {
                c.output(element);
            } else {
                final Instant timestamp = Optional
                        .ofNullable(element.getAsJodaInstant(timestampAttribute))
                        .orElseGet(() -> DateTimeUtil.toJodaInstant(timestampDefault));
                c.outputWithTimestamp(element, timestamp);
            }
        }
    }

    public static class JdbcBatchQuerySource extends PTransform<PBegin, PCollection<GenericRecord>> {

        private static final String DUMMY_FIELD = "Dummy_String_Field_";

        private final Parameters parameters;

        private final String timestampAttribute;
        private final String timestampDefault;

        private final Map<String, String> templateArgs;

        private final String outputSchemaString;

        private JdbcBatchQuerySource(
                final Parameters parameters,
                final String timestampAttribute,
                final String timestampDefault,
                final Map<String, String> templateArgs,
                final Schema outputSchema) {

            this.parameters = parameters;
            this.timestampAttribute = timestampAttribute;
            this.timestampDefault = timestampDefault;
            this.templateArgs = templateArgs;
            this.outputSchemaString = outputSchema.toString();
        }

        @Override
        public PCollection<GenericRecord> expand(final PBegin begin) {

            PCollection<GenericRecord> records = begin
                    .apply("Seed", Create
                            .of(createDummyRecord()).withCoder(AvroCoder.of(createDummySchema())));
            int num = 0;
            for(final PrepareParameterQuery preprocessQuery : parameters.prepareParameterQueries) {
                try {
                    final String prepQuery = TemplateUtil.executeStrictTemplate(preprocessQuery.getQuery(), templateArgs);
                    final Schema outputPreprocessSchema = JdbcUtil.createAvroSchemaFromQuery(
                            parameters.driver, parameters.url,
                            parameters.user, parameters.password,
                            prepQuery, preprocessQuery.getPrepareCalls());

                    records = records
                            .apply("PrepQuery" + num, ParDo
                                    .of(new QueryDoFn(parameters, prepQuery, preprocessQuery.getPrepareCalls(), outputPreprocessSchema.toString())))
                            .setCoder(AvroCoder.of(outputPreprocessSchema))
                            .apply("Reshuffle" + num, Reshuffle.viaRandomKey());
                    num = num + 1;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }

            final String query = TemplateUtil.executeStrictTemplate(parameters.query, templateArgs);

            return records
                    .apply("ExecuteQuery", ParDo
                            .of(new QueryDoFn(parameters, query, parameters.prepareCalls, outputSchemaString)));
        }

        public static class QueryDoFn extends DoFn<GenericRecord, GenericRecord> {

            private static final int DEFAULT_FETCH_SIZE = 50_000;

            private final String driver;
            private final String url;
            private final String user;
            private final String password;

            private final String query;
            private final List<String> prepareCalls;

            private final String outputSchemaString;
            private final Integer transactionIsolation;

            private transient Schema outputSchema;
            private transient JdbcUtil.CloseableDataSource dataSource;
            private transient Connection connection;

            QueryDoFn(final Parameters parameters,
                      final String query,
                      final List<String> prepareCalls,
                      final String outputSchemaString) {

                this.driver = parameters.driver;
                this.url = parameters.url;
                this.user = parameters.user;
                this.password = parameters.password;

                this.query = query;
                this.prepareCalls = prepareCalls;

                this.outputSchemaString = outputSchemaString;
                this.transactionIsolation = parameters.transactionIsolation;
            }

            @Setup
            public void setup() throws SQLException {
                this.dataSource = JdbcUtil.createDataSource(this.driver, this.url, user, password, true);
                this.connection = dataSource.getConnection();
                this.connection.setReadOnly(true);
                if(transactionIsolation != null) {
                    this.connection.setTransactionIsolation(transactionIsolation);
                }
                this.outputSchema = AvroSchemaUtil.convertSchema(outputSchemaString);
            }

            @Teardown
            public void teardown() throws SQLException {
                if(this.connection != null) {
                    try {
                        this.connection.close();
                    } finally {
                        this.connection = null;
                    }
                }

                if(this.dataSource != null) {
                    try {
                        this.dataSource.close();
                    } catch (IOException e) {
                    } finally {
                        this.dataSource = null;
                    }
                }
            }

            @ProcessElement
            public void processElement(ProcessContext c) throws SQLException, IOException {
                LOG.info(String.format("Start Query [%s]", query));
                if(!prepareCalls.isEmpty()) {
                    for(final String prepareCall : prepareCalls) {
                        try (final CallableStatement statement = connection
                                .prepareCall(prepareCall, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {

                            final boolean result = statement.execute();
                            if(result) {
                                LOG.info("Executed prepareCall: " + prepareCall);
                            } else {
                                LOG.error("Failed to execute prepareCall: " + prepareCall);
                            }
                        }
                    }
                }

                int count = 0;
                final Instant start = Instant.now();
                final GenericRecord params = c.element();
                if(isDummy(params.getSchema())) {
                    // Received dummy record means not use prepare statement.

                    try (final Statement statement = connection
                            .createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                        statement.setFetchSize(DEFAULT_FETCH_SIZE);

                        try (final ResultSet resultSet = statement.executeQuery(query)) {
                            while (resultSet.next()) {
                                final GenericRecord record = ResultSetToRecordConverter.convert(outputSchema, resultSet);
                                c.output(record);
                                count++;

                                if(count % 1000 == 0) {
                                    LOG.info(String.format("PreparedQuery [%s] reading record count [%d]", query, count));
                                }
                            }
                        }
                        LOG.info(String.format("Finished to read query [%s], total count: [%d]", query, count));
                    }
                } else {
                    // Received not dummy record means received record as prepare statement parameter.

                    try (final PreparedStatement statement = connection
                            .prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                        statement.setFetchSize(DEFAULT_FETCH_SIZE);

                        final ParameterMetaData meta = statement.getParameterMetaData();
                        for(int i=0; i<meta.getParameterCount(); i++) {
                            final Schema.Field field = params.getSchema().getFields().get(i);
                            final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
                            final Object fieldValue = params.get(field.name());

                            JdbcUtil.setStatement(statement, i+1, fieldSchema, fieldValue);
                        }

                        try (final ResultSet resultSet = statement.executeQuery()) {
                            while (resultSet.next()) {
                                final GenericRecord record = ResultSetToRecordConverter.convert(outputSchema, resultSet);
                                c.output(record);
                                count++;

                                if(count % 1000 == 0) {
                                    LOG.info(String.format("PreparedQuery [%s] reading record count [%d]", statement, count));
                                }
                            }
                        }
                        LOG.info(String.format("Finished to read prepared query [%s], total count: [%d]", statement, count));
                    }

                }

                final long time = Instant.now().getMillis() - start.getMillis();
                LOG.info(String.format("Query took [%d] millisec to execute.", count, time));
            }
        }


        private static Schema createDummySchema() {
            final SchemaBuilder.FieldAssembler<Schema> schemaFields = SchemaBuilder.record("root").fields();
            schemaFields.name(DUMMY_FIELD).type(AvroSchemaUtil.NULLABLE_STRING).noDefault();
            return schemaFields.endRecord();
        }

        private static GenericRecord createDummyRecord() {
            final Schema schema = createDummySchema();
            final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set(DUMMY_FIELD, null);
            return builder.build();
        }

        private static boolean isDummy(final Schema schema) {
            if(schema == null) {
                return false;
            }
            if(schema.getField(DUMMY_FIELD) != null) {
                return true;
            }
            return false;
        }

    }

}
