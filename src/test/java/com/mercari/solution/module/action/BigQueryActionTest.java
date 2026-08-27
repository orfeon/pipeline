package com.mercari.solution.module.action;

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.module.Action;
import com.mercari.solution.module.IllegalModuleException;
import com.mercari.solution.util.pipeline.Filter;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * Unit tests of the bigquery action service over a scripted HTTP transport: job submission,
 * adoption of an already existing job, {@code -r<n>} resubmission after a transient failure,
 * error classification, timeout cancellation, the typed payload and the configuration escape hatch.
 */
public class BigQueryActionTest {

    /** Scripted transport: responses are consumed in order; a default response serves any further GET (polling). */
    private static class ScriptedTransport extends MockHttpTransport {

        record Call(String method, String url, String body) {}

        final Deque<MockLowLevelHttpResponse> responses = new ArrayDeque<>();
        final List<Call> calls = new ArrayList<>();
        // built per request: a response's content stream can be consumed only once
        java.util.function.Supplier<MockLowLevelHttpResponse> defaultGetResponse;
        java.util.function.Supplier<MockLowLevelHttpResponse> defaultPostResponse;

        ScriptedTransport respond(final int status, final String json) {
            responses.add(response(status, json));
            return this;
        }

        static MockLowLevelHttpResponse response(final int status, final String json) {
            return new MockLowLevelHttpResponse()
                    .setStatusCode(status)
                    .setContentType("application/json; charset=UTF-8")
                    .setContent(json);
        }

        @Override
        public LowLevelHttpRequest buildRequest(final String method, final String url) {
            return new MockLowLevelHttpRequest(url) {
                @Override
                public LowLevelHttpResponse execute() throws java.io.IOException {
                    calls.add(new Call(method, url, getContentAsString()));
                    if(!responses.isEmpty()) {
                        return responses.poll();
                    }
                    if("GET".equals(method) && defaultGetResponse != null) {
                        return defaultGetResponse.get();
                    }
                    if("POST".equals(method) && defaultPostResponse != null) {
                        return defaultPostResponse.get();
                    }
                    throw new IllegalStateException("unexpected request: " + method + " " + url);
                }
            };
        }
    }

    private static String job(final String jobId, final String state, final String errorReason, final String statistics) {
        return "{\"jobReference\":{\"projectId\":\"p\",\"jobId\":\"" + jobId + "\"},"
                + "\"status\":{\"state\":\"" + state + "\""
                + (errorReason == null ? "" : ",\"errorResult\":{\"reason\":\"" + errorReason + "\",\"message\":\"boom\"}")
                + "}"
                + (statistics == null ? "" : ",\"statistics\":" + statistics)
                + "}";
    }

    private static String error(final int code, final String reason) {
        return "{\"error\":{\"code\":" + code + ",\"message\":\"" + reason + "\",\"errors\":[{\"reason\":\"" + reason + "\",\"message\":\"" + reason + "\"}]}}";
    }

    private static BigQueryAction createAction(final ScriptedTransport transport, final String parametersYaml) {
        return createAction(transport, "jobs.query", Action.Trigger.once, parametersYaml);
    }

    private static BigQueryAction createAction(final ScriptedTransport transport, final String operation, final Action.Trigger trigger, final String parametersYaml) {
        final PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName("job");
        options.as(GcpOptions.class).setProject("p");
        final JsonObject parameters = Config.convertConfigJson(parametersYaml, Config.Format.yaml);
        final BigQueryAction action = (BigQueryAction) Action.createService(
                "bq", "bigquery", trigger, operation, parameters, options, null);
        final Bigquery bigquery = new Bigquery.Builder(transport, GsonFactory.getDefaultInstance(), null)
                .setApplicationName("test")
                .build();
        action.setBigquery(bigquery, (millis) -> {});
        action.setup();
        return action;
    }

    private static final String QUERY_PARAMETERS = """
            query: SELECT 1
            jobId: fixed
            """;

    @Test
    public void testSubmitAndWaitPayloadIsTyped() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "RUNNING", null, null))
                .respond(200, job("fixed", "DONE", null,
                        "{\"totalBytesProcessed\":\"12345\",\"query\":{\"numDmlAffectedRows\":\"7\",\"totalBytesBilled\":\"20971520\"}}"));
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS);

        final ActionResult result = action.execute(List.of());

        Assertions.assertEquals("fixed", result.getJobId());
        Assertions.assertEquals("DONE", result.getState());
        final Map<String, Object> payload = result.getPayloadValues();
        final Map<?, ?> statistics = (Map<?, ?>) payload.get("statistics");
        Assertions.assertEquals(12345L, ((Number) statistics.get("totalBytesProcessed")).longValue());
        Assertions.assertEquals(7L, ((Number) ((Map<?, ?>) statistics.get("query")).get("numDmlAffectedRows")).longValue());
        // the envelope JSON carries the numbers as numbers too
        Assertions.assertTrue(result.getPayload().contains("\"numDmlAffectedRows\":7"), result.getPayload());

        // a module-level condition sees the typed values
        final Map<String, Object> values = Action.createConditionValues("bigquery", result);
        Assertions.assertTrue(Filter.filter(Filter.parse("payload.statistics.query.numDmlAffectedRows > 5"), values));
        Assertions.assertFalse(Filter.filter(Filter.parse("payload.statistics.query.numDmlAffectedRows > 7"), values));
        Assertions.assertTrue(Filter.filter(Filter.parse("state = 'DONE' AND payload.statistics.totalBytesProcessed < 100000"), values));

        Assertions.assertEquals("POST", transport.calls.get(0).method());
        Assertions.assertTrue(transport.calls.get(0).url().endsWith("/projects/p/jobs"), transport.calls.get(0).url());
        Assertions.assertEquals(2, transport.calls.size());
    }

    @Test
    public void testAdoptsRunningExistingJob() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(409, error(409, "duplicate"))
                .respond(200, job("fixed", "RUNNING", null, null))
                .respond(200, job("fixed", "DONE", null, "{\"totalBytesProcessed\":\"1\"}"));
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS);

        final ActionResult result = action.execute(List.of());

        Assertions.assertEquals("fixed", result.getJobId());
        Assertions.assertEquals("DONE", result.getState());
        Assertions.assertEquals(1, transport.calls.stream().filter(c -> "POST".equals(c.method())).count());
    }

    @Test
    public void testResubmitsAfterTransientFailureOfExistingJob() throws Exception {
        // the deterministic id already exists and failed with backendError: resubmit as <id>-r1
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(409, error(409, "duplicate"))
                .respond(200, job("fixed", "DONE", "backendError", null))
                .respond(200, job("fixed-r1", "RUNNING", null, null))
                .respond(200, job("fixed-r1", "DONE", null, "{\"totalBytesProcessed\":\"1\"}"));
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS);

        final ActionResult result = action.execute(List.of());

        Assertions.assertEquals("fixed-r1", result.getJobId());
        Assertions.assertEquals("DONE", result.getState());
        final List<ScriptedTransport.Call> posts = transport.calls.stream().filter(c -> "POST".equals(c.method())).toList();
        Assertions.assertEquals(2, posts.size());
        Assertions.assertTrue(posts.get(1).body().contains("\"jobId\":\"fixed-r1\""), posts.get(1).body());
    }

    @Test
    public void testPermanentFailureOfExistingJobIsNonRetryable() {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(409, error(409, "duplicate"))
                .respond(200, job("fixed", "DONE", "invalidQuery", null));
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS);

        final NonRetryableException e = Assertions.assertThrows(NonRetryableException.class, () -> action.execute(List.of()));
        Assertions.assertTrue(e.getMessage().contains("invalidQuery"), e.getMessage());
        Assertions.assertEquals(1, transport.calls.stream().filter(c -> "POST".equals(c.method())).count());
    }

    @Test
    public void testRejectedSubmissionIsNonRetryable() {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(400, error(400, "invalidQuery"));
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS);

        Assertions.assertThrows(NonRetryableException.class, () -> action.execute(List.of()));
    }

    @Test
    public void testServerErrorOnSubmissionStaysRetryable() {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(503, error(503, "backendError"));
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS);

        final Exception e = Assertions.assertThrows(Exception.class, () -> action.execute(List.of()));
        Assertions.assertFalse(e instanceof NonRetryableException, e.toString());
    }

    @Test
    public void testCompletedJobErrorsAreClassified() {
        {
            final ScriptedTransport transport = new ScriptedTransport()
                    .respond(200, job("fixed", "RUNNING", null, null))
                    .respond(200, job("fixed", "DONE", "notFound", null));
            final BigQueryAction action = createAction(transport, QUERY_PARAMETERS);
            Assertions.assertThrows(NonRetryableException.class, () -> action.execute(List.of()));
        }
        {
            final ScriptedTransport transport = new ScriptedTransport()
                    .respond(200, job("fixed", "RUNNING", null, null))
                    .respond(200, job("fixed", "DONE", "rateLimitExceeded", null));
            final BigQueryAction action = createAction(transport, QUERY_PARAMETERS);
            final Exception e = Assertions.assertThrows(Exception.class, () -> action.execute(List.of()));
            Assertions.assertFalse(e instanceof NonRetryableException, e.toString());
        }
    }

    @Test
    public void testTimeoutCancelsJob() {
        // the job never reaches DONE: after timeoutSeconds the job is cancelled and the firing fails permanently
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "RUNNING", null, null));
        transport.defaultGetResponse = () -> ScriptedTransport.response(200, job("fixed", "RUNNING", null, null));
        transport.defaultPostResponse = () -> ScriptedTransport.response(200, "{\"job\":" + job("fixed", "RUNNING", null, null) + "}");
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS + "timeoutSeconds: 1\n");

        final NonRetryableException e = Assertions.assertThrows(NonRetryableException.class, () -> action.execute(List.of()));

        Assertions.assertTrue(e.getMessage().contains("did not complete"), e.getMessage());
        Assertions.assertTrue(transport.calls.stream().anyMatch(c -> "POST".equals(c.method()) && c.url().endsWith("/jobs/fixed/cancel")),
                transport.calls.toString());
    }

    @Test
    public void testTimeoutWithoutCancel() {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "RUNNING", null, null));
        transport.defaultGetResponse = () -> ScriptedTransport.response(200, job("fixed", "RUNNING", null, null));
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS + "timeoutSeconds: 1\ncancelOnTimeout: false\n");

        Assertions.assertThrows(NonRetryableException.class, () -> action.execute(List.of()));
        Assertions.assertTrue(transport.calls.stream().noneMatch(c -> c.url().endsWith("/cancel")));
    }

    @Test
    public void testWaitFalseReturnsSubmittedState() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "PENDING", null, null));
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS + "wait: false\n");

        final ActionResult result = action.execute(List.of());

        Assertions.assertEquals("PENDING", result.getState());
        Assertions.assertEquals(1, transport.calls.size());
    }

    @Test
    public void testConfigurationEscapeHatchAndCommonJobSettings() throws Exception {
        final BigQueryAction action = createAction(new ScriptedTransport(), """
                query: SELECT 1
                useLegacySql: false
                jobTimeoutMs: 60000
                reservation: projects/p/locations/us/reservations/r
                labels:
                  team: data
                configuration:
                  query:
                    useLegacySql: true
                    maximumBytesBilled: "1000"
                    defaultDataset:
                      datasetId: ds
                """);
        final JobConfiguration built = new JobConfiguration()
                .setJobType("QUERY")
                .setQuery(new JobConfigurationQuery().setQuery("SELECT 1").setUseLegacySql(false).setPriority("INTERACTIVE"));

        final JobConfiguration merged = action.finishJobConfiguration(action.getParameters(), built);

        // explicit parameter wins over the raw configuration
        Assertions.assertFalse(merged.getQuery().getUseLegacySql());
        // raw-only fields are kept
        Assertions.assertEquals(1000L, merged.getQuery().getMaximumBytesBilled());
        Assertions.assertEquals("ds", merged.getQuery().getDefaultDataset().getDatasetId());
        Assertions.assertEquals("SELECT 1", merged.getQuery().getQuery());
        Assertions.assertEquals(60000L, merged.getJobTimeoutMs());
        Assertions.assertEquals("projects/p/locations/us/reservations/r", merged.getReservation());
        Assertions.assertEquals(Map.of("team", "data"), merged.getLabels());
    }

    @Test
    public void testDryRunDoesNotPoll() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "DONE", null, "{\"totalBytesProcessed\":\"4096\"}"));
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS + "dryRun: true\n");

        final ActionResult result = action.execute(List.of());

        Assertions.assertEquals(1, transport.calls.size());
        Assertions.assertTrue(transport.calls.get(0).body().contains("\"dryRun\":true"), transport.calls.get(0).body());
        Assertions.assertEquals(4096L, ((Number) ((Map<?, ?>) result.getPayloadValues().get("statistics")).get("totalBytesProcessed")).longValue());
    }

    @Test
    public void testValidation() {
        final IllegalModuleException e = Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), """
                query: SELECT 1
                jobTimeoutMs: 0
                """));
        Assertions.assertTrue(e.getMessage().contains("jobTimeoutMs"), e.getMessage());
    }

    /** The JobConfiguration the action submitted, parsed back from the first POST body. */
    private static JobConfiguration submittedConfiguration(final ScriptedTransport transport) throws Exception {
        final String body = transport.calls.stream().filter(c -> "POST".equals(c.method())).findFirst().orElseThrow().body();
        return GsonFactory.getDefaultInstance().fromString(body, com.google.api.services.bigquery.model.Job.class).getConfiguration();
    }

    @Test
    public void testQueryParametersAndDestinationOptions() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "DONE", null, "{\"totalBytesProcessed\":\"1\"}"));
        final BigQueryAction action = createAction(transport, """
                query: SELECT * FROM t WHERE dt = @dt AND id IN UNNEST(@ids) AND flag = @flag AND ratio > @ratio
                wait: false
                jobId: fixed
                queryParameters:
                  dt: { type: date, value: "2026-08-27" }
                  ids: [1, 2, 3]
                  flag: true
                  ratio: 0.5
                  name: alice
                  tags: { type: STRING, value: [a, b] }
                defaultDataset: ds
                maximumBytesBilled: 1000000
                useQueryCache: false
                connectionProperties:
                  time_zone: Asia/Tokyo
                destinationTable: p.ds.out
                writeDisposition: WRITE_TRUNCATE
                timePartitioning: { type: DAY, field: dt, expirationMs: 86400000 }
                clustering: [id, name]
                schemaUpdateOptions: [ALLOW_FIELD_ADDITION]
                """);
        action.execute(List.of());

        final JobConfigurationQuery query = submittedConfiguration(transport).getQuery();
        Assertions.assertEquals("NAMED", query.getParameterMode());
        final Map<String, com.google.api.services.bigquery.model.QueryParameter> params = new java.util.HashMap<>();
        query.getQueryParameters().forEach(q -> params.put(q.getName(), q));
        Assertions.assertEquals("DATE", params.get("dt").getParameterType().getType());
        Assertions.assertEquals("2026-08-27", params.get("dt").getParameterValue().getValue());
        Assertions.assertEquals("ARRAY", params.get("ids").getParameterType().getType());
        Assertions.assertEquals("INT64", params.get("ids").getParameterType().getArrayType().getType());
        Assertions.assertEquals(3, params.get("ids").getParameterValue().getArrayValues().size());
        Assertions.assertEquals("BOOL", params.get("flag").getParameterType().getType());
        Assertions.assertEquals("FLOAT64", params.get("ratio").getParameterType().getType());
        Assertions.assertEquals("STRING", params.get("name").getParameterType().getType());
        Assertions.assertEquals("alice", params.get("name").getParameterValue().getValue());
        Assertions.assertEquals("STRING", params.get("tags").getParameterType().getArrayType().getType());
        Assertions.assertEquals("b", params.get("tags").getParameterValue().getArrayValues().get(1).getValue());

        Assertions.assertEquals("ds", query.getDefaultDataset().getDatasetId());
        Assertions.assertEquals("p", query.getDefaultDataset().getProjectId());
        Assertions.assertEquals(1000000L, query.getMaximumBytesBilled());
        Assertions.assertFalse(query.getUseQueryCache());
        Assertions.assertEquals("time_zone", query.getConnectionProperties().get(0).getKey());
        Assertions.assertEquals("DAY", query.getTimePartitioning().getType());
        Assertions.assertEquals("dt", query.getTimePartitioning().getField());
        Assertions.assertEquals(86400000L, query.getTimePartitioning().getExpirationMs());
        Assertions.assertEquals(List.of("id", "name"), query.getClustering().getFields());
        Assertions.assertEquals(List.of("ALLOW_FIELD_ADDITION"), query.getSchemaUpdateOptions());
        Assertions.assertEquals("out", query.getDestinationTable().getTableId());
    }

    @Test
    public void testQueryParametersTemplatedPerElement() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("j", "DONE", null, "{\"totalBytesProcessed\":\"1\"}"));
        final BigQueryAction action = createAction(transport, "jobs.query", Action.Trigger.perElement, """
                query: DELETE FROM t WHERE dt = @dt
                wait: false
                queryParameters:
                  dt: { type: DATE, value: "${date_str}" }
                  label: "${name}-suffix"
                """);
        final com.mercari.solution.module.MElement element = com.mercari.solution.module.MElement.builder()
                .withString("date_str", "2026-01-02")
                .withString("name", "n")
                .withEventTime(org.joda.time.Instant.now())
                .build();
        action.execute(List.of(element));

        final JobConfigurationQuery query = submittedConfiguration(transport).getQuery();
        Assertions.assertEquals("2026-01-02", query.getQueryParameters().get(0).getParameterValue().getValue());
        Assertions.assertEquals("n-suffix", query.getQueryParameters().get(1).getParameterValue().getValue());
    }

    @Test
    public void testPositionalQueryParametersListForm() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "DONE", null, "{\"totalBytesProcessed\":\"1\"}"));
        final BigQueryAction action = createAction(transport, """
                query: SELECT ? AS a
                wait: false
                jobId: fixed
                queryParameters:
                  - parameterType: { type: INT64 }
                    parameterValue: { value: "5" }
                """);
        action.execute(List.of());
        final JobConfigurationQuery query = submittedConfiguration(transport).getQuery();
        Assertions.assertEquals("POSITIONAL", query.getParameterMode());
        Assertions.assertEquals("5", query.getQueryParameters().get(0).getParameterValue().getValue());
    }

    @Test
    public void testLoadOptions() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "DONE", null, "{\"load\":{\"outputRows\":\"10\",\"badRecords\":\"0\"}}"));
        final BigQueryAction action = createAction(transport, "jobs.load", Action.Trigger.once, """
                jobId: fixed
                sourceUris: [gs://b/dt=2026-08-27/*.csv]
                wait: false
                sourceFormat: CSV
                destinationTable: ds.loaded
                schema:
                  fields:
                    - { name: id, type: int64, mode: required }
                    - { name: name, type: string }
                    - { name: ts, type: timestamp }
                csvOptions:
                  skipLeadingRows: 1
                  fieldDelimiter: "\t"
                  allowJaggedRows: true
                  nullMarker: "NULL"
                ignoreUnknownValues: true
                maxBadRecords: 5
                hivePartitioningOptions: { mode: AUTO, sourceUriPrefix: gs://b/ }
                rangePartitioning: { field: id, start: 0, end: 1000, interval: 10 }
                clustering: [name]
                decimalTargetTypes: [NUMERIC, BIGNUMERIC]
                """);
        final ActionResult result = action.execute(List.of());

        final JobConfigurationLoad load = submittedConfiguration(transport).getLoad();
        Assertions.assertEquals(3, load.getSchema().getFields().size());
        Assertions.assertEquals("INTEGER", load.getSchema().getFields().get(0).getType());
        Assertions.assertEquals("REQUIRED", load.getSchema().getFields().get(0).getMode());
        Assertions.assertEquals("TIMESTAMP", load.getSchema().getFields().get(2).getType());
        Assertions.assertEquals(1, load.getSkipLeadingRows());
        Assertions.assertEquals("\t", load.getFieldDelimiter());
        Assertions.assertTrue(load.getAllowJaggedRows());
        Assertions.assertEquals("NULL", load.getNullMarker());
        Assertions.assertTrue(load.getIgnoreUnknownValues());
        Assertions.assertEquals(5, load.getMaxBadRecords());
        Assertions.assertEquals("AUTO", load.getHivePartitioningOptions().getMode());
        Assertions.assertEquals("id", load.getRangePartitioning().getField());
        Assertions.assertEquals(10L, load.getRangePartitioning().getRange().getInterval());
        Assertions.assertEquals(List.of("name"), load.getClustering().getFields());
        Assertions.assertEquals(List.of("NUMERIC", "BIGNUMERIC"), load.getDecimalTargetTypes());
        Assertions.assertEquals("p", load.getDestinationTable().getProjectId());
        Assertions.assertEquals(10L, ((Number) ((Map<?, ?>) ((Map<?, ?>) result.getPayloadValues().get("statistics")).get("load")).get("outputRows")).longValue());
    }

    @Test
    public void testLoadParquetAndAutodetect() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "DONE", null, "{\"load\":{\"outputRows\":\"1\"}}"));
        final BigQueryAction action = createAction(transport, "jobs.load", Action.Trigger.once, """
                jobId: fixed
                sourceUris: [gs://b/*.parquet]
                wait: false
                sourceFormat: PARQUET
                destinationTable: ds.loaded
                autodetect: true
                parquetOptions: { enumAsString: true, enableListInference: true }
                useAvroLogicalTypes: true
                """);
        action.execute(List.of());
        final JobConfigurationLoad load = submittedConfiguration(transport).getLoad();
        Assertions.assertTrue(load.getAutodetect());
        Assertions.assertTrue(load.getParquetOptions().getEnumAsString());
        Assertions.assertTrue(load.getUseAvroLogicalTypes());
        Assertions.assertNull(load.getSchema());
    }

    @Test
    public void testStepTwoValidation() {
        for(final String yaml : List.of(
                "query: SELECT 1\nqueryParameters: text\n",
                "query: SELECT 1\ntimePartitioning: { field: dt }\n",
                "query: SELECT 1\ntimePartitioning: { type: DAY }\nrangePartitioning: { field: id, start: 0, end: 1, interval: 1 }\n",
                "query: SELECT 1\nschemaUpdateOptions: [ALLOW_ANYTHING]\n",
                "query: SELECT 1\nrangePartitioning: { field: id, start: 10, end: 1, interval: 1 }\n")) {
            Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), yaml), yaml);
        }
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "jobs.load", Action.Trigger.once, """
                sourceUris: [gs://b/*]
                destinationTable: ds.t
                autodetect: true
                schema:
                  fields: [{ name: id, type: int64 }]
                """));
    }

    @Test
    public void testExtractJob() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("j", "DONE", null, "{\"extract\":{\"destinationUriFileCounts\":[\"3\"]}}"));
        final BigQueryAction action = createAction(transport, "jobs.extract", Action.Trigger.perElement, """
                wait: false
                sourceTable: ds.events_${version}
                destinationUris: ["gs://b/export/${version}/*.avro"]
                destinationFormat: AVRO
                compression: SNAPPY
                useAvroLogicalTypes: true
                """);
        final com.mercari.solution.module.MElement element = com.mercari.solution.module.MElement.builder()
                .withString("version", "v1")
                .withEventTime(org.joda.time.Instant.now())
                .build();
        final ActionResult result = action.execute(List.of(element));

        final com.google.api.services.bigquery.model.JobConfigurationExtract extract = submittedConfiguration(transport).getExtract();
        Assertions.assertEquals("events_v1", extract.getSourceTable().getTableId());
        Assertions.assertEquals("p", extract.getSourceTable().getProjectId());
        Assertions.assertEquals(List.of("gs://b/export/v1/*.avro"), extract.getDestinationUris());
        Assertions.assertEquals("AVRO", extract.getDestinationFormat());
        Assertions.assertEquals("SNAPPY", extract.getCompression());
        Assertions.assertTrue(extract.getUseAvroLogicalTypes());
        Assertions.assertEquals("jobs.extract", result.getOperation());
        final List<?> counts = (List<?>) ((Map<?, ?>) ((Map<?, ?>) result.getPayloadValues().get("statistics")).get("extract")).get("destinationUriFileCounts");
        Assertions.assertEquals(3L, ((Number) counts.get(0)).longValue());
    }

    @Test
    public void testExtractModel() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "DONE", null, null));
        final BigQueryAction action = createAction(transport, "jobs.extract", Action.Trigger.once, """
                wait: false
                jobId: fixed
                sourceModel: other.ds.m
                destinationUris: [gs://b/model/]
                destinationFormat: ML_TF_SAVED_MODEL
                """);
        action.execute(List.of());
        final com.google.api.services.bigquery.model.JobConfigurationExtract extract = submittedConfiguration(transport).getExtract();
        Assertions.assertNull(extract.getSourceTable());
        Assertions.assertEquals("other", extract.getSourceModel().getProjectId());
        Assertions.assertEquals("m", extract.getSourceModel().getModelId());
    }

    @Test
    public void testCopySnapshotWithRelativeExpiration() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "DONE", null, null));
        final BigQueryAction action = createAction(transport, "jobs.copy", Action.Trigger.once, """
                wait: false
                jobId: fixed
                sourceTables: [ds.events]
                destinationTable: ds.events_snapshot
                operationType: SNAPSHOT
                destinationExpirationTime: 7d
                """);
        final java.time.Instant before = java.time.Instant.now();
        action.execute(List.of());

        final com.google.api.services.bigquery.model.JobConfigurationTableCopy copy = submittedConfiguration(transport).getCopy();
        Assertions.assertEquals("SNAPSHOT", copy.getOperationType());
        Assertions.assertEquals("events", copy.getSourceTables().get(0).getTableId());
        Assertions.assertEquals("events_snapshot", copy.getDestinationTable().getTableId());
        final java.time.Instant expiration = java.time.Instant.parse(copy.getDestinationExpirationTime());
        Assertions.assertTrue(expiration.isAfter(before.plus(java.time.Duration.ofDays(7)).minusSeconds(60)), copy.getDestinationExpirationTime());
        Assertions.assertTrue(expiration.isBefore(before.plus(java.time.Duration.ofDays(7)).plusSeconds(60)), copy.getDestinationExpirationTime());
    }

    @Test
    public void testCopyCollectsSourceTablesFromElements() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("j", "DONE", null, null));
        final BigQueryAction action = createAction(transport, "jobs.copy", Action.Trigger.collect, """
                wait: false
                sourceTablesField: table
                destinationTable: ds.merged
                writeDisposition: WRITE_TRUNCATE
                destinationExpirationTime: "2030-01-01T00:00:00Z"
                """);
        final List<com.mercari.solution.module.MElement> elements = List.of(
                com.mercari.solution.module.MElement.builder().withString("table", "ds.a").withEventTime(org.joda.time.Instant.now()).build(),
                com.mercari.solution.module.MElement.builder().withString("table", "ds.b").withEventTime(org.joda.time.Instant.now()).build());
        action.execute(elements);

        final com.google.api.services.bigquery.model.JobConfigurationTableCopy copy = submittedConfiguration(transport).getCopy();
        Assertions.assertEquals("COPY", copy.getOperationType());
        Assertions.assertEquals(List.of("a", "b"), copy.getSourceTables().stream().map(t -> t.getTableId()).toList());
        Assertions.assertEquals("WRITE_TRUNCATE", copy.getWriteDisposition());
        Assertions.assertEquals("2030-01-01T00:00:00Z", copy.getDestinationExpirationTime());
    }

    @Test
    public void testExtractAndCopyValidation() {
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "jobs.extract", Action.Trigger.once,
                "sourceTable: ds.t\nsourceModel: ds.m\ndestinationUris: [gs://b/x]\n"));
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "jobs.extract", Action.Trigger.once,
                "sourceTable: ds.t\n"));
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "jobs.copy", Action.Trigger.once,
                "sourceTablesField: table\ndestinationTable: ds.t\n"));
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "jobs.copy", Action.Trigger.once,
                "sourceTables: [ds.a]\ndestinationTable: ds.t\ndestinationExpirationTime: soon\n"));
    }

    @Test
    public void testResultRows() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "RUNNING", null, null))
                .respond(200, job("fixed", "DONE", null, "{\"totalBytesProcessed\":\"1\"}"))
                .respond(200, "{\"jobComplete\":true,\"totalRows\":\"2\","
                        + "\"schema\":{\"fields\":[{\"name\":\"cnt\",\"type\":\"INTEGER\"},{\"name\":\"name\",\"type\":\"STRING\"}]},"
                        + "\"rows\":[{\"f\":[{\"v\":\"42\"},{\"v\":\"a\"}]},{\"f\":[{\"v\":\"7\"},{\"v\":\"b\"}]}]}");
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS + "resultRows: 10\n");

        final ActionResult result = action.execute(List.of());

        final Map<String, Object> payload = result.getPayloadValues();
        Assertions.assertEquals(2L, ((Number) payload.get("totalRows")).longValue());
        Assertions.assertEquals(2, ((List<?>) payload.get("resultRows")).size());
        Assertions.assertEquals(42L, ((Number) ((Map<?, ?>) payload.get("firstRow")).get("cnt")).longValue());
        Assertions.assertEquals("a", ((Map<?, ?>) payload.get("firstRow")).get("name"));
        final ScriptedTransport.Call results = transport.calls.get(2);
        Assertions.assertTrue(results.url().contains("/queries/fixed") && results.url().contains("maxResults=10"), results.url());

        final Map<String, Object> values = Action.createConditionValues("bigquery", result);
        Assertions.assertTrue(Filter.filter(Filter.parse("payload.firstRow.cnt > 40"), values));
        Assertions.assertFalse(Filter.filter(Filter.parse("payload.firstRow.name = 'b'"), values));
    }

    @Test
    public void testResultRowsValidation() {
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), QUERY_PARAMETERS + "resultRows: 1\nwait: false\n"));
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), QUERY_PARAMETERS + "resultRows: 0\n"));
    }

    @Test
    public void testWaitForExternalJob() throws Exception {
        // status-only polls until DONE, then one full fetch
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("ext", "RUNNING", null, null))
                .respond(200, job("ext", "DONE", null, null))
                .respond(200, job("ext", "DONE", null, "{\"totalBytesProcessed\":\"5\"}"));
        final BigQueryAction action = createAction(transport, "jobs.wait", Action.Trigger.perElement, "jobId: ${id}\n");
        final com.mercari.solution.module.MElement element = com.mercari.solution.module.MElement.builder()
                .withString("id", "ext").withEventTime(org.joda.time.Instant.now()).build();

        final ActionResult result = action.execute(List.of(element));

        Assertions.assertEquals("ext", result.getJobId());
        Assertions.assertEquals("DONE", result.getState());
        Assertions.assertEquals(5L, ((Number) ((Map<?, ?>) result.getPayloadValues().get("statistics")).get("totalBytesProcessed")).longValue());
        Assertions.assertTrue(transport.calls.stream().allMatch(c -> "GET".equals(c.method())));
        Assertions.assertTrue(transport.calls.get(0).url().contains("fields=jobReference"), transport.calls.get(0).url());
        Assertions.assertFalse(transport.calls.get(2).url().contains("fields="), transport.calls.get(2).url());
        Assertions.assertFalse(action.getParameters().cancelOnTimeout, "jobs.wait must not cancel foreign jobs by default");
    }

    @Test
    public void testWaitCollectsJobIdsAndFailsPermanentlyOnJobError() throws Exception {
        {
            // duplicated ids are waited for once; each job: status poll + full fetch
            final ScriptedTransport transport = new ScriptedTransport();
            transport.defaultGetResponse = () -> ScriptedTransport.response(200, job("x", "DONE", null, "{\"totalBytesProcessed\":\"1\"}"));
            final BigQueryAction action = createAction(transport, "jobs.wait", Action.Trigger.collect, "jobIdField: jobId\n");
            final List<com.mercari.solution.module.MElement> elements = List.of(
                    com.mercari.solution.module.MElement.builder().withString("jobId", "a").withEventTime(org.joda.time.Instant.now()).build(),
                    com.mercari.solution.module.MElement.builder().withString("jobId", "b").withEventTime(org.joda.time.Instant.now()).build(),
                    com.mercari.solution.module.MElement.builder().withString("jobId", "a").withEventTime(org.joda.time.Instant.now()).build(),
                    com.mercari.solution.module.MElement.builder().withString("jobId", " ").withEventTime(org.joda.time.Instant.now()).build());
            final ActionResult result = action.execute(elements);
            Assertions.assertEquals("a,b", result.getJobId());
            Assertions.assertEquals(2, ((List<?>) result.getPayloadValues().get("jobs")).size());
            Assertions.assertEquals(4, transport.calls.size());
        }
        {
            // one shared poll loop: a still-running job is re-polled while finished ones are not fetched again
            final ScriptedTransport transport = new ScriptedTransport()
                    .respond(200, job("a", "RUNNING", null, null))
                    .respond(200, job("b", "DONE", null, null))
                    .respond(200, job("b", "DONE", null, "{\"totalBytesProcessed\":\"2\"}"))
                    .respond(200, job("a", "DONE", null, null))
                    .respond(200, job("a", "DONE", null, "{\"totalBytesProcessed\":\"1\"}"));
            final BigQueryAction action = createAction(transport, "jobs.wait", Action.Trigger.collect, "jobIdField: jobId\n");
            final List<com.mercari.solution.module.MElement> elements = List.of(
                    com.mercari.solution.module.MElement.builder().withString("jobId", "a").withEventTime(org.joda.time.Instant.now()).build(),
                    com.mercari.solution.module.MElement.builder().withString("jobId", "b").withEventTime(org.joda.time.Instant.now()).build());
            final ActionResult result = action.execute(elements);
            Assertions.assertEquals(5, transport.calls.size());
            Assertions.assertTrue(transport.calls.get(3).url().contains("/jobs/a"), transport.calls.get(3).url());
            final List<?> jobs = (List<?>) result.getPayloadValues().get("jobs");
            Assertions.assertEquals(1L, ((Number) ((Map<?, ?>) ((Map<?, ?>) jobs.get(0)).get("statistics")).get("totalBytesProcessed")).longValue());
        }
        {
            // a job this action did not submit cannot be resubmitted: even a transient reason is final
            final ScriptedTransport transport = new ScriptedTransport()
                    .respond(200, job("ext", "DONE", "backendError", null))
                    .respond(200, job("ext", "DONE", "backendError", null));
            final BigQueryAction action = createAction(transport, "jobs.wait", Action.Trigger.once, "jobId: ext\n");
            Assertions.assertThrows(NonRetryableException.class, () -> action.execute(List.of()));
        }
    }

    @Test
    public void testTablesGet() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, "{\"tableReference\":{\"projectId\":\"p\",\"datasetId\":\"ds\",\"tableId\":\"t\"},"
                        + "\"numRows\":\"120\",\"numBytes\":\"4096\",\"lastModifiedTime\":\"1756252800000\",\"type\":\"TABLE\","
                        + "\"schema\":{\"fields\":[{\"name\":\"id\",\"type\":\"INTEGER\"}]}}");
        final BigQueryAction action = createAction(transport, "tables.get", Action.Trigger.once, "table: ds.t\n");

        final ActionResult result = action.execute(List.of());

        Assertions.assertEquals("p.ds.t", result.getJobId());
        Assertions.assertEquals("DONE", result.getState());
        Assertions.assertEquals(120L, ((Number) result.getPayloadValues().get("numRows")).longValue());
        Assertions.assertTrue(transport.calls.get(0).url().endsWith("/projects/p/datasets/ds/tables/t"), transport.calls.get(0).url());
        final Map<String, Object> values = Action.createConditionValues("bigquery", result);
        Assertions.assertTrue(Filter.filter(Filter.parse("payload.numRows > 100 AND payload.lastModifiedTime > 1756000000000"), values));
        Assertions.assertTrue(Filter.filter(Filter.parse("payload.type = 'TABLE'"), values));
    }

    @Test
    public void testTablesGetNotFound() throws Exception {
        {
            final ScriptedTransport transport = new ScriptedTransport().respond(404, error(404, "notFound"));
            final BigQueryAction action = createAction(transport, "tables.get", Action.Trigger.once, "table: ds.t\n");
            Assertions.assertThrows(NonRetryableException.class, () -> action.execute(List.of()));
        }
        {
            final ScriptedTransport transport = new ScriptedTransport().respond(404, error(404, "notFound"));
            final BigQueryAction action = createAction(transport, "tables.get", Action.Trigger.once, "table: ds.t\nignoreNotFound: true\n");
            final ActionResult result = action.execute(List.of());
            Assertions.assertEquals("NOT_FOUND", result.getState());
            Assertions.assertNull(result.getPayload());
        }
    }

    @Test
    public void testTablesDelete() throws Exception {
        {
            final ScriptedTransport transport = new ScriptedTransport()
                    .respond(204, "");
            final BigQueryAction action = createAction(transport, "tables.delete", Action.Trigger.perElement, "table: ds.${name}\n");
            final com.mercari.solution.module.MElement element = com.mercari.solution.module.MElement.builder()
                    .withString("name", "old_20260101").withEventTime(org.joda.time.Instant.now()).build();
            final ActionResult result = action.execute(List.of(element));
            Assertions.assertEquals("DELETED", result.getState());
            Assertions.assertEquals("p.ds.old_20260101", result.getJobId());
            Assertions.assertEquals("DELETE", transport.calls.get(0).method());
        }
        {
            // ignoreNotFound defaults to true for delete: a retried firing after a successful delete is a no-op
            final ScriptedTransport transport = new ScriptedTransport().respond(404, error(404, "notFound"));
            final BigQueryAction action = createAction(transport, "tables.delete", Action.Trigger.once, "table: ds.t\n");
            Assertions.assertEquals("NOT_FOUND", action.execute(List.of()).getState());
        }
        {
            final ScriptedTransport transport = new ScriptedTransport().respond(404, error(404, "notFound"));
            final BigQueryAction action = createAction(transport, "tables.delete", Action.Trigger.once, "table: ds.t\nignoreNotFound: false\n");
            Assertions.assertThrows(NonRetryableException.class, () -> action.execute(List.of()));
        }
    }

    @Test
    public void testWaitAndTablesValidation() {
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "jobs.wait", Action.Trigger.once, "location: US\n"));
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "jobs.wait", Action.Trigger.perElement, "jobIdField: id\n"));
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "tables.get", Action.Trigger.once, "ignoreNotFound: true\n"));
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "tables.delete", Action.Trigger.once, "projectId: p\n"));
    }

    private static final String TABLE_JSON = "{\"tableReference\":{\"projectId\":\"p\",\"datasetId\":\"ds\",\"tableId\":\"t\"},\"numRows\":\"0\",\"type\":\"TABLE\"}";
    private static final String DATASET_JSON = "{\"datasetReference\":{\"projectId\":\"p\",\"datasetId\":\"ds\"},\"location\":\"US\"}";

    @Test
    public void testTablesInsert() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport().respond(200, TABLE_JSON);
        final BigQueryAction action = createAction(transport, "tables.insert", Action.Trigger.once, """
                table: ds.t
                schema:
                  fields:
                    - { name: id, type: int64, mode: required }
                    - { name: dt, type: date }
                description: created by pipeline
                labels: { team: data }
                expirationTime: 1d
                timePartitioning: { type: DAY, field: dt }
                clustering: [id]
                requirePartitionFilter: true
                resource:
                  friendlyName: Events
                  description: overridden by the explicit parameter
                """);
        final ActionResult result = action.execute(List.of());

        Assertions.assertEquals("CREATED", result.getState());
        Assertions.assertEquals("p.ds.t", result.getJobId());
        final ScriptedTransport.Call call = transport.calls.get(0);
        Assertions.assertEquals("POST", call.method());
        Assertions.assertTrue(call.url().endsWith("/projects/p/datasets/ds/tables"), call.url());
        final com.google.api.services.bigquery.model.Table sent = GsonFactory.getDefaultInstance().fromString(call.body(), com.google.api.services.bigquery.model.Table.class);
        Assertions.assertEquals("t", sent.getTableReference().getTableId());
        Assertions.assertEquals(2, sent.getSchema().getFields().size());
        Assertions.assertEquals("created by pipeline", sent.getDescription());
        Assertions.assertEquals("Events", sent.getFriendlyName());
        Assertions.assertEquals(Map.of("team", "data"), sent.getLabels());
        Assertions.assertEquals("DAY", sent.getTimePartitioning().getType());
        Assertions.assertEquals(List.of("id"), sent.getClustering().getFields());
        Assertions.assertTrue(sent.getRequirePartitionFilter());
        Assertions.assertTrue(sent.getExpirationTime() > System.currentTimeMillis() + 23 * 3600 * 1000L);
    }

    @Test
    public void testTablesInsertIfNotExistsAndView() throws Exception {
        {
            final ScriptedTransport transport = new ScriptedTransport()
                    .respond(409, error(409, "duplicate"))
                    .respond(200, TABLE_JSON);
            final BigQueryAction action = createAction(transport, "tables.insert", Action.Trigger.once, """
                    table: ds.t
                    view: SELECT * FROM `p.ds.events_v2`
                    """);
            final ActionResult result = action.execute(List.of());
            Assertions.assertEquals("EXISTS", result.getState());
            Assertions.assertEquals("TABLE", result.getPayloadValues().get("type"));
            final com.google.api.services.bigquery.model.Table sent = GsonFactory.getDefaultInstance().fromString(transport.calls.get(0).body(), com.google.api.services.bigquery.model.Table.class);
            Assertions.assertEquals("SELECT * FROM `p.ds.events_v2`", sent.getView().getQuery());
            Assertions.assertFalse(sent.getView().getUseLegacySql());
        }
        {
            final ScriptedTransport transport = new ScriptedTransport().respond(409, error(409, "duplicate"));
            final BigQueryAction action = createAction(transport, "tables.insert", Action.Trigger.once, """
                    table: ds.t
                    ifNotExists: false
                    schema: { fields: [{ name: id, type: int64 }] }
                    """);
            Assertions.assertThrows(NonRetryableException.class, () -> action.execute(List.of()));
        }
    }

    @Test
    public void testTablesPatchSwapsView() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport().respond(200, TABLE_JSON);
        final BigQueryAction action = createAction(transport, "tables.patch", Action.Trigger.perElement, """
                table: ds.current
                view: SELECT * FROM `p.ds.events_${version}`
                """);
        final com.mercari.solution.module.MElement element = com.mercari.solution.module.MElement.builder()
                .withString("version", "v3").withEventTime(org.joda.time.Instant.now()).build();
        final ActionResult result = action.execute(List.of(element));

        Assertions.assertEquals("DONE", result.getState());
        final ScriptedTransport.Call call = transport.calls.get(0);
        Assertions.assertEquals("PATCH", call.method());
        Assertions.assertTrue(call.url().endsWith("/projects/p/datasets/ds/tables/current"), call.url());
        final com.google.api.services.bigquery.model.Table sent = GsonFactory.getDefaultInstance().fromString(call.body(), com.google.api.services.bigquery.model.Table.class);
        Assertions.assertEquals("SELECT * FROM `p.ds.events_v3`", sent.getView().getQuery());
        Assertions.assertNull(sent.getTableReference());
        Assertions.assertNull(sent.getSchema());
    }

    @Test
    public void testDatasetsGetInsertDelete() throws Exception {
        {
            final ScriptedTransport transport = new ScriptedTransport().respond(200, DATASET_JSON);
            final BigQueryAction action = createAction(transport, "datasets.get", Action.Trigger.once, "dataset: ds\n");
            final ActionResult result = action.execute(List.of());
            Assertions.assertEquals("p.ds", result.getJobId());
            Assertions.assertEquals("US", result.getPayloadValues().get("location"));
        }
        {
            final ScriptedTransport transport = new ScriptedTransport().respond(404, error(404, "notFound"));
            final BigQueryAction action = createAction(transport, "datasets.get", Action.Trigger.once, "dataset: other.ds\nignoreNotFound: true\n");
            Assertions.assertEquals("NOT_FOUND", action.execute(List.of()).getState());
            Assertions.assertTrue(transport.calls.get(0).url().endsWith("/projects/other/datasets/ds"), transport.calls.get(0).url());
        }
        {
            final ScriptedTransport transport = new ScriptedTransport().respond(200, DATASET_JSON);
            final BigQueryAction action = createAction(transport, "datasets.insert", Action.Trigger.once, """
                    dataset: ds
                    location: asia-northeast1
                    defaultTableExpirationMs: 3600000
                    labels: { env: test }
                    resource:
                      defaultPartitionExpirationMs: "7200000"
                    """);
            final ActionResult result = action.execute(List.of());
            Assertions.assertEquals("CREATED", result.getState());
            final com.google.api.services.bigquery.model.Dataset sent = GsonFactory.getDefaultInstance().fromString(transport.calls.get(0).body(), com.google.api.services.bigquery.model.Dataset.class);
            Assertions.assertEquals("asia-northeast1", sent.getLocation());
            Assertions.assertEquals(3600000L, sent.getDefaultTableExpirationMs());
            Assertions.assertEquals(7200000L, sent.getDefaultPartitionExpirationMs());
            Assertions.assertEquals("ds", sent.getDatasetReference().getDatasetId());
        }
        {
            final ScriptedTransport transport = new ScriptedTransport()
                    .respond(409, error(409, "duplicate"))
                    .respond(200, DATASET_JSON);
            final BigQueryAction action = createAction(transport, "datasets.insert", Action.Trigger.once, "dataset: ds\n");
            Assertions.assertEquals("EXISTS", action.execute(List.of()).getState());
        }
        {
            final ScriptedTransport transport = new ScriptedTransport().respond(204, "");
            final BigQueryAction action = createAction(transport, "datasets.delete", Action.Trigger.once, "dataset: ds\ndeleteContents: true\n");
            Assertions.assertEquals("DELETED", action.execute(List.of()).getState());
            Assertions.assertEquals("DELETE", transport.calls.get(0).method());
            Assertions.assertTrue(transport.calls.get(0).url().contains("deleteContents=true"), transport.calls.get(0).url());
        }
        {
            final ScriptedTransport transport = new ScriptedTransport().respond(404, error(404, "notFound"));
            final BigQueryAction action = createAction(transport, "datasets.delete", Action.Trigger.once, "dataset: ds\n");
            Assertions.assertEquals("NOT_FOUND", action.execute(List.of()).getState());
        }
    }

    @Test
    public void testTablesAndDatasetsValidation() {
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "tables.insert", Action.Trigger.once, "table: ds.t\n"));
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "tables.insert", Action.Trigger.once, "table: ds.t\nschema: { fields: [{ name: id, type: int64 }] }\nexpirationTime: never\n"));
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "tables.patch", Action.Trigger.once, "description: x\n"));
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), "datasets.insert", Action.Trigger.once, "location: US\n"));
    }

    @Test
    public void testPayloadDropsQueryPlanAndTimeline() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "DONE", null,
                        "{\"totalBytesProcessed\":\"1\",\"query\":{\"numDmlAffectedRows\":\"2\",\"queryPlan\":[{\"name\":\"S00\"}],\"timeline\":[{\"elapsedMs\":\"1\"}]}}"));
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS);
        final ActionResult result = action.execute(List.of());
        final Map<?, ?> query = (Map<?, ?>) ((Map<?, ?>) result.getPayloadValues().get("statistics")).get("query");
        Assertions.assertFalse(query.containsKey("queryPlan"));
        Assertions.assertFalse(query.containsKey("timeline"));
        Assertions.assertEquals(2L, ((Number) query.get("numDmlAffectedRows")).longValue());
    }

    @Test
    public void testQuotaErrorsStayRetryableAndRawConfigurationDefaults() throws Exception {
        {
            // HTTP 403 with a transient reason is retryable (not a rejected request)
            final ScriptedTransport transport = new ScriptedTransport().respond(403, error(403, "rateLimitExceeded"));
            final BigQueryAction action = createAction(transport, QUERY_PARAMETERS);
            final Exception e = Assertions.assertThrows(Exception.class, () -> action.execute(List.of()));
            Assertions.assertFalse(e instanceof NonRetryableException, e.toString());
        }
        {
            final ScriptedTransport transport = new ScriptedTransport().respond(403, error(403, "accessDenied"));
            final BigQueryAction action = createAction(transport, QUERY_PARAMETERS);
            Assertions.assertThrows(NonRetryableException.class, () -> action.execute(List.of()));
        }
        {
            // priority / useLegacySql given only in the raw configuration are honoured; absent ones get the module defaults
            final BigQueryAction action = createAction(new ScriptedTransport(), """
                    query: SELECT 1
                    configuration:
                      query:
                        priority: BATCH
                    """);
            final JobConfiguration merged = action.finishJobConfiguration(action.getParameters(),
                    new JobConfiguration().setJobType("QUERY").setQuery(new JobConfigurationQuery().setQuery("SELECT 1")));
            Assertions.assertEquals("BATCH", merged.getQuery().getPriority());
            Assertions.assertFalse(merged.getQuery().getUseLegacySql());
            final BigQueryAction plain = createAction(new ScriptedTransport(), "query: SELECT 1\n");
            final JobConfiguration defaults = plain.finishJobConfiguration(plain.getParameters(),
                    new JobConfiguration().setJobType("QUERY").setQuery(new JobConfigurationQuery().setQuery("SELECT 1")));
            Assertions.assertEquals("INTERACTIVE", defaults.getQuery().getPriority());
            Assertions.assertFalse(defaults.getQuery().getUseLegacySql());
        }
    }

    @Test
    public void testWaitToleratesTransientPollErrors() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(503, error(503, "backendError"))
                .respond(200, job("a", "DONE", null, null))
                .respond(200, job("a", "DONE", null, "{\"totalBytesProcessed\":\"1\"}"));
        final BigQueryAction action = createAction(transport, "jobs.wait", Action.Trigger.once, "jobId: a\n");
        final ActionResult result = action.execute(List.of());
        Assertions.assertEquals("DONE", result.getState());
        Assertions.assertEquals(3, transport.calls.size());

        final ScriptedTransport notFound = new ScriptedTransport().respond(404, error(404, "notFound"));
        final BigQueryAction missing = createAction(notFound, "jobs.wait", Action.Trigger.once, "jobId: nope\n");
        Assertions.assertThrows(NonRetryableException.class, () -> missing.execute(List.of()));
    }

    @Test
    public void testResultRowsNumericAndSpecialFloats() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "DONE", null, "{\"totalBytesProcessed\":\"1\"}"))
                .respond(200, "{\"jobComplete\":true,\"totalRows\":\"1\","
                        + "\"schema\":{\"fields\":[{\"name\":\"amount\",\"type\":\"NUMERIC\"},{\"name\":\"ratio\",\"type\":\"FLOAT\"},"
                        + "{\"name\":\"dt\",\"type\":\"DATETIME\"},{\"name\":\"ts\",\"type\":\"TIMESTAMP\"}]},"
                        + "\"rows\":[{\"f\":[{\"v\":\"12.50\"},{\"v\":\"NaN\"},{\"v\":\"2026-08-27T01:02:03\"},{\"v\":\"1.7562528001E9\"}]}]}");
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS + "resultRows: 1\n");
        final ActionResult result = action.execute(List.of());
        final Map<?, ?> row = (Map<?, ?>) result.getPayloadValues().get("firstRow");
        Assertions.assertEquals(new java.math.BigDecimal("12.50"), row.get("amount"));
        Assertions.assertTrue(Double.isNaN((Double) row.get("ratio")));
        Assertions.assertEquals("2026-08-27T01:02:03", row.get("dt"));
        Assertions.assertEquals(1756252800100000L, ((Number) row.get("ts")).longValue());
        Assertions.assertTrue(result.getPayload().contains("NaN"), result.getPayload());
    }

    @Test
    public void testTemplatedExpirationParseErrorIsNotRetryable() {
        final ScriptedTransport transport = new ScriptedTransport();
        final BigQueryAction action = createAction(transport, "jobs.copy", Action.Trigger.perElement, """
                sourceTables: [ds.a]
                destinationTable: ds.b
                destinationExpirationTime: "${when}"
                """);
        final com.mercari.solution.module.MElement element = com.mercari.solution.module.MElement.builder()
                .withString("when", "next week").withEventTime(org.joda.time.Instant.now()).build();
        final Exception e = Assertions.assertThrows(Exception.class, () -> action.execute(List.of(element)));
        Assertions.assertFalse(Action.isRetryable(e), e.toString());
    }

    @Test
    public void testNonFatalErrorsDoNotFailTheJob() throws Exception {
        // status.errors without errorResult (e.g. rows skipped within maxBadRecords) is a success
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, "{\"jobReference\":{\"projectId\":\"p\",\"jobId\":\"fixed\"},"
                        + "\"status\":{\"state\":\"DONE\",\"errors\":[{\"reason\":\"invalid\",\"message\":\"bad row skipped\"}]},"
                        + "\"statistics\":{\"load\":{\"outputRows\":\"9\",\"badRecords\":\"1\"}}}");
        final BigQueryAction action = createAction(transport, "jobs.load", Action.Trigger.once, """
                jobId: fixed
                sourceUris: [gs://b/*.csv]
                destinationTable: ds.t
                maxBadRecords: 5
                """);
        final ActionResult result = action.execute(List.of());
        Assertions.assertEquals("DONE", result.getState());
        Assertions.assertEquals(1L, ((Number) ((Map<?, ?>) ((Map<?, ?>) result.getPayloadValues().get("statistics")).get("load")).get("badRecords")).longValue());
    }

    @Test
    public void testResultRowsNullCells() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, job("fixed", "DONE", null, "{\"totalBytesProcessed\":\"1\"}"))
                .respond(200, "{\"jobComplete\":true,\"totalRows\":\"1\","
                        + "\"schema\":{\"fields\":[{\"name\":\"n\",\"type\":\"INTEGER\"},{\"name\":\"s\",\"type\":\"STRING\"},{\"name\":\"arr\",\"type\":\"INTEGER\",\"mode\":\"REPEATED\"}]},"
                        + "\"rows\":[{\"f\":[{\"v\":null},{\"v\":null},{\"v\":[]}]}]}");
        final BigQueryAction action = createAction(transport, QUERY_PARAMETERS + "resultRows: 1\n");
        final ActionResult result = action.execute(List.of());
        final Map<?, ?> row = (Map<?, ?>) result.getPayloadValues().get("firstRow");
        Assertions.assertTrue(row.containsKey("n"));
        Assertions.assertNull(row.get("n"));
        Assertions.assertNull(row.get("s"));
        Assertions.assertEquals(List.of(), row.get("arr"));
    }

    @Test
    public void testDryRunHasNoJobId() throws Exception {
        final ScriptedTransport transport = new ScriptedTransport()
                .respond(200, "{\"jobReference\":{\"projectId\":\"p\",\"location\":\"US\"},\"status\":{\"state\":\"DONE\"},\"statistics\":{\"totalBytesProcessed\":\"4096\"}}");
        final BigQueryAction action = createAction(transport, "query: SELECT 1\ndryRun: true\n");
        final ActionResult result = action.execute(List.of());
        Assertions.assertNull(result.getJobId());
        Assertions.assertEquals("DONE", result.getState());
    }

    @Test
    public void testInternalJsonFieldsCannotBeSetFromConfig() {
        final BigQueryAction action = createAction(new ScriptedTransport(), """
                query: SELECT 1
                configurationJson: "not json"
                tableSchemaJson: "garbage"
                """);
        Assertions.assertNull(action.getParameters().configurationJson);
        Assertions.assertNull(action.getParameters().tableSchemaJson);
        Assertions.assertThrows(IllegalModuleException.class, () -> createAction(new ScriptedTransport(), QUERY_PARAMETERS + "resultRows: 5000\n"));
    }

    @Test
    public void testExpirationAcceptsDateForms() {
        final java.time.Instant now = java.time.Instant.parse("2026-08-27T00:00:00Z");
        Assertions.assertEquals(java.time.Instant.parse("2026-09-03T00:00:00Z"), BigQueryAction.resolveExpirationTime("7d", now));
        Assertions.assertEquals(java.time.Instant.parse("2030-01-01T00:00:00Z"), BigQueryAction.resolveExpirationTime("2030-01-01T00:00:00Z", now));
        Assertions.assertEquals(java.time.Instant.parse("2030-01-01T00:00:00Z"), BigQueryAction.resolveExpirationTime("2030-01-01", now));
        Assertions.assertThrows(IllegalArgumentException.class, () -> BigQueryAction.resolveExpirationTime("next week", now));
    }

    @Test
    public void testDeepMerge() {
        final JsonObject base = new Gson().fromJson("{\"query\":{\"a\":1,\"nested\":{\"x\":1}},\"keep\":true}", JsonObject.class);
        final JsonObject overlay = new Gson().fromJson("{\"query\":{\"a\":2,\"nested\":{\"y\":2}},\"labels\":{\"k\":\"v\"}}", JsonObject.class);
        BigQueryAction.deepMerge(base, overlay);
        Assertions.assertEquals(
                new Gson().fromJson("{\"query\":{\"a\":2,\"nested\":{\"x\":1,\"y\":2}},\"keep\":true,\"labels\":{\"k\":\"v\"}}", JsonObject.class),
                base);
    }

    @Test
    public void testEmptyQueryIsSkipped() throws Exception {
        final BigQueryAction action = createAction(new ScriptedTransport(), "query: \"  \"\n");
        final ActionResult result = action.execute(List.of());
        Assertions.assertEquals("SKIPPED", result.getState());
        Assertions.assertNull(result.getJobId());
    }

}
