package com.mercari.solution.server.launch;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.config.Config;
import com.mercari.solution.util.cloud.google.CloudRunUtil;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Cloud Run launchers against a fake Cloud Run Admin API (JDK HttpServer). Sequential: the fake server records calls in shared state. */
@Execution(ExecutionMode.SAME_THREAD)
public class CloudRunLaunchersTest {

    private record Received(String method, String path, String query, String auth, JsonObject body) {}

    private static HttpServer server;
    private static int port;
    private static final List<Received> RECEIVED = new ArrayList<>();
    private static final AtomicInteger workerPoolPolls = new AtomicInteger();
    private static volatile boolean workerPoolExists = false;
    private static volatile boolean executionDone = false;

    private static final String CONFIG = """
            sources:
              - name: in
                module: create
                parameters:
                  elements: [1]
            """;

    @BeforeAll
    public static void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        port = server.getAddress().getPort();
        server.createContext("/v2/", exchange -> {
            final String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            final JsonObject json = body.isBlank() ? new JsonObject() : new Gson().fromJson(body, JsonObject.class);
            final String path = exchange.getRequestURI().getPath();
            synchronized (RECEIVED) {
                RECEIVED.add(new Received(exchange.getRequestMethod(), path, exchange.getRequestURI().getQuery(),
                        exchange.getRequestHeaders().getFirst("Authorization"), json));
            }
            if(path.endsWith("/jobs/missing:run")) {
                respond(exchange, 404, "{\"error\":{\"code\":404,\"message\":\"Resource 'missing' was not found\"}}");
            } else if(path.endsWith(":run")) {
                final String jobName = path.substring("/v2/".length(), path.length() - ":run".length());
                respond(exchange, 200, "{\"name\":\"projects/p/locations/asia-northeast1/operations/op-1\","
                        + "\"metadata\":{\"name\":\"" + jobName + "/executions/mp-job-abc12\",\"createTime\":\"2026-08-28T00:00:00Z\"}}");
            } else if(path.contains("/executions/")) {
                if(executionDone) {
                    respond(exchange, 200, "{\"name\":\"" + path.substring(4) + "\",\"createTime\":\"2026-08-28T00:00:00Z\","
                            + "\"completionTime\":\"2026-08-28T00:01:00Z\",\"succeededCount\":1}");
                } else {
                    executionDone = true;
                    respond(exchange, 200, "{\"name\":\"" + path.substring(4) + "\",\"createTime\":\"2026-08-28T00:00:00Z\"}");
                }
            } else if(path.endsWith("/workerPools") && "POST".equals(exchange.getRequestMethod())) {
                if(workerPoolExists) {
                    respond(exchange, 409, "{\"error\":{\"code\":409,\"message\":\"already exists\"}}");
                } else {
                    respond(exchange, 200, "{\"name\":\"projects/p/locations/asia-northeast1/operations/wp-create\",\"done\":false}");
                }
            } else if(path.contains("/workerPools/") && "PATCH".equals(exchange.getRequestMethod())) {
                respond(exchange, 200, "{\"name\":\"projects/p/locations/asia-northeast1/operations/wp-patch\",\"done\":true,"
                        + "\"response\":{\"name\":\"" + path.substring(4) + "\",\"createTime\":\"2026-08-28T00:00:00Z\"}}");
            } else if(path.contains("/operations/")) {
                if(workerPoolPolls.getAndIncrement() == 0) {
                    respond(exchange, 200, "{\"name\":\"" + path.substring(4) + "\",\"done\":false}");
                } else {
                    respond(exchange, 200, "{\"name\":\"" + path.substring(4) + "\",\"done\":true,"
                            + "\"response\":{\"name\":\"projects/p/locations/asia-northeast1/workerPools/mp-test\",\"createTime\":\"2026-08-28T00:00:00Z\"}}");
                }
            } else {
                respond(exchange, 500, "{\"error\":{\"code\":500,\"message\":\"unexpected " + path + "\"}}");
            }
        });
        server.start();
    }

    @AfterAll
    public static void stop() {
        server.stop(0);
    }

    @BeforeEach
    public void reset() {
        synchronized (RECEIVED) {
            RECEIVED.clear();
        }
        workerPoolPolls.set(0);
        workerPoolExists = false;
        executionDone = false;
    }

    private static CloudRunUtil client() {
        return new CloudRunUtil("http://127.0.0.1:" + port + "/v2/", () -> "test-token");
    }

    private static LaunchRequest request(final Map<String, String> env, final JsonObject parameters, final JsonObject args) throws Exception {
        final Config config = Config.load(CONFIG, null, Config.Format.yaml, (String) null);
        return new LaunchRequest(config, parameters, args, "alice@example.com", LaunchDefaults.of(env));
    }

    private static List<Received> received() {
        synchronized (RECEIVED) {
            return new ArrayList<>(RECEIVED);
        }
    }

    @Test
    public void testCloudRunJobRunsPreCreatedJobWithOverrides() throws Exception {
        final CloudRunJobLauncher launcher = new CloudRunJobLauncher(client(), new ConfigStager());
        final JsonObject parameters = new JsonObject();
        parameters.addProperty("taskTimeout", 1200);
        parameters.addProperty("env", "FOO=bar,BAZ=1");
        final JsonObject args = new JsonObject();
        args.addProperty("date", "2026-08-28");
        args.addProperty("n", 3);

        final JsonObject job = launcher.launch(request(Map.of(
                "MERCARI_PIPELINE_LAUNCH_PROJECT", "p",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_REGION", "asia-northeast1",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_JOB", "mp-job"), parameters, args));

        final List<Received> calls = received();
        Assertions.assertEquals(1, calls.size(), "run only: no create/get of the job");
        final Received run = calls.get(0);
        Assertions.assertEquals("POST", run.method());
        Assertions.assertEquals("/v2/projects/p/locations/asia-northeast1/jobs/mp-job:run", run.path());
        Assertions.assertEquals("Bearer test-token", run.auth());

        final JsonObject overrides = run.body().getAsJsonObject("overrides");
        Assertions.assertEquals("1200s", overrides.get("timeout").getAsString());
        Assertions.assertFalse(overrides.has("taskCount"));
        final JsonObject container = overrides.getAsJsonArray("containerOverrides").get(0).getAsJsonObject();
        final JsonArray containerArgs = container.getAsJsonArray("args");
        Assertions.assertEquals(3, containerArgs.size());
        final String configArg = containerArgs.get(0).getAsString();
        Assertions.assertTrue(configArg.startsWith("--config=data:"), configArg);
        final String decoded = new String(Base64.getDecoder().decode(configArg.substring("--config=data:".length())), StandardCharsets.UTF_8);
        Assertions.assertTrue(decoded.contains("\"module\":\"create\""), decoded); // Config.getContent() is the normalized JSON
        Assertions.assertEquals("--args.date=2026-08-28", containerArgs.get(1).getAsString());
        Assertions.assertEquals("--args.n=3", containerArgs.get(2).getAsString());
        final JsonArray env = container.getAsJsonArray("env");
        Assertions.assertEquals(2, env.size());
        Assertions.assertEquals("FOO", env.get(0).getAsJsonObject().get("name").getAsString());
        Assertions.assertEquals("bar", env.get(0).getAsJsonObject().get("value").getAsString());

        Assertions.assertEquals("direct", job.get("runner").getAsString());
        Assertions.assertEquals("cloudRunJob", job.get("environment").getAsString());
        Assertions.assertEquals("mp-job-abc12", job.get("id").getAsString());
        Assertions.assertEquals("projects/p/locations/asia-northeast1/jobs/mp-job/executions/mp-job-abc12", job.get("name").getAsString());
        Assertions.assertEquals("p", job.get("project").getAsString());
        Assertions.assertEquals("asia-northeast1", job.get("location").getAsString());
        Assertions.assertEquals("mp-job", job.get("job").getAsString());
        Assertions.assertEquals("RUNNING", job.get("state").getAsString());
        Assertions.assertEquals(
                "https://console.cloud.google.com/run/jobs/executions/details/asia-northeast1/mp-job-abc12/general?project=p",
                job.get("consoleUrl").getAsString());
    }

    @Test
    public void testCloudRunJobParametersOverrideEnvAndStageConfig() throws Exception {
        final List<String[]> staged = new ArrayList<>();
        final CloudRunJobLauncher launcher = new CloudRunJobLauncher(client(),
                new ConfigStager((path, content) -> staged.add(new String[]{path, content})));
        final JsonObject parameters = new JsonObject();
        parameters.addProperty("project", "ui-project");
        parameters.addProperty("region", "us-central1");
        parameters.addProperty("jobName", "ui-job");
        parameters.addProperty("taskCount", 2);
        parameters.addProperty("wait", 30);

        final JsonObject job = launcher.launch(request(Map.of(
                "MERCARI_PIPELINE_LAUNCH_PROJECT", "env-project",
                "MERCARI_PIPELINE_LAUNCH_REGION", "asia-northeast1",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_JOB", "env-job",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_TASK_TIMEOUT", "600",
                "MERCARI_PIPELINE_LAUNCH_STAGING_LOCATION", "gs://bucket/staging"), parameters, null));

        final Received run = received().get(0);
        Assertions.assertEquals("/v2/projects/ui-project/locations/us-central1/jobs/ui-job:run", run.path());
        final JsonObject overrides = run.body().getAsJsonObject("overrides");
        Assertions.assertEquals(2, overrides.get("taskCount").getAsInt());
        Assertions.assertEquals("600s", overrides.get("timeout").getAsString(), "env default timeout applied");
        final String configArg = overrides.getAsJsonArray("containerOverrides").get(0).getAsJsonObject()
                .getAsJsonArray("args").get(0).getAsString();
        Assertions.assertEquals(1, staged.size());
        Assertions.assertTrue(staged.get(0)[0].startsWith("gs://bucket/staging/launch/"), staged.get(0)[0]);
        Assertions.assertTrue(staged.get(0)[0].endsWith("/config.json"), staged.get(0)[0]);
        Assertions.assertEquals("--config=" + staged.get(0)[0], configArg);
        Assertions.assertEquals(staged.get(0)[0], job.get("config").getAsString());

        // wait: polled the execution until completionTime appeared
        Assertions.assertEquals("SUCCEEDED", job.get("state").getAsString());
        Assertions.assertTrue(received().size() >= 3, "run + at least two execution polls");
    }

    @Test
    public void testCloudRunJobNotFoundIsExplained() throws Exception {
        final CloudRunJobLauncher launcher = new CloudRunJobLauncher(client(), new ConfigStager());
        final JsonObject parameters = new JsonObject();
        parameters.addProperty("jobName", "missing");
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> launcher.launch(request(Map.of(
                        "MERCARI_PIPELINE_LAUNCH_PROJECT", "p",
                        "MERCARI_PIPELINE_LAUNCH_REGION", "asia-northeast1"), parameters, null)));
        Assertions.assertTrue(e.getMessage().contains("gcloud run jobs create missing"), e.getMessage());
    }

    @Test
    public void testCloudRunJobRequiresJobName() throws Exception {
        final CloudRunJobLauncher launcher = new CloudRunJobLauncher(client(), new ConfigStager());
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> launcher.launch(request(Map.of(
                        "MERCARI_PIPELINE_LAUNCH_PROJECT", "p",
                        "MERCARI_PIPELINE_LAUNCH_REGION", "asia-northeast1"), new JsonObject(), null)));
        Assertions.assertTrue(e.getMessage().contains("MERCARI_PIPELINE_LAUNCH_DIRECT_JOB"), e.getMessage());
        Assertions.assertTrue(received().isEmpty());
    }

    @Test
    public void testWorkerPoolCreateWaitsForOperation() throws Exception {
        final CloudRunWorkerPoolLauncher launcher = new CloudRunWorkerPoolLauncher(client(), new ConfigStager());
        final JsonObject parameters = new JsonObject();
        parameters.addProperty("name", "mp-test");
        parameters.addProperty("instances", 2);

        final JsonObject job = launcher.launch(request(Map.of(
                "MERCARI_PIPELINE_LAUNCH_PROJECT", "p",
                "MERCARI_PIPELINE_LAUNCH_REGION", "asia-northeast1",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_IMAGE", "asia-northeast1-docker.pkg.dev/p/repo/direct:latest",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_SERVICE_ACCOUNT", "runner@p.iam.gserviceaccount.com",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_MEMORY", "8Gi",
                "MERCARI_PIPELINE_LAUNCH_LABELS", "team=data"), parameters, null));

        final List<Received> calls = received();
        final Received create = calls.get(0);
        Assertions.assertEquals("POST", create.method());
        Assertions.assertEquals("/v2/projects/p/locations/asia-northeast1/workerPools", create.path());
        Assertions.assertEquals("workerPoolId=mp-test", create.query());
        final JsonObject template = create.body().getAsJsonObject("template");
        final JsonObject container = template.getAsJsonArray("containers").get(0).getAsJsonObject();
        Assertions.assertEquals("asia-northeast1-docker.pkg.dev/p/repo/direct:latest", container.get("image").getAsString());
        Assertions.assertTrue(container.getAsJsonArray("args").get(0).getAsString().startsWith("--config=data:"));
        Assertions.assertEquals("4", container.getAsJsonObject("resources").getAsJsonObject("limits").get("cpu").getAsString());
        Assertions.assertEquals("8Gi", container.getAsJsonObject("resources").getAsJsonObject("limits").get("memory").getAsString());
        Assertions.assertEquals("runner@p.iam.gserviceaccount.com", template.get("serviceAccount").getAsString());
        Assertions.assertEquals(2, create.body().getAsJsonObject("scaling").get("manualInstanceCount").getAsInt());
        final JsonObject labels = create.body().getAsJsonObject("labels");
        Assertions.assertEquals("data", labels.get("team").getAsString());
        Assertions.assertEquals("alice-example-com", labels.get(LaunchResult.USER_LABEL).getAsString());

        // operation polled until done
        Assertions.assertTrue(calls.stream().filter(r -> r.path().contains("/operations/")).count() >= 2);

        Assertions.assertEquals("cloudRunWorkerPool", job.get("environment").getAsString());
        Assertions.assertEquals("mp-test", job.get("id").getAsString());
        Assertions.assertEquals("CREATED", job.get("state").getAsString());
        Assertions.assertEquals("gcloud run worker-pools delete mp-test --project=p --region=asia-northeast1",
                job.get("stopCommand").getAsString());
        Assertions.assertEquals("https://console.cloud.google.com/run/workerpools/details/asia-northeast1/mp-test?project=p",
                job.get("consoleUrl").getAsString());
    }

    @Test
    public void testWorkerPoolExistingRequiresReplace() throws Exception {
        workerPoolExists = true;
        final CloudRunWorkerPoolLauncher launcher = new CloudRunWorkerPoolLauncher(client(), new ConfigStager());
        final Map<String, String> env = Map.of(
                "MERCARI_PIPELINE_LAUNCH_PROJECT", "p",
                "MERCARI_PIPELINE_LAUNCH_REGION", "asia-northeast1",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_IMAGE", "img");
        final JsonObject parameters = new JsonObject();
        parameters.addProperty("name", "mp-test");
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> launcher.launch(request(env, parameters, null)));
        Assertions.assertTrue(e.getMessage().contains("replaceExisting"), e.getMessage());

        parameters.addProperty("replaceExisting", true);
        final JsonObject job = launcher.launch(request(env, parameters, null));
        Assertions.assertEquals("UPDATED", job.get("state").getAsString());
        Assertions.assertTrue(received().stream().anyMatch(r -> "PATCH".equals(r.method())
                && r.path().equals("/v2/projects/p/locations/asia-northeast1/workerPools/mp-test")));
    }

    @Test
    public void testWorkerPoolDefaultNameAndImageRequired() throws Exception {
        final CloudRunWorkerPoolLauncher launcher = new CloudRunWorkerPoolLauncher(client(), new ConfigStager());
        final IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> launcher.launch(request(Map.of(
                        "MERCARI_PIPELINE_LAUNCH_PROJECT", "p",
                        "MERCARI_PIPELINE_LAUNCH_REGION", "asia-northeast1"), new JsonObject(), null)));
        Assertions.assertTrue(e.getMessage().contains("MERCARI_PIPELINE_LAUNCH_DIRECT_IMAGE"), e.getMessage());

        final JsonObject job = launcher.launch(request(Map.of(
                "MERCARI_PIPELINE_LAUNCH_PROJECT", "p",
                "MERCARI_PIPELINE_LAUNCH_REGION", "asia-northeast1",
                "MERCARI_PIPELINE_LAUNCH_DIRECT_IMAGE", "img"), new JsonObject(), null));
        Assertions.assertTrue(job.get("id").getAsString().matches("^mp-[a-z0-9-]+-\\d{12}$"), job.get("id").getAsString());
    }

    @Test
    public void testCloudRunUtilErrorsAndOperationWait() {
        final CloudRunUtil client = client();
        final CloudRunUtil.CloudRunException e = Assertions.assertThrows(CloudRunUtil.CloudRunException.class,
                () -> client.getJob("projects/p/locations/r/jobs/unknown"));
        Assertions.assertEquals(500, e.status);
        Assertions.assertTrue(e.isRetryable());
        Assertions.assertTrue(e.getMessage().contains("unexpected"), e.getMessage());

        final JsonObject failed = new JsonObject();
        failed.addProperty("name", "projects/p/locations/r/operations/x");
        failed.addProperty("done", true);
        final JsonObject error = new JsonObject();
        error.addProperty("code", 9);
        error.addProperty("message", "boom");
        failed.add("error", error);
        final CloudRunUtil.CloudRunException failure = Assertions.assertThrows(CloudRunUtil.CloudRunException.class,
                () -> client.waitOperation(failed, Duration.ofSeconds(1), Duration.ofMillis(10)));
        Assertions.assertTrue(failure.getMessage().contains("boom"));

        // LRO errors carry google.rpc.Code, surfaced as the equivalent HTTP status
        final JsonObject notFound = failed.deepCopy();
        notFound.getAsJsonObject("error").addProperty("code", 5);
        Assertions.assertTrue(Assertions.assertThrows(CloudRunUtil.CloudRunException.class,
                () -> client.waitOperation(notFound, Duration.ofSeconds(1), Duration.ofMillis(10))).isNotFound());
        Assertions.assertEquals(409, CloudRunUtil.httpStatus(6));
        Assertions.assertEquals(400, CloudRunUtil.httpStatus(3));
        Assertions.assertEquals(503, CloudRunUtil.httpStatus(14));

        // a pending operation past the deadline reports a timeout the caller can recover from
        final JsonObject pending = new JsonObject();
        pending.addProperty("name", "projects/p/locations/r/operations/slow");
        final CloudRunUtil.OperationTimeoutException timeout = Assertions.assertThrows(CloudRunUtil.OperationTimeoutException.class,
                () -> client.waitOperation(pending, Duration.ofMillis(1), Duration.ofMillis(1)));
        Assertions.assertEquals("projects/p/locations/r/operations/slow", timeout.operationName);

        Assertions.assertEquals("1800s", CloudRunJobLauncher.timeoutDuration("1800"));
        Assertions.assertEquals("1800s", CloudRunJobLauncher.timeoutDuration("1800s"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> CloudRunJobLauncher.timeoutDuration("30m"));

        Assertions.assertEquals("e", CloudRunUtil.lastSegment("projects/p/locations/r/jobs/j/executions/e"));
        final JsonObject execution = new JsonObject();
        Assertions.assertEquals("RUNNING", CloudRunUtil.executionState(execution));
        execution.addProperty("completionTime", "t");
        execution.addProperty("failedCount", 1);
        Assertions.assertEquals("FAILED", CloudRunUtil.executionState(execution));
    }

    private static void respond(final HttpExchange exchange, final int status, final String body) throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try(final OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

}
