package com.mercari.solution.server.launch;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mercari.solution.server.ServerVersion;
import com.mercari.solution.util.cloud.google.CloudRunUtil;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * {@code direct/cloudRunWorkerPool}: deploy the {@code direct} image as a Cloud Run Worker Pool
 * running this config (streaming / long-running pipelines). One launch creates one worker pool;
 * an existing pool of the same name is only updated when {@code replaceExisting} is set.
 * Stopping is left to the user ({@code gcloud run worker-pools delete}); the response carries
 * the command.
 */
public class CloudRunWorkerPoolLauncher implements Launcher {

    public static final String KEY_IMAGE = "IMAGE";
    public static final String KEY_CPU = "CPU";
    public static final String KEY_MEMORY = "MEMORY";
    public static final String KEY_INSTANCES = "INSTANCES";

    private static final String DEFAULT_CPU = "4";
    private static final String DEFAULT_MEMORY = "6Gi";
    private static final int DEFAULT_INSTANCES = 1;
    private static final Duration CREATE_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(3);
    private static final DateTimeFormatter NAME_SUFFIX = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    private final CloudRunUtil cloudRun;
    private final ConfigStager stager;

    public CloudRunWorkerPoolLauncher() {
        this(new CloudRunUtil(), new ConfigStager());
    }

    public CloudRunWorkerPoolLauncher(final CloudRunUtil cloudRun, final ConfigStager stager) {
        this.cloudRun = cloudRun;
        this.stager = stager;
    }

    @Override
    public String runner() {
        return "direct";
    }

    @Override
    public String environment() {
        return "cloudRunWorkerPool";
    }

    @Override
    public JsonObject launch(final LaunchRequest request) throws Exception {
        final LaunchDefaults defaults = request.defaults();
        final String runner = runner();
        final String project = defaults.require(runner, LaunchDefaults.KEY_PROJECT,
                request.param("project"), LaunchDefaults.optionsProject(runner, request.config().getOptions()));
        final String region = defaults.require(runner, LaunchDefaults.KEY_REGION,
                request.param("region"), LaunchDefaults.optionsRegion(runner, request.config().getOptions()));
        final String image = defaults.resolve(runner, KEY_IMAGE, request.param("image"))
                .orElseThrow(() -> new IllegalArgumentException("direct image is required to deploy a worker pool: specify image"
                        + " in the launch parameters or set " + LaunchDefaults.envName(runner, KEY_IMAGE)));
        final String serviceAccount = defaults.resolve(runner, LaunchDefaults.KEY_SERVICE_ACCOUNT,
                request.param("serviceAccount")).orElse(null);
        final String subnetwork = defaults.resolve(runner, LaunchDefaults.KEY_SUBNETWORK,
                request.param("subnetwork")).orElse(null);
        final String cpu = defaults.resolve(runner, KEY_CPU, request.param("cpu")).orElse(DEFAULT_CPU);
        final String memory = defaults.resolve(runner, KEY_MEMORY, request.param("memory")).orElse(DEFAULT_MEMORY);
        final int instances = defaults.resolve(runner, KEY_INSTANCES, request.param("instances"))
                .map(Integer::parseInt).orElse(DEFAULT_INSTANCES);
        final String name = request.param("name") != null ? request.param("name") : defaultName(request);
        validateName(name);
        final boolean replaceExisting = request.paramBool("replaceExisting");

        final String launchId = ConfigStager.newLaunchId();
        final String stagingLocation = defaults.resolve(runner, LaunchDefaults.KEY_STAGING_LOCATION,
                request.param("stagingLocation")).orElse(null);
        final String configValue = stager.stage(stagingLocation, launchId, request.config().getContent());
        final List<String> args = ConfigStager.containerArgs(configValue, request.argsMap());

        final JsonObject workerPool = workerPool(image, args, serviceAccount, subnetwork, cpu, memory, instances,
                LaunchResult.labels(request, runner, ServerVersion.get()));
        final String workerPoolName = CloudRunUtil.workerPoolName(project, region, name);

        JsonObject operation = null;
        boolean existed = false;
        try {
            operation = cloudRun.createWorkerPool(project, region, name, workerPool);
        } catch (final CloudRunUtil.CloudRunException e) {
            if(e.status != 409) {
                throw e;
            }
            if(!replaceExisting) {
                throw new IllegalArgumentException("Worker pool " + name + " already exists in " + project + "/" + region
                        + ". Choose another name, or set replaceExisting to deploy a new revision of it", e);
            }
            existed = true;
        }
        if(existed) {
            operation = cloudRun.patchWorkerPool(workerPoolName, workerPool);
        }
        final JsonObject finished = cloudRun.waitOperation(operation, CREATE_TIMEOUT, POLL_INTERVAL);
        final JsonObject resource = finished.has("response") && finished.get("response").isJsonObject()
                ? finished.getAsJsonObject("response")
                : cloudRun.getWorkerPool(workerPoolName);

        return LaunchResult.job(this)
                .id(name)
                .name(workerPoolName)
                .project(project)
                .location(region)
                .put("launchId", launchId)
                .put("config", configValue.startsWith("gs://") ? configValue : null)
                .createTime(resource.has("createTime") ? resource.get("createTime").getAsString() : null)
                .state(existed ? "UPDATED" : "CREATED")
                .consoleUrl(CloudRunUtil.workerPoolConsoleUrl(workerPoolName, project))
                .put("stopCommand", "gcloud run worker-pools delete " + name + " --project=" + project + " --region=" + region)
                .build();
    }

    static JsonObject workerPool(
            final String image,
            final List<String> args,
            final String serviceAccount,
            final String subnetwork,
            final String cpu,
            final String memory,
            final int instances,
            final Map<String, String> labels) {

        final JsonObject container = new JsonObject();
        container.addProperty("image", image);
        final JsonArray argsArray = new JsonArray();
        args.forEach(argsArray::add);
        container.add("args", argsArray);
        final JsonObject limits = new JsonObject();
        limits.addProperty("cpu", cpu);
        limits.addProperty("memory", memory);
        final JsonObject resources = new JsonObject();
        resources.add("limits", limits);
        container.add("resources", resources);

        final JsonArray containers = new JsonArray();
        containers.add(container);
        final JsonObject template = new JsonObject();
        template.add("containers", containers);
        if(serviceAccount != null) {
            template.addProperty("serviceAccount", serviceAccount);
        }
        if(subnetwork != null) {
            final JsonObject networkInterface = new JsonObject();
            networkInterface.addProperty("subnetwork", subnetwork);
            final JsonArray interfaces = new JsonArray();
            interfaces.add(networkInterface);
            final JsonObject vpcAccess = new JsonObject();
            vpcAccess.add("networkInterfaces", interfaces);
            template.add("vpcAccess", vpcAccess);
        }

        final JsonObject scaling = new JsonObject();
        scaling.addProperty("manualInstanceCount", instances);

        final JsonObject workerPool = new JsonObject();
        workerPool.add("template", template);
        workerPool.add("scaling", scaling);
        if(labels != null && !labels.isEmpty()) {
            final JsonObject labelsObject = new JsonObject();
            labels.forEach(labelsObject::addProperty);
            workerPool.add("labels", labelsObject);
        }
        return workerPool;
    }

    private static String defaultName(final LaunchRequest request) {
        final String base = request.config().getName() != null && !request.config().getName().isBlank()
                ? request.config().getName()
                : "pipeline";
        final String slug = base.trim().toLowerCase().replaceAll("[^a-z0-9-]", "-").replaceAll("-+", "-").replaceAll("^-|-$", "");
        final String prefix = slug.isEmpty() ? "pipeline" : slug.substring(0, Math.min(slug.length(), 40));
        return "mp-" + prefix + "-" + ZonedDateTime.now(ZoneOffset.UTC).format(NAME_SUFFIX);
    }

    private static void validateName(final String name) {
        if(!name.matches("^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$")) {
            throw new IllegalArgumentException("worker pool name must match ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$ but: " + name);
        }
    }

}
