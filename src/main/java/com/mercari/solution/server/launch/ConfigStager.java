package com.mercari.solution.server.launch;

import com.mercari.solution.util.cloud.google.StorageUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Turns a config body into a {@code --config=...} value for a container launch: staged to GCS
 * under {@code MERCARI_PIPELINE_LAUNCH_STAGING_LOCATION} when configured, otherwise inlined as
 * {@code data:<base64>} (bounded, because container args count against the resource spec size).
 */
public class ConfigStager {

    /** Inline configs above this size need a staging bucket (Cloud Run's arg/spec limits are around 32KB). */
    static final int MAX_INLINE_BYTES = 24 * 1024;

    private static final DateTimeFormatter DATE_PATH = DateTimeFormatter.ofPattern("yyyy/MM/dd");

    @FunctionalInterface
    public interface Writer {
        void write(String gcsPath, String content) throws IOException;
    }

    private final Writer writer;

    public ConfigStager() {
        this(StorageUtil::writeString);
    }

    public ConfigStager(final Writer writer) {
        this.writer = writer;
    }

    /**
     * @param stagingLocation {@code gs://bucket/prefix} or null to inline.
     * @param launchId a unique id for this launch (part of the staged object name).
     * @return the {@code --config} value ({@code gs://...} or {@code data:...}).
     */
    public String stage(final String stagingLocation, final String launchId, final String content) throws IOException {
        if(stagingLocation != null && !stagingLocation.isBlank()) {
            if(!stagingLocation.startsWith("gs://")) {
                throw new IllegalArgumentException("staging location must start with gs://, but: " + stagingLocation);
            }
            final String base = stagingLocation.endsWith("/") ? stagingLocation : stagingLocation + "/";
            final String ext = content.stripLeading().startsWith("{") ? "json" : "yaml";
            final String path = base + "launch/" + ZonedDateTime.now(ZoneOffset.UTC).format(DATE_PATH)
                    + "/" + launchId + "/config." + ext;
            writer.write(path, content);
            return path;
        }
        final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        if(bytes.length > MAX_INLINE_BYTES) {
            throw new IllegalArgumentException("config is too large to pass inline (" + bytes.length + " bytes > "
                    + MAX_INLINE_BYTES + "); set " + LaunchDefaults.envName(LaunchDefaults.KEY_STAGING_LOCATION)
                    + " to a gs:// location so the config can be staged to GCS");
        }
        return "data:" + Base64.getEncoder().encodeToString(bytes);
    }

    /** The container args for a direct-image run: {@code --config=...} plus {@code --args.k=v}. */
    public static List<String> containerArgs(final String configValue, final Map<String, String> args) {
        final List<String> list = new ArrayList<>();
        list.add("--config=" + configValue);
        for(final Map.Entry<String, String> entry : args.entrySet()) {
            list.add("--args." + entry.getKey() + "=" + entry.getValue());
        }
        return list;
    }

    public static String newLaunchId() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, 16);
    }

}
