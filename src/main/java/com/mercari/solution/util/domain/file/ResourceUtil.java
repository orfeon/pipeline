package com.mercari.solution.util.domain.file;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.MimeTypes;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;

/**
 * Unified file loading over Beam {@link FileSystems} (docs/developer/cloud-auth.md §6.2):
 * one code path for {@code gs://}, {@code s3://}, and local paths, with credentials resolved
 * by the registered filesystems ({@code options.gcp.credentials} / {@code options.aws}).
 *
 * Single-shot reads of configs, schema files, proto descriptors, queries, and models go through
 * here. GCS-specific operations (directory listing, streaming reads with a shared client) stay
 * on {@code StorageUtil}.
 */
public class ResourceUtil {

    public static boolean isStorageUri(final String path) {
        return path != null && (path.startsWith("gs://") || path.startsWith("s3://"));
    }

    public static String readString(final String path) {
        return new String(readBytes(path), StandardCharsets.UTF_8);
    }

    public static byte[] readBytes(final String path) {
        try {
            final MatchResult.Metadata metadata = match(path);
            try(final InputStream is = Channels.newInputStream(FileSystems.open(metadata.resourceId()))) {
                return is.readAllBytes();
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to read resource: " + path, e);
        }
    }

    public static boolean exists(final String path) {
        try {
            match(path);
            return true;
        } catch (final FileNotFoundException e) {
            return false;
        } catch (final IOException e) {
            throw new RuntimeException("Failed to check resource: " + path, e);
        }
    }

    public static void writeString(final String path, final String content) {
        writeBytes(path, content.getBytes(StandardCharsets.UTF_8));
    }

    public static void writeBytes(final String path, final byte[] content) {
        try {
            final ResourceId resourceId = newResource(path);
            try(final WritableByteChannel channel = FileSystems.create(resourceId, MimeTypes.BINARY)) {
                channel.write(ByteBuffer.wrap(content));
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to write resource: " + path, e);
        }
    }

    private static MatchResult.Metadata match(final String path) throws IOException {
        try {
            return FileSystems.matchSingleFileSpec(path);
        } catch (final IllegalArgumentException e) {
            registerDefaultFileSystems(e);
            return FileSystems.matchSingleFileSpec(path);
        }
    }

    private static ResourceId newResource(final String path) {
        try {
            return FileSystems.matchNewResource(path, false);
        } catch (final IllegalArgumentException e) {
            registerDefaultFileSystems(e);
            return FileSystems.matchNewResource(path, false);
        }
    }

    // FileSystems statically registers only the local filesystem; gs/s3 handlers appear with
    // setDefaultPipelineOptions. Config.load runs before Options.setOptions wires them, so on
    // a missing scheme register once with default options — a later Options.setOptions
    // re-registers with the fully-wired options, and an already-wired registration is never
    // clobbered here.
    private static synchronized void registerDefaultFileSystems(final RuntimeException cause) {
        if(cause.getMessage() == null || !cause.getMessage().contains("No filesystem found")) {
            throw cause;
        }
        FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create());
    }

}
