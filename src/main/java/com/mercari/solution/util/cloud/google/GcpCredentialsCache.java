package com.mercari.solution.util.cloud.google;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Central provider for GCP credentials (docs/developer/cloud-auth.md §4.1).
 *
 * Resolution order:
 * <ol>
 *   <li>an explicitly configured source ({@code options.gcp.credentials}, propagated to workers
 *       via {@link MCredentialFactory.CredentialsSourceOptions})</li>
 *   <li>the {@code MERCARI_PIPELINE_GCP_CREDENTIALS} environment variable / system property</li>
 *   <li>Application Default Credentials (which itself honors
 *       {@code GOOGLE_APPLICATION_CREDENTIALS})</li>
 * </ol>
 *
 * A source is one of: inline credentials JSON, {@code classpath:path/in/jar.json},
 * {@code gs://}/{@code s3://} (via Beam FileSystems), or a local file path. Workload identity
 * federation configs ({@code "type": "external_account"}) contain no key material, so they may
 * be embedded in configs or bundled in the application jar — the latter is the supported way to
 * run on Amazon Managed Service for Apache Flink, where environment variables cannot be set.
 */
public class GcpCredentialsCache {

    public static final String ENV_CREDENTIALS_SOURCE = "MERCARI_PIPELINE_GCP_CREDENTIALS";

    private static final String SCOPE_CLOUD_PLATFORM = "https://www.googleapis.com/auth/cloud-platform";

    private static String source;
    private static GoogleCredentials credentials;

    public static synchronized void setSource(final String newSource) {
        if(newSource == null || newSource.equals(source)) {
            return;
        }
        source = newSource;
        credentials = null;
    }

    public static synchronized GoogleCredentials credentials() throws IOException {
        if(credentials == null) {
            GoogleCredentials c = resolve();
            if(c.createScopedRequired()) {
                c = c.createScoped(SCOPE_CLOUD_PLATFORM);
            }
            credentials = c;
        }
        return credentials;
    }

    public static AccessToken accessToken() throws IOException {
        final GoogleCredentials c = credentials();
        synchronized (c) {
            c.refreshIfExpired();
            return c.getAccessToken();
        }
    }

    private static GoogleCredentials resolve() throws IOException {
        String s = source;
        if(s == null) {
            s = System.getProperty(ENV_CREDENTIALS_SOURCE, System.getenv(ENV_CREDENTIALS_SOURCE));
        }
        if(s == null) {
            return GoogleCredentials.getApplicationDefault();
        }
        try(final InputStream is = open(s.trim())) {
            return GoogleCredentials.fromStream(is);
        }
    }

    private static InputStream open(final String s) throws IOException {
        if(s.startsWith("{")) {
            return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
        }
        if(s.startsWith("classpath:")) {
            final String path = s.substring("classpath:".length());
            final InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
            if(is == null) {
                throw new IOException("GCP credentials classpath resource not found: " + path);
            }
            return is;
        }
        if(s.contains("://")) {
            final MatchResult.Metadata metadata = FileSystems.matchSingleFileSpec(s);
            final ReadableByteChannel channel = FileSystems.open(metadata.resourceId());
            return Channels.newInputStream(channel);
        }
        return Files.newInputStream(Paths.get(s));
    }

}
