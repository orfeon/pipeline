package com.mercari.solution.config.options;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.mercari.solution.util.cloud.google.GcpCredentialsCache;
import com.mercari.solution.util.cloud.google.IAMUtil;
import com.mercari.solution.util.cloud.google.MCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.function.Supplier;

public class GCPOptions implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(GCPOptions.class);

    // Beam's GcpOptions only infers the project from the gcloud config files, which a container
    // (Cloud Run Jobs / Services running the direct or prism image) does not have. The resolution
    // below adds the environment variables, the project of the credentials and, on Google Cloud
    // only, the google-cloud client resolution (credentials key file, gcloud, GCE metadata server).
    // A successful answer is cached for the JVM (it cannot change during a run); a failed lookup
    // (e.g. the metadata server timing out during a cold start) is not, so the next call retries.
    private static final Object DEFAULT_PROJECT_LOCK = new Object();
    private static volatile String defaultProject;

    private String project;
    private String credentials;
    private String impersonateServiceAccount;
    private String workerRegion;
    private String workerZone;
    private Boolean enableStreamingEngine;
    private String dataflowKmsKey;
    private List<String> gcpOauthScopes;

    private PubsubOptions pubsub;
    private BigQueryOptions bigquery;
    private FirestoreOptions firestore;

    public String getProject() {
        return project;
    }

    public String getWorkerRegion() {
        return workerRegion;
    }

    public String getImpersonateServiceAccount() {
        return impersonateServiceAccount;
    }

    public static void setOptions(
            final PipelineOptions pipelineOptions,
            final GCPOptions gcp) {

        if(gcp == null) {
            return;
        }

        if(gcp.project != null) {
            pipelineOptions.as(GcpOptions.class).setProject(gcp.project);
        }
        if(gcp.credentials != null) {
            // Propagated to workers with the pipeline options; consumed by MCredentialFactory
            // (Beam GCP IOs) and GcpCredentialsCache (this project's client utils).
            pipelineOptions.as(MCredentialFactory.CredentialsSourceOptions.class)
                    .setGcpCredentialsSource(gcp.credentials);
            pipelineOptions.as(GcpOptions.class).setCredentialFactoryClass(MCredentialFactory.class);
            GcpCredentialsCache.setSource(gcp.credentials);
        }
        if(gcp.workerRegion != null) {
            pipelineOptions.as(GcpOptions.class).setWorkerRegion(gcp.workerRegion);
        }
        if(gcp.workerZone != null) {
            pipelineOptions.as(GcpOptions.class).setWorkerZone(gcp.workerZone);
        }
        if(gcp.enableStreamingEngine != null) {
            pipelineOptions.as(GcpOptions.class).setEnableStreamingEngine(gcp.enableStreamingEngine);
        }
        if(gcp.impersonateServiceAccount != null) {
            pipelineOptions.as(GcpOptions.class).setImpersonateServiceAccount(gcp.impersonateServiceAccount);
        }
        if(gcp.dataflowKmsKey != null) {
            pipelineOptions.as(GcpOptions.class).setDataflowKmsKey(gcp.dataflowKmsKey);
        }
        if(gcp.gcpOauthScopes != null && !gcp.gcpOauthScopes.isEmpty()) {
            pipelineOptions.as(GcpOptions.class).setGcpOauthScopes(gcp.gcpOauthScopes);
        }

        PubsubOptions.setOptions(pipelineOptions, gcp.pubsub);
        BigQueryOptions.setOptions(pipelineOptions, gcp.bigquery);
        FirestoreOptions.setOptions(pipelineOptions, gcp.firestore);
    }

    /**
     * Fills {@code GcpOptions.project} when neither the config, the command line nor Beam's own
     * gcloud-config inference set it, from the execution environment ({@code GOOGLE_CLOUD_PROJECT},
     * the credentials, the GCE metadata server on Cloud Run / GCE). BigQueryIO query and load jobs
     * and the Datastore sink take their project from this option, so without it the direct / prism
     * images fail at run time with "Required parameter projectId must be specified".
     * Called after the runner options so that {@code options.dataflow.project} keeps precedence.
     */
    public static void applyDefaultProject(final PipelineOptions pipelineOptions) {
        final GcpOptions gcpOptions = pipelineOptions.as(GcpOptions.class);
        final String project = resolveProject(gcpOptions.getProject(), GCPOptions::defaultProject);
        if(project != null && !project.equals(gcpOptions.getProject())) {
            LOG.info("options.gcp.project is not set: using the execution environment's project {}", project);
            gcpOptions.setProject(project);
        }
    }

    static String resolveProject(final String current, final Supplier<String> fallback) {
        if(current != null && !current.isBlank()) {
            return current;
        }
        final String resolved = fallback.get();
        return resolved == null || resolved.isBlank() ? null : resolved;
    }

    private static String defaultProject() {
        String project = defaultProject;
        if(project != null) {
            return project;
        }
        synchronized (DEFAULT_PROJECT_LOCK) {
            if(defaultProject != null) {
                return defaultProject;
            }
            project = resolveDefaultProject();
            if(project != null && !project.isBlank()) {
                defaultProject = project;
            }
            return project;
        }
    }

    /**
     * Cheap sources first (no network): the environment variables / system properties, then the
     * project embedded in the credentials. The google-cloud resolution ({@code ServiceOptions}) runs
     * only on Google Cloud, because off it the App Engine and metadata-server probes block for
     * seconds; {@code MERCARI_PIPELINE_ON_GCP} overrides that detection (see IAMUtil.isOnGcp).
     */
    private static String resolveDefaultProject() {
        for(final String name : List.of("GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT")) {
            final String value = System.getProperty(name, System.getenv(name));
            if(value != null && !value.isBlank()) {
                return value;
            }
        }
        try {
            final GoogleCredentials credentials = GcpCredentialsCache.credentials();
            if(credentials instanceof ServiceAccountCredentials serviceAccount && serviceAccount.getProjectId() != null) {
                return serviceAccount.getProjectId();
            }
            if(credentials != null && credentials.getQuotaProjectId() != null) {
                return credentials.getQuotaProjectId();
            }
        } catch (final Throwable e) {
            LOG.debug("No GCP project in the credentials: {}", e.getMessage());
        }
        if(!IAMUtil.isOnGcp()) {
            return null;
        }
        try {
            return ServiceOptions.getDefaultProjectId();
        } catch (final Throwable e) {
            LOG.warn("Failed to resolve the default GCP project from the execution environment", e);
            return null;
        }
    }

    public static class PubsubOptions implements Serializable {

        private String pubsubRootUrl;

        private static void setOptions(
                final PipelineOptions pipelineOptions,
                final PubsubOptions pubsub) {

            if(pubsub == null) {
                return;
            }

            if(pubsub.pubsubRootUrl != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions.class).setPubsubRootUrl(pubsub.pubsubRootUrl);
            }
        }

    }

    public static class BigQueryOptions implements Serializable {

        private String bigqueryProject;
        private String bigqueryEndpoint;
        private Boolean enableStorageReadApiV2;
        private Boolean useStorageApiConnectionPool;

        private static void setOptions(
                final PipelineOptions pipelineOptions,
                final BigQueryOptions bigquery) {

            if(bigquery == null) {
                return;
            }

            if(bigquery.bigqueryProject != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions.class).setBigQueryProject(bigquery.bigqueryProject);
            }
            if(bigquery.bigqueryEndpoint != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions.class).setBigQueryEndpoint(bigquery.bigqueryEndpoint);
            }
            if(bigquery.enableStorageReadApiV2 != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions.class).setEnableStorageReadApiV2(bigquery.enableStorageReadApiV2);
            }
            if(bigquery.useStorageApiConnectionPool != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions.class).setUseStorageApiConnectionPool(bigquery.useStorageApiConnectionPool);
            }
        }

    }

    public static class FirestoreOptions implements Serializable {

        private String firestoreProject;
        private String firestoreHost;
        private String emulatorHost;
        private String firestoreDb;

        private static void setOptions(
                final PipelineOptions pipelineOptions,
                final FirestoreOptions firestore) {

            if(firestore == null) {
                return;
            }

            if(firestore.firestoreProject != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions.class).setFirestoreProject(firestore.firestoreProject);
            }
            if(firestore.firestoreHost != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions.class).setFirestoreHost(firestore.firestoreHost);
            }
            if(firestore.emulatorHost != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions.class).setEmulatorHost(firestore.emulatorHost);
            }
            if(firestore.firestoreDb != null) {
                pipelineOptions.as(org.apache.beam.sdk.io.gcp.firestore.FirestoreOptions.class).setFirestoreDb(firestore.firestoreDb);
            }
        }

    }

}

