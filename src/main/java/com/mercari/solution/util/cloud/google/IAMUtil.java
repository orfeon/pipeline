package com.mercari.solution.util.cloud.google;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.api.services.iamcredentials.v1.IAMCredentials;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.*;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import com.google.cloud.iam.credentials.v1.SignJwtRequest;
import com.google.cloud.iam.credentials.v1.SignJwtResponse;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.file.JsonUtil;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.joda.time.DateTime;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IAMUtil {

    private static final String ENDPOINT_METADATA_ID_TOKEN = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience=";
    private static final String ENDPOINT_METADATA_ACCESS_TOKEN = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
    private static final String ENDPOINT_METADATA_PROJECT = "http://metadata.google.internal/computeMetadata/v1/project/project-id";
    private static final String ENDPOINT_METADATA_SERVICE_ACCOUNT = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email";

    private static final Pattern PATTERN_MAIL = Pattern.compile("^[a-zA-Z0-9_.+-]+@([a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]*\\.)+[a-zA-Z]{2,}$");
    private static final Pattern PATTERN_SERVICE_ACCOUNT = Pattern.compile("^projects\\/-\\/serviceAccounts\\/[a-zA-Z0-9_.+-]+@([a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]*\\.)+[a-zA-Z]{2,}$");

    private static final String SCOPE_USER_INFO = "https://www.googleapis.com/auth/userinfo.email";

    private static final Integer DEFAULT_RETRY = 3;
    private static final Integer DEFAULT_INTERVAL_MILLIS = 5000;

    public enum AccountType {
        USER_ACCOUNT,
        MACHINE_DIRECT,
        MACHINE_MANAGED,
        FEDERATED,
        DERIVED_SHORT_LIVED,
        NONE,
        UNKNOWN
    }

    private static volatile Boolean onGcp;

    /**
     * Whether the GCE metadata server is reachable (cached after one short probe).
     * Off Google Cloud, metadata lookups are skipped immediately instead of hanging
     * through retries (docs/developer/cloud-auth.md §4.2).
     * Override with the MERCARI_PIPELINE_ON_GCP env var / system property.
     */
    public static boolean isOnGcp() {
        Boolean cached = onGcp;
        if(cached == null) {
            synchronized (IAMUtil.class) {
                if(onGcp == null) {
                    onGcp = probeMetadataServer();
                }
                cached = onGcp;
            }
        }
        return cached;
    }

    private static boolean probeMetadataServer() {
        final String override = System
                .getProperty("MERCARI_PIPELINE_ON_GCP", System.getenv("MERCARI_PIPELINE_ON_GCP"));
        if(override != null) {
            return Boolean.parseBoolean(override);
        }
        try(final HttpClient client = HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofMillis(500))
                .build()) {
            final HttpResponse<Void> response = client.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://metadata.google.internal/"))
                            .timeout(java.time.Duration.ofMillis(1000))
                            .header("Metadata-Flavor", "Google")
                            .build(),
                    HttpResponse.BodyHandlers.discarding());
            return "Google".equals(response.headers().firstValue("Metadata-Flavor").orElse(null));
        } catch (final Throwable e) {
            return false;
        }
    }

    public static String signJwt(final String serviceAccount, final int expiration) {
        final long exp = DateTime.now().plusSeconds(expiration).getMillis() / 1000;
        final JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("sub", serviceAccount);
        jsonObject.addProperty("exp", exp);
        try(final IamCredentialsClient client = IamCredentialsClient.create(IamCredentialsSettings
                .newBuilder().build())) {
            try {
                final SignJwtResponse res = client.signJwt(SignJwtRequest.newBuilder()
                        .setName(serviceAccount)
                        .setPayload(jsonObject.toString())
                        .build());
                return res.getSignedJwt();
            } catch (final PermissionDeniedException e) {
                throw e;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getMetadataAttribute(final String endpoint, int retry, int intervalMillis) {
        if(!isOnGcp()) {
            return null;
        }
        try(final HttpClient client = HttpClient.newHttpClient()) {
            final HttpResponse<String> response = client.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create(endpoint))
                            .header("Metadata-Flavor", "Google")
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            if(response.statusCode() >= 400 && response.statusCode() < 500) {
                return null;
            } else if(response.statusCode() >= 500) {
                if(retry <= 0) {
                    return null;
                }
                Thread.sleep(intervalMillis);
                return getMetadataAttribute(endpoint, retry-1, intervalMillis);
            }
            return response.body();
        } catch (final Throwable e) {
            return null;
        }
    }

    public static String getMetadataProject() {
        return getMetadataAttribute(ENDPOINT_METADATA_PROJECT, DEFAULT_RETRY, DEFAULT_INTERVAL_MILLIS);
    }

    public static String getMetadataServiceAccount() {
        return getMetadataAttribute(ENDPOINT_METADATA_SERVICE_ACCOUNT, DEFAULT_RETRY, DEFAULT_INTERVAL_MILLIS);
    }

    public static String getMetadataIdToken(final String endpoint) {
        final String url = ENDPOINT_METADATA_ID_TOKEN + endpoint;
        return getMetadataAttribute(url, DEFAULT_RETRY, DEFAULT_INTERVAL_MILLIS);
    }

    public static String getMetadataIdToken(final HttpClient client, final String endpoint)
            throws IOException, URISyntaxException, InterruptedException {
        if(!isOnGcp()) {
            return null;
        }
        final String metaserver = ENDPOINT_METADATA_ID_TOKEN + endpoint;
        final HttpRequest req = HttpRequest.newBuilder()
                .uri(new URI(metaserver))
                .header("Metadata-Flavor", "Google")
                .GET()
                .build();

        final HttpResponse<String> res = client.send(req, HttpResponse.BodyHandlers.ofString());
        return res.body();
    }

    public static AccessToken getAccessToken() throws IOException {
        return GcpCredentialsCache.accessToken();
    }

    public static String getTokenValue() throws IOException {
        return GcpCredentialsCache.accessToken().getTokenValue();
    }

    public static String getAccessToken2() {
        final String body = getMetadataAttribute(ENDPOINT_METADATA_ACCESS_TOKEN, DEFAULT_RETRY, DEFAULT_INTERVAL_MILLIS);
        final JsonElement responseJson = new Gson().fromJson(body, JsonElement.class);
        if(!responseJson.isJsonObject()) {
            throw new IllegalStateException("Illegal token response: " + responseJson);
        }
        final JsonObject jsonObject = responseJson.getAsJsonObject();
        if(!jsonObject.has("access_token")) {
            throw new IllegalStateException("Illegal token response: " + responseJson);
        }
        return jsonObject.get("access_token").getAsString();
    }

    public static String signJwt(final String serviceAccount) {
        final JsonObject payload = new JsonObject();
        if(serviceAccount.startsWith("projects")) {
            var strs = serviceAccount.split("/");
            payload.addProperty("sub", strs[strs.length - 1]);
        } else {
            payload.addProperty("sub", serviceAccount);
        }
        final long exp = DateTime.now().plusSeconds(60).getMillis() / 1000;
        payload.addProperty("exp", exp);

        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        try {
            final Credentials credential = GcpCredentialsCache.credentials();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));

            var iam = new IAMCredentials.Builder(transport, jsonFactory, initializer).build();
            var jwt = iam.projects().serviceAccounts()
                    .signJwt(serviceAccount, new com.google.api.services.iamcredentials.v1.model.SignJwtRequest()
                            .setPayload(payload.toString()))
                    .execute();
            return jwt.getSignedJwt();
        } catch (IOException e) {
            throw new RuntimeException("Failed to signJwt for service account: " + serviceAccount, e);
        }
    }

    public static String signJwt(final String serviceAccount, final String payloadJson) {
        final JsonObject payload = new Gson().fromJson(payloadJson, JsonObject.class);
        return signJwt(serviceAccount, payload);
    }

    public static String signJwt(final String serviceAccount, final JsonObject payload) {
        final HttpTransport transport = new NetHttpTransport();
        final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        try {
            final Credentials credential = GcpCredentialsCache.credentials();
            final HttpRequestInitializer initializer = new ChainingHttpRequestInitializer(
                    new HttpCredentialsAdapter(credential),
                    // Do not log 404. It clutters the output and is possibly even required by the caller.
                    new RetryHttpRequestInitializer(ImmutableList.of(404)));

            var iam = new IAMCredentials.Builder(transport, jsonFactory, initializer).build();
            var jwt = iam.projects().serviceAccounts()
                    .signJwt(serviceAccount, new com.google.api.services.iamcredentials.v1.model.SignJwtRequest()
                            .setPayload(payload.toString()))
                    .execute();
            return jwt.getSignedJwt();
        } catch (IOException e) {
            throw new RuntimeException("Failed to signJwt for service account: " + serviceAccount, e);
        }
    }

    public static boolean isServiceAccountResource(String resource) {
        if(resource == null) {
            return false;
        }
        final Matcher matcher = PATTERN_SERVICE_ACCOUNT.matcher(resource);
        return matcher.find();
    }

    public static boolean isMail(String resource) {
        if(resource == null) {
            return false;
        }
        final Matcher matcher = PATTERN_MAIL.matcher(resource);
        return matcher.find();
    }

    public static String getUserAccount() {
        try(final HttpClient client = HttpClient.newHttpClient()) {
            final GoogleCredentials credentials = GcpCredentialsCache.credentials()
                    .createScoped(Collections.singletonList(SCOPE_USER_INFO));
            credentials.refreshIfExpired();
            final String accessToken = credentials.getAccessToken().getTokenValue();

            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://www.googleapis.com/oauth2/v1/userinfo"))
                    .header("Authorization", "Bearer " + accessToken)
                    .GET()
                    .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                final String responseBody = response.body();
                final Object a = JsonUtil.getJsonPathValue(responseBody, "$.email");
                if(a != null) {
                    return a.toString();
                } else {
                    return null;
                }
            } else {
                System.err.println("API Request failed: HTTP " + response.statusCode());
                System.err.println(response.body());
            }
        } catch (final IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    public static String getIdentityByTokenInfo(GoogleCredentials credential) {
        try {
            credential.refreshIfExpired();
            final String accessToken = credential.getAccessToken().getTokenValue();
            try(final HttpClient client = HttpClient.newHttpClient()) {
                final HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("https://oauth2.googleapis.com/tokeninfo?access_token=" + accessToken))
                        .GET()
                        .build();
                final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                final JsonObject json = new Gson().fromJson(response.body(), JsonObject.class);
                if (json.has("email")) {
                    return json.get("email").toString();
                }
                // If Workload Identity, email is sub (Subject)
                if (json.has("sub")) {
                    return json.get("sub").toString();
                }
                return null;
            }
        } catch (final Exception e) {
            //LOG.error("Failed to resolve identity via Token Info API", e);
            return null;
        }
    }

    public static String getAccount() {
        try {
            final Credentials credential = GcpCredentialsCache.credentials();
            return getAccount(credential);
        } catch (final IOException e) {
            throw new IllegalArgumentException("Failed to get service account", e);
        }
    }

    public static String getAccount(final Credentials credential) {
        return switch (credential) {
            case UserCredentials c -> IAMUtil.getUserAccount();
            case ExternalAccountCredentials c -> {
                final String email = c.getServiceAccountEmail();
                if(email != null && !email.isEmpty()) {
                    yield email;
                }
                yield getIdentityByTokenInfo(c);
            }
            case ServiceAccountCredentials c -> c.getClientEmail();
            case ServiceAccountJwtAccessCredentials c -> c.getClientEmail();
            case ImpersonatedCredentials c -> c.getAccount();
            case ComputeEngineCredentials c -> {
                final String account = c.getAccount();
                if("default".equalsIgnoreCase(account)) {
                    yield getMetadataServiceAccount();
                } else {
                    yield account;
                }
            }
            case null, default -> null;
        };
    }

    public static AccountType accountType(final Credentials credentials) {
        return switch (credentials) {
            case UserCredentials c -> AccountType.USER_ACCOUNT;
            case ExternalAccountAuthorizedUserCredentials c -> AccountType.USER_ACCOUNT;
            case ServiceAccountCredentials c -> AccountType.MACHINE_DIRECT;
            case ServiceAccountJwtAccessCredentials c -> AccountType.MACHINE_DIRECT;
            case ComputeEngineCredentials c -> AccountType.MACHINE_MANAGED;
            case GdchCredentials c -> AccountType.MACHINE_MANAGED;
            case ExternalAccountCredentials c -> AccountType.FEDERATED;
            case ImpersonatedCredentials c -> AccountType.DERIVED_SHORT_LIVED;
            case DownscopedCredentials c -> AccountType.DERIVED_SHORT_LIVED;
            case null -> AccountType.NONE;
            default -> AccountType.UNKNOWN;
        };
    }

}
