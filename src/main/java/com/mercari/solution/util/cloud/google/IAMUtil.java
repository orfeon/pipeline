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
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import com.google.cloud.iam.credentials.v1.SignJwtRequest;
import com.google.cloud.iam.credentials.v1.SignJwtResponse;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.joda.time.DateTime;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IAMUtil {

    private static final String ENDPOINT_METADATA_ID_TOKEN = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience=";
    private static final String ENDPOINT_METADATA_ACCESS_TOKEN = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
    private static final String ENDPOINT_METADATA_PROJECT = "http://metadata.google.internal/computeMetadata/v1/project/project-id";
    private static final String ENDPOINT_METADATA_SERVICE_ACCOUNT = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/email";

    private static final Pattern PATTERN_MAIL = Pattern.compile("^[a-zA-Z0-9_.+-]+@([a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]*\\.)+[a-zA-Z]{2,}$");
    private static final Pattern PATTERN_SERVICE_ACCOUNT = Pattern.compile("^projects\\/-\\/serviceAccounts\\/[a-zA-Z0-9_.+-]+@([a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]*\\.)+[a-zA-Z]{2,}$");

    private static final Integer DEFAULT_RETRY = 3;
    private static final Integer DEFAULT_INTERVAL_MILLIS = 5000;

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

    public static String getProject() {
        return getMetadataAttribute(ENDPOINT_METADATA_PROJECT, DEFAULT_RETRY, DEFAULT_INTERVAL_MILLIS);
    }

    public static String getServiceAccount() {
        return getMetadataAttribute(ENDPOINT_METADATA_SERVICE_ACCOUNT, DEFAULT_RETRY, DEFAULT_INTERVAL_MILLIS);
    }

    private static String getMetadataAttribute(final String endpoint, int retry, int intervalMillis) {
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

    public static String getIdToken(final String endpoint) {
        final String url = ENDPOINT_METADATA_ID_TOKEN + endpoint;
        return getMetadataAttribute(url, DEFAULT_RETRY, DEFAULT_INTERVAL_MILLIS);
    }

    public static String getIdToken(final HttpClient client, final String endpoint)
            throws IOException, URISyntaxException, InterruptedException {
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
        final GoogleCredentials credentials = GoogleCredentials
                .getApplicationDefault()
                .createScoped("https://www.googleapis.com/auth/cloud-platform");
        return credentials.refreshAccessToken();
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
            final Credentials credential = GoogleCredentials.getApplicationDefault();
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
            final Credentials credential = GoogleCredentials.getApplicationDefault();
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


}
