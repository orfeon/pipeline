package com.mercari.solution.util.cloud.hashicorp;

import com.google.gson.*;
import com.mercari.solution.util.domain.file.JsonUtil;
import com.mercari.solution.util.cloud.google.IAMUtil;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class VaultClient {

    private static Logger LOG = LoggerFactory.getLogger(VaultClient.class);

    private static final String HEADER_TOKEN = "X-Vault-Token";
    private static final String HEADER_NAMESPACE = "X-Vault-Namespace";

    private static final String DEFAULT_ENDPOINT_GCP_AUTH = "/v1/auth/gcp/login";
    private static final String DEFAULT_ENDPOINT_AWS_AUTH = "/v1/auth/aws/login";
    private static final String ENDPOINT_VAULT_REVOKE = "/v1/auth/token/revoke-self";

    private final String vaultHost;
    private final String namespace;
    private final HttpClient client;

    private final String token;
    // true when this client performed a login and owns the token lifecycle;
    // false when the token was supplied externally (must not be revoked here)
    private final boolean loginToken;

    /** Vault gcp auth backend: login with a GCP-signed JWT. */
    public VaultClient(final String vaultHost,
                       final String serviceAccount,
                       final String namespace,
                       final String role,
                       final String endpointToken) {

        this(vaultHost, namespace, loginGcp(vaultHost, serviceAccount, namespace, role, endpointToken), true);
    }

    public static VaultClient withGcpAuth(
            final String vaultHost,
            final String serviceAccount,
            final String namespace,
            final String role,
            final String endpointOverride) {

        return new VaultClient(vaultHost, serviceAccount, namespace, role, endpointOverride);
    }

    /** Vault aws auth backend (iam method): login with a SigV4-signed sts:GetCallerIdentity request. */
    public static VaultClient withAwsIamAuth(
            final String vaultHost,
            final String namespace,
            final String role,
            final String iamServerId,
            final String endpointOverride) {

        final JsonObject payload = createAwsIamLoginPayload(role, iamServerId);
        final String token = login(
                vaultHost, namespace,
                endpointOverride == null ? DEFAULT_ENDPOINT_AWS_AUTH : endpointOverride,
                payload);
        return new VaultClient(vaultHost, namespace, token, true);
    }

    /** A pre-issued token (e.g. from the VAULT_TOKEN environment variable); never revoked by this client. */
    public static VaultClient withToken(
            final String vaultHost,
            final String namespace,
            final String token) {

        return new VaultClient(vaultHost, namespace, token, false);
    }

    private VaultClient(final String vaultHost,
                        final String namespace,
                        final String token,
                        final boolean loginToken) {

        if(!vaultHost.startsWith("http://") && !vaultHost.startsWith("https://")) {
            throw new IllegalArgumentException("host must be start with http:// or https://, got: " + vaultHost);
        }
        this.vaultHost = vaultHost;
        this.namespace = namespace;
        this.client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(Duration.ofSeconds(60))
                .build();
        this.token = token;
        this.loginToken = loginToken;
    }

    // Calling Vault API: https://www.vaultproject.io/api/auth/gcp#login
    private static String loginGcp(
            final String vaultHost,
            final String serviceAccount,
            final String namespace,
            final String role,
            final String endpointOverride) {

        final String jwt = IAMUtil.signJwt(serviceAccount, 60);
        final JsonObject payload = new JsonObject();
        payload.addProperty("jwt", jwt);
        payload.addProperty("role", role);
        return login(
                vaultHost, namespace,
                endpointOverride == null ? DEFAULT_ENDPOINT_GCP_AUTH : endpointOverride,
                payload);
    }

    // Calling Vault API: https://developer.hashicorp.com/vault/api-docs/auth/aws#login
    private static JsonObject createAwsIamLoginPayload(final String role, final String iamServerId) {
        final String stsUrl = "https://sts.amazonaws.com/";
        final byte[] body = "Action=GetCallerIdentity&Version=2011-06-15".getBytes(StandardCharsets.UTF_8);

        final SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
                .method(SdkHttpMethod.POST)
                .uri(URI.create(stsUrl))
                .putHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
                .contentStreamProvider(() -> new ByteArrayInputStream(body));
        if(iamServerId != null) {
            requestBuilder.putHeader("X-Vault-AWS-IAM-Server-ID", iamServerId);
        }
        final SdkHttpFullRequest signed = Aws4Signer.create().sign(
                requestBuilder.build(),
                Aws4SignerParams.builder()
                        .awsCredentials(DefaultCredentialsProvider.create().resolveCredentials())
                        .signingName("sts")
                        .signingRegion(Region.US_EAST_1)
                        .build());

        final JsonObject headers = new JsonObject();
        signed.headers().forEach((name, values) -> {
            final JsonArray array = new JsonArray();
            values.forEach(array::add);
            headers.add(name, array);
        });

        final JsonObject payload = new JsonObject();
        if(role != null) {
            payload.addProperty("role", role);
        }
        payload.addProperty("iam_http_request_method", "POST");
        payload.addProperty("iam_request_url", Base64.getEncoder().encodeToString(stsUrl.getBytes(StandardCharsets.UTF_8)));
        payload.addProperty("iam_request_body", Base64.getEncoder().encodeToString(body));
        payload.addProperty("iam_request_headers", Base64.getEncoder().encodeToString(headers.toString().getBytes(StandardCharsets.UTF_8)));
        return payload;
    }

    private static String login(
            final String vaultHost,
            final String namespace,
            final String endpoint,
            final JsonObject payload) {

        final String url = createUrl(vaultHost, endpoint);
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload.toString()));
        if(namespace != null) {
            requestBuilder.header(HEADER_NAMESPACE, namespace);
        }

        try(final HttpClient loginClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .followRedirects(HttpClient.Redirect.NEVER)
                .connectTimeout(Duration.ofSeconds(60))
                .build()) {
            final HttpResponse<String> response = loginClient
                    .send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            final JsonObject responseJson = new Gson().fromJson(response.body(), JsonObject.class);

            if (responseJson != null && responseJson.has("errors")) {
                throw new RuntimeException(String.format("Vault login errors: %s", responseJson));
            } else if (response.statusCode() != 200) {
                throw new RuntimeException(String.format("Failed to access to %s, cause: %s", url, response.body()));
            }
            return responseJson.getAsJsonObject("auth").get("client_token").getAsString();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public JsonObject call(final String endpoint,
                           final String method,
                           final String body,
                           final int retry) throws IOException, InterruptedException {

        final String url = createUrl(vaultHost, endpoint);
        final HttpRequest.BodyPublisher bodyPublisher = body == null ?
                HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body);
        final HttpRequest.Builder request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header(HEADER_TOKEN, token)
                .header("Content-Type", "application/json")
                .method(method, bodyPublisher);

        if(namespace != null) {
            request.header(HEADER_NAMESPACE, namespace);
        }
        if(token != null) {
            request.header(HEADER_TOKEN, token);
        }

        final HttpResponse response = client
                .send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

        if(response.statusCode() >= 400) {
            throw new RuntimeException(String.format("Failed to call endpoint %s:%s, body: %s, status: %d, message: %s",
                    endpoint, method, body, response.statusCode(), response.body()));
        }
        final JsonObject responseJson = new Gson().fromJson(response.body().toString(), JsonObject.class);
        if(responseJson != null && responseJson.has("errors")) {
            throw new RuntimeException(String.format("Errors: endpoint %s:%s, body: %s, status: %d, message: %s",
                    endpoint, method, body, response.statusCode(), responseJson.toString()));
        }

        return responseJson;
    }

    // Calling Vault API: https://www.vaultproject.io/api-docs/auth/token#revoke-a-token-self
    public void revokeToken() throws IOException, InterruptedException {
        if(!loginToken) {
            // externally supplied token (withToken): its lifecycle is not ours to end
            return;
        }
        call(ENDPOINT_VAULT_REVOKE, "POST", "", 3);
    }

    // Calling Vault API: https://www.vaultproject.io/api/secret/kv/kv-v2.html#read-secret-version
    public JsonObject readKVSecret(final String endpoint) throws IOException, InterruptedException {
        final JsonObject responseJson = call(endpoint, "GET", null, 3);
        return responseJson.get("data").getAsJsonObject().get("data").getAsJsonObject();
    }

    // Calling Vault API: https://www.vaultproject.io/api/secret/transit/index.html#decrypt-data
    public String decryptSecret(final String endpoint,
                                final String encrypted) {

        final JsonArray batchCipherTexts = new JsonArray();
        final JsonObject childAttr = new JsonObject();
        childAttr.addProperty("ciphertext", encrypted);
        batchCipherTexts.add(childAttr);
        final JsonObject jsonObject = new JsonObject();
        jsonObject.add("batch_input", batchCipherTexts);

        try {
            final JsonObject responseJson = call(endpoint, "POST", jsonObject.toString(), 3);
            final JsonArray results = responseJson.getAsJsonObject("data").getAsJsonArray("batch_results");
            return results.get(0).getAsJsonObject().get("plaintext").getAsString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // Calling Vault API: https://www.vaultproject.io/api/secret/transit/index.html#decrypt-data
    public List<String> decryptSecret(final String endpoint,
                                      final List<String> encrypted) throws IOException, InterruptedException {

        final JsonArray batchCipherTexts = new JsonArray();
        encrypted.forEach(v -> {
            final JsonObject childAttr = new JsonObject();
            childAttr.addProperty("ciphertext", v);
            batchCipherTexts.add(childAttr);
        });
        final JsonObject jsonObject = new JsonObject();
        jsonObject.add("batch_input", batchCipherTexts);

        final JsonObject responseJson = call(endpoint, "POST", jsonObject.toString(), 3);
        final JsonArray results = responseJson.getAsJsonObject("data").getAsJsonArray("batch_results");
        final List<String> decrypted = new ArrayList<>();
        for(int i=0; i<results.size(); i++) {
            decrypted.add(results.get(i).getAsJsonObject().get("plaintext").getAsString());
        }
        return decrypted;
    }

    // Calling Vault API: https://www.vaultproject.io/api/secret/transit/index.html#decrypt-data
    public JsonElement decryptSecrets(final String endpoint,
                                      final String jsonPath,
                                      final JsonElement data) {

        try {
            if (data.isJsonPrimitive()) {
                final List<String> decrypted = decryptSecret(endpoint, Arrays.asList(data.getAsString()));
                return new JsonPrimitive(decrypted.get(0));
            } else if (data.isJsonArray()) {
                final List<String> encrypted = new ArrayList<>();
                for (JsonElement element : data.getAsJsonArray()) {
                    encrypted.add(element.getAsString());
                }
                final List<String> decrypted = decryptSecret(endpoint, encrypted);
                final JsonArray array = new JsonArray(decrypted.size());
                decrypted.forEach(array::add);
                return array;
            } else {
                final List<KV<String, Object>> pathAndEncrypted = JsonUtil.read(data.getAsJsonObject(), jsonPath);

                final List<String> encrypted = pathAndEncrypted.stream()
                        .map(KV::getValue)
                        .map(Object::toString)
                        .collect(Collectors.toList());

                final List<KV<String, Object>> pathAndDecrypted = new ArrayList<>();
                final List<String> decrypted = decryptSecret(endpoint, encrypted);
                for (int i = 0; i < pathAndEncrypted.size(); i++) {
                    pathAndDecrypted.add(KV.of(pathAndEncrypted.get(i).getKey(), decrypted.get(i)));
                }
                final JsonObject result = JsonUtil.set(data.getAsJsonObject(), pathAndDecrypted);
                return result;
            }
        } catch (Exception e) {
            throw new IllegalStateException("Faild to decrypt data: " + data.toString()
                    + " with jsonPath: " + jsonPath, e);
        }
    }

    private static String createUrl(String host, String endpoint) {
        if(!host.startsWith("http://") && !host.startsWith("https://")) {
            throw new IllegalArgumentException("host must be start with http:// or https://, got: " + host);
        }
        if(host.endsWith("/")) {
           host = host.substring(0, host.length() - 1);
        }
        if(!endpoint.startsWith("/")) {
            endpoint = "/" + endpoint;
        }
        return host + endpoint;
    }

}
