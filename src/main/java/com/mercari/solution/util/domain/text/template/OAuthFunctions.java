package com.mercari.solution.util.domain.text.template;

import com.google.gson.JsonElement;
import com.mercari.solution.util.domain.file.JsonUtil;
import com.mercari.solution.util.schema.converter.JsonToMapConverter;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.Signature;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Collectors;

public class OAuthFunctions {

    public Map<String, Object> clientCredentials(String endpoint, String client_id, String client_secret, String scope) {
        return sendRequest(endpoint, Map
                .of("grant_type", "client_credentials",
                        "client_id", client_id,
                        "client_secret", client_secret,
                        "scope", scope));
    }

    public String clientCredentialsToken(String endpoint, String client_id, String client_secret, String scope) {
        final Map<String, Object> response = sendRequest(endpoint, Map.of(
                "grant_type", "client_credentials",
                "client_id", client_id,
                "client_secret", client_secret,
                "scope", scope));
        return getAccessToken(response);
    }

    public Map<String, Object> jwtBearer(String endpoint, String iss, String sub, String aud, long durationMin, String pemKey) {
        final RSAPrivateKey privateKey = loadPrivateKey(pemKey);
        final String jwtAssertion = createJwtAssertion(iss, sub, aud, durationMin, privateKey);
        final Map<String, String> params = Map.of(
                "grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion", jwtAssertion
        );
        return sendRequest(endpoint, params);
    }

    public String jwtBearerToken(String endpoint, String iss, String sub, String aud, long durationMin, String pemKey) {
        final Map<String, Object> response = jwtBearer(endpoint, iss, sub, aud, durationMin, pemKey);
        return getAccessToken(response);
    }

    private Map<String, Object> sendRequest(String endpoint, Map<String, String> params) {
        final String paramsString = params.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .map(entry -> {
                    final String encodedKey = URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8);
                    final String encodedValue = URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8);
                    return encodedKey + "=" + encodedValue;
                })
                .collect(Collectors.joining("&"));
        return sendRequest(endpoint, paramsString);
    }

    private Map<String, Object> sendRequest(String endpoint, String params) {
        try(final HttpClient client = HttpClient.newHttpClient()) {
            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .POST(HttpRequest.BodyPublishers.ofString(params))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if(response.statusCode() >= 400) {
                throw new RuntimeException("Error oauth.clientCredentials. code: " + response.statusCode() + ", message: " + response.body());
            }
            final JsonElement jsonElement = JsonUtil.fromJson(response.body());
            return JsonToMapConverter.convert(jsonElement);
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static RSAPrivateKey loadPrivateKey(String pemKey) {
        final String privateKeyPEM = pemKey
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replaceAll(System.lineSeparator(), "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");
        final byte[] encoded = Base64.getDecoder().decode(privateKeyPEM);
        final PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
        try {
            final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return (RSAPrivateKey) keyFactory.generatePrivate(keySpec);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String createJwtAssertion(
            final String iss,
            final String sub,
            final String aud,
            final long durationMin,
            final RSAPrivateKey privateKey) {
        Base64.Encoder urlEncoder = Base64.getUrlEncoder().withoutPadding();

        String headerJson = "{\"alg\":\"RS256\",\"typ\":\"JWT\"}";
        String encodedHeader = urlEncoder.encodeToString(headerJson.getBytes(StandardCharsets.UTF_8));

        long iat = Instant.now().getEpochSecond();
        long exp = iat + durationMin * 60;
        final String payloadJson = String.format(
                "{\"iss\":\"%s\",\"sub\":\"%s\",\"aud\":\"%s\",\"iat\":%d,\"exp\":%d}",
                iss, sub, aud, iat, exp);
        final String encodedPayload = urlEncoder.encodeToString(payloadJson.getBytes(StandardCharsets.UTF_8));

        String dataToSign = encodedHeader + "." + encodedPayload;

        try {
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(privateKey);
            signature.update(dataToSign.getBytes(StandardCharsets.UTF_8));
            byte[] signatureBytes = signature.sign();

            String encodedSignature = urlEncoder.encodeToString(signatureBytes);

            return dataToSign + "." + encodedSignature;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String getAccessToken(Map<String, Object> response) {
        if(response.isEmpty()) {
            throw new IllegalStateException("clientCredentials response is empty");
        }
        if(!response.containsKey("access_token") && !response.containsKey("accessToken")) {
            throw new IllegalStateException("clientCredentials response does not contains access_token: " + response.keySet());
        }
        if(response.containsKey("access_token")) {
            return response.get("access_token").toString();
        } else {
            return response.get("accessToken").toString();
        }
    }
}
