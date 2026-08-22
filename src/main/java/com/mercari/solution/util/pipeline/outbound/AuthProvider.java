package com.mercari.solution.util.pipeline.outbound;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.gson.JsonObject;
import com.mercari.solution.util.domain.file.JsonUtil;
import com.mercari.solution.util.TemplateUtil;
import com.mercari.solution.util.cloud.google.GcpCredentialsCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Supplies credentials for outbound HTTP requests.
 *
 * <p>Created once per worker ({@code @Setup}) from serializable {@link Parameters}; token-bearing
 * providers cache the token until shortly before expiry and re-fetch on {@link #invalidate()}
 * (called once after a 401). All configuration values are static templates (resolved once,
 * typically {@code ${utils.secrets.get(...)}}) — element fields are not available.
 */
public interface AuthProvider {

    Logger LOG = LoggerFactory.getLogger(AuthProvider.class);

    String SCOPE_CLOUD_PLATFORM = "https://www.googleapis.com/auth/cloud-platform";

    enum Type {
        none,
        basic,
        bearer,
        apiKey,
        oauth2,
        gcpOidc,
        gcpOauth
    }

    enum In {
        header,
        query
    }

    enum Grant {
        clientCredentials,
        jwtBearer,
        refreshToken
    }

    /** Headers to add to the request (may be empty). */
    Map<String, String> headers() throws IOException;

    /** Query parameters to add to the request URL (may be empty). */
    default Map<String, String> queryParams() {
        return Map.of();
    }

    /** Drops any cached token so the next {@link #headers()} call fetches a fresh one. */
    default void invalidate() {}

    default boolean isNone() {
        return false;
    }

    class Parameters implements Serializable {

        public Type type;
        // basic
        public String username;
        public String password;
        // bearer
        public String token;
        // apiKey
        public String name;
        public String value;
        public In in;
        // oauth2
        public Grant grant;
        public String tokenUrl;
        public String clientId;
        public String clientSecret;
        public String scope;        // oauth2 / gcpOauth
        public String audience;     // oauth2 / gcpOidc
        public Integer refreshBeforeSeconds;
        // oauth2 refreshToken
        public String refreshToken;
        // oauth2 jwtBearer
        public String issuer;
        public String subject;
        public String privateKey;   // PKCS#8 PEM (use ${utils.secrets.get(...)})
        public String keyId;
        public Integer jwtLifetimeMinutes;
        // gcp
        public String serviceAccount;

        public List<String> validate(final String prefix) {
            final List<String> errorMessages = new ArrayList<>();
            if(type == null) {
                return errorMessages;
            }
            switch (type) {
                case basic -> {
                    if(username == null) {
                        errorMessages.add(prefix + ".username must not be null for auth.type basic");
                    }
                    if(password == null) {
                        errorMessages.add(prefix + ".password must not be null for auth.type basic");
                    }
                }
                case bearer -> {
                    if(token == null) {
                        errorMessages.add(prefix + ".token must not be null for auth.type bearer");
                    }
                }
                case apiKey -> {
                    if(name == null) {
                        errorMessages.add(prefix + ".name must not be null for auth.type apiKey");
                    }
                    if(value == null) {
                        errorMessages.add(prefix + ".value must not be null for auth.type apiKey");
                    }
                }
                case oauth2 -> {
                    if(tokenUrl == null) {
                        errorMessages.add(prefix + ".tokenUrl must not be null for auth.type oauth2");
                    }
                    if(Grant.jwtBearer.equals(grant)) {
                        if(issuer == null) {
                            errorMessages.add(prefix + ".issuer must not be null for oauth2 grant jwtBearer");
                        }
                        if(privateKey == null) {
                            errorMessages.add(prefix + ".privateKey must not be null for oauth2 grant jwtBearer");
                        }
                        if(jwtLifetimeMinutes != null && jwtLifetimeMinutes < 1) {
                            errorMessages.add(prefix + ".jwtLifetimeMinutes must be >= 1");
                        }
                    } else if(Grant.refreshToken.equals(grant)) {
                        if(refreshToken == null) {
                            errorMessages.add(prefix + ".refreshToken must not be null for oauth2 grant refreshToken");
                        }
                        if(clientId == null) {
                            errorMessages.add(prefix + ".clientId must not be null for oauth2 grant refreshToken");
                        }
                    } else {
                        if(clientId == null) {
                            errorMessages.add(prefix + ".clientId must not be null for auth.type oauth2");
                        }
                        if(clientSecret == null) {
                            errorMessages.add(prefix + ".clientSecret must not be null for auth.type oauth2");
                        }
                    }
                }
                default -> {}
            }
            if(refreshBeforeSeconds != null && refreshBeforeSeconds < 0) {
                errorMessages.add(prefix + ".refreshBeforeSeconds must be >= 0");
            }
            return errorMessages;
        }

        public void setDefaults() {
            if(type == null) {
                type = Type.none;
            }
            if(in == null) {
                in = In.header;
            }
            if(grant == null) {
                grant = Grant.clientCredentials;
            }
            if(refreshBeforeSeconds == null) {
                refreshBeforeSeconds = 60;
            }
            if(jwtLifetimeMinutes == null) {
                jwtLifetimeMinutes = 60;
            }
            if(Type.gcpOauth.equals(type) && scope == null) {
                scope = SCOPE_CLOUD_PLATFORM;
            }
        }

        public boolean isNone() {
            return type == null || Type.none.equals(type);
        }
    }

    /**
     * Creates the provider. {@code defaultAudience} is used for gcpOidc when {@code audience} is
     * omitted (the static origin of the target URL).
     */
    static AuthProvider create(final Parameters parameters, final String defaultAudience) {
        if(parameters == null || parameters.isNone()) {
            return NONE;
        }
        final Map<String, Object> values = new HashMap<>();
        TemplateUtil.setFunctions(values);
        return switch (parameters.type) {
            case none -> NONE;
            case basic -> {
                final String credential = render(parameters.username, values) + ":" + render(parameters.password, values);
                yield new StaticAuth(Map.of("Authorization", "Basic " + Base64.getEncoder().encodeToString(credential.getBytes(StandardCharsets.UTF_8))), Map.of());
            }
            case bearer -> new StaticAuth(Map.of("Authorization", "Bearer " + render(parameters.token, values)), Map.of());
            case apiKey -> {
                final String name = render(parameters.name, values);
                final String value = render(parameters.value, values);
                yield In.query.equals(parameters.in)
                        ? new StaticAuth(Map.of(), Map.of(name, value))
                        : new StaticAuth(Map.of(name, value), Map.of());
            }
            case oauth2 -> Grant.refreshToken.equals(parameters.grant)
                    ? new OAuth2RefreshToken(
                            render(parameters.tokenUrl, values),
                            render(parameters.clientId, values),
                            render(parameters.clientSecret, values),
                            render(parameters.refreshToken, values),
                            render(parameters.scope, values),
                            parameters.refreshBeforeSeconds)
                    : Grant.jwtBearer.equals(parameters.grant)
                    ? new OAuth2JwtBearer(
                            render(parameters.tokenUrl, values),
                            render(parameters.issuer, values),
                            Optional.ofNullable(render(parameters.subject, values)).orElse(render(parameters.issuer, values)),
                            Optional.ofNullable(render(parameters.audience, values)).orElse(render(parameters.tokenUrl, values)),
                            render(parameters.scope, values),
                            render(parameters.privateKey, values),
                            render(parameters.keyId, values),
                            parameters.jwtLifetimeMinutes,
                            parameters.refreshBeforeSeconds)
                    : new OAuth2ClientCredentials(
                            render(parameters.tokenUrl, values),
                            render(parameters.clientId, values),
                            render(parameters.clientSecret, values),
                            render(parameters.scope, values),
                            render(parameters.audience, values),
                            parameters.refreshBeforeSeconds);
            case gcpOauth -> new GcpOauth(render(parameters.scope, values), render(parameters.serviceAccount, values));
            case gcpOidc -> new GcpOidc(
                    Optional.ofNullable(render(parameters.audience, values)).orElse(defaultAudience),
                    render(parameters.serviceAccount, values));
        };
    }

    private static String render(final String text, final Map<String, Object> values) {
        if(text == null || !TemplateUtil.isTemplateText(text)) {
            return text;
        }
        return TemplateUtil.executeStrictTemplate(TemplateUtil.createStrictTemplate("auth", text), values);
    }

    AuthProvider NONE = new AuthProvider() {
        @Override
        public Map<String, String> headers() {
            return Map.of();
        }
        @Override
        public boolean isNone() {
            return true;
        }
    };

    class StaticAuth implements AuthProvider {
        private final Map<String, String> headers;
        private final Map<String, String> queryParams;

        StaticAuth(final Map<String, String> headers, final Map<String, String> queryParams) {
            this.headers = headers;
            this.queryParams = queryParams;
        }

        @Override
        public Map<String, String> headers() {
            return headers;
        }

        @Override
        public Map<String, String> queryParams() {
            return queryParams;
        }
    }

    /** Cached token with expiry; shared refresh logic for token-bearing providers. */
    abstract class CachedTokenAuth implements AuthProvider {

        public record Cached(String token, Instant expiresAt) {}

        private final AtomicReference<Cached> cached = new AtomicReference<>();
        private final int refreshBeforeSeconds;

        CachedTokenAuth(final int refreshBeforeSeconds) {
            this.refreshBeforeSeconds = refreshBeforeSeconds;
        }

        /** Fetches a fresh token; expiresAt may be null for tokens whose lifetime is unknown (re-fetched each hour). */
        protected abstract Cached fetch() throws IOException;

        protected Cached cached(final String token, final Instant expiresAt) {
            return new Cached(token, expiresAt);
        }

        @Override
        public Map<String, String> headers() throws IOException {
            return Map.of("Authorization", "Bearer " + token());
        }

        protected String token() throws IOException {
            final Cached c = cached.get();
            if(c != null && c.expiresAt != null && Instant.now().plusSeconds(refreshBeforeSeconds).isBefore(c.expiresAt)) {
                return c.token;
            }
            synchronized (this) {
                final Cached again = cached.get();
                if(again != null && again != c && again.expiresAt != null && Instant.now().plusSeconds(refreshBeforeSeconds).isBefore(again.expiresAt)) {
                    return again.token;
                }
                Cached fresh = fetch();
                if(fresh.expiresAt == null) {
                    fresh = new Cached(fresh.token, Instant.now().plus(Duration.ofHours(1)));
                }
                cached.set(fresh);
                return fresh.token;
            }
        }

        @Override
        public void invalidate() {
            cached.set(null);
        }
    }

    class OAuth2ClientCredentials extends CachedTokenAuth {

        private final String tokenUrl;
        private final String clientId;
        private final String clientSecret;
        private final String scope;
        private final String audience;
        private transient HttpClient client;

        OAuth2ClientCredentials(String tokenUrl, String clientId, String clientSecret, String scope, String audience, int refreshBeforeSeconds) {
            super(refreshBeforeSeconds);
            this.tokenUrl = tokenUrl;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            this.scope = scope;
            this.audience = audience;
        }

        @Override
        protected Cached fetch() throws IOException {
            if(client == null) {
                client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            }
            final Map<String, String> form = new LinkedHashMap<>();
            form.put("grant_type", "client_credentials");
            if(scope != null) {
                form.put("scope", scope);
            }
            if(audience != null) {
                form.put("audience", audience);
            }
            final String basic = Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(tokenUrl))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("Accept", "application/json")
                    .header("Authorization", "Basic " + basic)
                    .POST(HttpRequest.BodyPublishers.ofString(formEncode(form)))
                    .build();
            return postTokenRequest(client, request, tokenUrl);
        }
    }

    /**
     * OAuth2 refresh-token grant (RFC 6749 §6): a long-lived refresh token obtained out of band
     * (user-delegated APIs) is exchanged for access tokens. Client authentication is HTTP basic
     * when {@code clientSecret} is set, otherwise {@code client_id} in the form (public clients).
     * A rotated refresh token in the response replaces the current one for the worker's lifetime.
     */
    class OAuth2RefreshToken extends CachedTokenAuth {

        private final String tokenUrl;
        private final String clientId;
        private final String clientSecret;
        private final String scope;
        private volatile String refreshToken;
        private transient HttpClient client;

        OAuth2RefreshToken(String tokenUrl, String clientId, String clientSecret, String refreshToken, String scope, int refreshBeforeSeconds) {
            super(refreshBeforeSeconds);
            this.tokenUrl = tokenUrl;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            this.refreshToken = refreshToken;
            this.scope = scope;
        }

        @Override
        protected Cached fetch() throws IOException {
            if(client == null) {
                client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            }
            final Map<String, String> form = new LinkedHashMap<>();
            form.put("grant_type", "refresh_token");
            form.put("refresh_token", refreshToken);
            if(scope != null) {
                form.put("scope", scope);
            }
            final HttpRequest.Builder request = HttpRequest.newBuilder()
                    .uri(URI.create(tokenUrl))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("Accept", "application/json");
            if(clientSecret != null) {
                request.header("Authorization", "Basic " + Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8)));
            } else {
                form.put("client_id", clientId);
            }
            final TokenResponse response = postTokenRequestFull(client, request.POST(HttpRequest.BodyPublishers.ofString(formEncode(form))).build(), tokenUrl);
            if(response.refreshToken() != null && !response.refreshToken().isBlank()) {
                refreshToken = response.refreshToken();   // rotation
            }
            return response.cached();
        }
    }

    /** OAuth2 JWT bearer grant (RFC 7523): a self-signed RS256 assertion is exchanged for an access token. */
    class OAuth2JwtBearer extends CachedTokenAuth {

        private final String tokenUrl;
        private final String issuer;
        private final String subject;
        private final String audience;
        private final String scope;
        private final String privateKeyPem;
        private final String keyId;
        private final int lifetimeMinutes;
        private transient java.security.interfaces.RSAPrivateKey privateKey;
        private transient HttpClient client;

        OAuth2JwtBearer(String tokenUrl, String issuer, String subject, String audience, String scope,
                        String privateKeyPem, String keyId, int lifetimeMinutes, int refreshBeforeSeconds) {
            super(refreshBeforeSeconds);
            this.tokenUrl = tokenUrl;
            this.issuer = issuer;
            this.subject = subject;
            this.audience = audience;
            this.scope = scope;
            this.privateKeyPem = privateKeyPem;
            this.keyId = keyId;
            this.lifetimeMinutes = lifetimeMinutes;
        }

        @Override
        protected Cached fetch() throws IOException {
            if(privateKey == null) {
                privateKey = com.mercari.solution.util.domain.text.template.OAuthFunctions.loadPrivateKey(privateKeyPem);
            }
            if(client == null) {
                client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            }
            final String assertion = com.mercari.solution.util.domain.text.template.OAuthFunctions
                    .createJwtAssertion(issuer, subject, audience, scope, keyId, lifetimeMinutes, privateKey);
            final Map<String, String> form = new LinkedHashMap<>();
            form.put("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer");
            form.put("assertion", assertion);
            if(scope != null) {
                form.put("scope", scope);
            }
            final HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(tokenUrl))
                    .timeout(Duration.ofSeconds(30))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(formEncode(form)))
                    .build();
            return postTokenRequest(client, request, tokenUrl);
        }
    }

    record TokenResponse(CachedTokenAuth.Cached cached, String refreshToken) {}

    /** Sends a token request and reads access_token / expires_in. */
    static CachedTokenAuth.Cached postTokenRequest(final HttpClient client, final HttpRequest request, final String tokenUrl) throws IOException {
        return postTokenRequestFull(client, request, tokenUrl).cached();
    }

    static TokenResponse postTokenRequestFull(final HttpClient client, final HttpRequest request, final String tokenUrl) throws IOException {
        try {
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if(response.statusCode() / 100 != 2) {
                throw new IOException("oauth2 token request to " + tokenUrl + " failed with status " + response.statusCode() + ": " + response.body());
            }
            final JsonObject json = JsonUtil.fromJson(response.body(), JsonObject.class);
            if(!json.has("access_token")) {
                throw new IOException("oauth2 token response has no access_token: " + response.body());
            }
            final String token = json.get("access_token").getAsString();
            final Instant expiresAt = json.has("expires_in") && !json.get("expires_in").isJsonNull()
                    ? Instant.now().plusSeconds(json.get("expires_in").getAsLong())
                    : null;
            LOG.info("oauth2 access token acquired from {} (expires at {})", tokenUrl, expiresAt);
            final String rotated = json.has("refresh_token") && !json.get("refresh_token").isJsonNull() ? json.get("refresh_token").getAsString() : null;
            return new TokenResponse(new CachedTokenAuth.Cached(token, expiresAt), rotated);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("oauth2 token request interrupted", e);
        }
    }

    class GcpOauth extends CachedTokenAuth {

        private final String scope;
        private final String serviceAccount;
        private transient GoogleCredentials credentials;

        GcpOauth(final String scope, final String serviceAccount) {
            super(60);
            this.scope = scope == null ? SCOPE_CLOUD_PLATFORM : scope;
            this.serviceAccount = serviceAccount;
        }

        @Override
        protected Cached fetch() throws IOException {
            if(credentials == null) {
                GoogleCredentials source = GcpCredentialsCache.credentials();
                if(serviceAccount != null) {
                    source = ImpersonatedCredentials.create(source, serviceAccount, null, List.of(scope), 3600);
                } else {
                    source = source.createScoped(scope);
                }
                credentials = source;
            }
            credentials.refreshIfExpired();
            final AccessToken token = credentials.getAccessToken();
            return cached(token.getTokenValue(), token.getExpirationTime() == null ? null : token.getExpirationTime().toInstant());
        }
    }

    class GcpOidc extends CachedTokenAuth {

        private final String audience;
        private final String serviceAccount;
        private transient IdTokenCredentials credentials;

        GcpOidc(final String audience, final String serviceAccount) {
            super(60);
            this.audience = audience;
            this.serviceAccount = serviceAccount;
        }

        @Override
        protected Cached fetch() throws IOException {
            if(audience == null) {
                throw new IOException("auth.audience must be set for gcpOidc when the target url is not static");
            }
            if(credentials == null) {
                final GoogleCredentials source = GcpCredentialsCache.credentials();
                final IdTokenProvider provider;
                if(serviceAccount != null) {
                    provider = ImpersonatedCredentials.create(source, serviceAccount, null, List.of(SCOPE_CLOUD_PLATFORM), 3600);
                } else if(source instanceof IdTokenProvider p) {
                    provider = p;
                } else {
                    throw new IOException("GCP credentials of type " + source.getClass().getSimpleName()
                            + " cannot mint ID tokens; set auth.serviceAccount to impersonate a service account");
                }
                credentials = IdTokenCredentials.newBuilder()
                        .setIdTokenProvider(provider)
                        .setTargetAudience(audience)
                        .build();
            }
            credentials.refreshIfExpired();
            final AccessToken token = credentials.getIdToken();
            return cached(token.getTokenValue(), token.getExpirationTime() == null ? null : token.getExpirationTime().toInstant());
        }
    }

    static String formEncode(final Map<String, String> form) {
        final StringBuilder sb = new StringBuilder();
        for(final Map.Entry<String, String> entry : form.entrySet()) {
            if(sb.length() > 0) {
                sb.append('&');
            }
            sb.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8))
                    .append('=')
                    .append(URLEncoder.encode(entry.getValue() == null ? "" : entry.getValue(), StandardCharsets.UTF_8));
        }
        return sb.toString();
    }
}
