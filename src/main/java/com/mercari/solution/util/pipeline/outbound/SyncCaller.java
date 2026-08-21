package com.mercari.solution.util.pipeline.outbound;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/**
 * Blocking send-with-retry over {@link HttpTransport} + {@link ResponsePolicy}: the synchronous
 * counterpart of the http sink's async state machine, for callers that run one request at a time
 * (actions, sources). Retries on the policy's retry verdict and on transport errors with the
 * policy's backoff, refreshes credentials once on 401.
 */
public final class SyncCaller {

    private static final Logger LOG = LoggerFactory.getLogger(SyncCaller.class);

    private SyncCaller() {}

    /** Final result: the last response (null when every attempt failed at the transport level). */
    public record Result(
            OutboundRequest request,
            OutboundRequest.Response response,
            ResponsePolicy.Parsed parsed,
            ResponsePolicy.Verdict verdict,
            int attempts,
            String error) {

        public boolean succeeded() {
            return ResponsePolicy.Verdict.SUCCESS.equals(verdict);
        }
    }

    public static Result call(
            final String name,
            final HttpTransport transport,
            final ResponsePolicy policy,
            final OutboundRequest request) throws InterruptedException {

        final Instant startedAt = Instant.now();
        boolean authRetried = false;
        int attempt = 0;
        while(true) {
            attempt++;
            OutboundRequest.Response response;
            try {
                response = transport.sendSync(request);
            } catch (final IOException e) {
                final Duration backoff = policy.backoff(attempt, null, startedAt);
                if(backoff == null) {
                    return new Result(request, null, null, ResponsePolicy.Verdict.FAILED, attempt,
                            "request failed after " + attempt + " attempt(s): " + e.getMessage());
                }
                LOG.warn("{}: request to {} failed ({}), retrying in {} ms (attempt {})", name, request.url(), e.toString(), backoff.toMillis(), attempt + 1);
                Thread.sleep(backoff.toMillis());
                continue;
            }
            final ResponsePolicy.Parsed parsed = policy.parse(response);
            if(response.statusCode() == 401 && !authRetried && !transport.auth().isNone()) {
                LOG.warn("{}: 401 from {}, refreshing credentials once", name, request.url());
                transport.auth().invalidate();
                authRetried = true;
                continue;
            }
            final ResponsePolicy.Verdict verdict = policy.classify(response, parsed);
            switch (verdict) {
                case SUCCESS -> {
                    return new Result(request, response, parsed, verdict, attempt, null);
                }
                case RETRY -> {
                    final Duration backoff = policy.backoff(attempt, response, startedAt);
                    if(backoff == null) {
                        return new Result(request, response, parsed, ResponsePolicy.Verdict.FAILED, attempt,
                                "status " + response.statusCode() + " after " + attempt + " attempt(s): " + abbreviate(parsed.text()));
                    }
                    LOG.warn("{}: status {} from {}, retrying in {} ms (attempt {})", name, response.statusCode(), request.url(), backoff.toMillis(), attempt + 1);
                    Thread.sleep(backoff.toMillis());
                }
                default -> {
                    return new Result(request, response, parsed, verdict, attempt,
                            "status " + response.statusCode() + ": " + abbreviate(parsed.text()));
                }
            }
        }
    }

    public static String abbreviate(final String text) {
        if(text == null) {
            return "";
        }
        return text.length() > 512 ? text.substring(0, 512) + "..." : text;
    }
}
