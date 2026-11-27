package com.mercari.solution.server;

import java.util.Locale;

/**
 * Cross-site request forgery guard for the state-changing {@code /api} routes ({@code /pipeline}, {@code /launch},
 * {@code /agent}, {@code /feature}), which run pipelines and launch jobs with the server's credentials.
 * <p>
 * The server has no authentication of its own (Cloud Run IAM / IAP sits in front, and local runs have none), so
 * a page on another site that the user visits could otherwise drive these routes through the user's browser:
 * with a {@code text/plain} "simple" request, which a browser sends cross-site without a CORS preflight. The
 * guard closes that:
 * <ul>
 *   <li>only {@code POST} is accepted;</li>
 *   <li>the body must be declared {@code application/json} — a content type a cross-site page can send only after
 *       a CORS preflight, which this server never approves;</li>
 *   <li>a request the browser marks as coming from another site ({@code Sec-Fetch-Site: cross-site} or
 *       {@code same-site}) is refused outright.</li>
 * </ul>
 * The Builder UI already posts JSON from the same origin, and non-browser clients (curl, scripts) are unaffected
 * as long as they send {@code Content-Type: application/json}.
 */
final class ApiRequestGuard {

    private ApiRequestGuard() {
    }

    /** Why the request is refused, or null when it may proceed. */
    static String rejectReason(final String method, final String contentType, final String secFetchSite) {
        if (!"POST".equalsIgnoreCase(method)) {
            return "method not allowed: use POST";
        }
        if (secFetchSite != null) {
            final String site = secFetchSite.trim().toLowerCase(Locale.ROOT);
            if ("cross-site".equals(site) || "same-site".equals(site)) {
                return "cross-site requests are not accepted";
            }
        }
        if (!isJson(contentType)) {
            return "Content-Type must be application/json";
        }
        return null;
    }

    static int status(final String method) {
        return "POST".equalsIgnoreCase(method) ? 403 : 405;
    }

    private static boolean isJson(final String contentType) {
        if (contentType == null) {
            return false;
        }
        final int semicolon = contentType.indexOf(';');
        final String mediaType = (semicolon < 0 ? contentType : contentType.substring(0, semicolon))
                .trim().toLowerCase(Locale.ROOT);
        return "application/json".equals(mediaType);
    }

}
