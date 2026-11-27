package com.mercari.solution.server;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ApiRequestGuardTest {

    @Test
    public void testAcceptsSameOriginJsonPost() {
        Assertions.assertNull(ApiRequestGuard.rejectReason("POST", "application/json", null));
        Assertions.assertNull(ApiRequestGuard.rejectReason("POST", "application/json; charset=UTF-8", "same-origin"));
        Assertions.assertNull(ApiRequestGuard.rejectReason("post", "Application/JSON", "none"));
    }

    @Test
    public void testRejectsSimpleCrossSiteRequests() {
        // the content types a cross-site page can send without a CORS preflight
        Assertions.assertNotNull(ApiRequestGuard.rejectReason("POST", "text/plain", null));
        Assertions.assertNotNull(ApiRequestGuard.rejectReason("POST", "application/x-www-form-urlencoded", null));
        Assertions.assertNotNull(ApiRequestGuard.rejectReason("POST", "multipart/form-data; boundary=x", null));
        Assertions.assertNotNull(ApiRequestGuard.rejectReason("POST", null, null));
        Assertions.assertNotNull(ApiRequestGuard.rejectReason("POST", "application/jsonx", null));
        // a browser-marked cross-site request is refused even with a JSON content type
        Assertions.assertNotNull(ApiRequestGuard.rejectReason("POST", "application/json", "cross-site"));
        Assertions.assertNotNull(ApiRequestGuard.rejectReason("POST", "application/json", "same-site"));
    }

    @Test
    public void testRejectsOtherMethods() {
        Assertions.assertNotNull(ApiRequestGuard.rejectReason("GET", "application/json", null));
        Assertions.assertEquals(405, ApiRequestGuard.status("GET"));
        Assertions.assertEquals(403, ApiRequestGuard.status("POST"));
    }

}
