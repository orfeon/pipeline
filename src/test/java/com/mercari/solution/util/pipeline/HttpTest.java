package com.mercari.solution.util.pipeline;

import com.mercari.solution.util.domain.web.HttpUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

// Only network independent parts (request construction) are tested here
public class HttpTest {

    @Test
    public void testTransformCreation() {
        final HttpUtil.Request request = HttpUtil.Request.fromJsonString("""
                {
                  "name": "request1",
                  "endpoint": "https://example.com/api",
                  "method": "GET"
                }
                """);
        request.setDefaults();
        Assertions.assertTrue(request.validate().isEmpty());
        Assertions.assertEquals("request1", request.getName());

        final Http.Transform transform = Http.transform(
                List.of(request), 30, null, null, new ArrayList<>(), false);
        Assertions.assertNotNull(transform.outputTag);
        Assertions.assertNotNull(transform.failureTag);
    }

}
