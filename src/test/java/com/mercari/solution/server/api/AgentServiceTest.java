package com.mercari.solution.server.api;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AgentServiceTest {

    @Test
    public void testVertexBaseUrl() {
        // LangChain4j appends "/models/{model}:generateContent" to the base url
        Assertions.assertEquals(
                "https://aiplatform.googleapis.com/v1/projects/myproject/locations/global/publishers/google",
                AgentService.vertexBaseUrl("myproject", "global"));
        Assertions.assertEquals(
                "https://asia-northeast1-aiplatform.googleapis.com/v1/projects/myproject/locations/asia-northeast1/publishers/google",
                AgentService.vertexBaseUrl("myproject", "asia-northeast1"));
    }

}
