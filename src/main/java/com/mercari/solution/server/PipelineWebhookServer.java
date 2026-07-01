package com.mercari.solution.server;

import com.mercari.solution.server.api.*;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class PipelineWebhookServer extends HttpServlet {

    @Override
    public void init() {

    }

    @Override
    protected void doGet(
            final HttpServletRequest request,
            final HttpServletResponse response) {

        process(request, response);
    }

    @Override
    protected void doPost(
            final HttpServletRequest request,
            final HttpServletResponse response) {

        process(request, response);
    }

    private void process(
            final HttpServletRequest request,
            final HttpServletResponse response) {

        switch (request.getPathInfo()) {
            case "/probe" -> ProbeService.serve(request, response);
            default -> {
                throw new IllegalArgumentException("Not supported path: " + request.getPathInfo());
            }
        }
    }

}
