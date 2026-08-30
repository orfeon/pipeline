package com.mercari.solution.server;

import com.mercari.solution.server.api.AgentService;
import com.mercari.solution.server.api.FeatureService;
import com.mercari.solution.server.api.LaunchService;
import com.mercari.solution.server.api.PipelineService;
import com.mercari.solution.server.api.SpecService;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.*;


public class PipelineApiServer extends HttpServlet {

    @Override
    public void init() {
        SpecService.init();
    }

    @Override
    protected void doGet(
            final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        process(request, response);
    }

    @Override
    protected void doPost(
            final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        process(request, response);
    }

    private void process(
            final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        final String path = request.getPathInfo();
        if (path == null || path.isBlank() || "/".equals(path)) {
            // GET /api without a sub-path (e.g. a browser or health probe): not a route
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.setContentType("application/json");
            response.getWriter().println("{\"status\":\"error\",\"error\":\"unknown api path; use /api/spec, /api/pipeline, /api/launch, /api/agent or /api/feature\"}");
            return;
        }
        switch (path) {
            case "/spec" -> SpecService.serve(request, response);
            case "/pipeline" -> PipelineService.serve(request, response);
            case "/launch" -> LaunchService.serve(request, response);
            case "/agent" -> AgentService.serve(request, response);
            case "/feature" -> FeatureService.serve(request, response);
            default -> {
                // Handle dynamic paths like /api/spec/{type}/{name}
                final String pathInfo = request.getPathInfo();
                if (pathInfo != null && pathInfo.startsWith("/spec/")) {
                    handleModuleSchemaRequest(pathInfo, request, response);
                }
            }
        }
    }

    /**
     * Handle requests for schemas.
     * Path format: /api/spec/{type}/{moduleName} or /api/spec/{schemaName}
     */
    private void handleModuleSchemaRequest(
            final String path,
            final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        // Remove prefix "/spec/"
        final String remaining = path.substring("/spec/".length());
        final String[] parts = remaining.split("/");

        if (parts.length == 1) {
            switch (parts[0]) {
                case "system" -> SpecService.serveSystemSchema(request, response);
                case "options" -> SpecService.serveOptionsSchema(request, response);
                case "launch" -> SpecService.serveLaunchSchema(request, response);
                default -> {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    response.setContentType("application/json");
                    response.getWriter().println("{\"error\": \"Unknown schema: " + parts[0] + "\"}");
                }
            }
        } else if (parts.length == 2) {
            final String moduleType = parts[0];
            final String moduleName = parts[1];
            SpecService.serveModuleSchema(request, response, moduleType, moduleName);
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            response.setContentType("application/json");
            response.getWriter().println("{\"error\": \"Invalid path. Expected: /api/spec/{type}/{moduleName}\"}");
        }
    }

}