package com.mercari.solution.server;

import com.mercari.solution.server.api.PipelineService;
import com.mercari.solution.server.api.ProbeService;
import com.mercari.solution.server.api.SchemaService;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.*;


public class PipelineApiServer extends HttpServlet {

    //private ServerLogHandler serverLogHandler;

    @Override
    public void init() {
        //serverLogHandler = createLogHandler();
    }

    /*
    private static ServerLogHandler createLogHandler() {
        final java.util.logging.Logger rootLogger = LogManager.getLogManager().getLogger("");
        final ServerLogHandler serverLogHandler = ServerLogHandler.of(Level.ALL);
        rootLogger.addHandler(serverLogHandler);
        rootLogger.setLevel(Level.ALL);
        return serverLogHandler;
    }
     */

    @Override
    protected void doGet(
            final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        switch (request.getServletPath()) {
            case "/probe" -> ProbeService.serve(request, response);
            case "/api/schema" -> SchemaService.serve(request, response);
            case "/api/pipeline" -> PipelineService.serve(request, response);
            default -> {}
        }
    }

    @Override
    protected void doPost(
            final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {

        switch (request.getServletPath()) {
            case "/probe" -> ProbeService.serve(request, response);
            case "/api/schema" -> SchemaService.serve(request, response);
            case "/api/pipeline" -> PipelineService.serve(request, response);
            default -> {}
        }
    }

}
