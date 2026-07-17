package com.mercari.solution.server;

import com.mercari.solution.server.api.AgentService;
import com.mercari.solution.server.code.CodeRepository;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WebListener
public class ServerInitializer implements ServletContextListener {

    private static final Logger LOG = LoggerFactory.getLogger(ServerInitializer.class);

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        final ServletContext context = servletContextEvent.getServletContext();
        try {
            AgentService.init(context);
            LOG.info("AgentService ChatModel initialized successfully.");
        } catch (Exception e) {
            LOG.warn("Failed to initialize AgentService ChatModel. Agent feature will not be available.", e);
        }
        try {
            CodeRepository.init(context);
        } catch (Exception e) {
            LOG.warn("Failed to initialize CodeRepository. Code-reading tools will not be available.", e);
        }
    }

}
