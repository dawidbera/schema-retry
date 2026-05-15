package io.schemaretry.example;

import io.schemaretry.RetryOrchestrator;
import io.schemaretry.SchemaRetryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * Bootstrap class for the basic example application.
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("Starting schema-retry example application...");

        try {
            // 1. Load configuration from application.yaml in classpath
            InputStream configStream = Main.class.getClassLoader().getResourceAsStream("application.yaml");
            if (configStream == null) {
                log.error("Could not find application.yaml in classpath");
                return;
            }
            SchemaRetryConfig config = SchemaRetryConfig.load(configStream);

            // 2. Initialize the RetryOrchestrator
            try (RetryOrchestrator orchestrator = new RetryOrchestrator(config)) {
                
                // 3. Register and start listeners
                OrderProcessor orderProcessor = new OrderProcessor();
                orchestrator.registerListeners(orderProcessor);

                log.info("Application started and listening for messages. Press Ctrl+C to exit.");
                
                // Keep the main thread alive
                Thread.currentThread().join();
            }
        } catch (InterruptedException e) {
            log.info("Application interrupted, shutting down...");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Failed to start application", e);
        }
    }
}
