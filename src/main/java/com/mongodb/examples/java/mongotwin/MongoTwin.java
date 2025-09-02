package com.mongodb.examples.java.mongotwin;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MongoTwin {

    private static final Logger LOG = LoggerFactory.getLogger(MongoTwin.class);

    public static void main(String[] args) {
        LOG.info("MongoTwin Version 1.0");

        // Parse command line arguments
        CommandLineArgs cmdArgs = CommandLineArgs.parse(args);

        // Get MongoDB URI from environment variable
        String mongoUri = System.getenv("MONGODB_URI");
        if (mongoUri == null || mongoUri.trim().isEmpty()) {
            mongoUri = "mongodb://localhost:27017"; // Default fallback
            LOG.warn("MONGODB_URI environment variable not set, using default: " + mongoUri);
        }

        LOG.info("Configuration:");
        LOG.info("  Threads: " + cmdArgs.getNumberOfThreads());
        LOG.info("  Total Messages: " + cmdArgs.getTotalMessages());
        LOG.info("  Messages per Thread: " + cmdArgs.getMessagesPerThread());
        LOG.info("  Populate DB: " + cmdArgs.isPopulateDb());
        LOG.info("  Strategy: " + cmdArgs.getStrategy());
        LOG.info("  MongoDB URI: " + mongoUri);

        // Configuration
        int numberOfThreads = cmdArgs.getNumberOfThreads();
        int messagesPerThread = cmdArgs.getMessagesPerThread();

        MongoClient singletonClient = MongoClients.create(mongoUri);

        if (cmdArgs.isPopulateDb()) {
            // LOG.info("Dropping existing database");
            // singletonClient.getDatabase("digitwin").drop();
        }

        LOG.info("Starting " + numberOfThreads + " threads, each processing " + messagesPerThread + " messages");

        // Create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        boolean populateDb = cmdArgs.isPopulateDb();
        Date startTime = new Date();

        // Create and submit worker threads
        for (int i = 0; i < numberOfThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                //Generator and strategy per thread
                MessageGenerator generator = new MessageGenerator(cmdArgs.getNumberOfDevices());

                LOG.info("Thread {} started", threadId + 1);

                try (WriteStrategy strategy = createStrategy(singletonClient, cmdArgs.getStrategy())) {
                    try {
                        for (int j = 0; j < messagesPerThread; j++) {
                            // Generate message
                            Map<String, Object> message;

                            if (populateDb) {
                                //If populate is true then generate message with a specified ID and also fully populate
                                // the fields in them
                                message = generator.getMessage(true, threadId * messagesPerThread + j);
                            } else {
                                message = generator.getMessage();
                            }

                            // Write the message using strategy
                            strategy.WriteMessage(message);
                        }
                    } catch (Exception e) {
                        LOG.error("Error in thread " + threadId + ": " + e.getMessage());

                        e.printStackTrace();

                    }
                } catch (Exception e) {
                    LOG.error("Error {}", e.getMessage());
                    System.exit(1);

                }
                LOG.info("Thread " + threadId + " completed");
            });

        }

        // Shutdown executor
        executor.shutdown();

        try {
            // Wait for all threads to complete
            if (!executor.awaitTermination(60, TimeUnit.DAYS)) {
                LOG.warn("Executor did not terminate within 60 seconds, forcing shutdown");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.warn("Main thread was interrupted");
            executor.shutdownNow();
        }

        LOG.info("All threads completed");
        Date endTime = new Date();
        LOG.info("Took " + (endTime.getTime() - startTime.getTime()) + "ms to process " + numberOfThreads * messagesPerThread + " messages");
    }

    // Factory method or simple selection
    static WriteStrategy createStrategy(MongoClient mongoClient, String strategy) {
        return switch (strategy) {

            case "ReadReplaceStrategy" -> new ReadReplaceStrategy(mongoClient);
            case "BlobStrategy" -> new BlobStrategy(mongoClient);
            case "ServerSideStrategy" -> new ServerSideStrategy(mongoClient);
            default -> throw new IllegalStateException("Unexpected Strategy value: " + strategy);
        };
    }
}
