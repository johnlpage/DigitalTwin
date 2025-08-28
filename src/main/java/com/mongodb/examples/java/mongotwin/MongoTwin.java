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

        // Configuration
        int numberOfThreads = 5; // Default number of threads
        int messagesPerThread = 100000; // Default number of messages per thread

        MongoClient singletonClient = MongoClients.create();
        LOG.info("Starting " + numberOfThreads + " threads, each processing " + messagesPerThread + " messages");

        // Create thread pool
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        boolean populateDb = true;
        Date startTime = new Date();
        // Create and submit worker threads
        for (int i = 0; i < numberOfThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                //Generator and strategy per thread
                MessageGenerator generator = new MessageGenerator();


                LOG.info("Thread " + threadId + " started");
                try (WriteStrategy strategy = createStrategy(singletonClient)) {
                    try {
                        for (int j = 0; j < messagesPerThread; j++) {
                            // Generate message
                            Map<String, Object> message;

                            if (populateDb) {
                                message = generator.getMessage(true, threadId * messagesPerThread + j);
                            } else {
                                message = generator.getMessage();
                            }

                            // Write the message using strategy
                            strategy.WriteMessage(message);
                        }
                    } catch (Exception e) {
                        LOG.error("Error in thread " + threadId + ": " + e.getMessage());
                        StackTraceElement[] stackTrace = e.getStackTrace();
                        StackTraceElement element = stackTrace[0]; // First element is where exception occurred

                        LOG.error("Exception occurred at:");
                        LOG.error("Class: " + element.getClassName());
                        LOG.error("Method: " + element.getMethodName());
                  
                        LOG.error("Line: " + element.getLineNumber());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                LOG.info("Thread " + threadId + " completed");
            });
        }

        // Shutdown executor
        executor.shutdown();

        try {
            // Wait for all threads to complete
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
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
    static WriteStrategy createStrategy(MongoClient mongoClient) {
        return new ReadReplaceStrategy(mongoClient);
    }
}
