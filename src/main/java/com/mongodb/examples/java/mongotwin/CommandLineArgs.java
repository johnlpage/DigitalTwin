package com.mongodb.examples.java.mongotwin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandLineArgs {
    private static final Logger LOG = LoggerFactory.getLogger(CommandLineArgs.class);

    private int numberOfThreads = 5;           // Default number of threads
    private int totalMessages = 500000;        // Default total number of messages
    private int numberOfDevices = totalMessages;
    private boolean populateDb = false;         // Default populate DB flag
    private String strategy = null;            // Strategy parameter (required)

    // Parse command line arguments
    public static CommandLineArgs parse(String[] args) {
        CommandLineArgs cmdArgs = new CommandLineArgs();

        for (int i = 0; i < args.length; i++) {
            switch (args[i].toLowerCase()) {
                case "--threads":
                case "-t":
                    if (i + 1 < args.length) {
                        cmdArgs.numberOfThreads = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--devices":
                case "-d":
                    if (i + 1 < args.length) {
                        cmdArgs.numberOfDevices = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--messages":
                case "-m":
                    if (i + 1 < args.length) {
                        cmdArgs.totalMessages = Integer.parseInt(args[++i]);
                    }
                    break;

                case "--populate":
                case "-p":
                    if (i + 1 < args.length) {
                        cmdArgs.populateDb = Boolean.parseBoolean(args[++i]);
                    }
                    break;

                case "--strategy":
                case "-s":
                    if (i + 1 < args.length) {
                        cmdArgs.strategy = args[++i];
                    }
                    break;

                case "--help":
                case "-h":
                    printUsage();
                    System.exit(0);
                    break;

                default:
                    LOG.warn("Unknown argument: " + args[i]);
                    break;
            }
        }

        // Validate arguments
        cmdArgs.validate();

        return cmdArgs;
    }

    private static void printUsage() {
        System.out.println("Usage: java MongoDbPopulator [options]");
        System.out.println("Options:");
        System.out.println("  --threads, -t <number>     Number of threads (default: 5)");
        System.out.println("  --messages, -m <number>    Total number of messages (default: 500000)");
        System.out.println("  --populate, -p <boolean>   Whether to populate DB (default: false)");
        System.out.println("  --strategy, -s <string>    Strategy to use (required)");
        System.out.println("  --help, -h                 Show this help message");
        System.out.println();
        System.out.println("Environment Variables:");
        System.out.println("  MONGODB_URI               MongoDB connection URI");
        System.out.println("                           (default: mongodb://localhost:27017)");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java MongoDbPopulator --threads 10 --messages 1000000 --strategy bulk");
        System.out.println("  java MongoDbPopulator -t 5 -m 100000 -p true -s single");
    }

    private void validate() {
        if (numberOfThreads <= 0) {
            throw new IllegalArgumentException("Number of threads must be positive");
        }
        if (totalMessages <= 0) {
            throw new IllegalArgumentException("Total messages must be positive");
        }

        if (populateDb) {
            numberOfDevices = totalMessages;
        }

    }

    // Getters
    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    public int getTotalMessages() {
        return totalMessages;
    }

    public int getNumberOfDevices() {
        return numberOfDevices;
    }

    public boolean isPopulateDb() {
        return populateDb;
    }

    public String getStrategy() {
        return strategy;
    }

    // Calculate messages per thread
    public int getMessagesPerThread() {
        return (int) Math.ceil((double) totalMessages / numberOfThreads);
    }
}
