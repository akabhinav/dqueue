package com.dqueue.core;

/**
 * Configuration for the distributed queue system.
 */
public record QueueConfig(
    int partitionCount,
    int replicationFactor,
    int batchSize,
    int flushIntervalMs,
    int maxRetries,
    long retentionMs,
    int maxMessageSize,
    String storagePath,
    boolean persistenceEnabled,
    int maxConcurrentConnections
) {

    public QueueConfig {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("Partition count must be positive");
        }
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("Replication factor must be positive");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size must be positive");
        }
    }

    /**
     * Default configuration optimized for high throughput.
     */
    public static QueueConfig defaultConfig() {
        return new QueueConfig(
            100,              // 100 partitions
            3,                // 3 replicas
            1000,             // Batch 1000 messages
            100,              // Flush every 100ms
            3,                // Max 3 retries
            604800000L,       // 7 days retention
            1048576,          // 1MB max message size
            "./data",         // Storage path
            true,             // Enable persistence
            100000            // 100K concurrent connections
        );
    }

    /**
     * Memory-only configuration for testing.
     */
    public static QueueConfig memoryOnlyConfig() {
        return new QueueConfig(
            10,               // 10 partitions
            1,                // No replication
            100,              // Smaller batch
            100,              // Flush every 100ms
            3,                // Max 3 retries
            86400000L,        // 1 day retention
            1048576,          // 1MB max message size
            null,             // No storage
            false,            // Disable persistence
            10000             // 10K concurrent connections
        );
    }
}
