package com.dqueue.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Routes messages to partitions using consistent hashing.
 */
public class PartitionRouter {
    private final int partitionCount;
    private final MessageDigest md5;

    public PartitionRouter(int partitionCount) {
        this.partitionCount = partitionCount;
        try {
            this.md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }

    /**
     * Route a message key to a partition using consistent hashing.
     */
    public int route(String key) {
        if (key == null || key.isEmpty()) {
            // Random partition for null/empty keys
            return (int) (System.nanoTime() % partitionCount);
        }

        // Use MD5 hash for consistent routing
        byte[] hash = md5.digest(key.getBytes(StandardCharsets.UTF_8));

        // Convert first 4 bytes to int
        int hashValue = ((hash[0] & 0xFF) << 24) |
                       ((hash[1] & 0xFF) << 16) |
                       ((hash[2] & 0xFF) << 8) |
                       (hash[3] & 0xFF);

        // Map to partition
        return Math.abs(hashValue) % partitionCount;
    }
}
