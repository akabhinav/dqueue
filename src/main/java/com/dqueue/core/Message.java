package com.dqueue.core;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Immutable message record representing a queue message.
 * Uses Java 21 record for conciseness and immutability.
 */
public record Message(
    String id,
    String key,
    byte[] payload,
    Map<String, String> headers,
    long timestamp,
    int priority,
    long expiryTime,
    int retryCount
) implements Serializable, Comparable<Message> {

    /**
     * Creates a new message with default values.
     */
    public Message {
        if (id == null) {
            id = UUID.randomUUID().toString();
        }
        if (timestamp == 0) {
            timestamp = Instant.now().toEpochMilli();
        }
        if (priority < 0 || priority > 10) {
            throw new IllegalArgumentException("Priority must be between 0 and 10");
        }
        headers = Map.copyOf(headers); // Ensure immutability
    }

    /**
     * Builder for Message creation.
     */
    public static class Builder {
        private String id;
        private String key;
        private byte[] payload;
        private Map<String, String> headers = Map.of();
        private long timestamp = 0;
        private int priority = 5; // Default medium priority
        private long expiryTime = 0; // 0 means no expiry
        private int retryCount = 0;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder payload(byte[] payload) {
            this.payload = payload;
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        public Builder expiryTime(long expiryTime) {
            this.expiryTime = expiryTime;
            return this;
        }

        public Builder retryCount(int retryCount) {
            this.retryCount = retryCount;
            return this;
        }

        public Message build() {
            return new Message(id, key, payload, headers, timestamp, priority, expiryTime, retryCount);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Compare messages by priority (higher first) and then timestamp (earlier first).
     */
    @Override
    public int compareTo(Message other) {
        // Higher priority comes first
        int priorityCompare = Integer.compare(other.priority, this.priority);
        if (priorityCompare != 0) {
            return priorityCompare;
        }
        // Earlier timestamp comes first
        return Long.compare(this.timestamp, other.timestamp);
    }

    /**
     * Check if message has expired.
     */
    public boolean isExpired() {
        return expiryTime > 0 && Instant.now().toEpochMilli() > expiryTime;
    }

    /**
     * Create a copy with incremented retry count.
     */
    public Message withIncrementedRetry() {
        return new Message(id, key, payload, headers, timestamp, priority, expiryTime, retryCount + 1);
    }
}
