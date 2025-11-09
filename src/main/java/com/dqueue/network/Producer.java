package com.dqueue.network;

import com.dqueue.core.DistributedQueue;
import com.dqueue.core.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Producer API for sending messages to the distributed queue.
 * Uses async operations with virtual threads for high throughput.
 */
public class Producer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    private final DistributedQueue queue;
    private final ProducerConfig config;
    private volatile boolean closed = false;

    public Producer(DistributedQueue queue, ProducerConfig config) {
        this.queue = queue;
        this.config = config;
        logger.info("Producer initialized");
    }

    /**
     * Send a single message asynchronously.
     */
    public CompletableFuture<Long> send(Message message) {
        if (closed) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Producer is closed"));
        }

        return queue.produce(message)
            .orTimeout(config.timeoutMs(), TimeUnit.MILLISECONDS)
            .exceptionally(throwable -> {
                logger.error("Failed to send message: {}", throwable.getMessage());
                throw new RuntimeException("Send failed", throwable);
            });
    }

    /**
     * Send a batch of messages for better throughput.
     */
    public CompletableFuture<List<Long>> sendBatch(List<Message> messages) {
        if (closed) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Producer is closed"));
        }

        if (messages.size() > config.maxBatchSize()) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Batch size exceeds limit"));
        }

        return queue.produceBatch(messages)
            .orTimeout(config.timeoutMs() * 2, TimeUnit.MILLISECONDS)
            .exceptionally(throwable -> {
                logger.error("Failed to send batch: {}", throwable.getMessage());
                throw new RuntimeException("Batch send failed", throwable);
            });
    }

    /**
     * Send a message and wait for completion.
     */
    public long sendSync(Message message) {
        try {
            return send(message).get(config.timeoutMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Sync send failed", e);
        }
    }

    @Override
    public void close() {
        closed = true;
        logger.info("Producer closed");
    }

    /**
     * Producer configuration.
     */
    public record ProducerConfig(
        long timeoutMs,
        int maxBatchSize,
        boolean enableCompression
    ) {
        public static ProducerConfig defaultConfig() {
            return new ProducerConfig(
                5000,    // 5 second timeout
                1000,    // Max 1000 messages per batch
                false    // No compression by default
            );
        }
    }
}
