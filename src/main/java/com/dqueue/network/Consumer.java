package com.dqueue.network;

import com.dqueue.core.DistributedQueue;
import com.dqueue.core.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * Consumer API for receiving messages from the distributed queue.
 * Supports both pull and push models.
 */
public class Consumer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);

    private final DistributedQueue queue;
    private final ConsumerConfig config;
    private final AtomicLong currentOffset;
    private final ExecutorService pollExecutor;
    private volatile boolean closed = false;
    private volatile boolean polling = false;

    public Consumer(DistributedQueue queue, ConsumerConfig config) {
        this.queue = queue;
        this.config = config;
        this.currentOffset = new AtomicLong(0);
        this.pollExecutor = Executors.newVirtualThreadPerTaskExecutor();
        logger.info("Consumer initialized for partition {}", config.partitionId());
    }

    /**
     * Poll messages from the queue (pull model).
     */
    public CompletableFuture<List<Message>> poll() {
        if (closed) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Consumer is closed"));
        }

        long offset = currentOffset.get();
        return queue.consume(config.partitionId(), offset, config.maxPollRecords())
            .thenApply(messages -> {
                if (!messages.isEmpty()) {
                    logger.debug("Polled {} messages from offset {}", messages.size(), offset);
                }
                return messages;
            });
    }

    /**
     * Poll from all partitions.
     */
    public CompletableFuture<List<Message>> pollAll() {
        if (closed) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Consumer is closed"));
        }

        long offset = currentOffset.get();
        return queue.consumeAll(offset, config.maxPollRecords());
    }

    /**
     * Commit the current offset.
     */
    public CompletableFuture<Void> commit(long offset) {
        if (closed) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Consumer is closed"));
        }

        currentOffset.set(offset);
        return queue.acknowledge(config.partitionId(), offset)
            .thenRun(() -> logger.debug("Committed offset {}", offset));
    }

    /**
     * Start continuous polling (push model).
     * Invokes the callback for each batch of messages.
     */
    public void startPolling(BiConsumer<List<Message>, Consumer> messageHandler) {
        if (polling) {
            throw new IllegalStateException("Already polling");
        }

        polling = true;
        pollExecutor.submit(() -> {
            logger.info("Started continuous polling");

            while (polling && !closed) {
                try {
                    List<Message> messages = poll()
                        .get(config.pollTimeoutMs(), TimeUnit.MILLISECONDS);

                    if (!messages.isEmpty()) {
                        // Process messages in callback
                        messageHandler.accept(messages, this);

                        // Auto-commit if enabled
                        if (config.autoCommit()) {
                            long lastOffset = currentOffset.get() + messages.size();
                            commit(lastOffset).get();
                        }
                    } else {
                        // No messages, wait before next poll
                        Thread.sleep(config.pollIntervalMs());
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.info("Polling interrupted");
                    break;
                } catch (Exception e) {
                    logger.error("Error during polling", e);
                    try {
                        Thread.sleep(config.pollIntervalMs());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            logger.info("Stopped continuous polling");
        });
    }

    /**
     * Stop continuous polling.
     */
    public void stopPolling() {
        polling = false;
    }

    /**
     * Get the current offset.
     */
    public long getCurrentOffset() {
        return currentOffset.get();
    }

    @Override
    public void close() {
        closed = true;
        polling = false;
        pollExecutor.shutdown();
        try {
            if (!pollExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                pollExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pollExecutor.shutdownNow();
        }
        logger.info("Consumer closed");
    }

    /**
     * Consumer configuration.
     */
    public record ConsumerConfig(
        int partitionId,
        int maxPollRecords,
        long pollTimeoutMs,
        long pollIntervalMs,
        boolean autoCommit
    ) {
        public static ConsumerConfig defaultConfig(int partitionId) {
            return new ConsumerConfig(
                partitionId,
                100,     // Poll 100 messages at a time
                5000,    // 5 second poll timeout
                100,     // 100ms between polls
                true     // Auto-commit enabled
            );
        }

        public static ConsumerConfig allPartitionsConfig() {
            return new ConsumerConfig(
                -1,      // -1 indicates all partitions
                100,
                5000,
                100,
                true
            );
        }
    }
}
