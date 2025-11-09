package com.dqueue.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main distributed queue implementation.
 * Manages multiple partitions and provides producer/consumer APIs.
 * Uses Java 21 Virtual Threads for handling millions of concurrent operations.
 */
public class DistributedQueue implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DistributedQueue.class);

    private final QueueConfig config;
    private final Partition[] partitions;
    private final PartitionRouter router;
    private final ScheduledExecutorService cleanupExecutor;
    private final ExecutorService virtualThreadExecutor;
    private final AtomicBoolean closed;

    public DistributedQueue(QueueConfig config) {
        this.config = config;
        this.partitions = new Partition[config.partitionCount()];
        this.router = new PartitionRouter(config.partitionCount());
        this.closed = new AtomicBoolean(false);

        // Initialize partitions
        for (int i = 0; i < config.partitionCount(); i++) {
            partitions[i] = new Partition(i, config);
        }

        // Use Virtual Threads for massive concurrency (Java 21)
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

        // Cleanup executor for expired messages
        this.cleanupExecutor = Executors.newScheduledThreadPool(1);
        startCleanupTask();

        logger.info("Distributed Queue initialized with {} partitions", config.partitionCount());
    }

    /**
     * Produce a message to the queue.
     * Returns a CompletableFuture with the assigned offset.
     */
    public CompletableFuture<Long> produce(Message message) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Queue is closed"));
        }

        return CompletableFuture.supplyAsync(() -> {
            try {
                // Route to partition based on message key
                int partitionId = router.route(message.key());
                Partition partition = partitions[partitionId];

                // Append to partition
                long offset = partition.append(message);

                logger.debug("Message {} appended to partition {} at offset {}",
                    message.id(), partitionId, offset);

                return offset;
            } catch (Exception e) {
                logger.error("Failed to produce message: {}", e.getMessage(), e);
                throw new CompletionException(e);
            }
        }, virtualThreadExecutor);
    }

    /**
     * Produce multiple messages in batch for better throughput.
     */
    public CompletableFuture<List<Long>> produceBatch(List<Message> messages) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Queue is closed"));
        }

        return CompletableFuture.supplyAsync(() -> {
            List<Long> offsets = new ArrayList<>(messages.size());
            for (Message message : messages) {
                int partitionId = router.route(message.key());
                Partition partition = partitions[partitionId];
                long offset = partition.append(message);
                offsets.add(offset);
            }
            logger.debug("Batch of {} messages produced", messages.size());
            return offsets;
        }, virtualThreadExecutor);
    }

    /**
     * Consume messages from a specific partition.
     */
    public CompletableFuture<List<Message>> consume(int partitionId, long fromOffset, int maxMessages) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Queue is closed"));
        }

        if (partitionId < 0 || partitionId >= partitions.length) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Invalid partition ID: " + partitionId));
        }

        return CompletableFuture.supplyAsync(() -> {
            Partition partition = partitions[partitionId];
            List<Message> messages = partition.poll(fromOffset, maxMessages);
            logger.debug("Consumed {} messages from partition {} starting at offset {}",
                messages.size(), partitionId, fromOffset);
            return messages;
        }, virtualThreadExecutor);
    }

    /**
     * Consume messages from all partitions.
     */
    public CompletableFuture<List<Message>> consumeAll(long fromOffset, int maxMessages) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Queue is closed"));
        }

        return CompletableFuture.supplyAsync(() -> {
            List<Message> allMessages = new ArrayList<>();
            int messagesPerPartition = Math.max(1, maxMessages / partitions.length);

            for (Partition partition : partitions) {
                List<Message> messages = partition.poll(fromOffset, messagesPerPartition);
                allMessages.addAll(messages);
                if (allMessages.size() >= maxMessages) {
                    break;
                }
            }

            return allMessages.subList(0, Math.min(allMessages.size(), maxMessages));
        }, virtualThreadExecutor);
    }

    /**
     * Acknowledge messages up to the given offset in a partition.
     */
    public CompletableFuture<Void> acknowledge(int partitionId, long upToOffset) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Queue is closed"));
        }

        return CompletableFuture.runAsync(() -> {
            if (partitionId >= 0 && partitionId < partitions.length) {
                partitions[partitionId].acknowledge(upToOffset);
                logger.debug("Acknowledged messages up to offset {} in partition {}",
                    upToOffset, partitionId);
            }
        }, virtualThreadExecutor);
    }

    /**
     * Get partition information.
     */
    public PartitionInfo getPartitionInfo(int partitionId) {
        if (partitionId < 0 || partitionId >= partitions.length) {
            throw new IllegalArgumentException("Invalid partition ID: " + partitionId);
        }

        Partition partition = partitions[partitionId];
        return new PartitionInfo(
            partitionId,
            partition.getCurrentOffset(),
            partition.getMessageCount()
        );
    }

    /**
     * Get information about all partitions.
     */
    public List<PartitionInfo> getAllPartitionInfo() {
        List<PartitionInfo> info = new ArrayList<>();
        for (int i = 0; i < partitions.length; i++) {
            info.add(getPartitionInfo(i));
        }
        return info;
    }

    /**
     * Start cleanup task for expired messages.
     */
    private void startCleanupTask() {
        cleanupExecutor.scheduleAtFixedRate(() -> {
            try {
                int totalCleaned = 0;
                for (Partition partition : partitions) {
                    int cleaned = partition.cleanupExpired();
                    totalCleaned += cleaned;
                }
                if (totalCleaned > 0) {
                    logger.info("Cleaned up {} expired messages", totalCleaned);
                }
            } catch (Exception e) {
                logger.error("Error during cleanup task", e);
            }
        }, 60, 60, TimeUnit.SECONDS); // Run every 60 seconds
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Shutting down distributed queue...");

            cleanupExecutor.shutdown();
            virtualThreadExecutor.shutdown();

            try {
                if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanupExecutor.shutdownNow();
                }
                if (!virtualThreadExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    virtualThreadExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cleanupExecutor.shutdownNow();
                virtualThreadExecutor.shutdownNow();
            }

            logger.info("Distributed queue shut down successfully");
        }
    }

    /**
     * Partition information record.
     */
    public record PartitionInfo(int partitionId, long currentOffset, long messageCount) {}
}
