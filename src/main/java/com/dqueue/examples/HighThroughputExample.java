package com.dqueue.examples;

import com.dqueue.core.DistributedQueue;
import com.dqueue.core.Message;
import com.dqueue.core.QueueConfig;
import com.dqueue.network.Producer;
import com.dqueue.metrics.MetricsCollector;
import com.dqueue.metrics.QueueMetrics;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

/**
 * Example demonstrating high throughput with batch operations and virtual threads.
 * Shows how the system can handle millions of requests.
 */
public class HighThroughputExample {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting High Throughput Example...\n");
        System.out.println("This example will produce 1 million messages using virtual threads\n");

        // Create queue with optimized configuration
        QueueConfig config = new QueueConfig(
            100,           // 100 partitions
            1,             // No replication for demo
            1000,          // Large batch size
            100,           // Flush every 100ms
            3,             // Max retries
            3600000L,      // 1 hour retention
            1048576,       // 1MB max message
            null,          // No persistence for demo
            false,         // Disable persistence
            1000000        // 1M concurrent connections
        );

        DistributedQueue queue = new DistributedQueue(config);
        QueueMetrics metrics = new QueueMetrics();
        MetricsCollector metricsCollector = new MetricsCollector(metrics, 5);
        Producer producer = new Producer(queue, Producer.ProducerConfig.defaultConfig());

        int totalMessages = 1_000_000;
        int batchSize = 1000;
        int numBatches = totalMessages / batchSize;

        System.out.println("Configuration:");
        System.out.println("  Total Messages: " + totalMessages);
        System.out.println("  Batch Size: " + batchSize);
        System.out.println("  Number of Batches: " + numBatches);
        System.out.println("  Partitions: " + config.partitionCount());
        System.out.println();

        CountDownLatch latch = new CountDownLatch(numBatches);
        long startTime = System.currentTimeMillis();

        // Produce messages in batches using virtual threads
        System.out.println("Starting message production...");

        for (int batch = 0; batch < numBatches; batch++) {
            final int batchNum = batch;

            // Create batch of messages
            List<Message> messages = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                int messageId = batchNum * batchSize + i;
                String content = "Message-" + messageId;

                Message message = Message.builder()
                    .key("key-" + (messageId % 1000))  // Distribute across partitions
                    .payload(content.getBytes(StandardCharsets.UTF_8))
                    .priority(5)
                    .build();

                messages.add(message);
            }

            // Send batch asynchronously
            producer.sendBatch(messages).thenAccept(offsets -> {
                for (Message msg : messages) {
                    metrics.recordProduced(msg.payload().length, 0);
                }
                latch.countDown();

                if (batchNum % 100 == 0) {
                    System.out.printf("Progress: %d/%d batches (%.1f%%)%n",
                        batchNum, numBatches, (batchNum * 100.0 / numBatches));
                }
            }).exceptionally(throwable -> {
                System.err.println("Batch " + batchNum + " failed: " + throwable.getMessage());
                latch.countDown();
                return null;
            });
        }

        // Wait for all batches to complete
        System.out.println("\nWaiting for all messages to be produced...");
        latch.await();

        long endTime = System.currentTimeMillis();
        long durationMs = endTime - startTime;
        double durationSeconds = durationMs / 1000.0;
        double throughput = totalMessages / durationSeconds;

        System.out.println("\n" + "=".repeat(60));
        System.out.println("PERFORMANCE RESULTS");
        System.out.println("=".repeat(60));
        System.out.printf("Total Messages: %,d%n", totalMessages);
        System.out.printf("Duration: %.2f seconds%n", durationSeconds);
        System.out.printf("Throughput: %,.0f messages/second%n", throughput);
        System.out.printf("Average Latency: %.3f ms/message%n", (durationMs * 1.0 / totalMessages));
        System.out.println("=".repeat(60));

        // Show partition distribution
        System.out.println("\nPartition Distribution:");
        List<DistributedQueue.PartitionInfo> partitions = queue.getAllPartitionInfo();
        int nonEmptyPartitions = 0;
        long totalMessagesInQueue = 0;

        for (DistributedQueue.PartitionInfo info : partitions) {
            if (info.messageCount() > 0) {
                nonEmptyPartitions++;
                totalMessagesInQueue += info.messageCount();
            }
        }

        System.out.printf("  Non-empty partitions: %d/%d%n", nonEmptyPartitions, partitions.size());
        System.out.printf("  Total messages in queue: %,d%n", totalMessagesInQueue);
        System.out.printf("  Average per partition: %,d%n", totalMessagesInQueue / Math.max(1, nonEmptyPartitions));

        // Cleanup
        producer.close();
        metricsCollector.close();
        queue.close();

        System.out.println("\nExample completed!");
    }
}
