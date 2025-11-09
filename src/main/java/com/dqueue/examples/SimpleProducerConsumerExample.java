package com.dqueue.examples;

import com.dqueue.core.DistributedQueue;
import com.dqueue.core.Message;
import com.dqueue.core.QueueConfig;
import com.dqueue.network.Consumer;
import com.dqueue.network.Producer;
import com.dqueue.metrics.MetricsCollector;
import com.dqueue.metrics.QueueMetrics;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Simple example demonstrating basic producer-consumer pattern.
 */
public class SimpleProducerConsumerExample {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Simple Producer-Consumer Example...\n");

        // Create queue with memory-only configuration for demo
        QueueConfig config = QueueConfig.memoryOnlyConfig();
        DistributedQueue queue = new DistributedQueue(config);

        // Set up metrics
        QueueMetrics metrics = new QueueMetrics();
        MetricsCollector metricsCollector = new MetricsCollector(metrics, 10);

        // Create producer
        Producer producer = new Producer(queue, Producer.ProducerConfig.defaultConfig());

        // Create consumer for partition 0
        Consumer consumer = new Consumer(queue, Consumer.ConsumerConfig.defaultConfig(0));

        CountDownLatch latch = new CountDownLatch(10);

        // Start consuming
        System.out.println("Starting consumer...");
        consumer.startPolling((messages, cons) -> {
            for (Message msg : messages) {
                String content = new String(msg.payload(), StandardCharsets.UTF_8);
                System.out.println("Consumed: " + content);
                metrics.recordConsumed(msg.payload().length, 0);
                latch.countDown();
            }
        });

        // Produce messages
        System.out.println("Producing 10 messages...\n");
        for (int i = 0; i < 10; i++) {
            String content = "Message " + i;
            Message message = Message.builder()
                .key("key-" + (i % 3))  // Distribute across partitions
                .payload(content.getBytes(StandardCharsets.UTF_8))
                .priority(5)
                .build();

            producer.send(message).thenAccept(offset -> {
                System.out.println("Produced: " + content + " (offset: " + offset + ")");
                metrics.recordProduced(content.getBytes().length, 0);
            });

            Thread.sleep(100);
        }

        // Wait for consumption
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        if (completed) {
            System.out.println("\nAll messages consumed successfully!");
        } else {
            System.out.println("\nTimeout waiting for messages");
        }

        // Show partition info
        System.out.println("\nPartition Information:");
        queue.getAllPartitionInfo().forEach(info ->
            System.out.printf("Partition %d: offset=%d, messages=%d%n",
                info.partitionId(), info.currentOffset(), info.messageCount())
        );

        // Cleanup
        consumer.close();
        producer.close();
        metricsCollector.close();
        queue.close();

        System.out.println("\nExample completed!");
    }
}
