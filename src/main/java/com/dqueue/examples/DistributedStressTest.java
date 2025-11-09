package com.dqueue.examples;

import com.dqueue.core.Message;
import com.dqueue.network.QueueClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stress test for distributed queue cluster.
 * Tests throughput and latency with multiple concurrent producers and consumers.
 *
 * Usage:
 *   First, start multiple queue servers using MultiNodeClusterExample
 *   Then run: java DistributedStressTest <numProducers> <messagesPerProducer> <node1Port> [node2Port] [node3Port]...
 */
public class DistributedStressTest {
    private static final Logger logger = LoggerFactory.getLogger(DistributedStressTest.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: java DistributedStressTest <numProducers> <messagesPerProducer> <port1> [port2] [port3]...");
            System.out.println();
            System.out.println("Example:");
            System.out.println("  java DistributedStressTest 10 10000 9001 9002 9003");
            System.out.println("  (10 producers, 10K messages each, 3 nodes)");
            return;
        }

        int numProducers = Integer.parseInt(args[0]);
        int messagesPerProducer = Integer.parseInt(args[1]);

        List<Integer> nodePorts = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            nodePorts.add(Integer.parseInt(args[i]));
        }

        System.out.println("=".repeat(60));
        System.out.println("DISTRIBUTED QUEUE STRESS TEST");
        System.out.println("=".repeat(60));
        System.out.println("Configuration:");
        System.out.println("  Producers: " + numProducers);
        System.out.println("  Messages per producer: " + messagesPerProducer);
        System.out.println("  Total messages: " + (numProducers * messagesPerProducer));
        System.out.println("  Cluster nodes: " + nodePorts.size());
        System.out.println("  Node ports: " + nodePorts);
        System.out.println("=".repeat(60));
        System.out.println();

        // Create clients connected to different nodes
        List<QueueClient> clients = new ArrayList<>();
        for (int i = 0; i < numProducers; i++) {
            int port = nodePorts.get(i % nodePorts.size());
            clients.add(new QueueClient("localhost", port));
        }

        AtomicLong successCount = new AtomicLong(0);
        AtomicLong failureCount = new AtomicLong(0);

        System.out.println("Starting stress test...\n");
        long startTime = System.currentTimeMillis();

        // Launch concurrent producers
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int p = 0; p < numProducers; p++) {
            final int producerId = p;
            final QueueClient client = clients.get(p);

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                logger.info("Producer {} started on node port {}",
                    producerId, nodePorts.get(producerId % nodePorts.size()));

                for (int i = 0; i < messagesPerProducer; i++) {
                    try {
                        Message message = Message.builder()
                            .key("producer-" + producerId)
                            .payload(("Message " + i + " from producer " + producerId)
                                .getBytes(StandardCharsets.UTF_8))
                            .priority(5)
                            .build();

                        client.produce(message).get();
                        successCount.incrementAndGet();

                        // Log progress
                        long current = successCount.get();
                        if (current % 10000 == 0) {
                            double progress = (current * 100.0) / (numProducers * messagesPerProducer);
                            logger.info("Progress: {}/{} ({:.1f}%)",
                                current, numProducers * messagesPerProducer, progress);
                        }

                    } catch (Exception e) {
                        failureCount.incrementAndGet();
                        if (failureCount.get() % 100 == 0) {
                            logger.error("Producer {} error (total failures: {}): {}",
                                producerId, failureCount.get(), e.getMessage());
                        }
                    }
                }

                logger.info("Producer {} completed", producerId);
            });

            futures.add(future);
        }

        // Wait for all producers to complete
        System.out.println("Waiting for all producers to complete...");
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long endTime = System.currentTimeMillis();
        long durationMs = endTime - startTime;
        double durationSeconds = durationMs / 1000.0;

        // Results
        System.out.println("\n" + "=".repeat(60));
        System.out.println("STRESS TEST RESULTS");
        System.out.println("=".repeat(60));
        System.out.printf("Total Messages Sent: %,d%n", successCount.get());
        System.out.printf("Failed Messages: %,d%n", failureCount.get());
        System.out.printf("Success Rate: %.2f%%%n",
            (successCount.get() * 100.0) / (successCount.get() + failureCount.get()));
        System.out.printf("Duration: %.2f seconds%n", durationSeconds);
        System.out.printf("Throughput: %,.0f messages/second%n", successCount.get() / durationSeconds);
        System.out.printf("Average Latency: %.3f ms/message%n",
            (durationMs * 1.0) / successCount.get());
        System.out.println("=".repeat(60));

        // Verify messages across cluster
        System.out.println("\nVerifying messages across cluster nodes...");

        for (int i = 0; i < nodePorts.size(); i++) {
            QueueClient verifyClient = new QueueClient("localhost", nodePorts.get(i));
            try {
                var partitions = verifyClient.getAllPartitionInfo().get();
                long totalMessages = partitions.stream()
                    .mapToLong(p -> p.messageCount())
                    .sum();

                System.out.printf("Node on port %d: %,d messages across %d partitions%n",
                    nodePorts.get(i), totalMessages, partitions.size());
            } catch (Exception e) {
                System.err.println("Failed to query node on port " + nodePorts.get(i));
            } finally {
                verifyClient.close();
            }
        }

        // Cleanup
        System.out.println("\nCleaning up...");
        clients.forEach(QueueClient::close);

        System.out.println("Stress test completed!");
    }
}
